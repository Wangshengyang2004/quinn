//! Stream write admission and congestion-controller inputs, for comparison across builds
//!
//! The digests printed here are compared between builds: bounding unsent stream data must not
//! change write admission while the bound is unset, nor what the congestion controller is told
//! when the application keeps up with the path.
use super::*;
use crate::congestion::{BbrConfig, Controller, ControllerFactory, ControllerMetrics};
use std::hash::{DefaultHasher, Hash, Hasher};

type Log = Arc<Mutex<Vec<(&'static str, u64, u64)>>>;

/// The fork's BBR with HY2's explicit pacing rate, reporting every call to `log`
struct Recorded {
    inner: Box<dyn Controller>,
    log: Log,
}

impl Recorded {
    fn push(&self, call: &'static str, a: u64, b: u64) {
        self.log.lock().unwrap().push((call, a, b));
    }
}

#[derive(Default)]
struct RecordedFactory(Log);

impl ControllerFactory for RecordedFactory {
    fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
        let mut config = BbrConfig::default();
        config.initial_window_packets(32).max_window_packets(20_000);
        Box::new(Recorded {
            inner: Arc::new(config).build(now, mtu),
            log: self.0.clone(),
        })
    }
}

impl Controller for Recorded {
    fn on_sent(&mut self, now: Instant, bytes: u64, packet: u64, in_flight: u64) {
        self.push("sent", packet, bytes);
        self.inner.on_sent(now, bytes, packet, in_flight);
    }
    fn on_begin_congestion_event(&mut self, prior_bytes_in_flight: u64) {
        self.push("begin", prior_bytes_in_flight, 0);
        self.inner.on_begin_congestion_event(prior_bytes_in_flight);
    }
    fn on_app_limited(&mut self) {
        self.push("idle", 0, 0);
        self.inner.on_app_limited();
    }
    fn on_rtt_update(&mut self, rtt: &RttEstimator) {
        self.inner.on_rtt_update(rtt);
    }
    fn on_packet_lost(&mut self, packet: u64, bytes: u64) {
        self.push("lost", packet, bytes);
        self.inner.on_packet_lost(packet, bytes);
    }
    fn on_packet_discarded(&mut self, packet: u64) {
        self.push("discarded", packet, 0);
        self.inner.on_packet_discarded(packet);
    }
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        packet: u64,
        bytes: u64,
        in_flight_at_send: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        self.push("ack", packet, app_limited as u64);
        self.inner
            .on_ack(now, sent, packet, bytes, in_flight_at_send, app_limited, rtt);
    }
    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_acked: Option<u64>,
    ) {
        self.push("end", in_flight, app_limited as u64);
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_acked);
    }
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        persistent: bool,
        lost_bytes: u64,
        lost_packets: u64,
        last_packet: Option<u64>,
        in_flight: Option<u64>,
    ) {
        self.push("congestion", lost_bytes, lost_packets);
        self.inner.on_congestion_event(
            now,
            sent,
            persistent,
            lost_bytes,
            lost_packets,
            last_packet,
            in_flight,
        );
    }
    fn on_mtu_update(&mut self, mtu: u16) {
        self.inner.on_mtu_update(mtu);
    }
    fn window(&self) -> u64 {
        self.inner.window()
    }
    fn can_send(&self, in_flight: u64, bytes: u64) -> bool {
        self.inner.can_send(in_flight, bytes)
    }
    fn pacing_rate(&self) -> Option<u64> {
        // As HY2 does: drive BBR's own pacing rate, converted to bytes per second.
        self.inner.metrics().pacing_rate.map(|rate| rate / 8)
    }
    fn metrics(&self) -> ControllerMetrics {
        self.inner.metrics()
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            log: self.log.clone(),
        })
    }
    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

fn read_all(pair: &mut Pair, server: ConnectionHandle, stream: StreamId) -> usize {
    let mut recv = pair.server_recv(server, stream);
    let Ok(mut chunks) = recv.read(true) else {
        return 0;
    };
    let mut bytes = 0;
    while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
        bytes += chunk.bytes.len();
    }
    let _ = chunks.finalize();
    bytes
}

const BULK_BYTES: usize = 8 << 20;

type Bulk = (Vec<(&'static str, u64, u64)>, u64, u64, u64, Duration);

/// A writer that refills its stream every millisecond moves `BULK_BYTES` over a 50 ms round trip
/// with no rate limit, and flow control credit for all of it. Returns every controller call, the
/// stats that summarise them and the elapsed time, plus the number of blocked writes.
fn bulk_calls(configure: impl FnOnce(&mut TransportConfig)) -> (Bulk, usize) {
    let mut server = server_config();
    let mut server_transport = TransportConfig::default();
    server_transport
        .deterministic_packet_numbers(true)
        .stream_receive_window(VarInt::from_u32(16 << 20));
    server.transport = Arc::new(server_transport);
    let mut pair = Pair::new(Default::default(), server);
    pair.latency = Duration::from_millis(25);
    let factory = Arc::new(RecordedFactory::default());
    let log = factory.0.clone();
    let mut config = client_config_with_deterministic_pns();
    let transport = Arc::get_mut(&mut config.transport).unwrap();
    transport
        .mtu_discovery_config(None)
        .congestion_controller_factory(factory);
    configure(transport);
    let (client, server) = pair.connect_with(config);
    pair.drive();
    log.lock().unwrap().clear();
    let start = pair.time;

    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    let mut written = 0;
    let mut received = 0;
    let mut accepted = false;
    let mut blocked = 0;
    while received < BULK_BYTES {
        assert!(pair.time - start < Duration::from_secs(20), "transfer stalled");
        while written < BULK_BYTES {
            let len = (BULK_BYTES - written).min(64 * 1024);
            match pair.client_send(client, stream).write(&vec![7; len]) {
                Ok(n) => written += n,
                Err(WriteError::Blocked) => {
                    blocked += 1;
                    break;
                }
                Err(e) => panic!("{e:?}"),
            }
        }
        pair.drive_client();
        pair.drive_server();
        if !accepted {
            accepted = pair.server_streams(server).accept(Dir::Uni).is_some();
        }
        if accepted {
            received += read_all(&mut pair, server, stream);
        }
        while pair.client_conn_mut(client).poll().is_some() {}
        pair.time += Duration::from_millis(1);
    }
    let stats = pair.client_conn_mut(client).stats();
    let calls = log.lock().unwrap().clone();
    (
        (
            calls,
            stats.path.lost_packets,
            stats.path.congestion_events,
            stats.path.sent_packets,
            pair.time - start,
        ),
        blocked,
    )
}

fn digest(value: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn bulk_controller_calls_are_deterministic() {
    let (first, blocked) = bulk_calls(|_| {});
    assert_eq!(blocked, 0, "credit covers the whole transfer");
    assert_eq!(first, bulk_calls(|_| {}).0);
    println!(
        "UNSENT_LIMIT bulk unset calls={} idle={} lost_packets={} congestion_events={} sent_packets={} elapsed_ms={} digest={:016x}",
        first.0.len(),
        first.0.iter().filter(|call| call.0 == "idle").count(),
        first.1,
        first.2,
        first.3,
        first.4.as_millis(),
        digest(&first)
    );
}

/// Results of a writer offering 5,000 bytes every millisecond for 500 ms to a stream whose
/// receive window is 64 KiB, together with the `Writable` events seen after each step
fn credit_limited_admission(
    configure: impl FnOnce(&mut TransportConfig),
) -> Vec<(u64, i64, bool)> {
    let mut server = server_config();
    let mut server_transport = TransportConfig::default();
    server_transport
        .deterministic_packet_numbers(true)
        .mtu_discovery_config(None)
        .stream_receive_window(VarInt::from_u32(64 * 1024));
    server.transport = Arc::new(server_transport);
    let mut pair = Pair::new(Default::default(), server);
    pair.latency = Duration::from_millis(10);
    let mut config = client_config_with_deterministic_pns();
    let transport = Arc::get_mut(&mut config.transport).unwrap();
    transport.mtu_discovery_config(None);
    configure(transport);
    let (client, server) = pair.connect_with(config);
    pair.drive();
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    let mut accepted = false;
    let mut sequence = Vec::new();
    for ms in 0..500 {
        let result = match pair.client_send(client, stream).write(&[7; 5000]) {
            Ok(n) => n as i64,
            Err(WriteError::Blocked) => -1,
            Err(e) => panic!("{e:?}"),
        };
        pair.drive_client();
        pair.drive_server();
        if !accepted {
            accepted = pair.server_streams(server).accept(Dir::Uni).is_some();
        }
        // Read slowly so the window, not the path, limits the writer.
        if accepted && ms % 20 == 0 {
            read_all(&mut pair, server, stream);
        }
        let mut writable = false;
        while let Some(event) = pair.client_conn_mut(client).poll() {
            writable |= matches!(event, Event::Stream(StreamEvent::Writable { id }) if id == stream);
        }
        sequence.push((ms, result, writable));
        pair.time += Duration::from_millis(1);
    }
    sequence
}

#[test]
fn credit_limited_admission_is_deterministic() {
    let first = credit_limited_admission(|_| {});
    assert_eq!(first, credit_limited_admission(|_| {}));
    assert!(first.iter().any(|x| x.1 == -1), "the window must block writes");
    assert!(first.iter().any(|x| x.2), "the writer must be woken");
    println!(
        "UNSENT_LIMIT admission unset blocked={} woken={} accepted={} digest={:016x}",
        first.iter().filter(|x| x.1 == -1).count(),
        first.iter().filter(|x| x.2).count(),
        first.iter().filter(|x| x.1 > 0).map(|x| x.1).sum::<i64>(),
        digest(&first)
    );
}

const DELAY: Duration = Duration::from_millis(100);
/// 10 Mbit/s
const RATE: u64 = 1_250_000;
/// `DELAY` at `RATE`
const BOUND: u64 = 125_000;

/// A controller with a fixed explicit pacing rate (or window pacing when `rate` is `None`) and a
/// window too large to block, reporting source exhaustion to `idle`
#[derive(Clone)]
struct Paced {
    rate: Option<u64>,
    idle: Arc<Mutex<usize>>,
}

impl ControllerFactory for Paced {
    fn build(self: Arc<Self>, _now: Instant, _mtu: u16) -> Box<dyn Controller> {
        Box::new((*self).clone())
    }
}

impl Controller for Paced {
    fn on_app_limited(&mut self) {
        *self.idle.lock().unwrap() += 1;
    }
    fn on_congestion_event(
        &mut self,
        _: Instant,
        _: Instant,
        _: bool,
        _: u64,
        _: u64,
        _: Option<u64>,
        _: Option<u64>,
    ) {
    }
    fn on_mtu_update(&mut self, _: u16) {}
    fn window(&self) -> u64 {
        // Above `u32::MAX` the window pacer is disabled.
        1 << 40
    }
    fn pacing_rate(&self) -> Option<u64> {
        self.rate
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }
    fn initial_window(&self) -> u64 {
        self.window()
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

fn limited_pair(controller: Paced, delay: Duration) -> (Pair, ConnectionHandle, ConnectionHandle) {
    let mut pair = Pair::default();
    pair.latency = Duration::from_millis(10);
    let mut config = client_config();
    Arc::get_mut(&mut config.transport)
        .unwrap()
        .mtu_discovery_config(None)
        .max_unsent_stream_delay(Some(delay))
        .congestion_controller_factory(Arc::new(controller));
    let (client, server) = pair.connect_with(config);
    pair.drive();
    (pair, client, server)
}

fn paced(rate: Option<u64>) -> Paced {
    Paced {
        rate,
        idle: Arc::default(),
    }
}

fn writable(pair: &mut Pair, client: ConnectionHandle, stream: StreamId) -> bool {
    let mut writable = false;
    while let Some(event) = pair.client_conn_mut(client).poll() {
        writable |= matches!(event, Event::Stream(StreamEvent::Writable { id }) if id == stream);
    }
    writable
}

/// Fill `stream` until a write is blocked, returning the bytes accepted
fn fill(pair: &mut Pair, client: ConnectionHandle, stream: StreamId) -> u64 {
    let mut written = 0;
    loop {
        match pair.client_send(client, stream).write(&[7; 64 * 1024]) {
            Ok(n) => written += n as u64,
            Err(WriteError::Blocked) => return written,
            Err(e) => panic!("{e:?}"),
        }
    }
}

#[test]
fn unsent_limit_holds_writer_at_bound_while_path_drains() {
    let (mut pair, client, server) = limited_pair(paced(Some(RATE)), DELAY);
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    let (mut accepted, mut received, mut max_unsent, mut woken) = (false, 0, 0, 0);
    for _ in 0..1000 {
        fill(&mut pair, client, stream);
        let unsent = pair.client_conn_mut(client).stream_unsent(stream);
        assert!(unsent <= BOUND, "unsent {unsent} exceeds {BOUND}");
        max_unsent = max_unsent.max(unsent);
        pair.drive_client();
        pair.drive_server();
        if !accepted {
            accepted = pair.server_streams(server).accept(Dir::Uni).is_some();
        }
        if accepted {
            received += read_all(&mut pair, server, stream);
        }
        woken += writable(&mut pair, client, stream) as usize;
        pair.time += Duration::from_millis(1);
    }
    println!("UNSENT_LIMIT bound max_unsent={max_unsent} received={received} woken={woken}");
    // At a fixed rate the bound is exact: admission never exceeds it.
    assert_eq!(max_unsent, BOUND);
    assert!(woken > 900, "transmission wakes the writer every step: {woken}");
    // The path still drains at its paced rate.
    assert!(received > 1_000_000, "{received}");
}

#[test]
fn unsent_limit_wakes_writer_on_transmission_without_acks() {
    let (mut pair, client, _) = limited_pair(paced(Some(RATE)), DELAY);
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    assert_eq!(fill(&mut pair, client, stream), BOUND);
    assert!(!writable(&mut pair, client, stream));
    let mut transmitted = 0;
    for _ in 0..50 {
        // Every datagram is lost, so no acknowledgement or credit ever arrives: only
        // transmission can make room.
        pair.drive_client();
        pair.server.inbound.clear();
        let room = BOUND - pair.client_conn_mut(client).stream_unsent(stream);
        assert!(room > 0, "pacing admits a packet at each wakeup");
        transmitted += room;
        assert!(writable(&mut pair, client, stream), "writer must wake");
        assert_eq!(fill(&mut pair, client, stream), room);
        // Blocked again: no further wakeup until transmission makes room.
        assert!(!writable(&mut pair, client, stream), "no busy loop");
        pair.time = pair.client.next_wakeup().unwrap();
    }
    assert!(transmitted > 50 * 1000, "{transmitted}");
}

#[test]
fn unsent_limit_admits_and_sends_other_stream_promptly() {
    let (mut pair, client, server) = limited_pair(paced(Some(RATE)), DELAY);
    let bulk = pair.client_streams(client).open(Dir::Uni).unwrap();
    let light = pair.client_streams(client).open(Dir::Uni).unwrap();
    let mut accepted = Vec::new();
    let mut written_at = std::collections::VecDeque::new();
    let (mut delivered, mut completed) = (0, 0);
    let mut max_delay = Duration::ZERO;
    let mut max_unsent = 0;
    for ms in 0..1000 {
        fill(&mut pair, client, bulk);
        max_unsent = max_unsent.max(pair.client_conn_mut(client).stream_unsent(bulk));
        if ms < 900 && ms % 10 == 0 {
            // The bulk stream's backlog does not delay admission on another stream.
            assert_eq!(pair.client_send(client, light).write(&[9; 100]), Ok(100));
            written_at.push_back(pair.time);
        }
        pair.drive_client();
        pair.drive_server();
        while let Some(id) = pair.server_streams(server).accept(Dir::Uni) {
            accepted.push(id);
        }
        if accepted.contains(&bulk) {
            read_all(&mut pair, server, bulk);
        }
        if accepted.contains(&light) {
            delivered += read_all(&mut pair, server, light);
            while (completed + 1) * 100 <= delivered {
                let sent = written_at.pop_front().unwrap();
                max_delay = max_delay.max(pair.time - sent);
                completed += 1;
            }
        }
        writable(&mut pair, client, bulk);
        pair.time += Duration::from_millis(1);
    }
    println!(
        "UNSENT_LIMIT rotation max_delay_ms={} bulk_max_unsent={max_unsent}",
        max_delay.as_millis()
    );
    assert_eq!(delivered, 90 * 100);
    assert_eq!(max_unsent, BOUND);
    // Same-priority streams rotate per packet, so each small write leaves within a couple of
    // paced packets (about 1 ms each at this rate) rather than behind a 100 ms backlog.
    assert!(max_delay <= pair.latency + Duration::from_millis(3), "{max_delay:?}");
}

#[test]
fn unsent_limit_exhausted_backlog_reports_source_idle() {
    // Window pacing is disabled and the delay is zero, so the allowance is the ten-packet floor
    // and one transmission empties it.
    let controller = paced(None);
    let idle = controller.idle.clone();
    let (mut pair, client, _) = limited_pair(controller, Duration::ZERO);
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    assert_eq!(fill(&mut pair, client, stream), 10 * 1200);
    *idle.lock().unwrap() = 0;
    pair.drive_client();
    assert_eq!(pair.client_conn_mut(client).stream_unsent(stream), 0);
    // Nothing is left to send and the writer is not short of credit: the source, as admitted
    // by this policy, is exhausted. BBR marks the following samples application-limited, so a
    // gap caused by a slow refill cannot lower its bandwidth estimate.
    assert!(*idle.lock().unwrap() > 0);
    assert!(writable(&mut pair, client, stream));
}

#[test]
fn unsent_limit_leaves_bulk_controller_calls_unchanged() {
    let (unset, _) = bulk_calls(|_| {});
    let (limited, blocked) = bulk_calls(|transport| {
        transport.max_unsent_stream_delay(Some(DELAY));
    });
    println!(
        "UNSENT_LIMIT bulk limited blocked_writes={blocked} calls={} digest={:016x}",
        limited.0.len(),
        digest(&limited)
    );
    assert!(blocked > 0, "the bound must engage");
    assert!(unset == limited, "controller inputs differ");
}
