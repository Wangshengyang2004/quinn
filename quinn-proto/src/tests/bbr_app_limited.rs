use super::*;

fn configured_pair() -> (Pair, ConnectionHandle, ConnectionHandle) {
    let mut pair = Pair::default();
    pair.latency = Duration::from_millis(100);
    let mut config = client_config();
    let mut controller = crate::congestion::BbrConfig::default();
    controller
        .initial_window_packets(32)
        .max_window_packets(256)
        .high_gain(2.885)
        .high_cwnd_gain(2.0)
        .congestion_window_gain(2.0)
        .enable_ack_aggregation_during_startup(false)
        .expire_ack_aggregation_in_startup(false)
        .enable_overestimate_avoidance(false);
    Arc::get_mut(&mut config.transport)
        .unwrap()
        .congestion_controller_factory(Arc::new(controller));
    let (client, server) = pair.connect_with(config);
    (pair, client, server)
}

fn bbr(connection: &Connection) -> Box<crate::congestion::Bbr> {
    connection
        .congestion_state()
        .clone_box()
        .into_any()
        .downcast::<crate::congestion::Bbr>()
        .unwrap()
}

fn full_bandwidth(connection: &Connection) -> bool {
    bbr(connection).reached_full_bandwidth()
}

fn consume_stream(pair: &mut Pair, server: ConnectionHandle, stream: StreamId) -> usize {
    let mut recv = pair.server_recv(server, stream);
    let mut chunks = recv.read(true).unwrap();
    let mut bytes = 0;
    while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
        assert!(chunk.bytes.iter().all(|byte| *byte == 7));
        bytes += chunk.bytes.len();
    }
    let _ = chunks.finalize();
    bytes
}

#[test]
fn bbr_app_limited_light_stream_does_not_exhaust_startup() {
    let (mut pair, client, server) = configured_pair();
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    let mut accepted = false;
    let mut received = 0;
    for ms in 0..8500 {
        if ms < 8000 && ms % 10 == 0 {
            assert_eq!(
                pair.client_send(client, stream).write(&[7; 368]).unwrap(),
                368
            );
        }
        pair.drive_client();
        pair.drive_server();
        if !accepted {
            if let Some(id) = pair.server_streams(server).accept(Dir::Uni) {
                assert_eq!(id, stream);
                accepted = true;
            }
        }
        if accepted {
            received += consume_stream(&mut pair, server, stream);
        }
        pair.time += Duration::from_millis(1);
    }
    assert_eq!(received, 800 * 368);
    assert!(
        !full_bandwidth(pair.client_conn_mut(client)),
        "a light stream cannot establish that the unconstrained path reached capacity"
    );
    run_backlogged_phase(&mut pair, client, server, stream, true);
}

#[test]
fn bbr_app_limited_backlogged_stream_still_exits_startup() {
    let (mut pair, client, server) = configured_pair();
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    run_backlogged_phase(&mut pair, client, server, stream, false);
}

fn run_backlogged_phase(
    pair: &mut Pair,
    client: ConnectionHandle,
    server: ConnectionHandle,
    stream: StreamId,
    mut accepted: bool,
) {
    let mut sent = 0;
    let mut received = 0;
    for _ in 0..5000 {
        match pair.client_send(client, stream).write(&[7; 64 * 1024]) {
            Ok(bytes) => sent += bytes,
            Err(WriteError::Blocked) => {}
            result => panic!("unexpected source result {result:?}"),
        }
        pair.drive_client();
        pair.drive_server();
        if !accepted {
            if let Some(id) = pair.server_streams(server).accept(Dir::Uni) {
                assert_eq!(id, stream);
                accepted = true;
            }
        }
        if accepted {
            received += consume_stream(pair, server, stream);
        }
        pair.time += Duration::from_millis(1);
    }
    assert!(sent > 1024 * 1024 && received > 1024 * 1024);
    assert!(
        full_bandwidth(pair.client_conn_mut(client)),
        "a continuously backlogged source must retain normal startup exit"
    );
}

/// 20 Mbit/s line in the client's direction
const LINE: u64 = 2_500_000;
/// 1 Mbit/s token-bucket policer with a 16 KiB burst
const POLICED: u64 = 125_000;
const BURST: u64 = 16_384;
/// Line rate until then, policed afterwards
const CLAMP: Duration = Duration::from_secs(2);

/// A client stream over a line that a policer clamps after [`CLAMP`]
struct Policed {
    pair: Pair,
    client: ConnectionHandle,
    server: ConnectionHandle,
    stream: StreamId,
    accepted: bool,
    written: usize,
    received: usize,
    start: Instant,
    line_free: Instant,
    tokens: u64,
    last_fill: Instant,
    /// Datagrams sent and dropped by the policer since the clamp
    policed: (u64, u64),
    startup_exit: Option<Duration>,
}

impl Policed {
    fn new() -> Self {
        let mut pair = Pair::default_with_deterministic_pns();
        pair.latency = Duration::from_millis(25);
        let mut config = client_config_with_deterministic_pns();
        let mut controller = crate::congestion::BbrConfig::default();
        controller
            .initial_window_packets(32)
            .max_window_packets(20_000)
            .high_gain(2.885)
            .high_cwnd_gain(2.0)
            .congestion_window_gain(2.0)
            .drain_to_target(false)
            .enable_ack_aggregation_during_startup(false)
            .expire_ack_aggregation_in_startup(false);
        Arc::get_mut(&mut config.transport)
            .unwrap()
            .mtu_discovery_config(None)
            .congestion_controller_factory(Arc::new(controller));
        let (client, server) = pair.connect_with(config);
        pair.drive();
        let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
        let start = pair.time;
        Self {
            pair,
            client,
            server,
            stream,
            accepted: false,
            written: 0,
            received: 0,
            start,
            line_free: start,
            tokens: BURST,
            last_fill: start + CLAMP,
            policed: (0, 0),
            startup_exit: None,
        }
    }

    /// Advances in one-millisecond steps until `until` after the start. With a `window`, the
    /// source keeps at most that many bytes written but not yet read by the peer.
    fn run(&mut self, until: Duration, window: Option<usize>) {
        while self.pair.time < self.start + until {
            loop {
                let room = window.map_or(64 * 1024, |w| {
                    (w + self.received)
                        .saturating_sub(self.written)
                        .min(64 * 1024)
                });
                if room == 0 {
                    break;
                }
                match self
                    .pair
                    .client_send(self.client, self.stream)
                    .write(&vec![7; room])
                {
                    Ok(n) => self.written += n,
                    Err(WriteError::Blocked) => break,
                    Err(e) => panic!("{e:?}"),
                }
            }
            let queued = self.pair.server.inbound.len();
            self.pair.drive_client();
            self.shape(queued);
            self.pair.drive_server();
            if !self.accepted {
                self.accepted = self
                    .pair
                    .server_streams(self.server)
                    .accept(Dir::Uni)
                    .is_some();
            }
            if self.accepted {
                self.received += consume_stream(&mut self.pair, self.server, self.stream);
            }
            while self.pair.client_conn_mut(self.client).poll().is_some() {}
            self.pair.time += Duration::from_millis(1);
            if self.startup_exit.is_none() && full_bandwidth(self.pair.client_conn_mut(self.client))
            {
                self.startup_exit = Some(self.pair.time - self.start);
            }
        }
    }

    /// Serializes the datagrams queued after the first `queued` onto the line, then polices them
    fn shape(&mut self, queued: usize) {
        let now = self.pair.time;
        let sent: Vec<_> = self.pair.server.inbound.drain(queued..).collect();
        for (_, ecn, packet) in sent {
            let len = packet.len() as u64;
            self.line_free =
                self.line_free.max(now) + Duration::from_nanos(len * 1_000_000_000 / LINE);
            let arrival = self.line_free + self.pair.latency;
            if arrival >= self.start + CLAMP {
                let elapsed = arrival.saturating_duration_since(self.last_fill);
                self.tokens =
                    (self.tokens + elapsed.as_micros() as u64 * POLICED / 1_000_000).min(BURST);
                self.last_fill = self.last_fill.max(arrival);
                self.policed.0 += 1;
                if self.tokens < len {
                    self.policed.1 += 1;
                    continue;
                }
                self.tokens -= len;
            }
            self.pair.server.inbound.push_back((arrival, ecn, packet));
        }
        self.pair
            .server
            .inbound
            .make_contiguous()
            .sort_by_key(|(arrival, _, _)| *arrival);
    }

    fn estimate(&mut self) -> u64 {
        bbr(self.pair.client_conn_mut(self.client)).bandwidth_estimate()
    }

    fn report(&mut self, label: &str) {
        let estimate = self.estimate();
        println!(
            "{label}: written={} received={} policed={:?} estimate={estimate} startup_exit={:?}",
            self.written, self.received, self.policed, self.startup_exit
        );
    }
}

#[test]
fn bbr_policed_backlogged_stream_estimate_follows_policer() {
    let mut path = Policed::new();
    path.run(CLAMP, None);
    path.report("open");
    assert!(path.startup_exit.is_some() && path.estimate() > 8 * POLICED);
    path.run(CLAMP + Duration::from_secs(10), None);
    path.report("policed");
    assert!(
        path.estimate() <= 2 * POLICED,
        "estimate stays {}x the policed rate after 10 s",
        path.estimate() / POLICED
    );
}

#[test]
fn bbr_policed_window_limited_stream_leaves_startup_on_loss() {
    let mut path = Policed::new();
    path.run(CLAMP, Some(64 * 1024));
    path.report("open");
    assert_eq!(
        path.startup_exit, None,
        "a window-limited source cannot show a plateau"
    );
    path.run(CLAMP + Duration::from_secs(5), Some(64 * 1024));
    path.report("policed");
    assert!(
        path.startup_exit.is_some(),
        "STARTUP continued for 5 s of policed loss"
    );
}
