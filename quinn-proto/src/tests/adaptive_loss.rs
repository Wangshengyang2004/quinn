//! Loss detection on a simulated bottleneck that reorders or drops the client's datagrams
//!
//! The digests printed here are compared across builds: adapting loss detection to reordering
//! must not change what the congestion controller is told on a path without spurious losses.
use super::unsent_limit::{Log, Recorded, read_all};
use super::*;
use crate::congestion::{Controller, ControllerFactory, CubicConfig};
use std::hash::{DefaultHasher, Hash, Hasher};

/// One-way delay in each direction
const LATENCY: Duration = Duration::from_millis(25);
/// Bottleneck rate of the client's direction in bytes per second (20 Mbit/s)
const RATE: u64 = 2_500_000;

/// What happens to a datagram the client sends
enum Fate {
    Deliver,
    Delay(Duration),
    Drop,
}

/// Cubic reporting every call to its log; unlike BBR, it draws no random numbers
#[derive(Default)]
struct RecordedCubic(Log);

impl ControllerFactory for RecordedCubic {
    fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
        Box::new(Recorded {
            inner: Arc::new(CubicConfig::default()).build(now, mtu),
            log: self.0.clone(),
        })
    }
}

/// A client keeping a stream backlogged through Cubic, over a bottleneck that queues without limit
struct Bottleneck {
    pair: Pair,
    client: ConnectionHandle,
    server: ConnectionHandle,
    stream: StreamId,
    accepted: bool,
    received: usize,
    log: Log,
    /// When the bottleneck finishes sending the last datagram queued
    busy_until: Instant,
    /// Datagrams sent by the client since the start
    datagrams: u64,
    start: Instant,
}

impl Bottleneck {
    fn new(configure: impl FnOnce(&mut TransportConfig)) -> Self {
        let mut pair = Pair::default_with_deterministic_pns();
        pair.latency = LATENCY;
        let factory = Arc::new(RecordedCubic::default());
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
        let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
        let start = pair.time;
        Self {
            pair,
            client,
            server,
            stream,
            accepted: false,
            received: 0,
            log,
            busy_until: start,
            datagrams: 0,
            start,
        }
    }

    /// Advance `duration` in steps of one millisecond, deciding each client datagram's fate from
    /// its index and send time, and calling `step` after each millisecond with the elapsed time
    fn run(
        &mut self,
        duration: Duration,
        mut fate: impl FnMut(u64, Duration) -> Fate,
        mut step: impl FnMut(&mut Self, Duration),
    ) {
        let end = self.pair.time + duration;
        while self.pair.time < end {
            loop {
                match self
                    .pair
                    .client_send(self.client, self.stream)
                    .write(&[7; 64 * 1024])
                {
                    Ok(_) => {}
                    Err(WriteError::Blocked) => break,
                    Err(e) => panic!("{e:?}"),
                }
            }
            let queued = self.pair.server.inbound.len();
            self.pair.drive_client();
            self.shape(queued, &mut fate);
            self.pair.drive_server();
            if !self.accepted {
                self.accepted = self
                    .pair
                    .server_streams(self.server)
                    .accept(Dir::Uni)
                    .is_some();
            }
            if self.accepted {
                self.received += read_all(&mut self.pair, self.server, self.stream);
            }
            while self.pair.client_conn_mut(self.client).poll().is_some() {}
            self.pair.time += Duration::from_millis(1);
            let elapsed = self.pair.time - self.start;
            step(self, elapsed);
        }
    }

    /// Pass the datagrams the client queued after the first `queued` through the bottleneck
    fn shape(&mut self, queued: usize, fate: &mut impl FnMut(u64, Duration) -> Fate) {
        let now = self.pair.time;
        let sent: Vec<_> = self.pair.server.inbound.drain(queued..).collect();
        for (_, ecn, packet) in sent {
            let index = self.datagrams;
            self.datagrams += 1;
            let extra = match fate(index, now - self.start) {
                Fate::Deliver => Duration::ZERO,
                Fate::Delay(extra) => extra,
                Fate::Drop => continue,
            };
            let serialization = Duration::from_nanos(packet.len() as u64 * 1_000_000_000 / RATE);
            self.busy_until = self.busy_until.max(now) + serialization;
            self.pair
                .server
                .inbound
                .push_back((self.busy_until + LATENCY + extra, ecn, packet));
        }
        self.pair
            .server
            .inbound
            .make_contiguous()
            .sort_by_key(|(arrival, _, _)| *arrival);
    }

    fn stats(&mut self) -> PathStats {
        self.pair.client_conn_mut(self.client).stats().path
    }

    fn calls(&self) -> Vec<(&'static str, u64, u64)> {
        self.log.lock().unwrap().clone()
    }
}

fn digest(value: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

type Lossy = (Vec<(&'static str, u64, u64)>, u64, u64, u64, usize);

/// Three seconds over the bottleneck dropping every 50th datagram, without reordering: every
/// controller call, the loss and send counters and the bytes delivered
fn lossy_calls(configure: impl FnOnce(&mut TransportConfig)) -> Lossy {
    let mut path = Bottleneck::new(configure);
    path.run(
        Duration::from_secs(3),
        |index, _| match index % 50 {
            49 => Fate::Drop,
            _ => Fate::Deliver,
        },
        |_, _| {},
    );
    let stats = path.stats();
    assert_eq!(stats.spurious_lost_packets, 0);
    (
        path.calls(),
        stats.lost_packets,
        stats.congestion_events,
        stats.sent_packets,
        path.received,
    )
}

#[test]
fn lossy_controller_calls_are_deterministic() {
    let first = lossy_calls(|_| {});
    assert!(first.1 > 0, "the bottleneck must drop packets");
    assert_eq!(first, lossy_calls(|_| {}));
    println!(
        "ADAPTIVE_LOSS lossy calls={} lost_packets={} congestion_events={} sent_packets={} received={} digest={:016x}",
        first.0.len(),
        first.1,
        first.2,
        first.3,
        first.4,
        digest(&first)
    );
}

/// Delay every 10th datagram by 15 ms, beyond both default loss thresholds, and drop none
fn reorder(index: u64, _: Duration) -> Fate {
    match index % 10 {
        9 => Fate::Delay(Duration::from_millis(15)),
        _ => Fate::Deliver,
    }
}

/// Three seconds over the bottleneck reordering as `reorder` does: every controller call, the
/// loss, spurious loss and send counters and the bytes delivered
fn reordered_calls(configure: impl FnOnce(&mut TransportConfig)) -> (Lossy, u64) {
    let mut path = Bottleneck::new(configure);
    path.run(Duration::from_secs(3), reorder, |_, _| {});
    let stats = path.stats();
    (
        (
            path.calls(),
            stats.lost_packets,
            stats.congestion_events,
            stats.sent_packets,
            path.received,
        ),
        stats.spurious_lost_packets,
    )
}

#[test]
fn reordered_controller_calls_are_deterministic() {
    let first = reordered_calls(|_| {});
    assert!(first.1 > 0, "the reordering must cause spurious losses");
    assert_eq!(first, reordered_calls(|_| {}));
    println!(
        "ADAPTIVE_LOSS reordered calls={} lost_packets={} spurious_lost_packets={} congestion_events={} sent_packets={} received={} digest={:016x}",
        first.0.0.len(),
        first.0.1,
        first.1,
        first.0.2,
        first.0.3,
        first.0.4,
        digest(&first)
    );
}
