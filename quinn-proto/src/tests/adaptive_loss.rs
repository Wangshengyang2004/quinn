//! Loss detection on a simulated bottleneck that reorders or drops the client's datagrams
//!
//! The digests printed here are compared across builds: adapting loss detection to reordering
//! must not change what the congestion controller is told on a path without spurious losses.
use super::unsent_limit::{Log, Recorded, RecordedFactory, bulk_calls, read_all};
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

fn adaptive(transport: &mut TransportConfig) {
    transport.adaptive_loss_thresholds(true);
}

#[test]
fn adaptive_loss_thresholds_ignore_paths_without_spurious_losses() {
    let lossy = lossy_calls(adaptive);
    assert_eq!(lossy, lossy_calls(|_| {}));
    let bulk = bulk_calls(adaptive).0;
    assert_eq!(bulk, bulk_calls(|_| {}).0);
    println!(
        "ADAPTIVE_LOSS lossy adaptive digest={:016x} bulk adaptive lost_packets={} digest={:016x}",
        digest(&lossy),
        bulk.1,
        digest(&bulk)
    );
}

#[test]
fn reordering_stops_loss_declarations_after_first_spurious_loss() {
    let mut path = Bottleneck::new(adaptive);
    let mut lost_at_detection = None;
    path.run(Duration::from_secs(1), reorder, |path, _| {
        let stats = path.stats();
        if lost_at_detection.is_none() && stats.spurious_lost_packets > 0 {
            lost_at_detection = Some(stats.lost_packets);
        }
    });
    let learned = path.stats();
    path.run(Duration::from_secs(2), reorder, |_, _| {});
    let stats = path.stats();
    println!(
        "ADAPTIVE_LOSS reordered adaptive lost_at_detection={lost_at_detection:?} lost_after_1s={} lost_packets={} spurious_lost_packets={} sent_packets={} congestion_events={} received={} window={:?} rtt={:?}",
        learned.lost_packets,
        stats.lost_packets,
        stats.spurious_lost_packets,
        stats.sent_packets,
        stats.congestion_events,
        path.received,
        stats.reordering_window,
        stats.rtt
    );
    assert!(lost_at_detection.is_some());
    // The window grows with each larger reordering seen, then holds.
    assert_eq!(stats.lost_packets, learned.lost_packets);
    assert_eq!(stats.spurious_lost_packets, stats.lost_packets);
    assert!(stats.reordering_window.is_some());
}

/// Drop the first datagram sent in each `period` from `start`, `count` times, recording when
fn periodic_drops(
    start: Duration,
    period: Duration,
    count: usize,
    drops: &mut Vec<Duration>,
) -> impl FnMut(u64, Duration) -> Fate + '_ {
    let mut next = start + period;
    move |_, elapsed| {
        if drops.len() == count || elapsed < next {
            return Fate::Deliver;
        }
        while next <= elapsed {
            next += period;
        }
        drops.push(elapsed);
        Fate::Drop
    }
}

/// Losses declared over three seconds in which one datagram is dropped every 200 ms, eight
/// times, without reordering: for each drop, how long after it was sent its loss was declared,
/// and the smoothed RTT and reordering window at that moment
fn detection_delays(path: &mut Bottleneck) -> Vec<(Duration, Duration, Option<Duration>)> {
    let mut drops = Vec::new();
    let mut declared = Vec::new();
    let mut lost = path.stats().lost_packets;
    let start = path.pair.time - path.start;
    path.run(
        Duration::from_secs(3),
        periodic_drops(start, Duration::from_millis(200), 8, &mut drops),
        |path, elapsed| {
            let stats = path.stats();
            if stats.lost_packets > lost {
                assert_eq!(
                    stats.lost_packets,
                    lost + 1,
                    "only the dropped packet is lost"
                );
                lost = stats.lost_packets;
                declared.push((elapsed, stats.rtt, stats.reordering_window));
            }
        },
    );
    assert_eq!(drops.len(), 8);
    assert_eq!(declared.len(), 8, "every drop is detected");
    drops
        .iter()
        .zip(declared)
        .map(|(sent, (at, rtt, window))| (at - *sent, rtt, window))
        .collect()
}

#[test]
fn real_loss_on_reordering_aware_path_is_detected_within_one_rtt() {
    let mut path = Bottleneck::new(adaptive);
    path.run(Duration::from_secs(2), reorder, |_, _| {});
    let trained = path.stats();
    assert!(trained.reordering_window.is_some());
    let delays = detection_delays(&mut path);
    assert_eq!(
        path.stats().spurious_lost_packets,
        trained.spurious_lost_packets
    );
    // The same reordering first, so both paths start from a similar window and RTT.
    let mut default = Bottleneck::new(|_| {});
    default.run(Duration::from_secs(2), reorder, |_, _| {});
    let default_delays = detection_delays(&mut default);
    println!(
        "ADAPTIVE_LOSS detection adaptive={:?} default={:?}",
        delays
            .iter()
            .map(|(delay, rtt, window)| (
                delay.as_millis(),
                rtt.as_millis(),
                window.map(|w| w.as_millis())
            ))
            .collect::<Vec<_>>(),
        default_delays
            .iter()
            .map(|(delay, rtt, _)| (delay.as_millis(), rtt.as_millis()))
            .collect::<Vec<_>>()
    );
    let mean = |delays: &[(Duration, Duration, Option<Duration>)]| {
        delays.iter().map(|x| x.0).sum::<Duration>() / delays.len() as u32
    };
    let mut largest_window = Duration::ZERO;
    for &(delay, rtt, window) in &delays {
        let window = window.expect("still reordering-aware");
        largest_window = largest_window.max(window);
        assert!(window <= rtt);
        // Loss is declared by the first ACK or timer after one RTT plus the window; the RTT used
        // is the larger of the latest and smoothed samples, and ACKs follow one-millisecond steps
        // and the receiver's ACK delay.
        assert!(
            delay <= rtt + window + Duration::from_millis(10),
            "{delay:?} {rtt:?} {window:?}"
        );
        assert!(delay > rtt, "declared before the window: {delay:?} {rtt:?}");
    }
    // Later than the default thresholds by no more than the window
    assert!(mean(&delays) <= mean(&default_delays) + largest_window + Duration::from_millis(2));
}

#[test]
fn reordering_window_never_exceeds_smoothed_rtt() {
    let mut path = Bottleneck::new(adaptive);
    let mut largest = Duration::ZERO;
    path.run(
        Duration::from_secs(3),
        |index, _| match index % 10 {
            9 => Fate::Delay(Duration::from_millis(60)),
            _ => Fate::Deliver,
        },
        |path, _| {
            let stats = path.stats();
            if let Some(window) = stats.reordering_window {
                assert!(window <= stats.rtt, "{window:?} {:?}", stats.rtt);
                largest = largest.max(window);
            }
        },
    );
    let stats = path.stats();
    println!(
        "ADAPTIVE_LOSS capped largest_window={largest:?} rtt={:?} lost_packets={} spurious_lost_packets={} max_time_reordering={:?}",
        stats.rtt,
        stats.lost_packets,
        stats.spurious_lost_packets,
        stats.spurious_loss_max_time_reordering
    );
    assert!(stats.spurious_loss_max_time_reordering > stats.rtt);
    assert!(largest > Duration::ZERO);
}

#[test]
fn decay_restores_default_thresholds() {
    let mut path = Bottleneck::new(adaptive);
    path.run(Duration::from_secs(2), reorder, |_, _| {});
    assert!(path.stats().reordering_window.is_some());

    // One drop every 200 ms, well apart: each loss starts a recovery episode.
    let mut stats = path.stats();
    let mut episodes = 0;
    let mut episodes_at_reset = None;
    let mut drops = Vec::new();
    let start = path.pair.time - path.start;
    path.run(
        Duration::from_secs(6),
        periodic_drops(start, Duration::from_millis(200), 25, &mut drops),
        |path, _| {
            let now = path.stats();
            if now.spurious_lost_packets > stats.spurious_lost_packets {
                episodes = 0;
            }
            if now.lost_packets > stats.lost_packets {
                episodes += 1;
            }
            if episodes_at_reset.is_none() && now.reordering_window.is_none() {
                episodes_at_reset = Some(episodes);
            }
            stats = now;
        },
    );
    println!("ADAPTIVE_LOSS decay episodes_at_reset={episodes_at_reset:?} episodes={episodes}");
    assert_eq!(episodes_at_reset, Some(16));

    // The packet threshold declares reordered packets lost again, and they train the path anew.
    let before = path.stats();
    path.run(Duration::from_secs(1), reorder, |_, _| {});
    let after = path.stats();
    assert!(after.spurious_lost_packets > before.spurious_lost_packets);
    assert!(after.reordering_window.is_some());
}

/// Whether the fork's BBR inside the client's recording controller has left startup
fn left_startup(path: &mut Bottleneck) -> bool {
    let recorded = path
        .pair
        .client_conn_mut(path.client)
        .congestion_state()
        .clone_box()
        .into_any()
        .downcast::<Recorded>()
        .unwrap();
    recorded
        .inner
        .into_any()
        .downcast::<crate::congestion::Bbr>()
        .unwrap()
        .reached_full_bandwidth()
}

/// The fork's BBR, as HY2 drives it, from the first packet for four seconds: when it left startup, the losses declared before
/// then, and the bytes delivered in the first second and in the three seconds after
fn startup(
    configure: impl FnOnce(&mut TransportConfig),
    fate: impl FnMut(u64, Duration) -> Fate,
) -> (Option<Duration>, u64, u64, usize, usize) {
    let mut path = Bottleneck::new(|transport| {
        transport.congestion_controller_factory(Arc::new(RecordedFactory::default()));
        configure(transport);
    });
    let mut exit = None;
    let mut first_second = 0;
    path.run(Duration::from_secs(4), fate, |path, elapsed| {
        if exit.is_none() && left_startup(path) {
            let stats = path.stats();
            exit = Some((elapsed, stats.lost_packets, stats.spurious_lost_packets));
        }
        if elapsed == Duration::from_secs(1) {
            first_second = path.received;
        }
    });
    let (at, lost, spurious) = exit.map_or((None, 0, 0), |(at, lost, spurious)| {
        (Some(at), lost, spurious)
    });
    (
        at,
        lost,
        spurious,
        first_second,
        path.received - first_second,
    )
}

#[test]
fn bbr_startup_under_reordering() {
    let clean = startup(|_| {}, |_, _| Fate::Deliver);
    let default = startup(|_| {}, reorder);
    let adaptive = startup(adaptive, reorder);
    for (name, (exit, lost, spurious, first, after)) in [
        ("clean", clean),
        ("reordered default", default),
        ("reordered adaptive", adaptive),
    ] {
        println!(
            "ADAPTIVE_LOSS startup {name}: exit_ms={:?} lost_before_exit={lost} spurious_before_exit={spurious} first_second_bytes={first} next_three_seconds_mbit_s={:.2}",
            exit.map(|at| at.as_millis()),
            after as f64 * 8.0 / 3.0 / 1e6
        );
    }
    // Spurious losses no longer accumulate towards BBR's loss exit, and the rate reached after
    // startup is that of the clean path.
    assert!(adaptive.1 < default.1);
    assert!(adaptive.0 >= clean.0.map(|at| at / 2));
    assert!(adaptive.4 as f64 >= clean.4 as f64 * 0.95);
}
