//! `TransportConfig::max_pto_interval` caps the backed-off application-data PTO.
use super::*;

const CAP: Duration = Duration::from_millis(200);

fn configs(max_pto_interval: Option<Duration>) -> (ServerConfig, ClientConfig) {
    let mut server = server_config();
    Arc::get_mut(&mut server.transport)
        .unwrap()
        .mtu_discovery_config(None);
    let mut client = client_config();
    Arc::get_mut(&mut client.transport)
        .unwrap()
        .mtu_discovery_config(None)
        .max_pto_interval(max_pto_interval);
    (server, client)
}

/// Advance `pair` until `until` with every datagram lost in both directions, including those
/// already on the way
///
/// Returns the distinct times at which the client sent datagrams.
fn blackhole(pair: &mut Pair, until: Instant) -> Vec<Instant> {
    pair.client.inbound.clear();
    pair.server.inbound.clear();
    let mut sent = Vec::new();
    loop {
        pair.client.drive(pair.time, pair.server.addr);
        if !pair.client.outbound.is_empty() && sent.last() != Some(&pair.time) {
            sent.push(pair.time);
        }
        pair.client.outbound.clear();
        pair.server.drive(pair.time, pair.client.addr);
        pair.server.outbound.clear();
        match min_opt(pair.client.next_wakeup(), pair.server.next_wakeup()) {
            Some(t) if t <= until => pair.time = pair.time.max(t),
            _ => return sent,
        }
    }
}

fn intervals(sent: &[Instant]) -> Vec<Duration> {
    sent.windows(2).map(|w| w[1] - w[0]).collect()
}

/// Intervals between the client's sends after a one-packet write on an established connection
/// whose datagrams are then all lost
fn data_pto_intervals(max_pto_interval: Option<Duration>) -> Vec<Duration> {
    let _guard = subscribe();
    let (server, client) = configs(max_pto_interval);
    let mut pair = Pair::new(Default::default(), server);
    let (client_ch, _) = pair.connect_with(client);
    pair.drive();
    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(&[0xab; 100]).unwrap();
    let until = pair.time + Duration::from_secs(20);
    let intervals = intervals(&blackhole(&mut pair, until));
    info!(?intervals, "data PTO intervals");
    // The first interval is the PTO without backoff
    assert!(intervals.len() >= 8, "{intervals:?}");
    intervals
}

#[test]
fn data_pto_doubles_by_default() {
    let intervals = data_pto_intervals(None);
    for pair in intervals[..8].windows(2) {
        assert_eq!(pair[1], pair[0] * 2, "{intervals:?}");
    }
}

#[test]
fn data_pto_doubles_up_to_max_interval() {
    let intervals = data_pto_intervals(Some(CAP));
    let base = intervals[0];
    assert!(base < CAP / 4, "base PTO {base:?} leaves no room to double");
    for (i, &interval) in intervals[..8].iter().enumerate() {
        assert_eq!(
            interval,
            (base * 2u32.pow(i as u32)).min(CAP),
            "{intervals:?}"
        );
    }
}

#[test]
fn data_pto_max_interval_below_base_keeps_base() {
    let intervals = data_pto_intervals(Some(Duration::from_millis(1)));
    let base = intervals[0];
    assert!(base > Duration::from_millis(1));
    assert!(intervals[..8].iter().all(|&x| x == base), "{intervals:?}");
}

#[test]
fn initial_pto_backoff_ignores_max_interval() {
    let _guard = subscribe();
    let (server, client) = configs(Some(Duration::from_millis(10)));
    let mut pair = Pair::new(Default::default(), server);
    pair.begin_connect(client);
    let until = pair.time + Duration::from_secs(20);
    let intervals = intervals(&blackhole(&mut pair, until));
    info!(?intervals, "Initial PTO intervals");
    // 333 ms initial RTT: 999 ms, then doubling
    assert!(intervals.len() >= 4, "{intervals:?}");
    for pair in intervals.windows(2) {
        assert_eq!(pair[1], pair[0] * 2, "{intervals:?}");
    }
}

/// Intervals between the client's sends after it completes the handshake but, with every later
/// datagram lost, never sees HANDSHAKE_DONE or an ACK of its Handshake flight
fn unconfirmed_handshake_pto_intervals(max_pto_interval: Option<Duration>) -> Vec<Duration> {
    let _guard = subscribe();
    let (server, client) = configs(max_pto_interval);
    let mut pair = Pair::new(Default::default(), server);
    pair.latency = Duration::from_millis(50);
    let client_ch = pair.begin_connect(client);
    while pair.client_conn_mut(client_ch).is_handshaking() {
        assert!(pair.step());
    }
    let until = pair.time + Duration::from_secs(20);
    let intervals = intervals(&blackhole(&mut pair, until));
    info!(?intervals, "unconfirmed handshake PTO intervals");
    assert!(intervals.len() >= 4, "{intervals:?}");
    intervals
}

#[test]
fn unconfirmed_handshake_pto_ignores_max_interval() {
    // Handshake and application-data probes interleave here, so compare with the default rather
    // than with a pure doubling sequence. 100 ms is below the PTO without backoff, so an applied
    // cap would pin application-data intervals to that PTO.
    assert_eq!(
        unconfirmed_handshake_pto_intervals(Some(Duration::from_millis(100))),
        unconfirmed_handshake_pto_intervals(None)
    );
}

const SCENARIO_CAP: Duration = Duration::from_secs(1);

struct RecoveryTimes {
    /// Client sends while blackholed
    blackholed: Vec<Instant>,
    restored: Instant,
    /// The client's first send after the path is restored
    first: Instant,
}

/// Fill the congestion window, lose everything in both directions for 5 s while the client's
/// address is rebound, restore the path, and check that the connection recovers
fn rebound_blackhole(max_pto_interval: Option<Duration>) -> RecoveryTimes {
    let _guard = subscribe();
    let (server, client) = configs(max_pto_interval);
    let mut pair = Pair::new(Default::default(), server);
    pair.latency = Duration::from_millis(50);
    let (client_ch, server_ch) = pair.connect_with(client);
    pair.drive();

    const LEN: usize = 256 * 1024;
    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    assert_eq!(
        pair.client_send(client_ch, s).write(&[0xab; LEN]).unwrap(),
        LEN
    );
    let restored = pair.time + Duration::from_secs(5);
    let blackholed = blackhole(&mut pair, restored);
    assert!(pair.client_conn_mut(client_ch).bytes_in_flight() > 0);
    info!(?blackholed, "client sends while blackholed");

    // The client's NAT mapping was rebuilt: the server's datagrams to the old address are lost,
    // so only a client datagram can restart the path.
    pair.client.addr = SocketAddr::new(
        Ipv4Addr::new(127, 0, 0, 1).into(),
        CLIENT_PORTS.lock().unwrap().next().unwrap(),
    );
    let first = loop {
        pair.client.drive(pair.time, pair.server.addr);
        if !pair.client.outbound.is_empty() {
            break pair.time;
        }
        pair.time = pair.time.max(pair.client.next_wakeup().unwrap());
    };
    assert!(first >= restored);

    pair.drive();
    assert!(!pair.client_conn_mut(client_ch).is_closed());
    assert_eq!(pair.client_conn_mut(client_ch).bytes_in_flight(), 0);
    assert_eq!(
        pair.server_conn_mut(server_ch).remote_address(),
        pair.client.addr
    );
    assert_eq!(pair.server_streams(server_ch).accept(Dir::Uni), Some(s));
    let mut recv = pair.server_recv(server_ch, s);
    let mut chunks = recv.read(true).unwrap();
    let mut received = 0;
    while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
        received += chunk.bytes.len();
    }
    let _ = chunks.finalize();
    assert_eq!(received, LEN);
    RecoveryTimes {
        blackholed,
        restored,
        first,
    }
}

#[test]
fn max_pto_interval_bounds_recovery_after_rebound_blackhole() {
    // Probes bypass congestion control and pacing, and the simulated clock fires the PTO timer
    // exactly, so the first datagram follows the last blackholed probe by exactly one capped
    // interval, which ends no later than one cap after restoration. No extra bound is needed.
    let capped = rebound_blackhole(Some(SCENARIO_CAP));
    let last = *capped.blackholed.last().unwrap();
    println!(
        "PTO_CEILING capped wait_ms={} interval_ms={}",
        (capped.first - capped.restored).as_millis(),
        (capped.first - last).as_millis()
    );
    assert_eq!(capped.first - last, SCENARIO_CAP);
    assert!(capped.first - capped.restored <= SCENARIO_CAP);

    // Without the option the backoff keeps doubling through the blackhole, and the next probe is
    // twice the previous interval after the last one. In this simulation (50 ms one-way latency)
    // that interval is 3.8 s and the first datagram leaves 2.362 s after restoration; with the
    // 1 s cap it leaves after 0.662 s.
    let uncapped = rebound_blackhole(None);
    let [.., before_last, last] = uncapped.blackholed[..] else {
        panic!("{:?}", uncapped.blackholed);
    };
    println!(
        "PTO_CEILING uncapped wait_ms={} interval_ms={}",
        (uncapped.first - uncapped.restored).as_millis(),
        (uncapped.first - last).as_millis()
    );
    assert_eq!(uncapped.first - last, (last - before_last) * 2);
    assert!(uncapped.first - last > SCENARIO_CAP);
}
