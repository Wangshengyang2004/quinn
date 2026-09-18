use super::*;

fn connected(adaptive: bool) -> (Pair, ConnectionHandle, ConnectionHandle) {
    configured(adaptive, 3)
}

fn configured(adaptive: bool, packet_threshold: u32) -> (Pair, ConnectionHandle, ConnectionHandle) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut transport = TransportConfig::default();
    transport
        .adaptive_loss_thresholds(adaptive)
        .packet_threshold(packet_threshold)
        .mtu_discovery_config(None)
        .enable_segmentation_offload(false)
        .deterministic_packet_numbers(true)
        .congestion_controller_factory(Arc::new(CongestionEventRecorderFactory { events }));
    let mut client = client_config();
    client.transport_config(Arc::new(transport));
    let mut server = server_config();
    let mut server_transport = TransportConfig::default();
    server_transport.mtu_discovery_config(None);
    server.transport = Arc::new(server_transport);
    let mut pair = Pair::new(Default::default(), server);
    pair.latency = Duration::from_millis(75);
    let (ch, sh) = pair.connect_with(client);
    (pair, ch, sh)
}

fn flight(
    pair: &mut Pair,
    ch: ConnectionHandle,
    sh: ConnectionHandle,
    drop_original: bool,
) -> (u64, u64, Duration) {
    delayed_flight(pair, ch, sh, drop_original, Duration::from_millis(1))
}

#[test]
fn learns_from_late_original_ack_and_preserves_real_loss_recovery() {
    let (mut pair, ch, sh) = connected(true);
    assert_eq!(
        flight(&mut pair, ch, sh, false).0,
        1,
        "initial threshold remains three"
    );
    let repeated = flight(&mut pair, ch, sh, false);
    assert_eq!(
        (repeated.0, repeated.1),
        (0, 0),
        "ACKed original must train the next flight"
    );
    let real_loss = flight(&mut pair, ch, sh, true);
    assert_eq!(
        real_loss.0, 0,
        "learned packet threshold should defer this loss"
    );
    assert!(
        real_loss.2 <= Duration::from_millis(175),
        "time threshold still recovers true loss"
    );
}

#[test]
fn disabled_adaptation_preserves_packet_threshold() {
    let (mut pair, ch, sh) = connected(false);
    assert_eq!(flight(&mut pair, ch, sh, false).0, 1);
    assert_eq!(flight(&mut pair, ch, sh, false).0, 1);
}

fn delayed_flight(
    pair: &mut Pair,
    ch: ConnectionHandle,
    sh: ConnectionHandle,
    drop_original: bool,
    delay: Duration,
) -> (u64, u64, Duration) {
    let stream = pair.client_streams(ch).open(Dir::Uni).unwrap();
    let payload: Vec<u8> = (0..64000).map(|x| (x % 251) as u8).collect();
    assert_eq!(
        pair.client_send(ch, stream).write(&payload).unwrap(),
        payload.len()
    );
    pair.client_send(ch, stream).finish().unwrap();
    let before = pair.client_conn_mut(ch).stats().path.lost_packets;
    let started = pair.time;
    pair.client.drive_outgoing(pair.time);
    let held = pair.client.outbound.pop_front().unwrap();
    assert!(pair.client.outbound.len() > 3 && pair.client.outbound.len() < 128);
    for (tx, data) in pair.client.outbound.drain(..) {
        pair.server
            .inbound
            .push_back((started + pair.latency, tx.ecn, data.as_ref().into()));
    }
    pair.time = started + pair.latency;
    pair.drive_server();
    if !drop_original {
        pair.time += delay;
        pair.server
            .inbound
            .push_back((pair.time, held.0.ecn, held.1.as_ref().into()));
        pair.drive_server();
    }
    pair.time = started + pair.latency * 2;
    pair.drive_client();
    let first = pair.client_conn_mut(ch).stats().path.lost_packets - before;
    pair.time += Duration::from_millis(1);
    pair.drive_client();

    let mut detected_at = if first > 0 {
        pair.latency * 2
    } else {
        pair.time - started
    };
    if drop_original {
        while pair.client_conn_mut(ch).stats().path.lost_packets == before {
            pair.time += Duration::from_micros(250);
            assert!(pair.time - started < Duration::from_millis(240));
            pair.drive_client();
        }
        detected_at = pair.time - started;
    }
    pair.drive();
    assert_eq!(pair.server_streams(sh).accept(Dir::Uni), Some(stream));
    let mut received = vec![0; payload.len()];
    let mut length = 0;
    let mut recv = pair.server_recv(sh, stream);
    let mut chunks = recv.read(false).unwrap();
    while let Some(chunk) = chunks.next(usize::MAX).unwrap() {
        let start = chunk.offset as usize;
        received[start..start + chunk.bytes.len()].copy_from_slice(&chunk.bytes);
        length += chunk.bytes.len();
    }
    let _ = chunks.finalize();
    assert_eq!(length, payload.len());
    assert_eq!(received, payload);
    let total = pair.client_conn_mut(ch).stats().path.lost_packets - before;
    (first, total, detected_at)
}

#[test]
fn learns_time_reordering_and_bounds_genuine_loss_delay() {
    let (mut pair, ch, sh) = configured(true, 3);
    assert_eq!(flight(&mut pair, ch, sh, false).0, 1);
    let delay = Duration::from_millis(30);
    let first = delayed_flight(&mut pair, ch, sh, false, delay);
    assert_eq!(first.0, 0, "packet reordering already learned");
    assert_eq!(first.1, 1, "first time-reordered flight supplies evidence");
    let repeated = delayed_flight(&mut pair, ch, sh, false, delay);
    assert_eq!(
        repeated.1, 0,
        "late original ACK must prevent repeated time-threshold loss"
    );
    let lost = delayed_flight(&mut pair, ch, sh, true, delay);
    assert_eq!(lost.0, 0);
    assert!(
        lost.2 <= Duration::from_millis(225),
        "genuine loss must recover within 1.5 times the 150 ms RTT"
    );
}

#[test]
fn learned_time_allowance_is_capped_and_decays_after_quiet_rtts() {
    // Isolate time recovery from the packet threshold at the public config seam.
    let (mut pair, ch, sh) = configured(true, 128);
    let delay = Duration::from_millis(74);
    assert_eq!(delayed_flight(&mut pair, ch, sh, false, delay).1, 1);
    let prior_rtt = pair.client_conn_mut(ch).stats().path.rtt;
    let bounded = delayed_flight(&mut pair, ch, sh, true, delay);
    assert!(
        bounded.2 >= Duration::from_millis(180),
        "late ACK should increase the allowance"
    );
    assert!(
        bounded.2 <= Duration::from_millis(190) + Duration::from_micros(250),
        "learned allowance must be capped: delay={:?}, smoothed RTT before flight={:?}",
        bounded.2,
        prior_rtt
    );
    pair.time += Duration::from_secs(11);
    let decayed = delayed_flight(&mut pair, ch, sh, true, delay);
    assert!(
        decayed.2 <= Duration::from_millis(175),
        "quiet periods restore the configured recovery threshold"
    );
}

#[test]
fn changing_path_resets_time_reordering_evidence() {
    let (mut pair, ch, sh) = configured(true, 128);
    let delay = Duration::from_millis(30);
    assert_eq!(delayed_flight(&mut pair, ch, sh, false, delay).1, 1);
    assert_eq!(delayed_flight(&mut pair, ch, sh, false, delay).1, 0);
    let now = pair.time;
    pair.client_conn_mut(ch).path_changed(now);
    assert_eq!(
        delayed_flight(&mut pair, ch, sh, false, delay).1,
        1,
        "the new path must learn independently of its predecessor"
    );
}
