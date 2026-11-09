use super::*;

/// Deliver a complete encrypted server flight with its first datagram last.
/// No ACK or timeout is returned to the sender before checking the plaintext.
fn reordered_server_flight(quic_go: bool) -> (usize, usize) {
    let mut server = server_config();
    let mut transport = TransportConfig::default();
    let mut cubic = congestion::CubicConfig::default();
    cubic.initial_window(64 << 20);
    transport
        .quic_go_packet_numbers(quic_go)
        .deterministic_packet_numbers(true)
        .enable_segmentation_offload(false)
        .mtu_discovery_config(None)
        .send_window(8 << 20)
        .congestion_controller_factory(Arc::new(cubic));
    server.transport = Arc::new(transport);
    let mut client = client_config();
    Arc::get_mut(&mut client.transport)
        .unwrap()
        .hy2_receive_history(true)
        .stream_receive_window(VarInt::from_u32(8 << 20))
        .receive_window(VarInt::from_u32(8 << 20))
        .mtu_discovery_config(None);
    let mut pair = Pair::new(Default::default(), server);
    pair.latency = Duration::from_millis(75);
    let (ch, sh) = pair.connect_with(client);
    let stream = pair.server_streams(sh).open(Dir::Uni).unwrap();
    let payload: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    assert_eq!(pair.server_send(sh, stream).write(&payload).unwrap(), payload.len());
    pair.server_send(sh, stream).finish().unwrap();
    let before = pair.server_conn_mut(sh).stats().path.sent_packets;
    let started = pair.time;
    // The pacer has a bounded burst budget. Accumulate a real flight over
    // several wakes, all before the first ACK can return.
    for tick in 0..=16 {
        pair.time = started + Duration::from_millis(tick);
        pair.server.drive_outgoing(pair.time);
    }
    let held = pair.server.outbound.pop_front().unwrap();
    let gap = pair.server.outbound.len();
    assert!(gap >= 1213, "fixture did not produce the measured deep gap: {gap}");
    pair.time = started + pair.latency + Duration::from_millis(17);
    for (tx, data) in pair.server.outbound.drain(..) {
        pair.client.inbound.push_back((pair.time, tx.ecn, data.as_ref().into()));
    }
    pair.drive_client();
    pair.time += Duration::from_millis(1);
    pair.client.inbound.push_back((pair.time, held.0.ecn, held.1.as_ref().into()));
    pair.drive_client();
    assert_eq!(pair.client_streams(ch).accept(Dir::Uni), Some(stream));
    let mut body = Vec::new();
    let mut recv = pair.client_recv(ch, stream);
    let mut chunks = recv.read(true).unwrap();
    loop {
        match chunks.next(usize::MAX) {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk.bytes),
            Ok(None) | Err(ReadError::Blocked) => break,
            Err(error) => panic!("unexpected read error: {error:?}"),
        }
    }
    let _ = chunks.finalize();
    assert_eq!(body, payload[..body.len()]);
    assert_eq!(
        pair.server_conn_mut(sh).stats().path.sent_packets - before,
        gap as u64 + 1,
        "sender must not retransmit before this assertion"
    );
    (body.len(), payload.len())
}

#[test]
fn quic_go_server_authenticates_deeply_reordered_original_without_retransmission() {
    let (read, total) = reordered_server_flight(true);
    assert_eq!(read, total, "all original packets arrived, so plaintext must be complete");
}

#[test]
fn ordinary_quinn_preserves_short_number_behavior() {
    let (read, total) = reordered_server_flight(false);
    assert!(read < total, "ordinary Quinn retains its existing short encoding policy");
}
