use super::*;

#[test]
fn hy2_history_delivers_deeply_reordered_stream_without_retransmission() {
    original_flight(false);
}

#[test]
fn hy2_history_sparse_arrivals_deliver_complete_stream_without_retransmission() {
    original_flight(true);
}

fn original_flight(sparse: bool) {
    let mut server = server_config();
    let mut receive = TransportConfig::default();
    receive.hy2_receive_history(true);
    server.transport_config(Arc::new(receive));
    let mut pair = Pair::new(Default::default(), server);
    pair.latency = Duration::from_millis(75);

    let mut congestion = crate::congestion::CubicConfig::default();
    congestion.initial_window(2 << 20);
    let mut transport = TransportConfig::default();
    transport
        .mtu_discovery_config(None)
        .enable_segmentation_offload(false)
        .deterministic_packet_numbers(true)
        .congestion_controller_factory(Arc::new(congestion));
    let mut client = client_config();
    client.transport_config(Arc::new(transport));
    let (ch, sh) = pair.connect_with(client);
    let stream = pair.client_streams(ch).open(Dir::Uni).unwrap();
    let payload: Vec<u8> = (0..768_000).map(|x| (x % 251) as u8).collect();
    assert_eq!(
        pair.client_send(ch, stream).write(&payload).unwrap(),
        payload.len()
    );
    pair.client_send(ch, stream).finish().unwrap();

    // Gather one original flight without delivering ACKs or triggering a PTO.
    // Advancing time lets the normal pacer release the whole flight.
    for _ in 0..100 {
        pair.client.drive_outgoing(pair.time);
        pair.time += Duration::from_millis(1);
    }
    assert!(pair.client.outbound.len() > 512);
    // This packet uses a two-byte packet number, so decoding remains unambiguous.
    let held = pair.client.outbound.remove(256).unwrap();
    let mut delayed = Vec::new();
    for (index, (tx, data)) in pair.client.outbound.drain(..).enumerate() {
        if sparse && index >= 256 && index % 2 == 1 {
            delayed.push((tx, data));
            continue;
        }
        pair.server
            .inbound
            .push_back((pair.time, tx.ecn, data.as_ref().into()));
    }
    if sparse {
        assert!(
            delayed.len() > 64,
            "exercise more than the ACK range budget"
        );
    }
    pair.drive_server();
    let before = pair.server_conn_mut(sh).stats().frame_rx.stream;
    pair.time += Duration::from_millis(1);
    pair.server
        .inbound
        .push_back((pair.time, held.0.ecn, held.1.as_ref().into()));
    pair.drive_server();
    let after = pair.server_conn_mut(sh).stats().frame_rx.stream;
    assert!(
        after > before,
        "the first authenticated late packet must be processed"
    );
    pair.server
        .inbound
        .push_back((pair.time, held.0.ecn, held.1.as_ref().into()));
    pair.drive_server();
    assert_eq!(pair.server_conn_mut(sh).stats().frame_rx.stream, after);

    for (tx, data) in delayed {
        pair.server
            .inbound
            .push_back((pair.time, tx.ecn, data.as_ref().into()));
    }
    pair.drive_server();

    assert_eq!(pair.server_streams(sh).accept(Dir::Uni), Some(stream));
    let mut received = vec![0; payload.len()];
    let mut length = 0;
    let mut recv = pair.server_recv(sh, stream);
    let mut chunks = recv.read(false).unwrap();
    while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
        let start = chunk.offset as usize;
        received[start..start + chunk.bytes.len()].copy_from_slice(&chunk.bytes);
        length += chunk.bytes.len();
    }
    let _ = chunks.finalize();
    assert_eq!(
        length,
        payload.len(),
        "no sender retransmission was delivered"
    );
    assert_eq!(received, payload);
}
