use super::*;

#[test]
fn authenticated_stream_survives_deep_reordering_and_rejects_duplicates() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut transport = TransportConfig::default();
    transport
        .mtu_discovery_config(None)
        .enable_segmentation_offload(false)
        .deterministic_packet_numbers(true)
        .congestion_controller_factory(Arc::new(CongestionEventRecorderFactory { events }));
    let mut client = client_config();
    client.transport_config(Arc::new(transport));
    let mut pair = Pair::new(Default::default(), server_config());
    pair.latency = Duration::from_millis(75);
    let (ch, sh) = pair.connect_with(client);
    let stream = pair.client_streams(ch).open(Dir::Uni).unwrap();
    let payload: Vec<u8> = (0..768000).map(|x| (x % 251) as u8).collect();
    assert_eq!(
        pair.client_send(ch, stream).write(&payload).unwrap(),
        payload.len()
    );
    pair.client_send(ch, stream).finish().unwrap();
    pair.client.drive_outgoing(pair.time);
    // A packet late in the flight carries a two-byte packet number, so the
    // delayed original remains decodable after the following burst arrives.
    let held = pair.client.outbound.remove(256).unwrap();
    assert!(pair.client.outbound.len() > 512);
    for (tx, data) in pair.client.outbound.drain(..) {
        pair.server
            .inbound
            .push_back((pair.time + pair.latency, tx.ecn, data.as_ref().into()));
    }
    pair.time += pair.latency;
    pair.drive_server();
    pair.time += Duration::from_millis(1);
    pair.server
        .inbound
        .push_back((pair.time, held.0.ecn, held.1.as_ref().into()));
    pair.drive_server();
    let before = pair.server_conn_mut(sh).stats().frame_rx.stream;
    pair.server
        .inbound
        .push_back((pair.time, held.0.ecn, held.1.as_ref().into()));
    pair.drive_server();
    assert_eq!(
        pair.server_conn_mut(sh).stats().frame_rx.stream,
        before,
        "the duplicate must not be processed twice"
    );
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
        "all original stream bytes arrived; no retransmission should be necessary"
    );
    assert_eq!(received, payload);
}
