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

fn full_bandwidth(connection: &Connection) -> bool {
    connection
        .congestion_state()
        .clone_box()
        .into_any()
        .downcast::<crate::congestion::Bbr>()
        .unwrap()
        .reached_full_bandwidth()
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
            assert_eq!(pair.client_send(client, stream).write(&[7; 368]).unwrap(), 368);
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
