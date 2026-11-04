use super::*;

fn acknowledge_while_congested(queued_data: bool, bbr: bool, pad: bool, gso: bool) {
    let mut pair = Pair::default();
    let mut config = client_config();
    if bbr {
        Arc::get_mut(&mut config.transport)
            .unwrap()
            .congestion_controller_factory(Arc::new(crate::congestion::BbrConfig::default()));
    }
    Arc::get_mut(&mut config.transport)
        .unwrap()
        .pad_to_mtu(pad)
        .enable_segmentation_offload(gso);
    let (client, server) = pair.connect_with(config);
    let upstream = pair.client_streams(client).open(Dir::Uni).unwrap();
    // Keep real client packets in flight without delivering them to the server.
    // Each write drains completely so the control case has no pending STREAM.
    for _ in 0..512 {
        if pair.client_conn_mut(client).congestion_window() < DEFAULT_MTU as u64 {
            break;
        }
        let previous = pair.client_conn_mut(client).stats().frame_tx.stream;
        assert_eq!(
            pair.client_send(client, upstream)
                .write(&[7; 1024])
                .unwrap(),
            1024
        );
        pair.time += Duration::from_micros(100);
        pair.client.drive_outgoing(pair.time);
        pair.client.outbound.clear();
        assert_eq!(
            pair.client_conn_mut(client).stats().frame_tx.stream,
            previous + 1,
            "fixture write must drain before the window fills"
        );
    }
    assert!(pair.client_conn_mut(client).congestion_window() < DEFAULT_MTU as u64);
    if queued_data {
        assert_eq!(
            pair.client_send(client, upstream)
                .write(&[9; 1024])
                .unwrap(),
            1024
        );
    }
    let downstream = pair.server_streams(server).open(Dir::Uni).unwrap();
    let received_before = pair.client_conn_mut(client).stats().frame_rx.stream;
    let acknowledgments_before = pair.client_conn_mut(client).stats().frame_tx.acks;
    let bytes_before = pair.client_conn_mut(client).bytes_in_flight();
    let streams_before = pair.client_conn_mut(client).stats().frame_tx.stream;
    // Two distinct ack-eliciting packets make an immediate ACK due, without
    // advancing a PTO or synthesizing an ACK state inside the connection.
    for byte in [1, 2] {
        assert_eq!(
            pair.server_send(server, downstream).write(&[byte]).unwrap(),
            1
        );
        pair.drive_server();
    }
    pair.drive_client();
    let connection = pair.client_conn_mut(client);
    assert_eq!(connection.stats().frame_rx.stream, received_before + 2);
    assert!(
        connection.stats().frame_tx.acks > acknowledgments_before,
        "a due ACK must leave even with queued data and a full congestion window"
    );
    assert_eq!(connection.stats().frame_tx.stream, streams_before);
    assert_eq!(
        connection.bytes_in_flight(),
        bytes_before,
        "ACK-only output must not consume congestion credit"
    );
}

#[test]
fn due_ack_with_queued_data_and_full_cwnd_cubic() {
    for pad in [false, true] {
        for gso in [false, true] {
            acknowledge_while_congested(true, false, pad, gso);
        }
    }
}
#[test]
fn due_ack_without_queued_data_and_full_cwnd_cubic() {
    acknowledge_while_congested(false, false, false, true);
}
#[test]
fn due_ack_with_queued_data_and_full_cwnd_bbr() {
    for pad in [false, true] {
        for gso in [false, true] {
            acknowledge_while_congested(true, true, pad, gso);
        }
    }
}
#[test]
fn due_ack_without_queued_data_and_full_cwnd_bbr() {
    acknowledge_while_congested(false, true, false, true);
}

fn acknowledge_while_paced(bbr: bool, pad: bool, gso: bool) {
    let mut pair = Pair::default();
    pair.latency = Duration::from_millis(5);
    let mut config = client_config();
    let transport = Arc::get_mut(&mut config.transport).unwrap();
    transport.pad_to_mtu(pad).enable_segmentation_offload(gso);
    if bbr {
        transport.congestion_controller_factory(Arc::new(crate::congestion::BbrConfig::default()));
    } else {
        let mut controller = crate::congestion::CubicConfig::default();
        controller.initial_window(256 * 1024);
        transport.congestion_controller_factory(Arc::new(controller));
    }
    let (client, server) = pair.connect_with(config);
    // Preserve the measured RTT but deliver the observation packets immediately.
    pair.latency = Duration::ZERO;
    let upstream = pair.client_streams(client).open(Dir::Uni).unwrap();
    assert_eq!(
        pair.client_send(client, upstream)
            .write(&[7; 128 * 1024])
            .unwrap(),
        128 * 1024
    );
    pair.client.drive_outgoing(pair.time);
    assert!(!pair.client.outbound.is_empty());
    pair.client.outbound.clear();
    assert!(pair.client_conn_mut(client).congestion_window() > DEFAULT_MTU as u64);
    pair.client.drive_outgoing(pair.time);
    assert!(
        pair.client.outbound.is_empty(),
        "the pacer must stop pending data with credit still available"
    );
    let before = pair.client_conn_mut(client).stats();
    let bytes_before = pair.client_conn_mut(client).bytes_in_flight();
    let downstream = pair.server_streams(server).open(Dir::Uni).unwrap();
    for byte in [1, 2] {
        assert_eq!(
            pair.server_send(server, downstream).write(&[byte]).unwrap(),
            1
        );
        pair.drive_server();
    }
    pair.drive_client();
    let connection = pair.client_conn_mut(client);
    assert_eq!(
        connection.stats().frame_rx.stream,
        before.frame_rx.stream + 2
    );
    assert!(
        connection.stats().frame_tx.acks > before.frame_tx.acks,
        "the pacer must allow a due ACK"
    );
    assert_eq!(connection.stats().frame_tx.stream, before.frame_tx.stream);
    assert_eq!(connection.bytes_in_flight(), bytes_before);
}
#[test]
fn due_ack_with_paced_data_cubic() {
    for pad in [false, true] {
        for gso in [false, true] {
            acknowledge_while_paced(false, pad, gso);
        }
    }
}
#[test]
fn due_ack_with_paced_data_bbr() {
    for pad in [false, true] {
        for gso in [false, true] {
            acknowledge_while_paced(true, pad, gso);
        }
    }
}
