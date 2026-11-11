//! Per-packet accounting of losses that are acknowledged afterwards
use super::reordering_calls::{Calls, reordering_pair, reordering_rounds};
use super::*;

#[test]
fn spurious_losses_are_counted_once() {
    let _guard = subscribe();
    let calls = Calls::default();
    let (mut pair, client_ch, _) = reordering_pair(&calls);
    reordering_rounds(&mut pair, client_ch, 20, true);
    let stats = pair.client_conn_mut(client_ch).stats().path;
    assert_eq!(stats.lost_packets, 20);
    assert_eq!(stats.spurious_lost_packets, 20);
    assert_eq!(stats.spurious_lost_bytes, stats.lost_bytes);
    assert_eq!(stats.spurious_congestion_events, 20);
    // Each held packet is acknowledged together with the rest of its round, which was sent at
    // the same instant.
    assert!(stats.spurious_loss_max_packet_reordering >= 3);
    assert_eq!(stats.spurious_loss_max_time_reordering, Duration::ZERO);
    assert_eq!(
        pair.client_conn_mut(client_ch)
            .lost_packet_records_for_test(),
        0
    );

    // Later ACK frames still cover the late packets; they are not counted again.
    reordering_rounds(&mut pair, client_ch, 3, false);
    let later = pair.client_conn_mut(client_ch).stats().path;
    assert_eq!(later.lost_packets, 23);
    assert_eq!(later.spurious_lost_packets, 20);
    assert_eq!(later.spurious_lost_bytes, stats.spurious_lost_bytes);
}

#[test]
fn genuine_losses_are_not_counted_and_expire() {
    let _guard = subscribe();
    let calls = Calls::default();
    let (mut pair, client_ch, _) = reordering_pair(&calls);
    reordering_rounds(&mut pair, client_ch, 5, false);
    let stats = pair.client_conn_mut(client_ch).stats().path;
    assert_eq!(stats.lost_packets, 5);
    assert_eq!(stats.spurious_lost_packets, 0);
    assert_eq!(stats.spurious_lost_bytes, 0);
    assert_eq!(stats.spurious_congestion_events, 0);
    // Records older than two PTOs are drained by the next loss detection.
    pair.time += Duration::from_secs(1);
    reordering_rounds(&mut pair, client_ch, 1, true);
    assert_eq!(
        pair.client_conn_mut(client_ch)
            .lost_packet_records_for_test(),
        0
    );
    assert_eq!(
        pair.client_conn_mut(client_ch)
            .stats()
            .path
            .spurious_lost_packets,
        1
    );
}

#[test]
fn late_ack_after_the_record_expired_is_not_counted() {
    let _guard = subscribe();
    let calls = Calls::default();
    let (mut pair, client_ch, _) = reordering_pair(&calls);
    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s)
        .write(&[0xab; 8 * 1200])
        .unwrap();
    pair.drive_client();
    let held = pair.server.inbound.pop_front().unwrap();
    pair.drive_server();
    pair.drive_client();
    assert_eq!(pair.client_conn_mut(client_ch).stats().path.lost_packets, 1);
    assert_eq!(
        pair.client_conn_mut(client_ch)
            .lost_packet_records_for_test(),
        1
    );
    pair.drive();

    // Well past two PTOs, a later loss detection drains the record.
    pair.time += Duration::from_secs(1);
    pair.client_send(client_ch, s).write(&[0xab; 1200]).unwrap();
    pair.drive();
    assert_eq!(
        pair.client_conn_mut(client_ch)
            .lost_packet_records_for_test(),
        0
    );

    pair.server.inbound.push_back(held);
    pair.drive();
    let stats = pair.client_conn_mut(client_ch).stats().path;
    assert_eq!(stats.spurious_lost_packets, 0);
    assert_eq!(stats.spurious_lost_bytes, 0);
    assert_eq!(stats.spurious_congestion_events, 0);
    assert_eq!(
        pair.client_conn_mut(client_ch)
            .lost_packet_records_for_test(),
        0
    );
}

#[test]
fn late_ack_only_packet_is_not_a_spurious_loss() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let (client_ch, server_ch) = pair.connect();
    pair.drive();

    // As in `late_ack_only_packet_is_not_a_spurious_congestion_event`
    let s = pair.server_streams(server_ch).open(Dir::Uni).unwrap();
    pair.server_send(server_ch, s).write(&[0; 4000]).unwrap();
    pair.drive_server();
    pair.drive_client();
    let held: Vec<_> = pair.server.inbound.drain(..).collect();
    assert!(!held.is_empty());

    let c = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, c).write(&[0; 8000]).unwrap();
    pair.drive();
    assert!(pair.client_conn_mut(client_ch).stats().path.lost_packets > 0);
    assert_eq!(
        pair.client_conn_mut(client_ch)
            .lost_packet_records_for_test(),
        0
    );

    pair.server.inbound.extend(held);
    pair.client_send(client_ch, c).write(&[0; 2000]).unwrap();
    pair.drive();
    let stats = pair.client_conn_mut(client_ch).stats().path;
    assert_eq!(stats.spurious_lost_packets, 0);
    assert_eq!(stats.spurious_lost_bytes, 0);
}
