//! A failed migration must not strand the old path's per-packet congestion state.
use super::*;

fn bbr_retained(conn: &Connection) -> usize {
    conn.congestion_state()
        .clone_box()
        .into_any()
        .downcast::<crate::congestion::Bbr>()
        .unwrap()
        .retained_packet_count()
}

/// Like [`Pair::step`], but the server's datagrams never arrive, so a new path stays unvalidated.
fn step_dropping_server_datagrams(pair: &mut Pair) -> bool {
    pair.drive_client();
    pair.server.drive(pair.time, pair.client.addr);
    pair.server.outbound.clear();
    match min_opt(pair.client.next_wakeup(), pair.server.next_wakeup()) {
        Some(t) => {
            pair.time = pair.time.max(t);
            true
        }
        None => false,
    }
}

/// Packets sent on the old path and acknowledged while the server validates a new path must be
/// released from the old path's BBR sampler, which is restored when that validation times out.
#[test]
fn failed_migration_releases_old_path_sampler_records() {
    let _guard = subscribe();
    let mut transport = TransportConfig::default();
    transport.congestion_controller_factory(Arc::new(crate::congestion::BbrConfig::default()));
    let mut config = server_config();
    config.transport = Arc::new(transport);
    let mut pair = Pair::new(Default::default(), config);
    let (client, server) = pair.connect();
    pair.drive();
    assert_eq!(bbr_retained(pair.server_conn_mut(server)), 0, "baseline");

    let old_address = pair.client.addr;
    let stream = pair.server_streams(server).open(Dir::Uni).unwrap();
    let mut retained_after_cycle = Vec::new();
    for _ in 0..3 {
        // A server packet on the old path, held back so nothing acknowledges it yet
        pair.server_send(server, stream).write(&[42; 1000]).unwrap();
        pair.drive_server();
        let held: Vec<_> = pair.client.inbound.drain(..).collect();
        assert!(!held.is_empty());
        assert!(bbr_retained(pair.server_conn_mut(server)) > 0);

        // The client reappears on another port and the server starts validating that path
        pair.client
            .addr
            .set_port(CLIENT_PORTS.lock().unwrap().next().unwrap());
        pair.client_conn_mut(client).ping();
        pair.drive_client();
        pair.server.drive(pair.time, pair.client.addr);
        pair.server.outbound.clear();
        assert_eq!(
            pair.server_conn_mut(server).remote_address(),
            pair.client.addr,
            "server must be validating the new path"
        );

        // The held packet is acknowledged from the new address, then validation times out
        pair.client.inbound.extend(held);
        while pair.server_conn_mut(server).remote_address() != old_address {
            assert!(
                step_dropping_server_datagrams(&mut pair),
                "validation timeout never restored the old path"
            );
        }

        pair.client.addr = old_address;
        pair.drive();
        retained_after_cycle.push(bbr_retained(pair.server_conn_mut(server)));
    }
    assert_eq!(
        retained_after_cycle, [0; 3],
        "old-path sampler records leaked across failed migrations"
    );
}
