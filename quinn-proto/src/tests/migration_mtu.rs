//! Migrated paths defer PMTU discovery until address validation completes.
use super::*;

#[test]
fn migration_mtu_probes_respect_anti_amplification_limit() {
    migration_mtu_validation(None);
}

#[derive(Debug, Default)]
struct LocalBudget(std::sync::atomic::AtomicU16);

impl LocalUdpPayloadLimit for LocalBudget {
    fn max_udp_payload_size(&self, _: SocketAddr) -> Option<u16> {
        let limit = self.0.load(std::sync::atomic::Ordering::Relaxed);
        (limit != 0).then_some(limit)
    }
}

#[test]
fn migration_mtu_probes_respect_dynamic_local_budget() {
    migration_mtu_validation(Some(Arc::new(LocalBudget::default())));
}

fn migration_mtu_validation(budget: Option<Arc<LocalBudget>>) {
    let _guard = subscribe();
    let mut config = server_config();
    Arc::get_mut(&mut config.transport)
        .unwrap()
        .local_udp_payload_limit(budget.clone().map(|x| x as Arc<dyn LocalUdpPayloadLimit>));
    let mut pair = Pair::new(Default::default(), config);
    let (client_ch, server_ch) = pair.connect();
    pair.drive();
    assert_eq!(
        usize::from(pair.server_conn_mut(server_ch).path_mtu()),
        DEFAULT_MTU
    );

    // The server's own counters. From here on everything it receives comes from the client's new
    // address, and everything it sends goes there too, bar the single padded PATH_CHALLENGE it
    // sends to the old address when it migrates.
    let server_stats = |pair: &mut Pair| pair.server_conn_mut(server_ch).stats();
    let before = server_stats(&mut pair);

    let path_responses = |pair: &mut Pair| {
        pair.client_conn_mut(client_ch)
            .stats()
            .frame_tx
            .path_response
    };
    let path_responses_before = path_responses(&mut pair);

    // The client shows up at a new IP address, as after a NAT rebinding that changed its public
    // address. The server starts MTU discovery from scratch for the new path.
    let old_address = pair.client.addr;
    if let Some(budget) = &budget {
        budget.0.store(1300, std::sync::atomic::Ordering::Relaxed);
    }
    pair.client.addr = SocketAddr::new(
        Ipv4Addr::new(127, 0, 0, 1).into(),
        CLIENT_PORTS.lock().unwrap().next().unwrap(),
    );
    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(&[0xab; 700]).unwrap();

    // Until the new path is validated, the server includes a PATH_CHALLENGE in every packet it
    // sends there, padded to `INITIAL_MTU`; only MTU probes are larger. Lose the datagrams of up to
    // `INITIAL_MTU` so the path stays unvalidated, and deliver the probes.
    let lose = |datagram: &[u8]| datagram.len() <= usize::from(INITIAL_MTU);

    // Delivering the client's data from its new address migrates the server onto the new path,
    // where MTU discovery starts over from `INITIAL_MTU`
    assert!(step_losing_server_datagrams(&mut pair, lose));
    assert_eq!(
        pair.server_conn_mut(server_ch).remote_address(),
        pair.client.addr
    );
    assert_eq!(pair.server_conn_mut(server_ch).path_mtu(), INITIAL_MTU);

    // Hold the path unvalidated for as long as the server keeps trying to validate it, which ends
    // when its path validation timer expires, because every PATH_CHALLENGE is lost
    while pair.server_conn_mut(server_ch).remote_address() == pair.client.addr {
        let now = server_stats(&mut pair);
        let sent = now.udp_tx.bytes - before.udp_tx.bytes;
        let received = now.udp_rx.bytes - before.udp_rx.bytes;
        info!(
            sent,
            received, "bytes exchanged with the unvalidated address"
        );
        // The anti-amplification limit is three times the bytes received on the path.
        // `poll_transmit` still starts a full-size datagram while any of that budget is left, and
        // the PATH_CHALLENGE to the old address is counted here too, so allow two more datagrams
        // of the unvalidated path's `INITIAL_MTU` on top.
        assert!(
            sent < 3 * received + 2 * u64::from(INITIAL_MTU),
            "server sent {sent} bytes to an unvalidated address it received {received} bytes from"
        );
        // MTU probes are not subject to the limit above, so they must wait for validation
        assert_eq!(
            now.path.sent_plpmtud_probes, before.path.sent_plpmtud_probes,
            "server sent an MTU probe to an unvalidated address"
        );
        assert_eq!(pair.server_conn_mut(server_ch).path_mtu(), INITIAL_MTU);
        if !step_losing_server_datagrams(&mut pair, lose) {
            break;
        }
    }
    assert_eq!(
        pair.server_conn_mut(server_ch).remote_address(),
        old_address,
        "path validation timeout must restore the old validated path"
    );
    println!(
        "QH2_PHASE timeout_fallback probes={} initial_probes={}",
        server_stats(&mut pair).path.sent_plpmtud_probes,
        before.path.sent_plpmtud_probes
    );
    assert_eq!(
        path_responses(&mut pair),
        path_responses_before,
        "no PATH_CHALLENGE should have reached the client yet"
    );

    // More data from the client lifts the limit, a PATH_CHALLENGE gets through, the path is
    // validated, and MTU discovery runs on it, back up from `INITIAL_MTU`
    pair.client_send(client_ch, s).write(&[0xab; 700]).unwrap();
    pair.drive();
    assert_eq!(
        pair.server_conn_mut(server_ch).remote_address(),
        pair.client.addr
    );
    assert!(path_responses(&mut pair) > path_responses_before);
    assert!(server_stats(&mut pair).path.sent_plpmtud_probes > before.path.sent_plpmtud_probes);
    let mtu = pair.server_conn_mut(server_ch).path_mtu();
    if let Some(budget) = &budget {
        assert!((INITIAL_MTU + 1..=1300).contains(&mtu));
        println!("QH2_PHASE validated_budget mtu={mtu}");
        budget.0.store(0, std::sync::atomic::Ordering::Relaxed);
        pair.server_conn_mut(server_ch).ping();
        pair.drive();
    }
    assert_eq!(
        usize::from(pair.server_conn_mut(server_ch).path_mtu()),
        DEFAULT_MTU
    );
    println!(
        "QH2_PHASE validated_recovery mtu={} probes={}",
        pair.server_conn_mut(server_ch).path_mtu(),
        server_stats(&mut pair).path.sent_plpmtud_probes
    );
}

/// Like [`Pair::step`], but drops the server's datagrams to the client's current address for which
/// `lose` returns true, and all of its datagrams to any other address
///
/// Returns false once neither endpoint has anything scheduled.
fn step_losing_server_datagrams(pair: &mut Pair, lose: impl Fn(&[u8]) -> bool) -> bool {
    pair.drive_client();

    pair.server.drive(pair.time, pair.client.addr);
    for (transmit, datagram) in pair.server.outbound.drain(..) {
        if transmit.destination != pair.client.addr || lose(&datagram) || datagram.len() > pair.mtu
        {
            continue;
        }
        let arrival = pair.time + pair.latency;
        let datagram = datagram.as_ref().into();
        pair.client
            .inbound
            .push_back((arrival, transmit.ecn, datagram));
    }

    match min_opt(pair.client.next_wakeup(), pair.server.next_wakeup()) {
        Some(t) => {
            pair.time = pair.time.max(t);
            true
        }
        None => false,
    }
}
