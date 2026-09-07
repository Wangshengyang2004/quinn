use super::*;

#[derive(Clone)]
struct FixedSupplyController {
    window: u64,
    rate: Option<u64>,
    paced: bool,
}
impl congestion::Controller for FixedSupplyController {
    fn pacing_enabled(&self) -> bool {
        self.paced
    }
    fn on_congestion_event(&mut self, _: Instant, _: Instant, _: bool, _: u64) {}
    fn on_mtu_update(&mut self, _: u16) {}
    fn window(&self) -> u64 {
        self.window
    }
    fn initial_window(&self) -> u64 {
        self.window
    }
    fn pacing_rate(&self) -> Option<u64> {
        self.rate
    }
    fn clone_box(&self) -> Box<dyn congestion::Controller> {
        Box::new(self.clone())
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}
impl congestion::ControllerFactory for FixedSupplyController {
    fn build(self: Arc<Self>, _: Instant, _: u16) -> Box<dyn congestion::Controller> {
        Box::new(self.as_ref().clone())
    }
}
fn pair_with(window: u64, rate: Option<u64>, paced: bool) -> (Pair, ConnectionHandle) {
    let mut transport = TransportConfig::default();
    transport
        .enable_segmentation_offload(false)
        .deterministic_packet_numbers(true)
        .congestion_controller_factory(Arc::new(FixedSupplyController {
            window,
            rate,
            paced,
        }));
    let mut config = client_config();
    config.transport_config(Arc::new(transport));
    let mut pair = Pair::default();
    pair.latency = Duration::from_millis(20);
    let (client, _) = pair.connect_with(config);
    (pair, client)
}

#[test]
fn idle_pacing_and_cwnd_supply_signals_follow_real_stream_demand() {
    let (mut pair, client) = pair_with(48_000, Some(48_000), true);
    let mut now = pair.time;
    let mut buf = Vec::new();
    assert!(
        pair.client_conn_mut(client)
            .poll_transmit(now, 1, &mut buf)
            .is_none()
    );
    assert!(pair.client_conn_mut(client).supply_state().0);
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    assert_eq!(
        pair.client_send(client, stream)
            .write(&vec![9; 128_000])
            .unwrap(),
        128_000
    );
    let mut paced = false;
    let mut cwnd_blocked = false;
    // Keep ACKs withheld; advance only the pacer's clock. The transport must
    // distinguish both rate and window blocking from an empty application.
    for _ in 0..2000 {
        buf.clear();
        let sent = pair.client_conn_mut(client).poll_transmit(now, 1, &mut buf);
        let (limited, pending, flight) = pair.client_conn_mut(client).supply_state();
        assert!(pending);
        if sent.is_none() {
            assert!(
                !limited,
                "queued stream bytes must not be declared application idle"
            );
            if flight + 1500 < 48_000 {
                paced = true;
            } else {
                cwnd_blocked = true;
                break;
            }
        }
        now += Duration::from_millis(1);
    }
    assert!(paced, "exercise pacing with available congestion window");
    assert!(
        cwnd_blocked,
        "exercise exhausted congestion window with pending data"
    );
}

#[test]
fn loss_retransmission_is_visible_as_actual_sendable_supply() {
    let (mut pair, client) = pair_with(u64::MAX, None, true);
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    assert_eq!(
        pair.client_send(client, stream)
            .write(&vec![7; 10_000])
            .unwrap(),
        10_000
    );
    pair.client.drive_outgoing(pair.time);
    assert!(pair.client.outbound.len() >= 5);
    pair.client.outbound.pop_front().unwrap();
    for (tx, buf) in pair.client.outbound.drain(..) {
        pair.server
            .inbound
            .push_back((pair.time, tx.ecn, buf.as_ref().into()));
    }
    pair.drive_server();
    // Process ACK/loss without also polling the retransmission out of the queue.
    let inbound: Vec<_> = pair.client.inbound.drain(..).collect();
    assert!(!inbound.is_empty(), "receiver produced ACK feedback");
    for (at, ecn, packet) in inbound {
        pair.time = pair.time.max(at);
        let mut response = Vec::new();
        let event =
            pair.client
                .endpoint
                .handle(at, pair.server.addr, None, ecn, packet, &mut response);
        if let Some(DatagramEvent::ConnectionEvent(ch, event)) = event {
            pair.client_conn_mut(ch).handle_event(event);
        }
    }
    assert!(pair.client_conn_mut(client).stats().path.lost_packets > 0);
    // STREAM retransmissions live in the stream scheduler; the connection's
    // has_pending_retransmits() inspects pending control-frame retransmits.
    assert!(pair.client_conn_mut(client).supply_state().1);
    let now = pair.time;
    let mut buf = Vec::new();
    assert!(
        pair.client_conn_mut(client)
            .poll_transmit(now, 1, &mut buf)
            .is_some()
    );
    assert!(!pair.client_conn_mut(client).supply_state().0);
}

#[test]
fn disabled_pacing_retains_actual_transport_window_and_demand() {
    let (mut pair, client) = pair_with(48_000, Some(48_000), false);
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    pair.client_send(client, stream)
        .write(&vec![9; 128_000])
        .unwrap();
    let now = pair.time;
    let mut buf = Vec::new();
    let mut sent_count = 0;
    loop {
        buf.clear();
        let sent = pair.client_conn_mut(client).poll_transmit(now, 1, &mut buf);
        let (limited, pending, flight) = pair.client_conn_mut(client).supply_state();
        assert!(pending);
        assert!(
            flight <= 48_000,
            "unpaced transmission must retain the window"
        );
        if sent.is_none() {
            assert!(!limited);
            assert!(flight + 1500 >= 48_000, "no early rate blocking");
            break;
        }
        sent_count += 1;
        assert!(sent_count < 100);
    }
    assert!(
        sent_count > 10,
        "must exceed initial pacer token burst without advancing time"
    );
}
