//! Logic for controlling the rate at which data is sent

use crate::Instant;
use crate::connection::RttEstimator;
use std::any::Any;
use std::sync::Arc;

mod bbr;
mod cubic;
mod new_reno;

pub use bbr::{Bbr, BbrConfig, BbrProfile};
pub use cubic::{Cubic, CubicConfig};
pub use new_reno::{NewReno, NewRenoConfig};

/// State of a congestion-controlled packet at the instant it was sent.
#[derive(Debug, Clone, Copy)]
pub struct SentPacketInfo {
    /// Sending timestamp.
    pub now: Instant,
    /// Connection-wide monotonic packet identifier used by congestion control.
    pub packet_number: u64,
    /// Congestion-controlled bytes in the packet.
    pub bytes: u64,
    /// Congestion-controlled bytes in flight before this packet was sent.
    pub bytes_in_flight: u64,
    /// Whether the sender was application-limited when this packet was sent.
    pub app_limited: bool,
}

/// A packet acknowledged in a congestion event.
#[derive(Debug, Clone, Copy)]
pub struct AckedPacket {
    /// Connection-wide monotonic packet identifier used by congestion control.
    pub packet_number: u64,
    /// Sending timestamp.
    pub sent: Instant,
    /// Congestion-controlled bytes in the packet.
    pub bytes: u64,
}

/// A packet declared lost in a congestion event.
#[derive(Debug, Clone, Copy)]
pub struct LostPacket {
    /// Connection-wide monotonic packet identifier used by congestion control.
    pub packet_number: u64,
    /// Sending timestamp.
    pub sent: Instant,
    /// Congestion-controlled bytes in the packet.
    pub bytes: u64,
}

/// ACK, loss and ECN feedback produced while processing one incoming ACK frame.
pub struct CongestionEvent<'a> {
    /// Feedback processing timestamp.
    pub now: Instant,
    /// Bytes in flight before applying this event.
    pub prior_in_flight: u64,
    /// Bytes in flight after applying this event.
    pub in_flight: u64,
    /// Current connection-level application-limited state.
    pub app_limited: bool,
    /// Current RTT estimator.
    pub rtt: &'a RttEstimator,
    /// Packets newly acknowledged by this event, in packet-number order.
    pub acked_packets: &'a [AckedPacket],
    /// Packets declared lost by this event, in packet-number order.
    pub lost_packets: &'a [LostPacket],
    /// Whether this event contains validated ECN congestion feedback.
    pub ecn: bool,
    /// Sending time associated with ECN feedback.
    pub ecn_sent: Option<Instant>,
    /// Whether the losses constitute persistent congestion.
    pub is_persistent_congestion: bool,
}

/// Common interface for different congestion controllers
pub trait Controller: Send + Sync {
    /// A congestion-controlled packet was just sent.
    ///
    /// Controllers that need per-packet send state should override this method.
    fn on_packet_sent(&mut self, packet: SentPacketInfo) {
        self.on_sent(packet.now, packet.bytes, packet.packet_number);
    }

    /// One or more packets were just sent
    #[allow(unused_variables)]
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {}

    /// A previously sent packet was discarded without being acknowledged or
    /// declared lost, for example when a packet number space is discarded.
    #[allow(unused_variables)]
    fn on_packet_neutered(&mut self, packet_number: u64) {}

    /// Packet deliveries were confirmed
    ///
    /// `app_limited` indicates whether the connection was blocked on outgoing
    /// application data prior to receiving these acknowledgements.
    #[allow(unused_variables)]
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
    }

    /// Packets are acked in batches, all with the same `now` argument. This indicates one of those batches has completed.
    #[allow(unused_variables)]
    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
    }

    /// Process all ACK, loss and ECN feedback produced by one connection event.
    ///
    /// The default adapter preserves the legacy callback behavior. Controllers
    /// that model ACK and loss jointly should override this method.
    fn on_ack_and_loss(&mut self, event: CongestionEvent<'_>) {
        for packet in event.acked_packets {
            self.on_ack(
                event.now,
                packet.sent,
                packet.bytes,
                event.app_limited,
                event.rtt,
            );
        }
        self.on_end_acks(
            event.now,
            event.in_flight,
            event.app_limited,
            event
                .acked_packets
                .last()
                .map(|packet| packet.packet_number),
        );

        if event.lost_packets.is_empty() && !event.ecn {
            return;
        }

        let congestion_sent = event
            .lost_packets
            .last()
            .map(|packet| packet.sent)
            .into_iter()
            .chain(event.ecn_sent)
            .max();
        if let Some(sent) = congestion_sent {
            self.on_congestion_event(
                event.now,
                sent,
                event.is_persistent_congestion,
                event
                    .lost_packets
                    .iter()
                    .fold(0u64, |total, packet| total.saturating_add(packet.bytes)),
            );
        }
    }

    /// Packets were deemed lost or marked congested
    ///
    /// `in_persistent_congestion` indicates whether all packets sent within the persistent
    /// congestion threshold period ending when the most recent packet in this batch was sent were
    /// lost.
    /// `lost_bytes` indicates how many bytes were lost. This value will be 0 for ECN triggers.
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    );

    /// The known MTU for the current network path has been updated
    fn on_mtu_update(&mut self, new_mtu: u16);

    /// Number of ack-eliciting bytes that may be in flight
    fn window(&self) -> u64;

    /// Explicit pacing rate in bytes per second.
    ///
    /// When absent, the connection derives its pacing rate from the congestion window and RTT.
    fn pacing_rate(&self) -> Option<u64> {
        None
    }

    /// Duplicate the controller's state
    fn clone_box(&self) -> Box<dyn Controller>;

    /// Initial congestion window
    fn initial_window(&self) -> u64;

    /// Returns Self for use in down-casting to extract implementation details
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// Constructs controllers on demand
pub trait ControllerFactory {
    /// Construct a fresh `Controller`
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller>;
}

const BASE_DATAGRAM_SIZE: u64 = 1200;
