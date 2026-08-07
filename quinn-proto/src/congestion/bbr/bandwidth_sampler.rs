use std::collections::BTreeMap;

use crate::congestion::{AckedPacket, LostPacket};
use crate::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub(crate) struct SendTimeState {
    pub(crate) packet_number: u64,
    pub(crate) is_app_limited: bool,
    pub(crate) total_bytes_sent: u64,
    pub(crate) total_bytes_acked: u64,
    pub(crate) bytes_in_flight: u64,
}

#[derive(Clone, Copy, Debug)]
struct PacketState {
    sent_at: Instant,
    size: u64,
    total_bytes_sent_at_last_acked_packet: u64,
    last_acked_packet_sent_at: Option<Instant>,
    last_acked_packet_acked_at: Option<Instant>,
    send_state: SendTimeState,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CongestionEventSample {
    pub(crate) max_bandwidth: u64,
    pub(crate) sample_rtt: Option<Duration>,
    pub(crate) sample_is_app_limited: bool,
    pub(crate) last_packet_send_state: Option<SendTimeState>,
    pub(crate) bytes_acked: u64,
    pub(crate) bytes_lost: u64,
    pub(crate) total_bytes_acked: u64,
    pub(crate) total_bytes_lost: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BandwidthSampler {
    total_bytes_sent: u64,
    total_bytes_acked: u64,
    total_bytes_lost: u64,
    total_bytes_neutered: u64,
    total_bytes_sent_at_last_acked_packet: u64,
    last_acked_packet_sent_at: Option<Instant>,
    last_acked_packet_acked_at: Option<Instant>,
    last_sent_packet: Option<u64>,
    is_app_limited: bool,
    end_of_app_limited_phase: Option<u64>,
    sent_packets: BTreeMap<u64, PacketState>,
}

impl BandwidthSampler {
    pub(crate) fn on_packet_sent(
        &mut self,
        now: Instant,
        packet_number: u64,
        bytes: u64,
        bytes_in_flight: u64,
        app_limited: bool,
    ) {
        self.last_sent_packet = Some(packet_number);
        if app_limited {
            self.on_app_limited();
        }
        self.total_bytes_sent = self.total_bytes_sent.saturating_add(bytes);

        if bytes_in_flight == 0 {
            self.last_acked_packet_sent_at = Some(now);
            self.last_acked_packet_acked_at = Some(now);
            self.total_bytes_sent_at_last_acked_packet = self.total_bytes_sent;
        }

        let packet = PacketState {
            sent_at: now,
            size: bytes,
            total_bytes_sent_at_last_acked_packet: self.total_bytes_sent_at_last_acked_packet,
            last_acked_packet_sent_at: self.last_acked_packet_sent_at,
            last_acked_packet_acked_at: self.last_acked_packet_acked_at,
            send_state: SendTimeState {
                packet_number,
                is_app_limited: self.is_app_limited,
                total_bytes_sent: self.total_bytes_sent,
                total_bytes_acked: self.total_bytes_acked,
                bytes_in_flight: bytes_in_flight.saturating_add(bytes),
            },
        };
        let old = self.sent_packets.insert(packet_number, packet);
        debug_assert!(old.is_none(), "congestion packet numbers must be unique");
    }

    pub(crate) fn on_packet_neutered(&mut self, packet_number: u64) {
        if let Some(packet) = self.sent_packets.remove(&packet_number) {
            self.total_bytes_neutered = self.total_bytes_neutered.saturating_add(packet.size);
        }
    }

    pub(crate) fn on_app_limited(&mut self) {
        self.is_app_limited = true;
        self.end_of_app_limited_phase = self.last_sent_packet;
    }

    pub(crate) fn on_congestion_event(
        &mut self,
        now: Instant,
        acked_packets: &[AckedPacket],
        lost_packets: &[LostPacket],
    ) -> CongestionEventSample {
        let acked_before = self.total_bytes_acked;
        let lost_before = self.total_bytes_lost;
        let mut event_sample = CongestionEventSample::default();
        let mut last_lost_state = None;

        for packet in lost_packets {
            self.total_bytes_lost = self.total_bytes_lost.saturating_add(packet.bytes);
            if let Some(state) = self.sent_packets.remove(&packet.packet_number) {
                debug_assert_eq!(state.size, packet.bytes);
                debug_assert_eq!(state.sent_at, packet.sent);
                last_lost_state = newest_send_state(last_lost_state, Some(state.send_state));
            }
        }

        let mut last_acked_state = None;
        for packet in acked_packets {
            let Some(state) = self.sent_packets.remove(&packet.packet_number) else {
                continue;
            };
            debug_assert_eq!(state.size, packet.bytes);
            debug_assert_eq!(state.sent_at, packet.sent);

            self.total_bytes_acked = self.total_bytes_acked.saturating_add(state.size);
            self.total_bytes_sent_at_last_acked_packet = state.send_state.total_bytes_sent;
            self.last_acked_packet_sent_at = Some(state.sent_at);
            self.last_acked_packet_acked_at = Some(now);
            last_acked_state = newest_send_state(last_acked_state, Some(state.send_state));

            if self.is_app_limited
                && self
                    .end_of_app_limited_phase
                    .is_none_or(|end| packet.packet_number > end)
            {
                self.is_app_limited = false;
            }

            let rtt = now.saturating_duration_since(state.sent_at);
            if !rtt.is_zero() {
                event_sample.sample_rtt = Some(
                    event_sample
                        .sample_rtt
                        .map_or(rtt, |current| current.min(rtt)),
                );
            }

            let Some(acked_at_baseline) = state.last_acked_packet_acked_at else {
                continue;
            };
            let ack_delta = now.saturating_duration_since(acked_at_baseline);
            if ack_delta.is_zero() {
                continue;
            }

            let send_rate = match state.last_acked_packet_sent_at {
                Some(sent_at_baseline) if state.sent_at > sent_at_baseline => bandwidth_from_delta(
                    state
                        .send_state
                        .total_bytes_sent
                        .saturating_sub(state.total_bytes_sent_at_last_acked_packet),
                    state.sent_at - sent_at_baseline,
                ),
                _ => u64::MAX,
            };
            let ack_rate = bandwidth_from_delta(
                self.total_bytes_acked
                    .saturating_sub(state.send_state.total_bytes_acked),
                ack_delta,
            );
            let bandwidth = send_rate.min(ack_rate);
            if bandwidth > event_sample.max_bandwidth {
                event_sample.max_bandwidth = bandwidth;
                event_sample.sample_is_app_limited = state.send_state.is_app_limited;
            }
        }

        event_sample.last_packet_send_state = newest_send_state(last_acked_state, last_lost_state);
        event_sample.bytes_acked = self.total_bytes_acked.saturating_sub(acked_before);
        event_sample.bytes_lost = self.total_bytes_lost.saturating_sub(lost_before);
        event_sample.total_bytes_acked = self.total_bytes_acked;
        event_sample.total_bytes_lost = self.total_bytes_lost;
        event_sample
    }
}

fn newest_send_state(
    left: Option<SendTimeState>,
    right: Option<SendTimeState>,
) -> Option<SendTimeState> {
    match (left, right) {
        (Some(left), Some(right)) if left.packet_number >= right.packet_number => Some(left),
        (_, Some(right)) => Some(right),
        (Some(left), None) => Some(left),
        (None, None) => None,
    }
}

pub(crate) fn bandwidth_from_delta(bytes: u64, delta: Duration) -> u64 {
    if delta.is_zero() {
        return 0;
    }
    let nanos = delta.as_nanos();
    u64::try_from(u128::from(bytes).saturating_mul(1_000_000_000) / nanos).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{AckedPacket, BandwidthSampler, LostPacket};
    use crate::{Duration, Instant};

    #[test]
    fn bandwidth_sample_uses_each_packets_send_state_and_the_lower_rate() {
        let start = Instant::now();
        let mut sampler = BandwidthSampler::default();

        sampler.on_packet_sent(start, 0, 1_000, 0, false);
        sampler.on_congestion_event(
            start + Duration::from_millis(100),
            &[AckedPacket {
                packet_number: 0,
                sent: start,
                bytes: 1_000,
            }],
            &[],
        );

        sampler.on_packet_sent(start + Duration::from_millis(110), 1, 1_000, 0, false);
        sampler.on_packet_sent(start + Duration::from_millis(210), 2, 1_000, 1_000, false);
        let sample = sampler.on_congestion_event(
            start + Duration::from_millis(220),
            &[
                AckedPacket {
                    packet_number: 1,
                    sent: start + Duration::from_millis(110),
                    bytes: 1_000,
                },
                AckedPacket {
                    packet_number: 2,
                    sent: start + Duration::from_millis(210),
                    bytes: 1_000,
                },
            ],
            &[],
        );

        assert_eq!(sample.max_bandwidth, 10_000);
        assert_eq!(sample.total_bytes_acked, 3_000);
    }

    #[test]
    fn bandwidth_sample_keeps_the_app_limited_state_captured_at_send() {
        let start = Instant::now();
        let mut sampler = BandwidthSampler::default();

        sampler.on_packet_sent(start, 0, 1_000, 0, false);
        sampler.on_congestion_event(
            start + Duration::from_millis(100),
            &[AckedPacket {
                packet_number: 0,
                sent: start,
                bytes: 1_000,
            }],
            &[],
        );
        sampler.on_packet_sent(start + Duration::from_millis(110), 1, 1_000, 0, true);

        let sample = sampler.on_congestion_event(
            start + Duration::from_millis(120),
            &[AckedPacket {
                packet_number: 1,
                sent: start + Duration::from_millis(110),
                bytes: 1_000,
            }],
            &[],
        );

        assert!(sample.sample_is_app_limited);
        assert!(sample.last_packet_send_state.unwrap().is_app_limited);
    }

    #[test]
    fn ack_and_loss_update_one_sampler_event() {
        let start = Instant::now();
        let mut sampler = BandwidthSampler::default();

        sampler.on_packet_sent(start, 0, 1_000, 0, false);
        sampler.on_packet_sent(start + Duration::from_millis(1), 1, 1_000, 1_000, false);

        let sample = sampler.on_congestion_event(
            start + Duration::from_millis(100),
            &[AckedPacket {
                packet_number: 0,
                sent: start,
                bytes: 1_000,
            }],
            &[LostPacket {
                packet_number: 1,
                sent: start + Duration::from_millis(1),
                bytes: 1_000,
            }],
        );

        assert_eq!(sample.total_bytes_acked, 1_000);
        assert_eq!(sample.total_bytes_lost, 1_000);
        assert_eq!(
            sample.last_packet_send_state.unwrap().packet_number,
            1,
            "the event must expose the newest packet's send state across ACK and loss",
        );
    }

    #[test]
    fn app_limited_phase_ends_after_a_newer_packet_is_acked() {
        let start = Instant::now();
        let mut sampler = BandwidthSampler::default();

        sampler.on_packet_sent(start, 0, 1_000, 0, false);
        sampler.on_congestion_event(
            start + Duration::from_millis(100),
            &[AckedPacket {
                packet_number: 0,
                sent: start,
                bytes: 1_000,
            }],
            &[],
        );

        sampler.on_app_limited();
        sampler.on_packet_sent(start + Duration::from_secs(1), 1, 1_000, 0, false);
        let sample = sampler.on_congestion_event(
            start + Duration::from_millis(1_100),
            &[AckedPacket {
                packet_number: 1,
                sent: start + Duration::from_secs(1),
                bytes: 1_000,
            }],
            &[],
        );
        assert!(sample.sample_is_app_limited);

        sampler.on_packet_sent(start + Duration::from_millis(1_110), 2, 1_000, 0, false);
        let sample = sampler.on_congestion_event(
            start + Duration::from_millis(1_210),
            &[AckedPacket {
                packet_number: 2,
                sent: start + Duration::from_millis(1_110),
                bytes: 1_000,
            }],
            &[],
        );
        assert!(!sample.sample_is_app_limited);
    }

    #[test]
    fn neutered_packet_state_is_removed() {
        let start = Instant::now();
        let mut sampler = BandwidthSampler::default();
        sampler.on_packet_sent(start, 7, 1_000, 0, false);

        sampler.on_packet_neutered(7);

        assert!(sampler.sent_packets.is_empty());
        assert_eq!(sampler.total_bytes_neutered, 1_000);
    }
}
