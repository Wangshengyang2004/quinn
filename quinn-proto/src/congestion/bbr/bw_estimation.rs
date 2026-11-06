use std::collections::{BTreeMap, VecDeque};
use std::fmt::{Debug, Display, Formatter};

use super::min_max::MinMax;
use crate::{Duration, Instant};

const A0_CANDIDATE_CAPACITY: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AckPoint {
    time: Instant,
    total_acked: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PacketSendState {
    pub(super) app_limited: bool,
    pub(super) bytes_in_flight: u64,
}

#[derive(Clone, Copy, Debug)]
struct SentPacketState {
    sent_time: Instant,
    size: u64,
    total_sent: u64,
    total_acked: u64,
    total_sent_at_last_ack: u64,
    last_acked_packet_sent_time: Option<Instant>,
    last_acked_packet_ack_time: Option<Instant>,
    send_state: PacketSendState,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BandwidthEstimation {
    total_acked: u64,
    total_sent: u64,
    total_sent_at_last_ack: u64,
    last_acked_packet_sent_time: Option<Instant>,
    last_acked_packet_ack_time: Option<Instant>,
    sent_packets: BTreeMap<u64, SentPacketState>,
    is_app_limited: bool,
    end_of_app_limited_phase: Option<u64>,
    has_non_app_limited_sample: bool,
    last_event_packet_send_state: Option<(u64, PacketSendState)>,
    max_filter: MinMax,
    event_max_bandwidth: Option<(u64, bool)>,
    acked_at_last_window: u64,
    estimate_at_last_window: u64,
    overestimate_avoidance: bool,
    recent_ack_points: [Option<AckPoint>; 2],
    a0_candidates: VecDeque<AckPoint>,
}

impl BandwidthEstimation {
    pub(crate) fn enable_overestimate_avoidance(&mut self) {
        self.overestimate_avoidance = true;
    }

    #[cfg(test)]
    pub(crate) fn overestimate_avoidance_enabled(&self) -> bool {
        self.overestimate_avoidance
    }

    #[cfg(test)]
    pub(super) fn retained_packet_count(&self) -> usize {
        self.sent_packets.len()
    }

    pub(crate) fn on_sent(
        &mut self,
        now: Instant,
        packet_number: u64,
        bytes: u64,
        bytes_in_flight_before: u64,
    ) {
        if bytes == 0 {
            return;
        }
        self.total_sent += bytes;

        if bytes_in_flight_before == 0 {
            self.last_acked_packet_ack_time = Some(now);
            self.total_sent_at_last_ack = self.total_sent;
            self.last_acked_packet_sent_time = Some(now);
            if self.overestimate_avoidance {
                self.recent_ack_points = [None; 2];
                self.update_recent_ack_point(now);
                self.a0_candidates.clear();
                self.push_a0_candidate(self.recent_ack_points[1].unwrap());
            }
        }

        self.sent_packets.insert(
            packet_number,
            SentPacketState {
                sent_time: now,
                size: bytes,
                total_sent: self.total_sent,
                total_acked: self.total_acked,
                total_sent_at_last_ack: self.total_sent_at_last_ack,
                last_acked_packet_sent_time: self.last_acked_packet_sent_time,
                last_acked_packet_ack_time: self.last_acked_packet_ack_time,
                send_state: PacketSendState {
                    app_limited: self.is_app_limited,
                    bytes_in_flight: bytes_in_flight_before + bytes,
                },
            },
        );
    }

    pub(crate) fn on_ack(&mut self, now: Instant, packet_number: u64) -> Option<bool> {
        let sent = self.sent_packets.remove(&packet_number)?;
        // Keep the newest valid send state across ACK and loss callbacks,
        // including packets whose delivery-rate sample has zero duration.
        self.record_event_send_state(packet_number, sent.send_state);
        self.total_acked += sent.size;
        self.total_sent_at_last_ack = sent.total_sent;
        self.last_acked_packet_sent_time = Some(sent.sent_time);
        self.last_acked_packet_ack_time = Some(now);
        if self.overestimate_avoidance {
            self.update_recent_ack_point(now);
        }

        if self.is_app_limited
            && self
                .end_of_app_limited_phase
                .is_none_or(|end| packet_number > end)
        {
            self.is_app_limited = false;
        }

        let last_acked_sent = sent.last_acked_packet_sent_time?;
        let send_rate = if sent.sent_time > last_acked_sent {
            Self::bw_from_delta(
                sent.total_sent - sent.total_sent_at_last_ack,
                sent.sent_time - last_acked_sent,
            )
            .unwrap_or(0)
        } else {
            u64::MAX
        };

        let a0 = self
            .overestimate_avoidance
            .then(|| self.choose_a0_point(sent.total_acked))
            .flatten();
        let ack_rate = if let Some(a0) = a0 {
            if now <= a0.time || self.total_acked < a0.total_acked {
                0
            } else {
                Self::bw_from_delta(self.total_acked - a0.total_acked, now - a0.time).unwrap_or(0)
            }
        } else if let Some(last_acked_time) = sent.last_acked_packet_ack_time {
            if now <= last_acked_time {
                return None;
            }
            Self::bw_from_delta(self.total_acked - sent.total_acked, now - last_acked_time)
                .unwrap_or(0)
        } else {
            return None;
        };

        let bandwidth = send_rate.min(ack_rate);
        // Select the event maximum before applying the app-limited gate. Updating
        // the filter per packet can expire its old peak on an earlier full-rate
        // sample, then admit a smaller app-limited event maximum against that
        // already-reduced peak. Hysteria gates the completed event instead.
        if bandwidth > 0
            && self
                .event_max_bandwidth
                .is_none_or(|(best, _)| bandwidth > best)
        {
            self.event_max_bandwidth = Some((bandwidth, sent.send_state.app_limited));
        }
        Some(sent.send_state.app_limited)
    }

    /// Commit one completed congestion event before its model is consumed.
    pub(crate) fn update_estimate(&mut self, round: u64) {
        if let Some((bandwidth, app_limited)) = self.event_max_bandwidth.take() {
            if !app_limited || bandwidth > self.max_filter.get() {
                self.max_filter.update_max(round, bandwidth);
            }
        }
    }

    pub(crate) fn on_packet_discarded(&mut self, packet_number: u64) {
        self.sent_packets.remove(&packet_number);
    }

    pub(crate) fn on_packet_lost(&mut self, packet_number: u64) {
        if let Some(sent) = self.sent_packets.remove(&packet_number) {
            self.record_event_send_state(packet_number, sent.send_state);
        }
    }

    fn record_event_send_state(&mut self, packet_number: u64, send_state: PacketSendState) {
        if self
            .last_event_packet_send_state
            .is_none_or(|(last, _)| packet_number > last)
        {
            self.last_event_packet_send_state = Some((packet_number, send_state));
        }
    }

    pub(crate) fn on_app_limited(&mut self, end_packet_number: u64) {
        self.is_app_limited = true;
        self.end_of_app_limited_phase = Some(end_packet_number);
    }

    pub(super) fn last_event_packet_send_state(&self) -> Option<PacketSendState> {
        self.last_event_packet_send_state.map(|(_, state)| state)
    }

    pub(crate) fn has_non_app_limited_sample(&self) -> bool {
        self.has_non_app_limited_sample
    }

    pub(crate) fn bytes_acked_this_window(&self) -> u64 {
        self.total_acked - self.acked_at_last_window
    }

    pub(crate) fn estimate_increased_this_window(&self) -> bool {
        self.get_estimate() > self.estimate_at_last_window
    }

    pub(crate) fn end_acks(
        &mut self,
        _current_round: u64,
        _app_limited: bool,
        new_aggregation_epoch: bool,
    ) {
        debug_assert!(self.event_max_bandwidth.is_none());
        self.acked_at_last_window = self.total_acked;
        self.estimate_at_last_window = self.get_estimate();
        // Overshoot history uses the same completed event send state as startup.
        // A full-rate ACK can be older than an application-limited lost packet.
        self.has_non_app_limited_sample |= self
            .last_event_packet_send_state
            .is_some_and(|(_, state)| !state.app_limited);
        self.last_event_packet_send_state = None;
        if self.overestimate_avoidance && new_aggregation_epoch {
            if let Some(point) = self.recent_ack_points[0].or(self.recent_ack_points[1]) {
                self.push_a0_candidate(point);
            }
        }
    }

    pub(crate) fn get_estimate(&self) -> u64 {
        self.max_filter.get()
    }

    pub(crate) const fn bw_from_delta(bytes: u64, delta: Duration) -> Option<u64> {
        let window_duration_ns = delta.as_nanos();
        if window_duration_ns == 0 {
            return None;
        }
        let b_ns = bytes * 1_000_000_000;
        let bytes_per_second = b_ns / (window_duration_ns as u64);
        Some(bytes_per_second)
    }

    fn update_recent_ack_point(&mut self, time: Instant) {
        let point = AckPoint {
            time,
            total_acked: self.total_acked,
        };
        match self.recent_ack_points[1] {
            None => self.recent_ack_points[1] = Some(point),
            Some(recent) if time < recent.time => {
                self.recent_ack_points[1] = Some(AckPoint { time, ..recent });
            }
            Some(recent) if time > recent.time => {
                self.recent_ack_points = [Some(recent), Some(point)];
            }
            Some(_) => self.recent_ack_points[1] = Some(point),
        }
    }

    fn push_a0_candidate(&mut self, point: AckPoint) {
        if self.a0_candidates.len() == A0_CANDIDATE_CAPACITY {
            self.a0_candidates.pop_front();
        }
        self.a0_candidates.push_back(point);
    }

    fn choose_a0_point(&mut self, total_bytes_acked_at_send: u64) -> Option<AckPoint> {
        if self.a0_candidates.len() <= 1 {
            return self.a0_candidates.front().copied();
        }

        let selected = self
            .a0_candidates
            .iter()
            .position(|point| point.total_acked > total_bytes_acked_at_send)
            .map_or(self.a0_candidates.len() - 1, |index| {
                index.saturating_sub(1)
            });
        let point = self.a0_candidates[selected];
        for _ in 0..selected {
            self.a0_candidates.pop_front();
        }
        Some(point)
    }
}

impl Display for BandwidthEstimation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:.3} MB/s",
            self.get_estimate() as f32 / (1024 * 1024) as f32
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discarding_packet_preserves_delivery_and_event_samples() {
        let now = Instant::now();
        let mut estimation = BandwidthEstimation::default();
        estimation.on_sent(now, 1, 1200, 0);
        estimation.on_app_limited(1);
        estimation.on_sent(now, 2, 1200, 1200);
        estimation.on_ack(now + Duration::from_millis(50), 1);
        estimation.update_estimate(0);
        let event = estimation.last_event_packet_send_state;
        let acked = estimation.total_acked;
        let estimate = estimation.get_estimate();
        let ack_time = estimation.last_acked_packet_ack_time;
        assert_eq!(
            event,
            Some((
                1,
                PacketSendState {
                    app_limited: false,
                    bytes_in_flight: 1200
                }
            ))
        );
        assert_eq!(estimation.sent_packets.len(), 1);
        estimation.on_packet_discarded(2);
        estimation.on_packet_discarded(2);
        estimation.on_packet_discarded(99);
        assert!(estimation.sent_packets.is_empty());
        assert_eq!(estimation.last_event_packet_send_state, event);
        assert_eq!(estimation.total_acked, acked);
        assert_eq!(estimation.get_estimate(), estimate);
        assert_eq!(estimation.last_acked_packet_ack_time, ack_time);
    }

    #[test]
    fn zero_duration_sample_retains_last_ack_send_state_until_batch_end() {
        let now = Instant::now();
        let mut estimation = BandwidthEstimation::default();
        estimation.on_app_limited(0);
        estimation.on_sent(now, 1, 1200, 0);
        assert_eq!(estimation.on_ack(now, 1), None);
        assert_eq!(
            estimation
                .last_event_packet_send_state()
                .map(|state| state.app_limited),
            Some(true)
        );
        estimation.update_estimate(0);
        estimation.end_acks(0, false, false);
        assert_eq!(estimation.last_event_packet_send_state(), None);
    }

    #[test]
    fn application_limited_samples_raise_but_do_not_lower_the_estimate() {
        let mut estimator = BandwidthEstimation::default();
        let start = Instant::now();
        for round in 0..25 {
            let now = start + Duration::from_millis(round * 10);
            let bytes = if round < 2 { 10_000 } else { 1_000 };
            if round >= 2 {
                estimator.on_app_limited(round - 1);
            }
            estimator.on_sent(now, round, bytes, 0);
            estimator.on_ack(now + Duration::from_millis(50), round);
            estimator.update_estimate(round);
            if round > 0 {
                assert_eq!(estimator.get_estimate(), 200_000);
            }
        }
    }

    #[test]
    fn full_rate_samples_expire_an_old_peak() {
        let mut estimator = BandwidthEstimation::default();
        let start = Instant::now();
        for round in 0..25 {
            let now = start + Duration::from_millis(round * 10);
            let bytes = if round < 2 { 10_000 } else { 1_000 };
            estimator.on_sent(now, round, bytes, 0);
            estimator.on_ack(now + Duration::from_millis(50), round);
            estimator.update_estimate(round);
            if round == 1 {
                assert_eq!(estimator.get_estimate(), 200_000);
            }
        }
        assert_eq!(estimator.get_estimate(), 20_000);
    }

    #[test]
    fn a0_selection_uses_bytes_acked_when_packet_was_sent_and_prunes_history() {
        let now = Instant::now();
        let mut estimation = BandwidthEstimation {
            a0_candidates: VecDeque::from([
                AckPoint {
                    time: now,
                    total_acked: 0,
                },
                AckPoint {
                    time: now + Duration::from_millis(1),
                    total_acked: 100,
                },
                AckPoint {
                    time: now + Duration::from_millis(2),
                    total_acked: 200,
                },
            ]),
            ..Default::default()
        };

        assert_eq!(estimation.choose_a0_point(150).unwrap().total_acked, 100);
        assert_eq!(estimation.a0_candidates.len(), 2);
    }

    #[test]
    fn overestimate_avoidance_anchors_ack_rate_to_a0() {
        let start = Instant::now();
        let mut estimation = BandwidthEstimation::default();
        estimation.enable_overestimate_avoidance();
        estimation.on_sent(start, 1, 1000, 0);
        estimation.on_sent(start + Duration::from_millis(10), 2, 1000, 1000);
        estimation.on_ack(start + Duration::from_millis(100), 1);

        estimation.update_estimate(0);
        assert_eq!(estimation.get_estimate(), 10_000);
    }

    #[test]
    fn packet_samples_use_send_state_captured_for_that_packet() {
        let start = Instant::now();
        let mut estimation = BandwidthEstimation::default();
        estimation.on_sent(start, 1, 1000, 0);
        estimation.on_sent(start + Duration::from_millis(10), 2, 1000, 1000);

        estimation.on_ack(start + Duration::from_millis(20), 1);
        estimation.on_ack(start + Duration::from_millis(30), 2);

        // Packet 2 was sent 10 ms after the opening packet (100 KB/s send rate), while 2 KB
        // were acknowledged over 30 ms (66.666 KB/s ACK rate). BBR uses the lower rate.
        estimation.update_estimate(0);
        assert_eq!(estimation.get_estimate(), 66_666);
    }

    #[test]
    fn app_limited_state_exits_only_after_acknowledging_beyond_the_marker() {
        let start = Instant::now();
        let mut estimation = BandwidthEstimation::default();
        estimation.on_sent(start, 1, 1000, 0);
        estimation.on_app_limited(1);
        estimation.on_sent(start + Duration::from_millis(10), 2, 1000, 1000);

        assert_eq!(
            estimation.on_ack(start + Duration::from_millis(20), 1),
            Some(false)
        );
        assert!(estimation.is_app_limited);
        assert_eq!(
            estimation.on_ack(start + Duration::from_millis(30), 2),
            Some(true)
        );
        assert!(!estimation.is_app_limited);
        estimation.update_estimate(0);
        assert_eq!(estimation.get_estimate(), 66_666);
        assert_eq!(
            estimation
                .last_event_packet_send_state()
                .map(|state| state.app_limited),
            Some(true)
        );

        estimation.on_sent(start + Duration::from_millis(40), 3, 1000, 0);
        assert_eq!(
            estimation.on_ack(start + Duration::from_millis(50), 3),
            Some(false)
        );
        estimation.update_estimate(0);
        estimation.end_acks(0, false, false);
        assert!(estimation.has_non_app_limited_sample());
    }

    #[test]
    fn lost_packets_are_removed_from_sampler_state() {
        let start = Instant::now();
        let mut estimation = BandwidthEstimation::default();
        estimation.on_sent(start, 7, 1200, 0);
        assert!(estimation.sent_packets.contains_key(&7));

        estimation.on_packet_lost(7);
        assert!(!estimation.sent_packets.contains_key(&7));
    }

    #[test]
    fn event_send_state_ignores_callback_order_and_unknown_loss() {
        let start = Instant::now();
        let mut estimation = BandwidthEstimation::default();
        estimation.on_sent(start, 1, 1200, 0);
        estimation.on_app_limited(1);
        estimation.on_sent(start + Duration::from_millis(10), 2, 1200, 1200);
        estimation.on_sent(start + Duration::from_millis(20), 3, 1200, 2400);

        // The caller may deliver loss callbacks before ACKs or out of order.
        estimation.on_packet_lost(3);
        estimation.on_packet_lost(2);
        estimation.on_ack(start + Duration::from_millis(100), 1);
        estimation.on_packet_lost(999);
        assert_eq!(
            estimation
                .last_event_packet_send_state()
                .map(|state| state.app_limited),
            Some(true)
        );
        assert_eq!(estimation.last_event_packet_send_state.unwrap().0, 3);

        estimation.update_estimate(0);
        estimation.end_acks(0, false, false);
        estimation.on_packet_lost(3); // Duplicate notification has no send state.
        estimation.on_sent(start + Duration::from_millis(110), 1000, 0, 0);
        estimation.on_packet_lost(1000); // ACK-only packet is not sampled.
        assert_eq!(estimation.last_event_packet_send_state(), None);
    }
}

#[cfg(test)]
mod event_filter_diagnostic {
    use super::*;
    #[test]
    fn diagnostic_mixed_event_filter_matches_go() {
        let mut mismatches = Vec::new();
        for round in [1, 11] {
            for second_app_limited in [false, true] {
                let mut e = BandwidthEstimation::default();
                e.max_filter.update_max(0, 100_000);
                let start = Instant::now();
                e.on_sent(start, 1, 1_000, 0);
                if second_app_limited {
                    e.on_app_limited(1);
                }
                e.on_sent(start + Duration::from_millis(10), 2, 1_000, 1_000);
                let now = start + Duration::from_millis(100);
                assert_eq!(e.on_ack(now, 1), Some(false));
                assert_eq!(e.on_ack(now, 2), Some(second_app_limited));
                e.update_estimate(round);
                let actual = e.get_estimate();
                let expected = if round == 11 && !second_app_limited {
                    20_000
                } else {
                    100_000
                };
                println!(
                    "round={round} second_app_limited={second_app_limited} estimate_bytes_s={actual} expected_go={expected}"
                );
                if actual != expected {
                    mismatches.push((round, second_app_limited, actual, expected));
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "event filter divergence: {mismatches:?}"
        );
    }
}
