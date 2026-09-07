use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use rand::{Rng, SeedableRng};

use crate::congestion::bbr::bandwidth_sampler::{
    BandwidthSampler, CongestionEventSample, SendTimeState, bandwidth_from_delta,
};
use crate::congestion::bbr::min_max::MinMax;
#[cfg(test)]
use crate::connection::RttEstimator;
use crate::{Duration, Instant};

use super::{BASE_DATAGRAM_SIZE, CongestionEvent, Controller, ControllerFactory, SentPacketInfo};

mod bandwidth_sampler;
mod min_max;

/// Experimental! Use at your own risk.
///
/// Aims for reduced buffer bloat and improved performance over high bandwidth-delay product networks.
/// Based on google's quiche implementation <https://source.chromium.org/chromium/chromium/src/+/master:net/third_party/quiche/src/quic/core/congestion_control/bbr_sender.cc>
/// of BBR <https://datatracker.ietf.org/doc/html/draft-cardwell-iccrg-bbr-congestion-control>.
/// More discussion and links at <https://groups.google.com/g/bbr-dev>.
#[derive(Debug, Clone)]
pub struct Bbr {
    diagnostic_feedback: super::FeedbackDiagnostics,
    ramp_budget: Option<(Instant, u64)>,
    ramp_exit: &'static str,
    probe_cycle_bw: u64,
    probe_extensions: u64,
    probe_episode: Option<(Instant, Instant)>,
    probe_restarts: u64,
    probe_episode_count: u64,
    probe_aborts: u64,
    config: Arc<BbrConfig>,
    current_mtu: u64,
    bandwidth_sampler: BandwidthSampler,
    max_bandwidth: MinMax,
    acked_bytes: u64,
    mode: Mode,
    loss_state: LossState,
    recovery_state: RecoveryState,
    recovery_window: u64,
    is_at_full_bandwidth: bool,
    pacing_gain: f32,
    high_gain: f32,
    drain_gain: f32,
    cwnd_gain: f32,
    high_cwnd_gain: f32,
    cwnd_gain_constant: f32,
    startup_rounds: u64,
    exit_startup_on_loss: bool,
    drain_to_target: bool,
    enable_ack_aggregation_during_startup: bool,
    expire_ack_aggregation_in_startup: bool,
    detect_overshooting: bool,
    bytes_lost_while_detecting_overshooting: u64,
    bytes_lost_multiplier_while_detecting_overshooting: u64,
    cwnd_to_calculate_min_pacing_rate: u64,
    has_non_app_limited_sample: bool,
    loss_events_in_round: u64,
    bytes_lost_in_round: u64,
    last_cycle_start: Option<Instant>,
    current_cycle_offset: u8,
    init_cwnd: u64,
    min_cwnd: u64,
    exit_probe_rtt_at: Option<Instant>,
    probe_rtt_round_passed: bool,
    min_rtt_timestamp: Option<Instant>,
    min_rtt: Duration,
    exiting_quiescence: bool,
    pacing_rate: u64,
    max_acked_packet_number: u64,
    max_sent_packet_number: u64,
    end_recovery_at_packet_number: u64,
    cwnd: u64,
    current_round_trip_end_packet_number: Option<u64>,
    round_count: u64,
    bw_at_last_round: u64,
    round_wo_bw_gain: u64,
    ack_aggregation: AckAggregationState,
    random_number_generator: rand::rngs::StdRng,
}

impl Bbr {
    /// Construct a state using the given `config` and current time `now`
    pub fn new(config: Arc<BbrConfig>, current_mtu: u16) -> Self {
        let initial_window = config.computed_initial_window(current_mtu);
        let profile = config.profile.config();
        Self {
            diagnostic_feedback: super::FeedbackDiagnostics::default(),
            ramp_budget: None,
            ramp_exit: "none",
            probe_cycle_bw: 0,
            probe_extensions: 0,
            probe_episode: None,
            probe_restarts: 0,
            probe_episode_count: 0,
            probe_aborts: 0,
            config,
            current_mtu: current_mtu as u64,
            bandwidth_sampler: BandwidthSampler::default(),
            max_bandwidth: MinMax::new(K_BANDWIDTH_WINDOW_SIZE),
            acked_bytes: 0,
            mode: Mode::Startup,
            loss_state: Default::default(),
            recovery_state: RecoveryState::NotInRecovery,
            recovery_window: 0,
            is_at_full_bandwidth: false,
            pacing_gain: profile.high_gain,
            high_gain: profile.high_gain,
            drain_gain: 1.0 / profile.high_gain,
            cwnd_gain: profile.high_cwnd_gain,
            high_cwnd_gain: profile.high_cwnd_gain,
            cwnd_gain_constant: profile.cwnd_gain_constant,
            startup_rounds: profile.startup_rounds,
            exit_startup_on_loss: profile.exit_startup_on_loss,
            drain_to_target: profile.drain_to_target,
            enable_ack_aggregation_during_startup: profile.enable_ack_aggregation_during_startup,
            expire_ack_aggregation_in_startup: profile.expire_ack_aggregation_in_startup,
            detect_overshooting: profile.detect_overshooting,
            bytes_lost_while_detecting_overshooting: 0,
            bytes_lost_multiplier_while_detecting_overshooting: profile.bytes_lost_multiplier,
            cwnd_to_calculate_min_pacing_rate: initial_window,
            has_non_app_limited_sample: false,
            loss_events_in_round: 0,
            bytes_lost_in_round: 0,
            last_cycle_start: None,
            current_cycle_offset: 0,
            init_cwnd: initial_window,
            min_cwnd: calculate_min_window(current_mtu as u64),
            exit_probe_rtt_at: None,
            probe_rtt_round_passed: false,
            min_rtt_timestamp: None,
            min_rtt: Default::default(),
            exiting_quiescence: false,
            pacing_rate: 0,
            max_acked_packet_number: 0,
            max_sent_packet_number: 0,
            end_recovery_at_packet_number: 0,
            cwnd: initial_window,
            current_round_trip_end_packet_number: None,
            round_count: 0,
            bw_at_last_round: 0,
            round_wo_bw_gain: 0,
            ack_aggregation: AckAggregationState::default(),
            random_number_generator: rand::rngs::StdRng::from_os_rng(),
        }
    }

    fn enter_startup_mode(&mut self) {
        self.mode = Mode::Startup;
        self.pacing_gain = self.high_gain;
        self.cwnd_gain = self.high_cwnd_gain;
    }

    fn enter_probe_bandwidth_mode(&mut self, now: Instant) {
        self.mode = Mode::ProbeBw;
        self.cwnd_gain = self.cwnd_gain_constant;
        self.last_cycle_start = Some(now);
        self.probe_cycle_bw = self.max_bandwidth.get();
        // Pick a random offset for the gain cycle out of {0, 2..7} range. 1 is
        // excluded because in that case increased gain and decreased gain would not
        // follow each other.
        let mut rand_index = self
            .random_number_generator
            .random_range(0..K_PACING_GAIN.len() as u8 - 1);
        if rand_index >= 1 {
            rand_index += 1;
        }
        self.current_cycle_offset = rand_index;
        self.pacing_gain = K_PACING_GAIN[rand_index as usize];
    }

    fn update_recovery_state(&mut self, is_round_start: bool) {
        if !self.is_at_full_bandwidth {
            return;
        }
        // Exit recovery when there are no losses for a round.
        if self.loss_state.has_losses() {
            self.end_recovery_at_packet_number = self.max_sent_packet_number;
        }
        match self.recovery_state {
            // Enter conservation on the first loss.
            RecoveryState::NotInRecovery if self.loss_state.has_losses() => {
                self.recovery_state = RecoveryState::Conservation;
                // This will cause the |recovery_window| to be set to the
                // correct value in CalculateRecoveryWindow().
                self.recovery_window = 0;
                // Since the conservation phase is meant to be lasting for a whole
                // round, extend the current round as if it were started right now.
                self.current_round_trip_end_packet_number = Some(self.max_sent_packet_number);
            }
            RecoveryState::Growth | RecoveryState::Conservation => {
                if self.recovery_state == RecoveryState::Conservation && is_round_start {
                    self.recovery_state = RecoveryState::Growth;
                }
                // Exit recovery if appropriate.
                if !self.loss_state.has_losses()
                    && self.max_acked_packet_number > self.end_recovery_at_packet_number
                {
                    self.recovery_state = RecoveryState::NotInRecovery;
                }
            }
            _ => {}
        }
    }

    fn has_clear_probe_growth(
        &self,
        event: &CongestionEvent<'_>,
        sample: &CongestionEventSample,
    ) -> bool {
        if !self.config.feedback_probe
            || self.mode != Mode::ProbeBw
            || self.min_rtt.is_zero()
            || self.probe_cycle_bw == 0
            || sample.bytes_acked == 0
            || sample.sample_is_app_limited
            || event.app_limited
            || sample.bytes_lost != 0
            || self.bytes_lost_in_round != 0
            || self.recovery_state.in_recovery()
            || event.ecn
            || event.is_persistent_congestion
        {
            return false;
        }
        let queue_limit = self.min_rtt.saturating_add(self.min_rtt / 20);
        u128::from(sample.max_bandwidth) * 10 >= u128::from(self.probe_cycle_bw) * 11
            && sample
                .sample_rtt
                .is_some_and(|rtt| !rtt.is_zero() && rtt <= queue_limit)
            && event.rtt.get() <= queue_limit
    }

    fn can_extend_growth_probe(
        &self,
        event: &CongestionEvent<'_>,
        sample: &CongestionEventSample,
    ) -> bool {
        self.has_clear_probe_growth(event, sample)
            && self.pacing_gain > 1.0
            && self
                .probe_episode
                .is_some_and(|(deadline, _)| event.now < deadline)
            && self.last_cycle_start.is_some_and(|start| {
                event.now >= start
                    && event.now.duration_since(start) < self.min_rtt.saturating_mul(8)
            })
    }

    fn update_growth_probe(
        &mut self,
        event: &CongestionEvent<'_>,
        sample: &CongestionEventSample,
    ) -> bool {
        if !self.config.feedback_probe || self.mode != Mode::ProbeBw {
            return false;
        }
        let queue_limit = self.min_rtt.saturating_add(self.min_rtt / 20);
        let congested = sample.bytes_lost != 0
            || self.bytes_lost_in_round != 0
            || self.recovery_state.in_recovery()
            || event.ecn
            || event.is_persistent_congestion
            || sample.sample_rtt.is_some_and(|rtt| rtt > queue_limit)
            || event.rtt.get() > queue_limit;
        if congested {
            if let Some((deadline, ready)) = self.probe_episode {
                if event.now < deadline {
                    self.probe_episode = Some((event.now, ready));
                    self.probe_aborts = self.probe_aborts.saturating_add(1);
                }
            }
            return false;
        }
        if !self.has_clear_probe_growth(event, sample) {
            return false;
        }
        let needs_episode = match self.probe_episode {
            Some((deadline, ready)) if event.now >= deadline && event.now < ready => return false,
            Some((_, ready)) => event.now >= ready,
            None => true,
        };
        if needs_episode {
            let duration = self.min_rtt.saturating_mul(8);
            let Some(deadline) = event.now.checked_add(duration) else {
                return false;
            };
            let Some(ready) = deadline.checked_add(duration) else {
                return false;
            };
            self.probe_episode = Some((deadline, ready));
            self.probe_episode_count = self.probe_episode_count.saturating_add(1);
        }
        // A probe's delivery feedback can arrive after its UP phase has ended.
        // Restart only after flight fits the newly measured BDP; keep one budget.
        if self.pacing_gain <= 1.0 && event.in_flight <= self.get_target_cwnd(1.0) {
            self.current_cycle_offset = 0;
            self.pacing_gain = K_PACING_GAIN[0];
            self.last_cycle_start = Some(event.now);
            self.probe_cycle_bw = self.max_bandwidth.get();
            self.probe_restarts = self.probe_restarts.saturating_add(1);
            return true;
        }
        false
    }

    fn update_gain_cycle_phase(&mut self, now: Instant, in_flight: u64, extend_growth: bool) {
        // In most cases, the cycle is advanced after an RTT passes.
        let mut should_advance_gain_cycling = self
            .last_cycle_start
            .map(|last_cycle_start| now.duration_since(last_cycle_start) > self.min_rtt)
            .unwrap_or(false);
        // If the pacing gain is above 1.0, the connection is trying to probe the
        // bandwidth by increasing the number of bytes in flight to at least
        // pacing_gain * BDP.  Make sure that it actually reaches the target, as
        // long as there are no losses suggesting that the buffers are not able to
        // hold that much.
        if self.pacing_gain > 1.0
            && !self.loss_state.has_losses()
            && in_flight < self.get_target_cwnd(self.pacing_gain)
        {
            should_advance_gain_cycling = false;
        }

        // If pacing gain is below 1.0, the connection is trying to drain the extra
        // queue which could have been incurred by probing prior to it.  If the
        // number of bytes in flight falls down to the estimated BDP value earlier,
        // conclude that the queue has been successfully drained and exit this cycle
        // early.
        if self.pacing_gain < 1.0 && in_flight <= self.get_target_cwnd(1.0) {
            should_advance_gain_cycling = true;
        }

        if should_advance_gain_cycling && extend_growth {
            self.probe_extensions = self.probe_extensions.saturating_add(1);
            should_advance_gain_cycling = false;
        }
        if should_advance_gain_cycling {
            self.current_cycle_offset = (self.current_cycle_offset + 1) % K_PACING_GAIN.len() as u8;
            self.last_cycle_start = Some(now);
            // Stay in low gain mode until the target BDP is hit.  Low gain mode
            // will be exited immediately when the target BDP is achieved.
            if self.drain_to_target
                && self.pacing_gain < 1.0
                && (K_PACING_GAIN[self.current_cycle_offset as usize] - 1.0).abs() < f32::EPSILON
                && in_flight > self.get_target_cwnd(1.0)
            {
                return;
            }
            self.pacing_gain = K_PACING_GAIN[self.current_cycle_offset as usize];
            if self.pacing_gain > 1.0 {
                self.probe_cycle_bw = self.max_bandwidth.get();
            }
        }
    }

    fn maybe_exit_startup_or_drain(&mut self, now: Instant, in_flight: u64) {
        if self.mode == Mode::Startup && self.is_at_full_bandwidth {
            self.mode = Mode::Drain;
            self.pacing_gain = self.drain_gain;
            self.cwnd_gain = self.high_cwnd_gain;
        }
        if self.mode == Mode::Drain && in_flight <= self.get_target_cwnd(1.0) {
            self.enter_probe_bandwidth_mode(now);
        }
    }

    fn is_min_rtt_expired(&self, now: Instant) -> bool {
        self.min_rtt_timestamp
            .is_some_and(|timestamp| now.saturating_duration_since(timestamp) > K_MIN_RTT_EXPIRY)
    }

    fn maybe_update_min_rtt(&mut self, now: Instant, sample_rtt: Duration) -> bool {
        let expired = self.is_min_rtt_expired(now);
        if self.min_rtt.is_zero() || sample_rtt < self.min_rtt || expired {
            self.min_rtt = sample_rtt;
            self.min_rtt_timestamp = Some(now);
        }
        expired
    }

    fn maybe_enter_or_exit_probe_rtt(
        &mut self,
        now: Instant,
        is_round_start: bool,
        bytes_in_flight: u64,
        min_rtt_expired: bool,
    ) {
        if min_rtt_expired && !self.exiting_quiescence && self.mode != Mode::ProbeRtt {
            self.mode = Mode::ProbeRtt;
            self.pacing_gain = 1.0;
            // Do not decide on the time to exit ProbeRtt until the
            // |bytes_in_flight| is at the target small value.
            self.exit_probe_rtt_at = None;
        }

        if self.mode == Mode::ProbeRtt {
            if self.exit_probe_rtt_at.is_none() {
                // If the window has reached the appropriate size, schedule exiting
                // ProbeRtt.  The CWND during ProbeRtt is
                // kMinimumCongestionWindow, but we allow an extra packet since QUIC
                // checks CWND before sending a packet.
                if bytes_in_flight < self.get_probe_rtt_cwnd() + self.current_mtu {
                    const K_PROBE_RTT_TIME: Duration = Duration::from_millis(200);
                    self.exit_probe_rtt_at = Some(now + K_PROBE_RTT_TIME);
                    self.probe_rtt_round_passed = false;
                }
            } else {
                self.probe_rtt_round_passed |= is_round_start;
            }
            if self.exit_probe_rtt_at.is_some_and(|exit| now >= exit) && self.probe_rtt_round_passed
            {
                self.min_rtt_timestamp = Some(now);
                if !self.is_at_full_bandwidth {
                    self.enter_startup_mode();
                } else {
                    self.enter_probe_bandwidth_mode(now);
                }
            }
        }

        self.exiting_quiescence = false;
    }

    fn get_target_cwnd(&self, gain: f32) -> u64 {
        let bw = self.max_bandwidth.get();
        let bdp = self.min_rtt.as_micros() as u64 * bw;
        let bdpf = bdp as f64;
        let cwnd = ((gain as f64 * bdpf) / 1_000_000f64) as u64;
        // BDP estimate will be zero if no bandwidth samples are available yet.
        if cwnd == 0 {
            return self.init_cwnd;
        }
        self.clamp_cwnd(cwnd.max(self.min_cwnd))
    }

    fn max_cwnd(&self) -> u64 {
        self.config
            .max_window_packets
            .map_or(u64::MAX, |packets| packets.saturating_mul(self.current_mtu))
    }

    fn clamp_cwnd(&self, cwnd: u64) -> u64 {
        cwnd.clamp(self.min_cwnd, self.max_cwnd())
    }

    fn get_probe_rtt_cwnd(&self) -> u64 {
        self.min_cwnd
    }

    fn calculate_pacing_rate(&mut self, bytes_lost: u64) {
        let bw = self.max_bandwidth.get();
        if bw == 0 {
            return;
        }
        let target_rate = (bw as f64 * self.pacing_gain as f64) as u64;
        if self.is_at_full_bandwidth {
            self.pacing_rate = target_rate;
            return;
        }

        // Pace at the rate of initial_window / RTT as soon as RTT measurements are
        // available.
        if self.pacing_rate == 0 && self.min_rtt.as_nanos() != 0 {
            self.pacing_rate = bandwidth_from_delta(self.init_cwnd, self.min_rtt);
            return;
        }

        if self.detect_overshooting {
            self.bytes_lost_while_detecting_overshooting = self
                .bytes_lost_while_detecting_overshooting
                .saturating_add(bytes_lost);
            if self.pacing_rate > target_rate
                && self.bytes_lost_while_detecting_overshooting > 0
                && (self.has_non_app_limited_sample
                    || self
                        .bytes_lost_while_detecting_overshooting
                        .saturating_mul(self.bytes_lost_multiplier_while_detecting_overshooting)
                        > self.init_cwnd)
            {
                let min_rate =
                    bandwidth_from_delta(self.cwnd_to_calculate_min_pacing_rate, self.min_rtt);
                self.pacing_rate = target_rate.max(min_rate);
                self.bytes_lost_while_detecting_overshooting = 0;
                self.detect_overshooting = false;
            }
        }

        // Do not decrease the pacing rate during startup.
        if self.pacing_rate < target_rate {
            self.pacing_rate = target_rate;
        }
    }

    fn calculate_cwnd(&mut self, bytes_acked: u64, excess_acked: u64) {
        if self.mode == Mode::ProbeRtt {
            return;
        }
        let mut target_window = self.get_target_cwnd(self.cwnd_gain);
        if self.is_at_full_bandwidth {
            // Add the max recently measured ack aggregation to CWND.
            target_window += self.ack_aggregation.max_ack_height.get();
        } else if self.enable_ack_aggregation_during_startup {
            // Add the most recent excess acked.  Because CWND never decreases in
            // STARTUP, this will automatically create a very localized max filter.
            target_window += excess_acked;
        }
        // Instead of immediately setting the target CWND as the new one, BBR grows
        // the CWND towards |target_window| by only increasing it |bytes_acked| at a
        // time.
        if self.is_at_full_bandwidth {
            self.cwnd = target_window.min(self.cwnd + bytes_acked);
        } else if self.cwnd < target_window
            || self.acked_bytes < self.init_cwnd
            || (self.config.guarded_startup
                && self.mode == Mode::Startup
                && self.ramp_budget.is_some())
        {
            // If the connection is not yet out of startup phase, do not decrease
            // the window.
            self.cwnd += bytes_acked;
        }

        // Enforce the limits on the congestion window.
        if self.cwnd < self.min_cwnd {
            self.cwnd = self.min_cwnd;
        }
        self.cwnd = self.clamp_cwnd(self.cwnd);
    }

    fn calculate_recovery_window(&mut self, bytes_acked: u64, bytes_lost: u64, in_flight: u64) {
        if !self.recovery_state.in_recovery() {
            return;
        }
        // Set up the initial recovery window.
        if self.recovery_window == 0 {
            self.recovery_window = self
                .min_cwnd
                .max(in_flight + bytes_acked)
                .min(self.max_cwnd());
            return;
        }

        // Remove losses from the recovery window, while accounting for a potential
        // integer underflow.
        if self.recovery_window >= bytes_lost {
            self.recovery_window -= bytes_lost;
        } else {
            // k_max_segment_size = current_mtu
            self.recovery_window = self.current_mtu;
        }
        // In CONSERVATION mode, just subtracting losses is sufficient.  In GROWTH,
        // release additional |bytes_acked| to achieve a slow-start-like behavior.
        if self.recovery_state == RecoveryState::Growth {
            self.recovery_window += bytes_acked;
        }

        // Sanity checks.  Ensure that we always allow to send at least an MSS or
        // |bytes_acked| in response, whichever is larger.
        self.recovery_window = self
            .recovery_window
            .max(in_flight + bytes_acked)
            .max(self.min_cwnd)
            .min(self.max_cwnd());
    }

    fn maybe_exit_guarded_startup(
        &mut self,
        event: &CongestionEvent<'_>,
        sample: &CongestionEventSample,
    ) {
        if !self.config.guarded_startup
            || self.mode != Mode::Startup
            || self.is_at_full_bandwidth
            || self.min_rtt.is_zero()
        {
            return;
        }
        // ACK growth begins only after actual outstanding traffic establishes demand.
        if self.ramp_budget.is_none()
            && sample.bytes_acked > 0
            && event.prior_in_flight >= self.init_cwnd / 2
        {
            let deadline = event
                .now
                .checked_add(self.min_rtt.saturating_mul(8))
                .unwrap_or(event.now);
            self.ramp_budget = Some((deadline, self.round_count));
        }
        let Some((deadline, first_round)) = self.ramp_budget else {
            return;
        };
        let queue_limit = self.min_rtt.saturating_add(self.min_rtt / 8);
        let excessive_loss = self.exit_startup_on_loss
            && self.loss_events_in_round >= K_STARTUP_FULL_LOSS_COUNT
            && self.bytes_lost_in_round
                > (event.prior_in_flight.max(self.init_cwnd) as f64 * K_STARTUP_LOSS_THRESHOLD)
                    as u64;
        let reason = if event.ecn || event.is_persistent_congestion {
            Some("ecn_or_persistent")
        } else if excessive_loss {
            Some("loss")
        } else if sample.sample_rtt.is_some_and(|rtt| rtt > queue_limit)
            && event.rtt.get() > queue_limit
        {
            Some("queue")
        } else if event.now >= deadline || self.round_count.saturating_sub(first_round) >= 8 {
            Some("budget")
        } else {
            None
        };
        if let Some(reason) = reason {
            // Congestion/budget exits do not depend on an app-limited growth sample.
            self.is_at_full_bandwidth = true;
            self.ramp_exit = reason;
            self.ramp_budget = None;
        }
    }

    /// <https://datatracker.ietf.org/doc/html/draft-cardwell-iccrg-bbr-congestion-control#section-4.3.2.2>
    fn check_if_full_bw_reached(&mut self, last_packet_send_state: Option<SendTimeState>) {
        if last_packet_send_state.is_some_and(|state| state.is_app_limited) {
            return;
        }
        let target = (self.bw_at_last_round as f64 * K_STARTUP_GROWTH_TARGET as f64) as u64;
        let bw = self.max_bandwidth.get();
        if bw >= target {
            self.bw_at_last_round = bw;
            self.round_wo_bw_gain = 0;
            if self.expire_ack_aggregation_in_startup {
                self.ack_aggregation.max_ack_height.reset();
            }
            return;
        }

        self.round_wo_bw_gain += 1;
        let excessive_startup_loss = self.exit_startup_on_loss
            && self.loss_events_in_round >= K_STARTUP_FULL_LOSS_COUNT
            && last_packet_send_state.is_some_and(|state| {
                state.bytes_in_flight > 0
                    && self.bytes_lost_in_round
                        > (state.bytes_in_flight as f64 * K_STARTUP_LOSS_THRESHOLD) as u64
            });
        if self.round_wo_bw_gain >= self.startup_rounds || excessive_startup_loss {
            self.is_at_full_bandwidth = true;
        }
    }
}

impl Controller for Bbr {
    fn pacing_enabled(&self) -> bool {
        !(self.config.window_startup && self.mode == Mode::Startup)
    }

    fn diagnostics(&self) -> Option<super::ControllerDiagnostics> {
        Some(super::ControllerDiagnostics {
            ramp_active: self.mode == Mode::Startup && self.ramp_budget.is_some(),
            ramp_exit: self.ramp_exit,
            probe_extensions: self.probe_extensions,
            probe_restarts: self.probe_restarts,
            probe_episodes: self.probe_episode_count,
            probe_aborts: self.probe_aborts,
            probe_cycle_bw: self.probe_cycle_bw,
            phase: match self.mode {
                Mode::Startup => "startup",
                Mode::Drain => "drain",
                Mode::ProbeBw => "probe_bw",
                Mode::ProbeRtt => "probe_rtt",
            },
            recovery: match self.recovery_state {
                RecoveryState::NotInRecovery => "none",
                RecoveryState::Conservation => "conservation",
                RecoveryState::Growth => "growth",
            },
            bandwidth: self.max_bandwidth.get(),
            pacing: self.pacing_rate,
            min_rtt_us: self.min_rtt.as_micros().min(u64::MAX as u128) as u64,
            rounds: self.round_count,
            full_bandwidth: self.is_at_full_bandwidth,
            no_growth_rounds: self.round_wo_bw_gain,
            pacing_gain: self.pacing_gain,
            feedback: self.diagnostic_feedback,
        })
    }

    fn on_packet_sent(&mut self, packet: SentPacketInfo) {
        self.diagnostic_feedback.last_send_limited = packet.app_limited;
        self.max_sent_packet_number = packet.packet_number;
        if packet.bytes_in_flight == 0 {
            self.exiting_quiescence = true;
        }
        self.bandwidth_sampler.on_packet_sent(
            packet.now,
            packet.packet_number,
            packet.bytes,
            packet.bytes_in_flight,
            packet.app_limited,
        );
    }

    fn on_ack_and_loss(&mut self, event: CongestionEvent<'_>) {
        if event.app_limited
            || (!self.config.transport_supply_signal
                && event.prior_in_flight < self.get_target_cwnd(1.0))
        {
            self.bandwidth_sampler.on_app_limited();
        }
        let sample = self.bandwidth_sampler.on_congestion_event(
            event.now,
            event.acked_packets,
            event.lost_packets,
        );
        self.diagnostic_feedback.event_limited = event.app_limited;
        self.diagnostic_feedback.sample_limited = sample.sample_is_app_limited;
        self.diagnostic_feedback.sample_bandwidth = sample.max_bandwidth;
        self.diagnostic_feedback.prior_in_flight = event.prior_in_flight;
        self.diagnostic_feedback.in_flight = event.in_flight;
        self.diagnostic_feedback.acked_bytes = self
            .diagnostic_feedback
            .acked_bytes
            .saturating_add(sample.bytes_acked);
        self.diagnostic_feedback.lost_bytes = self
            .diagnostic_feedback
            .lost_bytes
            .saturating_add(sample.bytes_lost);
        self.loss_state.lost_bytes = sample.bytes_lost;
        if sample.bytes_lost > 0 {
            self.loss_events_in_round = self.loss_events_in_round.saturating_add(1);
            self.bytes_lost_in_round = self.bytes_lost_in_round.saturating_add(sample.bytes_lost);
        }

        let mut is_round_start = false;
        if sample.bytes_acked > 0 {
            if let Some(largest_acked_packet) = event.acked_packets.last() {
                self.max_acked_packet_number = largest_acked_packet.packet_number;
            }
            is_round_start = self
                .current_round_trip_end_packet_number
                .is_none_or(|end| self.max_acked_packet_number > end);
            if is_round_start {
                self.current_round_trip_end_packet_number = Some(self.max_sent_packet_number);
                self.round_count = self.round_count.saturating_add(1);
            }
            self.update_recovery_state(is_round_start);
        }

        if sample.bytes_acked > 0
            && (!sample.sample_is_app_limited || sample.max_bandwidth > self.max_bandwidth.get())
        {
            self.max_bandwidth
                .update_max(self.round_count, sample.max_bandwidth);
        }

        self.has_non_app_limited_sample |=
            sample.max_bandwidth > 0 && !sample.sample_is_app_limited;
        self.acked_bytes = self.acked_bytes.saturating_add(sample.bytes_acked);
        let min_rtt_expired = sample
            .sample_rtt
            .is_some_and(|rtt| self.maybe_update_min_rtt(event.now, rtt));

        let excess_acked = if sample.bytes_acked == 0 {
            0
        } else {
            self.ack_aggregation.update_ack_aggregation_bytes(
                sample.bytes_acked,
                event.now,
                self.round_count,
                self.max_bandwidth.get(),
            )
        };

        if self.mode == Mode::ProbeBw {
            if !self.update_growth_probe(&event, &sample) {
                let extend_growth = self.can_extend_growth_probe(&event, &sample);
                self.update_gain_cycle_phase(event.now, event.prior_in_flight, extend_growth);
            }
        }

        self.maybe_exit_guarded_startup(&event, &sample);
        if is_round_start && !self.is_at_full_bandwidth {
            self.check_if_full_bw_reached(sample.last_packet_send_state);
        }

        if self.config.guarded_startup && self.is_at_full_bandwidth {
            if self.ramp_exit == "none" {
                self.ramp_exit = "model";
            }
            self.ramp_budget = None;
        }
        self.maybe_exit_startup_or_drain(event.now, event.in_flight);

        self.maybe_enter_or_exit_probe_rtt(
            event.now,
            is_round_start,
            event.in_flight,
            min_rtt_expired,
        );
        if self.mode == Mode::ProbeRtt {
            self.bandwidth_sampler.on_app_limited();
        }

        // After the model is updated, recalculate the pacing rate and congestion window.
        self.calculate_pacing_rate(self.loss_state.lost_bytes);
        self.calculate_cwnd(sample.bytes_acked, excess_acked);
        self.calculate_recovery_window(
            sample.bytes_acked,
            self.loss_state.lost_bytes,
            event.in_flight,
        );

        if is_round_start {
            self.loss_events_in_round = 0;
            self.bytes_lost_in_round = 0;
        }
        self.loss_state.reset();
    }

    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        self.loss_state.lost_bytes += lost_bytes;
        if lost_bytes > 0 {
            self.loss_events_in_round = self.loss_events_in_round.saturating_add(1);
            self.bytes_lost_in_round = self.bytes_lost_in_round.saturating_add(lost_bytes);
        }
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.current_mtu = new_mtu as u64;
        self.min_cwnd = calculate_min_window(self.current_mtu);
        self.init_cwnd = self
            .config
            .computed_initial_window(new_mtu)
            .max(self.min_cwnd);
        self.cwnd = self.clamp_cwnd(self.cwnd);
        self.recovery_window = self.recovery_window.min(self.max_cwnd());
    }

    fn on_packet_neutered(&mut self, packet_number: u64) {
        self.bandwidth_sampler.on_packet_neutered(packet_number);
    }

    fn window(&self) -> u64 {
        if self.mode == Mode::ProbeRtt {
            return self.get_probe_rtt_cwnd();
        } else if self.recovery_state.in_recovery() && self.mode != Mode::Startup {
            return self.cwnd.min(self.recovery_window);
        }
        self.cwnd
    }

    fn pacing_rate(&self) -> Option<u64> {
        (self.pacing_rate > 0).then_some(self.pacing_rate)
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.config.computed_initial_window(self.current_mtu as u16)
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Configuration for the [`Bbr`] congestion controller
#[derive(Debug, Clone)]
pub struct BbrConfig {
    initial_window: u64,
    initial_window_packets: Option<u64>,
    max_window_packets: Option<u64>,
    profile: BbrProfile,
    transport_supply_signal: bool,
    window_startup: bool,
    guarded_startup: bool,
    feedback_probe: bool,
}

impl BbrConfig {
    /// Default limit on the amount of outstanding data in bytes.
    ///
    /// Recommended value: `min(10 * max_datagram_size, max(2 * max_datagram_size, 14720))`
    pub fn initial_window(&mut self, value: u64) -> &mut Self {
        self.initial_window = value;
        self.initial_window_packets = None;
        self
    }

    /// Select a Hysteria tuning profile or the adaptive tunnel controller.
    pub fn hysteria_profile(&mut self, profile: BbrProfile) -> &mut Self {
        self.profile = profile;
        let adaptive = profile == BbrProfile::Adaptive;
        self.transport_supply_signal = adaptive;
        self.window_startup = adaptive;
        self.guarded_startup = adaptive;
        self.feedback_probe = adaptive;
        self.initial_window_packets = Some(K_HYSTERIA_INITIAL_WINDOW_PACKETS);
        self.max_window_packets = Some(K_HYSTERIA_MAX_CONGESTION_WINDOW_PACKETS);
        self
    }

    fn computed_initial_window(&self, current_mtu: u16) -> u64 {
        self.initial_window_packets
            .map_or(self.initial_window, |packets| {
                packets.saturating_mul(u64::from(current_mtu))
            })
    }
}

impl Default for BbrConfig {
    fn default() -> Self {
        Self {
            initial_window: K_MAX_INITIAL_CONGESTION_WINDOW * BASE_DATAGRAM_SIZE,
            initial_window_packets: None,
            max_window_packets: None,
            profile: BbrProfile::Quinn,
            transport_supply_signal: false,
            window_startup: false,
            guarded_startup: false,
            feedback_probe: false,
        }
    }
}

/// BBR tuning profiles. Hysteria variants mirror Hysteria 2.10; Adaptive is a local extension.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum BbrProfile {
    /// Preserve Quinn's default BBR behavior.
    #[default]
    Quinn,
    /// Bounded ACK-clocked startup and feedback-driven reprobes for reliable tunnels.
    Adaptive,
    /// Hysteria's conservative profile for unstable or shallow-buffered paths.
    HysteriaConservative,
    /// Hysteria's standard profile.
    HysteriaStandard,
    /// Hysteria's aggressive profile for high-BDP paths.
    HysteriaAggressive,
}

#[derive(Debug, Clone, Copy)]
struct BbrProfileConfig {
    high_gain: f32,
    high_cwnd_gain: f32,
    cwnd_gain_constant: f32,
    startup_rounds: u64,
    exit_startup_on_loss: bool,
    drain_to_target: bool,
    detect_overshooting: bool,
    bytes_lost_multiplier: u64,
    enable_ack_aggregation_during_startup: bool,
    expire_ack_aggregation_in_startup: bool,
}

impl BbrProfile {
    fn config(self) -> BbrProfileConfig {
        match self {
            Self::HysteriaConservative | Self::Adaptive => BbrProfileConfig {
                high_gain: 2.25,
                high_cwnd_gain: 1.75,
                cwnd_gain_constant: 1.75,
                startup_rounds: 2,
                exit_startup_on_loss: true,
                drain_to_target: true,
                detect_overshooting: true,
                bytes_lost_multiplier: 1,
                enable_ack_aggregation_during_startup: false,
                expire_ack_aggregation_in_startup: false,
            },
            Self::HysteriaStandard => BbrProfileConfig {
                high_gain: K_DEFAULT_HIGH_GAIN,
                high_cwnd_gain: K_DERIVED_HIGH_CWNDGAIN,
                cwnd_gain_constant: 2.0,
                startup_rounds: 3,
                exit_startup_on_loss: true,
                drain_to_target: false,
                detect_overshooting: false,
                bytes_lost_multiplier: 2,
                enable_ack_aggregation_during_startup: false,
                expire_ack_aggregation_in_startup: false,
            },
            Self::HysteriaAggressive => BbrProfileConfig {
                high_gain: 3.0,
                high_cwnd_gain: 2.25,
                cwnd_gain_constant: 2.5,
                startup_rounds: 4,
                exit_startup_on_loss: true,
                drain_to_target: false,
                detect_overshooting: false,
                bytes_lost_multiplier: 2,
                enable_ack_aggregation_during_startup: true,
                expire_ack_aggregation_in_startup: true,
            },
            Self::Quinn => BbrProfileConfig {
                high_gain: K_DEFAULT_HIGH_GAIN,
                high_cwnd_gain: K_DEFAULT_HIGH_GAIN,
                cwnd_gain_constant: K_DERIVED_HIGH_CWNDGAIN,
                startup_rounds: K_ROUND_TRIPS_WITHOUT_GROWTH_BEFORE_EXITING_STARTUP as u64,
                exit_startup_on_loss: false,
                drain_to_target: DRAIN_TO_TARGET,
                detect_overshooting: false,
                bytes_lost_multiplier: 2,
                enable_ack_aggregation_during_startup: true,
                expire_ack_aggregation_in_startup: true,
            },
        }
    }
}

impl ControllerFactory for BbrConfig {
    fn build(self: Arc<Self>, _now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(Bbr::new(self, current_mtu))
    }
}

#[derive(Debug, Default, Copy, Clone)]
struct AckAggregationState {
    max_ack_height: MinMax,
    aggregation_epoch_start_time: Option<Instant>,
    aggregation_epoch_bytes: u64,
}

impl AckAggregationState {
    fn update_ack_aggregation_bytes(
        &mut self,
        newly_acked_bytes: u64,
        now: Instant,
        round: u64,
        max_bandwidth: u64,
    ) -> u64 {
        // Compute how many bytes are expected to be delivered, assuming max
        // bandwidth is correct.
        let expected_bytes_acked = max_bandwidth
            * now
                .saturating_duration_since(self.aggregation_epoch_start_time.unwrap_or(now))
                .as_micros() as u64
            / 1_000_000;

        // Reset the current aggregation epoch as soon as the ack arrival rate is
        // less than or equal to the max bandwidth.
        if self.aggregation_epoch_bytes <= expected_bytes_acked {
            // Reset to start measuring a new aggregation epoch.
            self.aggregation_epoch_bytes = newly_acked_bytes;
            self.aggregation_epoch_start_time = Some(now);
            return 0;
        }

        // Compute how many extra bytes were delivered vs max bandwidth.
        // Include the bytes most recently acknowledged to account for stretch acks.
        self.aggregation_epoch_bytes += newly_acked_bytes;
        let diff = self.aggregation_epoch_bytes - expected_bytes_acked;
        self.max_ack_height.update_max(round, diff);
        diff
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Mode {
    // Startup phase of the connection.
    Startup,
    // After achieving the highest possible bandwidth during the startup, lower
    // the pacing rate in order to drain the queue.
    Drain,
    // Cruising mode.
    ProbeBw,
    // Temporarily slow down sending in order to empty the buffer and measure
    // the real minimum RTT.
    ProbeRtt,
}

// Indicates how the congestion control limits the amount of bytes in flight.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RecoveryState {
    // Do not limit.
    NotInRecovery,
    // Allow an extra outstanding byte for each byte acknowledged.
    Conservation,
    // Allow two extra outstanding bytes for each byte acknowledged (slow
    // start).
    Growth,
}

impl RecoveryState {
    pub(super) fn in_recovery(&self) -> bool {
        !matches!(self, Self::NotInRecovery)
    }
}

#[derive(Debug, Clone, Default)]
struct LossState {
    lost_bytes: u64,
}

impl LossState {
    pub(super) fn reset(&mut self) {
        self.lost_bytes = 0;
    }

    pub(super) fn has_losses(&self) -> bool {
        self.lost_bytes != 0
    }
}

fn calculate_min_window(current_mtu: u64) -> u64 {
    4 * current_mtu
}

// The gain used for the STARTUP, equal to 2/ln(2).
const K_DEFAULT_HIGH_GAIN: f32 = 2.885;
// The newly derived CWND gain for STARTUP, 2.
const K_DERIVED_HIGH_CWNDGAIN: f32 = 2.0;
// The cycle of gains used during the ProbeBw stage.
const K_PACING_GAIN: [f32; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];

const K_STARTUP_GROWTH_TARGET: f32 = 1.25;
const K_ROUND_TRIPS_WITHOUT_GROWTH_BEFORE_EXITING_STARTUP: u8 = 3;
const K_STARTUP_FULL_LOSS_COUNT: u64 = 8;
const K_STARTUP_LOSS_THRESHOLD: f64 = 0.02;
const K_HYSTERIA_INITIAL_WINDOW_PACKETS: u64 = 32;
const K_HYSTERIA_MAX_CONGESTION_WINDOW_PACKETS: u64 = 20_000;
const K_BANDWIDTH_WINDOW_SIZE: u64 = 10;
const K_MIN_RTT_EXPIRY: Duration = Duration::from_secs(10);

// Do not allow initial congestion window to be greater than 200 packets.
const K_MAX_INITIAL_CONGESTION_WINDOW: u64 = 200;

const DRAIN_TO_TARGET: bool = true;

#[cfg(test)]
mod tests {
    use super::*;

    fn ack_one_packet(bbr: &mut Bbr, sent_at: Instant, acked_at: Instant, packet_number: u64) {
        let bytes_in_flight = if packet_number == 0 {
            0
        } else {
            BASE_DATAGRAM_SIZE
        };
        bbr.on_packet_sent(super::super::SentPacketInfo {
            now: sent_at,
            packet_number,
            bytes: BASE_DATAGRAM_SIZE,
            bytes_in_flight,
            app_limited: false,
        });
        bbr.on_ack_and_loss(super::super::CongestionEvent {
            now: acked_at,
            prior_in_flight: bytes_in_flight + BASE_DATAGRAM_SIZE,
            in_flight: bytes_in_flight,
            app_limited: false,
            rtt: &RttEstimator::new(Duration::from_millis(333)),
            acked_packets: &[super::super::AckedPacket {
                packet_number,
                sent: sent_at,
                bytes: BASE_DATAGRAM_SIZE,
            }],
            lost_packets: &[],
            ecn: false,
            ecn_sent: None,
            is_persistent_congestion: false,
        });
    }

    #[test]
    fn first_rtt_sample_starts_the_probe_rtt_expiry_clock() {
        let start = Instant::now();
        let mut bbr = Bbr::new(Arc::new(BbrConfig::default()), BASE_DATAGRAM_SIZE as u16);

        ack_one_packet(&mut bbr, start, start + Duration::from_millis(100), 0);
        assert_eq!(bbr.mode, Mode::Startup);
        assert_eq!(bbr.round_count, 1);
        assert_eq!(
            bbr.min_rtt_timestamp,
            Some(start + Duration::from_millis(100))
        );

        ack_one_packet(
            &mut bbr,
            start + Duration::from_secs(9),
            start + Duration::from_millis(9_100),
            1,
        );
        assert_eq!(bbr.mode, Mode::Startup);

        ack_one_packet(
            &mut bbr,
            start + Duration::from_secs(11),
            start + Duration::from_millis(11_100),
            2,
        );
        assert_eq!(bbr.mode, Mode::ProbeRtt);
    }

    #[test]
    fn startup_loss_does_not_enter_recovery_before_full_bandwidth() {
        let start = Instant::now();
        let mut bbr = Bbr::new(Arc::new(BbrConfig::default()), BASE_DATAGRAM_SIZE as u16);
        bbr.on_packet_sent(super::super::SentPacketInfo {
            now: start,
            packet_number: 0,
            bytes: BASE_DATAGRAM_SIZE,
            bytes_in_flight: 0,
            app_limited: false,
        });
        bbr.on_packet_sent(super::super::SentPacketInfo {
            now: start + Duration::from_millis(1),
            packet_number: 1,
            bytes: BASE_DATAGRAM_SIZE,
            bytes_in_flight: BASE_DATAGRAM_SIZE,
            app_limited: false,
        });

        bbr.on_ack_and_loss(super::super::CongestionEvent {
            now: start + Duration::from_millis(100),
            prior_in_flight: 2 * BASE_DATAGRAM_SIZE,
            in_flight: 0,
            app_limited: false,
            rtt: &RttEstimator::new(Duration::from_millis(333)),
            acked_packets: &[super::super::AckedPacket {
                packet_number: 0,
                sent: start,
                bytes: BASE_DATAGRAM_SIZE,
            }],
            lost_packets: &[super::super::LostPacket {
                packet_number: 1,
                sent: start + Duration::from_millis(1),
                bytes: BASE_DATAGRAM_SIZE,
            }],
            ecn: false,
            ecn_sent: None,
            is_persistent_congestion: false,
        });

        assert_eq!(bbr.recovery_state, RecoveryState::NotInRecovery);
        assert_eq!(bbr.bytes_lost_in_round, 0);
    }

    #[test]
    fn startup_loss_uses_inflight_captured_when_the_packet_was_sent() {
        let mut config = BbrConfig::default();
        config.hysteria_profile(BbrProfile::HysteriaStandard);
        let mut bbr = Bbr::new(Arc::new(config), BASE_DATAGRAM_SIZE as u16);
        bbr.bw_at_last_round = 1_000;
        bbr.max_bandwidth.update_max(0, 1_000);
        bbr.loss_events_in_round = K_STARTUP_FULL_LOSS_COUNT;
        bbr.bytes_lost_in_round = 21;
        bbr.check_if_full_bw_reached(Some(SendTimeState {
            packet_number: 9,
            is_app_limited: false,
            total_bytes_sent: 1_000,
            total_bytes_acked: 0,
            bytes_in_flight: 1_000,
        }));

        assert!(bbr.is_at_full_bandwidth);
    }

    #[test]
    fn probe_rtt_round_can_complete_before_the_timer() {
        let start = Instant::now();
        let mut bbr = Bbr::new(Arc::new(BbrConfig::default()), BASE_DATAGRAM_SIZE as u16);
        bbr.mode = Mode::ProbeRtt;
        bbr.is_at_full_bandwidth = true;
        bbr.exit_probe_rtt_at = Some(start + Duration::from_millis(200));
        bbr.probe_rtt_round_passed = false;

        bbr.maybe_enter_or_exit_probe_rtt(start + Duration::from_millis(100), true, 0, false);
        assert_eq!(bbr.mode, Mode::ProbeRtt);
        bbr.maybe_enter_or_exit_probe_rtt(start + Duration::from_millis(250), false, 0, false);

        assert_eq!(bbr.mode, Mode::ProbeBw);
        assert_eq!(bbr.get_probe_rtt_cwnd(), bbr.min_cwnd);
    }

    #[test]
    fn loss_only_event_does_not_reset_ack_aggregation_epoch() {
        let start = Instant::now();
        let mut bbr = Bbr::new(Arc::new(BbrConfig::default()), BASE_DATAGRAM_SIZE as u16);
        bbr.ack_aggregation.aggregation_epoch_start_time = Some(start);
        bbr.ack_aggregation.aggregation_epoch_bytes = 4_000;
        bbr.on_packet_sent(super::super::SentPacketInfo {
            now: start,
            packet_number: 0,
            bytes: BASE_DATAGRAM_SIZE,
            bytes_in_flight: 0,
            app_limited: false,
        });

        bbr.on_ack_and_loss(super::super::CongestionEvent {
            now: start + Duration::from_millis(100),
            prior_in_flight: BASE_DATAGRAM_SIZE,
            in_flight: 0,
            app_limited: false,
            rtt: &RttEstimator::new(Duration::from_millis(100)),
            acked_packets: &[],
            lost_packets: &[super::super::LostPacket {
                packet_number: 0,
                sent: start,
                bytes: BASE_DATAGRAM_SIZE,
            }],
            ecn: false,
            ecn_sent: None,
            is_persistent_congestion: false,
        });

        assert_eq!(
            bbr.ack_aggregation.aggregation_epoch_start_time,
            Some(start)
        );
        assert_eq!(bbr.ack_aggregation.aggregation_epoch_bytes, 4_000);
    }

    #[test]
    fn startup_loss_does_not_end_bandwidth_growth_early() {
        let mut bbr = Bbr::new(Arc::new(BbrConfig::default()), BASE_DATAGRAM_SIZE as u16);
        bbr.bw_at_last_round = 1;
        bbr.round_wo_bw_gain = 1;
        bbr.recovery_state = RecoveryState::Conservation;

        bbr.check_if_full_bw_reached(Some(SendTimeState {
            packet_number: 0,
            is_app_limited: false,
            total_bytes_sent: 0,
            total_bytes_acked: 0,
            bytes_in_flight: BASE_DATAGRAM_SIZE,
        }));

        assert_eq!(bbr.round_wo_bw_gain, 2);
        assert!(!bbr.is_at_full_bandwidth);
    }

    #[test]
    fn hysteria_profiles_match_reference_tuning() {
        let cases = [
            (
                BbrProfile::HysteriaConservative,
                2.25,
                1.75,
                1.75,
                2,
                true,
                false,
            ),
            (
                BbrProfile::HysteriaStandard,
                K_DEFAULT_HIGH_GAIN,
                2.0,
                2.0,
                3,
                false,
                false,
            ),
            (
                BbrProfile::HysteriaAggressive,
                3.0,
                2.25,
                2.5,
                4,
                false,
                true,
            ),
        ];

        for (profile, high_gain, high_cwnd, probe_cwnd, rounds, drain, aggregate) in cases {
            let mut config = BbrConfig::default();
            config.hysteria_profile(profile);
            let bbr = Bbr::new(Arc::new(config), BASE_DATAGRAM_SIZE as u16);
            assert_eq!(bbr.init_cwnd, 32 * BASE_DATAGRAM_SIZE);
            assert_eq!(bbr.high_gain, high_gain);
            assert_eq!(bbr.high_cwnd_gain, high_cwnd);
            assert_eq!(bbr.cwnd_gain_constant, probe_cwnd);
            assert_eq!(bbr.startup_rounds, rounds);
            assert_eq!(bbr.drain_to_target, drain);
            assert_eq!(bbr.enable_ack_aggregation_during_startup, aggregate);
            assert!(bbr.exit_startup_on_loss);
        }
    }

    #[test]
    fn hysteria_initial_window_scales_with_mtu() {
        let mut config = BbrConfig::default();
        config.hysteria_profile(BbrProfile::HysteriaStandard);
        let mut bbr = Bbr::new(Arc::new(config), 1200);

        bbr.on_mtu_update(1400);

        assert_eq!(bbr.init_cwnd, 32 * 1400);
        assert_eq!(bbr.initial_window(), 32 * 1400);
    }

    #[test]
    fn hysteria_cwnd_is_capped_at_reference_packet_limit() {
        let mut config = BbrConfig::default();
        config.hysteria_profile(BbrProfile::HysteriaStandard);
        let mut bbr = Bbr::new(Arc::new(config), 1200);
        bbr.cwnd = u64::MAX;

        bbr.calculate_cwnd(0, 0);
        assert_eq!(bbr.cwnd, K_HYSTERIA_MAX_CONGESTION_WINDOW_PACKETS * 1200);

        bbr.cwnd = u64::MAX;
        bbr.on_mtu_update(1400);
        assert_eq!(bbr.cwnd, K_HYSTERIA_MAX_CONGESTION_WINDOW_PACKETS * 1400);
    }
}

#[cfg(test)]
mod feedback_tests {
    use super::*;
    fn ack_one_packet(bbr: &mut Bbr, sent_at: Instant, acked_at: Instant, packet_number: u64) {
        let bytes_in_flight = if packet_number == 0 {
            0
        } else {
            BASE_DATAGRAM_SIZE
        };
        bbr.on_packet_sent(super::super::SentPacketInfo {
            now: sent_at,
            packet_number,
            bytes: BASE_DATAGRAM_SIZE,
            bytes_in_flight,
            app_limited: false,
        });
        bbr.on_ack_and_loss(super::super::CongestionEvent {
            now: acked_at,
            prior_in_flight: bytes_in_flight + BASE_DATAGRAM_SIZE,
            in_flight: bytes_in_flight,
            app_limited: false,
            rtt: &RttEstimator::new(Duration::from_millis(333)),
            acked_packets: &[super::super::AckedPacket {
                packet_number,
                sent: sent_at,
                bytes: BASE_DATAGRAM_SIZE,
            }],
            lost_packets: &[],
            ecn: false,
            ecn_sent: None,
            is_persistent_congestion: false,
        });
    }
    #[test]
    fn supply_signal_distinguishes_small_flight_from_no_supply() {
        for enabled in [false, true] {
            let mut config = BbrConfig::default();
            config.transport_supply_signal = enabled;
            let mut bbr = Bbr::new(Arc::new(config), 1200);
            let start = Instant::now();
            bbr.min_rtt = Duration::from_millis(200);
            bbr.max_bandwidth.update_max(0, 12_500_000);
            // The old model is much larger than the current flight. A transport
            // with pending data must still provide non-limited samples.
            for n in 0..3 {
                ack_one_packet(
                    &mut bbr,
                    start + Duration::from_millis(n * 210),
                    start + Duration::from_millis(n * 210 + 200),
                    n,
                );
            }
            assert_eq!(bbr.diagnostic_feedback.sample_limited, !enabled);
            // Explicit lack of supply continues to mark subsequent samples.
            bbr.on_packet_sent(SentPacketInfo {
                now: start + Duration::from_secs(1),
                packet_number: 3,
                bytes: BASE_DATAGRAM_SIZE,
                bytes_in_flight: BASE_DATAGRAM_SIZE,
                app_limited: true,
            });
            bbr.on_ack_and_loss(CongestionEvent {
                now: start + Duration::from_millis(1200),
                prior_in_flight: 2 * BASE_DATAGRAM_SIZE,
                in_flight: BASE_DATAGRAM_SIZE,
                app_limited: true,
                rtt: &RttEstimator::new(Duration::from_millis(200)),
                acked_packets: &[super::super::AckedPacket {
                    packet_number: 3,
                    sent: start + Duration::from_secs(1),
                    bytes: BASE_DATAGRAM_SIZE,
                }],
                lost_packets: &[],
                ecn: false,
                ecn_sent: None,
                is_persistent_congestion: false,
            });
            assert!(bbr.diagnostic_feedback.sample_limited);
        }
    }
}

#[cfg(test)]
mod window_startup_tests {
    use super::*;

    #[test]
    fn pacing_bypass_is_opt_in_and_ends_after_startup() {
        for enabled in [false, true] {
            let mut config = BbrConfig::default();
            config.hysteria_profile(BbrProfile::HysteriaConservative);
            config.window_startup = enabled;
            let mut controller = Bbr::new(Arc::new(config), 1200);
            let mut baseline_config = BbrConfig::default();
            baseline_config.hysteria_profile(BbrProfile::HysteriaConservative);
            let mut baseline = Bbr::new(Arc::new(baseline_config), 1200);
            for mode in [Mode::Startup, Mode::Drain, Mode::ProbeBw, Mode::ProbeRtt] {
                controller.mode = mode;
                assert_eq!(
                    controller.pacing_enabled(),
                    !(enabled && mode == Mode::Startup)
                );
                baseline.mode = mode;
                assert_eq!(controller.window(), baseline.window());
            }
        }
        assert!(!BbrConfig::default().window_startup);
    }
}

#[cfg(test)]
mod ack_clock_tests {
    use super::*;

    #[test]
    fn acknowledged_growth_is_opt_in_bounded_and_startup_only() {
        for enabled in [false, true] {
            for mode in [Mode::Startup, Mode::Drain, Mode::ProbeBw] {
                let mut config = BbrConfig::default();
                config.hysteria_profile(BbrProfile::HysteriaConservative);
                config.guarded_startup = enabled;
                let mut bbr = Bbr::new(Arc::new(config), 1200);
                bbr.mode = mode;
                bbr.ramp_budget = Some((Instant::now() + Duration::from_secs(1), 0));
                bbr.cwnd = 2 * bbr.init_cwnd;
                bbr.acked_bytes = bbr.cwnd;
                let before = bbr.cwnd;
                bbr.calculate_cwnd(1200, 0);
                assert_eq!(
                    bbr.cwnd,
                    before
                        + if enabled && mode == Mode::Startup {
                            1200
                        } else {
                            0
                        }
                );
                bbr.cwnd = bbr.max_cwnd() - 600;
                bbr.calculate_cwnd(1200, 0);
                assert!(bbr.cwnd <= bbr.max_cwnd());
                bbr.is_at_full_bandwidth = true;
                bbr.calculate_cwnd(1200, 0);
                assert_eq!(bbr.cwnd, bbr.get_target_cwnd(bbr.cwnd_gain));
            }
        }
        assert!(!BbrConfig::default().guarded_startup);
    }
}

#[cfg(test)]
mod guarded_ramp_tests {
    use super::*;

    fn controller() -> Bbr {
        let mut config = BbrConfig::default();
        config.hysteria_profile(BbrProfile::HysteriaConservative);
        config.hysteria_profile(BbrProfile::Adaptive);
        let mut bbr = Bbr::new(Arc::new(config), 1200);
        bbr.min_rtt = Duration::from_millis(200);
        bbr.cwnd = 2 * bbr.init_cwnd;
        bbr.acked_bytes = bbr.cwnd;
        bbr
    }
    fn event(rtt: &RttEstimator, now: Instant, flight: u64) -> CongestionEvent<'_> {
        CongestionEvent {
            now,
            prior_in_flight: flight,
            in_flight: flight / 2,
            app_limited: true,
            rtt,
            acked_packets: &[],
            lost_packets: &[],
            ecn: false,
            ecn_sent: None,
            is_persistent_congestion: false,
        }
    }
    fn sample() -> CongestionEventSample {
        CongestionEventSample {
            bytes_acked: 1200,
            max_bandwidth: 1_000_000,
            sample_is_app_limited: true,
            sample_rtt: Some(Duration::from_millis(200)),
            ..Default::default()
        }
    }

    #[test]
    fn idle_acks_cannot_start_unbounded_window_growth() {
        let mut bbr = controller();
        let now = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(200));
        let mut event = event(&rtt, now, 1200);
        bbr.maybe_exit_guarded_startup(&event, &sample());
        assert!(bbr.ramp_budget.is_none());
        let before = bbr.cwnd;
        bbr.calculate_cwnd(1200, 0);
        assert_eq!(bbr.cwnd, before);
        event.prior_in_flight = bbr.init_cwnd;
        bbr.maybe_exit_guarded_startup(&event, &sample());
        assert!(bbr.ramp_budget.is_some());
        bbr.calculate_cwnd(1200, 0);
        assert_eq!(bbr.cwnd, before + 1200);
        bbr.cwnd = bbr.max_cwnd() - 600;
        bbr.calculate_cwnd(1200, 0);
        assert_eq!(bbr.cwnd, bbr.max_cwnd());
    }

    #[test]
    fn congestion_exits_survive_app_limited_and_growing_model_feedback() {
        for reason in ["queue", "loss", "ecn_or_persistent"] {
            let mut bbr = controller();
            let now = Instant::now();
            let rtt = RttEstimator::new(Duration::from_millis(if reason == "queue" {
                250
            } else {
                200
            }));
            let mut event = event(&rtt, now, bbr.init_cwnd);
            let mut sample = sample();
            sample.sample_rtt = Some(rtt.get());
            sample.last_packet_send_state = Some(SendTimeState {
                packet_number: 1,
                is_app_limited: true,
                total_bytes_sent: bbr.init_cwnd,
                total_bytes_acked: 1200,
                bytes_in_flight: bbr.init_cwnd,
            });
            bbr.max_bandwidth.update_max(0, 100_000_000);
            if reason == "loss" {
                bbr.loss_events_in_round = K_STARTUP_FULL_LOSS_COUNT;
                bbr.bytes_lost_in_round = bbr.init_cwnd / 10;
            }
            if reason == "ecn_or_persistent" {
                event.ecn = true;
            }
            bbr.maybe_exit_guarded_startup(&event, &sample);
            assert!(bbr.is_at_full_bandwidth, "{reason}");
            assert_eq!(bbr.ramp_exit, reason);
            assert!(bbr.ramp_budget.is_none());
            bbr.check_if_full_bw_reached(sample.last_packet_send_state);
            assert!(bbr.is_at_full_bandwidth);
            bbr.maybe_exit_startup_or_drain(now, event.in_flight);
            assert!(bbr.pacing_enabled());
            assert_ne!(bbr.mode, Mode::Startup);
        }
    }

    #[test]
    fn active_ramp_ends_at_either_frozen_deadline_or_round_budget() {
        for by_round in [false, true] {
            let mut bbr = controller();
            let now = Instant::now();
            let rtt = RttEstimator::new(Duration::from_millis(200));
            let mut event = event(&rtt, now, bbr.init_cwnd);
            bbr.maybe_exit_guarded_startup(&event, &sample());
            let original = bbr.ramp_budget;
            event.now += Duration::from_millis(100);
            bbr.maybe_exit_guarded_startup(&event, &sample());
            assert_eq!(bbr.ramp_budget, original);
            assert!(!bbr.is_at_full_bandwidth);
            if by_round {
                bbr.round_count += 8;
            } else {
                event.now = now + Duration::from_millis(1600);
            }
            bbr.maybe_exit_guarded_startup(&event, &sample());
            assert_eq!(bbr.ramp_exit, "budget");
            assert!(bbr.is_at_full_bandwidth);
        }
    }

    #[test]
    fn guarded_policy_is_opt_in_and_tolerates_subthreshold_loss() {
        let mut bbr = controller();
        let now = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(200));
        let mut event = event(&rtt, now, bbr.init_cwnd);
        bbr.loss_events_in_round = K_STARTUP_FULL_LOSS_COUNT;
        bbr.bytes_lost_in_round = bbr.init_cwnd / 100;
        bbr.maybe_exit_guarded_startup(&event, &sample());
        assert!(!bbr.is_at_full_bandwidth);
        event.is_persistent_congestion = true;
        bbr.maybe_exit_guarded_startup(&event, &sample());
        assert!(bbr.is_at_full_bandwidth);
        let mut default = controller();
        Arc::get_mut(&mut default.config).unwrap().guarded_startup = false;
        default.maybe_exit_guarded_startup(&event, &sample());
        assert!(!default.is_at_full_bandwidth);
        assert!(default.ramp_budget.is_none());
        assert!(!BbrConfig::default().guarded_startup);
    }
}

#[cfg(test)]
mod feedback_probe_tests {
    use super::*;

    fn controller(now: Instant) -> Bbr {
        let mut config = BbrConfig::default();
        config.hysteria_profile(BbrProfile::HysteriaConservative);
        config.feedback_probe = true;
        let mut bbr = Bbr::new(Arc::new(config), 1200);
        bbr.min_rtt = Duration::from_millis(200);
        bbr.mode = Mode::ProbeBw;
        bbr.is_at_full_bandwidth = true;
        bbr.pacing_gain = 1.25;
        bbr.current_cycle_offset = 0;
        bbr.last_cycle_start = Some(now);
        bbr.probe_cycle_bw = 1_000_000;
        bbr.probe_episode = Some((
            now + Duration::from_millis(1600),
            now + Duration::from_millis(3200),
        ));
        bbr.max_bandwidth.update_max(0, 1_200_000);
        bbr.cwnd = 1_000_000;
        bbr
    }
    fn event(rtt: &RttEstimator, now: Instant) -> CongestionEvent<'_> {
        CongestionEvent {
            now,
            prior_in_flight: 600_000,
            in_flight: 590_000,
            app_limited: false,
            rtt,
            acked_packets: &[],
            lost_packets: &[],
            ecn: false,
            ecn_sent: None,
            is_persistent_congestion: false,
        }
    }
    fn sample() -> CongestionEventSample {
        CongestionEventSample {
            bytes_acked: 1200,
            max_bandwidth: 1_200_000,
            sample_is_app_limited: false,
            sample_rtt: Some(Duration::from_millis(200)),
            ..Default::default()
        }
    }

    #[test]
    fn growth_extension_requires_fresh_supply_and_clear_congestion_feedback() {
        let now = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(200));
        let queued = RttEstimator::new(Duration::from_millis(240));
        assert!(
            controller(now)
                .can_extend_growth_probe(&event(&rtt, now + Duration::from_millis(400)), &sample())
        );
        for invalid in [
            "disabled",
            "sample_idle",
            "transport_idle",
            "no_ack",
            "no_growth",
            "no_sample_rtt",
            "zero_sample_rtt",
            "queued_sample",
            "queued_smoothed",
            "sample_loss",
            "round_loss",
            "recovery",
            "ecn",
            "persistent",
            "zero_baseline",
            "zero_min_rtt",
            "wrong_phase",
            "wrong_gain",
        ] {
            let mut bbr = controller(now);
            let mut sample = sample();
            let mut event = event(&rtt, now + Duration::from_millis(400));
            match invalid {
                "disabled" => Arc::get_mut(&mut bbr.config).unwrap().feedback_probe = false,
                "sample_idle" => sample.sample_is_app_limited = true,
                "transport_idle" => event.app_limited = true,
                "no_ack" => sample.bytes_acked = 0,
                "no_growth" => sample.max_bandwidth = 1_050_000,
                "no_sample_rtt" => sample.sample_rtt = None,
                "zero_sample_rtt" => sample.sample_rtt = Some(Duration::ZERO),
                "queued_sample" => sample.sample_rtt = Some(Duration::from_millis(240)),
                "queued_smoothed" => event.rtt = &queued,
                "sample_loss" => sample.bytes_lost = 1200,
                "round_loss" => bbr.bytes_lost_in_round = 1200,
                "recovery" => bbr.recovery_state = RecoveryState::Growth,
                "ecn" => event.ecn = true,
                "persistent" => event.is_persistent_congestion = true,
                "zero_baseline" => bbr.probe_cycle_bw = 0,
                "zero_min_rtt" => bbr.min_rtt = Duration::ZERO,
                "wrong_phase" => bbr.mode = Mode::Drain,
                "wrong_gain" => bbr.pacing_gain = 1.0,
                _ => unreachable!(),
            }
            assert!(!bbr.can_extend_growth_probe(&event, &sample), "{invalid}");
        }
        assert!(!BbrConfig::default().feedback_probe);
    }

    #[test]
    fn extension_preserves_window_and_original_clock_then_returns_to_normal_cycle() {
        let now = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(200));
        let mut event = event(&rtt, now + Duration::from_millis(400));
        let mut bbr = controller(now);
        let before_window = bbr.window();
        let extend = bbr.can_extend_growth_probe(&event, &sample());
        bbr.update_gain_cycle_phase(event.now, event.prior_in_flight, extend);
        assert_eq!(bbr.pacing_gain, 1.25);
        assert_eq!(bbr.probe_extensions, 1);
        assert_eq!(bbr.last_cycle_start, Some(now));
        assert_eq!(bbr.window(), before_window);
        event.now = now + Duration::from_millis(1600);
        let extend = bbr.can_extend_growth_probe(&event, &sample());
        assert!(!extend);
        bbr.update_gain_cycle_phase(event.now, event.prior_in_flight, extend);
        assert_eq!(bbr.pacing_gain, 0.75);
        assert_eq!(bbr.probe_extensions, 1);
        assert_eq!(bbr.probe_cycle_bw, 1_000_000);
        assert_eq!(bbr.last_cycle_start, Some(event.now));
        assert_eq!(bbr.window(), before_window);
        let mut original = controller(now);
        Arc::get_mut(&mut original.config).unwrap().feedback_probe = false;
        event.now = now + Duration::from_millis(400);
        let extend = original.can_extend_growth_probe(&event, &sample());
        original.update_gain_cycle_phase(event.now, event.prior_in_flight, extend);
        assert_eq!(original.pacing_gain, 0.75);
        assert_eq!(original.probe_extensions, 0);
    }

    #[test]
    fn existing_under_target_hold_and_new_phase_baseline_are_preserved() {
        let now = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(200));
        let event = event(&rtt, now + Duration::from_secs(2));
        let mut bbr = controller(now);
        assert!(!bbr.can_extend_growth_probe(&event, &sample()));
        bbr.update_gain_cycle_phase(event.now, 1, false);
        assert_eq!(bbr.pacing_gain, 1.25);
        assert_eq!(bbr.probe_extensions, 0);
        bbr.enter_probe_bandwidth_mode(event.now);
        assert_eq!(bbr.probe_cycle_bw, bbr.max_bandwidth.get());
        assert_eq!(bbr.last_cycle_start, Some(event.now));
    }
}

#[cfg(test)]
mod feedback_reprobe_tests {
    use super::*;

    fn controller(now: Instant, bandwidth: u64) -> Bbr {
        let mut config = BbrConfig::default();
        config.hysteria_profile(BbrProfile::HysteriaConservative);
        config.feedback_probe = true;
        let mut bbr = Bbr::new(Arc::new(config), 1200);
        bbr.min_rtt = Duration::from_millis(200);
        bbr.mode = Mode::ProbeBw;
        bbr.is_at_full_bandwidth = true;
        bbr.pacing_gain = 1.0;
        bbr.current_cycle_offset = 2;
        bbr.last_cycle_start = Some(now);
        bbr.probe_cycle_bw = 1_000_000;
        bbr.max_bandwidth.update_max(0, bandwidth);
        bbr.cwnd = 1_000_000;
        bbr
    }
    fn event(rtt: &RttEstimator, now: Instant) -> CongestionEvent<'_> {
        CongestionEvent {
            now,
            prior_in_flight: 600_000,
            in_flight: 200_000,
            app_limited: false,
            rtt,
            acked_packets: &[],
            lost_packets: &[],
            ecn: false,
            ecn_sent: None,
            is_persistent_congestion: false,
        }
    }
    fn sample(bandwidth: u64) -> CongestionEventSample {
        CongestionEventSample {
            bytes_acked: 1200,
            max_bandwidth: bandwidth,
            sample_is_app_limited: false,
            sample_rtt: Some(Duration::from_millis(200)),
            ..Default::default()
        }
    }

    #[test]
    fn late_growth_feedback_can_restart_after_original_up_phase_ended() {
        let now = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(200));
        let mut bbr = controller(now, 1_000_000);
        bbr.pacing_gain = 1.25;
        bbr.current_cycle_offset = 0;
        let mut event = event(&rtt, now + Duration::from_millis(400));
        assert!(!bbr.update_growth_probe(&event, &sample(1_000_000)));
        bbr.update_gain_cycle_phase(event.now, event.prior_in_flight, false);
        assert_eq!(bbr.pacing_gain, 0.75);
        assert_eq!(bbr.probe_cycle_bw, 1_000_000);
        bbr.max_bandwidth.update_max(1, 1_200_000);
        event.now += Duration::from_millis(50);
        let before_window = bbr.window();
        assert!(bbr.update_growth_probe(&event, &sample(1_200_000)));
        assert_eq!(bbr.pacing_gain, 1.25);
        assert_eq!(bbr.probe_restarts, 1);
        assert_eq!(bbr.probe_cycle_bw, 1_200_000);
        assert_eq!(bbr.window(), before_window);
        assert_eq!(bbr.last_cycle_start, Some(event.now));
    }

    #[test]
    fn restarts_require_drained_flight_and_share_deadline_and_cooldown() {
        let now = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(200));
        let mut bbr = controller(now, 1_200_000);
        let mut event = event(&rtt, now + Duration::from_millis(400));
        event.in_flight = 500_000;
        assert!(!bbr.update_growth_probe(&event, &sample(1_200_000)));
        assert_eq!(bbr.probe_restarts, 0);
        let budget = bbr.probe_episode.unwrap();
        event.now += Duration::from_millis(100);
        event.in_flight = 200_000;
        assert!(bbr.update_growth_probe(&event, &sample(1_200_000)));
        bbr.pacing_gain = 1.0;
        bbr.current_cycle_offset = 2;
        bbr.max_bandwidth.update_max(2, 1_500_000);
        event.now += Duration::from_millis(400);
        assert!(bbr.update_growth_probe(&event, &sample(1_500_000)));
        assert_eq!(bbr.probe_restarts, 2);
        assert_eq!(bbr.probe_episode, Some(budget));
        bbr.pacing_gain = 1.0;
        bbr.max_bandwidth.update_max(3, 1_900_000);
        event.now = budget.0;
        assert!(!bbr.update_growth_probe(&event, &sample(1_900_000)));
        bbr.pacing_gain = 1.25;
        bbr.last_cycle_start = Some(event.now);
        assert!(!bbr.can_extend_growth_probe(&event, &sample(1_900_000)));
        bbr.pacing_gain = 1.0;
        event.now = budget.1;
        assert!(bbr.update_growth_probe(&event, &sample(1_900_000)));
        assert_eq!(bbr.probe_episode_count, 2);
        assert_eq!(bbr.probe_restarts, 3);
    }

    #[test]
    fn congestion_aborts_episode_without_renewing_cooldown_and_default_is_inert() {
        let now = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(200));
        let queued = RttEstimator::new(Duration::from_millis(240));
        for cause in ["queue", "loss", "ecn"] {
            let mut bbr = controller(now, 1_200_000);
            let mut event = event(&rtt, now + Duration::from_millis(400));
            assert!(bbr.update_growth_probe(&event, &sample(1_200_000)));
            let ready = bbr.probe_episode.unwrap().1;
            event.now += Duration::from_millis(100);
            let mut bad = sample(1_500_000);
            match cause {
                "queue" => {
                    event.rtt = &queued;
                    bad.sample_rtt = Some(queued.get());
                }
                "loss" => bad.bytes_lost = 1200,
                "ecn" => event.ecn = true,
                _ => unreachable!(),
            }
            assert!(!bbr.update_growth_probe(&event, &bad));
            assert_eq!(bbr.probe_aborts, 1);
            assert_eq!(bbr.probe_episode, Some((event.now, ready)));
            event.rtt = &rtt;
            event.ecn = false;
            event.now += Duration::from_millis(100);
            bbr.pacing_gain = 1.0;
            bbr.max_bandwidth.update_max(2, 1_500_000);
            assert!(!bbr.update_growth_probe(&event, &sample(1_500_000)));
            assert_eq!(bbr.probe_restarts, 1);
        }
        let mut default = controller(now, 1_200_000);
        Arc::get_mut(&mut default.config).unwrap().feedback_probe = false;
        assert!(!default.update_growth_probe(
            &event(&rtt, now + Duration::from_millis(400)),
            &sample(1_200_000)
        ));
        assert!(default.probe_episode.is_none());
        assert_eq!(default.probe_restarts, 0);
        assert_eq!(default.pacing_gain, 1.0);
    }
}

#[cfg(test)]
mod adaptive_profile_tests {
    use super::*;
    #[test]
    fn profile_selection_enables_bounded_components_and_resets_them() {
        let mut config = BbrConfig::default();
        config.hysteria_profile(BbrProfile::Adaptive);
        assert!(
            config.transport_supply_signal
                && config.window_startup
                && config.guarded_startup
                && config.feedback_probe
        );
        let bbr = Bbr::new(Arc::new(config.clone()), 1200);
        assert_eq!(bbr.initial_window(), 32 * 1200);
        assert_eq!(bbr.high_gain, 2.25);
        assert!(bbr.ramp_budget.is_none());
        for profile in [
            BbrProfile::Quinn,
            BbrProfile::HysteriaConservative,
            BbrProfile::HysteriaStandard,
            BbrProfile::HysteriaAggressive,
        ] {
            config.hysteria_profile(profile);
            assert!(
                !config.transport_supply_signal
                    && !config.window_startup
                    && !config.guarded_startup
                    && !config.feedback_probe
            );
        }
    }
}
