//! Pacing of packet transmissions.

use crate::{Duration, Instant};

use tracing::warn;

/// A simple token-bucket pacer
///
/// The pacer's capacity is derived on a fraction of the congestion window
/// which can be sent in regular intervals
/// Once the bucket is empty, further transmission is blocked.
/// The bucket refills at a rate slightly faster
/// than one congestion window per RTT, as recommended in
/// <https://tools.ietf.org/html/draft-ietf-quic-recovery-34#section-7.7>
pub(super) struct Pacer {
    capacity: u64,
    last_window: u64,
    last_mtu: u16,
    tokens: u64,
    prev: Instant,
    explicit_rate: Option<RatePacer>,
}

impl Pacer {
    /// Obtains a new [`Pacer`].
    pub(super) fn new(smoothed_rtt: Duration, window: u64, mtu: u16, now: Instant) -> Self {
        let capacity = optimal_capacity(smoothed_rtt, window, mtu);
        Self {
            capacity,
            last_window: window,
            last_mtu: mtu,
            tokens: capacity,
            prev: now,
            explicit_rate: None,
        }
    }

    /// Record that a packet has been transmitted.
    pub(super) fn on_transmit(&mut self, packet_length: u16) {
        self.tokens = self.tokens.saturating_sub(packet_length.into());
        if let Some(pacer) = &mut self.explicit_rate {
            pacer.on_transmit(packet_length);
        }
    }

    /// Pace explicit byte rates independently from congestion-window pacing.
    pub(super) fn delay_with_rate(
        &mut self,
        smoothed_rtt: Duration,
        bytes_to_send: u64,
        mtu: u16,
        window: u64,
        rate: Option<u64>,
        now: Instant,
    ) -> Option<Instant> {
        if let Some(rate) = rate.filter(|rate| *rate > 0) {
            return self
                .explicit_rate
                .get_or_insert_with(|| RatePacer::new(rate, mtu, now))
                .delay(rate, bytes_to_send, mtu, now);
        }
        self.explicit_rate = None;
        self.delay(smoothed_rtt, bytes_to_send, mtu, window, now)
    }

    /// Return how long we need to wait before sending `bytes_to_send`
    ///
    /// If we can send a packet right away, this returns `None`. Otherwise, returns `Some(d)`,
    /// where `d` is the time before this function should be called again.
    ///
    /// The 5/4 ratio used here comes from the suggestion that N = 1.25 in the draft IETF RFC for
    /// QUIC.
    pub(super) fn delay(
        &mut self,
        smoothed_rtt: Duration,
        bytes_to_send: u64,
        mtu: u16,
        window: u64,
        now: Instant,
    ) -> Option<Instant> {
        debug_assert_ne!(
            window, 0,
            "zero-sized congestion control window is nonsense"
        );

        if window != self.last_window || mtu != self.last_mtu {
            self.capacity = optimal_capacity(smoothed_rtt, window, mtu);

            // Clamp the tokens
            self.tokens = self.capacity.min(self.tokens);
            self.last_window = window;
            self.last_mtu = mtu;
        }

        // if we can already send a packet, there is no need for delay
        if self.tokens >= bytes_to_send {
            return None;
        }

        // we disable pacing for extremely large windows
        if window > u64::from(u32::MAX) {
            return None;
        }

        let window = window as u32;

        let time_elapsed = now.checked_duration_since(self.prev).unwrap_or_else(|| {
            warn!("received a timestamp early than a previous recorded time, ignoring");
            Default::default()
        });

        if smoothed_rtt.as_nanos() == 0 {
            return None;
        }

        let elapsed_rtts = time_elapsed.as_secs_f64() / smoothed_rtt.as_secs_f64();
        let new_tokens = window as f64 * 1.25 * elapsed_rtts;
        self.tokens = self
            .tokens
            .saturating_add(new_tokens as _)
            .min(self.capacity);

        self.prev = now;

        // if we can already send a packet, there is no need for delay
        if self.tokens >= bytes_to_send {
            return None;
        }

        let unscaled_delay = smoothed_rtt
            .checked_mul((bytes_to_send.max(self.capacity) - self.tokens) as _)
            .unwrap_or(Duration::MAX)
            / window;

        // divisions come before multiplications to prevent overflow
        // this is the time at which the pacing window becomes empty
        Some(self.prev + (unscaled_delay / 5) * 4)
    }
}

/// Token bucket for controllers that specify a byte rate directly.
///
/// Keep fractional-byte credit and a bounded amount of wakeup slack. Refilling
/// only a nominal burst after a late timer would permanently discard bandwidth
/// and feed an artificially low delivery rate back to the controller.
struct RatePacer {
    credit: u128,
    rate: u64,
    capacity: u128,
    updated_at: Instant,
}

impl RatePacer {
    const BYTE_SCALE: u128 = 1_000_000_000;
    const WAKEUP_SLACK_NANOS: u128 = 4_000_000;
    const MIN_WAKEUP: Duration = Duration::from_micros(200);

    fn capacity(rate: u64, mtu: u16) -> u128 {
        (u128::from(rate) * Self::WAKEUP_SLACK_NANOS)
            .max(u128::from(mtu) * u128::from(MIN_BURST_SIZE) * Self::BYTE_SCALE)
    }

    fn new(rate: u64, mtu: u16, now: Instant) -> Self {
        let capacity = Self::capacity(rate, mtu);
        Self {
            credit: capacity,
            rate,
            capacity,
            updated_at: now,
        }
    }

    fn on_transmit(&mut self, bytes: u16) {
        self.credit = self
            .credit
            .saturating_sub(u128::from(bytes) * Self::BYTE_SCALE);
    }

    fn delay(&mut self, rate: u64, bytes: u64, mtu: u16, now: Instant) -> Option<Instant> {
        debug_assert!(rate > 0);
        // Account elapsed time at the previous rate before applying a new rate.
        // A backwards clock must not grant credit or rewind the refill origin.
        let elapsed = now.saturating_duration_since(self.updated_at);
        self.credit = self
            .credit
            .saturating_add(elapsed.as_nanos().saturating_mul(u128::from(self.rate)))
            .min(self.capacity);
        self.updated_at = now.max(self.updated_at);
        self.rate = rate;
        self.capacity = Self::capacity(rate, mtu);
        self.credit = self.credit.min(self.capacity);

        let needed = u128::from(bytes) * Self::BYTE_SCALE;
        if self.credit >= needed {
            return None;
        }
        let nanos = (needed - self.credit).div_ceil(u128::from(rate));
        let wait =
            Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64).max(Self::MIN_WAKEUP);
        // For an unrepresentable deadline, recheck credit conservatively later.
        Some(
            self.updated_at
                .checked_add(wait)
                .unwrap_or_else(|| self.updated_at + Duration::from_secs(1)),
        )
    }
}

/// Calculates a pacer capacity for a certain window and RTT
///
/// The goal is to emit a burst (of size `capacity`) in timer intervals
/// which compromise between
/// - ideally distributing datagrams over time
/// - constantly waking up the connection to produce additional datagrams
///
/// Too short burst intervals means we will never meet them since the timer
/// accuracy in user-space is not high enough. If we miss the interval by more
/// than 25%, we will lose that part of the congestion window since no additional
/// tokens for the extra-elapsed time can be stored.
///
/// Too long burst intervals make pacing less effective.
fn optimal_capacity(smoothed_rtt: Duration, window: u64, mtu: u16) -> u64 {
    let rtt = smoothed_rtt.as_nanos().max(1);

    let capacity = ((window as u128 * BURST_INTERVAL_NANOS) / rtt) as u64;

    // Small bursts are less efficient (no GSO), could increase latency and don't effectively
    // use the channel's buffer capacity. Large bursts might block the connection on sending.
    capacity.clamp(MIN_BURST_SIZE * mtu as u64, MAX_BURST_SIZE * mtu as u64)
}

/// The burst interval
///
/// The capacity will we refilled in 4/5 of that time.
/// 2ms is chosen here since framework timers might have 1ms precision.
/// If kernel-level pacing is supported later a higher time here might be
/// more applicable.
const BURST_INTERVAL_NANOS: u128 = 2_000_000; // 2ms

/// Allows some usage of GSO, and doesn't slow down the handshake.
const MIN_BURST_SIZE: u64 = 10;

/// Creating 256 packets took 1ms in a benchmark, so larger bursts don't make sense.
const MAX_BURST_SIZE: u64 = 256;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn does_not_panic_on_bad_instant() {
        let old_instant = Instant::now();
        let new_instant = old_instant + Duration::from_micros(15);
        let rtt = Duration::from_micros(400);

        assert!(
            Pacer::new(rtt, 30000, 1500, new_instant)
                .delay(Duration::from_micros(0), 0, 1500, 1, old_instant)
                .is_none()
        );
        assert!(
            Pacer::new(rtt, 30000, 1500, new_instant)
                .delay(Duration::from_micros(0), 1600, 1500, 1, old_instant)
                .is_none()
        );
        assert!(
            Pacer::new(rtt, 30000, 1500, new_instant)
                .delay(Duration::from_micros(0), 1500, 1500, 3000, old_instant)
                .is_none()
        );
    }

    #[test]
    fn derives_initial_capacity() {
        let window = 2_000_000;
        let mtu = 1500;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();

        let pacer = Pacer::new(rtt, window, mtu, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, pacer.capacity);

        let pacer = Pacer::new(Duration::from_millis(0), window, mtu, now);
        assert_eq!(pacer.capacity, MAX_BURST_SIZE * mtu as u64);
        assert_eq!(pacer.tokens, pacer.capacity);

        let pacer = Pacer::new(rtt, 1, mtu, now);
        assert_eq!(pacer.capacity, MIN_BURST_SIZE * mtu as u64);
        assert_eq!(pacer.tokens, pacer.capacity);
    }

    #[test]
    fn adjusts_capacity() {
        let window = 2_000_000;
        let mtu = 1500;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();

        let mut pacer = Pacer::new(rtt, window, mtu, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, pacer.capacity);
        let initial_tokens = pacer.tokens;

        pacer.delay(rtt, mtu as u64, mtu, window * 2, now);
        assert_eq!(
            pacer.capacity,
            (2 * window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, initial_tokens);

        pacer.delay(rtt, mtu as u64, mtu, window / 2, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 / 2 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, initial_tokens / 2);

        pacer.delay(rtt, mtu as u64, mtu * 2, window, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );

        pacer.delay(rtt, mtu as u64, 20_000, window, now);
        assert_eq!(pacer.capacity, 20_000_u64 * MIN_BURST_SIZE);
    }

    #[test]
    fn computes_pause_correctly() {
        let window = 2_000_000u64;
        let mtu = 1000;
        let rtt = Duration::from_millis(50);
        let old_instant = Instant::now();

        let mut pacer = Pacer::new(rtt, window, mtu, old_instant);
        let packet_capacity = pacer.capacity / mtu as u64;

        for _ in 0..packet_capacity {
            assert_eq!(
                pacer.delay(rtt, mtu as u64, mtu, window, old_instant),
                None,
                "When capacity is available packets should be sent immediately"
            );

            pacer.on_transmit(mtu);
        }

        let pace_duration = Duration::from_nanos((BURST_INTERVAL_NANOS * 4 / 5) as u64);

        assert_eq!(
            pacer
                .delay(rtt, mtu as u64, mtu, window, old_instant)
                .expect("Send must be delayed")
                .duration_since(old_instant),
            pace_duration
        );

        // Refill half of the tokens
        assert_eq!(
            pacer.delay(
                rtt,
                mtu as u64,
                mtu,
                window,
                old_instant + pace_duration / 2
            ),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity / 2);

        for _ in 0..packet_capacity / 2 {
            assert_eq!(
                pacer.delay(rtt, mtu as u64, mtu, window, old_instant),
                None,
                "When capacity is available packets should be sent immediately"
            );

            pacer.on_transmit(mtu);
        }

        // Refill all capacity by waiting more than the expected duration
        assert_eq!(
            pacer.delay(
                rtt,
                mtu as u64,
                mtu,
                window,
                old_instant + pace_duration * 3 / 2
            ),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity);
    }
}

#[cfg(test)]
mod explicit_rate_tests {
    use super::*;

    fn delivered_mbps(rate_mbps: u64, quantum_us: u64, late_us: u64) -> f64 {
        let start = Instant::now();
        let rtt = Duration::from_millis(60);
        let mut now = start;
        let mut p = Pacer::new(rtt, 1_000_000, 1200, start);
        let mut sent = 0u64;
        let end = start + Duration::from_secs(10);
        let rate = rate_mbps * 1_000_000 / 8;
        let mut iterations = 0;
        while now < end {
            iterations += 1;
            assert!(iterations < 5_000_000);
            if let Some(next) = p.delay_with_rate(rtt, 1200, 1200, 1_000_000, Some(rate), now) {
                let nanos = next.saturating_duration_since(start).as_nanos() as u64;
                let quantum = quantum_us * 1000;
                let rounded = if quantum > 0 {
                    nanos.div_ceil(quantum) * quantum
                } else {
                    nanos
                };
                now = (start + Duration::from_nanos(rounded) + Duration::from_micros(late_us))
                    .max(now + Duration::from_nanos(1));
            } else {
                p.on_transmit(1200);
                sent += 1200;
            }
        }
        sent as f64 * 8.0 / 10.0 / 1_000_000.0
    }

    #[test]
    fn explicit_rate_survives_millisecond_timer_quantization() {
        let actual = delivered_mbps(100, 1000, 0);
        assert!(
            (95.0..=101.0).contains(&actual),
            "100 Mbps target delivered {actual} Mbps"
        );
    }

    #[test]
    fn explicit_rate_recovers_one_millisecond_late_wakeups() {
        let actual = delivered_mbps(100, 1000, 1000);
        assert!(
            (95.0..=101.0).contains(&actual),
            "100 Mbps target delivered {actual} Mbps"
        );
    }

    #[test]
    fn explicit_rates_track_targets_with_ideal_and_late_timers() {
        for rate in [10, 20, 50, 100, 1000] {
            for (quantum, late) in [(0, 0), (1000, 0), (1000, 1000)] {
                let actual = delivered_mbps(rate, quantum, late);
                let ratio = actual / rate as f64;
                assert!(
                    (0.99..=1.01).contains(&ratio),
                    "rate={rate}, quantum={quantum}, late={late}, delivered={actual}"
                );
            }
        }
    }

    #[test]
    fn absent_rate_preserves_window_pacing_decisions() {
        let start = Instant::now();
        let rtt = Duration::from_millis(60);
        let mut legacy = Pacer::new(rtt, 200_000, 1200, start);
        let mut mixed = Pacer::new(rtt, 200_000, 1200, start);
        for i in 0..10_000 {
            let now = start + Duration::from_micros(i * 10);
            let a = legacy.delay(rtt, 1200, 1200, 200_000, now);
            let b = mixed.delay_with_rate(rtt, 1200, 1200, 200_000, None, now);
            assert_eq!(a, b);
            if a.is_none() {
                legacy.on_transmit(1200);
                mixed.on_transmit(1200);
            }
        }
    }
}

#[cfg(test)]
mod rate_credit_tests {
    use super::*;

    #[test]
    fn idle_credit_is_bounded_and_rate_changes_do_not_mint_credit() {
        let now = Instant::now();
        let mut p = RatePacer::new(12_500_000, 1200, now);
        p.delay(12_500_000, 1200, 1200, now + Duration::from_secs(60));
        assert_eq!(p.credit, 50_000 * RatePacer::BYTE_SCALE);
        p.on_transmit(1200);
        let remaining = p.credit;
        p.delay(125_000_000, 1200, 1200, now + Duration::from_secs(60));
        assert_eq!(p.credit, remaining);
        p.delay(1_250_000, 1200, 1200, now + Duration::from_secs(60));
        assert_eq!(p.credit, 12_000 * RatePacer::BYTE_SCALE);
    }

    #[test]
    fn elapsed_time_uses_old_rate_and_preserves_fractional_bytes() {
        let now = Instant::now();
        let mut p = RatePacer::new(1, 1200, now);
        for _ in 0..10 {
            p.on_transmit(1200);
        }
        p.delay(1, 1200, 1200, now + Duration::from_millis(500));
        assert_eq!(p.credit, RatePacer::BYTE_SCALE / 2);
        p.delay(100, 1200, 1200, now + Duration::from_secs(1));
        assert_eq!(p.credit, RatePacer::BYTE_SCALE);
        p.delay(100, 1200, 1200, now + Duration::from_secs(2));
        assert_eq!(p.credit, 101 * RatePacer::BYTE_SCALE);
    }

    #[test]
    fn backwards_clock_does_not_grant_credit_twice() {
        let now = Instant::now();
        let mut p = RatePacer::new(1, 1200, now);
        for _ in 0..10 {
            p.on_transmit(1200);
        }
        p.delay(1, 1200, 1200, now + Duration::from_secs(1));
        assert_eq!(p.credit, RatePacer::BYTE_SCALE);
        p.delay(1, 1200, 1200, now);
        p.delay(1, 1200, 1200, now + Duration::from_secs(1));
        assert_eq!(p.credit, RatePacer::BYTE_SCALE);
    }

    #[test]
    fn mtu_growth_does_not_grant_new_credit() {
        let now = Instant::now();
        let mut p = RatePacer::new(1, 1200, now);
        for _ in 0..10 {
            p.on_transmit(1200);
        }
        assert!(p.delay(1, 1500, 1500, now).is_some());
        assert_eq!(p.credit, 0);
        assert_eq!(p.capacity, 15_000 * RatePacer::BYTE_SCALE);
    }
}
