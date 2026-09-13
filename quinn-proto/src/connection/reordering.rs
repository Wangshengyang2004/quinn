use std::{collections::BTreeMap, ops::RangeInclusive};

use crate::{Duration, Instant, TIMER_GRANULARITY};

// Bound per-path metadata independently of bandwidth and peer ACK ranges.
const MAX_RECENT_LOSSES: usize = 1024;
const MAX_LEARNED_THRESHOLD: u64 = 1024;
const MAX_LEARNED_TIME_THRESHOLD: f32 = 1.5;
const TIME_REORDERING_MARGIN: f32 = 1.0 / 32.0;
const QUIET_ROUNDS_BEFORE_DECAY: u32 = 32;

/// Learns only from ACKs proving that a previous loss declaration was spurious.
/// Payload/retransmission state and congestion accounting remain with the caller.
pub(super) struct LossReordering {
    created: Instant,
    recent: BTreeMap<u64, RecentLoss>,
    learned: u64,
    learned_time: f32,
    last_evidence: Instant,
}

struct RecentLoss {
    gap: u64,
    sent: Instant,
    rtt: Duration,
    expires: Instant,
}

impl LossReordering {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            created: now,
            recent: BTreeMap::new(),
            learned: 0,
            learned_time: 1.0,
            last_evidence: now,
        }
    }

    pub(super) fn threshold(&mut self, now: Instant, rtt: Duration, minimum: u32) -> u64 {
        let epoch = rtt.max(TIMER_GRANULARITY) * QUIET_ROUNDS_BEFORE_DECAY;
        let elapsed = now.saturating_duration_since(self.last_evidence);
        let epochs = elapsed.as_nanos() / epoch.as_nanos();
        if epochs != 0 {
            self.learned >>= epochs.min(63) as u32;
            self.learned_time = 1.0 + (self.learned_time - 1.0) / (1u64 << epochs.min(63)) as f32;
            self.last_evidence = now;
        }
        self.learned.max(u64::from(minimum))
    }

    pub(super) fn time_threshold(&mut self, now: Instant, rtt: Duration, minimum: f32) -> f32 {
        // Both thresholds share the same quiet-period decay.
        self.threshold(now, rtt, 0);
        self.learned_time.max(minimum)
    }

    pub(super) fn record_loss(
        &mut self,
        now: Instant,
        sent: Instant,
        pn: u64,
        gap: u64,
        rtt: Duration,
    ) {
        // Do not train a new path from packets transmitted on its predecessor.
        if sent < self.created {
            return;
        }
        self.recent.insert(
            pn,
            RecentLoss {
                gap,
                sent,
                rtt: rtt.max(TIMER_GRANULARITY),
                expires: now + rtt.max(TIMER_GRANULARITY) * 3,
            },
        );
        if self.recent.len() > MAX_RECENT_LOSSES {
            self.recent.pop_first();
        }
    }

    pub(super) fn on_ack(&mut self, now: Instant, range: RangeInclusive<u64>) {
        // Iterate retained entries, never the (potentially enormous) ACK interval.
        let covered: Vec<_> = self.recent.range(range).map(|(&pn, _)| pn).collect();
        for pn in covered {
            let loss = self.recent.remove(&pn).unwrap();
            if now <= loss.expires {
                self.learned = self
                    .learned
                    .max(loss.gap.saturating_add(1).min(MAX_LEARNED_THRESHOLD));
                let observed = now.saturating_duration_since(loss.sent).as_secs_f32()
                    / loss.rtt.as_secs_f32()
                    + TIME_REORDERING_MARGIN;
                self.learned_time = self
                    .learned_time
                    .max(observed.min(MAX_LEARNED_TIME_THRESHOLD));
                self.last_evidence = now;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn learns_only_retained_originals_and_decays_with_quiet_rtts() {
        let now = Instant::now();
        let rtt = Duration::from_millis(100);
        let mut state = LossReordering::new(now);
        state.on_ack(now, 0..=u64::MAX);
        assert_eq!(state.threshold(now, rtt, 3), 3);
        state.record_loss(now, now, 8, 100, rtt);
        assert_eq!(state.threshold(now, rtt, 3), 3);
        state.on_ack(now, 8..=8);
        assert_eq!(state.threshold(now, rtt, 3), 101);
        assert_eq!(state.threshold(now, rtt, 200), 200);
        assert_eq!(state.threshold(now + rtt * 32, rtt, 3), 50);
        assert_eq!(state.threshold(now + rtt * 320, rtt, 3), 3);
    }

    #[test]
    fn old_path_and_expired_losses_cannot_train() {
        let now = Instant::now();
        let rtt = Duration::from_millis(100);
        let mut state = LossReordering::new(now);
        state.record_loss(now, now - rtt, 1, 100, rtt);
        state.record_loss(now, now, 2, 100, rtt);
        state.on_ack(now + rtt * 4, 0..=u64::MAX);
        assert_eq!(state.threshold(now + rtt * 4, rtt, 3), 3);
        assert!(state.recent.is_empty());
    }

    #[test]
    fn storage_and_learning_are_bounded() {
        let now = Instant::now();
        let rtt = Duration::from_millis(100);
        let mut state = LossReordering::new(now);
        for pn in 0..4096 {
            state.record_loss(now, now, pn, u64::MAX, rtt);
            assert!(state.recent.len() <= MAX_RECENT_LOSSES);
        }
        state.on_ack(now, 0..=100);
        assert_eq!(state.threshold(now, rtt, 3), 3);
        state.on_ack(now, 0..=u64::MAX);
        assert_eq!(state.threshold(now, rtt, 3), MAX_LEARNED_THRESHOLD);
        assert!(state.recent.is_empty());
    }
}
