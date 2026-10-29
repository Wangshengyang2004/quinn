//! Opt-in receive-window growth matching apernet/quic-go a10df75c260c.
//!
//! Growth is evaluated when issuing flow-control credit, after more than half
//! a window has been consumed. A fast reader doubles the window up to its cap;
//! a slow reader, an unread stream, and a connection without an RTT sample do
//! not grow. Ordinary Quinn connections retain their fixed windows.

use crate::{Duration, Instant};

#[derive(Debug)]
pub(super) struct AutoTune {
    pub(super) window: u64,
    maximum: u64,
    epoch: Option<Instant>,
    epoch_read: u64,
}

impl AutoTune {
    pub(super) fn new(window: u64, maximum: u64) -> Self {
        Self {
            window,
            maximum: maximum.max(window),
            epoch: None,
            epoch_read: 0,
        }
    }

    pub(super) fn start(&mut self, now: Instant) {
        self.epoch.get_or_insert(now);
    }

    pub(super) fn grow(&mut self, read: u64, now: Instant, rtt: Option<Duration>) -> bool {
        let consumed = read.saturating_sub(self.epoch_read);
        if consumed <= self.window / 2 || self.window == 0 {
            return false;
        }
        let (Some(epoch), Some(rtt)) = (self.epoch, rtt.filter(|rtt| !rtt.is_zero())) else {
            return false;
        };
        let fraction = consumed as f64 / self.window as f64;
        // quic-go converts the floating-point nanosecond threshold back to a
        // duration before comparison. Comparing floating-point seconds instead
        // can incorrectly grow at the exact boundary (e.g. 3 * 0.1 seconds).
        let threshold_nanos = (4.0 * fraction * rtt.as_nanos() as f64) as u128;
        let fast = now.saturating_duration_since(epoch).as_nanos() < threshold_nanos;
        let old = self.window;
        if fast {
            self.window = self.window.saturating_mul(2).min(self.maximum);
        }
        self.epoch = Some(now);
        self.epoch_read = read;
        self.window > old
    }

    pub(super) fn ensure_minimum(&mut self, minimum: u64, read: u64, now: Instant) -> bool {
        if minimum <= self.window {
            return false;
        }
        let old = self.window;
        self.window = minimum.min(self.maximum);
        self.epoch = Some(now);
        self.epoch_read = read;
        self.window > old
    }

    pub(super) fn set_window(&mut self, window: u64, read: u64) {
        if window != self.window {
            self.window = window;
            self.maximum = self.maximum.max(window);
            self.epoch = None;
            self.epoch_read = read;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growth_requires_consumption_and_measured_rtt() {
        let now = Instant::now();
        let mut tune = AutoTune::new(600, 800);
        tune.start(now);
        let rtt = Some(Duration::from_millis(100));
        assert!(!tune.grow(300, now, rtt)); // Exactly half is insufficient.
        assert!(!tune.grow(301, now, None));
        assert!(!tune.grow(301, now, Some(Duration::ZERO)));
        assert!(tune.grow(301, now + Duration::from_millis(100), rtt));
        assert_eq!(tune.window, 800); // Cap, not an unconditional doubling.
        assert!(!tune.grow(702, now + Duration::from_millis(110), rtt));
        assert_eq!(tune.window, 800);
    }

    #[test]
    fn slow_epoch_resets_and_later_fast_reader_grows() {
        let now = Instant::now();
        let mut tune = AutoTune::new(1000, 4000);
        tune.start(now);
        let rtt = Some(Duration::from_millis(100));
        // 750/1000 consumed: the strict threshold is 4 * .75 * 100 = 300 ms.
        assert!(!tune.grow(750, now + Duration::from_millis(300), rtt));
        assert_eq!(tune.window, 1000);
        assert!(!tune.grow(1250, now + Duration::from_millis(301), rtt));
        assert!(tune.grow(1500, now + Duration::from_millis(400), rtt));
        assert_eq!(tune.window, 2000);
    }

    #[test]
    fn stream_growth_raises_connection_minimum_within_cap() {
        let now = Instant::now();
        let mut tune = AutoTune::new(1000, 2000);
        assert!(tune.ensure_minimum(1500, 100, now));
        assert_eq!(tune.window, 1500);
        assert!(tune.ensure_minimum(3000, 200, now));
        assert_eq!(tune.window, 2000);
        assert!(!tune.ensure_minimum(3000, 300, now));
    }
}
