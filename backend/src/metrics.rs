use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// In-process counters for `/internal/stats`.
///
/// Deliberately bare: atomics and a start time, nothing that needs a
/// background task, its own lock discipline, or a new dependency. Counts are
/// per-process and reset on restart, which is all a lightweight ops endpoint
/// needs; durable aggregates live in the database.
#[derive(Debug)]
pub struct Metrics {
    started_at: Instant,
    report_cache_hits: AtomicU64,
    report_cache_misses: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            report_cache_hits: AtomicU64::new(0),
            report_cache_misses: AtomicU64::new(0),
        }
    }

    pub fn record_cache_hit(&self) {
        self.report_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.report_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn report_cache_hits(&self) -> u64 {
        self.report_cache_hits.load(Ordering::Relaxed)
    }

    pub fn report_cache_misses(&self) -> u64 {
        self.report_cache_misses.load(Ordering::Relaxed)
    }

    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_counters_accumulate() {
        let metrics = Metrics::new();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();
        assert_eq!(metrics.report_cache_hits(), 2);
        assert_eq!(metrics.report_cache_misses(), 1);
    }

    /// Sleeps rather than asserting straight off `new()`, because `uptime() >
    /// Duration::ZERO` on a freshly built `Metrics` is a race against the
    /// platform clock's tick, not a test of this code.
    ///
    /// `Instant` on macOS/arm64 is `CLOCK_UPTIME_RAW`, which advances in
    /// ~41.7ns steps. Measured on an M-series host: two adjacent
    /// `Instant::now()`/`elapsed()` reads return *exactly* zero 41% of the time
    /// idle and 50% under load, so the old assertion failed 4 of 12 full-suite
    /// runs locally while passing 400 of 400 on its own (alone, the debug-build
    /// work between the two reads is slow enough to always cross a tick; with
    /// every core boosted by 160-odd parallel tests it often isn't). Linux CI
    /// never saw it: there `Instant` is nanosecond-resolution `CLOCK_MONOTONIC`.
    ///
    /// So: sleep an interval `thread::sleep` guarantees to be a floor, and
    /// assert a bound well under it. The margin is deliberate slack against
    /// clock coarseness, and there is deliberately no upper bound — a loaded
    /// or suspended machine may report far more uptime than it slept, and that
    /// is not a bug in a counter that only feeds `/internal/stats`.
    #[test]
    fn uptime_advances_with_wall_clock() {
        let metrics = Metrics::new();
        std::thread::sleep(Duration::from_millis(5));
        assert!(metrics.uptime() >= Duration::from_millis(1));
    }
}
