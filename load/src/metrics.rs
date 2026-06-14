use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Thread-safe latency recorder (nanoseconds), reported in microseconds.
pub struct Latency {
    hist: Mutex<Histogram<u64>>,
}

impl Default for Latency {
    fn default() -> Self {
        // 1µs..60s, 3 significant figures.
        Self { hist: Mutex::new(Histogram::<u64>::new_with_bounds(1_000, 60_000_000_000, 3).unwrap()) }
    }
}

impl Latency {
    pub fn record_nanos(&self, nanos: u64) {
        let mut h = self.hist.lock().unwrap();
        let _ = h.saturating_record(nanos);
    }
    /// (count, p50_us, p99_us, p999_us, max_us)
    pub fn summary_us(&self) -> (u64, u64, u64, u64, u64) {
        let h = self.hist.lock().unwrap();
        (
            h.len(),
            h.value_at_quantile(0.50) / 1000,
            h.value_at_quantile(0.99) / 1000,
            h.value_at_quantile(0.999) / 1000,
            h.max() / 1000,
        )
    }
}

#[derive(Default)]
pub struct Counters {
    pub connected: AtomicU64,
    pub connect_failed: AtomicU64,
    pub subscribed: AtomicU64,
    pub sent: AtomicU64,
    pub received: AtomicU64,
}

impl Counters {
    pub fn inc(field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_quantiles() {
        let l = Latency::default();
        for _ in 0..1000 {
            l.record_nanos(2_000_000); // 2ms
        }
        let (count, p50, _p99, _p999, max) = l.summary_us();
        assert_eq!(count, 1000);
        assert!((1900..=2100).contains(&p50), "p50={p50}");
        assert!(max >= 1900);
    }

    #[test]
    fn counters_count() {
        let c = Counters::default();
        Counters::inc(&c.received);
        Counters::inc(&c.received);
        assert_eq!(c.received.load(Ordering::Relaxed), 2);
    }
}
