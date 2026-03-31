//! Brutal congestion control algorithm
//!
//! Ported from Hysteria's Brutal CC (<https://github.com/apernet/hysteria>).
//!
//! Brutal is a rate-based congestion controller that maintains a fixed target
//! sending rate regardless of network conditions. It compensates for packet loss
//! by tracking the ACK rate and inflating the congestion window accordingly.
//!
//! **This is NOT a "friendly" congestion controller.** It will aggressively consume
//! bandwidth up to the configured target rate and does not back off on congestion.
//! Use responsibly.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use quinn::congestion::{Controller, ControllerFactory, ControllerMetrics};
use quinn_proto::RttEstimator;

use crate::BASE_DATAGRAM_SIZE;

/// Number of 1-second slots for tracking ACK/loss statistics.
const PKT_INFO_SLOT_COUNT: usize = 5;

/// Minimum number of samples (ack + loss) before adjusting the ACK rate.
const MIN_SAMPLE_COUNT: u64 = 50;

/// Minimum ACK rate. Even under heavy loss, we don't inflate the window
/// beyond `1 / MIN_ACK_RATE` times the target BDP.
const MIN_ACK_RATE: f64 = 0.8;

/// The congestion window is set to `CWND_MULTIPLIER * BDP / ack_rate`.
/// A multiplier of 2 provides headroom for bursty sends and retransmissions.
const CWND_MULTIPLIER: f64 = 2.0;

/// Default initial window when RTT is unknown (bytes).
const DEFAULT_INITIAL_WINDOW: u64 = 10240;

// ---------------------------------------------------------------------------
// Config & Factory
// ---------------------------------------------------------------------------

/// Configuration for the [`Brutal`] congestion controller.
#[derive(Debug, Clone)]
pub struct BrutalConfig {
    /// Target sending rate in **bytes per second**.
    bps: u64,
    /// Initial congestion window (bytes) used before RTT is available.
    initial_window: u64,
}

impl BrutalConfig {
    /// Create a new configuration with the given target rate.
    ///
    /// `bps` is in **bytes per second** (not bits). For example, 12_500_000 ≈ 100 Mbps.
    pub fn new(bps: u64) -> Self {
        Self {
            bps,
            initial_window: DEFAULT_INITIAL_WINDOW,
        }
    }

    /// Override the initial window size.
    pub fn initial_window(&mut self, bytes: u64) -> &mut Self {
        self.initial_window = bytes;
        self
    }
}

impl ControllerFactory for BrutalConfig {
    fn build(self: Arc<Self>, _now: Instant, _current_mtu: u16) -> Box<dyn Controller> {
        Box::new(Brutal::new(self))
    }
}

// ---------------------------------------------------------------------------
// Per-second packet statistics slot
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
struct PktInfoSlot {
    /// Wall-clock second (epoch-relative) this slot belongs to.
    timestamp: i64,
    /// Number of acknowledged packets in this second.
    ack_count: u64,
    /// Number of lost packets in this second.
    loss_count: u64,
}

// ---------------------------------------------------------------------------
// Brutal controller
// ---------------------------------------------------------------------------

/// Brutal congestion controller.
///
/// See [module-level docs](self) for details.
#[derive(Debug, Clone)]
pub struct Brutal {
    config: Arc<BrutalConfig>,
    current_mtu: u64,

    /// Per-second sliding window of ACK/loss counts.
    slots: [PktInfoSlot; PKT_INFO_SLOT_COUNT],

    /// Computed ACK rate (fraction of packets successfully delivered).
    ack_rate: f64,

    /// Last known smoothed RTT, in fractional seconds.
    smoothed_rtt_secs: f64,

    /// Monotonic reference point (first `Instant` we observe).
    /// Used to derive a second-granularity timestamp.
    epoch: Option<Instant>,
}

impl Brutal {
    fn new(config: Arc<BrutalConfig>) -> Self {
        Self {
            config,
            current_mtu: BASE_DATAGRAM_SIZE,
            slots: [PktInfoSlot::default(); PKT_INFO_SLOT_COUNT],
            ack_rate: 1.0,
            smoothed_rtt_secs: 0.0,
            epoch: None,
        }
    }

    /// Convert an `Instant` to a second-granularity timestamp relative to our epoch.
    fn timestamp(&mut self, now: Instant) -> i64 {
        let epoch = *self.epoch.get_or_insert(now);
        now.saturating_duration_since(epoch).as_secs() as i64
    }

    /// Record an ACK for the current second.
    fn record_ack(&mut self, now: Instant) {
        let ts = self.timestamp(now);
        let idx = (ts as usize) % PKT_INFO_SLOT_COUNT;
        let slot = &mut self.slots[idx];
        if slot.timestamp == ts {
            slot.ack_count += 1;
        } else {
            *slot = PktInfoSlot {
                timestamp: ts,
                ack_count: 1,
                loss_count: 0,
            };
        }
    }

    /// Record a loss for the current second.
    fn record_loss(&mut self, now: Instant) {
        let ts = self.timestamp(now);
        let idx = (ts as usize) % PKT_INFO_SLOT_COUNT;
        let slot = &mut self.slots[idx];
        if slot.timestamp == ts {
            slot.loss_count += 1;
        } else {
            *slot = PktInfoSlot {
                timestamp: ts,
                ack_count: 0,
                loss_count: 1,
            };
        }
    }

    /// Recompute `ack_rate` from the sliding window.
    fn update_ack_rate(&mut self, now: Instant) {
        let current_ts = self.timestamp(now);
        let min_ts = current_ts - PKT_INFO_SLOT_COUNT as i64;

        let (mut ack_count, mut loss_count) = (0u64, 0u64);
        for slot in &self.slots {
            if slot.timestamp < min_ts {
                continue;
            }
            ack_count += slot.ack_count;
            loss_count += slot.loss_count;
        }

        let total = ack_count + loss_count;
        if total < MIN_SAMPLE_COUNT {
            // Not enough data; assume perfect delivery.
            self.ack_rate = 1.0;
            return;
        }

        let rate = ack_count as f64 / total as f64;
        self.ack_rate = rate.max(MIN_ACK_RATE);
    }

    /// Compute the congestion window.
    fn cwnd(&self) -> u64 {
        if self.smoothed_rtt_secs <= 0.0 {
            return self.config.initial_window;
        }
        let cwnd = (self.config.bps as f64 * self.smoothed_rtt_secs * CWND_MULTIPLIER
            / self.ack_rate) as u64;
        cwnd.max(self.current_mtu)
    }
}

impl Controller for Brutal {
    fn on_sent(&mut self, _now: Instant, _bytes: u64, _last_packet_number: u64) {
        // No-op: Brutal doesn't track sent packets.
    }

    fn on_ack(
        &mut self,
        now: Instant,
        _sent: Instant,
        _bytes: u64,
        _pn: u64,
        _app_limited: bool,
        rtt: &RttEstimator,
    ) {
        // Update RTT estimate.
        self.smoothed_rtt_secs = rtt.get().as_secs_f64();

        // Count this ACK.
        self.record_ack(now);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        _in_flight: u64,
        _app_limited: bool,
        _largest_packet_num_acked: Option<u64>,
    ) {
        // Recompute ACK rate after each batch of acknowledgements.
        self.update_ack_rate(now);
    }

    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        _is_ecn: bool,
        _lost_bytes: u64,
        _largest_lost: u64,
    ) {
        // Brutal intentionally ignores congestion signals.
        // Loss accounting is done per-packet in `on_packet_lost`.
    }

    fn on_packet_lost(&mut self, _lost_bytes: u16, _packet_number: u64, now: Instant) {
        self.record_loss(now);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.current_mtu = new_mtu as u64;
    }

    fn window(&self) -> u64 {
        self.cwnd()
    }

    fn metrics(&self) -> ControllerMetrics {
        let mut metrics = ControllerMetrics::default();
        metrics.congestion_window = self.cwnd();
        metrics
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.config.initial_window
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn make_brutal(bps: u64) -> Brutal {
        let config = Arc::new(BrutalConfig::new(bps));
        Brutal::new(config)
    }

    /// We can't construct `RttEstimator` directly (its constructor is private),
    /// so we use a helper wrapper that implements the trait methods we need.
    /// In tests, we just feed the RTT through the `on_ack` path.
    fn make_controller_with_rtt(bps: u64, rtt: Duration) -> Brutal {
        let mut b = make_brutal(bps);
        b.smoothed_rtt_secs = rtt.as_secs_f64();
        b
    }

    // -----------------------------------------------------------------------
    // Basic construction & defaults
    // -----------------------------------------------------------------------

    #[test]
    fn initial_state() {
        let b = make_brutal(1_000_000);
        assert_eq!(b.ack_rate, 1.0);
        assert_eq!(b.smoothed_rtt_secs, 0.0);
        assert_eq!(b.config.bps, 1_000_000);
        assert_eq!(b.initial_window(), DEFAULT_INITIAL_WINDOW);
    }

    #[test]
    fn window_returns_initial_when_no_rtt() {
        let b = make_brutal(1_000_000);
        assert_eq!(b.window(), DEFAULT_INITIAL_WINDOW);
    }

    // -----------------------------------------------------------------------
    // Congestion window calculation: cwnd = bps * rtt * 2 / ack_rate
    // -----------------------------------------------------------------------

    #[test]
    fn cwnd_basic_calculation() {
        let mut b = make_brutal(1_000_000); // 1 MB/s
        b.smoothed_rtt_secs = 0.1; // 100ms RTT
        b.ack_rate = 1.0;

        // cwnd = 1_000_000 * 0.1 * 2 / 1.0 = 200_000
        assert_eq!(b.window(), 200_000);
    }

    #[test]
    fn cwnd_compensates_for_loss() {
        let mut b = make_brutal(1_000_000);
        b.smoothed_rtt_secs = 0.1;
        b.ack_rate = 0.8; // 20% loss → ack_rate clamped to 0.8

        // cwnd = 1_000_000 * 0.1 * 2 / 0.8 = 250_000
        assert_eq!(b.window(), 250_000);
    }

    #[test]
    fn cwnd_never_below_mtu() {
        let mut b = make_brutal(100); // very low bps
        b.smoothed_rtt_secs = 0.001; // 1ms RTT

        // cwnd = 100 * 0.001 * 2 / 1.0 = 0.2 → clamped to mtu (1200)
        assert_eq!(b.window(), BASE_DATAGRAM_SIZE);
    }

    // -----------------------------------------------------------------------
    // ACK rate tracking
    // -----------------------------------------------------------------------

    #[test]
    fn ack_rate_stays_at_1_with_insufficient_samples() {
        let mut b = make_brutal(1_000_000);
        let now = Instant::now();

        // Record only 10 ACKs → below MIN_SAMPLE_COUNT (50)
        for i in 0..10 {
            b.record_ack(now + Duration::from_millis(i * 10));
        }
        b.update_ack_rate(now + Duration::from_millis(100));

        assert_eq!(b.ack_rate, 1.0);
    }

    #[test]
    fn ack_rate_computes_correctly_with_enough_samples() {
        let mut b = make_brutal(1_000_000);
        let now = Instant::now();

        // 40 ACKs + 10 losses = 50 total, ack_rate = 40/50 = 0.8
        for _ in 0..40 {
            b.record_ack(now);
        }
        for _ in 0..10 {
            b.record_loss(now);
        }
        b.update_ack_rate(now);

        assert!((b.ack_rate - 0.8).abs() < 1e-10);
    }

    #[test]
    fn ack_rate_clamped_at_min() {
        let mut b = make_brutal(1_000_000);
        let now = Instant::now();

        // 10 ACKs + 90 losses = 100 total, rate = 0.1 → clamped to 0.8
        for _ in 0..10 {
            b.record_ack(now);
        }
        for _ in 0..90 {
            b.record_loss(now);
        }
        b.update_ack_rate(now);

        assert!((b.ack_rate - MIN_ACK_RATE).abs() < 1e-10);
    }

    #[test]
    fn ack_rate_high_delivery() {
        let mut b = make_brutal(1_000_000);
        let now = Instant::now();

        // 95 ACKs + 5 losses = 100 total, ack_rate = 0.95
        for _ in 0..95 {
            b.record_ack(now);
        }
        for _ in 0..5 {
            b.record_loss(now);
        }
        b.update_ack_rate(now);

        assert!((b.ack_rate - 0.95).abs() < 1e-10);
    }

    // -----------------------------------------------------------------------
    // Slot rotation (sliding window expiry)
    // -----------------------------------------------------------------------

    #[test]
    fn old_slots_are_expired() {
        let mut b = make_brutal(1_000_000);
        let now = Instant::now();

        // Fill slot at t=0 with 50 losses, 0 ACKs → rate would be 0 (clamped to 0.8)
        for _ in 0..50 {
            b.record_loss(now);
        }
        b.update_ack_rate(now);
        assert!((b.ack_rate - MIN_ACK_RATE).abs() < 1e-10);

        // Jump forward 6 seconds → old slot should be expired
        let later = now + Duration::from_secs(6);

        // Record 50 ACKs in the new time window
        for _ in 0..50 {
            b.record_ack(later);
        }
        b.update_ack_rate(later);

        // Now ack_rate should be 1.0 (only new ACKs visible)
        assert!((b.ack_rate - 1.0).abs() < 1e-10);
    }

    // -----------------------------------------------------------------------
    // Controller trait integration
    // -----------------------------------------------------------------------

    #[test]
    fn record_ack_increments_slot() {
        let mut b = make_brutal(1_000_000);
        let now = Instant::now();

        b.record_ack(now);
        b.record_ack(now);

        let ts = b.timestamp(now);
        let idx = (ts as usize) % PKT_INFO_SLOT_COUNT;
        assert_eq!(b.slots[idx].ack_count, 2);
    }

    #[test]
    fn on_packet_lost_records_loss() {
        let mut b = make_brutal(1_000_000);
        let now = Instant::now();

        b.on_packet_lost(1200, 1, now);

        let ts = b.timestamp(now);
        let idx = (ts as usize) % PKT_INFO_SLOT_COUNT;
        assert_eq!(b.slots[idx].loss_count, 1);
    }

    #[test]
    fn on_congestion_event_is_noop() {
        let mut b = make_brutal(1_000_000);
        let now = Instant::now();

        // Verify congestion events don't change any state
        let window_before = b.window();
        let rate_before = b.ack_rate;

        b.on_congestion_event(now, now, false, false, 10000, 42);

        assert_eq!(b.window(), window_before);
        assert_eq!(b.ack_rate, rate_before);
    }

    #[test]
    fn mtu_update_changes_minimum_window() {
        let mut b = make_brutal(100);
        b.smoothed_rtt_secs = 0.001;

        // Initially mtu = 1200, cwnd clamped to 1200
        assert_eq!(b.window(), BASE_DATAGRAM_SIZE);

        // Update MTU to 1500
        b.on_mtu_update(1500);
        assert_eq!(b.window(), 1500);
    }

    // -----------------------------------------------------------------------
    // Clone & factory
    // -----------------------------------------------------------------------

    #[test]
    fn clone_box_preserves_state() {
        let mut b = make_brutal(1_000_000);
        b.smoothed_rtt_secs = 0.05;
        b.ack_rate = 0.9;

        let cloned = b.clone_box();
        assert_eq!(cloned.window(), b.window());
    }

    #[test]
    fn factory_builds_controller() {
        let config = Arc::new(BrutalConfig::new(1_000_000));
        let now = Instant::now();
        let controller = ControllerFactory::build(config, now, 1200);

        assert_eq!(controller.initial_window(), DEFAULT_INITIAL_WINDOW);
        assert_eq!(controller.window(), DEFAULT_INITIAL_WINDOW);
    }

    // -----------------------------------------------------------------------
    // End-to-end: simulate a stream of ACKs and losses
    // -----------------------------------------------------------------------

    #[test]
    fn e2e_window_adapts_to_loss() {
        let mut b = make_controller_with_rtt(10_000_000, Duration::from_millis(50));
        let now = Instant::now();

        // Simulate 100 packets: 80 ACKed, 20 lost, all in the same second
        for _ in 0..80 {
            b.record_ack(now);
        }
        for pn in 80..100u64 {
            b.on_packet_lost(1200, pn, now);
        }
        b.on_end_acks(now, 96_000, false, Some(79));

        // ack_rate should be 80/100 = 0.8
        assert!((b.ack_rate - 0.8).abs() < 1e-10);

        // cwnd = 10_000_000 * 0.05 * 2 / 0.8 = 1_250_000
        assert_eq!(b.window(), 1_250_000);
    }

    #[test]
    fn e2e_window_with_perfect_delivery() {
        let mut b = make_controller_with_rtt(10_000_000, Duration::from_millis(50));
        let now = Instant::now();

        for _ in 0..60 {
            b.record_ack(now);
        }
        b.on_end_acks(now, 72_000, false, Some(59));

        // ack_rate = 1.0 (60 samples > 50 min, all ACKs)
        assert!((b.ack_rate - 1.0).abs() < 1e-10);

        // cwnd = 10_000_000 * 0.05 * 2 / 1.0 = 1_000_000
        assert_eq!(b.window(), 1_000_000);
    }

    #[test]
    fn e2e_slots_across_multiple_seconds() {
        let mut b = make_controller_with_rtt(10_000_000, Duration::from_millis(100));
        let start = Instant::now();

        // Second 0: 20 ACKs, 5 losses
        let t0 = start;
        for _ in 0..20 {
            b.record_ack(t0);
        }
        for _ in 0..5 {
            b.record_loss(t0);
        }

        // Second 1: 15 ACKs, 10 losses
        let t1 = start + Duration::from_secs(1);
        for _ in 0..15 {
            b.record_ack(t1);
        }
        for _ in 0..10 {
            b.record_loss(t1);
        }

        b.update_ack_rate(t1);

        // total = 25 + 15 = 50, ack = 35, rate = 35/50 = 0.7 → clamped to 0.8
        assert!((b.ack_rate - MIN_ACK_RATE).abs() < 1e-10);

        // Second 2: 40 ACKs, 0 losses
        let t2 = start + Duration::from_secs(2);
        for _ in 0..40 {
            b.record_ack(t2);
        }
        b.update_ack_rate(t2);

        // total = 25 + 25 + 40 = 90, ack = 75, rate = 75/90 ≈ 0.833
        let expected = 75.0 / 90.0;
        assert!((b.ack_rate - expected).abs() < 1e-10);
    }

    #[test]
    fn into_any_downcast() {
        let b = make_brutal(1_000_000);
        let boxed: Box<dyn Controller> = Box::new(b);
        let any = boxed.into_any();
        assert!(any.downcast::<Brutal>().is_ok());
    }
}
