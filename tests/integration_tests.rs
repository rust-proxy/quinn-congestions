//! Integration tests for quinn-congestions
//!
//! These tests verify the congestion control algorithms work correctly
//! when integrated with the Quinn ecosystem.

use std::{sync::Arc, time::Instant};

use quinn::congestion::{Controller, ControllerFactory};
use quinn_congestions::brutal::BrutalConfig;

/// Test that BrutalConfig can be cloned and produces identical controllers
#[test]
fn brutal_config_clone_produces_identical_controllers() {
	let config = Arc::new(BrutalConfig::new(1_000_000));
	let config2 = Arc::new(BrutalConfig::new(1_000_000));

	let now = Instant::now();
	let c1 = ControllerFactory::build(config.clone(), now, 1200);
	let c2 = ControllerFactory::build(config2.clone(), now, 1200);

	assert_eq!(c1.initial_window(), c2.initial_window());
	assert_eq!(c1.window(), c2.window());
}

/// Test Brutal congestion window scaling with different MTU values
#[test]
fn brutal_window_affected_by_mtu() {
	let config = Arc::new(BrutalConfig::new(10_000_000));
	let now = Instant::now();

	// Create controller with default MTU (1200)
	let mut c1 = ControllerFactory::build(config.clone(), now, 1200);

	// Create controller with larger MTU (9000)
	let mut c2 = ControllerFactory::build(config.clone(), now, 9000);

	// Update MTU on first controller to match second
	c1.on_mtu_update(9000);

	// Both should now report same minimum window based on MTU
	// (Note: window calculation also depends on RTT which is initially 0)
	assert_eq!(c1.window(), c2.window());
}

/// Test that Brutal ignores congestion events (as designed)
#[test]
fn brutal_ignores_congestion_events() {
	let config = Arc::new(BrutalConfig::new(1_000_000));
	let now = Instant::now();

	let mut controller = ControllerFactory::build(config, now, 1200);

	// Get initial window
	let initial_window = controller.window();

	// Trigger multiple congestion events
	for i in 0..10 {
		controller.on_congestion_event(
			now,      // now
			now,      // sent
			true,     // persistent congestion
			false,    // not ECN
			10000,    // lost bytes
			i as u64, // largest lost packet
		);
	}

	// Window should remain unchanged (Brutal ignores congestion events)
	assert_eq!(controller.window(), initial_window);
}

/// Test various bandwidth configurations
#[test]
fn brutal_handles_various_bandwidth_configs() {
	let test_cases = vec![
		1_000_000,   // 1 MB/s
		10_000_000,  // 10 MB/s
		100_000_000, // 100 MB/s
	];

	for bps in test_cases {
		let config = Arc::new(BrutalConfig::new(bps));
		let now = Instant::now();
		let controller = ControllerFactory::build(config, now, 1200);

		// All should have same initial window (before RTT is known)
		assert_eq!(controller.initial_window(), 10240);
		assert_eq!(controller.window(), 10240);
	}
}

/// Test custom initial window configuration
#[test]
fn brutal_custom_initial_window() {
	let mut config = BrutalConfig::new(1_000_000);
	config.initial_window(65535);

	let config = Arc::new(config);
	let now = Instant::now();
	let controller = ControllerFactory::build(config, now, 1200);

	assert_eq!(controller.initial_window(), 65535);
	assert_eq!(controller.window(), 65535);
}

/// Test Controller trait object behavior
#[test]
fn brutal_trait_object_behavior() {
	let config = Arc::new(BrutalConfig::new(10_000_000));
	let now = Instant::now();

	// Create as trait object
	let controller: Box<dyn Controller> = ControllerFactory::build(config, now, 1200);

	// Verify basic properties
	assert_eq!(controller.initial_window(), 10240);

	// Clone via trait method
	let cloned = controller.clone_box();
	assert_eq!(cloned.window(), controller.window());
}

/// Test that metrics are properly reported
#[test]
fn brutal_reports_correct_metrics() {
	let config = Arc::new(BrutalConfig::new(10_000_000));
	let now = Instant::now();

	let controller = ControllerFactory::build(config, now, 1200);
	let metrics = controller.metrics();

	// Before RTT is known, window should be the initial window
	assert_eq!(metrics.congestion_window, 10240);
}

/// Test that into_any works for downcasting
#[test]
fn brutal_into_any_downcast() {
	use quinn_congestions::brutal::Brutal;

	let config = Arc::new(BrutalConfig::new(1_000_000));
	let now = Instant::now();

	let controller: Box<dyn Controller> = ControllerFactory::build(config, now, 1200);

	// Convert to Any and downcast back
	let any = controller.into_any();
	let downcast_result = any.downcast::<Brutal>();

	assert!(downcast_result.is_ok());
}

/// Test concurrent clone behavior
#[test]
fn brutal_concurrent_clones_are_independent() {
	let config = Arc::new(BrutalConfig::new(10_000_000));
	let now = Instant::now();

	let mut original = ControllerFactory::build(config, now, 1200);
	let original_window = original.window();

	// Clone the controller
	let cloned = original.clone_box();

	// Both should have same window initially
	assert_eq!(cloned.window(), original_window);

	// Modify MTU on original
	original.on_mtu_update(9000);

	// Clone should not be affected (it has its own state)
	assert_eq!(cloned.window(), original_window);
}
