# quinn-congestions

## Differences from the upstream quinn BBR

The `bbr` module is a port of the BBR implementation from quinn upstream
(quinn-proto 0.12), adapted to the `Controller` trait of the `bbrv3` branch
(Tipuch/quinn). Compared with the upstream BBR, it has three differences:

1. **min RTT tracking uses `rtt.min()` instead of `rtt.get()`**

   In `on_ack`, `min_rtt` is updated whenever the estimator's historical minimum
   (`rtt.min()`) is lower, instead of the smoothed RTT (`rtt.get()`). This tracks
   the true minimum RTT more aggressively, matching the min_rtt semantics of the
   BBR spec more closely.

2. **RNG switched from `StdRng` to `Pcg32`**

   The random number generator used to pick the random gain-cycle offset in
   ProbeBw is now `Pcg32` (from `rand_pcg`, seeded via `Pcg32::from_rng(&mut
   rand::rng())`) — the same RNG the bbrv3 branch's `bbr3` uses — instead of
   `rand::rngs::StdRng::from_os_rng()`. This also moved the crate from rand 0.9
   to the rand 0.10 ecosystem (`RngExt`, `rand_pcg`).

3. **Controller trait signature adapted for the bbrv3 branch**

   `on_ack`, `on_end_acks` and `on_congestion_event` accept an extra
   `_space: SpaceId` argument, which is required by the `Controller` trait of
   the `bbrv3` branch but not by upstream quinn.

Everything else — the state machine (Startup / Drain / ProbeBw / ProbeRtt),
recovery logic, ACK aggregation and all tuning constants (`K_PACING_GAIN`,
`K_DEFAULT_HIGH_GAIN`, ...) — is identical to the upstream implementation.
