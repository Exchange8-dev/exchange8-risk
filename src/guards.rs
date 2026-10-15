// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Risk
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:Risk rate guards + reservation ledger.
//!
//! Two stateful extensions over the pure 21-check matrix (`core.rs`):
//! - **Token-bucket rate guards** (§5.3): messages/sec + burst per connection,
//! replacing the "no rate limiting anywhere" gap. Fail-closed.
//! - **Limit buckets** (§5.2): in-memory reservation counters keyed by
//!   connection × currency, rebuilt at SOD from Clearing day-so-far queries;
//!   the persistent audit trail is `risk.risk_reservation` (schema done).
//!
//! Design: `design/sentin8-exchange8-risk-technical-design.md` §5.2–5.3.

/// Token-bucket rate guard for one connection.
///
/// Refill is continuous (rate tokens/sec, capacity = burst). `try_acquire`
/// consumes one token or fails closed. Deterministic under a supplied clock so
/// the hot path has no syscalls; the shell feeds `now_us`.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    /// Bucket capacity (burst size).
    pub capacity: f64,
    /// Sustained refill rate (tokens/sec).
    pub rate_per_sec: f64,
    tokens: f64,
    last_refill_us: u64,
}

impl TokenBucket {
    pub fn new(capacity: f64, rate_per_sec: f64, now_us: u64) -> Self {
        TokenBucket {
            capacity,
            rate_per_sec,
            tokens: capacity,
            last_refill_us: now_us,
        }
    }

    fn refill(&mut self, now_us: u64) {
        if now_us <= self.last_refill_us {
            return;
        }
        let elapsed_s = (now_us - self.last_refill_us) as f64 / 1_000_000.0;
        self.tokens = (self.tokens + elapsed_s * self.rate_per_sec).min(self.capacity);
        self.last_refill_us = now_us;
    }

    /// Consume one token; false = rate exceeded (fail-closed).
    pub fn try_acquire(&mut self, now_us: u64) -> bool {
        self.refill(now_us);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Tokens available right now (diagnostics).
    pub fn available(&mut self, now_us: u64) -> f64 {
        self.refill(now_us);
        self.tokens
    }
}

/// Limit-bucket kinds (design §5.2 table).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BucketKind {
    /// `CheckCapitalization` parity: notional per connection × currency.
    Notional,
    /// Count of working orders per connection.
    OpenOrdersMax,
}

/// One limit bucket's in-memory state: consumed vs. limit.
#[derive(Debug, Clone)]
pub struct Bucket {
    pub kind: BucketKind,
    /// Scope identity (connection id or connection × currency).
    pub scope_id: String,
    pub limit_value: f64,
    pub consumed: f64,
}

impl Bucket {
    /// Reserve `value` against the limit; false = limit exceeded.
    pub fn reserve(&mut self, value: f64) -> bool {
        if self.consumed + value > self.limit_value {
            return false;
        }
        self.consumed += value;
        true
    }

    /// Release a reservation (order killed/filled/cancelled or STP decrement).
    /// Never underflows.
    pub fn release(&mut self, value: f64) {
        self.consumed = (self.consumed - value).max(0.0);
    }
}

/// The reservation ledger: in-memory buckets + an audit trail the shell
/// mirrors to `risk.risk_reservation`.
pub struct ReservationLedger {
    buckets: std::collections::HashMap<(BucketKind, String), Bucket>,
    /// Audit entries (order_id, bucket, value, delta sign).
    pub journal: Vec<(u64, BucketKind, String, f64)>,
}

impl ReservationLedger {
    pub fn new() -> Self {
        ReservationLedger {
            buckets: std::collections::HashMap::new(),
            journal: Vec::new(),
        }
    }

    /// Register/refresh a limit definition (admin CRUD or SOD load).
    pub fn set_limit(&mut self, kind: BucketKind, scope_id: String, limit_value: f64) {
        self.buckets
            .entry((kind, scope_id.clone()))
            .or_insert_with(|| Bucket {
                kind,
                scope_id,
                limit_value,
                consumed: 0.0,
            })
            .limit_value = limit_value;
    }

    /// Rebuild consumed state at SOD from Clearing day-so-far (e.g. open
    /// order count, live notional).
    pub fn seed_consumed(&mut self, kind: BucketKind, scope_id: &str, consumed: f64) {
        if let Some(b) = self.buckets.get_mut(&(kind, scope_id.to_string())) {
            b.consumed = consumed;
        }
    }

    /// Reserve for an order submission. Records the audit entry on success.
    pub fn reserve(&mut self, kind: BucketKind, scope_id: &str, order_id: u64, value: f64) -> bool {
        let key = (kind, scope_id.to_string());
        match self.buckets.get_mut(&key) {
            Some(b) => {
                if b.reserve(value) {
                    self.journal
                        .push((order_id, kind, scope_id.to_string(), value));
                    true
                } else {
                    false
                }
            }
            None => {
                // No limit configured: platform spec allowed unchecked flow,
                // but this service fails closed on unknown scopes.
                false
            }
        }
    }

    /// Release an order's reservation (kill/fill/cancel/STP decrement).
    pub fn release(&mut self, kind: BucketKind, scope_id: &str, order_id: u64, value: f64) {
        if let Some(b) = self.buckets.get_mut(&(kind, scope_id.to_string())) {
            b.release(value);
            self.journal
                .push((order_id, kind, scope_id.to_string(), -value));
        }
    }

    /// Snapshot a bucket (admin view).
    pub fn bucket(&self, kind: BucketKind, scope_id: &str) -> Option<&Bucket> {
        self.buckets.get(&(kind, scope_id.to_string()))
    }

    /// Available headroom (diagnostics).
    pub fn available(&self, kind: BucketKind, scope_id: &str) -> Option<f64> {
        self.bucket(kind, scope_id)
            .map(|b| b.limit_value - b.consumed)
    }
}

impl Default for ReservationLedger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_allows_burst_then_throttles() {
        // 5 burst, 10/sec.
        let mut tb = TokenBucket::new(5.0, 10.0, 0);
        for _ in 0..5 {
            assert!(tb.try_acquire(0));
        }
        // Bucket drained: 6th immediately fails.
        assert!(!tb.try_acquire(0));
        // After 100ms: 1 token refilled.
        assert!(tb.try_acquire(100_000));
        assert!(!tb.try_acquire(100_000));
        // After a full second: refilled past capacity, capped at 5; the
        // acquire above consumed one → 4 remain.
        assert!(tb.try_acquire(1_200_000));
        let mut probe = tb.clone();
        assert_eq!(probe.available(1_200_000), 4.0);
    }

    #[test]
    fn token_bucket_never_exceeds_capacity() {
        let mut tb = TokenBucket::new(3.0, 1000.0, 0);
        // Long idle: refills to capacity, not beyond.
        assert_eq!(tb.available(10_000_000), 3.0);
    }

    #[test]
    fn notional_reserve_and_release() {
        let mut led = ReservationLedger::new();
        led.set_limit(BucketKind::Notional, "conn1|CAD".into(), 100_000.0);
        assert!(led.reserve(BucketKind::Notional, "conn1|CAD", 1, 60_000.0));
        assert_eq!(
            led.available(BucketKind::Notional, "conn1|CAD"),
            Some(40_000.0)
        );
        // Exceeding the remaining headroom fails.
        assert!(!led.reserve(BucketKind::Notional, "conn1|CAD", 2, 50_000.0));
        assert!(led.reserve(BucketKind::Notional, "conn1|CAD", 3, 40_000.0));
        // Release a killed order's reservation.
        led.release(BucketKind::Notional, "conn1|CAD", 1, 60_000.0);
        assert_eq!(
            led.available(BucketKind::Notional, "conn1|CAD"),
            Some(60_000.0)
        );
        // Journal carries +60k, +40k, −60k.
        let deltas: Vec<f64> = led.journal.iter().map(|(_, _, _, v)| *v).collect();
        assert_eq!(deltas, vec![60_000.0, 40_000.0, -60_000.0]);
    }

    #[test]
    fn exact_limit_fits() {
        let mut led = ReservationLedger::new();
        led.set_limit(BucketKind::Notional, "c|USD".into(), 100.0);
        assert!(led.reserve(BucketKind::Notional, "c|USD", 1, 100.0));
        assert_eq!(led.available(BucketKind::Notional, "c|USD"), Some(0.0));
        // One more unit fails.
        assert!(!led.reserve(BucketKind::Notional, "c|USD", 2, 1.0));
    }

    #[test]
    fn open_orders_max_counts_working_orders() {
        let mut led = ReservationLedger::new();
        led.set_limit(BucketKind::OpenOrdersMax, "conn9".into(), 3.0);
        assert!(led.reserve(BucketKind::OpenOrdersMax, "conn9", 1, 1.0));
        assert!(led.reserve(BucketKind::OpenOrdersMax, "conn9", 2, 1.0));
        assert!(led.reserve(BucketKind::OpenOrdersMax, "conn9", 3, 1.0));
        assert!(!led.reserve(BucketKind::OpenOrdersMax, "conn9", 4, 1.0));
        // A cancel frees a slot.
        led.release(BucketKind::OpenOrdersMax, "conn9", 2, 1.0);
        assert!(led.reserve(BucketKind::OpenOrdersMax, "conn9", 5, 1.0));
    }

    #[test]
    fn unknown_scope_fails_closed() {
        let mut led = ReservationLedger::new();
        // No limit configured for this scope: fail closed.
        assert!(!led.reserve(BucketKind::Notional, "ghost|CAD", 1, 1.0));
    }

    #[test]
    fn release_never_underflows() {
        let mut led = ReservationLedger::new();
        led.set_limit(BucketKind::Notional, "c".into(), 100.0);
        led.reserve(BucketKind::Notional, "c", 1, 50.0);
        // Double release (event replay) must not drive consumed negative.
        led.release(BucketKind::Notional, "c", 1, 50.0);
        led.release(BucketKind::Notional, "c", 1, 50.0);
        assert_eq!(led.available(BucketKind::Notional, "c"), Some(100.0));
    }

    #[test]
    fn sod_seed_rebuilds_consumed() {
        let mut led = ReservationLedger::new();
        led.set_limit(BucketKind::Notional, "c|CAD".into(), 1_000.0);
        led.seed_consumed(BucketKind::Notional, "c|CAD", 300.0);
        assert_eq!(led.available(BucketKind::Notional, "c|CAD"), Some(700.0));
    }

    #[test]
    fn set_limit_refreshes_existing() {
        let mut led = ReservationLedger::new();
        led.set_limit(BucketKind::Notional, "c".into(), 100.0);
        led.reserve(BucketKind::Notional, "c", 1, 50.0);
        // Admin raises the limit; consumption persists.
        led.set_limit(BucketKind::Notional, "c".into(), 200.0);
        assert_eq!(led.available(BucketKind::Notional, "c"), Some(150.0));
    }
}
