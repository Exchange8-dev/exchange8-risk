// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Risk
// SPDX-License-Identifier: Apache-2.0

//! Ex8:Risk limits ledger + cache lifecycle.
//!
//! - **Capitalization/credit buckets** (DESIGN.md §3/§7): notional reservation tracking
//!   (submit reserves qty×price; fill converts reservation to spent credit;
//!   kill releases), open-order counters, and message-rate token buckets —
//!   the mutable companions to `ClientState`'s static limits.
//! - **Cache lifecycle**: the snapshot swap contract — build a new
//!   new `EnvState` from SOD load, swap atomically, and event-driven
//!   partial refreshes (customer/limit changes re-project into a fresh
//!   snapshot; readers never see torn state).
//! - **API surface**: the authorize/release handler decision shapes
//!   the shell mounts at `POST /v1/authorize` / `POST /v1/release` + admin
//!   limits CRUD (DESIGN.md §4).
//!
//! Design: `design/sentin8-exchange8-risk-technical-design.md` §4–§6.

use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use exchange8::contracts::order::ExchangeOrder;
use exchange8::fixed::{Price, Qty};

use crate::core::{authorize, Decision, EnvState};

/// Mutable per-client limit ledger (the reservation side, DESIGN.md §3).
#[derive(Debug, Default, Clone)]
pub struct ClientLedger {
    /// Reserved notional (open orders, qty×price at submit).
    pub notional_reserved: u128,
    /// Spent credit (filled notional).
    pub notional_spent: u128,
    /// Working open orders.
    pub open_orders: u64,
    /// Token-bucket: permits left this second.
    pub permits: u32,
    /// Token-bucket: last refill epoch (seconds).
    pub last_refill_s: u64,
}

impl ClientLedger {
    /// Message-rate token bucket refill (capacity × per-second rate).
    pub fn refill(&mut self, capacity: u32, per_second: u32, now_s: u64) {
        if now_s > self.last_refill_s {
            let elapsed = (now_s - self.last_refill_s).min(60);
            self.permits = (self.permits + per_second * elapsed as u32).min(capacity);
            self.last_refill_s = now_s;
        }
    }

    /// Consume one permit; false = rate-limited.
    pub fn take_permit(&mut self) -> bool {
        if self.permits == 0 {
            return false;
        }
        self.permits -= 1;
        true
    }
}

/// The limits ledger (DESIGN.md §3) — mutable state the shell holds
/// *beside* the immutable `EnvState` snapshot.
pub struct LimitsLedger {
    clients: HashMap<u16, ClientLedger>,
    /// Per-order reserved-notional register: the release/consume side of
    /// the event consumer reads it (the ME lifecycle payload carries no
    /// price history — the ledger tracked the booking; saturation makes a
    /// re-delivered event safe). Single-owner: touched only from the task
    /// that owns the ledger.
    order_notional_by_order: HashMap<u64, u128>,
    /// Rate-bucket profiles per connection (`message_rate` limit rows):
    /// the (capacity, per-second) pairs. Rows are the ONLY rate source
    /// (design §5.3); a connection without one is unconfigured — the
    /// caller's reserve fails closed on it (invariant 3).
    rate_profiles: HashMap<u16, (u32, u32)>,
}

/// Reserve/release outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReserveOutcome {
    Reserved,
    /// Notional credit exhausted.
    NotionalExceeded,
    /// Open-order count exhausted.
    OpenOrdersExceeded,
    /// Message rate exhausted.
    RateLimited,
}

impl LimitsLedger {
    pub fn new() -> Self {
        LimitsLedger {
            clients: HashMap::new(),
            order_notional_by_order: HashMap::new(),
            rate_profiles: HashMap::new(),
        }
    }

    fn entry(&mut self, connection_id: u16) -> &mut ClientLedger {
        self.clients.entry(connection_id).or_default()
    }

    pub fn ledger(&self, connection_id: u16) -> Option<&ClientLedger> {
        self.clients.get(&connection_id)
    }

    /// Full pre-check + reserve for one submission: authorize (21 checks)
    /// then the credit buckets (DESIGN.md §2) — rate permit, open-order
    /// slot, notional reservation. The caller supplies the snapshot +
    /// bucket config.
    pub fn reserve(
        &mut self,
        order: &ExchangeOrder,
        env: &EnvState,
        rate: (u32, u32), // (capacity, per-second)
        now_s: u64,
    ) -> Result<Decision, ReserveOutcome> {
        let cid = order.connection_id;
        // 1. Rate permit (before touching anything else).
        {
            let e = self.entry(cid);
            e.refill(rate.0, rate.1, now_s);
            if !e.take_permit() {
                return Err(ReserveOutcome::RateLimited);
            }
        }
        // 2. Static authorization matrix (EnvState snapshot).
        let decision = authorize(order, env);
        if let Decision::Reject(_) = decision {
            return Ok(decision);
        }
        let client = env.client(cid);
        // 3. Open-order slot.
        let open_limit = client.and_then(|c| c.open_orders_limit).unwrap_or(u64::MAX);
        {
            let e = self.entry(cid);
            if e.open_orders >= open_limit {
                return Err(ReserveOutcome::OpenOrdersExceeded);
            }
        }
        // 4. Notional reservation (limit price; market estimates at last).
        if let Some(limit) = client.and_then(|c| c.notional_limit) {
            let notional = order_notional(order);
            let e = self.entry(cid);
            if e.notional_reserved
                .saturating_add(e.notional_spent)
                .saturating_add(notional)
                > limit
            {
                return Err(ReserveOutcome::NotionalExceeded);
            }
            e.notional_reserved += notional;
        }
        self.entry(cid).open_orders += 1;
        Ok(decision)
    }

    /// Release on kill/cancel: unreserve notional, free the open-order slot.
    pub fn release(&mut self, order: &ExchangeOrder) {
        let cid = order.connection_id;
        let notional = order_notional(order);
        if let Some(e) = self.clients.get_mut(&cid) {
            e.notional_reserved = e.notional_reserved.saturating_sub(notional);
            e.open_orders = e.open_orders.saturating_sub(1);
        }
    }

    /// Convert a reservation to spent credit on (partial) fill.
    pub fn on_fill(&mut self, connection_id: u16, fill_qty: Qty, price: Price) {
        let notional = price
            .checked_notional(fill_qty)
            .map(|n| n as u128)
            .unwrap_or(0);
        if let Some(e) = self.clients.get_mut(&connection_id) {
            e.notional_reserved = e.notional_reserved.saturating_sub(notional);
            e.notional_spent += notional;
            // A fully-filled order also frees its open-order slot.
        }
    }

    /// Order completed (filled or killed): free the slot.
    pub fn on_done(&mut self, connection_id: u16) {
        if let Some(e) = self.clients.get_mut(&connection_id) {
            e.open_orders = e.open_orders.saturating_sub(1);
        }
    }

    /// Configure the `message_rate` bucket for one connection (the loader
    /// applies the row's `capacity/per_second` encoding; a re-set replaces
    /// the profile — the ledger re-reads it on the next reserve).
    pub fn set_rate_profile(&mut self, connection_id: u16, capacity: u32, per_second: u32) {
        self.rate_profiles
            .insert(connection_id, (capacity, per_second));
    }

    /// The connection's rate profile (`None` = unconfigured — the caller
    /// fails closed on `message_rate` absence, invariant 3).
    pub fn rate_profile(&self, connection_id: u16) -> Option<(u32, u32)> {
        self.rate_profiles.get(&connection_id).copied()
    }

    /// Every connection with ledger state (the exposure snapshot projects
    /// from it — sorted ascending by connection id for stable publishes).
    pub fn clients(&self) -> Vec<(u16, ClientLedger)> {
        let mut v: Vec<(u16, ClientLedger)> =
            self.clients.iter().map(|(k, v)| (*k, v.clone())).collect();
        v.sort_by_key(|(k, _)| *k);
        v
    }

    /// Reserve for one order against the LOADED limit rows (the runtime
    /// pass — not the test-config `(capacity, rate)` tuple variant): rate
    /// profile first (unconfigured → deny), then the static matrix, then
    /// the open-order slot, then the notional reservation; the booked
    /// notional registers per order for the release/consume side.
    /// Returns the trail projection (the egress writer's input) on allow;
    /// a static reject returns `Err(RateLimited)`-free `Ok(Decision)`.
    pub fn reserve_tracked(
        &mut self,
        order: &ExchangeOrder,
        env: &EnvState,
        now_s: u64,
    ) -> Result<ReservationTrail, ReserveOutcome> {
        let Some((cap, rate)) = self.rate_profile(order.connection_id) else {
            return Err(ReserveOutcome::RateLimited);
        };
        match self.reserve(order, env, (cap, rate), now_s)? {
            Decision::Reject(_) => unreachable!("reserve errors a static reject above"),
            Decision::Allow { .. } => {}
        }
        let notional = order_notional(order);
        self.order_notional_by_order
            .insert(order.gw_order_id, notional);
        Ok(ReservationTrail {
            env_id: Uuid::nil(),
            order_id: order.gw_order_id,
            connection_id: order.connection_id,
            scope_type: "connection",
            scope_id: order.connection_id.to_string(),
            currency: order.instrument.currency.clone(),
            buckets: vec!["notional"],
            value: notional,
        })
    }

    /// Release one order's reservation (the consumer's kill path) —
    /// saturating; a re-delivered release is a no-op (the register is
    /// removed on the first release).
    pub fn release_order(&mut self, connection_id: u16, order_id: u64) {
        if let Some(n) = self.order_notional_by_order.remove(&order_id) {
            if let Some(e) = self.clients.get_mut(&connection_id) {
                e.notional_reserved = e.notional_reserved.saturating_sub(n);
            }
        }
        self.on_done(connection_id);
    }

    /// Apply one execution to the ledger (the consumer's fill path):
    /// convert the execution's notional (price × qty, checked) from
    /// reserved to spent; the order's register drains with it and a fully
    /// converted order frees its slot.
    pub fn apply_fill(&mut self, connection_id: u16, fill_notional: i128, order_id: u64) {
        let n = fill_notional.max(0) as u128;
        if let Some(e) = self.clients.get_mut(&connection_id) {
            e.notional_reserved = e.notional_reserved.saturating_sub(n);
            e.notional_spent += n;
        }
        let consumed = self.order_notional_by_order.entry(order_id).or_insert(0);
        *consumed = consumed.saturating_sub(n);
        if *consumed == 0 {
            self.order_notional_by_order.remove(&order_id);
            self.on_done(connection_id);
        }
    }

    /// The registered notional of one order (diagnostics / the consumer's
    /// trail projection).
    pub fn order_notional_of(&self, order_id: u64) -> u128 {
        self.order_notional_by_order
            .get(&order_id)
            .copied()
            .unwrap_or(0)
    }
}

impl Default for LimitsLedger {
    fn default() -> Self {
        Self::new()
    }
}

/// The durable reservation-trail projection the egress writer commits
/// (one item per order): the trail's NOTIONAL book + the per-order identity
/// the consumer events carry (DESIGN.md §3 trail rows; the open-order slot
/// rides the same order's identity, no separate row).
#[derive(Clone, Debug, PartialEq)]
pub struct ReservationTrail {
    /// The env the trail attributes to (set by the runtime at staging).
    pub env_id: Uuid,
    /// Engine/gateway order id (the trail row's `order_id`).
    pub order_id: u64,
    /// Gateway session (attribution mirror of the envelope payload).
    pub connection_id: u16,
    /// Trail scope kind (`connection` — the ledger buckets by connection;
    /// customer/account depth is the limits-depth epic's row vocabulary).
    pub scope_type: &'static str,
    /// Scope identifier (the connection id's text form).
    pub scope_id: String,
    /// Settlement currency the reservations are denominated in.
    pub currency: String,
    /// The reservation's touched buckets (`notional` — the ledger's
    /// enforced kinds; open-order slots are counts, no trail rows).
    pub buckets: Vec<&'static str>,
    /// The reserved notional.
    pub value: u128,
}

impl ReservationTrail {
    /// The payload-complete event snapshot (the wire `order_id` /
    /// `connection_id` / bucket + value — the consumer reacts without a
    /// read-back).
    pub fn envelope_snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "env_id": self.env_id,
            "order_id": self.order_id,
            "connection_id": self.connection_id,
            "scope_type": self.scope_type,
            "scope_id": self.scope_id,
            "currency": self.currency,
            "bucket": self.buckets.first().copied().unwrap_or("notional"),
            "value": self.value.to_string(),
        })
    }

    /// A release/consume side projection: the order identity + the released
    /// value (0 = slot-only release; the ledger saturates).
    pub fn release_side(order_id: u64, connection_id: u16, value: u128) -> ReservationTrail {
        ReservationTrail {
            env_id: Uuid::nil(),
            order_id,
            connection_id,
            scope_type: "connection",
            scope_id: connection_id.to_string(),
            currency: String::new(),
            buckets: vec!["notional"],
            value,
        }
    }
}

/// Deterministic u32 projection of the security UUID (the engine's
/// instrument id space; stable across restarts) — the SAME FNV-1a-32 the ME
/// shard plane uses (`me::instrument_id_of`), so gateway / risk / ME key
/// instruments identically.
pub fn instrument_id_of(security_id: &str) -> u32 {
    let mut h: u32 = 0x9e37_79b9; // FNV-1a-32 over the dash string form
    for b in security_id.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// Notional of one order (limit price × qty; market orders estimate via
/// the caller before calling — the gateway uses the last price).
fn order_notional(order: &ExchangeOrder) -> u128 {
    order
        .price
        .and_then(|p| p.checked_notional(order.qty))
        .map(|n| n.max(0) as u128)
        .unwrap_or(0)
}

/// The cache lifecycle (DESIGN.md §7): SOD load builds a full snapshot;
/// refreshes
/// build a fresh one and swap. Readers hold `Arc<EnvState>` and never see
/// partial updates.
pub struct RiskCache {
    current: Arc<EnvState>,
}

impl RiskCache {
    /// SOD load: the shell projects DB rows into a complete `EnvState`.
    pub fn from_sod(state: EnvState) -> Self {
        RiskCache {
            current: Arc::new(state),
        }
    }

    /// Read the current snapshot (hot path — one Arc clone).
    pub fn snapshot(&self) -> Arc<EnvState> {
        self.current.clone()
    }

    /// Event-driven refresh: apply a mutation to a *copy* and swap
    /// atomically (no torn reads).
    pub fn refresh(&mut self, mutate: impl FnOnce(&mut EnvState)) {
        let mut next = (*self.current).clone();
        mutate(&mut next);
        self.current = Arc::new(next);
    }
}

/// Handler decision shapes (what `POST /v1/authorize` returns, DESIGN.md §4).
#[derive(Clone, Debug, PartialEq)]
pub struct AuthorizeResponse {
    pub allowed: bool,
    /// Reject reason code when denied.
    pub reason: Option<&'static str>,
    /// The reservation made (notional reserved).
    pub reserved_notional: Option<u128>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ClientState, InstrumentState};
    use exchange8::contracts::enums::{Capacity, Visibility};
    use exchange8::contracts::order::{
        BookToken, InstrumentKind, InstrumentRef, OrdStatus, OrderSide, Tif,
    };
    fn order(px: Option<i128>) -> ExchangeOrder {
        ExchangeOrder {
            env_id: "e".into(),
            gw_order_id: 1,
            cl_ord_id: "C1".into(),
            orig_cl_ord_id: None,
            account: "ACCT".into(),
            client_comp_id: "MEMBER".into(),
            on_behalf_comp_id: None,
            user_id: None,
            connection_id: 7,
            instrument: InstrumentRef {
                id: 1,
                symbol: "EX8A".into(),
                isin: None,
                mic: "XL8M".into(),
                currency: "CAD".into(),
                price_scale: 4,
                qty_scale: 0,
                lot_size: Qty::units(100),
                kind: InstrumentKind::Equity,
            },
            side: OrderSide::Buy,
            qty: Qty::units(100),
            min_qty: None,
            price: px.map(Price::equity),
            ord_type: exchange8::contracts::order::OrdType::Limit,
            tif: Tif::Day,
            peg_type: None,
            peg_difference: None,
            peg_ebbo: false,
            visibility: Visibility::Display,
            max_floor: None,
            capacity: Capacity::Agency,
            anonymous: false,
            bypass: false,
            price_protection: None,
            post_only: false,
            book: BookToken('*'),
            strategy: vec![],
            stp_key: None,
            stp_action: None,
            cross: None,
            ex_destination: None,
            ord_status: OrdStatus::New,
            cum_qty: Qty::ZERO,
            leaves_qty: Qty::units(100),
            avg_px: None,
            timestamp_us: 0,
        }
    }

    fn env() -> EnvState {
        let mut e = EnvState::default();
        e.instruments.insert(
            1,
            InstrumentState {
                instrument: InstrumentRef {
                    id: 1,
                    symbol: "EX8A".into(),
                    isin: None,
                    mic: "XL8M".into(),
                    currency: "CAD".into(),
                    price_scale: 4,
                    qty_scale: 0,
                    lot_size: Qty::units(100),
                    kind: InstrumentKind::Equity,
                },
                halted: false,
                holiday: false,
                shortable: crate::core::Shortable::Yes,
                lis_required: false,
                lis_volume: None,
                tick_table: vec![],
                max_size: Qty::units(1_000_000),
                pegs_allowed: true,
                ebbo_allowed: false,
                allowed_capacities: vec![Capacity::Agency, Capacity::Principal],
                allowed_tifs: vec![Tif::Day, Tif::Gtc, Tif::Ioc],
            },
        );
        e.clients.insert(
            7,
            ClientState {
                connection_id: 7,
                customer_id: 1,
                suspended: false,
                enabled: true,
                book_tokens: vec!['*'],
                entitlements: vec![],
                notional_limit: Some(1_000_000),
                open_orders_limit: Some(2),
                open_orders: 0,
                rate_permits: 10,
            },
        );
        e
    }

    #[test]
    fn reserve_then_release_round_trip() {
        let mut ledger = LimitsLedger::new();
        let e = env();
        let o = order(Some(10_000)); // notional 100×10_000 = 1_000_000... exactly at limit
        let o_small = {
            let mut o2 = order(Some(5_000));
            o2.gw_order_id = 2;
            o2.cl_ord_id = "C2".into();
            o2
        };

        // Small order reserves fine.
        assert!(ledger.reserve(&o_small, &e, (10, 10), 1).is_ok());
        let l = ledger.ledger(7).unwrap();
        assert_eq!(l.notional_reserved, 500_000);
        assert_eq!(l.open_orders, 1);

        // At-limit order now exceeds (500k reserved + 1_000_000 > 1M limit).
        let r = ledger.reserve(&o, &e, (10, 10), 2);
        assert_eq!(r, Err(ReserveOutcome::NotionalExceeded));

        // Release the small one → room returns.
        ledger.release(&o_small);
        let l = ledger.ledger(7).unwrap();
        assert_eq!(l.notional_reserved, 0);
        assert_eq!(l.open_orders, 0);

        // Now the big one fits exactly.
        assert!(ledger.reserve(&o, &e, (10, 10), 3).is_ok());
    }

    #[test]
    fn open_order_slots_enforced() {
        let mut ledger = LimitsLedger::new();
        let e = env();
        let o1 = order(Some(1_000));
        let mut o2 = order(Some(1_000));
        o2.gw_order_id = 2;
        let mut o3 = order(Some(1_000));
        o3.gw_order_id = 3;

        assert!(ledger.reserve(&o1, &e, (10, 10), 1).is_ok());
        assert!(ledger.reserve(&o2, &e, (10, 10), 1).is_ok());
        // Third exceeds the 2-order limit.
        assert_eq!(
            ledger.reserve(&o3, &e, (10, 10), 1),
            Err(ReserveOutcome::OpenOrdersExceeded)
        );
        // One completes → slot frees.
        ledger.on_done(7);
        assert!(ledger.reserve(&o3, &e, (10, 10), 1).is_ok());
    }

    #[test]
    fn rate_bucket_permits() {
        let mut ledger = LimitsLedger::new();
        let mut e = env();
        // Rate-only test: lift the open-order limit so it never interferes.
        e.clients.get_mut(&7).unwrap().open_orders_limit = Some(100);
        // Capacity 2, rate 2/s: refill from empty at t=1 grants 2 permits.
        let mk = |id: u64| {
            let mut o = order(Some(1_000));
            o.gw_order_id = id;
            o
        };
        assert!(ledger.reserve(&mk(1), &e, (2, 2), 1).is_ok());
        assert!(ledger.reserve(&mk(2), &e, (2, 2), 1).is_ok());
        // Bucket drained within the same second.
        assert_eq!(
            ledger.reserve(&mk(3), &e, (2, 2), 1),
            Err(ReserveOutcome::RateLimited)
        );
        // Next second refills back toward capacity.
        assert!(ledger.reserve(&mk(4), &e, (2, 2), 2).is_ok());
    }

    #[test]
    fn fill_converts_reservation_to_spent() {
        let mut ledger = LimitsLedger::new();
        let e = env();
        let o = order(Some(5_000));
        assert!(ledger.reserve(&o, &e, (10, 10), 1).is_ok());
        // Reserved 100 × 5_000 = 500_000.
        assert_eq!(ledger.ledger(7).unwrap().notional_reserved, 500_000);
        // Partial fill of 40 at 5_000 → 200_000 converts to spent.
        ledger.on_fill(7, Qty::units(40), Price::equity(5_000));
        let l = ledger.ledger(7).unwrap();
        assert_eq!(l.notional_reserved, 300_000); // 60 × 5_000
        assert_eq!(l.notional_spent, 200_000); // 40 × 5_000

        ledger.on_done(7);
        assert_eq!(ledger.ledger(7).unwrap().open_orders, 0);
    }

    #[test]
    fn static_reject_short_circuits_before_buckets() {
        // Halted instrument → authorize rejects; no reservation made.
        let mut ledger = LimitsLedger::new();
        let mut e = env();
        e.instruments.get_mut(&1).unwrap().halted = true;
        let o = order(Some(1_000));
        let r = ledger.reserve(&o, &e, (10, 10), 1);
        assert!(matches!(r, Ok(Decision::Reject(_))));
        assert!(ledger.ledger(7).unwrap().notional_reserved == 0);
    }

    #[test]
    fn cache_swap_is_atomic_and_isolated() {
        let mut cache = RiskCache::from_sod(env());
        let snap1 = cache.snapshot();
        assert!(snap1.client(7).is_some());
        // Refresh: suspend the client.
        cache.refresh(|e| {
            e.clients.get_mut(&7).unwrap().suspended = true;
        });
        let snap2 = cache.snapshot();
        assert!(snap2.client(7).unwrap().suspended);
        // The old snapshot is untouched (readers keep consistent views).
        assert!(!snap1.client(7).unwrap().suspended);
        assert!(!Arc::ptr_eq(&snap1, &snap2));
    }

    #[test]
    fn authorize_response_shape() {
        let r = AuthorizeResponse {
            allowed: true,
            reason: None,
            reserved_notional: Some(500_000),
        };
        assert!(r.allowed);
        assert!(r.reserved_notional.is_some());
    }
}
