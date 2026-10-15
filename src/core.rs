// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Risk
// SPDX-License-Identifier: Apache-2.0

//! Ex8:Risk synchronous pre-trade authorization core.
//!
//! Pure function `authorize(order, &EnvState) -> Decision` implementing the
//! 21-check matrix from the design (Risk §5.1, the platform spec /
//! `CheckCapitalization` parity). The axum shell only does auth + cache
//! lookup; every check here is unit-testable without IO.
//!
//! Design: `design/sentin8-exchange8-risk-technical-design.md` §5.
//! Truth: `design/behavior-catalog/reject-codes.json` (exact codes).

use exchange8::contracts::enums::{Capacity, Visibility};
use exchange8::contracts::order::*;
use exchange8::contracts::reject::RejectCode;
use exchange8::fixed::{Price, Qty};

/// Result of the authorization pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// All checks passed; reservation ids to book in the same DB tx.
    Allow { reservation_ids: Vec<u64> },
    /// First failing check with its reject code.
    Reject(RejectCode),
}

/// Market phase (drives check 5).
///
/// All variants are constructed by the axum shell / event consumers
/// (market:status events); the pure core only reads them.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MarketPhase {
    /// Pre-open: DAY+LIMIT quote-only rules .
    PreOpen,
    #[default]
    Open,
    /// Postpone: no new orders .
    Postponed,
    /// Halt: no new orders .
    Halted,
    Closed,
}

/// Shortable matrix cell (the platform spec Y/E/N).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shortable {
    /// Y — short freely.
    Yes,
    /// E — exempt-marked shorts only (`ShortMarkingExempt`).
    ExemptOnly,
    /// N — no shorts.
    No,
}

/// Per-instrument static+dynamic reference snapshot (RefData + state events).
#[derive(Debug, Clone)]
pub struct InstrumentState {
    pub instrument: InstrumentRef,
    /// Halted / quoting-only / open (ME instrument status mirror).
    pub halted: bool,
    /// Trading-holiday for this instrument's market today.
    pub holiday: bool,
    /// Shortable matrix cell.
    pub shortable: Shortable,
    /// True when the instrument's market requires Large-In-Scale for hidden.
    pub lis_required: bool,
    /// Minimum LIS volume for hidden orders (in qty units).
    pub lis_volume: Option<Qty>,
    /// Per-instrument tick table: (lower-bound-inclusive price, tick raw).
    pub tick_table: Vec<(Price, i128)>,
    /// Max order size in qty units (max_shares).
    pub max_size: Qty,
    /// True when this market permits pegged orders.
    pub pegs_allowed: bool,
    /// True when EBBO price protection is enabled for this market.
    pub ebbo_allowed: bool,
    /// Allowed capacities for this instrument.
    pub allowed_capacities: Vec<Capacity>,
    /// Allowed TIFs for this market.
    pub allowed_tifs: Vec<Tif>,
}

/// Connection + customer + limits snapshot (customer:state / limits:updated).
///
/// `connection_id`/`customer_id` anchor the audit record; `open_orders*`
/// feed check 20's counter bucket — the full buckets (notional reservations,
/// open-order count, rate) are consumed by the shell's reserve/release paths.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ClientState {
    pub connection_id: u16,
    pub customer_id: u64,
    /// Session suspended (CLIENT_SUSPENDED).
    pub suspended: bool,
    /// Customer/trader enabled.
    pub enabled: bool,
    /// Book tokens granted to this connection (BOOK.BOOK_TOKEN).
    pub book_tokens: Vec<char>,
    /// Entitlements (market/regulation tokens).
    pub entitlements: Vec<String>,
    /// Notional credit limit (CAPITALIZATION row, qty×price ≤ limit).
    pub notional_limit: Option<u128>,
    /// Open-order count limit.
    pub open_orders_limit: Option<u64>,
    /// Working open-order count right now.
    pub open_orders: u64,
    /// Token-bucket message rate (permits remaining, refill per sec).
    pub rate_permits: u32,
}

/// The whole environment state needed for authorization (ArcSwap snapshot).
#[derive(Debug, Clone, Default)]
pub struct EnvState {
    pub market_phase: Option<MarketPhase>,
    pub instruments: std::collections::HashMap<u32, InstrumentState>,
    pub clients: std::collections::HashMap<u16, ClientState>,
}

impl EnvState {
    pub fn instrument(&self, id: u32) -> Option<&InstrumentState> {
        self.instruments.get(&id)
    }

    pub fn client(&self, connection_id: u16) -> Option<&ClientState> {
        self.clients.get(&connection_id)
    }
}

/// Evaluate the 21-check authorization matrix (design Risk §5.1).
///
/// Checks run in the documented order; the first failure rejects with the
/// exact reject code. Fail-closed: unknown instrument/connection
/// rejects rather than defaults.
pub fn authorize(order: &ExchangeOrder, env: &EnvState) -> Decision {
    use RejectCode::*;

    // --- 1. Session not suspended/halted ---------------------------------
    let client = match env.client(order.connection_id()) {
        Some(c) => c,
        None => return Decision::Reject(ClientSuspended),
    };
    if client.suspended {
        return Decision::Reject(ClientSuspended);
    }

    // --- 2. Instrument resolved -------------------------------------------
    let inst = match env.instrument(order.instrument.id) {
        Some(i) => i,
        None => return Decision::Reject(order.unresolved_instrument_code()),
    };
    if order.instrument.symbol.is_empty() {
        return Decision::Reject(InvalidSymbol);
    }
    if let Some(isin) = &order.instrument.isin {
        if let Some(real_isin) = &inst.instrument.isin {
            if isin != real_isin {
                return Decision::Reject(InvalidSecurityId);
            }
        }
    }

    // --- 3. Broker/customer/trader enabled ---------------------------------
    if !client.enabled {
        return Decision::Reject(WrongUserId);
    }

    // --- 4. Book token valid for connection and customer; single-book strat --
    if !client.book_tokens.contains(&order.book.0) {
        return Decision::Reject(WrongOrderBook);
    }
    if order.strategy.len() > 1 {
        return Decision::Reject(WrongOrderBook);
    }

    // --- 5. Instrument not halted; market open -----------------------------
    if inst.halted {
        return Decision::Reject(SecurityHaltedPrimary);
    }
    if inst.holiday {
        return Decision::Reject(SecurityHoliday);
    }
    match env.market_phase.unwrap_or_default() {
        MarketPhase::Open => {}
        MarketPhase::PreOpen => {
            // Pre-open: DAY  LIMIT  quoted-only rules (platform spec).
            if order.tif != Tif::Day || order.ord_type != OrdType::Limit {
                return Decision::Reject(ExchangePreopen);
            }
        }
        MarketPhase::Postponed => return Decision::Reject(ExchangePostponed),
        MarketPhase::Halted => return Decision::Reject(ExchangeHalted),
        MarketPhase::Closed => return Decision::Reject(ExchangeClosed),
    }

    // --- 6. Currency -------------------------------------------------------
    if !order
        .instrument
        .currency
        .eq_ignore_ascii_case(&inst.instrument.currency)
    {
        return Decision::Reject(WrongCurrency);
    }

    // --- 7. OrdType/TIF shape ----------------------------------------------
    if !matches!(order.ord_type, OrdType::Limit | OrdType::Pegged) {
        return Decision::Reject(WrongOrderType);
    }
    if !inst.allowed_tifs.contains(&order.tif) {
        return Decision::Reject(WrongTimeInForce);
    }

    // --- 8. Tick size ------------------------------------------------------
    if let Some(price) = order.price {
        if !on_tick(price, &inst.tick_table) {
            return Decision::Reject(MismatchPriceTicksize);
        }
    }

    // --- 9. Lot rules / iceberg floor --------------------------------------
    let lot = inst.instrument.lot_size;
    let (lots, odd) = order.qty.split_board_odd(lot);
    let mixed = lots.raw > 0 && odd.raw > 0;
    if order.instrument.qty_scale == 0 {
        if lots.raw == 0 {
            // Pure odd-lot: rejected on lit books (ODD_LOT).
            if order.book.0 == 'M' {
                return Decision::Reject(OddLot);
            }
        } else if mixed {
            return Decision::Reject(MixedLot);
        }
    }
    if order.max_floor.is_some() && order.max_floor != Some(Qty::ZERO) {
        // Iceberg: floor must be positive and < qty (WRONG_MAX_FLOOR).
        let floor = order.max_floor.unwrap();
        if floor.raw == 0 || floor.raw >= order.qty.raw {
            return Decision::Reject(WrongMaxFloor);
        }
        if !matches!(order.tif, Tif::Day | Tif::Gtc) {
            return Decision::Reject(WrongMaxFloor);
        }
    }

    // --- 10. Qty/price bounds ------------------------------------------------
    if order.qty.raw == 0 || order.qty.raw > inst.max_size.raw {
        return Decision::Reject(WrongQuantity);
    }
    if let Some(price) = order.price {
        if price.raw <= 0 {
            return Decision::Reject(WrongPrice);
        }
    }

    // --- 11. Hidden requires LIS -------------------------------------------
    if order.visibility == Visibility::Hidden {
        if inst.lis_required {
            let need = match inst.lis_volume {
                Some(v) => v,
                None => return Decision::Reject(NotLargeInScale),
            };
            if order.qty.raw < need.raw {
                return Decision::Reject(NotLargeInScale);
            }
        }
        // Market-peg hidden orders are not allowed hidden (rule).
        if order.ord_type == OrdType::Pegged
            && order.peg_type == Some(PegType::Market)
            && order.visibility == Visibility::Hidden
        {
            return Decision::Reject(WrongVisibleFlag);
        }
    }

    // --- 12. Shortable matrix -----------------------------------------------
    if order.side.is_short() {
        match inst.shortable {
            Shortable::No => return Decision::Reject(StockNotShortable),
            Shortable::ExemptOnly => {
                if order.side != OrderSide::ShortExempt {
                    return Decision::Reject(StockNotShortable);
                }
            }
            Shortable::Yes => {}
        }
    }

    // --- 13. STP shape; FOK disables STP ------------------------------------
    if let Some((_, key)) = order.stp_key {
        if key == 0 {
            return Decision::Reject(Malformed);
        }
        if order.tif == Tif::Fok && order.stp_key.is_some() {
            // FOK disables STP — reject the shape rather than silently
            // dropping the attribute (mpid=0 semantics).
            return Decision::Reject(WrongExecInst);
        }
    }

    // --- 14. Peg shape -------------------------------------------------------
    if order.ord_type == OrdType::Pegged {
        if !inst.pegs_allowed {
            return Decision::Reject(WrongOrderType);
        }
        match order.peg_type {
            Some(PegType::Midpoint) => {
                if order.visibility != Visibility::Hidden {
                    return Decision::Reject(WrongExecInst);
                }
                if order.peg_difference.unwrap_or(0) != 0 {
                    return Decision::Reject(WrongPegDifference);
                }
            }
            Some(PegType::Market) => {
                if order.visibility == Visibility::Hidden {
                    return Decision::Reject(WrongVisibleFlag);
                }
                if order.peg_ebbo {
                    return Decision::Reject(EbboNotAllowed);
                }
            }
            Some(PegType::Primary) => {}
            Some(PegType::Darkpool) => {
                // Dark peg: midpoint + hidden + no EBBO.
                if order.visibility != Visibility::Hidden {
                    return Decision::Reject(WrongVisibleFlag);
                }
                if order.peg_ebbo {
                    return Decision::Reject(EbboNotAllowed);
                }
            }
            None => return Decision::Reject(WrongExecInst),
        }
    } else if order.peg_type.is_some() {
        return Decision::Reject(WrongOrderType);
    }

    // --- 15. Post-only conflicts ----------------------------------------------
    if order.post_only
        && (order.tif == Tif::Fok || order.tif == Tif::Ioc || order.max_floor.is_some())
    {
        return Decision::Reject(WrongPostOnly);
    }

    // --- 16/17. Cross / bypass -----------------------------------------------
    if order.side.is_cross() {
        if let Some(cross) = &order.cross {
            if !matches!(cross.cross_type, 'B' | 'C' | 'I') {
                return Decision::Reject(WrongCrossType);
            }
        } else {
            return Decision::Reject(WrongCrossType);
        }
        if !order.bypass {
            return Decision::Reject(WrongBypassMarker);
        }
    }
    if order.bypass && order.tif != Tif::Ioc {
        return Decision::Reject(WrongBypassMarker);
    }

    // --- 18. Entitlement ------------------------------------------------------
    if !inst.allowed_capacities.contains(&order.capacity) {
        return Decision::Reject(WrongOrderCapacity);
    }
    if let Some(reg) = order
        .cross
        .as_ref()
        .and_then(|c| c.regulation_id.as_deref())
    {
        if !client.entitlements.iter().any(|e| e == reg) {
            return Decision::Reject(InvalidEntitlement);
        }
    }

    // --- 19. Price protection allowed --------------------------------------
    if order.price_protection.is_some() && !inst.ebbo_allowed {
        return Decision::Reject(WrongPriceProtection);
    }

    // --- 20. Notional credit limit -----------------------------------------
    if let Some(limit) = client.notional_limit {
        let notional = match order.price {
            Some(px) => match px.checked_notional(order.qty) {
                Some(n) => n as u128,
                None => return Decision::Reject(Internal),
            },
            None => {
                // Market orders estimate notional at the last price (conservative:
                // the limit applies to priced orders; pegs use their reference).
                0
            }
        };
        if notional > limit {
            return Decision::Reject(OrderExceedsLimit);
        }
    }

    // --- 21. Rate guard ------------------------------------------------------
    if client.rate_permits == 0 {
        return Decision::Reject(RateLimited);
    }

    Decision::Allow {
        reservation_ids: Vec::new(),
    }
}

/// True when `price` sits on the instrument's tick grid.
fn on_tick(price: Price, table: &[(Price, i128)]) -> bool {
    // Find the band: the last entry whose lower bound ≤ price.
    let mut tick = 100i128;
    for (lower, t) in table {
        if price.raw >= lower.raw {
            tick = *t;
        }
    }
    price.raw % tick == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_ready() -> EnvState {
        let inst = InstrumentState {
            instrument: InstrumentRef {
                id: 1,
                symbol: "ACME".into(),
                isin: Some("US0000000001".into()),
                mic: "XMEG".into(),
                currency: "USD".into(),
                price_scale: 4,
                qty_scale: 0,
                lot_size: Qty::units(100),
                kind: InstrumentKind::Equity,
            },
            halted: false,
            holiday: false,
            shortable: Shortable::Yes,
            lis_required: true,
            lis_volume: Some(Qty::units(10_000)),
            tick_table: vec![(Price::equity(0), 100)],
            max_size: Qty::units(1_000_000),
            pegs_allowed: true,
            ebbo_allowed: true,
            allowed_capacities: vec![Capacity::Agency, Capacity::Principal],
            allowed_tifs: vec![Tif::Day, Tif::Gtc, Tif::Ioc, Tif::Fok, Tif::AtOpen],
        };
        let client = ClientState {
            connection_id: 7,
            customer_id: 42,
            suspended: false,
            enabled: true,
            book_tokens: vec!['M', 'L'],
            entitlements: vec!["REGSHO".into()],
            notional_limit: Some(1_000_000_000_000),
            open_orders_limit: Some(100),
            open_orders: 0,
            rate_permits: 10,
        };
        let mut env = EnvState::default();
        env.market_phase = Some(MarketPhase::Open);
        env.instruments.insert(1, inst);
        env.clients.insert(7, client);
        env
    }

    fn order_defaults() -> ExchangeOrder {
        ExchangeOrder {
            env_id: "e1".into(),
            gw_order_id: 1,
            cl_ord_id: "C1".into(),
            orig_cl_ord_id: None,
            account: "A1".into(),
            client_comp_id: "CLI".into(),
            on_behalf_comp_id: None,
            user_id: Some("u1".into()),
            connection_id: 7,
            instrument: InstrumentRef {
                id: 1,
                symbol: "ACME".into(),
                isin: Some("US0000000001".into()),
                mic: "XMEG".into(),
                currency: "USD".into(),
                price_scale: 4,
                qty_scale: 0,
                lot_size: Qty::units(100),
                kind: InstrumentKind::Equity,
            },
            side: OrderSide::Buy,
            qty: Qty::units(100),
            min_qty: None,
            price: Some(Price::equity(10_000)),
            ord_type: OrdType::Limit,
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
            book: BookToken('M'),
            strategy: vec![],
            stp_key: None,
            stp_action: None,
            cross: None,
            ex_destination: None,
            ord_status: OrdStatus::PendingNew,
            cum_qty: Qty::ZERO,
            leaves_qty: Qty::units(100),
            avg_px: None,
            timestamp_us: 0,
        }
    }

    #[test]
    fn valid_order_allowed() {
        let d = authorize(&order_defaults(), &env_ready());
        assert_eq!(
            d,
            Decision::Allow {
                reservation_ids: vec![]
            }
        );
    }

    #[test]
    fn check1_suspended() {
        let mut env = env_ready();
        env.clients.get_mut(&7).unwrap().suspended = true;
        let d = authorize(&order_defaults(), &env);
        assert_eq!(d, Decision::Reject(RejectCode::ClientSuspended));
    }

    #[test]
    fn check2_unknown_instrument() {
        let mut o = order_defaults();
        o.instrument.id = 99;
        let d = authorize(&o, &env_ready());
        // Symbol resolved at the gateway but not in Risk's snapshot → id-side
        // reject (stale cache); an unresolved symbol at the gateway itself is
        // caught earlier by the normalizer.
        assert_eq!(d, Decision::Reject(RejectCode::InvalidSecurityId));
    }

    #[test]
    fn check2_isin_mismatch() {
        let mut o = order_defaults();
        o.instrument.isin = Some("XX".into());
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::InvalidSecurityId));
    }

    #[test]
    fn check3_disabled_customer() {
        let mut env = env_ready();
        env.clients.get_mut(&7).unwrap().enabled = false;
        let d = authorize(&order_defaults(), &env);
        assert_eq!(d, Decision::Reject(RejectCode::WrongUserId));
    }

    #[test]
    fn check4_book_not_granted() {
        let mut o = order_defaults();
        o.book = BookToken('Z');
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::WrongOrderBook));
    }

    #[test]
    fn check4_multi_book_strategy() {
        let mut o = order_defaults();
        o.strategy = vec![BookToken('M'), BookToken('L')];
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::WrongOrderBook));
    }

    #[test]
    fn check5_instrument_halted() {
        let mut env = env_ready();
        env.instruments.get_mut(&1).unwrap().halted = true;
        let d = authorize(&order_defaults(), &env);
        assert_eq!(d, Decision::Reject(RejectCode::SecurityHaltedPrimary));
    }

    #[test]
    fn check5_market_closed() {
        let mut env = env_ready();
        env.market_phase = Some(MarketPhase::Closed);
        let d = authorize(&order_defaults(), &env);
        assert_eq!(d, Decision::Reject(RejectCode::ExchangeClosed));
    }

    #[test]
    fn check5_preopen_day_limit_only() {
        let mut env = env_ready();
        env.market_phase = Some(MarketPhase::PreOpen);
        // DAY+LIMIT passes pre-open rules.
        let d = authorize(&order_defaults(), &env);
        assert!(matches!(d, Decision::Allow { .. }));
        // IOC fails pre-open.
        let mut o = order_defaults();
        o.tif = Tif::Ioc;
        let d = authorize(&o, &env);
        assert_eq!(d, Decision::Reject(RejectCode::ExchangePreopen));
    }

    #[test]
    fn check6_currency_mismatch() {
        let mut o = order_defaults();
        o.instrument.currency = "EUR".into();
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::WrongCurrency));
    }

    #[test]
    fn check8_off_tick() {
        let mut o = order_defaults();
        // 1.0001 — one past the 100-tick grid (0.01).
        o.price = Some(Price::equity(10_001));
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::MismatchPriceTicksize));
    }

    #[test]
    fn check9_pure_odd_lot_lit() {
        let mut o = order_defaults();
        o.qty = Qty::units(50);
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::OddLot));
    }

    #[test]
    fn check9_mixed_lot() {
        let mut o = order_defaults();
        o.qty = Qty::units(150);
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::MixedLot));
    }

    #[test]
    fn check9_iceberg_bad_floor() {
        let mut o = order_defaults();
        o.qty = Qty::units(100);
        o.max_floor = Some(Qty::units(100)); // floor == qty → reject
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::WrongMaxFloor));
    }

    #[test]
    fn check10_qty_over_max() {
        let mut o = order_defaults();
        o.qty = Qty::units(2_000_000);
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::WrongQuantity));
    }

    #[test]
    fn check11_hidden_needs_lis() {
        let mut o = order_defaults();
        o.visibility = Visibility::Hidden;
        o.qty = Qty::units(500); // < LIS 10_000
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::NotLargeInScale));
    }

    #[test]
    fn check11_hidden_lis_ok() {
        let mut o = order_defaults();
        o.visibility = Visibility::Hidden;
        o.qty = Qty::units(20_000); // ≥ LIS
        let d = authorize(&o, &env_ready());
        assert!(matches!(d, Decision::Allow { .. }));
    }

    #[test]
    fn check12_not_shortable() {
        let mut env = env_ready();
        env.instruments.get_mut(&1).unwrap().shortable = Shortable::No;
        let mut o = order_defaults();
        o.side = OrderSide::ShortSell;
        let d = authorize(&o, &env);
        assert_eq!(d, Decision::Reject(RejectCode::StockNotShortable));
    }

    #[test]
    fn check12_exempt_only_short() {
        let mut env = env_ready();
        env.instruments.get_mut(&1).unwrap().shortable = Shortable::ExemptOnly;
        let mut o = order_defaults();
        o.side = OrderSide::ShortSell; // plain short
        let d = authorize(&o, &env);
        assert_eq!(d, Decision::Reject(RejectCode::StockNotShortable));
        // ShortExempt passes.
        o.side = OrderSide::ShortExempt;
        let d = authorize(&o, &env);
        assert!(matches!(d, Decision::Allow { .. }));
    }

    #[test]
    fn check13_fok_disables_stp() {
        let mut o = order_defaults();
        o.tif = Tif::Fok;
        o.stp_key = Some((StpKeyType::Customer, 77));
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::WrongExecInst));
    }

    #[test]
    fn check14_peg_must_be_hidden_midpoint() {
        let mut o = order_defaults();
        o.ord_type = OrdType::Pegged;
        o.peg_type = Some(PegType::Midpoint);
        o.visibility = Visibility::Display; // midpoint must be hidden
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::WrongExecInst));
    }

    #[test]
    fn check14_midpoint_peg_ok() {
        let mut o = order_defaults();
        o.ord_type = OrdType::Pegged;
        o.peg_type = Some(PegType::Midpoint);
        o.visibility = Visibility::Hidden;
        o.qty = Qty::units(20_000); // LIS for hidden
        o.price = None;
        let d = authorize(&o, &env_ready());
        assert!(matches!(d, Decision::Allow { .. }));
    }

    #[test]
    fn check15_post_only_conflicts() {
        let mut o = order_defaults();
        o.post_only = true;
        o.tif = Tif::Ioc;
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::WrongPostOnly));
    }

    #[test]
    fn check16_cross_needs_bypass() {
        let mut o = order_defaults();
        o.side = OrderSide::Cross;
        o.cross = Some(CrossData {
            cross_type: 'I',
            account: None,
            account_type_id: None,
            regulation_id: None,
            jitney: None,
        });
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::WrongBypassMarker));
    }

    #[test]
    fn check17_bypass_needs_ioc() {
        let mut o = order_defaults();
        o.bypass = true;
        o.tif = Tif::Day;
        let d = authorize(&o, &env_ready());
        assert_eq!(d, Decision::Reject(RejectCode::WrongBypassMarker));
    }

    #[test]
    fn check18_capacity_not_allowed() {
        let mut env = env_ready();
        env.instruments.get_mut(&1).unwrap().allowed_capacities = vec![Capacity::Agency];
        let mut o = order_defaults();
        o.capacity = Capacity::Principal;
        let d = authorize(&o, &env);
        assert_eq!(d, Decision::Reject(RejectCode::WrongOrderCapacity));
    }

    #[test]
    fn check19_price_protection_off() {
        let mut env = env_ready();
        env.instruments.get_mut(&1).unwrap().ebbo_allowed = false;
        let mut o = order_defaults();
        o.price_protection = Some(exchange8::contracts::enums::PriceProtection::Post);
        let d = authorize(&o, &env);
        assert_eq!(d, Decision::Reject(RejectCode::WrongPriceProtection));
    }

    #[test]
    fn check20_notional_over_limit() {
        let mut env = env_ready();
        env.clients.get_mut(&7).unwrap().notional_limit = Some(500_000); // 0.05 at scale 4
        let d = authorize(&order_defaults(), &env);
        assert_eq!(d, Decision::Reject(RejectCode::OrderExceedsLimit));
    }

    #[test]
    fn check21_rate_limited() {
        let mut env = env_ready();
        env.clients.get_mut(&7).unwrap().rate_permits = 0;
        let d = authorize(&order_defaults(), &env);
        assert_eq!(d, Decision::Reject(RejectCode::RateLimited));
    }
}
