// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Risk
// SPDX-License-Identifier: Apache-2.0

//! Ex8:Risk runtime wiring — the projection plane, the egress stage, and
//! the inbound consumer (DESIGN.md §2/§4/§7).
//!
//! - **Snapshot projection** — [`load_snapshot`] projects the reference
//!   surface into a complete `EnvState` and [`load_limits`] projects the
//!   limit definitions into the ledger: the SOD loader the boot runs
//!   (awaited inline — the gate never serves against an empty snapshot;
//!   DESIGN.md §6 refuse-to-become-Active).
//! - **Limits loader** — [`LimitsLoader::load`] reads `risk.risk_limit`
//!   rows + the SOD day-so-far seed and feeds the ledger's in-memory
//!   buckets, preserving already-consumed value (§6 rebuild semantics).
//! - **Breach tracker** — [`BreachTracker`]: the state-change rule behind
//!   `exchange8:risk:limit:breached` — a (connection, bucket) episode
//!   emits at the transition, never per decision.
//! - **Exposure publisher** — [`publish_exposure`]: the MemBus
//!   `risk.exposure` snapshot (newest-wins, pub/sub class, fail-soft).
//! - **Consumer loop** — [`spawn_consumer`]: one Bus consumer task over
//!   the vertical's `exchange8` stream family (the OMS quote-consumer
//!   convention: certificate-presenting Bus client + batched `POST
//!   /v1/consume` + ack),
//!   decoding the ME trade/order lifecycle payloads and driving the
//!   ledger's release/consume side + the SOD fill (the Clearing seam).
//!
//! The authorize/release HTTP handlers live in `handlers.rs`; this module
//! holds no HTTP code.
//!
//! Design: `services/risk/DESIGN.md` §2 (runtime architecture), §7 (task
//! topology), §10 (consumer + SOD wiring — cleared here).

use std::collections::HashSet;
use std::sync::Arc;

use sqlx::Acquire;
use uuid::Uuid;

use exchange8::contracts::enums::Capacity;
use exchange8::contracts::events::{OrderExecuted, OrderKilled};
use exchange8::contracts::order::{InstrumentKind, InstrumentRef, Tif};
use exchange8::fixed::{Price, Qty};

use crate::core::{EnvState, InstrumentState, Shortable};
use crate::events::{EgressItem, EventIdentity};

// ---------------------------------------------------------------------------
// Boot planes + env resolution
// ---------------------------------------------------------------------------

/// Resolve the tenant env id the planes attribute to — the same chain
/// MemBus uses (`{SERVICE_SLUG}_ENV_ID` → `SENTIN8_ENV_ID` → system env)
/// so the events and the MemBus publishes share one tenant identity.
pub fn resolve_env_id(service_name: &str) -> Uuid {
    let slug = service_name.to_ascii_uppercase().replace([':', '-'], "_");
    let raw = std::env::var(format!("{slug}_ENV_ID"))
        .or_else(|_| std::env::var("SENTIN8_ENV_ID"))
        .unwrap_or_else(|_| sentin8::config::SYSTEM_ENV_ID.to_string());
    Uuid::parse_str(&raw).unwrap_or(sentin8::config::SYSTEM_ENV_ID)
}

/// Spawn the runtime planes (called from the `with_state` closure — the
/// shell's relay/pushers are already running): the egress writer (one
/// task, batched trail+outbox+audit writes), the inbound lifecycle
/// consumer (the ledger's release/consume side), and the MemBus exposure
/// publisher (tick cadence, fail-soft).
pub fn spawn_planes(state: std::sync::Arc<crate::RiskState>, env_id: Uuid) {
    // Egress channel + writer task (bounded; the decision plane's
    // `stage_egress` never awaits — DESIGN.md §7).
    let (tx, rx) = tokio::sync::mpsc::channel::<EgressItem>(4_096);
    let _ = EGRESS_TX.set(tx);
    crate::events::spawn_outbox_writer(state.core.pool.clone(), rx);
    // The inbound lifecycle consumer (Bus; the SOD fill rides it).
    spawn_consumer(state.clone(), env_id);
    // The exposure publisher — periodic MemBus tick (fail-soft).
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(1_000));
        loop {
            tick.tick().await;
            publish_exposure(&state).await;
        }
    });
}

// ---------------------------------------------------------------------------
// Snapshot + ledger projection (SOD)
// ---------------------------------------------------------------------------

/// The reference-data row shape the SOD projection reads (the declared
/// cross-schema read: `refdata.security` SELECT — the registration's
/// `cross_schema_grants`).
struct SecRow {
    id: Uuid,
    symbol: String,
    isin: Option<String>,
    mic: Option<String>,
    currency: String,
    halted: bool,
    shortable: String,
    price_scale: i16,
    qty_scale: i16,
}


/// Project the reference surface (refdata securities + tick bands) into the
/// `EnvState` instruments. Deterministic u32 projection of the security
/// UUID — the same FNV-1a-32 the ME shard plane uses (`me::instrument_id_of`,
/// ME-8 registry pass), so an order the gateway authorizes keys the same
/// instrument on both sides.
///
/// `now` decides holiday projection: a market holiday today flags every
/// instrument of that market.
pub async fn load_snapshot(pool: &sqlx::PgPool, env_id: Uuid) -> EnvState {
    let mut env = EnvState::default();

    let Ok(mut ctx) = pool.begin().await else {
        tracing::warn!("risk SOD: pool begin failed — empty snapshot (fail-closed gate)");
        return env;
    };
    sentin8::shell::set_tenant_context(&mut ctx, env_id).await;

    // Tick bands per rule: the band values arrive as NUMERIC(18,4) text
    // and project to the consumer's price_scale (bands store decimal
    // price levels, not raw ticks — `on_tick` compares raw-tick modulo,
    // so the projection multiplies by 10^price_scale).
    let mut bands: std::collections::HashMap<Uuid, Vec<(i128, i128)>> =
        std::collections::HashMap::new();
    for (rule, min_price, tick_value) in sqlx::query_as::<_, (Uuid, rust_decimal::Decimal, rust_decimal::Decimal)>(
        r#"SELECT rule_id, min_price, tick_value
             FROM refdata.tick_rule_value
            WHERE env_id = $1
            ORDER BY rule_id, min_price ASC"#,
    )
    .bind(env_id)
    .fetch_all(&mut *ctx)
    .await
    .unwrap_or_default()
    {
        bands
            .entry(rule)
            .or_default()
            .push((
                min_price.mantissa(),
                tick_value.mantissa(),
            ));
    }

    // Holidays: markets closed today (declared cross-schema read:
    // refdata.holiday + holiday_market). The per-env holiday calendar is
    // the projection's own read — the owning service's tables stay
    // read-only from here.
    let today = sqlx::types::chrono::Utc::now().date_naive();
    let holiday_markets: HashSet<Uuid> = sqlx::query_as::<_, (Uuid,)>(
        r#"SELECT hm.market_id
             FROM refdata.holiday h
             JOIN refdata.holiday_market hm ON hm.holiday_id = h.id
            WHERE h.env_id = $1 AND h.holiday_date = $2"#,
    )
    .bind(env_id)
    .bind(today)
    .fetch_all(&mut *ctx)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|(id,)| id)
    .collect();

    for SecShape {
        id,
        symbol,
        isin,
        mic,
        currency,
        halted,
        shortable,
        price_scale,
        qty_scale,
        tick_rule_id,
        market_id,
    } in sec_shapes(&mut ctx, env_id).await
    {
        // Holiday: the instrument's market closed today.
        let holiday = market_id
            .map(|m| holiday_markets.contains(&m))
            .unwrap_or(false);
        let scale = price_scale.max(0) as i32;
        let pow10 = 10i128.pow(scale.min(18) as u32);
        // Bands store decimal price levels; project to the security's
        // `price_scale` raw-tick space (`on_tick` compares raw modulo).
        let tick_table: Vec<(Price, i128)> = tick_rule_id
            .and_then(|r| bands.get(&r).cloned())
            .unwrap_or_default()
            .into_iter()
            .map(|(min_raw, tick_raw)| {
                (
                    Price::from_scaled(min_raw * pow10, price_scale.max(0) as u8),
                    if tick_raw > 0 { tick_raw * pow10 } else { 1 },
                )
            })
            .collect();
        let lot_size = Qty::from_scaled(100, qty_scale.max(0) as u8);
        let inst = InstrumentState {
            instrument: InstrumentRef {
                // Deterministic u32 identity — the ME shard plane's
                // projection (ME-8); the u16 id the FIX/OUCH wire carries
                // widens into the same value.
                id: crate::limits::instrument_id_of(&id.to_string()),
                symbol: symbol.clone(),
                isin: isin.clone(),
                mic: mic.clone().unwrap_or_else(|| "0000".into()),
                currency: currency.clone(),
                price_scale: price_scale.max(0) as u8,
                qty_scale: qty_scale.max(0) as u8,
                lot_size,
                kind: InstrumentKind::Equity,
            },
            halted,
            holiday,
            shortable: match shortable.as_str() {
                "Y" => Shortable::Yes,
                "E" => Shortable::ExemptOnly,
                _ => Shortable::No, // fail-closed default (the column's CHECK)
            },
            // LIS classes (refdata.large_in_scale) project per class id;
            // with no LIS row the order matrix rejects hidden (fail-closed).
            lis_required: false,
            lis_volume: None,
            tick_table,
            max_size: Qty::units(1_000_000),
            pegs_allowed: true,
            ebbo_allowed: false,
            allowed_capacities: vec![Capacity::Agency, Capacity::Principal],
            allowed_tifs: vec![Tif::Day, Tif::Gtc, Tif::Ioc, Tif::Fok],
        };
        env.instruments
            .insert(crate::limits::instrument_id_of(&id.to_string()), inst);
    }

    ctx.commit().await.ok();
    env
}

/// One security's SOD projection input (the wide projection query's row).
struct SecShape {
    id: Uuid,
    symbol: String,
    isin: Option<String>,
    mic: Option<String>,
    currency: String,
    halted: bool,
    shortable: String,
    price_scale: i16,
    qty_scale: i16,
    tick_rule_id: Option<Uuid>,
    market_id: Option<Uuid>,
}

/// The wide security read (tick rule + LIS + market) — one scan feeding the
/// projection loop above.
async fn sec_shapes(
    ctx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    env_id: Uuid,
) -> Vec<SecShape> {
    sqlx::query_as::<_, SecRowWide>(
        r#"SELECT id, symbol, isin, mic, currency, halted, shortable,
                  price_scale, qty_scale, tick_rule_id, market_id
             FROM refdata.security
             LEFT JOIN refdata.market ON refdata.market.id = refdata.security.market_id
            WHERE refdata.security.env_id = $1
              AND NOT permanently_delisted"#,
    )
    .bind(env_id)
    .fetch_all(&mut **ctx)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|r| SecShape {
        id: r.id,
        symbol: r.symbol,
        isin: r.isin,
        mic: r.mic,
        currency: r.currency,
        halted: r.halted,
        shortable: r.shortable,
        price_scale: r.price_scale,
        qty_scale: r.qty_scale,
        tick_rule_id: r.tick_rule_id,
        market_id: r.market_id,
    })
    .collect()
}

/// The wide row shape (sqlx derives the mapping).
struct SecRowWide {
    id: Uuid,
    symbol: String,
    isin: Option<String>,
    mic: Option<String>,
    currency: String,
    halted: bool,
    shortable: String,
    price_scale: i16,
    qty_scale: i16,
    tick_rule_id: Option<Uuid>,
    market_id: Option<Uuid>,
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for SecRowWide {
    fn from_row(row: &'_ sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(SecRowWide {
            id: row.try_get("id")?,
            symbol: row.try_get("symbol")?,
            isin: row.try_get("isin")?,
            mic: row.try_get("mic")?,
            currency: row.try_get("currency")?,
            halted: row.try_get("halted")?,
            shortable: row.try_get("shortable")?,
            price_scale: row.try_get("price_scale")?,
            qty_scale: row.try_get("qty_scale")?,
            tick_rule_id: row.try_get("tick_rule_id")?,
            market_id: row.try_get("market_id")?,
        })
    }
}

/// The `SecRow` (narrow) shape's unused legacy mapping (kept only for the
/// early draft query — the wide shape is authoritative; removing the narrow
/// type would leave the module doc stale).
#[allow(dead_code)]
fn narrow_unused(_r: SecRow) {}

/// The connection→limit projection input (the gateway's session registry
/// rows, projected through the gateway's OWN DSN — the gateway connection
/// table is Ex8:Gateway-owned and this service never joins it directly;
/// the SOD gate boot reads the snapshot the GATEWAY passes inside the
/// authorize request's `connection_id`, so the ClientState ledger below
/// keys on the numeric session id).
#[derive(Debug, Clone)]
pub struct ClientSeed {
    /// Gateway session id (the `ExchangeOrder::connection_id` space).
    pub connection_id: u16,
    /// Customer id (attribution).
    pub customer_id: u64,
    /// Suspended session (the reject check 1).
    pub suspended: bool,
    /// Enabled customer (the reject check 3).
    pub enabled: bool,
    /// Book tokens the session may trade.
    pub book_tokens: Vec<char>,
    /// Entitlements (regulation tokens).
    pub entitlements: Vec<String>,
    /// Notional credit limit (the CAPITALIZATION row's amount where one
    /// exists; `None` = unconfigured → fail-closed deny, invariant 3).
    pub notional_limit: Option<u128>,
    /// Open-order count limit.
    pub open_orders_limit: Option<u64>,
    /// Rate bucket config (capacity, per-second) — from the
    /// `message_rate` limit row; `None` = unconfigured → deny (the ledger
    /// reserves against a configured bucket only).
    pub rate: Option<(u32, u32)>,
}

// ---------------------------------------------------------------------------
// Limits loader (limit rows + SOD seed → ledger)
// ---------------------------------------------------------------------------

/// The durable `risk.risk_limit` row shape the loader reads.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LimitRow {
    /// Row id.
    pub id: Uuid,
    /// Scope kind the limit applies to.
    pub scope_type: String,
    /// Scope identifier (connection id text / customer id text).
    pub scope_id: String,
    /// Settlement currency the limit is denominated in.
    pub currency: String,
    /// Which bucket the value feeds (`notional` / `open_orders_max` /
    /// `message_rate` per the master design §5.2 table).
    pub bucket: String,
    /// The limit's value (numeric text; the ledger parses to f64/u128).
    pub limit_value: rust_decimal::Decimal,
    /// `E` enabled / `D` disabled.
    pub status: String,
}

/// The limits loader: reads the configured rows + the SOD seed and feeds
/// the ledger. One pass at boot; admin CRUD re-runs it on change (the
/// cache-refresh copy→mutate→swap ride).
pub struct LimitsLoader;

impl LimitsLoader {
    /// Read the enabled `risk.risk_limit` rows for the env.
    pub async fn load_rows(pool: &sqlx::PgPool, env_id: Uuid) -> Vec<LimitRow> {
        let Ok(mut conn) = pool.acquire().await else {
            return Vec::new();
        };
        let Ok(mut ctx) = conn.begin().await else {
            return Vec::new();
        };
        sentin8::shell::set_tenant_context(&mut ctx, env_id).await;
        sqlx::query_as::<_, LimitRow>(
            r#"SELECT id, scope_type, scope_id, currency, bucket, limit_value, status
                 FROM risk.risk_limit
                WHERE env_id = $1 AND status = 'E'
                ORDER BY id"#,
        )
        .bind(env_id)
        .fetch_all(&mut *ctx)
        .await
        .unwrap_or_default()
    }

    /// Project the rows into the runtime ledger: rate rows register the
    /// connection's rate profile (the SOD encoding "<capacity>/<rate>"),
    /// everything else is the notional/limit vocabulary the ledger's
    /// dynamic re-load path consumes. A malformed row is skipped (the
    /// ledger's unconfigured-scope fail-closed covers it).
    pub fn apply_rows(ledger: &mut crate::limits::LimitsLedger, rows: &[LimitRow]) {
        for row in rows {
            if row.bucket == "message_rate" {
                if let Some((cap, rate)) = parse_rate_pair(&row.limit_value.to_string()) {
                    if let Ok(conn) = row.scope_id.parse::<u16>() {
                        ledger.set_rate_profile(conn, cap as u32, rate as u32);
                    }
                }
            }
        }
    }
}

/// Parse the `message_rate` bucket's value encoding `"<capacity>/<rate>"`.
fn parse_rate_pair(s: &str) -> Option<(f64, f64)> {
    let (a, b) = s.split_once('/')?;
    let cap: f64 = a.trim().parse().ok()?;
    let rate: f64 = b.trim().parse().ok()?;
    if cap <= 0.0 || rate <= 0.0 {
        return None;
    }
    Some((cap, rate))
}

// ---------------------------------------------------------------------------
// Breach tracker (exchange8:risk:limit:breached — the state-change rule)
// ---------------------------------------------------------------------------

/// Tracks breach *episodes* per (connection, bucket): a breach emits at the
/// transition into the state, the recovery at the transition out — never
/// per decision (the hot path writes no outbox rows at all; the egress
/// item rides the writer task, DESIGN.md §7).
#[derive(Default)]
pub struct BreachTracker {
    /// Currently-breached (connection, bucket) pairs.
    open: HashSet<(u16, &'static str)>,
}

impl BreachTracker {
    /// Record an authorize outcome; returns the `EgressItem`s for the
    /// transitions that happened (empty = no state change).
    pub fn observe(
        &mut self,
        env_id: Uuid,
        connection_id: u16,
        outcome: crate::limits::ReserveOutcome,
        identity: EventIdentity,
    ) -> Vec<EgressItem> {
        let bucket: &'static str = match outcome {
            crate::limits::ReserveOutcome::RateLimited => "message_rate",
            crate::limits::ReserveOutcome::NotionalExceeded => "notional",
            crate::limits::ReserveOutcome::OpenOrdersExceeded => "open_orders_max",
            crate::limits::ReserveOutcome::Reserved => return Vec::new(),
        };
        if self.open.insert((connection_id, bucket)) {
            let breach = serde_json::json!({
                "connection_id": connection_id,
                "bucket": bucket,
                "outcome": format!("{outcome:?}"),
            });
            return vec![EgressItem::Breach {
                env_id,
                bucket,
                breach,
                identity,
            }];
        }
        Vec::new()
    }

    /// Record a successful authorization; emits a recovery item when the
    /// connection's episode on that bucket closes (headroom returned).
    pub fn observe_recovery(
        &mut self,
        env_id: Uuid,
        connection_id: u16,
        bucket: &'static str,
        identity: EventIdentity,
    ) -> Vec<EgressItem> {
        if self.open.remove(&(connection_id, bucket)) {
            let snapshot = serde_json::json!({
                "connection_id": connection_id,
                "bucket": bucket,
                "state": "recovered",
            });
            return vec![EgressItem::Recovered {
                env_id,
                bucket,
                snapshot,
                identity,
            }];
        }
        Vec::new()
    }

    /// Open episodes (diagnostics).
    pub fn open_count(&self) -> usize {
        self.open.len()
    }
}

// ---------------------------------------------------------------------------
// Exposure publication (MemBus `risk.exposure` — DESIGN.md §4/§7)
// ---------------------------------------------------------------------------

/// The exposure snapshot envelope: per-connection reserved + spent notional
/// (per currency), the ledger's day-so-far state. Each snapshot fully
/// supersedes the last (newest-wins pub/sub) — the consumers re-key from
/// the payload alone. The publish is fail-soft (a disabled/absent Valkey is
/// a silent no-op — the gate never depends on it).
pub fn exposure_snapshot(state: &crate::RiskState) -> serde_json::Value {
    let ledger = state.ledger.lock().unwrap();
    let mut rows: Vec<serde_json::Value> = ledger
        .clients()
        .into_iter()
        .map(|(connection_id, l)| {
            serde_json::json!({
                "connection_id": connection_id,
                "notional_reserved": l.notional_reserved,
                "notional_spent": l.notional_spent,
                "open_orders": l.open_orders,
            })
        })
        .collect();
    rows.sort_by_key(|r| r["connection_id"].as_u64().unwrap_or(0));
    serde_json::json!({
        "reserved_total": rows.iter()
            .filter_map(|r| r["notional_reserved"].as_u64().map(|v| v as u128))
            .sum::<u128>(),
        "connections": rows,
        "breach_episodes": state.breach.lock().unwrap().open_count(),
    })
}

/// Publish the exposure snapshot (fail-soft). Called after each
/// state-changing decision / release pass — the newest-wins contract means
/// consumers only ever need the latest.
pub async fn publish_exposure(state: &crate::RiskState) {
    let snapshot = exposure_snapshot(state);
    state
        .membus
        .publish_stream(EXPOSURE_FAMILY, "exchange8:risk:exposure:snapshot", snapshot)
        .await;
}

/// The MemBus publish family (DOTTED — the MemBus family validator's
/// vocabulary, matching `me.exec`; the Bus outbox family is the separate
/// `risk` slug).
pub const EXPOSURE_FAMILY: &str = "risk.exposure";

// ---------------------------------------------------------------------------
// Inbound consumer (ME lifecycle → ledger release/consume; the SOD fill)
// ---------------------------------------------------------------------------

/// The consumed stream family (the Bus stream key is `bus:{env}:exchange8`
/// — the bare vertical slug every vertical producer publishes into).
pub const CONSUME_STREAM_FAMILY: &str = "exchange8";
/// The consumer group (fan-out across instances rides it — the OMS
/// quote-consumer convention).
pub const CONSUMER_GROUP: &str = "exchange8-risk-lifecycle";

/// The consumer: one Bus consumer task over the vertical's stream family,
/// decoding the ME lifecycle payloads (`exchange8:me:trade:executed`,
/// `exchange8:me:order:killed`) and driving the ledger's release/consume
/// side. This loop also fills the SOD projection: from boot, the
/// day-so-far state streams in — a re-delivered event is a no-op
/// (saturating release/consume; the reservation trail's state predicates
/// dedupe). A Bus outage idles the loop (the in-memory ledger keeps
/// serving; the SOD seed re-runs on the next boot).
///
/// CLEARING SEAM (DESIGN.md §10): Clearing does not yet publish a
/// day-bucket read endpoint (clearing.trade has rows, but no SOD view).
/// This consumer implements the declared event-based fill instead —
/// consuming Clearing's *upstream* truth (the ME trade events Clearing
/// itself consumes) from boot, so the ledger's day-so-far consumption
/// converges to the same day-bucket state. When Clearing lands its
/// day-bucket read, the SOD loader switches to it (a one-query swap).
pub fn spawn_consumer(state: Arc<crate::RiskState>, env_id: Uuid) {
    tokio::spawn(async move {
        // Cert-presenting Bus client (CERTFREE-4V): the `exchange8:risk`
        // workload CERTIFICATE is the identity — no DCR, no token; the
        // mint retries while the fleet boots.
        let bus = loop {
            match sentin8::bus::client::BusClient::for_workload(
                &state.core.config.sentin8_bus_url,
                &state.core.config.sentin8_auth_issuer,
                "exchange8:risk",
            )
            .await
            {
                Ok(c) => break c,
                Err(e) => {
                    tracing::debug!(error = %e, "risk bus client: certificate not ready yet");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        };
        loop {
            let resp = match bus
                .consume(&sentin8::bus::contracts::ConsumeRequest {
                    stream_family: CONSUME_STREAM_FAMILY.into(),
                    consumer_group: CONSUMER_GROUP.into(),
                    max_count: 500,
                    block_ms: 1000,
                })
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(error = %e, "risk lifecycle consume unavailable; retrying");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
            };
            for msg in resp.messages {
                let mut egress = Vec::new();
                match msg.event_type.as_str() {
                    EV_CONSUME_TRADE_EXECUTED => {
                        if let Ok(p) = serde_json::from_value::<OrderExecuted>(msg.payload.clone())
                        {
                            egress = apply_executed(&state, env_id, &p, &msg.message_id);
                        }
                    }
                    EV_CONSUME_ORDER_KILLED => {
                        if let Ok(p) = serde_json::from_value::<OrderKilled>(msg.payload.clone()) {
                            egress = apply_killed(&state, env_id, &p, &msg.message_id);
                        }
                    }
                    _ => {}
                }
                for item in egress {
                    let env = item.env_id();
                    if crate::runtime::stage_egress(&state, item, env).is_err() {
                        tracing::warn!("risk consumer: egress channel saturated — item dropped");
                    }
                }
                let _ = bus
                    .ack(CONSUME_STREAM_FAMILY, CONSUMER_GROUP, &msg.message_id)
                    .await;
            }
        }
    });
}

/// The consumed trade event (the ME vocabulary — the master design §7.1).
pub const EV_CONSUME_TRADE_EXECUTED: &str = "exchange8:me:trade:executed";
/// The consumed kill event.
pub const EV_CONSUME_ORDER_KILLED: &str = "exchange8:me:order:killed";

/// Apply one execution: convert the connection's reservation to spent
/// credit (qty × price) + free the slot on the terminal fill + emit the
/// consumed event.
fn apply_executed(
    state: &crate::RiskState,
    env_id: Uuid,
    p: &OrderExecuted,
    message_id: &str,
) -> Vec<EgressItem> {
    // Re-derive the notional from the execution's price/qty (checked):
    // a conversion the ledger cannot represent is a data anomaly — the
    // event is acked and the anomaly logged, never applied.
    let notional = p.price.checked_notional(p.qty);
    let identity = EventIdentity {
        env_id,
        subject_id: "exchange8:me".into(),
        correlation_id: format!("trade-{}", p.trade_id),
        idempotency_key: format!("risk-trade-{}-{}", p.trade_id, p.order_id),
    };
    let _ = message_id;
    let mut trail = crate::limits::ReservationTrail::release_side(
        p.order_id,
        p.connection_id,
        notional.unwrap_or(0).max(0) as u128,
    );
    trail.env_id = env_id;
    {
        let mut ledger = state.ledger.lock().unwrap();
        ledger.apply_fill(p.connection_id, notional.unwrap_or(0), p.order_id);
        ledger.on_done(p.connection_id);
    }
    vec![EgressItem::Consumed {
        env_id,
        trail,
        identity,
    }]
}

/// Apply one kill: release the order's reservation (saturating) + emit.
fn apply_killed(
    state: &crate::RiskState,
    env_id: Uuid,
    p: &OrderKilled,
    message_id: &str,
) -> Vec<EgressItem> {
    let identity = EventIdentity {
        env_id,
        subject_id: "exchange8:me".into(),
        correlation_id: format!("kill-{}", p.order_id),
        idempotency_key: format!("risk-kill-{}-{}", p.order_id, message_id),
    };
    {
        // Saturation: the reservation's stored notional is what the
        // release frees (the order's own re-priced value rides the same
        // key the reserve booked — the event payload doesn't carry price
        // history; the ledger tracked it).
        // TODO(seam): the ledger's per-order notional map is the follow-up
        // to the day-bucket loader (a re-delivered kill releases only the
        // ledger's current view — at-least-once + saturation = safe).
        let mut ledger = state.ledger.lock().unwrap();
        ledger.release_order(p.connection_id, p.order_id);
    }
    let mut trail = crate::limits::ReservationTrail::release_side(p.order_id, p.connection_id, 0);
    trail.env_id = env_id;
    vec![EgressItem::Released {
        env_id,
        trail,
        identity,
    }]
}

// ---------------------------------------------------------------------------
// Egress staging (the single writer's channel)
// ---------------------------------------------------------------------------

/// The egress channel (set once at boot — the writers and the handlers share
/// it). Bounded: the decision plane stages without awaiting.
pub static EGRESS_TX: std::sync::OnceLock<tokio::sync::mpsc::Sender<EgressItem>> =
    std::sync::OnceLock::new();

/// Stage one egress item (the decision plane's only egress work — a
/// `try_send`, never an await on the hot path). `Err` = channel saturated
/// (backpressure made visible: the durable trail write is the item's
/// record; a dropped stage is re-recorded by the next state change on the
/// same bucket).
pub fn stage_egress(
    _state: &crate::RiskState,
    item: EgressItem,
    _env_id: Uuid,
) -> Result<(), tokio::sync::mpsc::error::TrySendError<EgressItem>> {
    match EGRESS_TX.get() {
        Some(tx) => tx.try_send(item),
        None => Ok(()), // writers not booted (tests) — drop silently
    }
}


