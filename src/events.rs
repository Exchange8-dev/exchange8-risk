// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Risk
// SPDX-License-Identifier: Apache-2.0

//! Ex8:Risk typed event envelopes (DBX producer path).
//!
//! - **Vocabulary + envelope builders** — the `exchange8:risk:{resource}:
//!   {operation}` names, the `EventIdentity` block, and the
//!   `limit_event` / `reservation_event` builders the handler and the
//!   runtime stage call. The outbox row goes in the SAME transaction as
//!   the domain write via [`emit_outbox`]
//!   (`insert_outbox_row_with_publisher`); the shell's relay (boot 8b)
//!   pushes it to Bus `POST /v1/produce`; the audit pusher (8c) drains the
//!   journal rows the `journal_mutation` helper writes on the same tx.
//! - **Egress channel** — the synchronous gate never queues behind egress
//!   (DESIGN.md §7): a decision stages an [`EgressItem`] on a bounded
//!   channel and returns; the one `spawn_outbox_writer` task batches the
//!   items (512 events / 5 ms) into the `risk.risk_reservation` trail +
//!   `risk.outbox` + `risk.audit_journal` rows under one RLS-contexted
//!   transaction per env group (the ME outbox writer's batching).
//!
//! Stream family: `risk` — the Bus stream key is `bus:{env_id}:risk`; the
//! family slug is validated by `sentin8::bus::outbox::validate_stream_family`
//! (colons are structurally impossible in a family — the event NAMES carry
//! them; the gateway/me sibling convention). The MemBus snapshot family is
//! separate (`risk.exposure`, the runtime module publishes it).
//!
//! Design: `services/risk/DESIGN.md` §4 (event vocabulary + flow), §7
//! (egress backpressure).

use serde_json::{json, Value};
use uuid::Uuid;

/// Stream family for all risk-emitted events (bare service slug — the Bus
/// `XADD bus:{env}:risk` key; colons are structurally impossible per
/// `sentin8::bus::outbox::validate_stream_family`).
pub const STREAM_FAMILY: &str = "risk";

// ── Event vocabulary (DESIGN.md §4 table — colon segments only) ─────────

/// `exchange8:risk:limit:created` — a limit definition row was inserted.
pub const EV_LIMIT_CREATED: &str = "exchange8:risk:limit:created";
/// `exchange8:risk:limit:updated` — a limit definition row was updated
/// (value or status; the master design §8: every limit change emits this).
pub const EV_LIMIT_UPDATED: &str = "exchange8:risk:limit:updated";
/// `exchange8:risk:limit:deleted` — a limit definition row was deleted.
pub const EV_LIMIT_DELETED: &str = "exchange8:risk:limit:deleted";
/// `exchange8:risk:limit:breached` — a connection's bucket transitioned
/// into breach (throttle or limit exceeded; written on the state change,
/// never per decision — DESIGN.md §4).
pub const EV_LIMIT_BREACHED: &str = "exchange8:risk:limit:breached";
/// `exchange8:risk:reservation:reserved` — an authorized order booked its
/// notional reservation (same-tx trail row).
pub const EV_RESERVATION_RESERVED: &str = "exchange8:risk:reservation:reserved";
/// `exchange8:risk:reservation:released` — a reservation was released
/// (kill / cancel / admin correction; replay-safe release).
pub const EV_RESERVATION_RELEASED: &str = "exchange8:risk:reservation:released";
/// `exchange8:risk:reservation:consumed` — a fill converted a reservation
/// to spent credit (same-tx trail row state advance).
pub const EV_RESERVATION_CONSUMED: &str = "exchange8:risk:reservation:consumed";

/// Which limit-definition mutation produced the event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitOp {
    /// Row inserted.
    Created,
    /// Row updated (value or status).
    Updated,
    /// Row deleted.
    Deleted,
}

/// Which reservation transition produced the event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservationOp {
    /// Notional booked at authorize.
    Reserved,
    /// Released (kill / cancel / correction).
    Released,
    /// Converted to spent credit by a fill.
    Consumed,
}

/// The identity carried inside every envelope (decision/admin owned, never
/// derived from DB or headers — event-first: consumers react from the
/// payload alone).
/// The identity block every emitted event carries (DESIGN.md §4 —
/// payload completeness: consumers dedupe/react from the envelope alone).
#[derive(Clone, Debug)]
pub struct EventIdentity {
    /// Tenant environment of the event.
    pub env_id: Uuid,
    /// Acting subject (the caller's verified `sub` — the gateway workload
    /// for authorize, the admin user for corrections).
    pub subject_id: String,
    /// Correlation id (`X-Sentin8-Correlation-Id` when present; otherwise
    /// generated — direct M2M calls).
    pub correlation_id: String,
    /// Idempotency key: consumers dedupe on this (the reservation events
    /// key on the order id; breach events on the transition itself).
    pub idempotency_key: String,
}

/// Envelope for a limit-definition mutation: `snapshot` = the FULL
/// `risk.risk_limit` row (consumers — Clearing's audit mirror, ops — react
/// from the payload alone).
pub fn limit_event(op: LimitOp, limit_row: &Value, identity: &EventIdentity) -> Value {
    let event_type = match op {
        LimitOp::Created => EV_LIMIT_CREATED,
        LimitOp::Updated => EV_LIMIT_UPDATED,
        LimitOp::Deleted => EV_LIMIT_DELETED,
    };
    json!({
        "event_type": event_type,
        "env_id": identity.env_id,
        "subject_id": identity.subject_id,
        "correlation_id": identity.correlation_id,
        "idempotency_key": identity.idempotency_key,
        "snapshot": limit_row,
    })
}

/// Envelope for a reservation transition: the order identity + the bucket
/// the reservation carries, the notional value, and the trail state — the
/// consumer (Clearing's day-bucket mirror / Surveillance) reacts from the
/// payload alone.
pub fn reservation_event(
    op: ReservationOp,
    reservation: &Value,
    identity: &EventIdentity,
) -> Value {
    let event_type = match op {
        ReservationOp::Reserved => EV_RESERVATION_RESERVED,
        ReservationOp::Released => EV_RESERVATION_RELEASED,
        ReservationOp::Consumed => EV_RESERVATION_CONSUMED,
    };
    json!({
        "event_type": event_type,
        "env_id": identity.env_id,
        "subject_id": identity.subject_id,
        "correlation_id": identity.correlation_id,
        "idempotency_key": identity.idempotency_key,
        "snapshot": reservation,
    })
}

/// Write the outbox row for an envelope in the CALLER's transaction (same
/// tx as the domain change — outbox-first). The relay (shell boot step 8b)
/// pushes it to Bus post-commit; a failed write aborts the caller's tx via
/// `?` at the call site. `publisher_subject` carries the acting subject so
/// consumers can attribute the event.
///
/// # Errors
/// Propagates the SQL error (the validator's rejection of the constant
/// family text is structurally impossible — covered by the registry test).
pub async fn emit_outbox(
    tx: &mut sqlx::PgConnection,
    identity: &EventIdentity,
    envelope: &Value,
) -> Result<Uuid, sqlx::Error> {
    let ev = envelope["event_type"].as_str().unwrap_or_default();
    match sentin8::bus::outbox::insert_outbox_row_with_publisher(
        &mut *tx,
        identity.env_id,
        STREAM_FAMILY,
        ev,
        envelope,
        None,
        Some(&identity.subject_id),
    )
    .await
    {
        Ok(id) => Ok(id),
        Err(sentin8::bus::outbox::OutboxError::Db(e)) => Err(e),
        Err(e) => Err(sqlx::Error::Configuration(Box::new(e))),
    }
}

// ── Audit journal (architecture-principles §6.8) ─────────────────────────

/// Audit `action` words accepted by [`journal_mutation`] (maps one-to-one to
/// `sentin8_audit::AuditAction`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditOp {
    /// Resource created (limit row, reservation trail).
    Create,
    /// Resource modified (limit value/status, release).
    Update,
    /// Resource deleted.
    Delete,
    /// Lifecycle executed (fill conversion).
    Execute,
}

/// Build the audit envelope for one mutation and write the
/// `risk.audit_journal` row on the CALLER's transaction (same tx as the
/// domain change; the shell's pusher — boot step 8c — drains it to
/// Sentin8:Sys `POST /v1/audit` with the `risk-audit` M2M workload). Reads
/// are NOT journalized (audit-journal pattern: state changes only).
///
/// # Errors
/// Propagates the SQL error of the journal INSERT.
pub async fn journal_mutation(
    tx: &mut sqlx::PgConnection,
    identity: &EventIdentity,
    op: AuditOp,
    resource_type: &str,
    resource_id: &str,
    message: &str,
    before: Option<Value>,
    after: Option<Value>,
) -> Result<(), sqlx::Error> {
    use sentin8::audit::{AuditAction, AuditEvent, AuditSeverity};
    let action = match op {
        AuditOp::Create => AuditAction::Create,
        AuditOp::Update => AuditAction::Update,
        AuditOp::Delete => AuditAction::Delete,
        AuditOp::Execute => AuditAction::Execute,
    };
    let ev = AuditEvent::builder()
        .service("exchange8:risk")
        .action(action)
        .resource(resource_type, resource_id)
        .actor(&identity.subject_id, "machine")
        .tenant(Uuid::nil(), identity.env_id)
        .severity(AuditSeverity::Info)
        .correlation_id(&identity.correlation_id)
        .message(message);
    let ev = match before {
        Some(b) => ev.before(b),
        None => ev,
    };
    let ev = match after {
        Some(aft) => ev.after(aft),
        None => ev,
    };
    sentin8::audit::journal_insert(&mut *tx, &ev.build()).await
}

// ── Decision-plane egress (DESIGN.md §7 backpressure) ────────────────────

/// One egress item staged by the decision plane (the authorize path or the
/// event consumers) and written by [`spawn_outbox_writer`]. The trail rows
/// + outbox + audit rows commit in ONE transaction per env group; the
/// synchronous gate never waits on it.
#[derive(Clone, Debug)]
pub enum EgressItem {
    /// An authorized order's reservation: the trail INSERT (one row per
    /// touched bucket) + the `reserved` event + the audit row.
    Reserved {
        env_id: Uuid,
        order: crate::limits::ReservationTrail,
        identity: EventIdentity,
    },
    /// A released reservation (the consumer's kill path): trail UPDATE
    /// (reserved → released) + the `released` event + the audit row.
    Released {
        env_id: Uuid,
        trail: crate::limits::ReservationTrail,
        identity: EventIdentity,
    },
    /// A fill's conversion: trail UPDATE (reserved → consumed) + the
    /// `consumed` event + the audit row.
    Consumed {
        env_id: Uuid,
        trail: crate::limits::ReservationTrail,
        identity: EventIdentity,
    },
    /// A connection's bucket transitioned into breach (state-change rule:
    /// fired once per (connection, bucket) episode, never per decision).
    Breach {
        env_id: Uuid,
        bucket: &'static str,
        breach: Value,
        identity: EventIdentity,
    },
    /// A breach episode ended (headroom returned): one `limit:updated`
    /// event with the recovered-bucket snapshot (no trail row — the trail
    /// records reservations, not breach episodes; the audit row carries it).
    Recovered {
        env_id: Uuid,
        bucket: &'static str,
        snapshot: Value,
        identity: EventIdentity,
    },
}

impl EgressItem {
    /// The env the item attributes to (the writer groups transactions by
    /// it — the RLS context is transaction-local).
    pub fn env_id(&self) -> Uuid {
        match self {
            EgressItem::Reserved { env_id, .. }
            | EgressItem::Released { env_id, .. }
            | EgressItem::Consumed { env_id, .. }
            | EgressItem::Breach { env_id, .. }
            | EgressItem::Recovered { env_id, .. } => *env_id,
        }
    }

    /// The event_type the item's envelope carries (diagnostics/tests).
    pub fn event_type(&self) -> &'static str {
        match self {
            EgressItem::Reserved { .. } => EV_RESERVATION_RESERVED,
            EgressItem::Released { .. } => EV_RESERVATION_RELEASED,
            EgressItem::Consumed { .. } => EV_RESERVATION_CONSUMED,
            EgressItem::Breach { .. } => EV_LIMIT_BREACHED,
            EgressItem::Recovered { .. } => EV_LIMIT_UPDATED,
        }
    }
}

/// Writer budget: maximum items per flush (the ME outbox writer's batching
/// convention; the decision plane's volume is lower — the caps bound tail
/// latency, never the hot path).
const OUTBOX_BATCH_ITEMS: usize = 512;
/// Writer budget: maximum tick between flushes.
const OUTBOX_FLUSH_TICK_MS: u64 = 5;

/// Spawn the outbox-writer task: batch [`EgressItem`]s into the durable
/// rows (512 items / 5 ms, whichever first) under one RLS-contexted
/// transaction per env group. The shell's relay (boot 8b) produces the
/// outbox rows to Bus — this task only stages them. Called from the
/// `with_state` closure (the shell's relay/pushers are already running).
pub fn spawn_outbox_writer(
    pool: sqlx::PgPool,
    mut rx: tokio::sync::mpsc::Receiver<EgressItem>,
) {
    tokio::spawn(async move {
        loop {
            let mut batch: Vec<EgressItem> = Vec::with_capacity(OUTBOX_BATCH_ITEMS);
            let Some(first) = rx.recv().await else {
                break; // channel closed — drain done
            };
            batch.push(first);
            let deadline =
                tokio::time::Instant::now() + std::time::Duration::from_millis(OUTBOX_FLUSH_TICK_MS);
            while batch.len() < OUTBOX_BATCH_ITEMS {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(ev)) => batch.push(ev),
                    Ok(None) => break,
                    Err(_) => break, // tick expired — flush what we have
                }
            }
            if let Err(e) = write_batch(&pool, &batch).await {
                tracing::error!(
                    error = %e,
                    items = batch.len(),
                    "risk egress writer: batch write failed — items lost from this pass"
                );
            }
        }
        tracing::info!("risk egress writer: channel closed — stopped");
    });
}

/// Write one batch: group by env (the RLS context is transaction-local —
/// the ME writer's per-env grouping), then per item commit the trail rows +
/// outbox envelope + audit row on the item's transaction.
async fn write_batch(pool: &sqlx::PgPool, batch: &[EgressItem]) -> Result<(), sqlx::Error> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut envs: Vec<Uuid> = Vec::new();
    for ev in batch {
        let env = ev.env_id();
        if !envs.contains(&env) {
            envs.push(env);
        }
    }
    for env in envs {
        for item in batch.iter().filter(|e| e.env_id() == env) {
            if let Err(e) = write_item(pool, env, item).await {
                tracing::error!(error = %e, item = item.event_type(), "risk egress writer: item write failed");
            }
        }
    }
    Ok(())
}

/// Commit one egress item's durable rows (one transaction: trail + outbox +
/// audit, same-tx per the DBX pair rule).
async fn write_item(
    pool: &sqlx::PgPool,
    env_id: Uuid,
    item: &EgressItem,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sentin8::shell::set_tenant_context(&mut tx, env_id).await;
    match item {
        EgressItem::Reserved { order, identity, .. } => {
            // One trail row per touched bucket (the reservation's value
            // books once per bucket; the open-order slot is a count — no
            // trail row).
            for bucket in &order.buckets {
                sqlx::query(
                    r#"INSERT INTO risk.risk_reservation
                           (env_id, order_id, scope_type, scope_id, currency, bucket, value, state)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, 'reserved')"#,
                )
                .bind(env_id)
                .bind(order.order_id as i64)
                .bind(order.scope_type)
                .bind(order.scope_id.clone())
                .bind(order.currency.clone())
                .bind(*bucket)
                .bind(format!("{}", order.value))
                .execute(&mut *tx)
                .await?;
            }
            let envelope = reservation_event(
                ReservationOp::Reserved,
                &order.envelope_snapshot(),
                identity,
            );
            emit_outbox(&mut tx, identity, &envelope).await?;
            journal_mutation(
                &mut tx,
                identity,
                AuditOp::Create,
                "risk_reservation",
                &order.order_id.to_string(),
                &format!("reservation booked for order {}", order.order_id),
                None,
                Some(order.envelope_snapshot()),
            )
            .await?;
        }
        EgressItem::Released { trail, identity, .. } => {
            // Replay-safe: the state predicate makes a re-delivered release
            // a no-op (saturating release at the ledger mirrors it).
            let n = sqlx::query(
                r#"UPDATE risk.risk_reservation
                      SET state = 'released', released_at = now()
                    WHERE env_id = $1 AND order_id = $2
                      AND scope_id = $3
                      AND state = 'reserved'"#,
            )
            .bind(env_id)
            .bind(trail.order_id as i64)
            .bind(trail.scope_id.clone())
            .execute(&mut *tx)
            .await?
            .rows_affected();
            let envelope = reservation_event(
                ReservationOp::Released,
                &trail.envelope_snapshot(),
                identity,
            );
            emit_outbox(&mut tx, identity, &envelope).await?;
            journal_mutation(
                &mut tx,
                identity,
                AuditOp::Update,
                "risk_reservation",
                &trail.order_id.to_string(),
                &format!("reservation released for order {} ({n} rows)", trail.order_id),
                None,
                Some(trail.envelope_snapshot()),
            )
            .await?;
        }
        EgressItem::Consumed { trail, identity, .. } => {
            let n = sqlx::query(
                r#"UPDATE risk.risk_reservation
                      SET state = 'consumed'
                    WHERE env_id = $1 AND order_id = $2
                      AND scope_id = $3 AND bucket = $4
                      AND state = 'reserved'"#,
            )
            .bind(env_id)
            .bind(trail.order_id as i64)
            .bind(trail.scope_id.clone())
            .bind(trail.buckets.first().copied().unwrap_or("notional"))
            .execute(&mut *tx)
            .await?
            .rows_affected();
            let envelope = reservation_event(
                ReservationOp::Consumed,
                &trail.envelope_snapshot(),
                identity,
            );
            emit_outbox(&mut tx, identity, &envelope).await?;
            journal_mutation(
                &mut tx,
                identity,
                AuditOp::Execute,
                "risk_reservation",
                &trail.order_id.to_string(),
                &format!("reservation consumed for order {} ({n} rows)", trail.order_id),
                None,
                Some(trail.envelope_snapshot()),
            )
            .await?;
        }
        EgressItem::Breach { bucket, breach, identity, .. } => {
            // State-change record: the breach event + the audit row. No
            // trail row — the trail records reservations (DESIGN.md §3).
            let envelope = envelope_from(*bucket, breach, identity, EV_LIMIT_BREACHED);
            emit_outbox(&mut tx, identity, &envelope).await?;
            journal_mutation(
                &mut tx,
                identity,
                AuditOp::Update,
                "risk_limit",
                &format!("{}:{}", identity.idempotency_key, "breach"),
                "limit bucket transitioned into breach",
                None,
                Some(breach.clone()),
            )
            .await?;
        }
        EgressItem::Recovered { bucket, snapshot, identity, .. } => {
            let envelope = envelope_from(*bucket, snapshot, identity, EV_LIMIT_UPDATED);
            emit_outbox(&mut tx, identity, &envelope).await?;
            journal_mutation(
                &mut tx,
                identity,
                AuditOp::Update,
                "risk_limit",
                bucket,
                "limit bucket recovered from breach",
                None,
                Some(snapshot.clone()),
            )
            .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

/// The breach/recovered envelope form (bucket-scoped snapshot, the
/// state-change identity).
fn envelope_from(
    _bucket: &str,
    snapshot: &Value,
    identity: &EventIdentity,
    event_type: &'static str,
) -> Value {
    json!({
        "event_type": event_type,
        "env_id": identity.env_id,
        "subject_id": identity.subject_id,
        "correlation_id": identity.correlation_id,
        "idempotency_key": identity.idempotency_key,
        "snapshot": snapshot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::ReservationTrail;

    const EXPECTED: &[&str] = &[
        EV_LIMIT_CREATED,
        EV_LIMIT_UPDATED,
        EV_LIMIT_DELETED,
        EV_LIMIT_BREACHED,
        EV_RESERVATION_RESERVED,
        EV_RESERVATION_RELEASED,
        EV_RESERVATION_CONSUMED,
    ];

    fn identity() -> EventIdentity {
        EventIdentity {
            env_id: Uuid::nil(),
            subject_id: "exchange8:gateway".into(),
            correlation_id: "corr".into(),
            idempotency_key: "risk-res-1".into(),
        }
    }

    /// Registry test (DESIGN.md §9): every declared event name uses the
    /// platform event format (`vertical:service:resource:operation`, all
    /// colons — no dots) and the stream family validates.
    #[test]
    fn event_names_follow_the_colon_format_and_family_validates() {
        for n in EXPECTED {
            let segs: Vec<&str> = n.split(':').collect();
            assert_eq!(segs.len(), 4, "{n}: must have 4 segments");
            assert_eq!(segs[0], "exchange8", "{n}: wrong vertical");
            assert_eq!(segs[1], "risk", "{n}: wrong service");
            assert_eq!(n.find('.'), None, "{n}: dots are not event format");
            let lower = |s: &str| {
                s.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
            };
            assert!(segs.iter().all(|s| !s.is_empty() && lower(s)), "{n}");
        }
        assert_eq!(STREAM_FAMILY, "risk");
        sentin8::bus::outbox::validate_stream_family(STREAM_FAMILY)
            .expect("stream family must validate");
    }

    #[test]
    fn limit_event_envelope_is_payload_complete() {
        let id = identity();
        let row = json!({
            "id": "00000000-0000-0000-0000-000000000001",
            "scope_type": "connection",
            "scope_id": "7",
            "currency": "CAD",
            "limit_type": "order_value",
            "bucket": "notional",
            "limit_value": "1000000.00",
            "status": "E",
        });
        let env = limit_event(LimitOp::Created, &row, &id);
        assert_eq!(env["event_type"], EV_LIMIT_CREATED);
        assert_eq!(env["snapshot"]["bucket"], "notional");
        assert_eq!(env["idempotency_key"], "risk-res-1");
        assert_eq!(env["subject_id"], "exchange8:gateway");
    }

    #[test]
    fn reservation_event_maps_every_op() {
        let id = identity();
        let trail = ReservationTrail {
            env_id: id.env_id,
            order_id: 9,
            connection_id: 3,
            scope_type: "connection",
            scope_id: "3".into(),
            currency: "CAD".into(),
            buckets: vec!["notional"],
            value: 1_000,
        };
        for (op, want) in [
            (ReservationOp::Reserved, EV_RESERVATION_RESERVED),
            (ReservationOp::Released, EV_RESERVATION_RELEASED),
            (ReservationOp::Consumed, EV_RESERVATION_CONSUMED),
        ] {
            let env = reservation_event(op, &trail.envelope_snapshot(), &id);
            assert_eq!(env["event_type"], want);
            assert_eq!(env["snapshot"]["order_id"], 9);
        }
    }

    #[test]
    fn egress_item_carries_event_name_and_env() {
        let id = identity();
        let trail = ReservationTrail {
            env_id: id.env_id,
            order_id: 1,
            connection_id: 3,
            scope_type: "connection",
            scope_id: "3".into(),
            currency: "CAD".into(),
            buckets: vec!["notional"],
            value: 100,
        };
        let r = EgressItem::Reserved {
            env_id: id.env_id,
            order: trail.clone(),
            identity: EventIdentity {
                env_id: id.env_id,
                subject_id: "s".into(),
                correlation_id: "c".into(),
                idempotency_key: "risk-res-1".into(),
            },
        };
        assert_eq!(r.event_type(), EV_RESERVATION_RESERVED);
        assert_eq!(r.env_id(), Uuid::nil());
    }
}