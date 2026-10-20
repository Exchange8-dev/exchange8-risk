// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Risk
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:RISK service binary - synchronous pre-trade authorization.
//!
//! Design: `design/sentin8-exchange8-risk-technical-design.md` (see the
//! service doc for the exact file). Core logic in the sibling modules;
//! this shell wires the tokio HTTP/TLS surface via the platform
//! `sentin8-shell` crate (registration, DB pool, health).

#![allow(dead_code)]

// (master design §11.1; see DESIGN.md §2). Workers (bus consumers,
// session supervisors, purge jobs) are in-process tokio tasks, so the
mod core;
mod events;
mod guards;
mod limits;
mod openapi;
mod registration;
mod runtime;

use std::sync::Arc;

/// Service state: shell core + the MemBus publish handle + the runtime
/// planes (see DESIGN.md §2/§7).
///
/// Risk is a pub/sub-class publisher: `risk.exposure` per-env exposure
/// snapshots - newest-wins (each snapshot fully supersedes the last).
/// The decision planes live beside the shell core:
/// - `matrix` - the SOD-projected authorization snapshot (`RiskCache`,
///   copy→mutate→swap; the pure `authorize` reads it).
/// - `ledger` - the reservation ledger (release/consume side + rate
///   profiles; the consumer task and the authorize handler share it).
/// - `breach` - the breach-episode tracker (state-change rule).
pub struct RiskState {
    pub core: sentin8::shell::ServiceCore,
    pub membus: sentin8::membus::ServiceBus,
    pub matrix: limits::RiskCache,
    pub ledger: std::sync::Mutex<limits::LimitsLedger>,
    pub breach: std::sync::Mutex<runtime::BreachTracker>,
}

fn extend_router(
    app: axum::Router<std::sync::Arc<RiskState>>,
    _state: std::sync::Arc<RiskState>,
) -> axum::Router<std::sync::Arc<RiskState>> {
    // POST /v1/authorize + /v1/release arrive on the internal API port;
    // wiring lands with the auth middleware pass.

    app
        // ── Sentin8:Policy admin API (shared implementation;
        // handlers generic over this state via the HasPolicyAdmin impl below;
        // axum 0.8 cannot merge a bound Router<()> into Router<std::sync::Arc<RiskState>>).
        .route(
            "/v1/admin/policy-sets",
            axum::routing::get(
                sentin8::shield::admin::list_policy_sets::<std::sync::Arc<RiskState>>,
            ),
        )
        .route(
            "/v1/admin/policy-sets/{name}",
            axum::routing::get(sentin8::shield::admin::get_policy_set::<std::sync::Arc<RiskState>>)
                .put(sentin8::shield::admin::put_policy_set::<std::sync::Arc<RiskState>>)
                .delete(sentin8::shield::admin::delete_policy_set::<std::sync::Arc<RiskState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}/test",
            axum::routing::post(
                sentin8::shield::admin::test_policy_set::<std::sync::Arc<RiskState>>,
            ),
        )
        .route(
            "/v1/admin/policy-sets/enforce",
            axum::routing::post(
                sentin8::shield::admin::admin_enforce_policy_set::<std::sync::Arc<RiskState>>,
            ),
        )
        .route(
            "/v1/admin/policy-sets/shadow",
            axum::routing::post(
                sentin8::shield::admin::admin_shadow_policy_set::<std::sync::Arc<RiskState>>,
            ),
        )
        .route(
            "/v1/admin/policy-sets/disable",
            axum::routing::post(
                sentin8::shield::admin::admin_disable_policy_set::<std::sync::Arc<RiskState>>,
            ),
        )
        .route(
            "/v1/admin/relations",
            axum::routing::get(sentin8::shield::admin::list_relations::<std::sync::Arc<RiskState>>)
                .put(sentin8::shield::admin::put_relation::<std::sync::Arc<RiskState>>)
                .delete(sentin8::shield::admin::delete_relation::<std::sync::Arc<RiskState>>),
        )
        .route(
            "/v1/admin/relations/check",
            axum::routing::get(sentin8::shield::admin::check_relation::<std::sync::Arc<RiskState>>),
        )
        .route(
            "/v1/admin/attributes",
            axum::routing::get(
                sentin8::shield::admin::list_attributes::<std::sync::Arc<RiskState>>,
            )
            .put(sentin8::shield::admin::put_attribute::<std::sync::Arc<RiskState>>)
            .delete(sentin8::shield::admin::delete_attribute::<std::sync::Arc<RiskState>>),
        )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sentin8::shell::serve(
        sentin8::shell::Sentin8Shell::new("exchange8:risk", "EXCHANGE8_RISK")
            .with_registration(registration::print)
            .with_state(|core| async move {
                // Risk publishes exposure snapshots (pub/sub
                // class) onto the Memory Bus - DESIGN.md §4.
                let membus =
                    sentin8::membus::ServiceBus::connect("exchange8:risk", &core.config.valkey_url)
                        .await;
                // SOD projection: the gate never serves against an empty
                // snapshot (fail-closed - DESIGN.md §6); the projection
                // itself is best-effort here.
                let env_id = runtime::resolve_env_id("exchange8:risk");
                let snapshot = runtime::load_snapshot(&core.pool, env_id).await;
                let matrix = limits::RiskCache::from_sod(snapshot);
                // Limits + rate profiles project from the configured rows.
                let mut ledger = limits::LimitsLedger::new();
                runtime::LimitsLoader::apply_rows(
                    &mut ledger,
                    &runtime::LimitsLoader::load_rows(&core.pool, env_id).await,
                );
                let state = Arc::new(RiskState {
                    matrix,
                    ledger: std::sync::Mutex::new(ledger),
                    breach: std::sync::Mutex::new(runtime::BreachTracker::default()),
                    core,
                    membus,
                });
                // The egress writer + the lifecycle consumer + the exposure
                // publisher (DESIGN.md §7 topology).
                runtime::spawn_planes(state.clone(), env_id);
                Ok(state)
            })
            .with_router(extend_router),
    )
    .await
}

/// Shared policy-admin surface accessor: the shell boot loaded the
/// engine into `core.admin`; the shared handlers extract it via
/// `HasPolicyAdmin` so the admin surface and the request path evaluate the
/// SAME cache. (RiskState is bin-local, so the impl lives here.)
impl sentin8::shield::admin::HasPolicyAdmin for RiskState {
    fn policy_admin(&self) -> &sentin8::shield::admin::SharedAdminState {
        &self.core.admin
    }
}
