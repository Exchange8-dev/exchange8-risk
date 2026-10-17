# Exchange8:Risk — Detailed Design Document

## 1. Scope & Context

Exchange8:Risk owns synchronous pre-trade authorization for one environment: the
21-check authorization matrix, the notional/open-order/message-rate limit
ledgers and their reservation trail, the per-connection rate guards, and
the per-env exposure snapshot publication. It explicitly does **not** own:

- client protocols and session admission — Exchange8:Gateway terminates FIX/
  OUCH/REST, validates shape, and calls the pre-check;
- matching — Exchange8:ME consumes only authorized orders and is not a risk gate;
- position, mark-to-market, margining, liquidation, funding — Exchange8:Margin
  (the asynchronous derivatives risk service; this service may consult its
  margin requirements but never computes them);
- limit-definitions ownership for other services; the `risk` schema's
  tables are this service's alone;
- reference data (instruments, calendars, tick regimes) — Exchange8:RefData;
  Risk consumes its snapshots;
- trade persistence and clearing — Exchange8:Clearing (Risk reads day-so-far
  consumption to seed buckets at start-of-day);
- event durability, audit archival, identity, TLS — platform planes
  (Sentin8:Bus, Sentin8:Sys, Sentin8:Auth, `sentin8-tls`, Shield).

### System context

```mermaid
graph TD
    subgraph Env["exchange8 environment (one env_id per tenant env)"]
        GW["Exchange8:Gateway (submission pipeline)"]
        RISK["Exchange8:Risk (exchange8-risk)"]
        CLR["Exchange8:Clearing (day-so-far consumption)"]
        REF["Exchange8:RefData (instruments, tick regimes, calendars)"]
        MGN["Exchange8:Margin (margin requirements, derivatives)"]
    end
    AUTH["Sentin8:Auth (issuer, JWKS, certificate identity)"]
    BUSP["Sentin8:Bus (POST /v1/produce)"]
    SYS["Sentin8:Sys (POST /v1/audit, GET /v1/resolve/hostname)"]
    MBP["Sentin8:MemBus (Valkey-backed in-process publication)"]
    DBX[("PostgreSQL 18 — db exchange8, schema risk")]

    GW -- "POST /v1/authorize (M2M exchange8:risk:authorize)" --> RISK
    GW -- "POST /v1/release (reservation corrections)" --> RISK
    RISK -- "SOD seed: day-so-far consumption (events/query API)" --> CLR
    RISK -- "instrument/tick/LIS snapshots (events)" --> REF
    RISK -- "risk.exposure (newest-wins)" --> MBP
    RISK -- "risk.outbox relay → POST /v1/produce (M2M sentin8:bus:produce)" --> BUSP
    RISK -- "risk.audit_journal → POST /v1/audit (M2M sentin8:sys:audit:write)" --> SYS
    RISK -- "RLS-scoped SQL (role exchange8_risk_service)" --> DBX
    RISK -- "M2M verification" --> AUTH
    MGN -. "margin requirement (derivatives pre-trade)" .-> RISK
```

### Container view

```mermaid
graph TD
    subgraph RISKPROC["exchange8-risk binary (axum over sentin8::shell)"]
        HTTPX["HTTP/TLS surface: /v1/health + /v1/admin/policy-sets · /v1/admin/relations · /v1/admin/attributes (Shield admin API)"]
        REGX["registration.rs — self-description (scope-mirror set, pg role, error codes, admin UI pages, M2M grants, init SQL)"]
        STX["RiskState (ServiceCore + MemBus ServiceBus)"]
        subgraph Decision["Decision modules (no tokio/DB/clock)"]
            COREX["core.rs — authorize(): 21-check matrix, Decision"]
            GUAX["guards.rs — TokenBucket + ReservationLedger buckets"]
            LIMX["limits.rs — LimitsLedger + RiskCache + AuthorizeResponse shape"]
        end
        PERSISTX["Shell persistence (capped pool via sentin8_config::pg_pool, sentin8_set_tenant_context, outbox relay, audit pusher)"]
    end
    subgraph Planes["Platform planes"]
        AUTH2["Sentin8:Auth"]
        POL2["Sentin8:Policy (Shield engine + sentin8_policy_sets)"]
        BUS2X["Sentin8:Bus"]
        SYS2X["Sentin8:Sys"]
        TLS2X["sentin8-tls — exchange8-risk-tls resolver (GET /v1/resolve/hostname, sentin8:sys:instances:read)"]
        DBP2[("PostgreSQL 18 — db exchange8, schema risk (role exchange8_risk_service)")]
    end
    HTTPX --> STX
    STX --> COREX
    COREX --> GUAX --> LIMX
    STX --> REGX
    STX --> PERSISTX
    PERSISTX -- "DSN EXCHANGE8_RISK_DATABASE_URL" --> DBP2
    HTTPX -- "token verify" --> AUTH2
    HTTPX -- "policy-sets cache (enforce/shadow)" --> POL2
    PERSISTX -- "relay (risk_outbox_changed)" --> BUS2X
    PERSISTX -- "pusher" --> SYS2X
    TLS2X -- "hostname resolve, DB-free" --> SYS2X
```

## 2. Runtime Architecture

The binary boots via `sentin8::shell::serve`:

1. `Sentin8Shell::new("exchange8:risk", "EXCHANGE8_RISK")` — registration,
   allocator, graceful drain under `make upgrade-instance`.
2. `with_state` builds `RiskState { core, membus }` — `ServiceBus::connect`
   attaches the MemBus publish handle (`risk.exposure`, pub/sub class,
   newest-wins); an empty `valkey_url` or unreachable Valkey disables
   publishing with a WARN (fail-soft; the gate still boots).
3. `with_router(extend_router)` mounts `/v1/health` (shell) and the shared
   Shield policy-admin API (`/v1/admin/policy-sets*`,
   `/v1/admin/relations*`, `/v1/admin/attributes*`) through
   `HasPolicyAdmin` — admin and request path share one policy cache.
4. `registration::print` serves `--print-registration`: the `scope-mirror`
   Shield set (action `exchange8:risk:authorize` over the live effective
   scope set, plus `sentin8:sys:admin`/org-admin/env-admin permits), the
   `exchange8_risk_service` PostgreSQL role (database `exchange8`, no
   BYPASSRLS, no cross-schema grants), error codes, the RFW admin pages
   (`limits`, `reservations`), M2M grant workloads, and the `init` SQL for
   the `risk` schema.

### Component diagram

```mermaid
graph TD
    subgraph IngestX["Admission plane"]
        AX["Gateway authorize/release call site (contracts::risk DTOs)"]
        ADMX["Admin call sites: limit CRUD (RFW), policy admin (Shield mount)"]
    end
    subgraph DecisionX["Decision plane"]
        PC["RiskCache.snapshot() → Arc EnvState"]
        MAT["core.rs authorize(): checks 1–21, first-fail reject"]
        RL["LimitsLedger.reserve(): permit → matrix → open-order slot → notional reservation"]
        TB["guards.rs TokenBucket + ReservationLedger counters"]
    end
    subgraph EgressX["Egress plane"]
        MBX["MemBus publish (risk.exposure, fail-soft)"]
        OBX["risk.outbox + relay → Sentin8:Bus"]
        AJX["risk.audit_journal + pusher → Sentin8:Sys"]
    end
    ADMS["Shield SharedAdminState (core.admin)"]
    AX --> PC
    PC --> MAT
    MAT --> TB
    TB --> RL
    RL --> MBX
    RL --> OBX
    RL --> AJX
    ADMX --> ADMS
    ADMX --> OBX
```

### Request lifecycle — synchronous pre-trade authorize

```mermaid
sequenceDiagram
    autonumber
    participant GW as Exchange8:Gateway
    participant AU as Auth verify + Shield (step 1–5)
    participant RC as RiskCache (ArcSwap snapshot)
    participant MAT as core.rs authorize (checks 1–21)
    participant LG as LimitsLedger + TokenBucket
    participant OUT as Egress (MemBus / outbox / audit)

    GW->>AU: POST /v1/authorize (AuthorizeRequest = normalized ExchangeOrder, cert-bound M2M)
    AU->>AU: bearer verify → tenant resolve → Shield evaluate (scope-mirror, exchange8:risk:authorize)
    alt Shield denies (no-set / no-match / forbid)
        AU-->>GW: 403 (fail-closed, shadow would deny logged, never blocked)
    else permitted
        AU->>RC: snapshot() (one Arc clone)
        RC-->>MAT: EnvState (market phase, instrument, client)
        MAT->>LG: take_permit (rate guard, refill vs now_s)
        alt RateLimited
            LG-->>GW: reject (RateLimited)
        else permit ok
            MAT->>MAT: static checks 1–21 (fail-closed, first-fail reject code)
            alt static reject
                MAT-->>GW: AuthorizeResponse { decision: Reject, code }
                Note over LG: no ledger movement on static reject
            else all checks pass
                LG->>LG: open-order slot + notional reservation (checked math)
                LG-->>GW: AuthorizeResponse { decision: Allow (code None) }
                LG-->>OUT: exposure snapshot publish + same-tx reservation trail
            end
        end
    end
```

### Request lifecycle — limit update via admin

```mermaid
sequenceDiagram
    autonumber
    participant UI as Admin (Sentin8:Dash → Sentin8:BFF)
    participant RISK as Exchange8:Risk admin routes
    participant SH as Shield cache (SharedAdminState)
    participant LED as ReservationLedger (limits)
    participant DBX as risk.outbox / risk.audit_journal

    UI->>RISK: limit upsert (delegated bearer user token, scope gate per admin scopes)
    RISK->>SH: Shield evaluate (mirror set, tenant forbid wins)
    alt denied
        RISK-->>UI: 403 (fail-closed)
    else allowed
        RISK->>LED: set_limit(bucket, scope, limit_value) — consumption preserved
        RISK->>DBX: same-tx write: limit row + audit journal entry
        DBX-->>DBX: pg_notify risk_outbox_changed → relay, audit pusher → Sys
        RISK-->>UI: 200
    Note over UI,DBX: refresh is copy→mutate→swap, in-flight readers keep the old snapshot
    end
```

### Request lifecycle — reject path (first failure wins)

```mermaid
sequenceDiagram
    autonumber
    participant GWX as Exchange8:Gateway
    participant AUTHX as Auth/Shield pipeline
    participant MX as core.rs authorize
    participant FIX as FIX member OMS

    GWX->>AUTHX: authorize (order off-tick, halted instrument)
    AUTHX->>MX: evaluate
    MX->>MX: check 5 — instrument halted → first failure
    MX-->>AUTHX: Reject(SecurityHaltedPrimary)
    AUTHX-->>GWX: decision Reject, code
    GWX-->>FIX: FIX ExecutionReport reject (OrdRejReason 103 + text from the catalog)
    Note over MX: no bucket touched, no reservation, no event emission on reject
```

## 3. Data Model

Schema `risk` in database `exchange8` (database-per-vertical,
schema-per-service); role `exchange8_risk_service` holds exactly the grants
in `init.sql` (no BYPASSRLS, no cross-schema grants). RLS is FORCEd on
every table with `sentin8_context_initialized() AND env_id =
sentin8_current_env_id()` policies; context is set per connection via
`SELECT sentin8_set_tenant_context($1)` — always awaited, transaction-local
(`set_config(..., true)`) on the same connection as the query.

```mermaid
erDiagram
    risk_risk_limit ||--o{ risk_risk_reservation : "consumption of"
    risk_risk_limit {
        uuid id PK
        uuid env_id FK
        text scope_type "connection|trader|customer|account"
        text scope_id
        text currency
        text limit_type "order_value|gross|net|exposure|per_order_value|per_order_qty|per_trade_value|daily_value"
        text bucket "charge bucket the limit applies to"
        numeric limit_value
        text status "E|D (enabled/disabled)"
    }
    risk_risk_reservation {
        uuid id PK
        uuid env_id FK
        bigint order_id
        text scope_type
        text scope_id
        text currency
        text bucket
        numeric value
        text state "reserved|consumed|released"
        timestamptz created_at
        timestamptz released_at
    }
    risk_outbox {
        uuid id PK
        uuid env_id FK
        text event_type
        text stream_family
        jsonb payload
        text publisher_id_token
        text publisher_subject
        bool relayed
        timestamptz relayed_at
        int relay_attempts
        timestamptz created_at
    }
    risk_audit_journal {
        uuid id PK
        uuid env_id FK
        text action
        text target_resource_type
        text target_resource_id
        text actor_user_id
        text actor_subject_type
        text severity
        text correlation_id
        jsonb before_state
        jsonb after_state
        jsonb details
        bool pushed
        timestamptz pushed_at
        timestamptz created_at
    }
```

Per-table notes:

- `risk.risk_limit` — pre-trade limit definitions. `scope_type` is
  CHECK-enforced to `connection|trader|customer|account`, `limit_type` to
  the order_value / gross / net / exposure / per-order / per-trade / daily
  vocabulary; `status` is `E`/`D`. Indexed on `env_id` and
  `(env_id, scope_type, scope_id)`. Env-scoped (RLS on `env_id`); no org
  column; no secret-bearing columns.
- `risk.risk_reservation` — the persistent audit/rebuild trail of bucket
  consumption: one row per reservation (`order_id`, bucket, `value`), state
  CHECK-enforced to `reserved | consumed | released` with transitions
  stamped (`created_at`, `released_at`). Indexed per env+order and per
  env+scope+state. The in-memory ledger is authoritative per request; this
  table is the day-so-far reconstruction source and the audit trail.
- `risk.outbox` — the local outbox (platform `sentin8_outbox` shape): rows
  inserted in the same transaction as the domain change (`event_type`,
  `stream_family`, `payload`), `relayed`/`relayed_at`/`relay_attempts`
  maintained by the shell relay, partial index on unrelayed rows, and the
  `outbox_notify_trigger` firing `pg_notify('risk_outbox_changed')` for
  relay wake-up.
- `risk.audit_journal` — the local audit journal (platform `sentin8_audit`
  shape): admin/domain actions with before/after states, actor/subject/
  severity/correlation; `pushed`/`pushed_at` track delivery to Sys; partial
  index on unpushed rows.
- In-memory only: the `EnvState` snapshot (instruments, clients, market
  phase), the `LimitsLedger` per-connection counters, and the token
  buckets. Reference data (instruments, tick regimes) belongs to
  Exchange8:RefData's tables — this schema holds no copy of it.

### Reservation lifecycle

```mermaid
stateDiagram-v2
    [*] --> reserved: authorize passes (same-tx INSERT, bucket decremented in ledger)
    reserved --> consumed: fill (qty×price converts reservation to spent credit)
    reserved --> released: kill / cancel / STP decrement / admin correction
    consumed --> [*]: credit spent (open-order slot freed at completion)
    released --> [*]: headroom restored (saturating release, replay-safe)
```

## 4. API Surface

| Audience | Surface | Gate | Notes |
|---|---|---|---|
| Upstream services (M2M) | `POST /v1/authorize` | bearer verify → tenant resolve → Shield (`exchange8:risk:authorize`; also `exchange8:risk:read`,`exchange8:risk:write` grants, `sentin8:sys:admin`, sys org/env admins via the mirror) | request `AuthorizeRequest` (normalized order), response `AuthorizeResponse { decision, code }` |
| Upstream services (M2M) | `POST /v1/release` | same Shield action (`exchange8:risk:authorize`) | internal (Gateway/ME event consumers release automatically; endpoint reserved for admin corrections) |
| Admin / Shield | `GET/PUT/DELETE /v1/admin/policy-sets{,/{name},/{name}/test}`, `POST /v1/admin/policy-sets/{enforce,shadow,disable}`, `GET/PUT/DELETE /v1/admin/relations{,/check}`, `GET/PUT/DELETE /v1/admin/attributes` | `exchange8:risk:policy:read` / `exchange8:risk:policy:write` (scope mirror also covers `sentin8:sys:admin`, sys org/env admins) | shared Shield admin API mount |
| Admin (declared) | `GET /v1/admin/ui/limits` + `/v1/admin/limits`; `GET /v1/admin/ui/reservations` + `/v1/admin/reservations` | role `exchange8:risk:admin` (admin scopes: `exchange8:risk:read`, `exchange8:risk:write`, `exchange8:risk:policy:{read,write}`) | RFW pages `limits` (icon `0xe06d`), `reservations` (icon `0xe88f`) |
| Health | `GET /v1/health` | unauthenticated (shell) | liveness for Sentin8:Captain |
| Out | Bus `POST /v1/produce` | M2M `sentin8:bus:produce` | workloads `exchange8:risk`/`risk`, plus `sentin8:stat:measure` |
| Out (consume) | Bus `GET /v1/consume/sse` + `POST /v1/ack` | M2M `sentin8:bus:consume` | lifecycle consumer (family `exchange8`, group `exchange8-risk-lifecycle`) — the SSE direct-consumer pattern (BUSCONNP-1); ME lifecycle events drive the ledger's release/consume side. |
| Out | Sys `POST /v1/audit` | M2M `sentin8:sys:audit:write` | workload `risk-audit` |
| MemBus | `risk.exposure` publication | in-process (workload identity, Valkey) | pub/sub class, newest-wins per env |
| TLS | `GET /v1/resolve/hostname` (Sys owner API) | M2M `sentin8:sys:instances:read` | workload `exchange8-risk-tls` (database-free resolve) |

Emitted events ride the `exchange8:{service}:{resource}:{operation}`
vocabulary (per-change `event_type` mapping into `risk.outbox` is the open
writer surface): limit-definition and reservation changes
(`exchange8:risk:limit:*` / `exchange8:risk:reservation:*`), and the
MemBus `risk.exposure` exposure snapshots. Inbound events this service
consumes (cache refresh projection): instrument state and reference-data
updates (`sentin8:refdata:security:updated` family), customer/connection
state, market status (`sentin8`/exchange market families), limit
definitions, and Shield policy-set updates via the shared policy consumer.
Events use colons only — never dots.

### Multi-party: gateway → risk → me

```mermaid
sequenceDiagram
    autonumber
    participant CLI as Market participant (FIX/OUCH/REST)
    participant GWY as Exchange8:Gateway
    participant RKS as Exchange8:Risk
    participant MES as Exchange8:ME
    participant BUSX as Sentin8:Bus
    CLI->>GWY: new order (session-authenticated)
    GWY->>GWY: shape validation (gateway-local)
    GWY->>RKS: POST /v1/authorize (M2M exchange8:risk:authorize)
    alt risk pass
        RKS-->>GWY: Allow (reservation booked in ledger)
        GWY->>MES: order routed (framed envelope, M2M exchange8:order:write)
        MES-->>GWY: lifecycle events (fill / kill)
        MES->>BUSX: lifecycle events via me.outbox relay (idempotent on row id)
        GWY->>RKS: POST /v1/release or auto release on fill/kill (ledger freed)
    else risk reject
        RKS-->>GWY: Reject + exact catalog code
        GWY-->>CLI: reject (no ME traffic, no reservation)
    else risk unavailable
        BUSX-->>BUSX: (no path)
        GWY-->>CLI: fail-closed reject (RiskUnavailable)
    end
```

### Event flow: outbox → Bus → trigger

```mermaid
flowchart TD
    A["Domain change / reservation transition"] --> B["INSERT risk.outbox (payload, relayed=false) + pg_notify risk_outbox_changed"]
    B --> C["Shell relay wakes (LISTEN)"]
    C --> D["Read unrelayed rows (partial index)"]
    D --> E["POST /v1/produce — M2M sentin8:bus:produce, idempotency_key = outbox row id"]
    E -->|2xx| F["UPDATE relayed=true, relayed_at"]
    E -->|network / 5xx| G["relay_attempts++; leave unrelayed; backoff retry"]
    G --> B
    F --> H["Bus stream family (XADD)"]
    H -->|"direct consumer, SSE (BUSCONNP-1)"| I1["Exchange8:Clearing consumes (limit/reservation audit)"]
    H -->|direct consumer, SSE| I2["Exchange8:Surveillance consumers where declared"]
    H -->|subscription| I3["Trigger subscription (consumer group) → function / workflow where declared"]
```

## 5. Cross-Cutting Concerns

- **Authentication.** Sentin8:Auth is the only issuer. The Gateway and the
  admin surface present cert-bound tokens; the 8-step pipeline applies
  (bearer verify → tenant resolve → feature gate → Shield authorize →
  RLS context → execute → audit); requests that cannot resolve a tenant
  context are rejected, never defaulted.
- **RLS context.** Every tenant-table access sets context on the *same*
  connection: `SELECT sentin8_set_tenant_context($1)` awaited,
  transaction-local; explicit `env_id` WHERE clauses remain
  defense-in-depth on top of FORCE RLS. The integration matrix (§9)
  proves silent zero-row reads without context, insert rejection via
  WITH CHECK, own-env visibility, and cross-env invisibility.
- **Shield authorization.** Step 5 is the only gate. The seeded
  `scope-mirror` set (declared in `registration.rs` as
  `SCOPE_MIRROR_POLICIES`, asserted by tests) is action-granular over the
  live effective scope set for `exchange8:risk:authorize`; enforce mode
  decides, shadow logs would-deny without blocking; tenant-authored forbid
  beats the platform mirror; M2M parity holds (`token_tp` never bypasses);
  no-set / no-match deny (fail-closed).
- **Idempotency & exactly-once.** Bus delivery is at-least-once by stream
  semantics; the outbox relay's `idempotency_key = risk.outbox.id` makes
  produce effectively-once per domain event. Ledger releases are
  saturating, so replayed lifecycle events (double release of the same
  order) cannot under-consume or over-grow headroom.
- **Rate limiting / bot protection.** The service *is* the rate-limiting
  tier for order flow: per-connection token buckets (burst + sustained)
  fail closed (`RateLimited`); the exchange-core's own surfaces rely on
  platform-level protections (Shield + BFF posture).
- **Audit journaling.** Domain/admin changes ride `risk.audit_journal`
  (same transaction where applicable) with actor/subject/severity/
  correlation; the shell pusher forwards unpushed rows to
  Sys `POST /v1/audit` (workload `risk-audit`, scope
  `sentin8:sys:audit:write`); a Sys outage only delays delivery, never
  loses records.
- **Drain.** `sentin8::shell` graceful drain integrates with
  `make upgrade-instance`: the replacement instance reaches `Active`
  before the old drains in-flight authorize calls and flushes outstanding
  outbox/journal rows — a synchronous gate must never truncate decisions
  mid-flight.
- **TLS posture.** In-process TLS via `sentin8-tls` (post-quantum
  X25519MLKEM768 / ML-DSA-65); the DB-free resolver workload
  `exchange8-risk-tls` resolves hostnames through the Sys owner API
  (`GET /v1/resolve/hostname`, scope `sentin8:sys:instances:read`) — no
  platform-database DSN for resolution, no BYPASSRLS anywhere in the grant
  set.

## 6. Failure Modes & Recovery

**Fail-closed posture.** The authorization gate fails closed on every
condition that could hide lost state: unknown connection, unknown
instrument, unconfigured or exhausted bucket, refused context, stale
cache beyond the acceptable lag, and service unavailability itself — the
Gateway's behavior when this service is unreachable is a reject
(`RiskUnavailable`), never a bypass. A per-connection risk bypass flag
exists in the gateway contract as an explicit platform-admin lever, default
off, exercised only by an explicit operations decision.

```mermaid
flowchart TD
    REQ["authorize request"] --> CTX{"tenant context + Shield"}
    CTX -->|deny / no context| R403["403 — fail-closed"]
    CTX -->|ok| SNAP{"snapshot fresh?"}
    SNAP -->|stale beyond watermark| RST["reject — fail-closed (stale cache is exposure)"]
    SNAP -->|ok| PER{"rate permit"}
    PER -->|none| RRL["Reject RateLimited"]
    PER -->|ok| MAT{"checks 1–21"}
    MAT -->|first failure| RCO["Reject exact catalog code"]
    MAT -->|pass| BUC{"buckets: open-orders slot + notional headroom"}
    BUC -->|exceeded| RLX["Reject (OpenOrdersExceeded / NotionalExceeded)"]
    BUC -->|ok| ALW["Allow — reservation booked; same-tx outbox/journal rows"]
    ALW --> OB{"outbox INSERT ok?"}
    OB -->|no| RET["retry with the domain step; no partial delivery"]
    OB -->|yes| RLY["relay → POST /v1/produce"]
    RLY -->|non-2xx| RET2["relay_attempts++; backoff; row stays unrelayed"]
    RET2 --> RLY
    RLY -->|permanent / DLQ| DLQ["Bus DLQ (GET /v1/dlq) → manual replay"]
```

- **Risk instance down.** The gate is synchronous: no instance, no
  authorization, no order. Callers reject with `RiskUnavailable` — the
  multi-instance deployment means any replica can serve (state rides the
  snapshot + events; start-of-day load is authoritative), so a single
  replica loss is absorbed by the fleet, not by callers.
- **Cache / event lag.** The snapshot is generation-aware; beyond the
  max-lag watermark the service refuses (fail-closed) rather than authorize
  against stale instrument or limit state. MemBus publish is fail-soft
  (exposure publication degrades with a WARN; the gate itself does not).
- **DB unavailable at SOD load.** The service refuses to become `Active`
  (Captain health gate): authorizing against an empty snapshot would be a
  silent fail-open.
- **Outbox relay outage.** `risk.outbox` rows accumulate unrelayed
  (`relay_attempts` grows); the partial index replays them in `created_at`
  order when the Bus recovers; `idempotency_key = row id` makes replay
  safe; failed deliveries land in the DLQ for manual replay — never
  silently dropped.
- **Sys audit outage.** `risk.audit_journal` rows stay unpushed; the
  pusher retries — audit is delayed, never lost.

Recovery sequence:

```mermaid
sequenceDiagram
    autonumber
    participant OPS as Operator (make upgrade-instance / start)
    participant SHELL as sentin8-shell boot
    participant DBX as db exchange8, schema risk
    participant LED as RiskCache / LimitsLedger
    participant BUSE as Sentin8:Bus
    OPS->>SHELL: start (predecessor drained)
    SHELL->>DBX: SOD load: risk_limit rows + Clearing day-so-far seeds
    DBX-->>LED: full EnvState snapshot built → RiskCache::from_sod
    LED->>LED: seed_consumed + set_limit per row (ledger rebuilt)
    SHELL->>DBX: reset relay: unrelayed risk.outbox rows
    SHELL->>BUSE: POST /v1/produce (idempotency_key = row id, no duplicate delivery)
    SHELL-->>OPS: ready (health Active in captain registry)
```

## 7. Concurrency & Performance Model

```mermaid
graph LR
    subgraph ProducerX["Single decision plane"]
        GWR["Gateway authorize calls (M2M, per connection)"]
        SNAP2["RiskCache.snapshot() — one Arc clone per request"]
        TASK["Authorization task: rate permit → matrix → buckets"]
        LED2["LimitsLedger (single owner, saturated releases)"]
        EGX["Egress: MemBus publish + outbox relay + audit pusher"]
    end
    subgraph RefreshX["Refresh plane (single writer)"]
        EVC["Event consumers + SOD load"]
        SWP["copy → mutate → swap (EnvState)"]
    end
    GWR -- "request" --> SNAP2
    SNAP2 --> TASK --> LED2
    LED2 --> EGX
    EVC --> SWP --> SNAP2
```

- **Task layout.** The decision plane is a single-owner task over the
  ledger plus the shell's standard workers (outbox relay, audit pusher,
  MemBus handle); refresh is the sole mutation path for the snapshot.
- **Hot path budget.** Target P99 < 5 ms for `POST /v1/authorize`; the
  in-process work is one `Arc` clone, one token-bucket refill
  (arithmetic vs injected clock), and a fixed sequence of integer/fixed-
  point comparisons — no I/O, no allocation in the check pipeline.
- **Locking strategy.** The snapshot is immutable-after-swap (readers clone
  the `Arc`); the ledger is single-owner; no reader-writer or spin locks on
  the authorize path.
- **Notional math.** Fixed-point price/qty with checked multiplication and
  saturating ledger arithmetic (`u128` notional); overflow rejects
  `Internal` instead of wrapping.
- **Backpressure.** Egress (MemBus/outbox/audit) is asynchronous and
  retried; the synchronous gate never queues behind egress — a decision is
  returned before the exposure publication and journal pushes complete.

## 8. Invariants, Insecure-Defaults Rejected, Unsafe Rationale

Always-true conditions:

1. The same order evaluated against the same snapshot yields the same
   decision and the same reject code (pure function, catalog-exact codes).
2. No order is authorized without (a) a Shield permit for
   `exchange8:risk:authorize` and (b) a rate permit, an open-order slot,
   and notional headroom — every bucket fails closed.
3. Unknown instrument, unknown connection, unconfigured bucket, stale
   cache → reject, never default-allow; the fail-open "unchecked flow when
   no limit configured" posture is rejected by construction.
4. Released or spent notional never exceeds consumed headroom
   (saturating arithmetic); reservation state transitions are
   `reserved → consumed | released` only, never re-entering `reserved`.
5. Outbox rows are inserted in the same transaction as the domain change;
   delivery is idempotent per row id; audit rows are delivered at least
   once to Sys.
6. No tenant-table access without tenant context on the same connection;
   RLS FORCEd on all four tables; role `exchange8_risk_service` carries no
   BYPASSRLS, no cross-database grants, no cross-schema grants.
7. Shield's step-5 decision is the only authorization gate: the seeded
   scope mirror covers `exchange8:risk:authorize`; shadow never blocks;
   forbid beats permit; M2M tokens never bypass policy.
8. Reject codes are exactly the catalog codes
   (FIX 103/58-text-compat); no handler invents a code.

Insecure-defaults rejected: no plaintext provider/credential material in
`risk.*` tables (verification material lives in the identity plane; the
outbox carries publisher references only); no `danger_accept_invalid_certs`-style
TLS shortcuts (Sentin8:Acme root via OS trust store); no BYPASSRLS or
superuser runtime role; no fail-open defaults for unknown risk scopes; no
unbounded egress buffers without retry semantics.

Unsafe rationale: the decision modules contain no `unsafe`; all arithmetic
is checked/saturating fixed-point and the snapshot swap is expressed with
safe `Arc` semantics, so memory safety and determinism are enforced by the
type system.

## 9. Testing Strategy

```mermaid
graph TD
    U["Unit tests — core.rs matrix (checks 1–21, per-check reject), guards.rs buckets, limits.rs ledger/cache"]
    P["Policy wiring tests — tests/policy.rs (pure, no DB)"]
    S["SQL integration (RLS matrix) — tests/sql_matrix.rs (live DB, exchange8_probe NOBYPASSRLS role)"]
    G["Workspace suites — shell wiring, vertical enroll, wire/event parity"]
    E2EUI["E2E UI — platform tests (out of this unit)"]
    U --> S
    P --> S
    G --> E2EUI
    U -.->|"same inputs + same snapshot ⇒ same decision"| U
```

- **Pure matrix unit tests** (in-module, `core.rs`, `guards.rs`,
  `limits.rs`): one test per check (suspended, unresolved/mismatched
  instrument, disabled customer, book token, multi-book strategy, halted,
  holiday, market phases, currency, ord type/TIF, tick, odd/mixed lot,
  iceberg floor, max size/price, LIS, shortable cells, FOK×STP, peg
  variants, post-only, cross/bypass, capacity, entitlement, price
  protection, notional exceed/exact, rate); token-bucket burst/rate
  properties (capacity clamp, deterministic clock); reserve/release
  round-trips, exact-limit fits, slot counts, fill conversion, at-limit
  arithmetic, saturation (double release), unknown-scope fail-closed,
  seed-rebuild, limit refresh preserving consumption; snapshot swap
  atomicity and reader isolation.
- **Policy wiring tests** (`tests/policy.rs`, no DB): the *actual* seeded
  `SCOPE_MIRROR_POLICIES` text loads the engine — no-set deny, enforced
  no-match deny, shadow never blocks (would-deny reported), mirror permits
  its own grants, M2M parity, platform-admin scopes pass, forbid-beats-
  permit, role-keyed tenant sets decide, and the enroll-time dry-run gate
  accepts the declared set.
- **SQL integration matrix** (`tests/sql_matrix.rs`, live PostgreSQL 18 as
  the `exchange8_probe` NOBYPASSRLS role): every table declared in this
  service's `init.sql` exists with FORCE RLS + policies; zero rows without
  context; insert without context rejected (savepoint-guarded); own-env
  probe row visible exactly once; cross-env zero rows — one rolled-back
  transaction (zero residue; skips silently offline).
- **Deliberately not tested here:** platform token-verification internals,
  MemBus/Bus/Sys wire formats, RFW rendering, and Flutter surfaces —
  platform-owned suites (the platform testing strategy doc); E2E UI lives
  at platform level.

## 10. Open Questions

- Router mounting of the decision and admin surfaces: `POST /v1/authorize`,
  `POST /v1/release`, and the declared RFW data endpoints
  (`/v1/admin/limits`, `/v1/admin/reservations`) are defined at enrollment
  with shapes implemented; the handler wiring into the axum router is the
  open integration point.
- The SOD projection and event-consumer wiring (row → `EnvState`
  projection, customer/limit/instrument refresh families, the Clearing
  day-so-far seed query) — the snapshot contract is complete; the loaders
  are the remaining work.
- The concrete per-change `event_type` strings written into `risk.outbox`
  and the durable write path mirroring the in-memory ledger journal into
  `risk.risk_reservation` rows (schema is in place; the writer is open).
- Rate-guard configuration defaults per connection class (capacity and
  sustained rate are the mechanism's inputs; venue-wide defaults are a
  business configuration decision).
- The replace-validation mount (cancel/replace inheriting checks 1–4 plus
  re-price/re-notional) at the same authorize surface.
- Synchronous pre-trade options checks consulting the Exchange8:Margin margin
  requirement (derivatives extension: the cross-service hook and caching
  of the consulted requirement are future design).

---

**Design attribution:** exchange8:risk and its underlying concepts were designed by Hamid Jahanzad <Hamid@Jahanzad.com>.
