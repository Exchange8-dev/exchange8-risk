# Exchange8:Risk — Business Requirements & Functionality

> Audience: product owners, business analysts, and anyone evaluating what
> Exchange8:Risk **does for the business**. For architecture read `README.md`;
> for wire-level detail read `DESIGN.md`.

## 1. Business Purpose

- Protect the exchange's capital and its members' credit: no order reaches
  the matching engine until its notional cost fits the submitter's credit
  limit, its size and shape fit the instrument's rules, and its pace fits
  the connection's rate controls.
- Enforce the venue's market rules at the door: trading phases, instrument
  halts, short-sale ability, hidden-order minimums, tick and lot
  disciplines — every gate applied before a match is possible, never
  after.
- Give members precise, machine-usable feedback: every rejection returns
  the exact industry-standard reject code their order-management systems
  screen on, so a declined order is an actionable event, not a mystery.
- Make every admission decision auditable: reservations are booked,
  converted, and released in a persistent trail, and every limit change
  and admin action is journalled.
- Keep the exchange core safe under stress: a runaway client is throttled
  at the gate (rate guard) instead of loading the matching engine or the
  wider venue.

## 2. Business Scope

### What the service provides

| Capability | Description |
|---|---|
| Synchronous pre-trade authorization | A single decision point every submitted order must pass before routing: allow, or reject with the exact catalog reason code |
| Session & participant eligibility | Suspended sessions and disabled customers/traders cannot submit; book-token grants and entitlements are checked per order |
| Market & instrument state gating | Trading phase (pre-open, open, postponed, halted, closed), per-instrument halt and holiday enforcement, pre-open day/limit-only discipline |
| Order-shape validation | Order type / time-in-force admissibility per market, price-positive and quantity-bounded, tick-grid pricing, odd/mixed lot rules, iceberg floor rules |
| Advanced order-shape rules | Hidden orders require Large-In-Scale size; shortable matrix (Y / exempt-only / N); post-only, cross, bypass, peg, STP-shape and price-protection constraints |
| Notional credit limit | Reservation-based credit control: buy-power booked at submission, converted to spent on fills, freed on kills/cancels |
| Open-order count limit | Caps working orders per connection; slots free up as orders complete |
| Rate guards | Per-connection token-bucket throttling (burst + sustained rate), failing closed when exhausted |
| Reservation ledger & trail | Day-so-far consumption rebuilt at start-of-day from clearing data; every reservation transition durably recorded |
| Limit administration | Runtime limit definitions per scope (connection / trader / customer / account) and currency, enable/disable without service restarts |
| Exposure publication | Per-environment exposure snapshots published realtime for operations and downstream consumers |
| Admin surfaces | Shield policy administration and declared RFW admin pages (`limits`, `reservations`) |

### What the service does NOT own (business boundary)

| Concern | Owning service |
|---|---|
| Client connectivity, sessions, protocol parsing (FIX, OUCH, REST) | Exchange8:Gateway |
| Matching, order books, fill allocation | Exchange8:ME |
| Position ledger, mark-to-market, margining, liquidation (derivatives risk) | Exchange8:Margin |
| Smart order routing | Exchange8:SOR |
| Parent orders, allocations, drop copy | Exchange8:OMS |
| Reference data (instruments, tick regimes, calendars, books) | Exchange8:RefData |
| Trade persistence, clearing, netting | Exchange8:Clearing |
| Settlement of cleared trades | Exchange8:Settlement |
| Market surveillance | Exchange8:Surveillance |
| Corporate actions | Exchange8:Corporate Actions (vertical design) |
| Identity, token issuance, FIX session keys | Sentin8:Auth |
| Declarative policy engine and admin surface | Shield (Sentin8:Policy) |
| Durable event fan-out and audit archival | Sentin8:Bus / Sentin8:Sys |

## 3. Business Capabilities

### Pre-trade checks (admission)

| Capability | Business rule |
|---|---|
| Session suspension | A suspended or halted connection's orders are rejected outright (`ClientSuspended`) |
| Participant enablement | Orders from disabled customers/traders are rejected (`WrongUserId`-family code) |
| Identity of the instrument | Unresolvable instruments and mismatched symbols/ISINs are rejected; unknown ID-side state fails closed, never defaults |
| Book tokens & strategy | Only book-token-granted books may be used; an order may target a single book per strategy |
| Market phase discipline | Pre-open accepts only day-validity limit orders; postponed, halted, and closed phases reject all new orders; halted/holiday instruments reject at instrument level too |
| Currency discipline | The order currency must match the instrument's trading currency |
| Order type & validity | Only venue-permitted order types and time-in-force values are accepted, per instrument market configuration |
| Price/quantity bounds | Quantity must be positive and within the instrument's maximum; prices must be positive; limit-priced orders must sit on the tick grid |
| Lot discipline | Pure odd-lot orders are rejected on lit books; mixed whole-lot/odd orders are rejected; iceberg display floors must be positive and below total quantity on day/GTC orders |
| Hidden-order minimum | Hidden orders below the instrument's Large-In-Scale threshold are rejected; market-pegged hidden orders are not accepted |
| Short-sale control | The instrument's shortable cell decides: freely shortable, exempt-marking shorts only, or no shorts at all |
| Advanced-shape lattice | FOK disables STP; post-only excludes IOC/FOK/iceberg; bypass requires IOC; crosses require a valid cross type and the bypass marker; peg variants enforce hidden/EBBO/midpoint constraints |
| Capacity & entitlement | Order capacity must be permitted for the instrument; regulation-tagged orders require the matching entitlement |
| Price protection | Price-protection flags are only accepted where the market enables them (EBBO-enabled) |
| Credit limit | A priced order whose value exceeds the connection's notional credit limit is rejected; arithmetic anomalies reject rather than misprice |
| Pace control | When a connection's message-rate allowance is exhausted, further orders are rejected until tokens refill |

### Limits & credit

| Capability | Business rule |
|---|---|
| Notional reservation | Authorizing an order books its limit-price value against the connection's credit; fills convert that value to spent credit; kills/cancels restore headroom |
| Open-order slots | The count of working orders per connection is capped; completions free slots |
| Day-so-far reconstruction | At start-of-day, consumption is rebuilt from clearing data, so overnight fills constrain the new session's headroom |
| Exact-limit behavior | An order that exactly consumes remaining headroom is allowed; anything beyond is rejected — no rounding in the member's favor |
| Replay safety | A duplicated lifecycle event may release only what was consumed — headroom can never grow beyond its true value |
| Unconfigured scope | No bucket, no admission: an order against a connection or currency with no configured limit is rejected |

### Rate guards

| Capability | Business rule |
|---|---|
| Burst allowance | A connection may burst up to its configured bucket capacity at any instant |
| Sustained rate | Beyond the burst, messages are admitted at the configured sustained rate, refilling continuously |
| Fail-closed throttle | An exhausted bucket rejects promptly (rate-limited code) rather than admitting excess traffic into the venue |

### Limit administration

| Capability | Business rule |
|---|---|
| Runtime limit changes | Limits are defined per scope (connection / trader / customer / account), currency, and bucket; changes take effect without restarting the service |
| Enable/disable | A disabled limit stops being evaluated; a re-enabled one resumes as configured |
| Preservation on update | Raising or lowering a limit preserves already-consumed value — updating a limit never refunds usage |
| Exposure visibility | Admin pages expose the limit definitions and the live reservation trail for operations review |

### Events & surfaces

| Capability | Business rule |
|---|---|
| Realtime exposure publication | Per-environment exposure snapshots are published continuously (newest-wins) for operations monitoring |
| Durable change events | Limit and reservation changes are delivered durably through the platform event bus (idempotent per change) |
| Audit of privileged actions | Limit changes and administrative corrections are journalled and pushed to platform audit archival |

## 4. Business Events (Outbound)

Emitted via the platform event vocabulary
(`exchange8:{service}:{resource}:{operation}`, colons only):

| Event family | When it fires |
|---|---|
| Limit definition change (`exchange8:risk:limit:*`) | A limit row is created, updated, activated, or disabled through administration or start-of-day load |
| Reservation transition (`exchange8:risk:reservation:*`) | A reservation is booked, converts to consumed (fill), or is released (kill/cancel/correction) |
| Exposure snapshot (`risk.exposure`, realtime MemBus publication) | After each decision cycle affecting exposure; each snapshot fully supersedes the last |
| Guard events | A connection's rate bucket or limit bucket trips (throttle/limit exceeded recorded with the reject code) |

Inbound (consumed): instrument/reference-data state updates, market phase — ME trade/kill lifecycle events arrive over a live Bus stream subscription (no polling delay; missed events recovered by redelivery)
changes, customer/connection state and limit entitlement changes, Shield
policy-set updates, and start-of-day consumption seeds from clearing.

## 5. Business Rules & Invariants

1. **No authorization, no order.** Every submitted order is authorized
   before matching is possible; there is no path from a member to the
   matching core that bypasses this gate.
2. **Fail closed, everywhere.** Unknown instrument, unknown connection,
   unconfigured limit scope, stale state, service unavailability — every
   unknown rejects; no condition silently admits an order.
3. **First failure wins, with the exact catalog code.** Checks run in a
   fixed order and the first failure determines the rejection reason;
   codes are the venue's published contract and never change for a given
   business condition.
4. **Credit is reserved at submission.** Buying power is consumed when an
   order is authorized (reservation), converted to spent only through
   fills, and restored through kills/cancellations; a connection can never
   commit more buying power than its limit.
5. **Exact consumption is allowed.** An order consuming the last unit of
   available headroom is admitted; the next unit is rejected.
6. **Replay cannot create credit.** Repeated release events are
   idempotent — headroom never grows beyond its true day-so-far value.
7. **No limit configured means no admission.** Limits must exist for a
   scope before flow is accepted against it.
8. **Throttling protects the venue.** Rate exhaustion rejects immediately
   and never lets a storm reach downstream services.
9. **Limit changes take effect immediately** and never refund prior
   consumption; the durable consumption model is day-so-far and is
   rebuilt from clearing facts.
10. **Phase discipline is absolute.** Closed, halted, and postponed
    markets admit nothing; pre-open admits only day-validity limit
    orders (quoted-only behavior is the matching tier's job).
11. **Authorization is policy-decided.** Declarative Shield policy is the
    only authorization gate: no policy set denies all, tenant forbids
    override platform permits, shadow evaluations never block, and
    machine (M2M) credentials are held to exactly the same standard as
    users.
12. **Tenant isolation is unconditional.** All durable tables enforce
    row-level security per environment; one tenant's orders, limits, or
    reservations are never visible to another.
13. **Decisions must be auditable.** Reservations, limit changes, and
    administrative corrections are durably journalled and pushed to
    platform audit archival; an outage delays delivery but never loses a
    record.
14. **Every decision is deterministic.** The same order against the same
    state yields the same decision and reason — reproducible under audit
    or dispute.

## 6. Target-User Roles & Use Cases

| Persona | Role | What they do with Exchange8:Risk |
|---|---|---|
| Retail / institutional trader | Order submitter (via broker/OMS) | Experiences the gate only through outcomes: instant accept, or an exact reject explaining what to fix (size, credit, phase, short sale, shape) |
| Sell-side broker / market maker | Quoting member | Operates inside their booked credit and open-order limits; rate guards apply fairly per connection |
| Risk officer / credit desk | Limit administrator | Sets and tunes notional credit, open-order, and rate limits per connection, customer, or account; raises limits intraday without restarting anything |
| Exchange operations | Market operator | Reviews live reservations and exposure during halts or incidents; applies admin corrections through the release surface |
| Compliance / regulatory liaison | Control owner | Uses the audit trail: every decision, limit change, and correction is journalled with before/after states |
| FIX member OMS team | System integrator | Programs against the published reject-code catalog so their OMS reacts to each reason automatically |
| Platform / venue operations | Operator | Monitors exposure snapshots, reject ratios by code, and authorize latency; manages policy sets through the Shield surface |
| Surveillance & clearing | Consumers | Consume reservation transitions and limit changes for day-so-far reconciliation and behaviour analysis |

## 7. KPIs / Success Criteria

- **Decision latency**: synchronous authorization completes well inside
  the 5-millisecond envelope (P99), so the gate adds no perceptible
  round-trip latency to order entry.
- **Admission correctness**: zero known false allows — no order admitted
  against a halted instrument, exhausted credit, unconfigured scope, or
  expired rate allowance (CI-enforced per-check tests).
- **Reject fidelity**: every rejection returns the exact published catalog
  code for its business condition (member OMS compatibility verified by
  parity tests).
- **Credit integrity**: Σ reservations − releases − conversions equals the
  ledger's reserved notional at any time; no double-commit under
  replayed events.
- **Availability posture**: the gate accepts flow while any healthy
  replica serves it, and never degrades to bypass; unavailability is
  always visible to callers as a fail-closed reject.
- **Isolation**: zero cross-environment information flow (RLS matrix
  green in every suite run).
- **Audit completeness**: every privileged limit change lands in platform
  audit archival with before/after states.
- **Operational responsiveness**: limit changes take effect on the next
  order without a service restart.

## 8. Dependencies & Reliance

- **Exchange8:Gateway** — the only order-admission caller; it owns sessions,
  protocol translation, shape validation, and the fail-closed behavior on
  risk unavailability.
- **Exchange8:RefData** — instrument configuration needed by the matrix
  (status, tick regime, lot size, shortable cell, LIS thresholds,
  capacities, TIFs); consumed as snapshot events, never joined from SQL.
- **Exchange8:Clearing** — day-so-far consumption truth for start-of-day
  seeding (fills and working orders from the prior session).
- **Exchange8:Margin** — prospective supplier of margin requirements for
  derivative instruments' pre-trade checks (this service never computes
  margin itself).
- **Exchange8:ME / Exchange8:OMS / Exchange8:SOR** — lifecycle events (fills, kills,
  completions) that release or convert reservations.
- **Sentin8:Auth** — all identity issuance/verification; M2M workloads
  (`exchange8:risk`/`risk`, `risk-audit`, `exchange8-risk-tls`),
  cert-bound by default.
- **Shield (Sentin8:Policy)** — the only authorization gate; the seeded
  scope-mirror set is the service's declared contract; the admin surface
  shares the engine cache.
- **Sentin8:Bus / Sentin8:MemBus** — durable change delivery (outbox
  relay) and realtime exposure publication; publication degrades without
  blocking the gate.
- **Sentin8:Sys** — audit ingest (`POST /v1/audit`) and the TLS hostname
  resolution owner API.
- **Sentin8:DB (PostgreSQL 18, db `exchange8`, schema `risk`)** — limit
  definitions, reservation trail, outbox, audit journal; accessed only
  via the service's own role under row-level security.
- **Platform shell (`sentin8::shell`)** — registration, pool, health,
  drain, allocator; `sentin8-tls` for in-process post-quantum TLS.

## 9. Acceptance Criteria

1. A valid limit order from an enabled, entitled connection with
   sufficient credit and rate allowance is admitted with a decision that
   carries no failure code; the admission books its value as a
   reservation.
2. A priced order exceeding the connection's remaining notional credit is
   rejected with the catalog over-limit code, and the ledger is unchanged.
3. An order exactly consuming remaining headroom is admitted, and the
   next unit beyond it is rejected.
4. A fill converts the reserved value to spent credit; a kill or cancel
   restores it; a duplicate lifecycle event changes nothing (saturating
   release).
5. Each business condition in the check matrix (suspension, enablement,
   book, phase, halt, holiday, currency, type/TIF, tick, lot, floor, size,
   LIS, short sale, STP/FOK, peg variants, post-only, cross/bypass,
   capacity, entitlement, price protection, rate) rejects with its exact
   published code, in the documented precedence order.
6. A connection with an exhausted rate bucket receives prompt
   rate-limiting rejections; tokens refill at the configured sustained
   rate and the burst cap is never exceeded.
7. An order against a scope with no configured limit is rejected — never
   admitted by default.
8. Start-of-day load rebuilds consumption from clearing data so prior
   fills constrain the new session from the first order.
9. Limit definitions and enable/disable changes take effect without a
   service restart, preserve existing consumption, and are journalled to
   platform audit.
10. Every durable table is RLS-forced with provable environment
    isolation (no context → zero rows; cross-env → zero rows;
    contextless insert → rejected), and the service role holds no
    bypass privileges.
11. Shield's seeded policy is the only authorization gate: missing or
    non-matching sets deny (also for machine tokens), shadow never
    blocks, tenant forbids win, and platform-admin scopes pass.
12. Events are delivered durably and idempotently through the platform
    bus (retries on outage, replayable dead letters), and exposure
    snapshots publish realtime with newest-wins semantics.
13. Rolling instance upgrades complete with graceful drain; no in-flight
    authorization is truncated and no registry entry goes stale.
14. Risk-service unavailability manifests only as fail-closed rejections
    upstream — there is no degradation path that admits unexamined orders.

## 10. Open Business Questions

- Venue-wide rate-guard defaults (burst size and sustained rate per
  connection class or member tier) are a business configuration decision;
  the mechanism and its fail-closed semantics are final.
- The public business event payloads for reservation transitions and
  limit changes (the event-name vocabulary is fixed; the per-event field
  contract for clearing/surveillance consumers is being finalized).
- The precise division of pre-trade validation responsibility between the
  gateway's shape checks and this service's business checks (which codes
  members may receive from either surface) is being codified for the
  member-facing rulebook.
- Whether per-trader and per-account scoped limits (supported by the
  limit schema) will be enabled for all participant classes or only
  institutional members — an onboarding policy decision.
- Options pre-trade checks consulting margin requirements (derivatives
  extension): the business rule set and the requirement-freshness
  tolerance for a synchronous gate are pending the derivatives design.
- Risk-bypass operating procedure: the per-connection bypass lever is
  default-off and platform-admin-owned; the approval workflow and
  auditability around its use are a business-governance matter.

---

**Design attribution:** exchange8:risk and its underlying concepts were designed by Hamid Jahanzad <Hamid@Jahanzad.com>.
