-- SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
-- SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
-- SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Risk
-- SPDX-License-Identifier: Apache-2.0

-- Ex8:Risk - pre-trade risk limits and reservation ledger.
--
-- Design: design/sentin8-exchange8-data-model-technical-design.md §3.5 (RISK).
-- No migration code (new platform): `sentin8-cli vertical init` applies this.

CREATE SCHEMA IF NOT EXISTS risk;

-- Pre-trade limits (legacy risk-limit matrix).
CREATE TABLE IF NOT EXISTS risk.risk_limit (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    scope_type      TEXT NOT NULL CHECK (scope_type IN
                    ('connection','trader','customer','account')),
    scope_id        TEXT NOT NULL,
    currency        TEXT NOT NULL,
    limit_type      TEXT NOT NULL CHECK (limit_type IN
                    ('order_value','gross','net','exposure','per_order_value',
                     'per_order_qty','per_trade_value','daily_value')),
    bucket          TEXT NOT NULL,       -- charge bucket the limit applies to
    limit_value     NUMERIC(20,2) NOT NULL,
    status          TEXT NOT NULL DEFAULT 'E' CHECK (status IN ('E','D'))
);
CREATE INDEX IF NOT EXISTS idx_exchange8_risklimit_env ON risk.risk_limit (env_id);
CREATE INDEX IF NOT EXISTS idx_exchange8_risklimit_scope ON risk.risk_limit (env_id, scope_type, scope_id);

-- Bucket consumption ledger: active reservations reduce available headroom.
CREATE TABLE IF NOT EXISTS risk.risk_reservation (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    order_id    BIGINT NOT NULL,
    scope_type  TEXT NOT NULL,
    scope_id    TEXT NOT NULL,
    currency    TEXT NOT NULL,
    bucket      TEXT NOT NULL,
    value       NUMERIC(20,2) NOT NULL,
    state       TEXT NOT NULL DEFAULT 'reserved' CHECK (state IN
                ('reserved','consumed','released')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_exchange8_riskres_env_order ON risk.risk_reservation (env_id, order_id);
CREATE INDEX IF NOT EXISTS idx_exchange8_riskres_env_scope ON risk.risk_reservation (env_id, scope_type, scope_id, state);

-- ---------------------------------------------------------------------------
-- RLS (architecture-principles.md §4.3): enable + force on all tenant tables.
-- Policies use the platform helpers sentin8_context_initialized() and
-- sentin8_current_env_id(); services call set_tenant_context on the same
-- connection as the data query.
-- ---------------------------------------------------------------------------
DO $$
DECLARE tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['risk.risk_limit', 'risk.risk_reservation'] LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
            EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_select ON %2$s;
CREATE POLICY %1$I_select ON %2$s FOR SELECT USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_insert ON %2$s;
CREATE POLICY %1$I_insert ON %2$s FOR INSERT WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_update ON %2$s;
CREATE POLICY %1$I_update ON %2$s FOR UPDATE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_delete ON %2$s;
CREATE POLICY %1$I_delete ON %2$s FOR DELETE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'RLS setup failed for %: %', tbl, SQLERRM;
        END;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- Per-service role grants (schema-per-service: this service owns schema
-- `risk`; grants live in db `exchange8` only - design doc §1).
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'exchange8_risk_service') THEN
        ALTER ROLE exchange8_risk_service SET search_path = risk, public;
        GRANT USAGE ON SCHEMA risk TO exchange8_risk_service;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA risk TO exchange8_risk_service;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA risk TO exchange8_risk_service;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Local outbox (DBX §6.7 - generated from `sentin8_outbox::outbox_table_sql(
-- "risk")`): rows written in the same DB transaction as the domain change;
-- the shell's relay (boot step §8b) pushes them to Bus `POST /v1/produce`.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS risk.outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    event_type      TEXT NOT NULL,
    stream_family   TEXT NOT NULL,
    payload         JSONB NOT NULL,
    publisher_id_token  TEXT,
    publisher_subject   TEXT,
    relayed         BOOLEAN NOT NULL DEFAULT false,
    relayed_at      TIMESTAMPTZ,
    relay_attempts  INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_risk_outbox_unrelayed
    ON risk.outbox (created_at) WHERE relayed = false;
DO $$ BEGIN
    EXECUTE 'ALTER TABLE risk.outbox ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE risk.outbox FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS outbox_select ON risk.outbox';
    EXECUTE 'CREATE POLICY outbox_select ON risk.outbox FOR SELECT
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
    EXECUTE 'DROP POLICY IF EXISTS outbox_insert ON risk.outbox';
    EXECUTE 'CREATE POLICY outbox_insert ON risk.outbox FOR INSERT
        WITH CHECK (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
    EXECUTE 'DROP POLICY IF EXISTS outbox_update ON risk.outbox';
    EXECUTE 'CREATE POLICY outbox_update ON risk.outbox FOR UPDATE
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'RLS setup failed for risk.outbox: %', SQLERRM;
END $$;

CREATE OR REPLACE FUNCTION risk.risk_notify_outbox() RETURNS TRIGGER
LANGUAGE plpgsql AS $fn$
BEGIN
    PERFORM pg_notify('risk_outbox_changed', '');
    RETURN NEW;
END;
$fn$;
DROP TRIGGER IF EXISTS outbox_notify_trigger ON risk.outbox;
CREATE TRIGGER outbox_notify_trigger
    AFTER INSERT ON risk.outbox
    FOR EACH ROW
    EXECUTE FUNCTION risk.risk_notify_outbox();

-- ---------------------------------------------------------------------------
-- Local audit journal (DBX §6.8 - generated from
-- `sentin8_audit::audit_journal_sql("risk")`): rows written in the same
-- transaction as the domain change; the shell's pusher (boot step §8c) pushes
-- them to Sys `POST /v1/audit` (`sentin8:sys:audit:write`). A Sys outage
-- never drops audit.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS risk.audit_journal (
    id              UUID PRIMARY KEY,
    env_id          UUID NOT NULL,
    action          TEXT NOT NULL,
    target_resource_type TEXT,
    target_resource_id    TEXT,
    actor_user_id   TEXT,
    actor_subject_type TEXT,
    severity        TEXT NOT NULL DEFAULT 'info',
    correlation_id  TEXT,
    before_state    JSONB,
    after_state     JSONB,
    details         JSONB,
    pushed          BOOLEAN NOT NULL DEFAULT false,
    pushed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_risk_audit_journal_unpushed
    ON risk.audit_journal (created_at) WHERE pushed = false;
DO $$ BEGIN
    EXECUTE 'ALTER TABLE risk.audit_journal ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE risk.audit_journal FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS audit_journal_dml ON risk.audit_journal';
    EXECUTE 'CREATE POLICY audit_journal_dml ON risk.audit_journal
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())
        WITH CHECK (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'RLS setup failed for risk.audit_journal: %', SQLERRM;
END $$;

-- Outbox + journal role grants (same pattern as the table grants above).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'exchange8_risk_service') THEN
        GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA risk TO exchange8_risk_service;
    END IF;
END $$;
