-- 业务作用：创建 PostgreSQL Orchestrator 所需的 Saga 状态、治理、审计与定时器持久结构。
CREATE TABLE IF NOT EXISTS saga_instance (
    saga_id VARCHAR(256) PRIMARY KEY,
    tenant_id VARCHAR(256) NOT NULL,
    workflow_name VARCHAR(128) NOT NULL,
    business_key VARCHAR(256) NOT NULL,
    definition_version BIGINT NOT NULL CHECK (definition_version > 0),
    definition_digest CHAR(64) NOT NULL,
    start_request_digest CHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    control_state VARCHAR(16) NOT NULL,
    control_version BIGINT NOT NULL CHECK (control_version > 0),
    direction VARCHAR(16) NOT NULL,
    current_step VARCHAR(128),
    compensation_plan_version CHAR(64),
    version BIGINT NOT NULL CHECK (version > 0),
    deadline_at BIGINT,
    failure_code VARCHAR(64),
    traceparent VARCHAR(55),
    paused_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT saga_instance_business_key UNIQUE (tenant_id, workflow_name, business_key)
);

CREATE TABLE IF NOT EXISTS saga_step (
    saga_id VARCHAR(256) NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    ordinal BIGINT NOT NULL CHECK (ordinal > 0),
    forward_status VARCHAR(32) NOT NULL,
    cancel_status VARCHAR(32) NOT NULL,
    compensation_status VARCHAR(32) NOT NULL,
    resolution_status VARCHAR(32) NOT NULL,
    execute_effect_id CHAR(36),
    execute_command_id CHAR(36),
    execute_attempt BIGINT,
    cancel_effect_id CHAR(36),
    cancel_command_id CHAR(36),
    cancel_attempt BIGINT,
    compensate_effect_id CHAR(36),
    compensate_command_id CHAR(36),
    compensate_attempt BIGINT,
    resolve_effect_id CHAR(36),
    resolve_command_id CHAR(36),
    resolve_attempt BIGINT,
    compensation_plan_version CHAR(64),
    compensation_order BIGINT,
    last_error_code VARCHAR(64),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    PRIMARY KEY (saga_id, step_name)
);

CREATE TABLE IF NOT EXISTS saga_step_attempt (
    saga_id VARCHAR(256) NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    phase VARCHAR(16) NOT NULL,
    attempt_no BIGINT NOT NULL CHECK (attempt_no > 0),
    effect_id CHAR(36) NOT NULL,
    command_id CHAR(36) NOT NULL,
    status VARCHAR(32) NOT NULL,
    outcome_event_id VARCHAR(190),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    PRIMARY KEY (saga_id, step_name, phase, attempt_no),
    CONSTRAINT saga_step_attempt_effect UNIQUE (effect_id, attempt_no),
    CONSTRAINT saga_step_attempt_command UNIQUE (command_id)
);

CREATE TABLE IF NOT EXISTS saga_transition (
    saga_id VARCHAR(256) NOT NULL,
    transition_seq BIGINT NOT NULL CHECK (transition_seq > 0),
    from_state VARCHAR(32) NOT NULL,
    to_state VARCHAR(32) NOT NULL,
    trigger_kind VARCHAR(8) NOT NULL,
    trigger_id VARCHAR(190) NOT NULL,
    definition_version BIGINT NOT NULL CHECK (definition_version > 0),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (saga_id, transition_seq),
    CONSTRAINT saga_transition_trigger UNIQUE (saga_id, trigger_kind, trigger_id)
);

CREATE TABLE IF NOT EXISTS saga_control_transition (
    saga_id VARCHAR(256) NOT NULL,
    control_seq BIGINT NOT NULL CHECK (control_seq > 0),
    from_state VARCHAR(16) NOT NULL,
    to_state VARCHAR(16) NOT NULL,
    operation_id VARCHAR(190) NOT NULL,
    actor VARCHAR(128) NOT NULL,
    reason VARCHAR(512) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (saga_id, control_seq),
    CONSTRAINT saga_control_operation UNIQUE (saga_id, operation_id)
);

CREATE TABLE IF NOT EXISTS saga_management_audit (
    saga_id VARCHAR(256) NOT NULL,
    operation_id VARCHAR(190) NOT NULL,
    action VARCHAR(64) NOT NULL,
    actor VARCHAR(128) NOT NULL,
    reason VARCHAR(512) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (saga_id, operation_id)
);

CREATE TABLE IF NOT EXISTS saga_conflict_fact (
    saga_id VARCHAR(256) NOT NULL,
    incoming_event_id VARCHAR(190) NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    phase VARCHAR(16) NOT NULL,
    attempt_no BIGINT NOT NULL CHECK (attempt_no > 0),
    existing_status VARCHAR(32) NOT NULL,
    incoming_status VARCHAR(32) NOT NULL,
    conflict_kind VARCHAR(64) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (saga_id, incoming_event_id)
);

CREATE TABLE IF NOT EXISTS saga_timer (
    saga_id VARCHAR(256) NOT NULL,
    scope_kind VARCHAR(8) NOT NULL,
    scope_key VARCHAR(128) NOT NULL,
    kind VARCHAR(64) NOT NULL,
    attempt_no BIGINT NOT NULL CHECK (attempt_no > 0),
    timer_id VARCHAR(190) NOT NULL,
    due_at BIGINT NOT NULL,
    available_at BIGINT NOT NULL,
    state VARCHAR(16) NOT NULL,
    expected_saga_version BIGINT NOT NULL CHECK (expected_saga_version > 0),
    generation BIGINT NOT NULL CHECK (generation > 0),
    owner VARCHAR(128),
    fencing_token VARCHAR(64),
    claimed_until BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (saga_id, scope_kind, scope_key, kind, attempt_no),
    CONSTRAINT saga_timer_identity UNIQUE (timer_id)
);

CREATE TABLE IF NOT EXISTS saga_tenant_quota (
    tenant_id VARCHAR(256) NOT NULL,
    in_flight BIGINT NOT NULL DEFAULT 0 CHECK (in_flight >= 0),
    initialized BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id)
);

CREATE TABLE IF NOT EXISTS saga_tenant_action_rate (
    tenant_id VARCHAR(256) NOT NULL,
    window_start_ms BIGINT NOT NULL CHECK (window_start_ms >= 0),
    used BIGINT NOT NULL DEFAULT 0 CHECK (used >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, window_start_ms)
);

CREATE OR REPLACE FUNCTION nasaga_set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS nasaga_instance_updated_at ON saga_instance;
CREATE TRIGGER nasaga_instance_updated_at
BEFORE UPDATE ON saga_instance
FOR EACH ROW EXECUTE FUNCTION nasaga_set_updated_at();

DROP TRIGGER IF EXISTS nasaga_timer_updated_at ON saga_timer;
CREATE TRIGGER nasaga_timer_updated_at
BEFORE UPDATE ON saga_timer
FOR EACH ROW EXECUTE FUNCTION nasaga_set_updated_at();

CREATE INDEX IF NOT EXISTS saga_instance_status_idx ON saga_instance(status);
CREATE INDEX IF NOT EXISTS saga_instance_lifecycle_idx
    ON saga_instance(status, created_at, updated_at);
CREATE INDEX IF NOT EXISTS saga_step_ordinal_idx ON saga_step(saga_id, ordinal);
CREATE INDEX IF NOT EXISTS saga_attempt_status_idx ON saga_step_attempt(status, finished_at);
CREATE INDEX IF NOT EXISTS saga_attempt_retry_idx ON saga_step_attempt(attempt_no);
CREATE INDEX IF NOT EXISTS saga_transition_state_idx ON saga_transition(to_state, occurred_at);
CREATE INDEX IF NOT EXISTS saga_management_actor_idx
    ON saga_management_audit(actor, occurred_at);
CREATE INDEX IF NOT EXISTS saga_conflict_time_idx
    ON saga_conflict_fact(saga_id, occurred_at);
CREATE INDEX IF NOT EXISTS saga_timer_due_idx ON saga_timer(state, available_at, due_at);
