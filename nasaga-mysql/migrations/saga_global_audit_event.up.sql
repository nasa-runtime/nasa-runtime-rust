-- 业务作用：按 Saga 串行化审计事件的序号分配，使后提交事务不能越过未提交前驱。
CREATE TABLE saga_audit_stream_guard (
    saga_id VARCHAR(256) NOT NULL,
    generation BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (saga_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- 业务作用：建立跨审计类别的全局单调事件流，使分页期间新增事实和 attempt 终态变化可继续交付。
CREATE TABLE saga_audit_event (
    audit_seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    saga_id VARCHAR(256) NOT NULL,
    record_kind VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_identity_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_revision_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    step_name VARCHAR(128) NULL,
    phase VARCHAR(16) NULL,
    attempt_no INT UNSIGNED NULL,
    effect_id CHAR(36) NULL,
    command_id CHAR(36) NULL,
    attempt_status VARCHAR(32) NULL,
    outcome_event_id VARCHAR(190) NULL,
    transition_seq BIGINT UNSIGNED NULL,
    from_state VARCHAR(32) NULL,
    to_state VARCHAR(32) NULL,
    trigger_kind VARCHAR(8) NULL,
    trigger_id VARCHAR(190) NULL,
    definition_version INT UNSIGNED NULL,
    control_seq BIGINT UNSIGNED NULL,
    operation_id VARCHAR(190) NULL,
    action VARCHAR(64) NULL,
    actor VARCHAR(128) NULL,
    reason VARCHAR(512) NULL,
    incoming_event_id VARCHAR(190) NULL,
    existing_status VARCHAR(32) NULL,
    incoming_status VARCHAR(32) NULL,
    conflict_kind VARCHAR(64) NULL,
    occurred_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (audit_seq),
    UNIQUE KEY uk_saga_audit_event_revision
        (saga_id, record_kind, event_identity_digest, event_revision_digest),
    KEY idx_saga_audit_event_stream (saga_id, audit_seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- 每个 trigger 在分配 audit_seq 前递增所属 Saga 的 guard，确保序号可见顺序与提交顺序一致。
DELIMITER $$

CREATE TRIGGER nasaga_audit_attempt_insert
AFTER INSERT ON saga_step_attempt FOR EACH ROW
BEGIN
INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1)
ON DUPLICATE KEY UPDATE generation = generation + 1;
INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, step_name, phase,
     attempt_no, effect_id, command_id, attempt_status, outcome_event_id, occurred_at)
VALUES
    (NEW.saga_id, 'attempt',
     SHA2(CONCAT_WS(CHAR(31), NEW.step_name, NEW.phase, NEW.attempt_no), 256),
     SHA2('started', 256), NEW.step_name, NEW.phase, NEW.attempt_no, NEW.effect_id,
     NEW.command_id, NEW.status, NEW.outcome_event_id, NEW.started_at);
END$$

CREATE TRIGGER nasaga_audit_attempt_update
AFTER UPDATE ON saga_step_attempt FOR EACH ROW
BEGIN
INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1)
ON DUPLICATE KEY UPDATE generation = generation + 1;
INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, step_name, phase,
     attempt_no, effect_id, command_id, attempt_status, outcome_event_id, occurred_at)
VALUES
    (NEW.saga_id, 'attempt',
     SHA2(CONCAT_WS(CHAR(31), NEW.step_name, NEW.phase, NEW.attempt_no), 256),
     SHA2(CONCAT_WS(CHAR(31), 'status', NEW.status, COALESCE(NEW.outcome_event_id, '')), 256),
     NEW.step_name, NEW.phase, NEW.attempt_no, NEW.effect_id, NEW.command_id, NEW.status,
     NEW.outcome_event_id, COALESCE(NEW.finished_at, CURRENT_TIMESTAMP(6)));
END$$

CREATE TRIGGER nasaga_audit_transition_insert
AFTER INSERT ON saga_transition FOR EACH ROW
BEGIN
INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1)
ON DUPLICATE KEY UPDATE generation = generation + 1;
INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, transition_seq,
     from_state, to_state, trigger_kind, trigger_id, definition_version, occurred_at)
VALUES
    (NEW.saga_id, 'transition', SHA2(CAST(NEW.transition_seq AS CHAR), 256), SHA2('fact', 256),
     NEW.transition_seq, NEW.from_state, NEW.to_state, NEW.trigger_kind, NEW.trigger_id,
     NEW.definition_version, NEW.occurred_at);
END$$

CREATE TRIGGER nasaga_audit_control_insert
AFTER INSERT ON saga_control_transition FOR EACH ROW
BEGIN
INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1)
ON DUPLICATE KEY UPDATE generation = generation + 1;
INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, control_seq,
     from_state, to_state, operation_id, actor, reason, occurred_at)
VALUES
    (NEW.saga_id, 'control', SHA2(NEW.operation_id, 256), SHA2('fact', 256), NEW.control_seq,
     NEW.from_state, NEW.to_state, NEW.operation_id, NEW.actor, NEW.reason, NEW.occurred_at);
END$$

CREATE TRIGGER nasaga_audit_management_insert
AFTER INSERT ON saga_management_audit FOR EACH ROW
BEGIN
INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1)
ON DUPLICATE KEY UPDATE generation = generation + 1;
INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, operation_id,
     action, actor, reason, occurred_at)
VALUES
    (NEW.saga_id, 'management', SHA2(NEW.operation_id, 256), SHA2('fact', 256),
     NEW.operation_id, NEW.action, NEW.actor, NEW.reason, NEW.occurred_at);
END$$

CREATE TRIGGER nasaga_audit_conflict_insert
AFTER INSERT ON saga_conflict_fact FOR EACH ROW
BEGIN
INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1)
ON DUPLICATE KEY UPDATE generation = generation + 1;
INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, incoming_event_id,
     step_name, phase, attempt_no, existing_status, incoming_status, conflict_kind, occurred_at)
VALUES
    (NEW.saga_id, 'conflict', SHA2(NEW.incoming_event_id, 256), SHA2('fact', 256),
     NEW.incoming_event_id, NEW.step_name, NEW.phase, NEW.attempt_no, NEW.existing_status,
     NEW.incoming_status, NEW.conflict_kind, NEW.occurred_at);
END$$

DELIMITER ;

-- 业务作用：冻结需要历史映射的 Saga 集合；新事实已由 trigger 同事务进入统一事件流。
CREATE TEMPORARY TABLE nasaga_audit_backfill_saga (
    saga_id VARCHAR(256) NOT NULL,
    PRIMARY KEY (saga_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT IGNORE INTO nasaga_audit_backfill_saga (saga_id)
SELECT saga_id FROM saga_step_attempt
UNION SELECT saga_id FROM saga_transition
UNION SELECT saga_id FROM saga_control_transition
UNION SELECT saga_id FROM saga_management_audit
UNION SELECT saga_id FROM saga_conflict_fact;

START TRANSACTION;

-- 回填读取任何存量事实前先按稳定顺序取得全部 Saga guard，并持有到事件提交；
-- 在线事务若已分配较小序号，回填必须等待其提交后再分配可见序号。
INSERT INTO saga_audit_stream_guard (saga_id, generation)
SELECT saga_id, 1 FROM nasaga_audit_backfill_saga ORDER BY saga_id
ON DUPLICATE KEY UPDATE generation = saga_audit_stream_guard.generation + 1;

-- 存量事实只生成当前快照；唯一键保证部署重试不会复制同一事件 revision。
INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, step_name, phase,
     attempt_no, effect_id, command_id, attempt_status, outcome_event_id, occurred_at)
SELECT saga_id, 'attempt', SHA2(CONCAT_WS(CHAR(31), step_name, phase, attempt_no), 256),
    SHA2(IF(status = 'STARTED', 'started',
        CONCAT_WS(CHAR(31), 'status', status, COALESCE(outcome_event_id, ''))), 256),
    step_name, phase, attempt_no, effect_id, command_id, status, outcome_event_id,
    COALESCE(finished_at, started_at)
FROM saga_step_attempt
JOIN nasaga_audit_backfill_saga USING (saga_id);

INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, transition_seq,
     from_state, to_state, trigger_kind, trigger_id, definition_version, occurred_at)
SELECT saga_id, 'transition', SHA2(CAST(transition_seq AS CHAR), 256), SHA2('fact', 256),
    transition_seq, from_state, to_state, trigger_kind, trigger_id, definition_version, occurred_at
FROM saga_transition
JOIN nasaga_audit_backfill_saga USING (saga_id);

INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, control_seq,
     from_state, to_state, operation_id, actor, reason, occurred_at)
SELECT saga_id, 'control', SHA2(operation_id, 256), SHA2('fact', 256), control_seq,
    from_state, to_state, operation_id, actor, reason, occurred_at
FROM saga_control_transition
JOIN nasaga_audit_backfill_saga USING (saga_id);

INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, operation_id,
     action, actor, reason, occurred_at)
SELECT saga_id, 'management', SHA2(operation_id, 256), SHA2('fact', 256), operation_id,
    action, actor, reason, occurred_at
FROM saga_management_audit
JOIN nasaga_audit_backfill_saga USING (saga_id);

INSERT IGNORE INTO saga_audit_event
    (saga_id, record_kind, event_identity_digest, event_revision_digest, incoming_event_id,
     step_name, phase, attempt_no, existing_status, incoming_status, conflict_kind, occurred_at)
SELECT saga_id, 'conflict', SHA2(incoming_event_id, 256), SHA2('fact', 256), incoming_event_id,
    step_name, phase, attempt_no, existing_status, incoming_status, conflict_kind, occurred_at
FROM saga_conflict_fact
JOIN nasaga_audit_backfill_saga USING (saga_id);

COMMIT;

DROP TEMPORARY TABLE nasaga_audit_backfill_saga;
