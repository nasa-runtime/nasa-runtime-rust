-- 业务作用：为新部署建立当前 Outbox 结构；既有部署使用索引迁移补齐相同查询合同。
CREATE TABLE IF NOT EXISTS outbox_event (
    id BIGINT NOT NULL AUTO_INCREMENT,
    event_id CHAR(36) NOT NULL,
    aggregate_type VARCHAR(128) NOT NULL,
    aggregate_id VARCHAR(190) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    payload LONGBLOB NOT NULL,
    traceparent VARCHAR(64) NULL,
    dispatched TINYINT NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    dead TINYINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dispatched_at TIMESTAMP NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_id (event_id),
    KEY idx_dispatchable (dispatched, dead, id),
    KEY idx_dead (dead, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
