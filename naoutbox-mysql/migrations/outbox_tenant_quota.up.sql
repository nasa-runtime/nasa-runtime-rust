-- 业务作用：租户在飞事件配额账本。受信 append 事务原子预留(+1),投递/死信裁决同
-- 语句/事务释放(-1);条件自增在租户行锁上串行化并发 append,上限不可被无锁计数穿透。
-- 账本漂移由有界对账(事务内行锁先行,按租户重算 dispatched=0 AND dead=0 行数)收敛。

CREATE TABLE outbox_tenant_quota (
    tenant_id VARCHAR(256) NOT NULL,
    in_flight BIGINT UNSIGNED NOT NULL DEFAULT 0,
    -- 初始化标记:仅 reconcile 在受锁窗口置位;设限租户未置位时受信 append 与 Ready
    -- 校验均拒绝,防止存量待投递行的投递/死信释放扣掉新行名额。
    initialized TINYINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
