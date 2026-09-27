-- 业务作用：租户在飞实例配额账本。创建事务原子预留(+1)、终态迁移同事务释放(-1),
-- 条件自增在租户行锁上串行化并发创建,上限不可被无锁计数穿透。账本漂移由有界对账
-- (按租户重算非终态实例数,事务内行锁先行)收敛,不在请求路径扫描整表。

CREATE TABLE saga_tenant_quota (
    tenant_id VARCHAR(256) NOT NULL,
    in_flight BIGINT UNSIGNED NOT NULL DEFAULT 0,
    -- 初始化标记:仅 reconcile 在受锁窗口置位;设限租户未置位时预留与 Ready 均拒绝,
    -- 防止存量非终态实例的终态释放扣掉新实例名额、上限被静默穿透。
    initialized TINYINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
