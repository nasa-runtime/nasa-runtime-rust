-- 业务作用：租户管理动作速率账本。变更类管理动作(pause/resume/retry/manual close)在
-- 动作事务内按数据库时钟的固定窗口做条件自增预留,动作回滚时预算一并退还;单租户
-- 刷重试类动作不能挤占其它租户的恢复通道。过期窗口行由预留路径按租户顺带回收。

CREATE TABLE saga_tenant_action_rate (
    tenant_id VARCHAR(256) NOT NULL,
    window_start_ms BIGINT UNSIGNED NOT NULL,
    used BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, window_start_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
