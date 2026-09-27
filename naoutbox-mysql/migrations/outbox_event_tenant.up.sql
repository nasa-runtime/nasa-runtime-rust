-- 业务作用：outbox_event 增加受信租户归因列。历史行与未携带写入上下文的 append 固定
-- 归入 system 租户;租户身份只能由已认证业务上下文填充,不得从 payload、aggregate_id
-- 或自报 header 解析。索引服务按租户的在飞对账与积压观测。

ALTER TABLE outbox_event
    ADD COLUMN tenant VARCHAR(256) NOT NULL DEFAULT 'system' AFTER traceparent,
    ADD INDEX idx_tenant_dispatchable (tenant, dispatched, dead, id);
