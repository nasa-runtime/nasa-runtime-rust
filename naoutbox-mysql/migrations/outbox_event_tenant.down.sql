-- 回退移除租户归因列;执行前必须确认没有任何部署仍启用 outbox 租户配额,
-- 否则受信入口的预留与释放会因列缺失而失败。

ALTER TABLE outbox_event
    DROP INDEX idx_tenant_dispatchable,
    DROP COLUMN tenant;
