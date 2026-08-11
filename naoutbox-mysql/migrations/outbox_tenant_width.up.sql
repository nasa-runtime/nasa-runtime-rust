-- 业务作用：把存量部署的租户归因列宽对齐公开身份合同(256 字节)。仅在早期窄宽度
-- 版本迁移(tenant/tenant_id 为 VARCHAR(190))上执行过的数据库需要本迁移;新建库与
-- 直接执行当前版本 outbox_event_tenant/outbox_tenant_quota 的库已是 256,无需重复。
-- 列宽不足时 191..=256 字节的合法租户会通过全部身份校验,却在首笔 Outbox 写入或
-- 账本操作处失败;Ready 期结构校验按本迁移名给出修复指引。

ALTER TABLE outbox_event
    MODIFY COLUMN tenant VARCHAR(256) NOT NULL DEFAULT 'system';

ALTER TABLE outbox_tenant_quota
    MODIFY COLUMN tenant_id VARCHAR(256) NOT NULL;
