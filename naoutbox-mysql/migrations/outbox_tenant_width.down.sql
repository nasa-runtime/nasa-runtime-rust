-- 回退把租户列收窄回 190 字节,仅配合回退到旧版二进制使用。执行前必须确认两表中
-- 不存在长度超过 190 字节的租户值——收窄会截断或拒绝此类行,截断意味着租户归因与
-- 配额账本主键被静默改写。存在超长值时禁止回退,先按治理流程处置对应租户数据。
--
-- 前置核查(两条计数都必须为 0):
--   SELECT COUNT(*) FROM outbox_event WHERE CHAR_LENGTH(tenant) > 190;
--   SELECT COUNT(*) FROM outbox_tenant_quota WHERE CHAR_LENGTH(tenant_id) > 190;

ALTER TABLE outbox_event
    MODIFY COLUMN tenant VARCHAR(190) NOT NULL DEFAULT 'system';

ALTER TABLE outbox_tenant_quota
    MODIFY COLUMN tenant_id VARCHAR(190) NOT NULL;
