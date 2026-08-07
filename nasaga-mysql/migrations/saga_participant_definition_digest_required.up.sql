-- 业务作用：先以独立于 sql_mode 的约束核对历史摘要，再封闭 NULL 兼容窗。
-- 非严格模式会把 MODIFY NOT NULL 遇到的 NULL 静默改为空串；必须先添加 CHECK，
-- 让 NULL、空串、非 64 位或非小写十六进制摘要在任何会话模式下都使迁移失败。
ALTER TABLE saga_participant_step
    ADD CONSTRAINT chk_saga_participant_definition_digest
    CHECK (
        definition_digest IS NOT NULL
        AND CHAR_LENGTH(definition_digest) = 64
        AND BINARY definition_digest = BINARY LOWER(definition_digest)
        AND definition_digest NOT REGEXP '[^0-9a-f]'
    );

-- 只有 CHECK 已覆盖所有历史行后才能收紧列定义；此时 sql_mode 不再影响封口语义。
ALTER TABLE saga_participant_step
    MODIFY COLUMN definition_digest CHAR(64) NOT NULL;
