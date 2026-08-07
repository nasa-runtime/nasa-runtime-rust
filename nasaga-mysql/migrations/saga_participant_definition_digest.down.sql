-- 业务作用：仅在已回退到不读取 digest 的旧 binary 后移除参与方合同锚点。
ALTER TABLE saga_participant_step
    DROP COLUMN definition_digest;
