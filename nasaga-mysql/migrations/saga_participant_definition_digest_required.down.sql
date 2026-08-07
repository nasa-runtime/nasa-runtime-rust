-- 业务作用：回退到摘要绑定窗口时先解除格式约束，再恢复 nullable；不会删除已核验摘要。
ALTER TABLE saga_participant_step
    DROP CHECK chk_saga_participant_definition_digest;

ALTER TABLE saga_participant_step
    MODIFY COLUMN definition_digest CHAR(64) NULL;
