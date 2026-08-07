-- 业务作用：为参与方 gate 固定 definition 摘要，防止同一版本合同漂移复用旧效果身份。
-- 历史行暂时保持 NULL：旧业务事实无法可靠反推摘要。部署流程必须按受信 definition
-- 受信记录逐组回填后再执行摘要约束脚本封闭 NULL 窗；本脚本单独完成时仍不允许新 binary 启动。
ALTER TABLE saga_participant_step
    ADD COLUMN definition_digest CHAR(64) NULL AFTER definition_version;
