-- 业务作用：让参与方 gate 的 Saga/tenant/workflow/effect 身份按字节比较，避免默认
-- 不区分大小写排序规则把两个业务效果串到同一 gate。
-- 本脚本只在参与方数据库执行；不要在纯 Orchestrator 数据库执行。

-- 本文件只用于停写维护窗；在线发布必须走 tests/ci/saga-online-ddl-live 的 OSC 门禁。
ALTER TABLE saga_participant_step CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
