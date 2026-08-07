-- 业务作用：让 Saga 身份、业务幂等键和 trigger 唯一键按字节比较，避免默认排序规则
-- 把大小写不同的 tenant/workflow/business key/operation 错误合并。
-- 字符集转换可能重建大表；生产应先以 online-schema-change 工具演练并按表分批执行。

-- 本文件是停写维护窗的直接 migration；真实 Saga 索引形态在 MySQL 8.4 不支持原生
-- ALGORITHM=INPLACE。在线发布必须由 tests/ci/saga-online-ddl-live 使用 OSC 工具执行同一
-- ALTER 语义，禁止在有业务写入时直接运行本文件。
ALTER TABLE saga_instance CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
ALTER TABLE saga_step CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
ALTER TABLE saga_step_attempt CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
ALTER TABLE saga_transition CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
ALTER TABLE saga_control_transition CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
ALTER TABLE saga_timer CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
