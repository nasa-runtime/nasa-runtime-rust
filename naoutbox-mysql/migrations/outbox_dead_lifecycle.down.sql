-- 回退移除死信时刻列与处置事实表;处置事实是人工清理的复验证据,执行前必须导出留档,
-- 且确认没有任何部署仍开启死信清理。

ALTER TABLE outbox_event
    DROP INDEX idx_retention_dispatched,
    DROP INDEX idx_retention_dead,
    DROP COLUMN dead_at;
DROP TABLE outbox_dead_disposal;
