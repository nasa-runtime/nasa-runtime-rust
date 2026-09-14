-- 删除统一事件流会失去全局断点位置；执行前必须导出审计事件并停止所有 Saga 写入方。
DROP TRIGGER IF EXISTS nasaga_audit_attempt_insert;
DROP TRIGGER IF EXISTS nasaga_audit_attempt_update;
DROP TRIGGER IF EXISTS nasaga_audit_transition_insert;
DROP TRIGGER IF EXISTS nasaga_audit_control_insert;
DROP TRIGGER IF EXISTS nasaga_audit_management_insert;
DROP TRIGGER IF EXISTS nasaga_audit_conflict_insert;
DROP TABLE saga_audit_event;
DROP TABLE saga_audit_stream_guard;
