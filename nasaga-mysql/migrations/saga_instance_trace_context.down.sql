-- 业务作用：回退实例因果上下文列。trace 是观测面数据，删除不影响状态机、去重与投递
-- 正确性；回退后跨服务链路在 Saga 段重新断开。

ALTER TABLE saga_instance
    DROP COLUMN traceparent,
    ALGORITHM=INPLACE,
    LOCK=NONE;
