-- 业务作用：为 saga_instance 增加 canonical W3C traceparent 列，持久化实例的最新因果
-- 上下文。创建入口显式传入的 trace 在此落库；此后每次已认证 result 推进在同事务更新
-- 该列，timer 与崩溃恢复发出的命令由已提交实例读取同一 trace，跨进程重启保持链路连续。
-- 列可空：缺少上下文的实例保持 NULL 并正常投递，trace 是观测面而非投递前置条件。
-- 旧版本二进制按显式列名 SELECT，加列对其读路径向后兼容；本迁移须先于新版本部署执行。

ALTER TABLE saga_instance
    ADD COLUMN traceparent VARCHAR(55) NULL AFTER failure_code,
    ALGORITHM=INPLACE,
    LOCK=NONE;
