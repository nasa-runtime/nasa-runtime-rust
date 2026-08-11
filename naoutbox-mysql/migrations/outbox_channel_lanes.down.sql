-- 业务作用：回退通道分片支撑。回退前必须先停止全部按 lane dispatcher 并恢复单一
-- 未分片 dispatcher，否则删除列会让按 lane 领取即刻失败。

ALTER TABLE outbox_event
    DROP INDEX idx_channel_dispatchable,
    ALGORITHM=INPLACE,
    LOCK=NONE;

ALTER TABLE outbox_event
    DROP COLUMN channel,
    ALGORITHM=INPLACE,
    LOCK=NONE;
