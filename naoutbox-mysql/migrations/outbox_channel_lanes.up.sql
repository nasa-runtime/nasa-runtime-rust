-- 业务作用：为 outbox_event 增加通道列与按通道领取索引，支撑多通道分片投递。
-- 历史行由列默认值一次性归入 'global' 默认 lane；该归属规则一经发布不可更改，
-- 否则同一聚合根的历史事件会被拆进两个 lane、破坏单聚合根内的事件顺序。
-- 本迁移不改变任何投递行为：未安装通道路由、未启动按 lane dispatcher 前，
-- 单一未分片 dispatcher 继续服务全表，可独立上线并回退。

ALTER TABLE outbox_event
    ADD COLUMN channel VARCHAR(64) NOT NULL DEFAULT 'global' AFTER traceparent,
    ALGORITHM=INPLACE,
    LOCK=NONE;

ALTER TABLE outbox_event
    ADD INDEX idx_channel_dispatchable (channel, dispatched, dead, id),
    ALGORITHM=INPLACE,
    LOCK=NONE;
