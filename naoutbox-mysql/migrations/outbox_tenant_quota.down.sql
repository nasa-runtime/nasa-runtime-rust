-- 回退仅移除配额账本;在飞计数是可再生的运行时事实,删除不影响事件数据。
-- 执行前必须确认没有任何部署仍安装 outbox 租户配额。

DROP TABLE outbox_tenant_quota;
