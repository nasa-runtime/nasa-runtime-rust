# 共享传输接收预算

[English](receive-budget.en.md)

`transport::Connection::accept_with_budget` 接受可选 `Arc<transport::budget::Budget>`。多个 listener 和连接可以共享同一个容量。既有 `Connection::accept`、`Reader::receive` 和 `Inbound` 保持原有无共享预算的使用方式；显式启用预算的连接必须使用 `Reader::receive_bytes`，不能通过旧方法取得不带字节责任的文本。

`Received` 的二进制、UTF-8 文本、Ping 和 Pong 均使用 `Bytes`。TCP 在确认长度后、分配之前预约完整帧容量。WebSocket 在读取前覆盖标准后端缓冲、分片组装及临时复制；导出的消息独立按原 Vec 容量预约。`Bytes` 的 clone 和 slice 保留原 owner，最后一个视图释放后才归还容量。

WebSocket 后端会保留扩展过的缓冲，预算因此按连接高水位保留：固定 16 KiB，加上当前线长窗口的八倍保守容量。此计量不代表 RSS，不包含内核、分配器、线程和出站缓冲；它不能代替进程内存限制。空闲连接不预约最大业务消息，但已经达到的高水位不因消息被消费而清零。

握手线长限制为 64 KiB；后续窗口限制为 `2 × maximum_message + 64 KiB`。每次完整文本或二进制消息结束后开启下一窗口，控制帧不刷新该窗口的组装起点。连续控制流也受此有限窗口约束。单次底层读取至多 4096 字节，保守预约可以比实际读取更多。

容量不足立即拒绝，不等待其它连接归还后继续半条消息。超时、取消、非法输入和预算错误后调用者必须销毁连接。`Budget::used_bytes` 提供当前容量事实；`Budget::retain` 可让其它拥有型缓冲沿用同一共享预算，调用者必须传入实际保留容量并避免对已有共享分配重复计费。
