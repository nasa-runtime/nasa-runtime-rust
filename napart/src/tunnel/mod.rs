//! 盗洞物理队列与租约。
//!
//! 盗洞只在同一 Runner generation 内连接一个源 slot 与一个目标 slot。非严格类型允许
//! 多目标并存；严格类型始终只有一个目标，并把存量、增量和归还暂存分成独立 FIFO。

mod non_strict;
mod strict;

pub(crate) use non_strict::NonStrictTunnel;
pub(crate) use strict::{StrictQueue, StrictTunnel};
