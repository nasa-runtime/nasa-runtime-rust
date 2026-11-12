//! 当前进程和显式白名单资源的有界周期采集；抓取只读取缓存。

mod catalog;
mod config;
#[cfg(target_os = "linux")]
mod linux;
mod sample;
mod source;

pub use config::CollectorsConfig;
pub use source::ResourceCollector;
