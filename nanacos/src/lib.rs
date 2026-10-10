//! Nacos 配置中心与注册中心适配层。
//! 公共 owner 用 qualifier 选择独立连接，primary 为默认名称；显式注册清单控制发布范围。
//!
//! 本 crate 只处理连接、鉴权、配置文本读取/监听和实例注册发现，不绑定具体业务配置结构。
//! `managed` feature 提供独立 Nacos 3 HTTP adapter；发现使用 client API，注册核对服务范围的 admin 详情及完整实例列表。
//! HTTP 同服务端点的写入身份不因 cluster 改变；首次注册拒绝覆盖其他 cluster 的既有记录，发现仍保留 cluster 筛选。
//! `nadisc-runtime` 管理其具名连接、心跳与关闭；与 `nacos` feature 的 SDK 配置和兼容命名入口分别持有连接及认证状态。
// ============================================================================
// nacos —— Nacos 配置中心 + 注册中心的【传输/机制】层(抽自各业务 app,机制写一次复用)
//
// 设计边界：传输机制与应用策略保持分离。
//   进本 crate(机制,与配置类型无关):
//     · 连接/鉴权/命名空间:NacosConfigClient::connect(配置)/ NacosDiscoveryClient::connect(注册),各建各的 SDK service
//     · 配置:fetch(data_id, group) -> 裸文本;watch(...) -> 推送回调拿裸文本 + WatchGuard(drop 注销)
//     · 注册:register(...) -> RegistrationGuard(drop best-effort deregister,防僵尸实例;优雅停机应显式 deregister().await);
//       discover/discover_all;list_services;subscribe_channel/subscribe
//   留在各 app(策略,app 专属):
//     · typed AppConfig 结构、配置分层/优先级合并、ArcSwap apply、两阶段启动【时序编排】
//   核心原则:本 crate **只吐裸 yaml 字符串 / Instance 列表,绝不认识 AppConfig**;
//             app 拿到裸文本后自己 parse+merge+apply(与 scheduling 同原则:基础给机制、app 给策略)。
//
// feature 门控 `--features nacos`:
//   关 → 不编 nacos-sdk;所有入口 bail 提示重编译(避免"以为开了 Nacos 实际没生效")。
//   开 → 走真实 nacos-sdk(默认含 config/naming/auth-by-http)。
//
// 保活:NacosConfigClient / NacosDiscoveryClient / WatchGuard / RegistrationGuard / SubscribeGuard 都要被 main 持有到进程结束;
//   SDK client 释放引用不保证其内部任务已经结束；watch guard→best-effort 注销监听;registration guard→best-effort deregister;
//   subscribe guard→best-effort 取消订阅。优雅停机推荐对 RegistrationGuard 显式 `deregister().await` 再停服务。
// ============================================================================

mod client;
mod config;
#[cfg(feature = "managed")]
pub mod managed;
mod naming;

// 服务实例中性类型来自 discovery crate(provider-neutral);此处 re-export 方便直接依赖 nacos 的用户。
pub use client::NacosProps;
pub use nadisc::Instance;
// 配置中心与注册中心【各用独立 client】(只共享 NacosProps):只用一边的服务不会被迫初始化另一边。
// 多配置门面(fetch_many / watch_many_channel)的类型随配置 client 一起导出。
pub use config::{
    ConfigBundle, ConfigDocument, ConfigRef, MultiWatchGuard, NacosConfigClient, WatchGuard,
};
pub use naming::{NacosDiscoveryClient, RegistrationGuard, SubscribeGuard, SubscribeOptions};
