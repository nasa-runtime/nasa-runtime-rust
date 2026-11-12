// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#![deny(unsafe_code)]
#![deny(rust_2018_idioms, clippy::disallowed_methods, clippy::disallowed_types)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

//! Nacos 配置和注册兼容传输，公开 API 与 feature 保留原 SDK 合同。
//!
//! 双向控制流失效后撤销连接健康，由唯一连接状态机完成退避重连。
//! 本组件派生自 nacos-sdk 0.8.3，来源与 Apache-2.0 许可见随包提供的 NOTICE 和 LICENSE。
//! 业务优先经 nanacos 使用此传输；新多注册中心生命周期由独立 HTTP adapter 管理。

/// Nacos API
pub mod api;

mod common;
#[cfg(feature = "config")]
mod config;
#[cfg(feature = "naming")]
mod naming;

#[allow(dead_code)]
#[path = ""]
mod nacos_proto {
    #[path = "_.rs"]
    pub mod v2;
}

use crate::api::constants::ENV_NACOS_CLIENT_PROPS_FILE_PATH;
use std::collections::HashMap;

static PROPERTIES: std::sync::LazyLock<HashMap<String, String>> = std::sync::LazyLock::new(|| {
    let env_file_path = std::env::var(ENV_NACOS_CLIENT_PROPS_FILE_PATH).ok();
    let _ = env_file_path.as_ref().map(|file_path| {
        dotenvy::from_path(std::path::Path::new(file_path)).inspect_err(|_e| {
            let _ = dotenvy::dotenv();
        })
    });
    dotenvy::dotenv().ok();

    dotenvy::vars().collect::<HashMap<String, String>>()
});

pub(crate) mod properties {
    use crate::PROPERTIES;

    /// 业务作用：从进程首次读取时冻结的属性快照中获取可选配置值。
    ///
    /// 参数说明：
    /// - `key`：按原文精确匹配的配置键。
    ///
    /// 返回：存在时复制原值，否则为空；首次调用触发 dotenv 加载和环境快照，后续环境修改不会重新加载。
    pub(crate) fn get_value_option<Key>(key: Key) -> Option<String>
    where
        Key: AsRef<str>,
    {
        PROPERTIES.get(key.as_ref()).cloned()
    }

    /// 业务作用：读取固定属性快照中的文本配置，缺失时使用调用方的缺省值。
    ///
    /// 参数说明：
    /// - `key`：待查找配置键。
    /// - `default`：键不存在时采用的文本，空配置值不会触发此回退。
    ///
    /// 返回：原值或缺省值的独立字符串，不解析内容或动态重载环境。
    pub(crate) fn get_value<Key, Default>(key: Key, default: Default) -> String
    where
        Key: AsRef<str>,
        Default: AsRef<str>,
    {
        PROPERTIES
            .get(key.as_ref())
            .map_or(default.as_ref().to_string(), |value| value.to_string())
    }

    /// 业务作用：读取可表示为 u16 的属性，为端口等有限范围配置提供缺省回退。
    ///
    /// 参数说明：
    /// - `key`：固定属性快照中的配置键。
    /// - `default`：键缺失或原文不能解析为 u16 时采用的值。
    ///
    /// 返回：解析值或缺省值；不清除空白、不拒绝零值，也不报告无效配置错误。
    pub(crate) fn get_value_u16<Key>(key: Key, default: u16) -> u16
    where
        Key: AsRef<str>,
    {
        PROPERTIES.get(key.as_ref()).map_or(default, |value| {
            value.to_string().parse::<u16>().unwrap_or(default)
        })
    }

    /// 业务作用：读取布尔开关，无法解释的配置沿用调用方给定策略。
    ///
    /// 参数说明：
    /// - `key`：固定属性快照中的开关键。
    /// - `default`：键缺失或文本不是 Rust 布尔格式时采用的值。
    ///
    /// 返回：精确的 true 或 false 文本对应值，否则为缺省值；不进行大小写或空白归一，也不动态重载。
    pub(crate) fn get_value_bool<Key>(key: Key, default: bool) -> bool
    where
        Key: AsRef<str>,
    {
        PROPERTIES.get(key.as_ref()).map_or(default, |value| {
            value.to_string().parse::<bool>().unwrap_or(default)
        })
    }
}
