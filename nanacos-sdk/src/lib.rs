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

    pub(crate) fn get_value_option<Key>(key: Key) -> Option<String>
    where
        Key: AsRef<str>,
    {
        PROPERTIES.get(key.as_ref()).cloned()
    }

    pub(crate) fn get_value<Key, Default>(key: Key, default: Default) -> String
    where
        Key: AsRef<str>,
        Default: AsRef<str>,
    {
        PROPERTIES
            .get(key.as_ref())
            .map_or(default.as_ref().to_string(), |value| value.to_string())
    }

    pub(crate) fn get_value_u16<Key>(key: Key, default: u16) -> u16
    where
        Key: AsRef<str>,
    {
        PROPERTIES.get(key.as_ref()).map_or(default, |value| {
            value.to_string().parse::<u16>().unwrap_or(default)
        })
    }

    pub(crate) fn get_value_bool<Key>(key: Key, default: bool) -> bool
    where
        Key: AsRef<str>,
    {
        PROPERTIES.get(key.as_ref()).map_or(default, |value| {
            value.to_string().parse::<bool>().unwrap_or(default)
        })
    }
}
