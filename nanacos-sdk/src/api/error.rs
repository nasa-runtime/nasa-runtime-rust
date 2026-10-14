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

/// Nacos Sdk Rust Result.
pub type Result<T> = std::result::Result<T, Error>;

/// Nacos Sdk Rust Error.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// JSON 编解码失败。
    #[error("Serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),

    /// 未能取得所需业务结果，携带失败说明。
    #[error("get result failed: {0}")]
    ErrResult(String),

    /// 请求参数不合法，携带参数名及原因。
    #[error("param:{0} with error_message:{1}")]
    InvalidParam(String, String),

    /// 服务端拒绝请求，携带请求标识、返回码、错误码和可选消息。
    #[error("request_id:{0:?} ret_code:{1} error_code:{2} message:{3:?}")]
    ErrResponse(Option<String>, i32, i32, Option<String>),

    /// Config not found.
    #[cfg(feature = "config")]
    #[error("config not found: {0}")]
    ConfigNotFound(String),

    /// Config query conflict, it is being modified, please try later.
    #[cfg(feature = "config")]
    #[error("config query conflict: {0}")]
    ConfigQueryConflict(String),

    /// 请求所依赖的远端客户端已经关闭。
    #[error("remote client shutdown failed: {0}")]
    ClientShutdown(String),

    /// 远端客户端当前不健康。
    #[error("remote client unhealthy failed: {0}")]
    ClientUnhealthy(String),

    /// 底层 gRPC 连接或传输失败。
    #[error("tonic grpc transport error: {0}")]
    TonicGrpcTransport(#[from] tonic::transport::Error),

    /// 服务端返回 gRPC 失败状态。
    #[error("tonic grpc status error: {0}")]
    TonicGrpcStatus(#[from] tonic::Status),

    /// gRPC 请求缓冲或调度失败。
    #[error("grpc request error: {0}")]
    GrpcBufferRequest(#[from] tower::BoxError),

    /// 当前没有可尝试连接的 Nacos 服务端。
    #[error("no available server")]
    NoAvailableServer,

    /// 服务端地址格式不受支持。
    #[error("Wrong server address: {0}")]
    WrongServerAddress(String),

    /// 重试次数达到上限；保留该变体供现有调用方识别。
    #[deprecated]
    #[allow(unused)]
    #[error("Exceeded maximum retry attempts: {0}")]
    MaxRetriesExceeded(u32),
}
