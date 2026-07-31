use std::{future::Future, pin::Pin};

use crate::ApplicationResult;

/// 组件、资源和 action 使用的 object-safe 异步结果。
pub type ApplicationFuture<'a, T = ()> =
    Pin<Box<dyn Future<Output = ApplicationResult<T>> + Send + 'a>>;
