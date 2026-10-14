//! 配置管理的独立结果队列；只接受目标资源自身的确认。

use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::time::{Duration, Instant};

use rdkafka::admin::AdminClient;
use rdkafka::bindings as native;
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;

use crate::error::{NafkaError, Result};

/// 一次请求的本地原生内存，全部指针仅在同一阻塞任务内使用。
struct Request {
    /// 此操作独占的结果队列，不消费共享 Admin 的其它响应。
    queue: *mut native::rd_kafka_queue_t,
    /// 请求预算与操作类型，在提交后仍持有到本地等待结束。
    options: *mut native::rd_kafka_AdminOptions_t,
    /// 请求的唯一 topic 资源；修改请求同时持有其增量项。
    resource: *mut native::rd_kafka_ConfigResource_t,
    /// 拥有响应资源借用内存，读取结束前不能释放。
    event: *mut native::rd_kafka_event_t,
}

impl Drop for Request {
    /// 业务作用：在确认、拒绝或等待结束后归还请求与响应内存，不销毁共享 Admin 客户端。
    /// 参数说明：无。返回：逐项销毁已构造对象；销毁结果队列不撤销已交给 broker 的修改。
    fn drop(&mut self) {
        // 响应资源借用 event 内存；全部读取结束后先释放 event，再释放请求对象和队列。
        unsafe {
            if !self.event.is_null() {
                native::rd_kafka_event_destroy(self.event);
            }
            if !self.resource.is_null() {
                native::rd_kafka_ConfigResource_destroy(self.resource);
            }
            if !self.options.is_null() {
                native::rd_kafka_AdminOptions_destroy(self.options);
            }
            if !self.queue.is_null() {
                native::rd_kafka_queue_destroy(self.queue);
            }
        }
    }
}

/// 业务作用：读取目标 topic 的原生逐资源配置结果，保留资源级授权拒绝。
/// 参数说明：client 为持有到查询结束的 Admin；topic 为资源名；timeout 为总等待预算；request_timeout 为请求预算。
/// 返回：仅资源身份、错误码和配置项全部有效时返回配置；拒绝、空结果或超时不能作为可用的保留策略证据。
pub(crate) fn describe_topic_configs(
    client: &AdminClient<DefaultClientContext>,
    topic: &str,
    timeout: Duration,
    request_timeout: Duration,
) -> Result<BTreeMap<String, Option<String>>> {
    let deadline = Instant::now() + timeout;
    let ctopic = CString::new(topic).map_err(|_| NafkaError::Config("topic 含 NUL".into()))?;
    let pointer = client.inner().native_ptr();
    let mut request = Request {
        queue: unsafe { native::rd_kafka_queue_new(pointer) },
        options: unsafe {
            native::rd_kafka_AdminOptions_new(
                pointer,
                native::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS,
            )
        },
        resource: unsafe {
            native::rd_kafka_ConfigResource_new(
                native::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC,
                ctopic.as_ptr(),
            )
        },
        event: std::ptr::null_mut(),
    };
    if request.queue.is_null() || request.options.is_null() || request.resource.is_null() {
        return Err(NafkaError::Broker("配置查询无法分配原生资源".into()));
    }
    let mut error_buffer = [0 as std::ffi::c_char; 512];
    query_code(unsafe {
        native::rd_kafka_AdminOptions_set_request_timeout(
            request.options,
            request_timeout.as_millis().min(i32::MAX as u128) as i32,
            error_buffer.as_mut_ptr(),
            error_buffer.len(),
        )
    })?;
    unsafe {
        native::rd_kafka_DescribeConfigs(
            pointer,
            &mut request.resource,
            1,
            request.options,
            request.queue,
        );
    }
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(NafkaError::Broker("配置查询等待超时".into()));
        }
        request.event = unsafe {
            native::rd_kafka_queue_poll(
                request.queue,
                remaining.as_millis().clamp(1, i32::MAX as u128) as i32,
            )
        };
        if !request.event.is_null() {
            break;
        }
    }
    query_code(unsafe { native::rd_kafka_event_error(request.event) })?;
    let result = unsafe { native::rd_kafka_event_DescribeConfigs_result(request.event) };
    if result.is_null() {
        return Err(NafkaError::Broker("配置查询响应类型不匹配".into()));
    }
    let mut count = 0;
    let resources =
        unsafe { native::rd_kafka_DescribeConfigs_result_resources(result, &mut count) };
    if count != 1 || resources.is_null() {
        return Err(NafkaError::Broker("配置查询缺少唯一资源结果".into()));
    }
    let resource = unsafe { *resources };
    if resource.is_null() {
        return Err(NafkaError::Broker("配置查询资源结果为空".into()));
    }
    let name = unsafe { native::rd_kafka_ConfigResource_name(resource) };
    if name.is_null()
        || unsafe { CStr::from_ptr(name) } != ctopic.as_c_str()
        || unsafe { native::rd_kafka_ConfigResource_type(resource) }
            != native::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC
    {
        return Err(NafkaError::Broker("配置查询资源身份不匹配".into()));
    }
    // 请求级成功不能覆盖资源级拒绝；先读取原生资源错误，才能把配置用于 DLT 保留门禁。
    query_code(unsafe { native::rd_kafka_ConfigResource_error(resource) })?;
    let mut count = 0;
    let entries = unsafe { native::rd_kafka_ConfigResource_configs(resource, &mut count) };
    if count == 0 || entries.is_null() {
        return Err(NafkaError::Broker("配置查询返回空配置集".into()));
    }
    let mut values = BTreeMap::new();
    for index in 0..count {
        let entry = unsafe { *entries.add(index) };
        if entry.is_null() {
            return Err(NafkaError::Broker("配置查询条目为空".into()));
        }
        let name = unsafe { native::rd_kafka_ConfigEntry_name(entry) };
        if name.is_null() {
            return Err(NafkaError::Broker("配置查询条目缺少名称".into()));
        }
        let name = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|_| NafkaError::Broker("配置名称不是 UTF-8".into()))?
            .to_owned();
        let value = unsafe { native::rd_kafka_ConfigEntry_value(entry) };
        let value = if value.is_null() {
            None
        } else {
            Some(
                unsafe { CStr::from_ptr(value) }
                    .to_str()
                    .map_err(|_| NafkaError::Broker("配置值不是 UTF-8".into()))?
                    .to_owned(),
            )
        };
        if values.insert(name, value).is_some() {
            return Err(NafkaError::Broker("配置查询包含重复名称".into()));
        }
    }
    Ok(values)
}

/// 业务作用：把只读配置查询的原生错误转换为稳定分类，不输出 broker 自由文本。
/// 参数说明：code 为请求或资源的错误码。返回：NoError 成功；权限拒绝单独报告，其他查询失败保留固定码。
fn query_code(code: native::rd_kafka_resp_err_t) -> Result<()> {
    let code: RDKafkaErrorCode = code.into();
    if code == RDKafkaErrorCode::NoError {
        Ok(())
    } else if super::errors::authorization(code) {
        Err(NafkaError::Authorization)
    } else {
        Err(NafkaError::Broker(format!("配置查询失败: {code:?}")))
    }
}

/// 业务作用：增量设置或恢复目标 topic 的指定配置，并以明确的逐资源成功响应作为完成条件。
/// 参数说明：client 为共享 Admin，topic 为目标，changes 为设置值或恢复默认的变更，timeout 为总等待预算，request_timeout 为原生请求预算。
/// 返回：目标资源确认才成功；无权限、缺失/错配结果及超时均拒绝，不退回整组替换配置。
pub(crate) fn set_topic_configs(
    client: &AdminClient<DefaultClientContext>,
    topic: &str,
    changes: &BTreeMap<String, Option<String>>,
    timeout: Duration,
    request_timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let ctopic = CString::new(topic).map_err(|_| NafkaError::Config("topic 含 NUL".into()))?;
    // Admin 的 Arc 由外层阻塞任务持有；所有派生指针均在该持有期间创建、使用和释放。
    let pointer = client.inner().native_ptr();
    let mut request = Request {
        queue: unsafe { native::rd_kafka_queue_new(pointer) },
        options: unsafe {
            native::rd_kafka_AdminOptions_new(
                pointer,
                native::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS,
            )
        },
        resource: unsafe {
            native::rd_kafka_ConfigResource_new(
                native::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC,
                ctopic.as_ptr(),
            )
        },
        event: std::ptr::null_mut(),
    };
    if request.queue.is_null() || request.options.is_null() || request.resource.is_null() {
        return Err(NafkaError::Broker("增量管理请求无法分配原生资源".into()));
    }
    let mut error_buffer = [0 as std::ffi::c_char; 512];
    let code = unsafe {
        native::rd_kafka_AdminOptions_set_request_timeout(
            request.options,
            request_timeout.as_millis().min(i32::MAX as u128) as i32,
            error_buffer.as_mut_ptr(),
            error_buffer.len(),
        )
    };
    check(code)?;
    for (key, value) in changes {
        let ckey =
            CString::new(key.as_str()).map_err(|_| NafkaError::Config("配置键含 NUL".into()))?;
        let cvalue = value
            .as_ref()
            .map(|value| CString::new(value.as_str()))
            .transpose()
            .map_err(|_| NafkaError::Config("配置值含 NUL".into()))?;
        let error = unsafe {
            native::rd_kafka_ConfigResource_add_incremental_config(
                request.resource,
                ckey.as_ptr(),
                if cvalue.is_some() {
                    native::rd_kafka_AlterConfigOpType_t::RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET
                } else {
                    native::rd_kafka_AlterConfigOpType_t::RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE
                },
                cvalue
                    .as_ref()
                    .map_or(std::ptr::null(), |value| value.as_ptr()),
            )
        };
        if !error.is_null() {
            let code = unsafe { native::rd_kafka_error_code(error) };
            unsafe {
                native::rd_kafka_error_destroy(error);
            }
            check(code)?;
        }
    }
    // 仅提交明确的 SET/DELETE 增量；不能读取后整组写回，否则会覆盖并发管理者或未声明的配置。
    unsafe {
        native::rd_kafka_IncrementalAlterConfigs(
            pointer,
            &mut request.resource,
            1,
            request.options,
            request.queue,
        );
    }
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(NafkaError::AdminOutcomeUnknown {
                operation: "incremental-alter-configs",
            });
        }
        request.event = unsafe {
            native::rd_kafka_queue_poll(
                request.queue,
                remaining.as_millis().clamp(1, i32::MAX as u128) as i32,
            )
        };
        if !request.event.is_null() {
            break;
        }
    }
    check(unsafe { native::rd_kafka_event_error(request.event) })?;
    let result = unsafe { native::rd_kafka_event_IncrementalAlterConfigs_result(request.event) };
    if result.is_null() {
        return Err(NafkaError::AdminOutcomeUnknown {
            operation: "incremental-alter-configs",
        });
    }
    let mut count = 0;
    let resources =
        unsafe { native::rd_kafka_IncrementalAlterConfigs_result_resources(result, &mut count) };
    // 请求级成功不代表资源成功；必须取得且只取得目标 topic 的一项结果。
    if count != 1 || resources.is_null() {
        return Err(NafkaError::AdminOutcomeUnknown {
            operation: "incremental-alter-configs",
        });
    }
    let resource = unsafe { *resources };
    if resource.is_null() {
        return Err(NafkaError::AdminOutcomeUnknown {
            operation: "incremental-alter-configs",
        });
    }
    let name = unsafe { native::rd_kafka_ConfigResource_name(resource) };
    if name.is_null()
        || unsafe { CStr::from_ptr(name) } != ctopic.as_c_str()
        || unsafe { native::rd_kafka_ConfigResource_type(resource) }
            != native::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC
    {
        // 请求已经发出，错配响应不能证明本次修改未发生，必须让调用方回读真实资源。
        return Err(NafkaError::AdminOutcomeUnknown {
            operation: "incremental-alter-configs",
        });
    }
    check(unsafe { native::rd_kafka_ConfigResource_error(resource) })
}

/// 业务作用：将原生确认码转换为不含配置正文或 broker 自由文本的公共失败。
/// 参数说明：code 为请求或资源的原生错误码。返回：仅 NoError 成功；协议能力缺失单独报告，其他拒绝保留固定码。
fn check(code: native::rd_kafka_resp_err_t) -> Result<()> {
    let code: RDKafkaErrorCode = code.into();
    match code {
        RDKafkaErrorCode::NoError => Ok(()),
        code if super::errors::authorization(code) => Err(NafkaError::Authorization),
        RDKafkaErrorCode::OperationTimedOut
        | RDKafkaErrorCode::BadMessage
        | RDKafkaErrorCode::RequestTimedOut
        | RDKafkaErrorCode::TimedOutQueue
        | RDKafkaErrorCode::BrokerTransportFailure
        | RDKafkaErrorCode::NetworkException
        | RDKafkaErrorCode::AllBrokersDown => Err(NafkaError::AdminOutcomeUnknown {
            operation: "incremental-alter-configs",
        }),
        RDKafkaErrorCode::UnsupportedFeature | RDKafkaErrorCode::UnsupportedVersion => Err(
            NafkaError::UnsupportedCapability("incremental-alter-configs"),
        ),
        _ => Err(NafkaError::Broker(format!("增量配置修改失败: {code:?}"))),
    }
}
