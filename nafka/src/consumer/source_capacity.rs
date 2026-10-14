//! 普通消费输入的源与进程边界，凭证实际退出前不归还额度。

use crate::{
    config::BehaviorConfig,
    error::{NafkaError, Result},
};
use std::sync::{Arc, Mutex, OnceLock};

static PROCESS: OnceLock<Arc<ProcessCapacity>> = OnceLock::new();

struct ProcessCapacity {
    max_records: usize,
    max_bytes: usize,
    usage: Mutex<(usize, usize)>,
}

/// 同一源全部普通消费组共享的原文与框架副本预留，不包含 native 队列与解码对象堆大小。
pub(crate) struct SourceCapacity {
    process: Arc<ProcessCapacity>,
    max_records: usize,
    max_bytes: usize,
    usage: Mutex<(usize, usize)>,
}

impl SourceCapacity {
    /// 业务作用：在创建原生资源前冻结源和进程边界，所有源必须声明相同进程上限。
    /// 参数说明：config 为已验证的行为配置。
    /// 返回：零占用源池；声明冲突返回配置错误，进程边界只能随进程重启改变。
    pub(crate) fn new(config: &BehaviorConfig) -> Result<Arc<Self>> {
        let process = PROCESS.get_or_init(|| {
            Arc::new(ProcessCapacity {
                max_records: config.max_process_pending_records,
                max_bytes: config.max_process_pending_bytes,
                usage: Mutex::new((0, 0)),
            })
        });
        // 独立源不能通过不同声明绕开全进程边界；此检查必须先于线程与连接创建。
        if process.max_records != config.max_process_pending_records
            || process.max_bytes != config.max_process_pending_bytes
        {
            return Err(NafkaError::Config("同一进程的消费输入上限必须一致".into()));
        }
        Ok(Arc::new(Self {
            process: Arc::clone(process),
            max_records: config.max_source_pending_records,
            max_bytes: config.max_source_pending_bytes,
            usage: Mutex::new((0, 0)),
        }))
    }

    /// 业务作用：同时保留源和进程责任，拒绝时不产生半份计费。
    /// 参数说明：bytes 为原文及框架副本的保守预留。
    /// 返回：必须保留到实际责任退出的凭证；任一边界耗尽时为 None。
    pub(crate) fn reserve(self: &Arc<Self>, bytes: usize) -> Option<SourcePermit> {
        // 全路径固定为组、源、进程的锁顺序，归还时不得反向取得其它组的锁。
        let mut source = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut process = self
            .process
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.available(*source, *process, bytes) {
            return None;
        }
        source.0 += 1;
        source.1 += bytes;
        process.0 += 1;
        process.1 += bytes;
        Some(SourcePermit {
            source: Arc::clone(self),
            bytes,
        })
    }

    /// 业务作用：依据共同快照判断一次取入是否满足两层数量及字节边界。
    /// 参数说明：source/process 为锁内占用，bytes 为待取入原文及框架副本的保守预留。
    /// 返回：两个域都足够才允许继续。
    fn available(&self, source: (usize, usize), process: (usize, usize), bytes: usize) -> bool {
        source.0 < self.max_records
            && bytes <= self.max_bytes - source.1
            && process.0 < self.process.max_records
            && bytes <= self.process.max_bytes - process.1
    }

    /// 业务作用：恢复拉取前观察两层余量，避免无容量时重复拉取和定位。
    /// 参数说明：bytes 为等待输入大小。
    /// 返回：当前可重试的提示，取入时仍须原子裁决。
    pub(crate) fn can_reserve(&self, bytes: usize) -> bool {
        let source = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let process = self
            .process
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.available(*source, *process, bytes)
    }

    /// 业务作用：识别空池也无法接纳的原文，避免永久等待容量。
    /// 参数说明：无。
    /// 返回：源与进程的更小字节边界。
    pub(crate) fn maximum_record_bytes(&self) -> usize {
        self.max_bytes.min(self.process.max_bytes)
    }
}

/// 输入、业务与提交责任共用的源级凭证，由最终记录引用持有。
pub(crate) struct SourcePermit {
    source: Arc<SourceCapacity>,
    bytes: usize,
}

impl Drop for SourcePermit {
    /// 业务作用：实际责任退出时同时归还源和进程额度。
    /// 参数说明：无。
    /// 返回：减少计费，不表达业务确认或提交成功。
    fn drop(&mut self) {
        let mut source = self
            .source
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut process = self
            .source
            .process
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        source.0 -= 1;
        source.1 -= self.bytes;
        process.0 -= 1;
        process.1 -= self.bytes;
    }
}
