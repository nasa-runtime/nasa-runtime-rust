//! 同步回调独占的输入确认与有界输出暂存，不暴露原生 producer。

use super::{TransactionError as Error, TransactionPlan, TransactionResult as Result};
use crate::{dead_letter_headers, AckMode, KafkaHeaders, ReplayRecord, Tp};
use std::{collections::BTreeSet, marker::PhantomData, rc::Rc, sync::Arc};

/// 只会通过当前事务 producer 发送的独立输出。
#[derive(Clone, Debug)]
pub struct TransactionOutput {
    /// 已批准的目标 topic 与固定分区。
    pub destination: Tp,
    /// 原 Kafka 毫秒时间，None 交给 producer 生成。
    pub timestamp: Option<i64>,
    /// 独立原始 key，None 与空值不同。
    pub key: Option<Vec<u8>>,
    /// 独立正文，None 是 tombstone。
    pub payload: Option<Vec<u8>>,
    /// 保留顺序、重复项与 null 值的头。
    pub headers: KafkaHeaders,
}

/// 一个同步业务回调；数据库和普通 lane 发送不自动成为 Kafka 原子事务。
pub trait TransactionProcessor: Send + Sync + 'static {
    /// 业务作用：确认本批全部输入并暂存同一事务的输出。
    /// 参数说明：context 为不能跨线程移动的本批上下文。返回：正常返回才允许准备提交；错误或缺少 Manual 确认使整批中止，可能重调。
    fn process(&self, context: &mut TransactionContext) -> Result<()>;
}

impl<F> TransactionProcessor for F
where
    F: Fn(&mut TransactionContext) -> Result<()> + Send + Sync + 'static,
{
    /// 业务作用：把同步业务闭包接入相同的整批确认合同。
    /// 参数说明：context 为本批独占上下文。返回：保留闭包成功或失败，不提前发布交付结果。
    fn process(&self, context: &mut TransactionContext) -> Result<()> {
        self(context)
    }
}

/// 当前回调独占的事务责任；既不提供原生事务控制，也不允许异步线程持有写权限。
pub struct TransactionContext {
    plan: Arc<TransactionPlan>,
    records: Vec<ReplayRecord>,
    authority: Box<dyn Fn() -> Result<()>>,
    outputs: Vec<TransactionOutput>,
    acknowledged: BTreeSet<usize>,
    dlt_topics: BTreeSet<String>,
    attempt: u32,
    bytes: u64,
    closed: bool,
    rejection: Option<Error>,
    _thread_bound: PhantomData<Rc<()>>,
}

impl TransactionContext {
    /// 业务作用：把全部输入、输出与 owner 的实时权威检查绑定到唯一同步回调。
    /// 参数说明：plan 为冻结合同，records 为独立输入，authority 为当前代次检查，attempt 为本实例中止后的重试序号。返回：仅当前线程可操作的上下文。
    pub(crate) fn new(
        plan: Arc<TransactionPlan>,
        records: Vec<ReplayRecord>,
        authority: Box<dyn Fn() -> Result<()>>,
        attempt: u32,
    ) -> Self {
        Self {
            plan,
            records,
            authority,
            outputs: Vec::new(),
            acknowledged: BTreeSet::new(),
            dlt_topics: BTreeSet::new(),
            attempt,
            bytes: 0,
            closed: false,
            rejection: None,
            _thread_bound: PhantomData,
        }
    }

    /// 业务作用：只读访问本批必须整体处理的原文和来源坐标。
    /// 参数说明：无。返回：独立输入的只读借用，不能用外部修改改变提交责任。
    pub fn records(&self) -> &[ReplayRecord] {
        &self.records
    }

    /// 业务作用：识别同一执行实例中已确认 abort 后的重复处理。
    /// 参数说明：无。返回：从1开始的投递次数，重启不提供持久重试计数。
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// 业务作用：确认一条本批输入，不能单独推进组位点。
    /// 参数说明：index 为 records 中的下标。返回：暂存确认；提交仍要求回调与整个批次成功。
    pub fn ack(&mut self, index: usize) -> Result<()> {
        self.check()?;
        self.index(index)?;
        self.acknowledged.insert(index);
        Ok(())
    }

    /// 业务作用：暂存仅能通过当前事务 producer 发送的批准输出。
    /// 参数说明：output 为独立原文、目标与可选时间。返回：只表示本地接受，不是事务交付回执；准入错误即使被业务捕获也不能提交本批。
    pub fn send(&mut self, output: TransactionOutput) -> Result<()> {
        self.check()?;
        // 在 native 转换前保持头名精确；拒绝锁存到整批，业务捕获错误也不能提交部分输出。
        if !self.plan.outputs.contains(&output.destination)
            || output.timestamp.is_some_and(|t| t < 0)
            || output
                .headers
                .iter()
                .any(|h| h.name.is_empty() || h.name.contains('\0'))
        {
            return self.reject(Error::InvalidConfiguration);
        }
        let headers = header_bytes(&output.headers);
        let bytes = 64
            + output.key.as_ref().map_or(0, |v| v.len() as u64)
            + output.payload.as_ref().map_or(0, |v| v.len() as u64)
            + headers;
        if output.headers.len() > 256
            || headers > 65536
            || self.outputs.len() >= self.plan.limits.max_output_records as usize
            || bytes > self.plan.limits.max_output_bytes - self.bytes
        {
            return self.reject(Error::Capacity);
        }
        self.bytes += bytes;
        self.outputs.push(output);
        Ok(())
    }

    /// 业务作用：在本次原子范围内保存失败原文，并确认对应来源输入。
    /// 参数说明：index 为本批输入，destination 为批准 DLT 分区，reason 为最多1024 UTF-8字节的安全原因。返回：只暂存原文和来源头；最终 commit 前不对 read_committed 可见。
    pub fn dead_letter(&mut self, index: usize, destination: Tp, reason: &str) -> Result<()> {
        self.check()?;
        self.index(index)?;
        if reason.len() > 1024 {
            return self.reject(Error::InvalidConfiguration);
        }
        let record = &self.records[index];
        let headers = dead_letter_headers(
            &record.headers,
            &self.plan.group,
            &record.topic,
            record.partition,
            record.offset,
            reason,
            self.attempt,
        );
        let origin = crate::DltOrigin {
            source_id: self.plan.source_id.clone(),
            cluster_id: record.cluster_id.clone(),
        };
        let headers = match origin.attach(&headers) {
            Ok(headers) => headers,
            Err(_) => return self.reject(Error::InvalidConfiguration),
        };
        let dlt_topic = destination.topic.clone();
        self.send(TransactionOutput {
            destination,
            timestamp: (record.timestamp >= 0).then_some(record.timestamp),
            key: record.key.clone(),
            payload: record.payload.clone(),
            headers,
        })?;
        self.dlt_topics.insert(dlt_topic);
        self.acknowledged.insert(index);
        Ok(())
    }

    /// 业务作用：交付必须验证历史保留策略的目标，不以业务可伪造的头识别DLT。
    /// 参数说明：无。返回：本回调通过dead_letter准入的目标借用。
    pub(crate) fn dlt_topics(&self) -> &BTreeSet<String> {
        &self.dlt_topics
    }

    /// 业务作用：正常回调结束后冻结整批确认与输出，禁止部分成功推动完整批次位点。
    /// 参数说明：无。返回：完整确认时移交所有输出；缺少任一 Manual 确认则拒绝整批。
    pub(crate) fn seal(&mut self) -> Result<Vec<TransactionOutput>> {
        self.check()?;
        self.closed = true;
        if self.plan.ack_mode == AckMode::Manual && self.acknowledged.len() != self.records.len() {
            return Err(Error::MissingAck);
        }
        Ok(std::mem::take(&mut self.outputs))
    }

    /// 业务作用：在任何本地确认或输出前复验旧上下文是否仍有业务权威。
    /// 参数说明：无。返回：关闭、已拒绝、超时或失权均不能继续修改本批。
    fn check(&self) -> Result<()> {
        if self.closed {
            return Err(Error::ContextClosed);
        }
        (self.authority)()?;
        if let Some(error) = self.rejection {
            return Err(error);
        }
        Ok(())
    }

    /// 业务作用：防止确认不属于当前输入责任的坐标。
    /// 参数说明：index 为批内下标。返回：越界使本批永久不能提交。
    fn index(&mut self, index: usize) -> Result<()> {
        if index >= self.records.len() {
            return self.reject(Error::InvalidConfiguration);
        }
        Ok(())
    }

    /// 业务作用：保留不可通过捕获异常消除的批次失败。
    /// 参数说明：error 为准入拒绝。返回：同一固定错误并封闭成功路径。
    fn reject(&mut self, error: Error) -> Result<()> {
        self.rejection = Some(error);
        Err(error)
    }
}

/// 业务作用：统一输入和输出的有序消息头字节计费。
/// 参数说明：headers 为完整头集合。返回：名称 UTF-8 与非空值的总字节，不合并重复头。
pub(crate) fn header_bytes(headers: &KafkaHeaders) -> u64 {
    headers
        .iter()
        .map(|header| {
            header.name.len() as u64 + header.value.as_ref().map_or(0, |v| v.len() as u64)
        })
        .sum()
}
