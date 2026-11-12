use std::sync::{Arc, Mutex};

/// 同一来源输入凭证只物化一份原文，重取不绕过已预约的字节上限。
#[derive(Default)]
pub(crate) struct RetainedPayload {
    /// 首次物化后冻结 null 身份和正文；共享引用不持有可变消费上下文。
    state: Mutex<State>,
}

#[derive(Default)]
enum State {
    /// 仅预约额度，尚未取得原生正文。
    #[default]
    Unmaterialized,
    /// 来源 value 为 null，不与零长度正文合并。
    Tombstone,
    /// 已在当前输入凭证下计费的唯一原文。
    Bytes(Arc<Vec<u8>>),
}

impl RetainedPayload {
    /// 业务作用：在来源输入额度内物化或复用同一正文，防止重试增加未计费的长寿命副本。
    ///
    /// 参数说明：
    /// - `payload`：当前取入的原生 value 借用，None 保留 tombstone 身份。
    ///
    /// 返回：第一次复制正文，后续相同输入只共享；同一凭证下内容变化返回固定错误并保持原文。
    pub(crate) fn materialize(
        &self,
        payload: Option<&[u8]>,
    ) -> Result<Option<Arc<Vec<u8>>>, &'static str> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match (&*state, payload) {
            (State::Unmaterialized, Some(payload)) => {
                let bytes = Arc::new(payload.to_vec());
                *state = State::Bytes(bytes.clone());
                Ok(Some(bytes))
            }
            (State::Unmaterialized, None) => {
                *state = State::Tombstone;
                Ok(None)
            }
            (State::Tombstone, None) => Ok(None),
            (State::Bytes(bytes), Some(payload)) if bytes.as_slice() == payload => {
                Ok(Some(bytes.clone()))
            }
            // 来源位置和预算已经冻结；内容变化不能借原凭证继续处理或替换仍被引用的正文。
            _ => Err("retained input payload changed"),
        }
    }
}
