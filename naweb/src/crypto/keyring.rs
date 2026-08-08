//! 有界密钥环、状态机与每 key 调用次数治理。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use arc_swap::ArcSwap;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::MappingBuildError;

use super::{CryptoError, CryptoErrorKind};

/// 密钥材料对应的静态算法合同。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyAlgorithm {
    /// modern-v2 使用的 256-bit 主密钥，实际双向 key 由 HKDF 派生。
    A256Gcm,
    /// legacy-v1 的 AES ECB 兼容材料。
    LegacyAes,
    /// legacy-v1 的 RSA PKCS#1 v1.5 私钥材料。
    LegacyRsa,
    /// legacy-v1 的 RSA 包装会话 key 加 AES 数据材料。
    LegacyRsaAes,
}

/// 密钥在轮换生命周期中的可用状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyState {
    /// 新请求解密和新响应加密都可使用，单个 ring 必须恰好一个 active。
    Active,
    /// 只接受迁移期存量请求，不用于主动选择新 key。
    DecryptOnly,
    /// 不再参与任何密码操作，只可在外部审计记录中保留元数据。
    Retired,
}

/// 不实现明文调试输出的密钥材料容器。
pub struct SecretKeyMaterial {
    bytes: Zeroizing<Vec<u8>>,
}

impl SecretKeyMaterial {
    /// 业务作用：从已经过 secret 来源合并的拥有字节建立材料。
    ///
    /// # 参数
    ///
    /// - `bytes`: 高敏感密钥字节，调用方完成来源读取后移动进容器；不得来自公开配置样例。
    ///
    /// # 返回
    ///
    /// 返回 best-effort 清零容器；空材料会在 key ring 构建时拒绝。
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Zeroizing::new(bytes),
        }
    }

    /// 业务作用：在一次受控密码调用期间借用密钥字节。
    ///
    /// # 返回
    ///
    /// 返回只读切片，生命周期不超过当前材料容器。
    pub fn expose(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

impl std::fmt::Debug for SecretKeyMaterial {
    /// 业务作用：只输出材料长度，防止配置错误和 panic 泄露密钥。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回脱敏后的标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretKeyMaterial")
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// key ring 中一条不可变密钥记录与线程安全调用计数器。
pub struct KeyEntry {
    /// 信封公开携带的有限定位符，不是 secret。
    pub kid: Arc<str>,
    /// 静态算法，必须与路由协议和 provider 能力一致。
    pub algorithm: KeyAlgorithm,
    /// 轮换状态，构建快照后不可原地修改。
    pub state: KeyState,
    /// key 可被使用的最早系统时间；省略表示立即生效。
    pub not_before: Option<SystemTime>,
    /// key 可被使用的最晚系统时间；省略表示由轮换流程控制。
    pub not_after: Option<SystemTime>,
    /// 同一进程内该密钥身份跨快照累计允许的最大密码调用次数。
    pub max_invocations: u64,
    /// 密钥配置代次，用于审计，不参与协议 AAD。
    pub generation: u64,
    material: SecretKeyMaterial,
    /// 实际计数器通过 `ArcSwap` 绑定到进程级 key 使用注册表，使同一 kid 在热更新前后的请求共享
    /// 一个原子值，不能靠发布新快照重置密码调用额度。
    invocations: ArcSwap<AtomicU64>,
}

/// 构建一条不可变密钥记录所需的完整配置。
pub struct KeyEntrySpec {
    /// 最大 128 字节的安全 ASCII 公开定位符。
    pub kid: Arc<str>,
    /// 密钥材料对应的静态算法。
    pub algorithm: KeyAlgorithm,
    /// active、decrypt-only 或 retired 轮换状态。
    pub state: KeyState,
    /// 已由可信 secret 来源解码的敏感材料。
    pub material: SecretKeyMaterial,
    /// 可选最早生效系统时间。
    pub not_before: Option<SystemTime>,
    /// 可选最晚可用系统时间，必须晚于最早时间。
    pub not_after: Option<SystemTime>,
    /// 同一进程内跨快照累计允许的最大密码调用次数，必须大于零。
    pub max_invocations: u64,
    /// 配置代次，只用于审计与轮换定位。
    pub generation: u64,
}

impl std::fmt::Debug for KeyEntry {
    /// 业务作用：输出非敏感元数据，不输出材料和当前调用的外部输入。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回脱敏后的标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeyEntry")
            .field("kid", &self.kid)
            .field("algorithm", &self.algorithm)
            .field("state", &self.state)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl KeyEntry {
    /// 业务作用：建立一条由快照独占的密钥记录。
    ///
    /// # 参数
    ///
    /// - `spec`: 已完成来源合并的公开元数据、敏感材料、有效期、调用上限与代次。
    ///
    /// # 返回
    ///
    /// 返回尚未注册到任何 ring 的不可变 key 记录。
    pub fn new(spec: KeyEntrySpec) -> Self {
        Self {
            kid: spec.kid,
            algorithm: spec.algorithm,
            state: spec.state,
            not_before: spec.not_before,
            not_after: spec.not_after,
            max_invocations: spec.max_invocations,
            generation: spec.generation,
            material: spec.material,
            invocations: ArcSwap::from_pointee(AtomicU64::new(0)),
        }
    }

    /// 业务作用：借用敏感材料供单次 provider 操作使用。
    ///
    /// # 返回
    ///
    /// 返回只读字节切片；调用方不得记录、缓存或回显。
    pub fn material(&self) -> &[u8] {
        self.material.expose()
    }

    /// 业务作用：在每次密码操作前原子消费一次调用配额。
    ///
    /// # 参数
    ///
    /// - `now`: 请求固定快照所使用的当前系统时间。
    /// - `for_encryption`: `true` 表示新加密，只允许 active；解密允许 active/decrypt-only。
    ///
    /// # 返回
    ///
    /// key 状态、有效期和次数均允许时返回空值，否则 fail closed。
    pub fn begin_operation(
        &self,
        now: SystemTime,
        for_encryption: bool,
    ) -> Result<(), CryptoError> {
        if !self.usable_at(now, for_encryption) {
            return Err(CryptoError::new(CryptoErrorKind::Key, "key-not-usable"));
        }
        self.invocations
            .load()
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.max_invocations).then_some(current + 1)
            })
            .map(|_| ())
            .map_err(|_| CryptoError::new(CryptoErrorKind::Key, "key-invocation-limit"))
    }

    /// 业务作用：只检查当前时间和方向是否允许使用，不消费调用次数。
    ///
    /// # 参数
    ///
    /// - `now`: 当前请求固定使用的系统时间。
    /// - `for_encryption`: `true` 时只接受 active；`false` 时也接受 decrypt-only。
    ///
    /// # 返回
    ///
    /// 状态和生效窗口均允许时返回 `true`。该方法只用于候选集筛选，真正进入 provider 前仍须
    /// 调用 [`Self::begin_operation`] 原子消费一次额度。
    fn usable_at(&self, now: SystemTime, for_encryption: bool) -> bool {
        let state_allowed = if for_encryption {
            self.state == KeyState::Active
        } else {
            matches!(self.state, KeyState::Active | KeyState::DecryptOnly)
        };
        state_allowed
            && self.not_before.is_none_or(|value| now >= value)
            && self.not_after.is_none_or(|value| now <= value)
    }

    /// 业务作用：返回当前绑定的共享调用计数器。
    ///
    /// # 返回
    ///
    /// 返回只含原子次数、不含密钥材料的共享句柄，供运行时热更新注册表延续同一 kid 的额度。
    pub(super) fn invocation_counter(&self) -> Arc<AtomicU64> {
        self.invocations.load_full()
    }

    /// 业务作用：把当前快照记录绑定到进程级共享调用计数器。
    ///
    /// # 参数
    ///
    /// - `counter`: 同一 tenant、key scope 和 kid 在此前快照中持续使用的原子计数。
    pub(super) fn attach_invocation_counter(&self, counter: Arc<AtomicU64>) {
        self.invocations.store(counter);
    }

    /// 业务作用：返回当前已经消费的密码调用次数。
    ///
    /// # 返回
    ///
    /// 返回 Acquire 读取的累计值，只用于快照发布校验和 readiness，不包含请求信息。
    pub(super) fn invocation_count(&self) -> u64 {
        self.invocations.load().load(Ordering::Acquire)
    }

    /// 业务作用：计算不暴露原始材料的固定长度密钥指纹。
    ///
    /// # 返回
    ///
    /// 返回当前材料的 SHA-256 摘要，只用于阻止热更新在同一 kid 下静默替换材料；该摘要不会写入
    /// 日志、指标或协议响应。
    pub(super) fn material_fingerprint(&self) -> [u8; 32] {
        Sha256::digest(self.material()).into()
    }
}

/// 单个可信租户与路由密钥域的有界不可变密钥环。
pub struct KeyRing {
    /// 已认证租户或固定服务域。
    pub tenant_scope: Arc<str>,
    /// 路由静态声明的密钥域。
    pub key_scope: Arc<str>,
    active_kid: Arc<str>,
    keys: HashMap<Arc<str>, Arc<KeyEntry>>,
}

impl std::fmt::Debug for KeyRing {
    /// 业务作用：输出非敏感 ring 摘要，不输出密钥材料。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeyRing")
            .field("tenant_scope", &self.tenant_scope)
            .field("key_scope", &self.key_scope)
            .field("active_kid", &self.active_kid)
            .field("key_count", &self.keys.len())
            .finish_non_exhaustive()
    }
}

impl KeyRing {
    /// 业务作用：从完整 key 集合建立一个有界密钥环。
    ///
    /// # 参数
    ///
    /// - `tenant_scope`: 已认证租户域或单租户固定服务域，最大 128 字节。
    /// - `key_scope`: route policy 使用的静态域，最大 128 字节。
    /// - `keys`: 同一算法合同下的 active、decrypt-only 与 retired 记录，最多 16 条。
    ///
    /// # 返回
    ///
    /// 恰好一个 active 且材料合法时返回 ring；任何歧义都会拒绝启动。
    pub fn new(
        tenant_scope: impl Into<Arc<str>>,
        key_scope: impl Into<Arc<str>>,
        keys: Vec<Arc<KeyEntry>>,
    ) -> Result<Self, MappingBuildError> {
        let tenant_scope = tenant_scope.into();
        let key_scope = key_scope.into();
        if !valid_scope(&tenant_scope)
            || !valid_scope(&key_scope)
            || keys.is_empty()
            || keys.len() > 16
        {
            return Err(MappingBuildError::new("key ring scope 或数量非法"));
        }
        let mut map = HashMap::with_capacity(keys.len());
        let mut active = None;
        let mut active_algorithm = None;
        for key in keys {
            if !valid_kid(&key.kid)
                || (key.state != KeyState::Retired && key.material().is_empty())
                || (key.state != KeyState::Retired && key.max_invocations == 0)
                || key
                    .not_before
                    .zip(key.not_after)
                    .is_some_and(|(start, end)| start >= end)
            {
                return Err(MappingBuildError::new("key ring 含非法 key 元数据"));
            }
            if key.state != KeyState::Retired
                && key.algorithm == KeyAlgorithm::A256Gcm
                && key.material().len() != 32
            {
                return Err(MappingBuildError::new("A256GCM 主密钥必须恰好 32 字节"));
            }
            if key.state != KeyState::Retired {
                validate_legacy_key_material(&key)?;
            }
            if key.state != KeyState::Retired {
                if let Some(expected) = active_algorithm {
                    if key.algorithm != expected {
                        return Err(MappingBuildError::new("同一 key ring 混用了不同算法"));
                    }
                } else {
                    active_algorithm = Some(key.algorithm);
                }
            }
            if key.state == KeyState::Active && active.replace(key.kid.clone()).is_some() {
                return Err(MappingBuildError::new("key ring 存在多个 active key"));
            }
            if map.insert(key.kid.clone(), key).is_some() {
                return Err(MappingBuildError::new("key ring 的 kid 重复"));
            }
        }
        let active_kid =
            active.ok_or_else(|| MappingBuildError::new("key ring 缺少 active key"))?;
        Ok(Self {
            tenant_scope,
            key_scope,
            active_kid,
            keys: map,
        })
    }

    /// 业务作用：按信封显式 kid 解析可用于解密的 key。
    ///
    /// # 参数
    ///
    /// - `kid`: 经过长度和字符集检查的公开定位符，只在当前有限 ring 内查找。
    /// - `now`: 本次请求固定使用的系统时间。
    ///
    /// # 返回
    ///
    /// active/decrypt-only key 可用时返回共享记录；未知或失效 key 统一返回 key 错误。
    pub fn resolve_for_decrypt(
        &self,
        kid: &str,
        now: SystemTime,
    ) -> Result<Arc<KeyEntry>, CryptoError> {
        let key = self
            .keys
            .get(kid)
            .cloned()
            .ok_or_else(|| CryptoError::new(CryptoErrorKind::Key, "key-not-found"))?;
        key.begin_operation(now, false)?;
        Ok(key)
    }

    /// 业务作用：选择当前快照唯一 active key 进行新加密。
    ///
    /// # 参数
    ///
    /// - `now`: 本次响应固定使用的系统时间。
    ///
    /// # 返回
    ///
    /// active key 有效且未耗尽调用次数时返回共享记录，否则 fail closed。
    pub fn active_for_encrypt(&self, now: SystemTime) -> Result<Arc<KeyEntry>, CryptoError> {
        let key = self
            .keys
            .get(self.active_kid.as_ref())
            .cloned()
            .ok_or_else(|| CryptoError::new(CryptoErrorKind::Key, "active-key-missing"))?;
        key.begin_operation(now, true)?;
        Ok(key)
    }

    /// 业务作用：返回启动审计使用的 active key 非敏感元数据句柄。
    ///
    /// # 返回
    ///
    /// active 记录当前有效且尚有调用额度时返回，但不消费调用配额；该入口只允许启动审计读取
    /// 算法和状态。
    pub fn active_for_audit(&self) -> Result<Arc<KeyEntry>, MappingBuildError> {
        let key = self
            .keys
            .get(self.active_kid.as_ref())
            .cloned()
            .ok_or_else(|| MappingBuildError::new("key ring active key 缺失"))?;
        let now = SystemTime::now();
        if key.state != KeyState::Active
            || key.not_before.is_some_and(|value| now < value)
            || key.not_after.is_some_and(|value| now > value)
            || key.invocation_count() >= key.max_invocations
        {
            return Err(MappingBuildError::new("key ring active key 当前不可用"));
        }
        Ok(key)
    }

    /// 业务作用：返回 legacy-v1 无 kid 信封允许尝试的固定有界解密序列。
    ///
    /// # 参数
    ///
    /// - `now`: 本次请求固定使用的系统时间。
    ///
    /// # 返回
    ///
    /// 返回 active 加至多一个按 kid 排序的 decrypt-only key。调用方必须在真正尝试某一项之前
    /// 才调用 `begin_operation`，避免 active 已成功时仍提前消耗 previous key 配额。函数绝不遍历
    /// 无界历史 key，避免失败请求放大 CPU 成本。
    pub fn legacy_decrypt_candidates(
        &self,
        now: SystemTime,
    ) -> Result<Vec<Arc<KeyEntry>>, CryptoError> {
        let mut result = Vec::with_capacity(2);
        let active = self
            .keys
            .get(self.active_kid.as_ref())
            .cloned()
            .ok_or_else(|| CryptoError::new(CryptoErrorKind::Key, "active-key-missing"))?;
        if !active.usable_at(now, false) {
            return Err(CryptoError::new(CryptoErrorKind::Key, "key-not-usable"));
        }
        result.push(active);

        let mut previous = self
            .keys
            .values()
            .filter(|key| key.state == KeyState::DecryptOnly)
            .cloned()
            .collect::<Vec<_>>();
        previous.sort_by(|left, right| left.kid.cmp(&right.kid));
        if let Some(key) = previous.into_iter().next() {
            if key.usable_at(now, false) {
                result.push(key);
            }
        }
        Ok(result)
    }

    /// 业务作用：返回 key ring 中全部有限记录，供进程级调用次数注册表在热更新时重新绑定。
    ///
    /// # 返回
    ///
    /// 返回至多 16 个共享记录的迭代器；调用方只读取公开元数据和原子计数，不读取材料。
    pub(super) fn usage_entries(&self) -> impl Iterator<Item = &Arc<KeyEntry>> {
        self.keys.values()
    }
}

/// 业务作用：在监听端口前验证 legacy key 的真实算法格式。
///
/// 只检查长度、UTF-8、Base64/DER 和 RSA 模数，不执行私钥加解密，也不把材料写入诊断。这样错误
/// AES 长度或损坏 RSA 私钥会成为清晰的启动错误，而不是第一条业务请求上的统一 4xx/5xx。
fn validate_legacy_key_material(key: &KeyEntry) -> Result<(), MappingBuildError> {
    match key.algorithm {
        KeyAlgorithm::A256Gcm => Ok(()),
        KeyAlgorithm::LegacyAes => {
            let material = std::str::from_utf8(key.material())
                .map_err(|_| MappingBuildError::new("legacy AES key 必须是 UTF-8 文本"))?;
            if matches!(material.len(), 16 | 24 | 32) {
                Ok(())
            } else {
                Err(MappingBuildError::new(
                    "legacy AES key 必须恰好是 16、24 或 32 个 UTF-8 字节",
                ))
            }
        }
        KeyAlgorithm::LegacyRsa | KeyAlgorithm::LegacyRsaAes => {
            let material = std::str::from_utf8(key.material())
                .map_err(|_| MappingBuildError::new("legacy RSA 私钥必须是 UTF-8 Base64 文本"))?;
            ncrypto::validate_rsa_private_key(material)
                .map(|_| ())
                .map_err(|_| MappingBuildError::new("legacy RSA 私钥不是有效的 PKCS#8 Base64 key"))
        }
    }
}

/// 业务作用：校验租户与路由密钥域文本。
///
/// # 参数
///
/// - `value`: 启动配置提供的域文本，最大 128 字节。
///
/// # 返回
///
/// 非空且不含控制字符时返回 `true`。
fn valid_scope(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

/// 业务作用：校验信封公开 kid 的字符集与长度。
///
/// # 参数
///
/// - `value`: key 配置中的公开定位符，最大 128 字节。
///
/// # 返回
///
/// 只含安全 ASCII 字符时返回 `true`。
fn valid_kid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}
