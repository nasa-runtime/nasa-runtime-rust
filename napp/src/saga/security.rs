//! Saga 安全资源随配置视图一次发布，入站与出站读取同代材料。

use super::*;

/// 业务作用：保存同一 secret 代次中所有受管 Saga 凭据，调试输出不包含材料。
#[derive(Clone)]
pub(crate) struct SagaSecuritySnapshot {
    pub(super) secrets: Arc<nasecret::SecretSnapshot>,
    pub(super) http: BTreeMap<String, Arc<nasaga_runtime::SagaHttpCredentials>>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    grpc: BTreeMap<String, Arc<GrpcClientSnapshot>>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    tls: Option<Arc<crate::grpc::RotatingGrpcTlsSnapshot>>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    peer_certificates: BTreeMap<String, Arc<PeerCertificateSnapshot>>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    peer_policies: Vec<Vec<String>>,
}

impl std::fmt::Debug for SagaSecuritySnapshot {
    /// 业务作用：只暴露配置代次，避免日志泄露 key 或信任边材料。
    /// 参数说明：`formatter` 是日志格式化目标。
    /// 返回：不包含任何凭据的结构摘要。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SagaSecuritySnapshot")
            .field("generation", &self.secrets.generation())
            .finish_non_exhaustive()
    }
}

/// 业务作用：维护受管资源所需的 secret 引用，并从配置存储的同一原子视图读取材料。
pub(super) struct SagaSecurityState {
    config: Arc<crate::config::ConfigStore>,
    initial: Mutex<Arc<SagaSecuritySnapshot>>,
    #[cfg(any(
        feature = "nacos-config",
        feature = "saga-grpc",
        feature = "saga-grpc-pgsql"
    ))]
    overlap: Duration,
}

impl SagaSecurityState {
    /// 业务作用：复用本应用唯一的凭据资源目录，不为每个协议客户端建立独立发布点。
    /// 参数说明：`application` 提供当前配置存储与 Saga 资源生命周期。
    /// 返回：重叠窗口合法时返回共享资源目录；非法窗口拒绝装配。
    pub(super) fn for_application(application: &Application) -> ApplicationResult<Arc<Self>> {
        if let Some(state) = application.saga_runtime().security.get() {
            return Ok(state.clone());
        }
        let settings = read_saga_settings(application, ApplicationPhase::Ready)?;
        if !(1..=3_600_000).contains(&settings.credential_overlap_ms) {
            return Err(saga_error(
                ApplicationPhase::Ready,
                "Saga credential overlap must be between 1 and 3600000 milliseconds",
            ));
        }
        let runtime = application.saga_runtime();
        let state = runtime.security.get_or_init(|| {
            Arc::new(Self {
                config: application.saga_config_store(),
                initial: Mutex::new(Arc::new(SagaSecuritySnapshot {
                    secrets: application.secrets(),
                    http: BTreeMap::new(),
                    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                    grpc: BTreeMap::new(),
                    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                    tls: None,
                    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                    peer_certificates: BTreeMap::new(),
                    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                    peer_policies: Vec::new(),
                })),
                #[cfg(any(
                    feature = "nacos-config",
                    feature = "saga-grpc",
                    feature = "saga-grpc-pgsql"
                ))]
                overlap: Duration::from_millis(settings.credential_overlap_ms),
            })
        });
        Ok(state.clone())
    }

    /// 业务作用：取得一次请求固定使用的完整安全快照。
    /// 参数说明：无。
    /// 返回：配置已发布的同代资源；首次轮换前使用装配时校验过的初始快照。
    pub(super) fn current(&self) -> Arc<SagaSecuritySnapshot> {
        self.config.load().saga_security().unwrap_or_else(|| {
            self.initial
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        })
    }

    /// 业务作用：登记并校验 HMAC 资源引用，所有克隆认证器随后读取同一个配置快照。
    /// 参数说明：`reference` 是受信配置中固定的 secret 引用。
    /// 返回：已登记引用的动态认证器；材料缺失或非法时拒绝装配。
    fn http_authenticator(
        self: &Arc<Self>,
        reference: &str,
    ) -> ApplicationResult<nasaga_runtime::SagaHttpMessageAuthenticator> {
        let mut initial = self
            .initial
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !initial.http.contains_key(reference) {
            // 首次发布后资源目录已经封口，拒绝遗漏引用，避免请求读取不存在的同代资源。
            if self.config.load().saga_security().is_some() {
                return Err(saga_error(
                    ApplicationPhase::Running,
                    "Saga security resource registration is closed",
                ));
            }
            let key = http_key(&initial.secrets, reference)?;
            let credentials =
                nasaga_runtime::SagaHttpCredentials::from_hex_key(key).map_err(|_| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "Saga HTTP credential contract is invalid",
                    )
                })?;
            let mut http = initial.http.clone();
            http.insert(reference.to_owned(), Arc::new(credentials));
            *initial = Arc::new(SagaSecuritySnapshot {
                secrets: initial.secrets.clone(),
                http,
                ..(**initial).clone()
            });
        }
        drop(initial);
        nasaga_runtime::SagaHttpMessageAuthenticator::from_source(
            Arc::new(HttpCredentialSource {
                state: self.clone(),
                reference: reference.to_owned(),
            }),
            30_000,
        )
        .map_err(|_| {
            saga_error(
                ApplicationPhase::Ready,
                "Saga HTTP credential source is invalid",
            )
        })
    }

    /// 业务作用：在发布前校验全部已登记资源并构造新旧 HMAC 重叠窗口，任何失败保留原快照。
    /// 参数说明：`candidate` 是未发布的完整 secret 候选。
    /// 返回：所有资源都能构建时返回完整安全快照；任一引用删除、编码非法或历史窗口越界时拒绝整帧。
    #[cfg(any(feature = "nacos-config", feature = "config-watch"))]
    fn prepare(
        &self,
        candidate: Arc<nasecret::SecretSnapshot>,
    ) -> ApplicationResult<Arc<SagaSecuritySnapshot>> {
        let current = self.current();
        let mut http = BTreeMap::new();
        for (reference, previous) in &current.http {
            let key = http_key(&candidate, reference)?;
            let credentials = if http_key(&current.secrets, reference)? == key {
                previous.clone()
            } else {
                Arc::new(previous.rotated(key, self.overlap).map_err(|_| {
                    saga_error(
                        ApplicationPhase::Running,
                        "Saga HTTP credential rotation candidate is invalid",
                    )
                })?)
            };
            http.insert(reference.clone(), credentials);
        }
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        let grpc = current
            .grpc
            .iter()
            .map(|(reference, previous)| {
                let material = parse_grpc_credential(&candidate, reference)?;
                if candidate.get(reference).map(|value| value.expose())
                    == current.secrets.get(reference).map(|value| value.expose())
                {
                    Ok((reference.clone(), previous.clone()))
                } else {
                    Ok((
                        reference.clone(),
                        Arc::new(GrpcClientSnapshot::prepare(
                            material,
                            Some(previous),
                            self.overlap,
                        )?),
                    ))
                }
            })
            .collect::<ApplicationResult<BTreeMap<_, _>>>()?;
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        let tls = current
            .tls
            .as_ref()
            .map(|previous| {
                previous
                    .rotated(candidate.clone(), self.overlap)
                    .map(Arc::new)
            })
            .transpose()?;
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        let peer_certificates = current
            .peer_certificates
            .iter()
            .map(|(reference, previous)| {
                PeerCertificateSnapshot::prepare(
                    &candidate,
                    reference,
                    Some(previous),
                    self.overlap,
                )
                .map(|next| (reference.clone(), Arc::new(next)))
            })
            .collect::<ApplicationResult<BTreeMap<_, _>>>()?;
        let prepared = Arc::new(SagaSecuritySnapshot {
            secrets: candidate,
            http,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            grpc,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            tls,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            peer_certificates,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            peer_policies: current.peer_policies.clone(),
        });
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        prepared.validate_peer_policies()?;
        Ok(prepared)
    }
}

/// 业务作用：保存当前客户端身份与有界的旧 CA 重叠窗口，窗口到期无需另一次配置刷新即可收紧信任。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct GrpcClientSnapshot {
    current: Arc<ManagedGrpcCredentialMaterial>,
    previous_ca: Vec<(String, Instant)>,
    overlap_materials: Vec<(Instant, Arc<ManagedGrpcCredentialMaterial>)>,
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl GrpcClientSnapshot {
    /// 业务作用：预构造各旧 CA 到期边界对应的 TLS 材料，保证请求线程只做不可变快照选择。
    /// 参数说明：`material` 是新候选，`previous` 为当前已提交资源，`overlap` 为旧信任根最长保留窗口。
    /// 返回：证书、密钥和所有信任组合都合法时返回候选；authority 漂移或历史窗口超过八份时拒绝。
    fn prepare(
        material: ManagedGrpcCredentialMaterial,
        previous: Option<&Self>,
        overlap: Duration,
    ) -> ApplicationResult<Self> {
        grpc_target::validate_grpc_credential(&material)?;
        let now = Instant::now();
        let mut history = Vec::new();
        if let Some(previous) = previous {
            if material.domain_name != previous.current.domain_name {
                return Err(saga_error(
                    ApplicationPhase::Running,
                    "Saga gRPC credential rotation cannot change authority",
                ));
            }
            history = previous
                .previous_ca
                .iter()
                .filter(|(ca, expires)| *expires > now && ca != &material.ca_certificate_pem)
                .cloned()
                .collect();
            if material.ca_certificate_pem != previous.current.ca_certificate_pem {
                history.retain(|(ca, _)| ca != &previous.current.ca_certificate_pem);
                history.push((previous.current.ca_certificate_pem.clone(), now + overlap));
            }
        }
        if history.len() > 8 {
            return Err(saga_error(
                ApplicationPhase::Running,
                "Saga gRPC credential overlap capacity exceeded",
            ));
        }
        history.sort_by_key(|(_, expires)| *expires);
        let mut boundaries: Vec<_> = history.iter().map(|(_, expires)| *expires).collect();
        boundaries.dedup();
        let mut overlap_materials = Vec::new();
        for boundary in boundaries {
            let mut combined = material.clone();
            for (ca, expires) in &history {
                if *expires >= boundary {
                    combined.ca_certificate_pem.push('\n');
                    combined.ca_certificate_pem.push_str(ca);
                }
            }
            grpc_target::validate_grpc_credential(&combined)?;
            overlap_materials.push((boundary, Arc::new(combined)));
        }
        Ok(Self {
            current: Arc::new(material),
            previous_ca: history,
            overlap_materials,
        })
    }

    /// 业务作用：根据单调时钟选择仍获准使用的 CA 集合，禁止旧连接池越过信任根到期边界复用。
    /// 参数说明：无。
    /// 返回：同一有效窗口复用同一材料 Arc；窗口改变时返回另一 Arc，连接池据此重新握手。
    fn active(&self) -> Arc<ManagedGrpcCredentialMaterial> {
        let now = Instant::now();
        self.overlap_materials
            .iter()
            .find(|(expires, _)| *expires > now)
            .map(|(_, material)| material.clone())
            .unwrap_or_else(|| self.current.clone())
    }
}

/// 业务作用：把一个出站 mTLS 引用映射到当前配置视图中的身份与信任根。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct GrpcCredentialSource {
    state: Arc<SagaSecurityState>,
    reference: String,
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl grpc_target::GrpcCredentialSource for GrpcCredentialSource {
    /// 业务作用：为下一次 RPC 读取同代 mTLS 材料，旧 CA 到期时触发连接池替换。
    /// 参数说明：无。
    /// 返回：当前安全快照中的合法材料；准备门禁保证固定引用始终存在。
    fn credential(&self) -> Arc<ManagedGrpcCredentialMaterial> {
        self.state
            .current()
            .grpc
            .get(&self.reference)
            .expect("registered Saga gRPC credential remains present in every snapshot")
            .active()
    }
}

/// 业务作用：从专用 secret 快照解析 mTLS JSON，错误信息不携带证书或私钥。
/// 参数说明：`snapshot` 为同代秘密材料，`reference` 是固定出站凭据引用。
/// 返回：完整结构合法时返回尚需 TLS 准备的材料；缺失或非法 JSON 拒绝候选。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn parse_grpc_credential(
    snapshot: &nasecret::SecretSnapshot,
    reference: &str,
) -> ApplicationResult<ManagedGrpcCredentialMaterial> {
    let value = snapshot.get(reference).ok_or_else(|| {
        saga_error(
            ApplicationPhase::Running,
            "Saga gRPC credential is unavailable",
        )
    })?;
    serde_json::from_slice(value.expose()).map_err(|_| {
        saga_error(
            ApplicationPhase::Running,
            "Saga gRPC credential material is invalid",
        )
    })
}

/// 业务作用：为全部 gRPC client/result/command/Registry 入口登记可原子轮换的出站凭据。
/// 参数说明：`application` 提供配置发布点，`reference` 为受信配置中的 mTLS secret 引用。
/// 返回：携带同代材料来源的目标配置；证书、私钥或 CA 不能准备时拒绝 Ready。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
pub(super) fn grpc_credential(
    application: &Application,
    reference: &str,
) -> ApplicationResult<ManagedGrpcCredentialMaterial> {
    let state = SagaSecurityState::for_application(application)?;
    let mut initial = state
        .initial
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !initial.grpc.contains_key(reference) {
        // 已提交快照必须包含全部固定引用；运行期不能绕过候选准备追加资源。
        if state.config.load().saga_security().is_some() {
            return Err(saga_error(
                ApplicationPhase::Running,
                "Saga security resource registration is closed",
            ));
        }
        let material = parse_grpc_credential(&initial.secrets, reference)?;
        let prepared = Arc::new(GrpcClientSnapshot::prepare(material, None, state.overlap)?);
        let mut grpc = initial.grpc.clone();
        grpc.insert(reference.to_owned(), prepared);
        *initial = Arc::new(SagaSecuritySnapshot {
            grpc,
            ..(**initial).clone()
        });
    }
    let mut material = (*initial
        .grpc
        .get(reference)
        .expect("registered credential")
        .current)
        .clone();
    drop(initial);
    material.source = Some(Arc::new(GrpcCredentialSource {
        state,
        reference: reference.to_owned(),
    }));
    Ok(material)
}

/// 业务作用：将一个固定信任边投影到统一安全快照中的 HMAC 材料。
struct HttpCredentialSource {
    state: Arc<SagaSecurityState>,
    reference: String,
}

impl nasaga_runtime::SagaHttpCredentialSource for HttpCredentialSource {
    /// 业务作用：让既有入站和出站句柄在下一次认证时读取最新已提交材料。
    /// 参数说明：无。
    /// 返回：固定引用对应的当前与重叠期旧凭据；配置准备阶段保证该引用永不缺失。
    fn credentials(&self) -> Arc<nasaga_runtime::SagaHttpCredentials> {
        self.state
            .current()
            .http
            .get(&self.reference)
            .expect("registered Saga HTTP credential remains present in every snapshot")
            .clone()
    }
}

/// 业务作用：只从 secret 专用快照读取规范编码，错误不包含引用值或凭据正文。
/// 参数说明：`snapshot` 是已解析的秘密材料集合，`reference` 是配置选择的信任边。
/// 返回：存在且为 UTF-8 时返回借用字符串；缺失或非法编码返回脱敏错误。
fn http_key<'a>(
    snapshot: &'a nasecret::SecretSnapshot,
    reference: &str,
) -> ApplicationResult<&'a str> {
    let material = snapshot.get(reference).ok_or_else(|| {
        saga_error(
            ApplicationPhase::Running,
            "Saga HTTP credential is unavailable",
        )
    })?;
    std::str::from_utf8(material.expose()).map_err(|_| {
        saga_error(
            ApplicationPhase::Running,
            "Saga HTTP credential encoding is invalid",
        )
    })
}

/// 业务作用：为框架所有 HMAC 入站与出站边建立同代动态凭据来源。
/// 参数说明：`application` 提供统一安全资源，`reference` 为固定 secret 引用。
/// 返回：完整认证器；非法资源在 Ready 前拒绝。
pub(super) fn http_authenticator(
    application: &Application,
    reference: &str,
) -> ApplicationResult<nasaga_runtime::SagaHttpMessageAuthenticator> {
    SagaSecurityState::for_application(application)?.http_authenticator(reference)
}

/// 业务作用：只准备候选 secret 对应的 Saga 安全资源，不要求预先构造应用状态表。
/// 参数说明：`application` 为现有资源目录；`secrets` 为同代候选 secret。
/// 返回：已装配 Saga 时返回准备好的材料；未启用时为空；任一材料失败拒绝候选。
#[cfg(any(feature = "nacos-config", feature = "config-watch"))]
pub(crate) fn prepare_security_materials(
    application: &Application,
    secrets: Arc<nasecret::SecretSnapshot>,
) -> ApplicationResult<Option<Arc<SagaSecuritySnapshot>>> {
    application
        .saga_runtime()
        .security
        .get()
        .map(|state| state.prepare(secrets))
        .transpose()
}

/// 业务作用：把受管 gRPC listener 的握手器与 Saga 入站、出站凭据绑定到同一配置发布点。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
pub(crate) struct ManagedGrpcTlsSource {
    state: Arc<SagaSecurityState>,
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl nagrpc::GrpcTlsAcceptorSource for ManagedGrpcTlsSource {
    /// 业务作用：为一次新握手选择已经完成校验的当前 TLS 资源。
    /// 参数说明：无。
    /// 返回：当前或有界重叠窗口内的握手器；请求不解析 PEM 或读取独立 secret。
    fn acceptor(&self) -> nagrpc::GrpcTlsAcceptor {
        self.state
            .current()
            .tls
            .as_ref()
            .expect("registered TLS resource remains present")
            .acceptor()
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl ManagedGrpcTlsSource {
    /// 业务作用：把当前握手证书的到期事实提供给 readiness 和指标。
    /// 参数说明：无。
    /// 返回：当前已提交服务端证书链最早到期的 Unix 秒。
    pub(crate) fn expiry_timestamp(&self) -> u64 {
        self.state
            .current()
            .tls
            .as_ref()
            .expect("registered TLS resource remains present")
            .expiry_timestamp()
    }
}

/// 业务作用：在 listener 开放前登记完整 TLS 资源，后续配置候选必须同时通过全部 Saga 凭据校验。
/// 参数说明：`application` 提供统一发布点，`plan` 固定 listener 的秘密引用与证书时间政策。
/// 返回：Saga 已装配时返回可热切换 TLS 来源，未装配时返回空值；重复或晚于首次发布的装配拒绝。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
pub(crate) fn grpc_tls_source(
    application: &Application,
    plan: &crate::grpc::GrpcTlsPlan,
) -> ApplicationResult<Option<Arc<ManagedGrpcTlsSource>>> {
    let Some(state) = application.saga_runtime().security.get().cloned() else {
        return Ok(None);
    };
    let mut initial = state
        .initial
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if initial.tls.is_some() || state.config.load().saga_security().is_some() {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "Saga TLS resource registration is closed",
        ));
    }
    let tls = Arc::new(crate::grpc::RotatingGrpcTlsSnapshot::prepare(
        plan,
        initial.secrets.clone(),
        None,
        state.overlap,
    )?);
    *initial = Arc::new(SagaSecuritySnapshot {
        tls: Some(tls),
        ..(**initial).clone()
    });
    drop(initial);
    Ok(Some(Arc::new(ManagedGrpcTlsSource { state })))
}

/// 业务作用：将证书身份的信任窗口同时约束于轮换截止时刻和证书真实到期时刻。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct PeerCertificateSnapshot {
    current: (String, Instant),
    previous: Vec<(String, Instant)>,
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl PeerCertificateSnapshot {
    /// 业务作用：从同代 PEM 派生 principal，并保留尚未到期的旧身份以支持滚动轮换。
    /// 参数说明：`secrets` 为完整候选，`reference` 是证书引用，`previous` 为已提交资源，`overlap` 限制旧身份寿命。
    /// 返回：证书时间和用途合法时返回有界身份集合；材料非法或超过八个旧身份时拒绝。
    fn prepare(
        secrets: &nasecret::SecretSnapshot,
        reference: &str,
        previous: Option<&Self>,
        overlap: Duration,
    ) -> ApplicationResult<Self> {
        let pem = secrets.get(reference).ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "Saga gRPC peer certificate is unavailable",
            )
        })?;
        let principal = nagrpc::client_certificate_principal(pem.expose()).map_err(|_| {
            saga_error(
                ApplicationPhase::Running,
                "Saga gRPC peer certificate is invalid",
            )
        })?;
        let expiry = nagrpc::client_certificate_expiry_timestamp(pem.expose()).map_err(|_| {
            saga_error(
                ApplicationPhase::Running,
                "Saga gRPC peer certificate lifetime is invalid",
            )
        })?;
        let now = Instant::now();
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| {
                saga_error(ApplicationPhase::Running, "Saga gRPC peer clock is invalid")
            })?;
        let remaining = Duration::from_secs(expiry)
            .checked_sub(wall)
            .filter(|value| !value.is_zero())
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Running,
                    "Saga gRPC peer certificate has expired",
                )
            })?;
        let expires = now.checked_add(remaining).ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "Saga gRPC peer certificate lifetime overflows",
            )
        })?;
        let mut history = previous
            .map(|value| {
                value
                    .previous
                    .iter()
                    .filter(|(old, deadline)| *deadline > now && *old != principal)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(previous) = previous {
            if previous.current.0 != principal && previous.current.1 > now {
                history.retain(|(old, _)| *old != previous.current.0);
                history.push((
                    previous.current.0.clone(),
                    previous.current.1.min(now + overlap),
                ));
            }
        }
        if history.len() > 8 {
            return Err(saga_error(
                ApplicationPhase::Running,
                "Saga gRPC peer overlap capacity exceeded",
            ));
        }
        Ok(Self {
            current: (principal, expires),
            previous: history,
        })
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl SagaSecuritySnapshot {
    /// 业务作用：在一个安全快照内展开静态指纹或受管证书引用，供入站认证和 Catalog 共享信任事实。
    /// 参数说明：`binding` 是经校验的指纹或 secret 证书定位符。
    /// 返回：只包含当前仍有效的证书身份；缺失引用返回空集合使调用方拒绝。
    pub(super) fn peer_principals(&self, binding: &str) -> Vec<String> {
        let Some(reference) = binding.strip_prefix("secret://") else {
            return vec![binding.to_owned()];
        };
        let now = Instant::now();
        self.peer_certificates
            .get(reference)
            .map(|value| {
                std::iter::once(&value.current)
                    .chain(value.previous.iter())
                    .filter(|(_, expires)| *expires > now)
                    .map(|(principal, _)| principal.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 业务作用：在发布前拒绝同一认证表内证书轮换造成的身份碰撞，避免凭据得到另一主体的权限。
    /// 参数说明：无。
    /// 返回：所有固定认证表中的有效身份均唯一时成功，否则整帧候选不发布。
    fn validate_peer_policies(&self) -> ApplicationResult<()> {
        for policy in &self.peer_policies {
            let mut seen = BTreeSet::new();
            for binding in policy {
                for principal in self.peer_principals(binding) {
                    if !seen.insert(principal) {
                        return Err(saga_error(
                            ApplicationPhase::Running,
                            "Saga gRPC peer rotation creates an ambiguous identity",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// 业务作用：固定业务身份和权限绑定，仅让对应的证书材料随统一安全快照轮换。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[derive(Clone)]
pub(super) struct GrpcPeerBindings<T> {
    configured: BTreeMap<String, T>,
    state: Arc<SagaSecurityState>,
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl<T: Clone> GrpcPeerBindings<T> {
    /// 业务作用：在开放 gRPC 服务前登记所有证书引用，并冻结 principal 到业务权限的关系。
    /// 参数说明：`application` 提供安全发布点，`configured` 绑定指纹或证书引用到不可变业务授权。
    /// 返回：全部材料和唯一性门禁通过时返回动态认证表；非法或晚于首次发布的装配拒绝。
    pub(super) fn new(
        application: &Application,
        configured: BTreeMap<String, T>,
    ) -> ApplicationResult<Self> {
        let state = SagaSecurityState::for_application(application)?;
        let mut initial = state
            .initial
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.config.load().saga_security().is_some() {
            return Err(saga_error(
                ApplicationPhase::Running,
                "Saga security resource registration is closed",
            ));
        }
        let mut prepared = (**initial).clone();
        for binding in configured.keys() {
            validate_managed_grpc_principal(binding, ApplicationPhase::Start)?;
            if let Some(reference) = binding.strip_prefix("secret://") {
                if !prepared.peer_certificates.contains_key(reference) {
                    let certificate = PeerCertificateSnapshot::prepare(
                        &initial.secrets,
                        reference,
                        None,
                        state.overlap,
                    )?;
                    prepared
                        .peer_certificates
                        .insert(reference.to_owned(), Arc::new(certificate));
                }
            }
        }
        prepared
            .peer_policies
            .push(configured.keys().cloned().collect());
        prepared.validate_peer_policies()?;
        *initial = Arc::new(prepared);
        drop(initial);
        Ok(Self { configured, state })
    }

    /// 业务作用：每次 RPC 重新判断证书身份是否仍获准使用，已有 TLS 连接不能越过轮换截止时刻。
    /// 参数说明：`principal` 来自 listener 校验过的叶证书指纹。
    /// 返回：精确且唯一命中时复制固定业务授权；过期、未知或歧义身份均返回空值。
    pub(super) fn get(&self, principal: &str) -> Option<T> {
        let snapshot = self.state.current();
        let mut matches = self.configured.iter().filter(|(binding, _)| {
            snapshot
                .peer_principals(binding)
                .iter()
                .any(|candidate| candidate == principal)
        });
        let (_, value) = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        Some(value.clone())
    }

    /// 业务作用：校验装配角色是否包含要求的逻辑服务，不依赖某一代证书指纹。
    /// 参数说明：无。
    /// 返回：已冻结授权值的借用迭代器，不包含证书或私钥。
    pub(super) fn values(&self) -> impl Iterator<Item = &T> {
        self.configured.values()
    }
}

/// 业务作用：固定参与方 result 信任边的引用，在续租前从当前凭据生成实际发送合同。
pub(super) struct CapabilityResultSource {
    state: Arc<SagaSecurityState>,
    transport: SagaTransportKind,
    reference: Option<String>,
}

impl CapabilityResultSource {
    /// 业务作用：为 HTTP 或 gRPC Registry 续租计划绑定所选数据面的真实结果凭据。
    /// 参数说明：`application` 提供统一资源目录，`settings` 固定数据面协议及信任边。
    /// 返回：已知数据面对应的固定来源；缺少 HTTP/gRPC 凭据时拒绝装配。
    pub(super) fn new(
        application: &Application,
        settings: &SagaSettings,
    ) -> ApplicationResult<Self> {
        let selected = settings.transport.command_result.as_ref().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "Saga capability data transport is unavailable",
            )
        })?;
        let transport = selected.kind.ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "Saga capability data transport is unavailable",
            )
        })?;
        let reference = match transport {
            SagaTransportKind::Http => Some(
                selected
                    .http
                    .as_ref()
                    .and_then(|value| value.result_credential_ref.clone())
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Saga HTTP result credential is unavailable",
                        )
                    })?,
            ),
            SagaTransportKind::Grpc => Some(
                selected
                    .grpc
                    .as_ref()
                    .and_then(|value| value.credential_ref.clone())
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Saga gRPC result credential is unavailable",
                        )
                    })?,
            ),
            _ => None,
        };
        Ok(Self {
            state: SagaSecurityState::for_application(application)?,
            transport,
            reference,
        })
    }

    /// 业务作用：让本批 capability 与当前出站结果凭据一致，变化交由 Catalog 分配新的 route generation。
    /// 参数说明：`descriptors` 是本副本待续租的完整能力集合。
    /// 返回：HTTP/gRPC 全批摘要更新成功；缺失材料或证书失效拒绝续租，其它数据面保持已有后端合同。
    pub(super) fn refresh(
        &self,
        descriptors: &mut [nasaga_runtime::CapabilityDescriptor],
    ) -> ApplicationResult<()> {
        let Some(reference) = &self.reference else {
            return Ok(());
        };
        let snapshot = self.state.current();
        let digest = match self.transport {
            SagaTransportKind::Http => snapshot
                .http
                .get(reference)
                .ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Running,
                        "Saga HTTP result credential is unavailable",
                    )
                })?
                .current_result_contract()
                .to_owned(),
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            SagaTransportKind::Grpc => managed_grpc_result_contract_digest(
                &snapshot
                    .grpc
                    .get(reference)
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Running,
                            "Saga gRPC result credential is unavailable",
                        )
                    })?
                    .current,
                ApplicationPhase::Running,
            )?,
            _ => {
                return Err(saga_error(
                    ApplicationPhase::Running,
                    "Saga capability credential protocol is unavailable",
                ))
            }
        };
        for descriptor in descriptors {
            descriptor.result_contract_digest = Some(digest.clone());
        }
        Ok(())
    }
}
