use super::RequestResource;
use crate::api::plugin::{AuthContext, AuthPlugin, LoginIdentityContext};
use arc_swap::ArcSwap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// 登录上下文中访问密钥标识的键。
pub(crate) const ACCESS_KEY: &str = "accessKey";
/// 登录上下文中签名密钥的键。
pub(crate) const ACCESS_SECRET: &str = "secretKey";
/// 登录上下文中签名区域的键，存在时启用 v4 派生。
pub(crate) const SIGN_REGION_ID: &str = "signature_region_id";
/// 请求中声明签名算法代次的协议键。
pub const SIGNATURE_VERSION: &str = "signatureVersion";
/// 签名材料中的分组服务名键。
pub const GROUPED_NAME: &str = "groupedName";
/// 携带访问密钥标识的请求头名称。
pub const ACCESS_KEY_HEADER: &str = "Spas-AccessKey";
/// Aliyun RAM 签名协议的 v4 标识。
pub const V4: &str = "v4";
/// v4 签名采用的 HMAC 算法名称。
pub const SIGNATURE_V4_METHOD: &str = "HmacSHA256";
/// 签名作用域中的 Nacos 产品标识。
pub const SIGNATURE_V4_PRODUCE: &str = "mse-nacos";
/// v4 派生签名密钥所用的协议前缀。
pub const PREFIX: &str = "aliyun_v4";
/// v4 派生签名密钥所用的协议终止标识。
pub const CONSTANT: &str = "aliyun_v4_request";
/// 签名作用域采用的年月日格式。
pub const V4_SIGN_DATE_FORMATTER: &str = "%Y%m%d";
/// 签名结果在请求材料中的键。
pub const SIGNATURE_FILED: &str = "signature";
/// 待签名正文在请求材料中的键。
pub const DATA_FILED: &str = "data";
/// 访问密钥标识在请求材料中的键。
pub const AK_FILED: &str = "ak";
/// 签名时间戳的请求头名称。
pub const TIMESTAMP_HEADER: &str = "Timestamp";
/// 签名值的请求头名称。
pub const SIGNATURE_HEADER: &str = "Spas-Signature";
/// 签名作用域中的分组键。
pub const GROUP_KEY: &str = "group";
/// 签名作用域中的租户键。
pub const TENANT_KEY: &str = "tenant";
/// 兼容签名采用的 HMAC 算法名称。
pub const SHA_ENCRYPT: &str = "HmacSHA1";

/// 业务作用：取得 RAM 请求签名绑定的当前 Unix 毫秒时间戳。
///
/// 参数说明: 无。
///
/// 返回：系统时间距 Unix 纪元的毫秒数；时钟早于纪元时产生 panic，不生成无效的负时间签名。
fn get_current_timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time should be after UNIX_EPOCH")
        .as_millis()
}

mod sign_utils {
    use base64::Engine;
    use ring::hmac::{self, Algorithm, HMAC_SHA1_FOR_LEGACY_USE_ONLY};

    /// 业务作用：按选定 HMAC 算法计算认证标签，为 RAM 请求或派生密钥提供签名原语。
    ///
    /// 参数说明：
    /// - `data`：参与认证的原始消息字节，不进行字符转换。
    /// - `key`：HMAC 使用的原始密钥字节，调用方须限定其传播范围。
    /// - `alg`：协议要求的 HMAC 算法，决定输出标签长度。
    ///
    /// 返回：拥有独立存储的签名字节；不校验签名、不发送请求。
    pub fn sign_with_alg(data: &[u8], key: &[u8], alg: Algorithm) -> Vec<u8> {
        let key = hmac::Key::new(alg, key);
        let tag = hmac::sign(&key, data);
        tag.as_ref().to_vec()
    }

    /// 业务作用：将 HMAC 标签编码为 RAM 线上协议使用的标准 Base64 文本。
    ///
    /// 参数说明：
    /// - `data`：本次签名覆盖的消息原始字节。
    /// - `key`：签名密钥或上一层派生密钥的原始字节。
    /// - `alg`：本次协议字段指定的 HMAC 算法。
    ///
    /// 返回：使用标准字母表和填充规则的 Base64 签名，不采用 URL 安全字母表。
    pub fn sign_to_base64_with_alg(data: &[u8], key: &[u8], alg: Algorithm) -> String {
        base64::prelude::BASE64_STANDARD.encode(sign_with_alg(data, key, alg).as_slice())
    }

    /// 业务作用：生成兼容 RAM 请求合同的 HMAC-SHA1 签名文本。
    ///
    /// 参数说明：
    /// - `data`：已按协议拼接的待签名文本，以 UTF-8 原始字节参与计算。
    /// - `key`：共享密钥文本或已编码的派生密钥文本，按当前字符串字节使用。
    ///
    /// 返回：标准 Base64 编码的 HMAC-SHA1 标签；不在此处更改协议算法或资源串格式。
    pub fn sign_with_hmac_sha1(data: &str, key: &str) -> String {
        sign_to_base64_with_alg(
            data.as_bytes(),
            key.as_bytes(),
            HMAC_SHA1_FOR_LEGACY_USE_ONLY,
        )
    }
}

mod spas_adaptor {
    use crate::api::plugin::auth::auth_by_aliyun_ram::{get_current_timestamp, sign_utils};
    use crate::api::plugin::{SIGNATURE_HEADER, TIMESTAMP_HEADER};
    use std::collections::HashMap;

    /// 业务作用：为配置请求构造绑定资源与当前时刻的 RAM 签名头。
    ///
    /// 参数说明：
    /// - `resource`：命名空间和分组派生的资源串；为空时只签时间戳，否则以加号拼接资源与时刻。
    /// - `secret_key`：用于 HMAC-SHA1 的密钥文本；为空时不生成签名头。
    ///
    /// 返回：始终包含 Timestamp，非空密钥时再包含 Spas-Signature；时钟早于 Unix 纪元时产生 panic。
    pub fn get_sign_header_for_resource(
        resource: &str,
        secret_key: &str,
    ) -> HashMap<String, String> {
        let mut headers = HashMap::default();
        let timestamp = get_current_timestamp();
        headers.insert(TIMESTAMP_HEADER.to_string(), timestamp.to_string());
        if !secret_key.is_empty() {
            let sign = if resource.is_empty() {
                sign_utils::sign_with_hmac_sha1(&timestamp.to_string(), secret_key)
            } else {
                sign_utils::sign_with_hmac_sha1(
                    format!("{}+{}", resource, timestamp).as_str(),
                    secret_key,
                )
            };
            headers.insert(SIGNATURE_HEADER.to_string(), sign.to_string());
        }
        headers
    }
}

mod calculate_v4_signing_key_util {
    use crate::api::plugin::{PREFIX, V4_SIGN_DATE_FORMATTER};
    use chrono::Utc;
    use ring::hmac::{self, Algorithm};

    use super::{
        CONSTANT, SIGNATURE_V4_PRODUCE,
        sign_utils::{self, sign_to_base64_with_alg},
    };

    /// 业务作用：在 RAM v4 派生链中将主密钥约束到指定签名日期。
    ///
    /// 参数说明：
    /// - `secret`：原始访问密钥，先附加协议前缀后作为 HMAC 密钥。
    /// - `date`：签名日期文本，调用方须使用协议约定的年月日格式。
    /// - `alg`：派生链统一使用的 HMAC 算法。
    ///
    /// 返回：日期作用域的派生密钥字节；不保存密钥或检查日期有效性。
    pub fn first_signing_key(secret: &str, date: &str, alg: Algorithm) -> Vec<u8> {
        sign_utils::sign_with_alg(
            date.as_bytes(),
            format!("{}{}", PREFIX, secret).as_bytes(),
            alg,
        )
    }

    /// 业务作用：将日期派生密钥继续约束到目标区域，防止不同区域共用同一派生结果。
    ///
    /// 参数说明：
    /// - `secret`：用于建立日期派生层的原始访问密钥。
    /// - `date`：本次签名的日期文本，须与后续请求合同一致。
    /// - `region`：目标区域标识，以原始字节参与第二层派生。
    /// - `alg`：日期层与区域层共同使用的 HMAC 算法。
    ///
    /// 返回：区域作用域的派生密钥字节，不验证区域名称是否存在。
    pub fn region_signing_key(secret: &str, date: &str, region: &str, alg: Algorithm) -> Vec<u8> {
        let vec = first_signing_key(secret, date, alg);
        sign_utils::sign_with_alg(region.as_bytes(), vec.as_slice(), alg)
    }

    /// 业务作用：完成日期、区域、产品及终止标识的 RAM v4 派生链。
    ///
    /// 参数说明：
    /// - `secret`：派生链的原始访问密钥，不进入返回文本。
    /// - `date`：签名日期文本，限定派生结果的日期作用域。
    /// - `region`：签名区域标识，限定派生结果的地域作用域。
    /// - `product_code`：目标产品标识，防止同一派生密钥跨产品复用。
    /// - `alg`：各层派生统一使用的 HMAC 算法。
    ///
    /// 返回：最终派生密钥的标准 Base64 文本，仍属秘密材料，不能写入日志。
    pub fn final_signing_key_string(
        secret: &str,
        date: &str,
        region: &str,
        product_code: &str,
        alg: Algorithm,
    ) -> String {
        let second_sign_key = region_signing_key(secret, date, region, alg);
        let third_signing_key =
            sign_utils::sign_with_alg(product_code.as_bytes(), &second_sign_key, alg);
        sign_to_base64_with_alg(CONSTANT.as_bytes(), &third_signing_key, alg)
    }

    /// 业务作用：按当前 UTC 日期及 Nacos 产品合同生成 RAM v4 派生密钥。
    ///
    /// 参数说明：
    /// - `secret`：本地已加载的原始访问密钥。
    /// - `region`：本次请求配置的区域标识，不在此处推断或补全。
    ///
    /// 返回：HMAC-SHA256 派生的 Base64 密钥文本，每次调用重新读取日期，不缓存跨日结果。
    pub fn final_signing_key_string_with_default_info(secret: &str, region: &str) -> String {
        let sign_date = Utc::now().format(V4_SIGN_DATE_FORMATTER).to_string();
        final_signing_key_string(
            secret,
            &sign_date,
            region,
            SIGNATURE_V4_PRODUCE,
            hmac::HMAC_SHA256,
        )
    }
}

/// 使用本地 RAM 凭据逐请求签名的插件，周期 login 不向远端换取令牌。
pub(crate) struct AliyunRamAuthPlugin {
    /// 供接收端查找签名身份的访问密钥标识。
    access_key: ArcSwap<Option<String>>,
    /// 本地 HMAC 签名材料，不随请求明文发送。
    secret_key: ArcSwap<Option<String>>,
    /// 可选 v4 派生区域，未配置时使用兼容签名材料。
    signature_region_id: ArcSwap<Option<String>>,
    /// 首轮材料已保存的标记，不作为并发初始化锁。
    initialized: ArcSwap<bool>,
}

impl AliyunRamAuthPlugin {
    /// 业务作用：建立尚未加载 RAM 凭据的认证插件，等待首次 login 提供签名材料。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：密钥及区域均为空、初始化标记为假的插件；调用方应先完成 login 再请求签名。
    pub(crate) fn default() -> Self {
        Self {
            access_key: ArcSwap::from_pointee(None),
            secret_key: ArcSwap::from_pointee(None),
            signature_region_id: ArcSwap::from_pointee(None),
            initialized: ArcSwap::from_pointee(false),
        }
    }

    /// 业务作用：为服务发现请求生成绑定分组服务名与当前时刻的 RAM 身份材料。
    ///
    /// 参数说明：
    /// - `resource`：发现请求的服务及分组描述；缺少具体资源时不生成签名。
    ///
    /// 返回：包含签名、待签原文与访问密钥标识的上下文；配置区域时另带 v4 标识。缺少已初始化密钥或系统时钟非法时产生 panic。
    fn get_login_identify_for_naming(&self, resource: RequestResource) -> LoginIdentityContext {
        Self::get_sign_data(&resource)
            .map(|sign_data| {
                let mut res = LoginIdentityContext::default();
                let mut signature_key = self
                    .secret_key
                    .load()
                    .as_ref()
                    .as_ref()
                    .expect("Secret key should be initialized before getting login identity")
                    .clone();
                if let Some(region_id) = self.signature_region_id.load().as_ref() {
                    signature_key =
                        calculate_v4_signing_key_util::final_signing_key_string_with_default_info(
                            self.secret_key.load().as_ref().as_ref().expect(
                                "Secret key should be initialized before calculating signature",
                            ),
                            region_id,
                        );
                    res = res.add_context(SIGNATURE_VERSION, V4);
                }
                let signature = sign_utils::sign_with_hmac_sha1(&sign_data, &signature_key);
                res = res.add_context(SIGNATURE_FILED, signature);
                res = res.add_context(DATA_FILED, sign_data);
                res =
                    res.add_context(
                        AK_FILED,
                        self.access_key.load().as_ref().as_ref().expect(
                            "Access key should be initialized before getting login identity",
                        ),
                    );
                res
            })
            .unwrap_or_default()
    }

    /// 业务作用：按发现协议生成本次 RAM 签名的资源原文。
    ///
    /// 参数说明：
    /// - `resource`：用于派生分组服务名的请求描述，不以 namespace 拼接发现资源。
    ///
    /// 返回：资源缺失时为空；资源串为空时只返回时间戳，其余情况返回时间戳与分组服务名的连接串。
    fn get_sign_data(resource: &RequestResource) -> Option<String> {
        Self::get_grouped_service_name(resource).map(|grouped_service_name| {
            let timestamp = get_current_timestamp();
            if grouped_service_name.is_empty() {
                format!("{}", timestamp)
            } else {
                format!("{}@@{}", timestamp, grouped_service_name)
            }
        })
    }

    /// 业务作用：确定发现签名使用的分组服务名，避免对已有分组前缀重复拼接。
    ///
    /// 参数说明：
    /// - `resource`：包含可选服务名及分组的请求描述。
    ///
    /// 返回：无服务名时为空；服务名已含 @@ 时原样返回，否则存在分组则拼接，不存在分组则保留服务名。
    fn get_grouped_service_name(resource: &RequestResource) -> Option<String> {
        resource.resource.as_ref().map(|service_name| {
            if service_name.contains("@@") {
                service_name.clone()
            } else if let Some(group) = &resource.group {
                format!("{}@@{}", group, service_name)
            } else {
                service_name.clone()
            }
        })
    }

    /// 业务作用：为配置请求生成访问密钥标识、时间戳及资源签名头。
    ///
    /// 参数说明：
    /// - `resource`：配置请求的命名空间与分组，用于限定签名资源，不使用具体 dataId。
    ///
    /// 返回：当前密钥签出的身份上下文；存在区域配置时先派生 v4 密钥。未加载必需密钥或时钟非法时产生 panic。
    fn get_login_identify_for_config(&self, resource: RequestResource) -> LoginIdentityContext {
        let mut res = LoginIdentityContext::default();
        let mut signature_key = self
            .secret_key
            .load()
            .as_ref()
            .as_ref()
            .expect("Secret key should be initialized before getting login identity")
            .clone();
        if let Some(region_id) = self.signature_region_id.load().as_ref().as_ref() {
            signature_key =
                calculate_v4_signing_key_util::final_signing_key_string_with_default_info(
                    self.secret_key
                        .load()
                        .as_ref()
                        .as_ref()
                        .expect("Secret key should be initialized before calculating v4 signature"),
                    region_id,
                );
            res = res.add_context(SIGNATURE_VERSION, V4);
        }
        res = res.add_context(
            ACCESS_KEY_HEADER,
            self.access_key
                .load()
                .as_ref()
                .as_ref()
                .expect("Access key should be initialized before getting login identity"),
        );
        res = res.add_contexts(spas_adaptor::get_sign_header_for_resource(
            Self::get_resource_for_config(&resource).as_str(),
            signature_key.as_str(),
        ));
        res
    }

    /// 业务作用：按配置认证合同组合命名空间与分组，提供签名的资源边界。
    ///
    /// 参数说明：
    /// - `resource`：当前请求的资源描述；只判断 namespace 与 group 是否为空字符串，不裁剪空白。
    ///
    /// 返回：两者非空时以加号连接，仅有一项时保留该项，均缺失或为空时返回空串。
    fn get_resource_for_config(resource: &RequestResource) -> String {
        let namespace = resource
            .namespace
            .as_ref()
            .filter(|value| !value.is_empty());
        let group = resource.group.as_ref().filter(|value| !value.is_empty());
        if let (Some(n), Some(g)) = (namespace, group) {
            format!("{}+{}", n, g)
        } else if let Some(g) = group {
            g.to_string()
        } else if let Some(n) = namespace {
            n.to_string()
        } else {
            String::from("")
        }
    }
}

#[async_trait::async_trait]
impl AuthPlugin for AliyunRamAuthPlugin {
    /// 业务作用：从本地认证上下文加载 RAM 签名材料，不进行远端登录。
    ///
    /// 参数说明：
    /// - `auth_context`：必须包含 accessKey 与 secretKey，可选 signature_region_id；后续已初始化的调用不重新加载。
    ///
    /// 返回：按访问标识、密钥、可选区域、完成标记的顺序保存状态；缺少必需参数产生 panic。该标记不为并发首次初始化提供互斥。
    async fn login(&self, _: Arc<Vec<String>>, auth_context: Arc<AuthContext>) {
        // 周期调度复用首轮材料；已初始化后不能把一次普通刷新当作密钥热切换。
        if *self.initialized.load().as_ref() {
            return;
        }

        let ak = auth_context
            .params
            .get(ACCESS_KEY)
            .expect("Init aliyun ram auth plugin error, missing accessKey")
            .to_owned();
        let sk = auth_context
            .params
            .get(ACCESS_SECRET)
            .expect("Init aliyun ram auth plugin error, missing secretKey")
            .to_owned();
        tracing::info!("Initialize using ak: {}", ak);
        self.access_key.store(Arc::new(Some(ak)));
        self.secret_key.store(Arc::new(Some(sk)));
        if let Some(signature_region_id) = auth_context.params.get(SIGN_REGION_ID) {
            self.signature_region_id
                .store(Arc::new(Some(signature_region_id.to_string())));
            tracing::info!(
                "Initializing using signature region: {}",
                signature_region_id
            );
        }
        // 所需材料保存后才发布完成标记，后续顺序调用可安全跳过重复加载。
        self.initialized.store(Arc::new(true));
    }

    /// 业务作用：按请求类别选择发现或配置签名合同，避免混用两种资源编码方式。
    ///
    /// 参数说明：
    /// - `resource`：请求类别与目标范围，类别须为 Naming 或 Config。
    ///
    /// 返回：相应类别的签名上下文；未知类别返回空身份，所选签名路径缺少必需材料时产生 panic。
    fn get_login_identity(&self, resource: RequestResource) -> LoginIdentityContext {
        tracing::trace!("signature to resource {:?}", resource);
        match resource.request_type.as_str() {
            "Naming" => self.get_login_identify_for_naming(resource),
            "Config" => self.get_login_identify_for_config(resource),
            _ => LoginIdentityContext::default(),
        }
    }
}
