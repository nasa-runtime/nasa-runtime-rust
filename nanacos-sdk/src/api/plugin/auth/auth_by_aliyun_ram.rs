use super::RequestResource;
use crate::api::plugin::{AuthContext, AuthPlugin, LoginIdentityContext};
use arc_swap::ArcSwap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const ACCESS_KEY: &str = "accessKey";
pub(crate) const ACCESS_SECRET: &str = "secretKey";
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

fn get_current_timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time should be after UNIX_EPOCH")
        .as_millis()
}

mod sign_utils {
    use base64::Engine;
    use ring::hmac::{self, Algorithm, HMAC_SHA1_FOR_LEGACY_USE_ONLY};

    pub fn sign_with_alg(data: &[u8], key: &[u8], alg: Algorithm) -> Vec<u8> {
        let key = hmac::Key::new(alg, key);
        let tag = hmac::sign(&key, data);
        tag.as_ref().to_vec()
    }

    pub fn sign_to_base64_with_alg(data: &[u8], key: &[u8], alg: Algorithm) -> String {
        base64::prelude::BASE64_STANDARD.encode(sign_with_alg(data, key, alg).as_slice())
    }

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

    pub fn first_signing_key(secret: &str, date: &str, alg: Algorithm) -> Vec<u8> {
        sign_utils::sign_with_alg(
            date.as_bytes(),
            format!("{}{}", PREFIX, secret).as_bytes(),
            alg,
        )
    }

    pub fn region_signing_key(secret: &str, date: &str, region: &str, alg: Algorithm) -> Vec<u8> {
        let vec = first_signing_key(secret, date, alg);
        sign_utils::sign_with_alg(region.as_bytes(), vec.as_slice(), alg)
    }

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

pub(crate) struct AliyunRamAuthPlugin {
    access_key: ArcSwap<Option<String>>,
    secret_key: ArcSwap<Option<String>>,
    signature_region_id: ArcSwap<Option<String>>,
    initialized: ArcSwap<bool>,
}

impl AliyunRamAuthPlugin {
    pub(crate) fn default() -> Self {
        Self {
            access_key: ArcSwap::from_pointee(None),
            secret_key: ArcSwap::from_pointee(None),
            signature_region_id: ArcSwap::from_pointee(None),
            initialized: ArcSwap::from_pointee(false),
        }
    }

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
    /// no need to login
    async fn login(&self, _: Arc<Vec<String>>, auth_context: Arc<AuthContext>) {
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
        self.initialized.store(Arc::new(true));
    }

    /// Get the [`LoginIdentityContext`].
    fn get_login_identity(&self, resource: RequestResource) -> LoginIdentityContext {
        tracing::trace!("signature to resource {:?}", resource);
        match resource.request_type.as_str() {
            "Naming" => self.get_login_identify_for_naming(resource),
            "Config" => self.get_login_identify_for_config(resource),
            _ => LoginIdentityContext::default(),
        }
    }
}
