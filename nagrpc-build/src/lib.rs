//! NASA Rust gRPC 的唯一 protobuf/codegen 构建入口。
//!
//! # 核心价值
//!
//! 业务只拥有 proto、兼容 baseline 和一次 `compile` 调用；本 crate 统一冻结 HOST vendored
//! `protoc`、tonic/prost/codec 类型身份、descriptor、SHA-256 摘要与 managed service adapter。
//! 生成代码只引用 `nasa::grpc::codegen`，业务不安装系统 `protoc`，也不直接选择底层 codegen 版本。
//!
//! # 构建架构
//!
//! 一次构建先校验输入与规模，再生成完整 descriptor 和 Rust 源码，随后执行兼容检查并按规范
//! protobuf package 树输出 wrapper。WKT 或 `extern_path` package 继续留在 descriptor 中，但不会被
//! 误当成本地 `.rs`；跨 package 相对路径在同一模块树内保持唯一 Rust 类型身份。任一阶段失败都不会
//! 形成可由运行时登记的半份协议产物。
//!
//! 本 crate 不判断字段业务语义，不提供远端 proto registry、breaking-change 审批或发布编排。

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use heck::{ToSnakeCase, ToUpperCamelCase};
use prost::Message;
use prost_types::{
    DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    FileDescriptorSet, ServiceDescriptorProto,
};
use sha2::{Digest, Sha256};

/// generated code 与运行时门面共同遵守的 ABI 标识。
pub const CODEGEN_ABI: u32 = 2;
/// 单次构建允许处理的协议文件上限。
pub const MAX_PROTO_FILES: usize = 256;
/// 单次构建允许生成的 service 总数上限。
pub const MAX_SERVICES: usize = 64;
/// 单个 service 的 method 上限。
pub const MAX_METHODS_PER_SERVICE: usize = 128;
/// 单次构建允许生成的 method 总数上限。
pub const MAX_METHODS_TOTAL: usize = 256;
/// descriptor set 的最大编码字节数。
pub const MAX_DESCRIPTOR_BYTES: usize = 16 * 1024 * 1024;

/// codegen 失败的稳定分类。
#[derive(Debug)]
pub enum Error {
    /// 输入路径、运行时门面路径或协议规模不合法。
    InvalidInput(&'static str),
    /// Cargo 没有提供受管输出目录。
    MissingOutDir,
    /// HOST vendored protoc 不可用。
    ProtocUnavailable,
    /// protoc 或 Rust codegen 失败。
    Compile(io::Error),
    /// descriptor 无法解析或不满足规模合同。
    Descriptor(&'static str),
    /// descriptor baseline 检查发现 wire 或 RPC 合同不兼容。
    Compatibility(&'static str),
    /// 生成文件无法按稳定门面完成归一化。
    GeneratedOutput(io::Error),
}

impl fmt::Display for Error {
    /// 业务作用：输出不包含工作目录、环境变量值或协议内容的稳定构建错误。
    ///
    /// 参数说明：
    /// - `formatter`: 接收错误分类的格式化目标。
    ///
    /// 返回：成功写入稳定分类时完成，否则透传格式化失败。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidInput(reason) => *reason,
            Self::MissingOutDir => "Cargo OUT_DIR is unavailable",
            Self::ProtocUnavailable => "HOST vendored protoc is unavailable",
            Self::Compile(_) => "protobuf compilation failed",
            Self::Descriptor(reason) => *reason,
            Self::Compatibility(rule) => rule,
            Self::GeneratedOutput(_) => "generated Rust output could not be normalized",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for Error {
    /// 业务作用：保留 I/O 错误链供本地诊断，同时避免稳定展示文本拼入敏感路径。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在底层 I/O 原因时返回该错误，否则返回空。
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Compile(error) | Self::GeneratedOutput(error) => Some(error),
            _ => None,
        }
    }
}

/// 业务作用：用稳定默认值编译一个 proto 文件及其同目录导入。
///
/// 参数说明：
/// - `proto`: 需要生成 client、server 与 descriptor 的协议文件。
///
/// 返回：生成物完整写入 Cargo `OUT_DIR` 时成功；输入、protoc、descriptor 或输出归一化失败时返回错误。
pub fn compile(proto: impl AsRef<Path>) -> Result<(), Error> {
    let proto = proto.as_ref();
    let include = proto
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Builder::new().compile(&[proto], &[include])
}

/// 业务作用：在不运行 protoc 的场景比较两份 descriptor set，供协议发布流水线复用同一兼容规则。
///
/// 参数说明：
/// - `baseline`: 协议所有者批准的旧 descriptor bytes。
/// - `candidate`: 待发布的新 descriptor bytes。
///
/// 返回：两份 descriptor 合法且候选保持既有 wire/RPC 合同时成功；否则返回 descriptor 或稳定兼容规则。
pub fn check_descriptor_compatibility(baseline: &[u8], candidate: &[u8]) -> Result<(), Error> {
    if baseline.is_empty()
        || candidate.is_empty()
        || baseline.len() > MAX_DESCRIPTOR_BYTES
        || candidate.len() > MAX_DESCRIPTOR_BYTES
    {
        return Err(Error::Descriptor(
            "descriptor size is outside the managed boundary",
        ));
    }
    let baseline = FileDescriptorSet::decode(baseline)
        .map_err(|_| Error::Descriptor("baseline descriptor set is malformed"))?;
    let candidate = FileDescriptorSet::decode(candidate)
        .map_err(|_| Error::Descriptor("descriptor set is malformed"))?;
    validate_descriptor(&baseline)?;
    validate_descriptor(&candidate)?;
    validate_compatibility(&baseline, &candidate)
}

/// 受管 codegen 的加法配置入口。
#[derive(Debug, Clone)]
pub struct Builder {
    runtime_path: String,
    build_client: bool,
    build_server: bool,
    descriptor_baseline: Option<PathBuf>,
}

impl Default for Builder {
    /// 业务作用：创建指向 `nasa::grpc`、同时生成 client/server 的安全构建配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未读取协议或写入输出目录的 builder。
    fn default() -> Self {
        Self {
            runtime_path: "::nasa::grpc".to_owned(),
            build_client: true,
            build_server: true,
            descriptor_baseline: None,
        }
    }
}

impl Builder {
    /// 业务作用：创建使用稳定 NASA 门面的 codegen builder。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：client/server 均开启且无构建副作用的 builder。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：为独立使用 `nagrpc` 或依赖重命名场景设置唯一运行时门面路径。
    ///
    /// 参数说明：
    /// - `path`: 由 Rust 标识符和 `::` 组成的绝对模块路径。
    ///
    /// 返回：记录候选路径的 builder；最终 `compile` 会再次校验。
    pub fn runtime_path(mut self, path: impl Into<String>) -> Self {
        self.runtime_path = path.into();
        self
    }

    /// 业务作用：控制是否生成调用端代码，不改变 descriptor 与 server 的受管合同。
    ///
    /// 参数说明：
    /// - `enabled`: 是否生成 client。
    ///
    /// 返回：更新 client 生成选择后的 builder。
    pub fn build_client(mut self, enabled: bool) -> Self {
        self.build_client = enabled;
        self
    }

    /// 业务作用：控制是否生成服务端代码；关闭后不生成 managed service adapter。
    ///
    /// 参数说明：
    /// - `enabled`: 是否生成 server。
    ///
    /// 返回：更新 server 生成选择后的 builder。
    pub fn build_server(mut self, enabled: bool) -> Self {
        self.build_server = enabled;
        self
    }

    /// 业务作用：选择协议所有者批准的 descriptor baseline，并在生成前执行 wire/RPC 兼容门禁。
    ///
    /// 参数说明：
    /// - `path`: 已纳入协议 crate 归档的 baseline descriptor 文件。
    ///
    /// 返回：记录 baseline 路径的 builder；路径读取和兼容检查延迟到 `compile`。
    pub fn descriptor_baseline(mut self, path: impl Into<PathBuf>) -> Self {
        self.descriptor_baseline = Some(path.into());
        self
    }

    /// 业务作用：以同一 HOST protoc、descriptor 与门面身份编译一组协议。
    ///
    /// 参数说明：
    /// - `protos`: 需要编译的根协议文件，最多 256 个。
    /// - `includes`: 允许 protoc 解析 import 的根目录。
    ///
    /// 返回：所有生成文件归一化并写入 `OUT_DIR` 时成功；路径、规模或生成失败时不返回半成功合同。
    pub fn compile<P>(&self, protos: &[P], includes: &[P]) -> Result<(), Error>
    where
        P: AsRef<Path>,
    {
        validate_runtime_path(&self.runtime_path)?;
        if protos.is_empty() || protos.len() > MAX_PROTO_FILES || includes.is_empty() {
            return Err(Error::InvalidInput(
                "proto/include count is outside the managed boundary",
            ));
        }

        let protos = canonicalize_existing(protos, false)?;
        let includes = canonicalize_existing(includes, true)?;
        let out_dir = env::var_os("OUT_DIR")
            .map(PathBuf::from)
            .ok_or(Error::MissingOutDir)?;
        let out_dir = out_dir.canonicalize().map_err(Error::GeneratedOutput)?;
        let descriptor_path = out_dir.join("nagrpc_descriptor.bin");
        let protoc =
            protoc_bin_vendored::protoc_bin_path().map_err(|_| Error::ProtocUnavailable)?;

        let mut prost_config = prost_build::Config::new();
        prost_config.prost_path(format!("{}::codegen::prost", self.runtime_path));
        prost_config.prost_types_path(format!("{}::codegen::prost_types", self.runtime_path));
        prost_config.protoc_executable(protoc);

        tonic_prost_build::configure()
            .build_client(self.build_client)
            .build_server(self.build_server)
            .build_transport(true)
            .codec_path(format!(
                "{}::codegen::tonic_prost::ProstCodec",
                self.runtime_path
            ))
            .file_descriptor_set_path(&descriptor_path)
            .emit_rerun_if_changed(true)
            .compile_with_config(prost_config, &protos, &includes)
            .map_err(Error::Compile)?;

        let descriptor_bytes = fs::read(&descriptor_path).map_err(Error::GeneratedOutput)?;
        if descriptor_bytes.is_empty() || descriptor_bytes.len() > MAX_DESCRIPTOR_BYTES {
            return Err(Error::Descriptor(
                "descriptor size is outside the managed boundary",
            ));
        }
        let descriptor = FileDescriptorSet::decode(descriptor_bytes.as_slice())
            .map_err(|_| Error::Descriptor("descriptor set is malformed"))?;
        validate_descriptor(&descriptor)?;
        if let Some(baseline) = &self.descriptor_baseline {
            let baseline = baseline
                .canonicalize()
                .map_err(|_| Error::InvalidInput("descriptor baseline does not exist"))?;
            println!("cargo:rerun-if-changed={}", baseline.display());
            let baseline_bytes = fs::read(baseline).map_err(Error::GeneratedOutput)?;
            if baseline_bytes.is_empty() || baseline_bytes.len() > MAX_DESCRIPTOR_BYTES {
                return Err(Error::Descriptor(
                    "baseline descriptor size is outside the managed boundary",
                ));
            }
            check_descriptor_compatibility(&baseline_bytes, &descriptor_bytes)?;
        }
        write_package_outputs(
            &out_dir,
            &descriptor,
            &descriptor_bytes,
            &self.runtime_path,
            self.build_server,
        )?;
        Ok(())
    }
}

/// 业务作用：把存在的输入路径规范化，阻止构建逻辑依赖未解析的相对路径身份。
fn canonicalize_existing<P>(paths: &[P], directories: bool) -> Result<Vec<PathBuf>, Error>
where
    P: AsRef<Path>,
{
    paths
        .iter()
        .map(|path| {
            let canonical = path
                .as_ref()
                .canonicalize()
                .map_err(|_| Error::InvalidInput("a proto/include path does not exist"))?;
            if canonical.is_dir() != directories {
                return Err(Error::InvalidInput("proto/include path has the wrong kind"));
            }
            Ok(canonical)
        })
        .collect()
}

/// 业务作用：限制 generated code 可引用的运行时路径语法，避免把任意 token 注入输出文件。
fn validate_runtime_path(path: &str) -> Result<(), Error> {
    if !path.starts_with("::") {
        return Err(Error::InvalidInput(
            "runtime_path must be an absolute Rust module path",
        ));
    }
    let valid = path[2..].split("::").all(|segment| {
        !segment.is_empty()
            && segment.chars().enumerate().all(|(index, ch)| {
                ch == '_' || ch.is_ascii_alphanumeric() && (index > 0 || !ch.is_ascii_digit())
            })
    });
    if !valid {
        return Err(Error::InvalidInput(
            "runtime_path contains an invalid Rust identifier",
        ));
    }
    Ok(())
}

/// 业务作用：校验 descriptor 中 package/service/method 的规模和完整身份。
fn validate_descriptor(descriptor: &FileDescriptorSet) -> Result<(), Error> {
    if descriptor.file.is_empty() || descriptor.file.len() > MAX_PROTO_FILES {
        return Err(Error::Descriptor(
            "descriptor file count is outside the managed boundary",
        ));
    }
    let mut services = 0_usize;
    let mut methods = 0_usize;
    for file in &descriptor.file {
        let package = file.package.as_deref().unwrap_or_default();
        if package.is_empty() || package.len() > 256 || !valid_proto_name(package) {
            return Err(Error::Descriptor(
                "proto package name is missing or invalid",
            ));
        }
        for service in &file.service {
            services = services
                .checked_add(1)
                .ok_or(Error::Descriptor("service count overflowed"))?;
            if service
                .name
                .as_deref()
                .is_none_or(|name| name.is_empty() || name.len() > 128)
            {
                return Err(Error::Descriptor("service name is missing or too long"));
            }
            if service.method.len() > MAX_METHODS_PER_SERVICE {
                return Err(Error::Descriptor("a service exceeds the method boundary"));
            }
            if service.method.iter().any(|method| {
                method
                    .name
                    .as_deref()
                    .is_none_or(|name| name.is_empty() || name.len() > 128)
            }) {
                return Err(Error::Descriptor("a method name is missing or too long"));
            }
            methods = methods
                .checked_add(service.method.len())
                .ok_or(Error::Descriptor("method count overflowed"))?;
        }
    }
    if services > MAX_SERVICES || methods > MAX_METHODS_TOTAL {
        return Err(Error::Descriptor(
            "service or method total exceeds the managed boundary",
        ));
    }
    Ok(())
}

/// descriptor 兼容检查使用的全限定 symbol 索引。
struct DescriptorIndex<'a> {
    messages: BTreeMap<String, &'a DescriptorProto>,
    enums: BTreeMap<String, &'a EnumDescriptorProto>,
    services: BTreeMap<String, &'a ServiceDescriptorProto>,
}

impl<'a> DescriptorIndex<'a> {
    /// 业务作用：把跨文件与 nested symbol 规范化为 protobuf 全限定名，供 baseline 与候选确定性比较。
    ///
    /// 参数说明：
    /// - `descriptor`: 已通过规模与名称校验的 descriptor set。
    ///
    /// 返回：message、enum 与 service 的全限定索引；重复 symbol 返回兼容检查错误。
    fn new(descriptor: &'a FileDescriptorSet) -> Result<Self, Error> {
        let mut index = Self {
            messages: BTreeMap::new(),
            enums: BTreeMap::new(),
            services: BTreeMap::new(),
        };
        for file in &descriptor.file {
            let package = file.package.as_deref().unwrap_or_default();
            for message in &file.message_type {
                index.insert_message(package, message)?;
            }
            for enumeration in &file.enum_type {
                let name = qualified(package, enumeration.name.as_deref().unwrap_or_default());
                if index.enums.insert(name, enumeration).is_some() {
                    return Err(Error::Compatibility("GRPC_COMPAT_DUPLICATE_SYMBOL"));
                }
            }
            for service in &file.service {
                let name = qualified(package, service.name.as_deref().unwrap_or_default());
                if index.services.insert(name, service).is_some() {
                    return Err(Error::Compatibility("GRPC_COMPAT_DUPLICATE_SYMBOL"));
                }
            }
        }
        Ok(index)
    }

    /// 业务作用：递归登记 message 及其 nested message/enum，保留 protobuf lexical scope。
    ///
    /// 参数说明：
    /// - `scope`: package 或父 message 的全限定 scope。
    /// - `message`: 当前 message descriptor。
    ///
    /// 返回：整棵 nested symbol 唯一时成功，否则返回重复 symbol 分类。
    fn insert_message(&mut self, scope: &str, message: &'a DescriptorProto) -> Result<(), Error> {
        let name = qualified(scope, message.name.as_deref().unwrap_or_default());
        if self.messages.insert(name.clone(), message).is_some() {
            return Err(Error::Compatibility("GRPC_COMPAT_DUPLICATE_SYMBOL"));
        }
        for nested in &message.nested_type {
            self.insert_message(&name, nested)?;
        }
        for enumeration in &message.enum_type {
            let enum_name = qualified(&name, enumeration.name.as_deref().unwrap_or_default());
            if self.enums.insert(enum_name, enumeration).is_some() {
                return Err(Error::Compatibility("GRPC_COMPAT_DUPLICATE_SYMBOL"));
            }
        }
        Ok(())
    }
}

/// 业务作用：比较协议 baseline 与候选 descriptor，拒绝会改变既有 wire 或 RPC cardinality 的变更。
///
/// 参数说明：
/// - `baseline`: 协议所有者批准并随 contract crate 归档的旧 descriptor。
/// - `candidate`: 本次 proto 生成的新 descriptor。
///
/// 返回：旧 message/enum/service/method 仍兼容时成功；删除、复用或类型变化返回稳定规则编号。
fn validate_compatibility(
    baseline: &FileDescriptorSet,
    candidate: &FileDescriptorSet,
) -> Result<(), Error> {
    let old = DescriptorIndex::new(baseline)?;
    let new = DescriptorIndex::new(candidate)?;
    for (name, old_message) in old.messages {
        let new_message = new
            .messages
            .get(&name)
            .ok_or(Error::Compatibility("GRPC_COMPAT_MESSAGE_REMOVED"))?;
        validate_message_compatibility(old_message, new_message)?;
    }
    for (name, old_enum) in old.enums {
        let new_enum = new
            .enums
            .get(&name)
            .ok_or(Error::Compatibility("GRPC_COMPAT_ENUM_REMOVED"))?;
        validate_enum_compatibility(old_enum, new_enum)?;
    }
    for (name, old_service) in old.services {
        let new_service = new
            .services
            .get(&name)
            .ok_or(Error::Compatibility("GRPC_COMPAT_SERVICE_REMOVED"))?;
        for old_method in &old_service.method {
            let old_name = old_method.name.as_deref().unwrap_or_default();
            let new_method = new_service
                .method
                .iter()
                .find(|method| method.name.as_deref() == Some(old_name))
                .ok_or(Error::Compatibility("GRPC_COMPAT_METHOD_REMOVED"))?;
            if old_method.input_type != new_method.input_type
                || old_method.output_type != new_method.output_type
            {
                return Err(Error::Compatibility("GRPC_COMPAT_RPC_TYPE_CHANGED"));
            }
            if old_method.client_streaming() != new_method.client_streaming()
                || old_method.server_streaming() != new_method.server_streaming()
            {
                return Err(Error::Compatibility("GRPC_COMPAT_RPC_CARDINALITY_CHANGED"));
            }
        }
    }
    Ok(())
}

/// 业务作用：逐字段号检查类型身份，并要求删除字段显式 reserve 原编号或名称。
///
/// 参数说明：
/// - `old`: baseline message。
/// - `new`: 同一全限定名的候选 message。
///
/// 返回：既有字段保留等价 wire 合同或被 reserve 时成功，否则返回稳定规则编号。
fn validate_message_compatibility(
    old: &DescriptorProto,
    new: &DescriptorProto,
) -> Result<(), Error> {
    for old_field in &old.field {
        let number = old_field.number.unwrap_or_default();
        if let Some(new_field) = new.field.iter().find(|field| field.number == Some(number)) {
            if !field_contract_equal(old_field, new_field) {
                return Err(Error::Compatibility("GRPC_COMPAT_FIELD_REUSED"));
            }
            continue;
        }
        let name_reserved = old_field
            .name
            .as_ref()
            .is_some_and(|name| new.reserved_name.contains(name));
        let number_reserved = new.reserved_range.iter().any(|range| {
            range.start.is_some_and(|start| number >= start)
                && range.end.is_some_and(|end| number < end)
        });
        if !name_reserved && !number_reserved {
            return Err(Error::Compatibility("GRPC_COMPAT_FIELD_NOT_RESERVED"));
        }
    }
    Ok(())
}

/// 业务作用：比较同一字段号的 protobuf 类型、重复性、oneof 与 presence 合同。
///
/// 参数说明：
/// - `old`: baseline 字段。
/// - `new`: 使用同一字段号的候选字段。
///
/// 返回：wire 与生成 API 身份未变化时为 `true`。
fn field_contract_equal(old: &FieldDescriptorProto, new: &FieldDescriptorProto) -> bool {
    old.name == new.name
        && old.label == new.label
        && old.r#type == new.r#type
        && old.type_name == new.type_name
        && old.oneof_index == new.oneof_index
        && old.proto3_optional == new.proto3_optional
}

/// 业务作用：逐枚举数字检查名称身份，并要求删除数字显式 reserve 数字或名称。
///
/// 参数说明：
/// - `old`: baseline enum。
/// - `new`: 同一全限定名的候选 enum。
///
/// 返回：既有数字含义不变或被 reserve 时成功，否则返回稳定规则编号。
fn validate_enum_compatibility(
    old: &EnumDescriptorProto,
    new: &EnumDescriptorProto,
) -> Result<(), Error> {
    for old_value in &old.value {
        let number = old_value.number.unwrap_or_default();
        if let Some(new_value) = new.value.iter().find(|value| value.number == Some(number)) {
            if old_value.name != new_value.name {
                return Err(Error::Compatibility("GRPC_COMPAT_ENUM_NUMBER_REUSED"));
            }
            continue;
        }
        let name_reserved = old_value
            .name
            .as_ref()
            .is_some_and(|name| new.reserved_name.contains(name));
        let number_reserved = new.reserved_range.iter().any(|range| {
            range.start.is_some_and(|start| number >= start)
                && range.end.is_some_and(|end| number <= end)
        });
        if !name_reserved && !number_reserved {
            return Err(Error::Compatibility("GRPC_COMPAT_ENUM_NOT_RESERVED"));
        }
    }
    Ok(())
}

/// 业务作用：拼接 protobuf package 或 nested scope 与局部 symbol 名称。
///
/// 参数说明：
/// - `scope`: package 或父 message 名称。
/// - `name`: 当前局部 symbol 名称。
///
/// 返回：不带起始点的 protobuf 全限定名。
fn qualified(scope: &str, name: &str) -> String {
    if scope.is_empty() {
        name.to_owned()
    } else {
        format!("{scope}.{name}")
    }
}

/// 业务作用：确认 protobuf package 只含可稳定映射到 Rust 输出文件的标识符段。
fn valid_proto_name(name: &str) -> bool {
    name.split('.').all(|segment| {
        !segment.is_empty()
            && segment.chars().enumerate().all(|(index, ch)| {
                ch == '_' || ch.is_ascii_alphanumeric() && (index > 0 || !ch.is_ascii_digit())
            })
    })
}

/// generated package 的规范 Rust 模块树节点。
#[derive(Default)]
struct GeneratedPackageNode {
    package: Option<String>,
    children: BTreeMap<String, GeneratedPackageNode>,
}

impl GeneratedPackageNode {
    /// 业务作用：把一个实际生成 Rust 源码的 protobuf package 插入规范模块树。
    ///
    /// 参数说明：
    /// - `module`: prost-build 对 protobuf package 完成转义和 snake_case 后的模块路径。
    /// - `package`: descriptor 中保持原样的 protobuf package 名称。
    ///
    /// 返回：无；同一模块路径只记录一个 package，重复写入保持最后一次等价值。
    fn insert(&mut self, module: &prost_build::Module, package: &str) {
        let mut current = self;
        for part in module.parts() {
            current = current.children.entry(part.to_owned()).or_default();
        }
        current.package = Some(package.to_owned());
    }

    /// 业务作用：生成与 prost-build 相对类型路径一致的嵌套模块，使跨 package 引用共享同一类型身份。
    ///
    /// 参数说明：
    /// - `output`: 接收 Rust 模块源码的缓冲区。
    /// - `depth`: 当前节点的缩进深度。
    ///
    /// 返回：无；每个叶节点包含自身 descriptor 常量和归一化 generated source。
    fn render(&self, output: &mut String, depth: usize) {
        use std::fmt::Write as _;

        let indent = "    ".repeat(depth);
        if let Some(package) = &self.package {
            let _ = writeln!(
                output,
                "{indent}/// 当前 protobuf package 所属构建的完整 descriptor set。"
            );
            let _ = writeln!(
                output,
                "{indent}pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!(\"OUT_DIR\"), \"/{package}.descriptor.bin\"));"
            );
            let _ = writeln!(
                output,
                "{indent}/// 当前 protobuf package descriptor set 的小写 SHA-256 摘要。"
            );
            let _ = writeln!(
                output,
                "{indent}pub const FILE_DESCRIPTOR_SHA256: &str = include_str!(concat!(env!(\"OUT_DIR\"), \"/{package}.descriptor.sha256\"));"
            );
            let _ = writeln!(
                output,
                "{indent}include!(concat!(env!(\"OUT_DIR\"), \"/{package}.generated.rs\"));"
            );
        }
        for (module, child) in &self.children {
            let _ = writeln!(output, "{indent}pub mod {module} {{");
            child.render(output, depth + 1);
            let _ = writeln!(output, "{indent}}}");
        }
    }
}

/// 业务作用：按 package 汇总 service，并为每个本地生成 package 写入规范模块树、门面路径和 managed adapter。
///
/// 参数说明：
/// - `out_dir`: Cargo 为本次构建分配的受管输出目录。
/// - `descriptor`: protoc 生成并通过规模校验的完整 descriptor set。
/// - `descriptor_bytes`: reflection 与内容摘要共用的原始 descriptor bytes。
/// - `runtime_path`: generated code 唯一允许引用的运行时门面绝对路径。
/// - `build_server`: 是否为生成的 server 追加 managed registry 适配。
///
/// 返回：所有实际生成 Rust 源码的 package 均完成归一化时成功；WKT 或 extern_path 映射的 package
/// 没有本地 `.rs` 时跳过，任何已存在输出的读写失败返回稳定生成错误。
fn write_package_outputs(
    out_dir: &Path,
    descriptor: &FileDescriptorSet,
    descriptor_bytes: &[u8],
    runtime_path: &str,
    build_server: bool,
) -> Result<(), Error> {
    use std::fmt::Write as _;

    let mut packages: BTreeMap<&str, Vec<&FileDescriptorProto>> = BTreeMap::new();
    for file in &descriptor.file {
        let package = file
            .package
            .as_deref()
            .ok_or(Error::Descriptor("proto package name is missing"))?;
        packages.entry(package).or_default().push(file);
    }
    let digest = Sha256::digest(descriptor_bytes);
    let mut digest_hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(&mut digest_hex, "{byte:02x}");
    }

    let mut generated = Vec::new();
    let mut module_tree = GeneratedPackageNode::default();
    for (package, files) in packages {
        let module = prost_build::Module::from_protobuf_package_name(package);
        let generated_file = out_dir.join(module.to_file_name_or("_.default"));
        // WKT 和显式 extern_path package 只存在于 descriptor 中，其 Rust 类型由稳定外部映射提供。
        // 它们没有本地源码，不能据此判定业务 package 生成失败，也不能制造不可包含的空产物。
        if !generated_file.is_file() {
            continue;
        }
        let descriptor_file = out_dir.join(format!("{package}.descriptor.bin"));
        fs::write(&descriptor_file, descriptor_bytes).map_err(Error::GeneratedOutput)?;
        let digest_file = out_dir.join(format!("{package}.descriptor.sha256"));
        fs::write(digest_file, &digest_hex).map_err(Error::GeneratedOutput)?;
        let source = fs::read_to_string(&generated_file).map_err(Error::GeneratedOutput)?;
        // prost-build 已经通过 `prost_path`/`prost_types_path` 直接生成稳定门面路径；这里只归一化
        // tonic-prost-build 尚未提供配置入口的 tonic crate 身份，避免二次替换门面内部路径。
        let mut normalized =
            source.replace("tonic::", &format!("{runtime_path}::codegen::tonic::"));
        if build_server {
            normalized.push_str(&managed_adapters(files, runtime_path));
        }
        fs::write(out_dir.join(format!("{package}.generated.rs")), normalized)
            .map_err(Error::GeneratedOutput)?;
        module_tree.insert(&module, package);
        generated.push((package.to_owned(), module));
    }

    for (package, module) in generated {
        let mut wrapper = String::new();
        wrapper.push_str(
            "/// 同一次 codegen 的规范 package 模块树；跨 package 字段由该树保持唯一 Rust 类型身份。\n\
             #[doc(hidden)]\n\
             #[allow(dead_code)]\n\
             pub mod __nagrpc_packages {\n",
        );
        module_tree.render(&mut wrapper, 1);
        wrapper.push_str("}\n");
        let selected = module.parts().collect::<Vec<_>>().join("::");
        writeln!(
            &mut wrapper,
            "pub use self::__nagrpc_packages::{selected}::*;"
        )
        .map_err(|_| {
            Error::GeneratedOutput(io::Error::other("generated wrapper formatting failed"))
        })?;
        fs::write(out_dir.join(format!("{package}.rs")), wrapper)
            .map_err(Error::GeneratedOutput)?;
    }
    Ok(())
}

/// 业务作用：为 descriptor 中每个 generated server 生成消息边界与 registry 的统一适配实现。
fn managed_adapters(files: Vec<&FileDescriptorProto>, runtime_path: &str) -> String {
    let mut output = String::new();
    let mut seen = BTreeSet::new();
    for file in files {
        let package = file.package.as_deref().unwrap_or_default();
        for service in &file.service {
            let Some(proto_name) = service.name.as_deref() else {
                continue;
            };
            let rust_name = proto_name.to_upper_camel_case();
            if !seen.insert(rust_name.clone()) {
                continue;
            }
            let module = rust_name.to_snake_case();
            let mut methods = String::new();
            for method in &service.method {
                let Some(method_name) = method.name.as_deref() else {
                    continue;
                };
                let rpc_type = match (method.client_streaming(), method.server_streaming()) {
                    (false, false) => "Unary",
                    (true, false) => "ClientStreaming",
                    (false, true) => "ServerStreaming",
                    (true, true) => "BidirectionalStreaming",
                };
                methods.push_str(&format!(
                    "{runtime_path}::GrpcMethodDescriptor {{ full_name: \"/{package}.{proto_name}/{method_name}\", rpc_type: {runtime_path}::GrpcMethodType::{rpc_type} }},"
                ));
            }
            output.push_str(&format!(
                "\nconst _: () = assert!({runtime_path}::CODEGEN_ABI == {CODEGEN_ABI});\n\
                 impl<T> {runtime_path}::ManagedGrpcService for {module}_server::{rust_name}Server<T>\n\
                 where T: {module}_server::{rust_name} {{\n\
                     /// 业务作用：返回 generated server 在 descriptor 中声明的稳定 service 名称。\n\
                     ///\n\
                     /// 参数说明：无。\n\
                     ///\n\
                     /// 返回：返回 protobuf 完整 service 名称，供 registry 去重和路由装配。\n\
                     fn service_name(&self) -> &'static str {{\n\
                         <Self as {runtime_path}::codegen::tonic::server::NamedService>::NAME\n\
                     }}\n\
                     /// 业务作用：返回生成本 server 的完整 descriptor 集合。\n\
                     ///\n\
                     /// 参数说明：无。\n\
                     ///\n\
                     /// 返回：返回与 generated code 同次构建的静态 descriptor 字节。\n\
                     fn descriptor_set(&self) -> &'static [u8] {{ FILE_DESCRIPTOR_SET }}\n\
                     /// 业务作用：返回 generated adapter 与运行时共同校验的 codegen ABI。\n\
                     ///\n\
                     /// 参数说明：无。\n\
                     ///\n\
                     /// 返回：返回生成器写入的 ABI 常量。\n\
                     fn codegen_abi(&self) -> u32 {{ {CODEGEN_ABI} }}\n\
                     /// 业务作用：返回 descriptor 封口后的固定 RPC 方法目录。\n\
                     ///\n\
                     /// 参数说明：无。\n\
                     ///\n\
                     /// 返回：返回方法完整路径与四种 RPC 形态组成的静态切片。\n\
                     fn methods(&self) -> &'static [{runtime_path}::GrpcMethodDescriptor] {{\n\
                         &[{methods}]\n\
                     }}\n\
                     /// 业务作用：把 generated server 按统一消息上限和共享策略加入受管路由。\n\
                     ///\n\
                     /// 参数说明：\n\
                     /// - routes：接收受管 service 的 tonic 路由构造器。\n\
                     /// - limits：最终配置确定的编解码消息上限。\n\
                     /// - policy：listener 全部 service 共用的 RPC、stream 与消息预算。\n\
                     ///\n\
                     /// 返回：无返回值；server 成功加入路由后由 listener 取得唯一运行所有权。\n\
                     fn add_to_routes(self: Box<Self>, routes: &mut {runtime_path}::codegen::tonic::service::RoutesBuilder, limits: {runtime_path}::GrpcMessageLimits, policy: {runtime_path}::GrpcServicePolicy) {{\n\
                         let methods = self.methods();\n\
                         routes.add_service({runtime_path}::ManagedService::new((*self).max_decoding_message_size(limits.max_decoding_bytes).max_encoding_message_size(limits.max_encoding_bytes), policy, methods));\n\
                     }}\n\
                 }}\n"
            ));
        }
    }
    output
}
