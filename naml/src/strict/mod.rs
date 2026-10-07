//! 有界配置装配、确定来源计划和默认隐藏原值的诊断。

mod binding;
mod document;
mod environment;
mod error;
mod expression;
mod files;
mod imports;
mod loader;
mod path;
mod policy;
#[cfg(feature = "watch")]
mod watcher;

pub use binding::bind;
pub use document::{ParsedDocument, SourceDocument};
pub use environment::EnvironmentSnapshot;
pub use error::{ConfigError, ErrorKind, Result, SourcePosition};
pub use expression::{resolve, resolve_selected, Resolution};
pub use files::{expand_pattern, read_source, FileEvidence, ReadSource};
pub use imports::{natural_filename_cmp, parse_imports_checked, FilePattern};
pub use loader::{
    CheckReport, ConfigLoader, LoadedConfig, PreparedLoad, SourceKind, SourceRecord, WatchPlan,
};
pub use path::{ConfigPath, PathSegment};
pub use policy::{LoadLimits, LoadPolicy, ProfileSelection, ValueHint};
#[cfg(feature = "watch")]
pub use watcher::{ConfigWatchEvent, ConfigWatcher, WatchHealth};

pub(crate) use expression::resolve_compatible;
