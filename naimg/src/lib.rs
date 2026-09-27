//! # naimg —— 有界图片压缩与缩放
//!
//! 公开 crate 名为 `naimg`；内部依赖 crates.io 的 `image` 已别名为 `image_crate`，避免使用方误把
//! 底层 codec API 当作本组件稳定合同。
//!
//! 输入统一为内存字节，输出为新的图片字节。`scale` 执行等比缩放；同时给出 width 与 height 时，
//! `keep_aspect_ratio` 决定贴框还是拉伸；只给一个维度时按原图比例推导另一维。quality 范围为
//! 0.0..=1.0，且只影响 JPEG。
//!
//! ## 格式与输出边界
//! - 未指定输出格式时保留输入格式。
//! - 支持 JPEG、PNG、GIF、WebP、BMP、ICO、TIFF、PNM、QOI 与 TGA；其它格式应由上传边界转换。
//! - 不读取 EXIF orientation，带方向标记的图片保持原始像素方向。
//! - RGBA 转 JPEG 时直接丢弃 alpha，透明像素可能呈现为黑色；调用方需要其它底色时应先合成。
//! - 本 crate 不执行文件 I/O、MIME 映射、Content-Type 推断或对象存储操作。
//!
//! ## 资源与失败边界
//! 非法 scale、零尺寸、越界 quality、无法解码的输入以及超过输出像素上限的请求返回
//! [`ImageError`]。重采样和 codec 选择不会承诺与其它实现逐像素一致。

use image_crate::codecs::jpeg::JpegEncoder;
use image_crate::{DynamicImage, ExtendedColorType, ImageEncoder};
use std::io::Cursor;

pub use image_crate::ImageFormat;

/// 缩放过滤器，默认使用 [`Filter::Lanczos3`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Filter {
    /// 最近邻采样，速度最快但锯齿明显，适合像素风或缩略图占位。
    Nearest,
    /// 线性三角采样，速度和质量折中。
    Triangle,
    /// Catmull-Rom 采样，适合保留边缘细节。
    CatmullRom,
    /// 高斯采样，适合平滑缩放。
    Gaussian,
    /// Lanczos3 高质量采样，作为默认值。
    #[default]
    Lanczos3,
}

impl Filter {
    /// 业务作用: 转换为 `image` crate 的过滤器表示。
    ///
    /// 这是内部适配层，避免把第三方 crate 的类型直接暴露到业务配置结构中。
    fn to_image(self) -> image_crate::imageops::FilterType {
        use image_crate::imageops::FilterType as F;
        match self {
            Filter::Nearest => F::Nearest,
            Filter::Triangle => F::Triangle,
            Filter::CatmullRom => F::CatmullRom,
            Filter::Gaussian => F::Gaussian,
            Filter::Lanczos3 => F::Lanczos3,
        }
    }
}

/// 压缩选项；`Default` 表示不缩放、不指定质量并使用 Lanczos3。
#[derive(Debug, Clone, Default)]
pub struct CompressOpts {
    /// outputQuality,0.0–1.0,**仅 JPEG**;`None` = 编码器默认。
    pub quality: Option<f32>,
    /// 等比缩放比例(0–1 缩小,>1 放大)。**与 width/height 互斥,后者优先**。
    pub scale: Option<f64>,
    /// 目标宽。只给 width 时按原图比例等比推导高度。
    pub width: Option<u32>,
    /// 目标高。只给 height(width=None)时按原图比例等比推导宽。
    pub height: Option<u32>,
    /// 仅 width&height 同时给时有意义:`Some(false)` = 强制拉伸(`resize_exact`);`Some(true)`/`None` = 保持比例(`resize`)。
    /// 单维度(只给 width 或只给 height)恒等比,不受本开关影响。
    pub keep_aspect_ratio: Option<bool>,
    /// 重采样过滤器(默认 Lanczos3)。
    pub filter: Filter,
}

/// 图片压缩错误。
#[derive(Debug)]
pub enum ImageError {
    /// 解码输入失败(格式不识别 / 数据损坏)。
    Decode(String),
    /// 编码输出失败。
    Encode(String),
    /// 不支持的操作 / 格式。
    Unsupported(String),
    /// 非法参数。
    InvalidArgument(String),
}

impl core::fmt::Display for ImageError {
    /// 业务作用: 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ImageError::Decode(s) => write!(f, "image decode error: {s}"),
            ImageError::Encode(s) => write!(f, "image encode error: {s}"),
            ImageError::Unsupported(s) => write!(f, "unsupported: {s}"),
            ImageError::InvalidArgument(s) => write!(f, "invalid argument: {s}"),
        }
    }
}

impl std::error::Error for ImageError {}

/// 本 crate 统一 `Result`。
pub type Result<T> = core::result::Result<T, ImageError>;

/// 业务作用：解码输入图片，按目标尺寸或比例缩放，再编码为所选格式。
/// 同时配置宽高时优先使用宽高，否则使用 scale；两者都未配置时保持尺寸。
/// 返回：编码后的图片字节；参数、输入格式、解码或编码失败时返回错误。
///
/// # 参数
/// - `data`: 原始图片编码字节,函数会先识别格式再解码。
/// - `opts`: 压缩和缩放选项,包含质量、比例、目标宽高和重采样过滤器。
/// - `format`: 输出格式覆盖;`None` 保留输入格式,`Some` 强制使用指定格式编码。
pub fn compress(data: &[u8], opts: &CompressOpts, format: Option<ImageFormat>) -> Result<Vec<u8>> {
    validate_opts(opts)?;
    let in_format = image_crate::guess_format(data)
        .map_err(|e| ImageError::Decode(format!("guess_format: {e}")))?;
    let out_format = format.unwrap_or(in_format);

    let img = image_crate::load_from_memory(data)
        .map_err(|e| ImageError::Decode(format!("load: {e}")))?;
    // 防 OOM:resize 目标尺寸(scale/width/height 放大)**不受** image crate 解码 Limits 管辖,
    // 大 scale 或大 width/height 会分配无界像素缓冲。落到 resize 前校验目标像素数不超上界。
    let (tw, th) = resize_target_dims(img.width(), img.height(), opts);
    let target_pixels = u64::from(tw) * u64::from(th);
    if target_pixels > MAX_OUTPUT_PIXELS {
        return Err(ImageError::InvalidArgument(format!(
            "resize target {tw}x{th} ({target_pixels}px) exceeds max {MAX_OUTPUT_PIXELS}px"
        )));
    }
    let resized = resize(img, opts);
    encode(&resized, out_format, opts.quality)
}

/// resize 输出像素上界(防 OOM):100 MP ≈ 400MB RGBA。超过则 [`compress`] 返回 `InvalidArgument`。
const MAX_OUTPUT_PIXELS: u64 = 100_000_000;

/// 业务作用: 计算 resize 后的目标尺寸上界,与 [`resize`] 的分支一致(keep-aspect 的实际输出 ≤ (w,h),取上界足够)。
///
/// # 参数
/// - `src_w`: 输入图片解码后的原始宽度。
/// - `src_h`: 输入图片解码后的原始高度。
/// - `opts`: 压缩和缩放选项。
fn resize_target_dims(src_w: u32, src_h: u32, opts: &CompressOpts) -> (u32, u32) {
    if let (Some(tw), Some(th)) = (opts.width, opts.height) {
        (tw, th)
    } else if let (Some(tw), None) = (opts.width, opts.height) {
        // 只给 width:等比推导高(与 resize 分支一致)。
        let th = ((src_h as u64 * tw as u64) as f64 / src_w.max(1) as f64)
            .round()
            .max(1.0) as u32;
        (tw, th)
    } else if let (None, Some(th)) = (opts.width, opts.height) {
        let tw = ((src_w as u64 * th as u64) as f64 / src_h.max(1) as f64)
            .round()
            .max(1.0) as u32;
        (tw, th)
    } else if let Some(scale) = opts.scale {
        if scale == 1.0 {
            (src_w, src_h)
        } else {
            let nw = ((src_w as f64) * scale).round().max(1.0) as u32;
            let nh = ((src_h as f64) * scale).round().max(1.0) as u32;
            (nw, nh)
        }
    } else {
        (src_w, src_h)
    }
}

/// 业务作用: 便捷:质量 + 等比缩放。
///
/// # 参数
/// - `data`: 原始图片编码字节。
/// - `quality`: 输出质量,范围 `0.0..=1.0`;主要影响 JPEG。
/// - `scale`: 等比缩放比例,必须大于 0。
/// - `format`: 输出格式覆盖;`None` 保留输入格式。
pub fn compress_scale(
    data: &[u8],
    quality: f32,
    scale: f64,
    format: Option<ImageFormat>,
) -> Result<Vec<u8>> {
    let opts = CompressOpts {
        quality: Some(quality),
        scale: Some(scale),
        ..Default::default()
    };
    compress(data, &opts, format)
}

/// 业务作用: 便捷:质量 + 定宽高 + keepAspectRatio。
///
/// # 参数
/// - `data`: 原始图片编码字节。
/// - `quality`: 可选输出质量,范围 `0.0..=1.0`;`None` 使用编码器默认质量。
/// - `width`: 目标宽度,必须大于 0。
/// - `height`: 目标高度,必须大于 0。
/// - `keep_aspect`: 是否保持原始宽高比;`None` 按保持比例处理。
/// - `format`: 输出格式覆盖;`None` 保留输入格式。
pub fn compress_size(
    data: &[u8],
    quality: Option<f32>,
    width: u32,
    height: u32,
    keep_aspect: Option<bool>,
    format: Option<ImageFormat>,
) -> Result<Vec<u8>> {
    let opts = CompressOpts {
        quality,
        width: Some(width),
        height: Some(height),
        keep_aspect_ratio: keep_aspect,
        ..Default::default()
    };
    compress(data, &opts, format)
}

// ==================== 内部 ====================

/// 业务作用：在解码和缩放之前拒绝无效的图片处理参数。
/// quality 必须有限且位于 0..=1，scale 必须有限且大于零，显式宽高必须大于零。
/// 返回：有效参数通过；无效值返回参数错误，不静默改为默认值。
///
/// # 参数
/// - `opts`: 调用方传入的压缩和缩放选项。
fn validate_opts(opts: &CompressOpts) -> Result<()> {
    if let Some(q) = opts.quality {
        if !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return Err(ImageError::InvalidArgument(format!(
                "quality must be in 0.0..=1.0, got {q}"
            )));
        }
    }
    if let Some(s) = opts.scale {
        if !s.is_finite() || s <= 0.0 {
            return Err(ImageError::InvalidArgument(format!(
                "scale must be > 0, got {s}"
            )));
        }
    }
    if opts.width == Some(0) {
        return Err(ImageError::InvalidArgument("width must be > 0".into()));
    }
    if opts.height == Some(0) {
        return Err(ImageError::InvalidArgument("height must be > 0".into()));
    }
    Ok(())
}

/// 业务作用: 调整图片尺寸。
///
/// # 参数
/// - `img`: 已解码的输入图片。
/// - `opts`: 压缩和缩放选项；`width + height` 优先于 `scale`。
fn resize(img: DynamicImage, opts: &CompressOpts) -> DynamicImage {
    let filter = opts.filter.to_image();
    if let (Some(tw), Some(th)) = (opts.width, opts.height) {
        // size 分支:width/height 已由 validate_opts 保证 > 0。keepAspectRatio=Some(false) → 拉伸,否则保持比例贴框。
        match opts.keep_aspect_ratio {
            Some(false) => img.resize_exact(tw, th, filter),
            _ => img.resize(tw, th, filter),
        }
    } else if let (Some(tw), None) = (opts.width, opts.height) {
        // 只给 width:等比缩放到目标宽(对齐 thumbnailator `.width(w)`;此前静默 no-op 是缺陷)。
        let th = ((img.height() as u64 * tw as u64) as f64 / img.width().max(1) as f64)
            .round()
            .max(1.0) as u32;
        img.resize_exact(tw, th, filter)
    } else if let (None, Some(th)) = (opts.width, opts.height) {
        // 只给 height:等比缩放到目标高。
        let tw = ((img.width() as u64 * th as u64) as f64 / img.height().max(1) as f64)
            .round()
            .max(1.0) as u32;
        img.resize_exact(tw, th, filter)
    } else if let Some(scale) = opts.scale {
        // scale 已由 validate_opts 保证 > 0;1.0 为 no-op。
        if scale == 1.0 {
            return img;
        }
        let nw = ((img.width() as f64) * scale).round().max(1.0) as u32;
        let nh = ((img.height() as f64) * scale).round().max(1.0) as u32;
        // 等比(两维同比例)→ resize_exact 即等比,无变形。
        img.resize_exact(nw, nh, filter)
    } else {
        img
    }
}

/// 业务作用: 将处理后的图片重新编码成目标格式。
///
/// # 参数
/// - `img`: 已完成 resize 的图片。
/// - `format`: 输出图片格式。
/// - `quality`: 可选 JPEG 质量；非 JPEG 格式会忽略该值。
fn encode(img: &DynamicImage, format: ImageFormat, quality: Option<f32>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    if format == ImageFormat::Jpeg {
        // JPEG 不支持透明通道，因此转换为 RGB；合法质量零映射到编码器最低质量 1。
        // 其它质量按百分比转换并限制到编码器允许的 1..=100。
        let q = quality
            .map(|q| ((q * 100.0).round() as i32).clamp(1, 100) as u8)
            .unwrap_or(85);
        let rgb = img.to_rgb8();
        JpegEncoder::new_with_quality(&mut out, q)
            .write_image(
                rgb.as_raw(),
                rgb.width(),
                rgb.height(),
                ExtendedColorType::Rgb8,
            )
            .map_err(|e| ImageError::Encode(format!("jpeg: {e}")))?;
    } else {
        // 其它格式:quality 忽略(同 thumbnailator),按格式默认编码。
        img.write_to(&mut Cursor::new(&mut out), format)
            .map_err(|e| ImageError::Encode(format!("{format:?}: {e}")))?;
    }
    Ok(out)
}
