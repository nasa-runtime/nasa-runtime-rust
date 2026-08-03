//! # naimg —— 图片压缩/缩放(对照 原实现 原工具包 `ImageUtils`)
//!
//! 公开 crate 名为 `naimg`；内部依赖 crates.io 的 `image` 已别名为 `image_crate`，避免使用方误把
//! 底层 codec API 当作本组件稳定合同。
//!
//! 把图片按**质量(quality)**与**尺寸(scale/width/height)**压缩后写出。原实现 用 thumbnailator,Rust 用
//! [`image`](https://docs.rs/image) crate(`DynamicImage::resize` + `JpegEncoder` quality)。
//!
//! ## 与 原实现 的对应
//! - `.scale(s)` → 等比缩放(`resize_exact((w*s),(h*s))`)。
//! - `.size(w,h)` + `keepAspectRatio` → `resize`(保持比例,贴框)/`resize_exact`(拉伸)。
//! - `.width(w)` / `.height(h)`(单维度)→ 按原图比例等比推导另一维后缩放。
//! - `.outputQuality(q)`(0.0–1.0)→ `JpegEncoder::new_with_quality((q*100) as u8)`,**仅 JPEG 有效**(PNG 等忽略,同 thumbnailator)。
//! - **输出格式默认保留输入格式**(thumbnailator 未指定 outputFormat 时沿用源格式;历史图片处理路径即此)。
//! - 1.0 格式合同限定为 JPEG、PNG、GIF、WebP、BMP、ICO、TIFF、PNM、QOI、TGA；未声明的
//!   AVIF/EXR 等格式不随默认依赖编入，调用方应在上传边界先转换。
//!
//! ## 已知偏离 thumbnailator(文档化,未复刻)
//! - **EXIF orientation 不应用**:thumbnailator 默认 `useExifOrientation=true` 会按 EXIF 旋正照片;
//!   本 crate 的 `image::load_from_memory` 不读 EXIF,带旋转标记的手机照片输出方向保持原始像素方向。
//!   需要旋正时调用方先行处理(如 `kamadak-exif` + `DynamicImage::rotate*`)。
//! - **透明通道→JPEG 黑底**:RGBA(如透明 PNG)转 JPEG 时经 `to_rgb8()` 直接丢弃 alpha,
//!   透明像素 `(0,0,0,0)` 变黑色;不做白底合成、不报错。需要白底请先自行合成。
//!
//! ## 不复刻 原实现 的重载爆炸
//! 原实现 13 个 compress(InputStream/File/BufferedImage × scale/size/quality)→ 这里 `&[u8] → Vec<u8>` 统一,
//! 文件读写由调用方处理。
//!
//! ## 有意不迁移:`imgFormatMap`/`getImgFormat`/`addImgFormat`(浏览器 MIME 子类型映射)
//! 原实现 `ImageUtils` 还含 `jpg→jpeg`/`tif→tiff` 等扩展名→MIME 子类型映射。**本 crate 只负责压缩,不做 MIME 映射**:
//! 本组件只负责压缩,不设置 Content-Type,故浏览器 MIME 子类型映射由上传层按需处理。
//! 按扩展名派生 `image/{subtype}` 属于上传或 Web 层职责，不在本 crate 中隐式推断。
//!
//! ## 参数校验(对照 原实现 thumbnailator fail-fast)
//! 非法 `scale<=0` / `width|height==0` / `quality∉0.0..=1.0` 返 [`ImageError::InvalidArgument`],**不静默修正**
//! (与 thumbnailator 抛 `IllegalArgumentException` 语义一致;暴露调用方/配置错误)。
//!
//! ## 保真边界
//! **不与 thumbnailator 逐像素一致**(底层重采样算法不同),只保证语义等价(比例/尺寸/质量)。

use image_crate::codecs::jpeg::JpegEncoder;
use image_crate::{DynamicImage, ExtendedColorType, ImageEncoder};
use std::io::Cursor;

pub use image_crate::ImageFormat;

/// 缩放过滤器(默认 [`Filter::Lanczos3`],接近 thumbnailator 默认的高质量重采样)。
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
    /// Lanczos3 高质量采样，作为默认值对齐 thumbnailator 的高质量缩放预期。
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

/// 压缩选项(对照 thumbnailator builder 的可选链)。`Default` = 不缩放、不设质量、Lanczos3。
#[derive(Debug, Clone, Default)]
pub struct CompressOpts {
    /// outputQuality,0.0–1.0,**仅 JPEG**;`None` = 编码器默认。
    pub quality: Option<f32>,
    /// 等比缩放比例(0–1 缩小,>1 放大)。**与 width/height 互斥,后者优先**。
    pub scale: Option<f64>,
    /// 目标宽。只给 width(height=None)时按原图比例等比推导高(对齐 thumbnailator `.width(w)`)。
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
    /// 非法参数(对照 原实现 thumbnailator fail-fast:scale<=0 / width|height<=0 / quality 越界都抛异常)。
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

/// 业务作用: 主入口:字节入 → 字节出。`format = None` 保留输入格式(对照历史图片处理路径),`Some(f)` 显式覆盖。
///
/// 对照 原实现 `compress(InputStream, ...)` 全家族。缩放规则:`width&&height` 优先,否则 `scale`,都无则不缩放。
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

/// 业务作用: 便捷:质量 + 等比缩放(对照 原实现 `compress(is, compSize, scale, os)`)。
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

/// 业务作用: 便捷:质量 + 定宽高 + keepAspectRatio(对照 原实现 `compress(is, compSize, width, height, keepAspectRatio, os)`)。
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

/// 业务作用: 参数校验,**对齐 原实现 thumbnailator 的 fail-fast**(非法参数不静默修正)。
/// - `quality`:`Some` 时须有限且 ∈ `0.0..=1.0`(原实现 `outputQuality` 同界;注:`0.0` 合法,JPEG 编码器侧会落到
///   最低质量 1,见 [`encode`])。
/// - `scale`:`Some` 时须有限且 `> 0`(`1.0` 合法=no-op,同 原实现 `scale(1.0)`)。
/// - `width`/`height`:显式给出时须 `> 0`(原实现 `size(w,h)` 对 `<=0` 抛异常)。
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
        // JPEG:质量参数生效;JPEG 不支持 alpha → 转 rgb8。
        // quality 已由 validate_opts 保证 ∈ 0.0..=1.0;`q*100` ∈ [0,100],clamp(1,100) 仅把 原实现 合法的 `0.0`
        // 落到编码器最低质量 1(JPEG encoder 不接受 0),其余原样。
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
