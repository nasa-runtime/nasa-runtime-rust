# naimg

`naimg` 是图片压缩和缩放工具，对齐历史 `ImageUtils` 的核心能力。业务通常通过门面使用：

```toml
[dependencies]
nasa = { version = "1", features = ["image"] }
```

质量加等比缩放：

```rust
let out = nasa::image::compress_scale(
    &input_bytes,
    0.82,
    0.5,
    None,
)?;
```

固定尺寸压缩：

```rust
let out = nasa::image::compress_size(
    &input_bytes,
    Some(0.85),
    800,
    600,
    Some(true),
    Some(nasa::image::ImageFormat::Jpeg),
)?;
```

完整选项：

```rust
let opts = nasa::image::CompressOpts {
    quality: Some(0.9),
    width: Some(1024),
    height: Some(768),
    keep_aspect_ratio: Some(true),
    ..Default::default()
};

let out = nasa::image::compress(&input_bytes, &opts, None)?;
```

注意：

- `quality` 只影响 JPEG；PNG 等格式会忽略。
- `format = None` 时保留输入格式。
- 1.0 支持 JPEG、PNG、GIF、WebP、BMP、ICO、TIFF、PNM、QOI、TGA；AVIF、EXR 等未声明
  格式不会随默认构建编入，业务应在上传边界先转换。
- `scale <= 0`、`width/height = 0`、`quality` 越界会返回错误。
- 输出像素上限为 100MP，用于防止异常放大导致 OOM。

## 行为边界(实测)

- 单维度缩放:只给 `width`(或只给 `height`)按原图比例等比推导另一维,例如 64x32 + `width=16` → 16x8。
- EXIF orientation 不应用:带旋转标记的手机照片输出保持原始像素方向;需要旋正先自行处理(如 `kamadak-exif` + 旋转)。
- 透明通道 → JPEG:RGBA 转 JPEG 丢弃 alpha,透明像素变黑;需要白底先自行合成。
- 参数 fail-fast:`scale<=0`、`width|height==0`、`quality∉0.0..=1.0`、非图片字节均返回错误,不静默修正;1x1 超小图正常缩放不 panic。
- 输出像素上限 100MP,超出返回 `InvalidArgument`(防 OOM)。
- JPEG 质量实测有效(同图 quality 0.1 明显小于 0.95);PNG 等无损格式忽略 quality。

## YML 配置与使用

`naimg` 不主动读取 yml，但 `CompressOpts` 可以由业务配置映射。图片压缩通常直接配置一个 `image:` 段，再在上传、头像处理、K 线截图或内容审核等业务流程里按需调用。

单图配置示例：

```yaml
image:
  quality: 0.85
  scale: 0.5
  width: null
  height: null
  keep_aspect_ratio: true
  filter: lanczos3
  output_format: jpeg
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `quality` | `null` | JPEG 质量，范围 0.0 到 1.0；PNG 会忽略。 |
| `scale` | `null` | 按比例缩放；与 width/height 同时配置时以具体尺寸为主。 |
| `width` | `null` | 目标宽；只给宽时按比例计算高。 |
| `height` | `null` | 目标高；只给高时按比例计算宽。 |
| `keep_aspect_ratio` | `true` | 同时给宽高时是否保持纵横比。 |
| `filter` | `lanczos3` | 重采样算法，推荐保持默认。 |
| `output_format` | `null` | 输出格式；为空时保留输入格式。 |

应用侧映射示例：

```rust
let opts = naimg::CompressOpts {
    quality: cfg.image.quality,
    scale: cfg.image.scale,
    width: cfg.image.width,
    height: cfg.image.height,
    keep_aspect_ratio: cfg.image.keep_aspect_ratio,
    ..Default::default()
};

let out = naimg::compress(&input, &opts, cfg.image.output_format)?;
```
