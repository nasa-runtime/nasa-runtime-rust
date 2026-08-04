// ============================================================================
// proto/src/io.rs —— wire 编解码原语:varint / zigzag / Writer / Reader
//
// 逐字节对齐 原实现 com.nasa.common.base.ProtocolBytes(见 rust-ws/架构说明 附录 A)。
// 解码端按"攻击者输入"设防(附录 A.9):varint≤10 字节、长度/count 不超剩余、trailing 检查。
// ============================================================================

use std::fmt;

/// VARINT wire type,用于整数、bool 以及字段 key。
pub const WT_VARINT: u64 = 0;
/// LENGTH_DELIMITED wire type,用于字符串、字节数组和字符串数组。
pub const WT_LEN: u64 = 2;

/// 单个数组(events/uids/routers/excludes 等)的最大元素数。
/// 远超任何合法单条消息的收件人/事件数;超限即判畸形输入。如需更大应分批发送。
pub const MAX_ARRAY_ELEMS: u64 = 1 << 16;

/// 单个字符串 / 单个 byte[] 字段的最大长度。
/// read_len 已按"剩余字节"封顶;此处再加每字段绝对上限,即便外层放大 max_frame 也封住单字段。
pub const MAX_STR_LEN: usize = 16 * 1024 * 1024;
/// 单个 byte[] 字段最大长度,用于 payload 与集群转发消息体的防御性解码上限。
pub const MAX_BYTES_LEN: usize = 16 * 1024 * 1024;

/// 业务作用：已知 tag 的 wire type 校验(附录 A.3)。派生宏生成的 decode 用。
///
/// # 参数
/// - `tag`: 当前正在解码的 schema 字段 tag,用于错误里定位字段。
/// - `expected`: schema 为该 tag 声明的 wire type。
/// - `actual`: 输入字节流字段头里实际携带的 wire type。
pub fn check_wt(tag: u64, expected: u64, actual: u64) -> Result<()> {
    if expected != actual {
        return Err(CodecError::WireTypeMismatch {
            tag,
            expected,
            actual,
        });
    }
    Ok(())
}

/// 协议编解码错误,覆盖畸形输入、跨版本未知字段跳过失败和 JSON_BYTES serde 失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// 数据被截断(读越界)
    Truncated,
    /// varint 超过 10 字节或第 10 字节溢出
    VarintOverflow,
    /// 声明长度/count 超过剩余字节
    LengthExceeds,
    /// 已知 tag 的 wire type 与 schema 不符
    WireTypeMismatch {
        /// 出错字段的 schema tag。
        tag: u64,
        /// schema 声明的 wire type。
        expected: u64,
        /// 输入字节实际携带的 wire type。
        actual: u64,
    },
    /// BITPACK bitmap 出现 schema 字段范围外的位
    BadBitmap,
    /// 解码后仍有未消费字节
    TrailingBytes(usize),
    /// 编码长度加法超出当前平台可表示范围。
    LengthOverflow,
    /// 调用方提供的定长输出缓冲区不足。
    BufferTooSmall {
        /// 编码所需字节数。
        required: usize,
        /// 输出缓冲区剩余字节数。
        remaining: usize,
    },
    /// 非法 UTF-8
    Utf8,
    /// 该 mode 尚未实现(FAST_FIXED)
    Unsupported(&'static str),
    /// JSON_BYTES 模式 serde_json 编解码错误(消息文本)
    Json(String),
}

impl fmt::Display for CodecError {
    /// 业务作用：把编解码错误格式化为日志和上层错误链可读的文本。
    ///
    /// # 参数
    /// - `f`: 标准格式化器,由错误展示路径提供。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CodecError {}

/// 协议编解码统一结果类型。
pub type Result<T> = std::result::Result<T, CodecError>;

/* ============================== zigzag(附录 A.2)============================== */

/// 业务作用：有符号整型 → 无符号 varint 友好编码。`<<` 在 Rust 是位移不做溢出检查,与 原实现 一致。
///
/// # 参数
/// - `v`: schema 中的有符号整数值,会映射成适合 varint 的无符号值。
#[inline]
pub fn zigzag_enc(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// 业务作用：解码 ZigZag 整数；用于从无符号变长编码还原有符号值。
///
/// # 参数
/// - `v`: 从 varint 读取出的无符号 ZigZag 编码值。
#[inline]
pub fn zigzag_dec(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// 业务作用：往 Vec 写一个无符号 varint(LEB128)。free 函数,供数组内部累积用。
///
/// # 参数
/// - `buf`: 目标编码缓冲区,函数会把 varint 字节追加到末尾。
/// - `v`: 要写入的无符号整数值。
pub fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v & !0x7f != 0 {
        buf.push(((v & 0x7f) | 0x80) as u8);
        v >>= 7;
    }
    buf.push(v as u8);
}

/* ============================== Writer ============================== */

/// 保存协议写入缓冲区；用于按二进制协议编码字段。
#[derive(Default)]
pub struct Writer {
    /// 已写入的 wire payload 字节。
    pub buf: Vec<u8>,
}

impl Writer {
    /// 业务作用：创建空协议写入器。
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// 业务作用：VARINT_TLV 的字段头:`varint(tag<<3 | wiretype)`。
    ///
    /// # 参数
    /// - `tag`: schema 字段 tag。
    /// - `wt`: 字段 wire type,目前主要是 VARINT 或 LEN。
    pub fn key(&mut self, tag: u64, wt: u64) {
        write_varint(&mut self.buf, (tag << 3) | wt);
    }

    /// 业务作用：BITPACK 头部 bitmap,`k` 字节小端。
    ///
    /// # 参数
    /// - `bm`: BITPACK 字段存在性位图。
    /// - `k`: 位图占用字节数,按 schema 字段数量由派生代码决定。
    pub fn bitmap(&mut self, bm: u64, k: usize) {
        // 位图最多 8 字节（u64）。`k > 8` 会让 `>> (i*8)` 移位越界：debug 下 panic、
        // release 下按位宽取模产出错误值。`__rt` 是 pub 模块，下游可直接调用，必须自防。
        let k = k.min(8);
        for i in 0..k {
            self.buf.push((bm >> (i * 8)) as u8);
        }
    }

    /// 业务作用：LENGTH_DELIMITED:`varint(len) + payload`。
    ///
    /// # 参数
    /// - `payload`: 要写入的字符串、字节数组或数组内部编码内容。
    pub fn len_delim(&mut self, payload: &[u8]) {
        write_varint(&mut self.buf, payload.len() as u64);
        self.buf.extend_from_slice(payload);
    }

    /// 业务作用：VARINT 值(有符号整型,zigzag):long / int / short / byte。
    ///
    /// # 参数
    /// - `v`: schema 中的有符号整数值,写入前会先做 ZigZag 编码。
    pub fn val_i64(&mut self, v: i64) {
        write_varint(&mut self.buf, zigzag_enc(v));
    }

    /// 业务作用：VARINT 值(bool,**不 zigzag**,裸 varint 0/1)。
    ///
    /// # 参数
    /// - `b`: schema 中的布尔值,写成裸 varint 0 或 1。
    pub fn val_bool(&mut self, b: bool) {
        write_varint(&mut self.buf, if b { 1 } else { 0 });
    }

    /// 业务作用：写入字符串值；用于把文本字段编码进协议缓冲区。
    ///
    /// # 参数
    /// - `s`: schema 字符串字段值,按 UTF-8 字节写入 LENGTH_DELIMITED。
    pub fn val_str(&mut self, s: &str) {
        self.len_delim(s.as_bytes());
    }

    /// 业务作用：写入字节值；用于把二进制字段编码进协议缓冲区。
    ///
    /// # 参数
    /// - `b`: schema byte[] 字段值,原样写入 LENGTH_DELIMITED。
    pub fn val_bytes(&mut self, b: &[u8]) {
        self.len_delim(b);
    }

    /// 业务作用：String[](LD):内部 `[count][每元素 len+bytes]`,null 元素 → len 0。
    ///
    /// **归一化(lossy)边界**:`Some("")` 与 `None` 都编码为长度 0,解码端一律还原成 `None`
    /// (与既有 wire 格式逐字节一致;同 json.rs 声明的 JSON 模式 Some(空)↔None 归一)。
    /// 数组元素**语义上不区分空串与 null**——业务不要依赖 `Some("")` round-trip。
    ///
    /// # 参数
    /// - `arr`: schema 字符串数组字段,`None` 元素按长度 0 编码。
    pub fn val_str_array(&mut self, arr: &[Option<String>]) {
        let mut inner = Vec::new();
        write_varint(&mut inner, arr.len() as u64);
        for e in arr {
            match e {
                Some(s) => {
                    write_varint(&mut inner, s.len() as u64);
                    inner.extend_from_slice(s.as_bytes());
                }
                None => write_varint(&mut inner, 0),
            }
        }
        self.len_delim(&inner);
    }
}

/* ============================== Reader ============================== */

/// 保存协议读取游标；用于从字节切片顺序解码字段。
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// 业务作用：构造读取器并把游标定位到输入开头。
    ///
    /// # 参数
    /// - `data`: 待解码的完整协议 payload 字节切片。
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// 业务作用：返回未读取字节数；用于判断协议包是否还有字段。
    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    /// 业务作用：解析 read byte 输入；用于把文本或语法节点转换为内部结构。
    #[inline]
    fn read_byte(&mut self) -> Result<u8> {
        if self.pos >= self.data.len() {
            return Err(CodecError::Truncated);
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// 业务作用：无符号 varint:最多 10 字节,第 10 字节只允许最低位(溢出检测)。
    pub fn read_varint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        for i in 0..10 {
            let b = self.read_byte()?;
            if i == 9 && (b & 0xFE) != 0 {
                return Err(CodecError::VarintOverflow);
            }
            result |= ((b & 0x7f) as u64) << (i * 7);
            if b & 0x80 == 0 {
                return Ok(result);
            }
        }
        Err(CodecError::VarintOverflow)
    }

    /// 业务作用：LENGTH_DELIMITED 的长度:varint 且 ≤ 剩余字节(防伪造长度触发巨量分配)。
    pub fn read_len(&mut self) -> Result<usize> {
        let len = self.read_varint()?;
        if len > self.remaining() as u64 {
            return Err(CodecError::LengthExceeds);
        }
        Ok(len as usize)
    }

    /// 业务作用：解析 read i64 输入；用于把文本或语法节点转换为内部结构。
    pub fn read_i64(&mut self) -> Result<i64> {
        Ok(zigzag_dec(self.read_varint()?))
    }

    /// 业务作用：解析 read bool 输入；用于把文本或语法节点转换为内部结构。
    pub fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_varint()? != 0)
    }

    /// 业务作用：从当前位置取出指定长度的字节片段并推进游标。
    ///
    /// # 参数
    /// - `n`: 要从当前 payload 中读取的字节数。
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return Err(CodecError::LengthExceeds);
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// 业务作用：读取 UTF-8 字符串字段。
    pub fn read_str(&mut self) -> Result<String> {
        let n = self.read_len()?;
        if n > MAX_STR_LEN {
            return Err(CodecError::LengthExceeds);
        }
        let bytes = self.take(n)?;
        std::str::from_utf8(bytes)
            .map(|s| s.to_string())
            .map_err(|_| CodecError::Utf8)
    }

    /// 业务作用：读取 byte[] 字段。
    pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.read_len()?;
        if n > MAX_BYTES_LEN {
            return Err(CodecError::LengthExceeds);
        }
        Ok(self.take(n)?.to_vec())
    }

    /// 业务作用：String[]:外层 LD,内层 `[count][每元素 len+bytes]`;元素 len=0 → None(与 原实现 一致)。
    pub fn read_str_array(&mut self) -> Result<Vec<Option<String>>> {
        let n = self.read_len()?;
        let inner = self.take(n)?;
        let mut r = Reader::new(inner);
        let count = r.read_varint()?;
        // 设防:count ≤ 剩余字节(每元素至少 1 字节)+ ≤ 元素数硬上限,
        // 防"小帧放大成巨量分配"(每个 Option<String> 远大于 1 字节,16MB 帧可逼出数百 MB)。
        if count > r.remaining() as u64 || count > MAX_ARRAY_ELEMS {
            return Err(CodecError::LengthExceeds);
        }
        // 预留按上限封顶(合法大数组靠 push 自然增长,不按攻击者 count 一次性预分配)。
        let mut out = Vec::with_capacity((count as usize).min(4096));
        for _ in 0..count {
            let len = r.read_len()?;
            if len > MAX_STR_LEN {
                return Err(CodecError::LengthExceeds);
            }
            if len == 0 {
                out.push(None);
            } else {
                let b = r.take(len)?;
                out.push(Some(
                    std::str::from_utf8(b)
                        .map_err(|_| CodecError::Utf8)?
                        .to_string(),
                ));
            }
        }
        if r.remaining() != 0 {
            return Err(CodecError::TrailingBytes(r.remaining()));
        }
        Ok(out)
    }

    /// 业务作用：读 k 字节小端 bitmap。
    ///
    /// # 参数
    /// - `k`: BITPACK 位图占用字节数,必须由 schema 字段数量计算得出。
    pub fn read_bitmap(&mut self, k: usize) -> Result<u64> {
        // 同 `Writer::bitmap`：位图不超过 8 字节，越界移位在 release 下会静默产出错误位图。
        let k = k.min(8);
        let bytes = self.take(k)?;
        let mut bm: u64 = 0;
        for (i, b) in bytes.iter().enumerate() {
            bm |= (*b as u64) << (i * 8);
        }
        Ok(bm)
    }

    /// 业务作用：跳过未知 tag 的 value(VARINT_TLV 跨版本兼容)。
    ///
    /// # 参数
    /// - `wt`: 未知字段的 wire type,决定按 varint 还是 length-delimited 跳过。
    pub fn skip_value(&mut self, wt: u64) -> Result<()> {
        match wt {
            WT_VARINT => {
                self.read_varint()?;
                Ok(())
            }
            WT_LEN => {
                let n = self.read_len()?;
                self.take(n)?;
                Ok(())
            }
            _ => Err(CodecError::WireTypeMismatch {
                tag: 0,
                expected: 0,
                actual: wt,
            }),
        }
    }

    /// 业务作用：解码收尾:不允许 trailing bytes。
    pub fn expect_end(&self) -> Result<()> {
        if self.remaining() != 0 {
            return Err(CodecError::TrailingBytes(self.remaining()));
        }
        Ok(())
    }
}
