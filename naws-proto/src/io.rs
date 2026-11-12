// ============================================================================
// proto/src/io.rs —— wire 编解码原语:varint / zigzag / Writer / Reader
//
// 整型、长度前缀与字符串编码遵守协议字节合同，读写双方必须使用相同规则。
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
/// 参数说明：
/// - `tag`: 当前正在解码的 schema 字段 tag,用于错误里定位字段。
/// - `expected`: schema 为该 tag 声明的 wire type。
/// - `actual`: 输入字节流字段头里实际携带的 wire type。
///
/// 返回：类型匹配时通过；不匹配时返回带字段 tag、期望值和实际值的 WireTypeMismatch。
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
    /// varint 超过 10 字节、第 10 字节溢出或整数超出字段声明的位宽。
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
    /// 当前 schema 不适用于请求的编码模式。
    Unsupported(&'static str),
    /// JSON_BYTES 模式 serde_json 编解码错误(消息文本)
    Json(String),
}

impl fmt::Display for CodecError {
    /// 业务作用：把编解码错误格式化为日志和上层错误链可读的文本。
    ///
    /// 参数说明：
    /// - `f`: 标准格式化器,由错误展示路径提供。
    ///
    /// 返回：向格式化器写入错误变体及字段；输出目标失败时传播格式化错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CodecError {}

/// 协议编解码统一结果类型。
pub type Result<T> = std::result::Result<T, CodecError>;

/* ============================== zigzag(附录 A.2)============================== */

/// 业务作用：有符号整型 → 无符号 varint 友好编码。`<<` 在 Rust 是位移不做溢出检查。
///
/// 参数说明：
/// - `v`: schema 中的有符号整数值,会映射成适合 varint 的无符号值。
///
/// 返回：覆盖完整 i64 范围的无符号编码，正负小数均保持较短的 varint 表示。
#[inline]
pub fn zigzag_enc(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// 业务作用：解码 ZigZag 整数；用于从无符号变长编码还原有符号值。
///
/// 参数说明：
/// - `v`: 从 varint 读取出的无符号 ZigZag 编码值。
///
/// 返回：恢复原有符号整数，包括 i64 两端边界。
#[inline]
pub fn zigzag_dec(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// 业务作用：往 Vec 写一个无符号 varint(LEB128)。free 函数,供数组内部累积用。
///
/// 参数说明：
/// - `buf`: 目标编码缓冲区,函数会把 varint 字节追加到末尾。
/// - `v`: 要写入的无符号整数值。
///
/// 返回：向 buf 追加一至十个字节，不改变已有内容。
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
    /// 业务作用：编码字符等无符号整数，不应用有符号 zigzag。
    ///
    /// 参数说明：
    ///
    /// - `value`：已经符合字段位宽的无符号值。
    ///
    /// 返回：向当前输出追加无符号 varint。
    pub fn val_unsigned(&mut self, value: u64) {
        write_varint(&mut self.buf, value);
    }
    /// 业务作用：创建空协议写入器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未写入任何字段、缓冲区为空的写入器。
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
        }
    }

    /// 业务作用：VARINT_TLV 的字段头:`varint(tag<<3 | wiretype)`。
    ///
    /// 参数说明：
    /// - `tag`: schema 字段 tag。
    /// - `wt`: 字段 wire type,目前主要是 VARINT 或 LEN。
    ///
    /// 返回：追加字段编号与类型组合后的 varint 头；字段合法性由 schema 保证。
    pub fn key(&mut self, tag: u64, wt: u64) {
        write_varint(&mut self.buf, (tag << 3) | wt);
    }

    /// 业务作用：BITPACK 头部 bitmap,`k` 字节小端。
    ///
    /// 参数说明：
    /// - `bm`: BITPACK 字段存在性位图。
    /// - `k`: 位图占用字节数,按 schema 字段数量由派生代码决定。
    ///
    /// 返回：追加至多八个小端位图字节；k 超过八时按八处理。
    pub fn bitmap(&mut self, bm: u64, k: usize) {
        // 位图最多 8 字节；越界移位可能终止进程或产生错误值，公开写入器必须限制移位范围。
        let k = k.min(8);
        for i in 0..k {
            self.buf.push((bm >> (i * 8)) as u8);
        }
    }

    /// 业务作用：写入长度分隔字段，使读取方能确定载荷边界。
    ///
    /// 参数说明：
    /// - `payload`: 要写入的字符串、字节数组或数组内部编码内容。
    ///
    /// 返回：追加载荷长度及其原始字节；不转换载荷内容。
    pub fn len_delim(&mut self, payload: &[u8]) {
        write_varint(&mut self.buf, payload.len() as u64);
        self.buf.extend_from_slice(payload);
    }

    /// 业务作用：VARINT 值(有符号整型,zigzag):long / int / short / byte。
    ///
    /// 参数说明：
    /// - `v`: schema 中的有符号整数值,写入前会先做 ZigZag 编码。
    ///
    /// 返回：追加经过 ZigZag 转换的 varint，不写字段头。
    pub fn val_i64(&mut self, v: i64) {
        write_varint(&mut self.buf, zigzag_enc(v));
    }

    /// 业务作用：VARINT 值(bool,**不 zigzag**,裸 varint 0/1)。
    ///
    /// 参数说明：
    /// - `b`: schema 中的布尔值,写成裸 varint 0 或 1。
    ///
    /// 返回：追加裸 varint 0 或 1，不应用 ZigZag。
    pub fn val_bool(&mut self, b: bool) {
        write_varint(
            &mut self.buf,
            if b {
                1
            } else {
                0
            },
        );
    }

    /// 业务作用：写入字符串值；用于把文本字段编码进协议缓冲区。
    ///
    /// 参数说明：
    /// - `s`: schema 字符串字段值,按 UTF-8 字节写入 LENGTH_DELIMITED。
    ///
    /// 返回：追加 UTF-8 字节长度及文本内容，不写字段头。
    pub fn val_str(&mut self, s: &str) {
        self.len_delim(s.as_bytes());
    }

    /// 业务作用：写入字节值；用于把二进制字段编码进协议缓冲区。
    ///
    /// 参数说明：
    /// - `b`: schema byte[] 字段值,原样写入 LENGTH_DELIMITED。
    ///
    /// 返回：追加字节长度和原始二进制内容，不写字段头。
    pub fn val_bytes(&mut self, b: &[u8]) {
        self.len_delim(b);
    }

    /// 业务作用：String[](LD):内部 `[count][每元素 len+bytes]`,null 元素 → len 0。
    ///
    /// **归一化(lossy)边界**:`Some("")` 与 `None` 都编码为长度 0,解码端一律还原成 `None`
    /// (与既有 wire 格式逐字节一致;同 json.rs 声明的 JSON 模式 Some(空)↔None 归一)。
    /// 数组元素**语义上不区分空串与 null**——业务不要依赖 `Some("")` round-trip。
    ///
    /// 参数说明：
    /// - `arr`: schema 字符串数组字段,`None` 元素按长度 0 编码。
    ///
    /// 返回：追加带总长度、元素数及逐项长度的数组；空串与 None 均写为零长度。
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
    /// 调用方持有的原始输入，读取期间不复制整包。
    data: &'a [u8],
    /// 下一个未消费字节的位置，始终不超过输入长度。
    pos: usize,
}

impl<'a> Reader<'a> {
    /// 业务作用：取得经过长度边界检查的借用片段，供嵌套 codec 保留输入所有权。
    ///
    /// 参数说明：
    /// - `length`：本字段已经核验的字节数。
    ///
    /// 返回：原始输入的借用切片；剩余不足时拒绝，不复制内容。
    pub fn read_slice(&mut self, length: usize) -> Result<&'a [u8]> {
        self.take(length)
    }
    /// 业务作用：读取固定宽度字段，保持小端值解码前的长度边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：恰好 N 个字节；输入不足时拒绝，不读取后续字段或分配外部长度。
    pub fn read_fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self.take(N)?;
        Ok(bytes
            .try_into()
            .expect("fixed slice has the requested length"))
    }
    /// 业务作用：构造读取器并把游标定位到输入开头。
    ///
    /// 参数说明：
    /// - `data`: 待解码的完整协议 payload 字节切片。
    ///
    /// 返回：游标位于输入开头的借用读取器，不复制输入。
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
        }
    }

    /// 业务作用：返回未读取字节数；用于判断协议包是否还有字段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未消费的字节数；读取游标保持不变。
    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    /// 业务作用：为二进制解码读取下一个字节，并在输入耗尽时阻止越界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功取一个字节并推进游标；已到输入末尾时返回 Truncated。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最多消费十个字节并返回无符号值；输入不足或位宽溢出时拒绝，已读前缀不回退。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不超过剩余输入的长度；非法 varint 或长度越界时拒绝，长度前缀已被消费。
    pub fn read_len(&mut self) -> Result<usize> {
        let len = self.read_varint()?;
        if len > self.remaining() as u64 {
            return Err(CodecError::LengthExceeds);
        }
        Ok(len as usize)
    }

    /// 业务作用：读取 ZigZag varint 并还原协议中的有符号整数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功还原有符号整数；输入截断或 varint 溢出时传播错误。
    pub fn read_i64(&mut self) -> Result<i64> {
        Ok(zigzag_dec(self.read_varint()?))
    }

    /// 业务作用：读取协议布尔值，沿用零与非零的兼容解码规则。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：零为 false，非零为 true；输入截断或 varint 溢出时传播错误。
    pub fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_varint()? != 0)
    }

    /// 业务作用：从当前位置取出指定长度的字节片段并推进游标。
    ///
    /// 参数说明：
    /// - `n`: 要从当前 payload 中读取的字节数。
    ///
    /// 返回：成功借用指定区间并推进游标；剩余不足时保持游标并返回 LengthExceeds。
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return Err(CodecError::LengthExceeds);
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// 业务作用：读取 UTF-8 字符串字段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回拥有型 UTF-8 字符串；长度越界或非法 UTF-8 时拒绝，不回退已消费前缀。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回复制后的拥有型字节数组；长度超过输入或字段上限时拒绝。
    pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.read_len()?;
        if n > MAX_BYTES_LEN {
            return Err(CodecError::LengthExceeds);
        }
        Ok(self.take(n)?.to_vec())
    }

    /// 业务作用：String[]:外层 LD,内层 `[count][每元素 len+bytes]`;元素 len=0 → None。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回逐项拥有的可空字符串数组；空元素为 None，长度、数量、UTF-8 或尾部字节非法时拒绝。
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
    /// 参数说明：
    /// - `k`: BITPACK 位图占用字节数,必须由 schema 字段数量计算得出。
    ///
    /// 返回：至多读取八字节并返回小端位图；剩余不足时拒绝，k 超过八按八处理。
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
    /// 参数说明：
    /// - `wt`: 未知字段的 wire type,决定按 varint 还是 length-delimited 跳过。
    ///
    /// 返回：支持的字段类型完整跳过后成功；未知类型、截断或越界时拒绝，不回退已消费前缀。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：输入恰好耗尽时通过；否则返回剩余字节数，游标不变。
    pub fn expect_end(&self) -> Result<()> {
        if self.remaining() != 0 {
            return Err(CodecError::TrailingBytes(self.remaining()));
        }
        Ok(())
    }
}
