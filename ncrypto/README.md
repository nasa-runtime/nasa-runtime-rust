# ncrypto

`ncrypto` 提供哈希、HMAC/KDF、AES、RSA、Ed25519、Base64URL、随机值和现代口令加密工具。它同时保留若干旧系统兼容算法。

直接依赖：

```toml
[dependencies]
ncrypto = "1"
```

## 安全边界

新业务优先使用 `encrypt_modern` / `decrypt_modern`。默认写入的 NC2 使用 Argon2id 从口令派生密钥，再用 AES-256-GCM 同时提供机密性和完整性。旧系统兼容函数如 AES-ECB、CBC(IV=Key)、RSA PKCS#1 v1.5、RSA 私钥“加密”等只用于和既有系统逐字节互通，不应作为新系统保密边界。

默认构建不会执行 PKCS#1 v1.5 私钥解密或历史私钥 type-1 运算；RS256 公钥验签、RSA-OAEP、
Ed25519、现代 AEAD、哈希和 KDF 不受影响。确有迁移合同的调用方必须单独启用：

```toml
[dependencies]
ncrypto = { version = "1", features = ["legacy-rsa-private"] }
```

该 feature 只开放低层兼容运算，不会替调用方建立协议级风险门。Web 端点等上层集成仍应增加独立的配置准入、启动审计和运行时授权。

## 现代口令加密

```rust
let token = ncrypto::encrypt_modern("secret text", "strong-password")?;
let plain = ncrypto::decrypt_modern(&token, "strong-password")?;
assert_eq!(plain, "secret text");
```

二进制数据：

```rust
let token = ncrypto::encrypt_modern_bytes(b"payload", "strong-password")?;
let bytes = ncrypto::decrypt_modern_bytes(&token, "strong-password")?;
```

需要阻止密文被搬到另一个租户、记录或协议上下文时，必须绑定业务 AAD：

```rust
let aad = b"tenant=acme;record=42";
let token = ncrypto::encrypt_modern_with_aad("secret text", "strong-password", aad)?;
let plain = ncrypto::decrypt_modern_with_aad(&token, "strong-password", aad)?;
```

AAD 不进入 token，解密方必须从可信业务字段重新构造完全相同的字节。AAD 错配、口令错误、header 或密文被改动都会导致认证失败。

默认格式为 `NC2.m_cost.t_cost.p_cost.salt.nonce.ciphertext`：

- KDF 使用 Argon2id，默认 `m=65536 KiB`、`t=3`、`p=4`。
- 每次加密从操作系统安全随机源生成 16 字节 salt 和 12 字节 nonce。
- AES-256-GCM 的 16 字节认证标签包含在 `ciphertext` 段。
- 版本、KDF 参数、salt、nonce 和业务 AAD 全部进入认证上下文，不能被静默替换。
- 派生出的原始密钥离开作用域时主动清理。
- 单次明文上限 16 MiB、AAD 上限 64 KiB、口令上限 1024 字节、token 上限 24 MiB；Argon2id 参数也有上下界，避免不可信输入放大内存或 CPU 成本。

既有 `NC1.*`（PBKDF2-HMAC-SHA256 + AES-256-GCM）保持只读兼容，`decrypt_modern*` 会自动识别；所有 `encrypt_modern*` 只生成 NC2。NC1 没有业务 AAD 能力，向 NC1 解密入口传非空 AAD 会明确拒绝。读取 NC1 后可用 NC2 重新加密完成迁移。

如果输入本来就是 32 字节高熵主密钥，并且协议层负责 nonce、方向隔离和重放治理，可使用 `derive_web_aead_key`、`encrypt_web_aead`、`decrypt_web_aead` 这组原始协议接口；口令类输入不要直接走该接口。

## 哈希和口令校验

```rust
let digest = ncrypto::sha256("hello");
let upper = ncrypto::sha256_cased("hello", true);

let hash = ncrypto::bcrypt("password")?;
assert!(ncrypto::bcrypt_check("password", &hash));
```

## KDF 和随机值

```rust
let salt = ncrypto::generate_salt(16);
let key = ncrypto::pbkdf2_default("password", &salt)?;
let aes_key = ncrypto::generate_aes_key(256)?;
```

## AES 兼容入口

```rust
let cipher =
    ncrypto::encrypt_aes_cbc_iv("plain", "1234567890123456", "1234567890123456")?;
let plain =
    ncrypto::decrypt_aes_cbc_iv(&cipher, "1234567890123456", "1234567890123456")?;
```

## RSA / Ed25519

```rust
let (private_key, public_key) = ncrypto::generate_rsa_key_pair(2048)?;
let cipher = ncrypto::encrypt_rsa_oaep("plain", &public_key)?;
let plain = ncrypto::decrypt_rsa_oaep(&cipher, &private_key)?;

let (ed_private, ed_public) = ncrypto::generate_ed25519_key_pair()?;
let sig = ncrypto::sign_ed25519("payload", &ed_private)?;
assert!(ncrypto::verify_ed25519(
    "payload",
    &sig,
    &ed_public
));
```

## 编码

```rust
let encoded = ncrypto::base64_url_encode_str("hello");
let decoded = ncrypto::base64_url_decode_str(&encoded)?;
```

## YML 配置与使用

`ncrypto` 不主动读取 yml。密钥、盐、口令、私钥和 token 必须由业务通过环境变量、密钥管理系统或运行时注入，不应写入公开配置文件。

推荐配置只放算法选择和非敏感参数：

```yaml
crypto:
  mode: modern
  token_version: NC2
  rsa_key_bits: 2048
  bcrypt_cost: 12
  secrets:
    modern_password_env: APP_CRYPTO_PASSWORD
    rsa_private_key_env: APP_RSA_PRIVATE_KEY
```

字段说明：

| 键 | 说明 |
| --- | --- |
| `mode` | 新业务推荐 `modern`。 |
| `token_version` | 新写入固定为 `NC2`；`NC1` 只用于读取已有数据。 |
| `rsa_key_bits` | 新生成 RSA 密钥位数，推荐至少 2048。 |
| `bcrypt_cost` | bcrypt cost。 |
| `secrets.*_env` | 指向环境变量名，而不是直接写密钥值。 |

使用代码：

```rust
let password = std::env::var(&cfg.crypto.secrets.modern_password_env)?;
let token = ncrypto::encrypt_modern("payload", &password)?;
let plain = ncrypto::decrypt_modern(&token, &password)?;
```

旧兼容算法只能用于既有数据互通。新业务不要把 CBC 固定 IV、ECB、私钥加密等兼容入口作为保密方案。
