# ncrypto

`ncrypto` 提供哈希、HMAC/KDF、AES、RSA、Ed25519、Base64URL、随机值和现代口令加密工具。它同时保留若干旧系统兼容算法。

业务项目通过门面开启 `crypto`：

```toml
[dependencies]
nasa = { version = "1", features = ["crypto"] }
```

## 安全边界

新业务优先使用 `encrypt_modern` / `decrypt_modern`。旧系统兼容函数如 AES-ECB、CBC(IV=Key)、RSA PKCS#1 v1.5、RSA 私钥“加密”等只用于和既有系统逐字节互通，不应作为新系统保密边界。

默认构建不会执行 PKCS#1 v1.5 私钥解密或历史私钥 type-1 运算；RS256 公钥验签、RSA-OAEP、
Ed25519、现代 AEAD、哈希和 KDF 不受影响。确有迁移合同的调用方必须单独启用：

```toml
[dependencies]
nasa = { version = "1", features = ["crypto-legacy-rsa"] }
```

该 feature 不进入 `nasa/full`。Web 端点还必须启用 `web-crypto-legacy-rsa`，并在
`LocalCryptoProvider` 构造时显式打开运行时风险门；缺少任一层都会在启动审计或执行时拒绝。

## 现代口令加密

```rust
let token = nasa::crypto::encrypt_modern("secret text", "strong-password")?;
let plain = nasa::crypto::decrypt_modern(&token, "strong-password")?;
assert_eq!(plain, "secret text");
```

二进制数据：

```rust
let token = nasa::crypto::encrypt_modern_bytes(b"payload", "strong-password")?;
let bytes = nasa::crypto::decrypt_modern_bytes(&token, "strong-password")?;
```

现代 token 使用随机 salt、PBKDF2-HMAC-SHA256、AES-256-GCM，并带 `NC1.*` 自描述前缀。

## 哈希和口令校验

```rust
let digest = nasa::crypto::sha256("hello");
let upper = nasa::crypto::sha256_cased("hello", true);

let hash = nasa::crypto::bcrypt("password")?;
assert!(nasa::crypto::bcrypt_check("password", &hash));
```

## KDF 和随机值

```rust
let salt = nasa::crypto::generate_salt(16);
let key = nasa::crypto::pbkdf2_default("password", &salt)?;
let aes_key = nasa::crypto::generate_aes_key(256)?;
```

## AES 兼容入口

```rust
let cipher =
    nasa::crypto::encrypt_aes_cbc_iv("plain", "1234567890123456", "1234567890123456")?;
let plain =
    nasa::crypto::decrypt_aes_cbc_iv(&cipher, "1234567890123456", "1234567890123456")?;
```

## RSA / Ed25519

```rust
let (private_key, public_key) = nasa::crypto::generate_rsa_key_pair(2048)?;
let cipher = nasa::crypto::encrypt_rsa_oaep("plain", &public_key)?;
let plain = nasa::crypto::decrypt_rsa_oaep(&cipher, &private_key)?;

let (ed_private, ed_public) = nasa::crypto::generate_ed25519_key_pair()?;
let sig = nasa::crypto::sign_ed25519("payload", &ed_private)?;
assert!(nasa::crypto::verify_ed25519(
    "payload",
    &sig,
    &ed_public
));
```

## 编码

```rust
let encoded = nasa::crypto::base64_url_encode_str("hello");
let decoded = nasa::crypto::base64_url_decode_str(&encoded)?;
```

## YML 配置与使用

`ncrypto` 不主动读取 yml。密钥、盐、口令、私钥和 token 必须由业务通过环境变量、密钥管理系统或运行时注入，不应写入公开配置文件。

推荐配置只放算法选择和非敏感参数：

```yaml
crypto:
  mode: modern
  pbkdf2_iterations: 210000
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
| `pbkdf2_iterations` | 现代 token KDF 迭代次数；过低应拒绝启动。 |
| `rsa_key_bits` | 新生成 RSA 密钥位数，推荐至少 2048。 |
| `bcrypt_cost` | bcrypt cost。 |
| `secrets.*_env` | 指向环境变量名，而不是直接写密钥值。 |

使用代码：

```rust
let password = std::env::var(&cfg.crypto.secrets.modern_password_env)?;
let token = nasa::crypto::encrypt_modern("payload", &password)?;
let plain = nasa::crypto::decrypt_modern(&token, &password)?;
```

旧兼容算法只能用于既有数据互通。新业务不要把 CBC 固定 IV、ECB、私钥加密等兼容入口作为保密方案。
