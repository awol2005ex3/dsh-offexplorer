//! 集群连接配置的解析 / 归一化 / 校验，以及 Kerberos 取票。
//!
//! 配置从 Node 侧以 JSON 字符串传入：既兼容 Offset Explorer 的 connections.json
//! （PascalCase，如 `BootstrapServers` / `SASL` / `TLS`），也接受 dsh 设置里更自然的
//! camelCase。键名先统一规范化，再用 serde 反序列化。

use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::path::Path;
use std::process::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityType {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityType {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_uppercase().replace('-', "_").as_str() {
            "" | "PLAINTEXT" => Some(SecurityType::Plaintext),
            "SSL" => Some(SecurityType::Ssl),
            "SASL_PLAINTEXT" => Some(SecurityType::SaslPlaintext),
            "SASL_SSL" => Some(SecurityType::SaslSsl),
            "SASLSSL" => Some(SecurityType::SaslSsl),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SecurityType::Plaintext => "PLAINTEXT",
            SecurityType::Ssl => "SSL",
            SecurityType::SaslPlaintext => "SASL_PLAINTEXT",
            SecurityType::SaslSsl => "SASL_SSL",
        }
    }

    pub fn uses_sasl(self) -> bool {
        matches!(self, SecurityType::SaslPlaintext | SecurityType::SaslSsl)
    }

    pub fn uses_tls(self) -> bool {
        matches!(self, SecurityType::Ssl | SecurityType::SaslSsl)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    Gssapi,
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslMechanism {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_uppercase().as_str() {
            "" | "GSSAPI" => Some(SaslMechanism::Gssapi),
            "PLAIN" => Some(SaslMechanism::Plain),
            "SCRAM-SHA-256" => Some(SaslMechanism::ScramSha256),
            "SCRAM-SHA-512" => Some(SaslMechanism::ScramSha512),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SaslMechanism::Gssapi => "GSSAPI",
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// 支持 `["a:9092"]` 与 `"a:9092,b:9092"` 两种写法。
fn de_servers<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    let raw = Option::<OneOrMany>::deserialize(deserializer)?;
    Ok(match raw {
        Some(OneOrMany::One(s)) => split_servers(&s),
        Some(OneOrMany::Many(v)) => v,
        None => Vec::new(),
    })
}

fn split_servers(raw: &str) -> Vec<String> {
    raw.split([',', ';', ' ', '\n', '\r'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct KerberosConfig {
    pub krb5_conf_path: String,
    pub realm: String,
    pub cred_source: String,
    pub ccache_path: String,
    pub keytab_path: String,
    pub principal: String,
    pub username: String,
    pub password: String,
    pub auto_kinit: bool,
    pub kinit_path: String,
    pub service: String,
    pub host_template: String,
    pub resolve: String,
    pub disable_pafx_fast: bool,
    pub force_canonical: bool,
}

impl Default for KerberosConfig {
    fn default() -> Self {
        Self {
            krb5_conf_path: String::new(),
            realm: String::new(),
            cred_source: "ccache".into(),
            ccache_path: String::new(),
            keytab_path: String::new(),
            principal: String::new(),
            username: String::new(),
            password: String::new(),
            auto_kinit: false,
            kinit_path: String::new(),
            service: String::new(),
            host_template: String::new(),
            resolve: "literal".into(),
            disable_pafx_fast: false,
            force_canonical: false,
        }
    }
}

impl KerberosConfig {
    pub fn service_name(&self) -> &str {
        let s = self.service.trim();
        if s.is_empty() {
            "kafka"
        } else {
            s
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TlsConfig {
    pub ca_pem_path: String,
    pub client_cert_pem_path: String,
    pub client_key_pem_path: String,
    pub server_name: String,
    pub insecure_skip_verify: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            ca_pem_path: String::new(),
            client_cert_pem_path: String::new(),
            client_key_pem_path: String::new(),
            server_name: String::new(),
            insecure_skip_verify: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ClusterConfig {
    pub id: String,
    pub name: String,
    pub group: String,
    pub description: String,
    #[serde(deserialize_with = "de_servers")]
    pub bootstrap_servers: Vec<String>,
    pub kafka_version: String,
    pub security: String,
    pub sasl: String,
    pub krb: KerberosConfig,
    pub plain_user: String,
    pub plain_pass: String,
    pub scram_authz_id: String,
    pub tls: TlsConfig,
    pub schema_registry_url: String,
    pub max_message_bytes: i32,
    pub allow_auto_create_topic: bool,
    pub consumer_group: String,
    pub dial_timeout_ms: i64,
    pub request_timeout_ms: i64,
    pub extra_config: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            group: String::new(),
            description: String::new(),
            bootstrap_servers: Vec::new(),
            kafka_version: String::new(),
            security: "PLAINTEXT".into(),
            sasl: "GSSAPI".into(),
            krb: KerberosConfig::default(),
            plain_user: String::new(),
            plain_pass: String::new(),
            scram_authz_id: String::new(),
            tls: TlsConfig::default(),
            schema_registry_url: String::new(),
            max_message_bytes: 1024 * 1024,
            allow_auto_create_topic: false,
            consumer_group: String::new(),
            dial_timeout_ms: 10_000,
            request_timeout_ms: 20_000,
            extra_config: String::new(),
        }
    }
}

impl ClusterConfig {
    pub fn security_type(&self) -> Result<SecurityType, String> {
        SecurityType::parse(&self.security)
            .ok_or_else(|| format!("security 无效：{}（可选 PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL）", self.security))
    }

    pub fn sasl_mechanism(&self) -> Result<SaslMechanism, String> {
        SaslMechanism::parse(&self.sasl).ok_or_else(|| {
            format!(
                "sasl 机制无效：{}（可选 GSSAPI / PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512）",
                self.sasl
            )
        })
    }

    pub fn uses_sasl(&self) -> bool {
        self.security_type().map(SecurityType::uses_sasl).unwrap_or(false)
    }

    fn normalize(&mut self) {
        self.id = self.id.trim().to_string();
        self.name = self.name.trim().to_string();
        self.group = self.group.trim().to_string();
        self.bootstrap_servers = self
            .bootstrap_servers
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        self.security = self.security.trim().to_uppercase();
        if self.security.is_empty() {
            self.security = "PLAINTEXT".into();
        }
        self.sasl = self.sasl.trim().to_uppercase();
        if self.sasl.is_empty() {
            self.sasl = "GSSAPI".into();
        }
        self.krb.krb5_conf_path = self.krb.krb5_conf_path.trim().to_string();
        self.krb.ccache_path = strip_file_prefix(self.krb.ccache_path.trim());
        self.krb.keytab_path = self.krb.keytab_path.trim().to_string();
        self.krb.principal = self.krb.principal.trim().to_string();
        self.krb.kinit_path = self.krb.kinit_path.trim().to_string();
        self.max_message_bytes = self.max_message_bytes.clamp(1, 1024 * 1024 * 1024);
        if self.dial_timeout_ms <= 0 {
            self.dial_timeout_ms = 10_000;
        }
        if self.request_timeout_ms <= 0 {
            self.request_timeout_ms = 20_000;
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.bootstrap_servers.is_empty() {
            return Err("bootstrapServers 不能为空，形如 [\"broker1:9092\",\"broker2:9092\"]".into());
        }
        let security = self.security_type()?;
        if security.uses_sasl() {
            match self.sasl_mechanism()? {
                SaslMechanism::Gssapi => {
                    if self.krb.auto_kinit {
                        if self.krb.keytab_path.is_empty() {
                            return Err("勾选 autoKinit 后必须填写 krb.keytabPath".into());
                        }
                        if !self.krb.principal.contains('@') {
                            return Err(format!(
                                "autoKinit 需要带 realm 的 principal（user@REALM），当前：{}",
                                self.krb.principal
                            ));
                        }
                    }
                }
                _ => {
                    if self.plain_user.trim().is_empty() {
                        return Err(format!("{} 需要填写 plainUser", self.sasl_mechanism()?.as_str()));
                    }
                    if self.plain_pass.is_empty() {
                        return Err(format!("{} 需要填写 plainPass", self.sasl_mechanism()?.as_str()));
                    }
                }
            }
        }
        if security.uses_tls() {
            let has_cert = !self.tls.client_cert_pem_path.trim().is_empty();
            let has_key = !self.tls.client_key_pem_path.trim().is_empty();
            if has_cert != has_key {
                return Err("TLS 客户端证书与私钥必须同时提供（clientCertPemPath / clientKeyPemPath）".into());
            }
        }
        Ok(())
    }
}

fn strip_file_prefix(path: &str) -> String {
    let t = path.trim();
    t.strip_prefix("FILE:").unwrap_or(t).to_string()
}

/// 已知缩写键的显式映射；其余键仅把首字母小写。
fn canonical_key(key: &str) -> String {
    match key {
        "SASL" => "sasl".into(),
        "TLS" => "tls".into(),
        "CCachePath" => "ccachePath".into(),
        "CAPemPath" => "caPemPath".into(),
        "SchemaRegistryURL" => "schemaRegistryUrl".into(),
        "SCRAMAuthzID" => "scramAuthzId".into(),
        "LegacyZKAddrs" => "legacyZkAddrs".into(),
        "StartTS" => "startTs".into(),
        "DisablePAFXFast" => "disablePafxFast".into(),
        "KRB5ConfPath" => "krb5ConfPath".into(),
        "BootstrapServers" => "bootstrapServers".into(),
        "PlainUser" => "plainUser".into(),
        "PlainPass" => "plainPass".into(),
        "MaxMessageBytes" => "maxMessageBytes".into(),
        "AllowAutoCreateTopic" => "allowAutoCreateTopic".into(),
        "ConsumerGroup" => "consumerGroup".into(),
        "DialTimeoutMs" => "dialTimeoutMs".into(),
        "RequestTimeoutMs" => "requestTimeoutMs".into(),
        "ExtraConfig" => "extraConfig".into(),
        "KeytabPath" => "keytabPath".into(),
        "CcachePath" => "ccachePath".into(),
        "KinitPath" => "kinitPath".into(),
        "AutoKinit" => "autoKinit".into(),
        other => {
            let mut chars = other.chars();
            match chars.next() {
                Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        }
    }
}

fn canonicalize(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let old = std::mem::take(map);
            for (k, mut v) in old {
                canonicalize(&mut v);
                map.insert(canonical_key(&k), v);
            }
        }
        Value::Array(items) => {
            for item in items {
                canonicalize(item);
            }
        }
        _ => {}
    }
}

/// 解析并校验一份集群配置 JSON。
pub fn parse_cluster(json: &str) -> Result<ClusterConfig, String> {
    let text = json.trim();
    if text.is_empty() {
        return Err("集群配置为空".into());
    }
    let mut value: Value =
        serde_json::from_str(text).map_err(|e| format!("集群配置不是合法 JSON：{e}"))?;
    canonicalize(&mut value);
    let mut cluster: ClusterConfig =
        serde_json::from_value(value).map_err(|e| format!("集群配置字段解析失败：{e}"))?;
    cluster.normalize();
    cluster.validate()?;
    if cluster.id.is_empty() {
        cluster.id = if cluster.name.is_empty() {
            cluster
                .bootstrap_servers
                .first()
                .cloned()
                .unwrap_or_else(|| "cluster".into())
        } else {
            cluster.name.clone()
        };
    }
    Ok(cluster)
}

/// Kerberos 凭据缓存文件路径。未配置时按 principal 生成固定路径。
pub fn ccache_file(c: &ClusterConfig) -> String {
    if !c.krb.ccache_path.is_empty() {
        return c.krb.ccache_path.clone();
    }
    let principal = c.krb.principal.trim();
    let base = principal.split('@').next().unwrap_or("krb5");
    let name: String = base
        .chars()
        .map(|ch| if ch.is_alphanumeric() { ch } else { '_' })
        .collect();
    std::env::temp_dir()
        .join(format!("dsh_offexplorer_krb5cc_{name}"))
        .display()
        .to_string()
}

/// 构造不弹出控制台窗口的子进程（Windows 下宿主是后台进程）。
fn quiet_command(program: &str) -> Command {
    #[allow(unused_mut)]
    let mut cmd = Command::new(program);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    cmd
}

fn resolve_kinit(explicit: &str) -> Option<String> {
    let explicit = explicit.trim();
    if !explicit.is_empty() {
        return Some(explicit.to_string());
    }
    if quiet_command("kinit").arg("--version").output().is_ok() {
        return Some("kinit".to_string());
    }
    for guess in [
        r"C:\Program Files\MIT\Kerberos\bin\kinit.exe",
        r"D:\MIT\Kerberos\bin\kinit.exe",
        "/usr/bin/kinit",
        "/usr/local/bin/kinit",
    ] {
        if Path::new(guess).is_file() {
            return Some(guess.to_string());
        }
    }
    None
}

/// GSSAPI 需要在下发 librdkafka 配置之前落地的进程级环境。
///
/// 注意：`KRB5CCNAME` / `KRB5_CONFIG` 是**进程级**状态，宿主重启后失效；
/// Windows 上 `RDKAFKA_SASL_MIT=1` 才会启用 vendored 的 MIT provider
/// （默认 SSPI 会把 SPN 主机名转大写，对接大小写敏感 KDC 必然失败）。
pub fn apply_gssapi_env(c: &ClusterConfig) -> Result<Option<String>, String> {
    if !(c.uses_sasl() && c.sasl_mechanism()? == SaslMechanism::Gssapi) {
        return Ok(None);
    }
    if cfg!(windows) {
        std::env::set_var("RDKAFKA_SASL_MIT", "1");
    }
    if !c.krb.krb5_conf_path.is_empty() {
        std::env::set_var("KRB5_CONFIG", &c.krb.krb5_conf_path);
    }
    let mut message = None;
    if c.krb.auto_kinit {
        if let Some(msg) = kinit(c)? {
            message = Some(msg);
        }
    }
    if !c.krb.ccache_path.is_empty() || c.krb.auto_kinit {
        let ccache = ccache_file(c);
        std::env::set_var("KRB5CCNAME", format!("FILE:{ccache}"));
    }
    Ok(message)
}

/// 按配置执行一次 `kinit -c FILE:<ccache> -k -t <keytab> <principal>`。
///
/// 未勾选 autoKinit 时返回 `Ok(None)`（沿用宿主已有的票据）。
pub fn kinit(c: &ClusterConfig) -> Result<Option<String>, String> {
    if !c.krb.auto_kinit {
        return Ok(None);
    }
    let principal = c.krb.principal.trim();
    let keytab = c.krb.keytab_path.trim();
    if keytab.is_empty() || !principal.contains('@') {
        return Ok(None);
    }
    let kinit = resolve_kinit(&c.krb.kinit_path).ok_or_else(|| {
        "已勾选 autoKinit，但找不到 kinit（可在 krb.kinitPath 指定可执行文件路径）".to_string()
    })?;
    let ccache = ccache_file(c);
    let mut cmd = quiet_command(&kinit);
    cmd.arg("-c").arg(format!("FILE:{ccache}"));
    cmd.arg("-k").arg("-t").arg(keytab).arg(principal);
    if !c.krb.krb5_conf_path.is_empty() {
        cmd.env("KRB5_CONFIG", &c.krb.krb5_conf_path);
    }
    let out = cmd
        .output()
        .map_err(|e| format!("执行 {kinit} 失败：{e}"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let err = if err.is_empty() {
            format!("退出码 {:?}", out.status.code())
        } else {
            err
        };
        return Err(format!("kinit 取票失败（{principal}）：{err}"));
    }
    std::env::set_var("KRB5CCNAME", format!("FILE:{ccache}"));
    Ok(Some(format!("kinit 取票成功：{principal} → {ccache}")))
}
