//! Kafka 服务层：rdkafka 封装。
//!
//! 设计要点（与 dsh-hdfs 一致）：
//! - **无状态**：每次调用新建 consumer / producer / admin，不做进程级连接缓存；
//!   代价是一次 TCP + 握手，换取无锁、无泄漏、可并发。
//! - **阻塞调用放进 `spawn_blocking`**：metadata / poll / kinit 都是阻塞的，
//!   不能占死 napi 的 tokio worker。
//! - **错误带上下文**：bootstrap、topic、group 都写进 message，宿主侧能直接展示。

use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdkafka::admin::{
    AdminClient, AdminOptions, ConfigSource, NewPartitions, NewTopic, ResourceSpecifier,
    TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{ClientConfig, FromClientConfig};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;

use crate::config::{self, ClusterConfig, SaslMechanism};
use crate::types::{
    BrokerInfo, BrowseResult, ClusterIdentity, ConfigEntry, GroupDetail, GroupMember, GroupSummary,
    HealthReport, KinitResult, MessageHeader, MessageRecord, PartitionAssignment, PartitionDetail,
    PartitionIssue, ProduceResult, TopicDetail, TopicIssue, TopicSummary, Watermarks,
};

const META_TIMEOUT: Duration = Duration::from_secs(5);

fn group_id(c: &ClusterConfig) -> String {
    if !c.consumer_group.trim().is_empty() {
        return c.consumer_group.trim().to_string();
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("dsh-offexplorer-browse-{}-{nanos}", std::process::id())
}

fn apply_extra_config(cfg: &mut ClientConfig, raw: &str) -> Result<(), String> {
    let text = raw.trim();
    if text.is_empty() {
        return Ok(());
    }
    let map: HashMap<String, serde_json::Value> =
        serde_json::from_str(text).map_err(|e| format!("extraConfig 不是合法 JSON 对象：{e}"))?;
    for (key, value) in map {
        let text = match value {
            serde_json::Value::String(s) => s,
            other => other.to_string(),
        };
        cfg.set(key, text);
    }
    Ok(())
}

/// 构造一份 librdkafka 客户端配置（不含 group.id）。
fn client_config(c: &ClusterConfig) -> Result<ClientConfig, String> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", c.bootstrap_servers.join(","));
    cfg.set(
        "socket.connection.setup.timeout.ms",
        c.dial_timeout_ms.to_string(),
    );
    cfg.set("connections.max.idle.ms", "540000");
    cfg.set("fetch.wait.max.ms", "300");
    cfg.set("isolation.level", "read_committed");
    cfg.set("enable.auto.commit", "false");
    cfg.set("auto.offset.reset", "earliest");
    if c.allow_auto_create_topic {
        cfg.set("allow.auto.create.topics", "true");
    }
    let fetch_max = (c.max_message_bytes as i64 * 64).max(1 << 20);
    cfg.set("fetch.max.bytes", fetch_max.to_string());
    let part_max = (c.max_message_bytes as i64 * 8).max(1 << 16);
    cfg.set("max.partition.fetch.bytes", part_max.to_string());

    let security = c.security_type()?;
    cfg.set("security.protocol", security.as_str().to_lowercase());

    if security.uses_tls() {
        if !c.tls.ca_pem_path.trim().is_empty() {
            cfg.set("ssl.ca.location", c.tls.ca_pem_path.trim());
        }
        if !c.tls.client_cert_pem_path.trim().is_empty()
            && !c.tls.client_key_pem_path.trim().is_empty()
        {
            cfg.set("ssl.certificate.location", c.tls.client_cert_pem_path.trim());
            cfg.set("ssl.key.location", c.tls.client_key_pem_path.trim());
        }
        if c.tls.server_name.trim().is_empty() {
            cfg.set("ssl.endpoint.identification.algorithm", "none");
        } else {
            cfg.set("ssl.endpoint.identification.algorithm", "https");
        }
        if c.tls.insecure_skip_verify {
            cfg.set("enable.ssl.certificate.verification", "false");
        }
    }

    if security.uses_sasl() {
        let mechanism = c.sasl_mechanism()?;
        match mechanism {
            SaslMechanism::Gssapi => {
                cfg.set("sasl.mechanisms", "GSSAPI");
                cfg.set("sasl.kerberos.service.name", c.krb.service_name());
                if !cfg!(windows) {
                    if !c.krb.principal.is_empty() {
                        cfg.set("sasl.kerberos.principal", &c.krb.principal);
                    }
                    if !c.krb.keytab_path.is_empty() {
                        cfg.set("sasl.kerberos.keytab", &c.krb.keytab_path);
                    }
                    if !c.krb.ccache_path.is_empty() {
                        cfg.set("sasl.kerberos.ccache", &c.krb.ccache_path);
                    }
                    if !c.krb.krb5_conf_path.is_empty() {
                        std::env::set_var("KRB5_CONFIG", &c.krb.krb5_conf_path);
                    }
                }
            }
            SaslMechanism::Plain | SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                cfg.set("sasl.mechanisms", mechanism.as_str());
                cfg.set("sasl.username", c.plain_user.trim());
                cfg.set("sasl.password", &c.plain_pass);
            }
        }
    }

    apply_extra_config(&mut cfg, &c.extra_config)?;
    Ok(cfg)
}

/// 取票 + 进程级 GSSAPI 环境准备（必须在创建客户端之前调用）。
fn prepare(c: &ClusterConfig) -> Result<(), String> {
    if let Some(message) = config::apply_gssapi_env(c)? {
        eprintln!("dsh-offexplorer-native: {message}");
    }
    Ok(())
}

fn build_consumer(c: &ClusterConfig) -> Result<BaseConsumer, String> {
    prepare(c)?;
    let mut cfg = client_config(c)?;
    cfg.set("group.id", group_id(c));
    BaseConsumer::from_config(&cfg).map_err(|e| format!("创建 Kafka consumer 失败：{e}"))
}

fn build_producer(c: &ClusterConfig) -> Result<FutureProducer, String> {
    prepare(c)?;
    let mut cfg = client_config(c)?;
    // producer 专属：consumer/admin 设这些会触发 CONFWARN 且被忽略。
    cfg.set("request.timeout.ms", c.request_timeout_ms.to_string());
    cfg.set("message.timeout.ms", c.request_timeout_ms.to_string());
    cfg.set("retries", "2");
    FutureProducer::from_config(&cfg).map_err(|e| format!("创建 Kafka producer 失败：{e}"))
}

fn admin(c: &ClusterConfig) -> Result<AdminClient<DefaultClientContext>, String> {
    prepare(c)?;
    let cfg = client_config(c)?;
    AdminClient::from_config(&cfg).map_err(|e| format!("创建 Kafka admin client 失败：{e}"))
}

fn conn_error(c: &ClusterConfig, stage: &str, err: impl std::fmt::Display) -> String {
    let mut message = format!(
        "{stage} {} 失败：{err}",
        c.bootstrap_servers.join(",")
    );
    if c.uses_sasl()
        && c.sasl_mechanism().ok() == Some(SaslMechanism::Gssapi)
    {
        message.push_str(&format!(
            "\n提示：GSSAPI 读取 ccache {}。可勾选 autoKinit 并填写 keytab + principal，\
             或先手动执行 kinit -c FILE:<ccache> -k -t <keytab> <principal>；\
             再确认 krb5.conf 可达 KDC、sasl.kerberos.service.name（当前 {}）与 KDC 的 SPN 一致。",
            config::ccache_file(c),
            c.krb.service_name()
        ));
    }
    message
}

async fn blocking<T, F>(label: &'static str, f: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| format!("{label} 任务执行失败：{e}"))?
}

// ────────────────────────────────────────────────────────────────────────────
// 基础探测
// ────────────────────────────────────────────────────────────────────────────

pub async fn ping(c: ClusterConfig) -> Result<bool, String> {
    blocking("ping", move || {
        let consumer = build_consumer(&c)?;
        consumer
            .fetch_metadata(None, META_TIMEOUT)
            .map_err(|e| conn_error(&c, "连接", e))?;
        Ok(true)
    })
    .await
}

pub async fn cluster_identity(c: ClusterConfig) -> Result<ClusterIdentity, String> {
    blocking("clusterInfo", move || {
        let consumer = build_consumer(&c)?;
        let md = consumer
            .fetch_metadata(None, META_TIMEOUT)
            .map_err(|e| conn_error(&c, "读取集群元数据", e))?;
        let controller_id = md.orig_broker_id();
        let cluster_id = consumer
            .client()
            .fetch_cluster_id(META_TIMEOUT)
            .unwrap_or_default();
        Ok(ClusterIdentity {
            cluster_id,
            controller_id,
        })
    })
    .await
}

pub async fn list_brokers(c: ClusterConfig) -> Result<Vec<BrokerInfo>, String> {
    blocking("listBrokers", move || {
        let consumer = build_consumer(&c)?;
        let md = consumer
            .fetch_metadata(None, META_TIMEOUT)
            .map_err(|e| conn_error(&c, "读取 broker 列表", e))?;
        let controller = md.orig_broker_id();
        let seeds: Vec<String> = c
            .bootstrap_servers
            .iter()
            .map(|s| s.to_lowercase())
            .collect();
        let mut out: Vec<BrokerInfo> = md
            .brokers()
            .iter()
            .map(|b| {
                let host = b.host().to_string();
                let addr = format!("{}:{}", host, b.port()).to_lowercase();
                BrokerInfo {
                    node_id: b.id(),
                    is_controller: b.id() == controller,
                    is_seed: seeds.contains(&addr) || seeds.contains(&host.to_lowercase()),
                    host,
                    port: b.port(),
                    rack: String::new(),
                }
            })
            .collect();
        out.sort_by_key(|b| b.node_id);
        Ok(out)
    })
    .await
}

fn summarize_topic(topic: &rdkafka::metadata::MetadataTopic) -> TopicSummary {
    let parts = topic.partitions();
    let offline = parts.iter().filter(|p| p.leader() < 0).count() as i32;
    let under = parts
        .iter()
        .filter(|p| p.replicas().len() > p.isr().len())
        .count() as i32;
    let replication = parts
        .first()
        .map(|p| p.replicas().len() as i32)
        .unwrap_or(0);
    let name = topic.name().to_string();
    TopicSummary {
        internal: name.starts_with("__"),
        name,
        partition_count: parts.len() as i32,
        replication,
        usable: topic.error().is_none(),
        unusable_reason: topic
            .error()
            .map(|e| format!("{e:?}"))
            .unwrap_or_default(),
        offline_partitions: offline,
        under_replicated: under,
    }
}

pub async fn list_topics(c: ClusterConfig) -> Result<Vec<TopicSummary>, String> {
    blocking("listTopics", move || {
        let consumer = build_consumer(&c)?;
        let md = consumer
            .fetch_metadata(None, META_TIMEOUT)
            .map_err(|e| conn_error(&c, "读取 topic 列表", e))?;
        let mut out: Vec<TopicSummary> = md.topics().iter().map(summarize_topic).collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    })
    .await
}

pub async fn fetch_watermarks(
    c: ClusterConfig,
    topic: String,
    partition: i32,
) -> Result<Watermarks, String> {
    blocking("watermarks", move || {
        let consumer = build_consumer(&c)?;
        let (low, high) = consumer
            .fetch_watermarks(&topic, partition, META_TIMEOUT)
            .map_err(|e| format!("读取 {topic}[{partition}] 水位失败：{e}"))?;
        Ok(Watermarks {
            topic,
            partition,
            low,
            high,
        })
    })
    .await
}

pub async fn topic_detail(c: ClusterConfig, topic: String) -> Result<TopicDetail, String> {
    blocking("topicDetail", move || {
        let consumer = build_consumer(&c)?;
        let md = consumer
            .fetch_metadata(Some(&topic), META_TIMEOUT)
            .map_err(|e| conn_error(&c, "读取 topic 元数据", e))?;
        let live: HashSet<i32> = md.brokers().iter().map(|b| b.id()).collect();
        let t = md
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| format!("topic 不存在：{topic}"))?;
        let summary = summarize_topic(t);
        let mut partitions = Vec::new();
        for p in t.partitions() {
            let (earliest, latest) = consumer
                .fetch_watermarks(&topic, p.id(), META_TIMEOUT)
                .unwrap_or((0, 0));
            let offline_reps: Vec<i32> = p
                .replicas()
                .iter()
                .copied()
                .filter(|r| !live.contains(r))
                .collect();
            partitions.push(PartitionDetail {
                partition: p.id(),
                leader: p.leader(),
                leader_epoch: 0,
                replicas: p.replicas().to_vec(),
                isr: p.isr().to_vec(),
                offline_reps,
                earliest,
                latest,
                size: (latest - earliest).max(0),
                err: if p.leader() < 0 { "离线".into() } else { String::new() },
            });
        }
        partitions.sort_by_key(|p| p.partition);
        Ok(TopicDetail {
            topic: summary,
            partitions,
        })
    })
    .await
}

// ────────────────────────────────────────────────────────────────────────────
// 配置读取
// ────────────────────────────────────────────────────────────────────────────

fn config_entry(
    name: &str,
    value: Option<&str>,
    is_default: bool,
    is_read_only: bool,
    is_sensitive: bool,
    source: &ConfigSource,
) -> ConfigEntry {
    ConfigEntry {
        name: name.to_string(),
        value: value.unwrap_or_default().to_string(),
        default_value: if is_default { "默认".into() } else { "覆盖".into() },
        origin: config_source_label(source).to_string(),
        docs: String::new(),
        synthetic: false,
        read_only: is_read_only,
        is_sensitive,
        source: source_as_i8(source),
    }
}

fn config_source_label(source: &ConfigSource) -> &'static str {
    match source {
        ConfigSource::Unknown => "未知",
        ConfigSource::DynamicTopic => "动态 topic 配置",
        ConfigSource::DynamicBroker => "动态 broker 配置",
        ConfigSource::DynamicDefaultBroker => "动态默认 broker 配置",
        ConfigSource::StaticBroker => "静态 broker 配置",
        ConfigSource::Default => "默认",
    }
}

fn source_as_i8(source: &ConfigSource) -> i32 {
    match source {
        ConfigSource::Unknown => 0,
        ConfigSource::DynamicTopic => 1,
        ConfigSource::DynamicBroker => 2,
        ConfigSource::DynamicDefaultBroker => 3,
        ConfigSource::StaticBroker => 4,
        ConfigSource::Default => 5,
    }
}

async fn describe_configs(
    c: ClusterConfig,
    resource: ResourceSpecifier<'_>,
    label: &'static str,
) -> Result<Vec<ConfigEntry>, String> {
    let client = admin(&c)?;
    let options = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
    let results = client
        .describe_configs(&[resource], &options)
        .await
        .map_err(|e| format!("{label}：{e}"))?;
    let mut out = Vec::new();
    for result in results {
        match result {
            Ok(config_resource) => {
                for entry in &config_resource.entries {
                    out.push(config_entry(
                        &entry.name,
                        entry.value.as_deref(),
                        entry.is_default,
                        entry.is_read_only,
                        entry.is_sensitive,
                        &entry.source,
                    ));
                }
            }
            Err(code) => {
                return Err(format!("{label} 失败：{code:?}"));
            }
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

pub async fn topic_configs(c: ClusterConfig, topic: String) -> Result<Vec<ConfigEntry>, String> {
    if topic.trim().is_empty() {
        return Err("topic 不能为空".into());
    }
    let resource = ResourceSpecifier::Topic(topic.as_str());
    describe_configs(c, resource, "读取 topic 配置").await
}

pub async fn broker_configs(c: ClusterConfig, broker_id: i32) -> Result<Vec<ConfigEntry>, String> {
    let resource = ResourceSpecifier::Broker(broker_id);
    describe_configs(c, resource, "读取 broker 配置").await
}

// ────────────────────────────────────────────────────────────────────────────
// 消费者组
// ────────────────────────────────────────────────────────────────────────────

pub async fn list_groups(c: ClusterConfig) -> Result<Vec<GroupSummary>, String> {
    blocking("listGroups", move || {
        let consumer = build_consumer(&c)?;
        let groups = consumer
            .fetch_group_list(None, META_TIMEOUT)
            .map_err(|e| conn_error(&c, "读取消费者组列表", e))?;
        let mut out: Vec<GroupSummary> = groups
            .groups()
            .iter()
            .map(|g| GroupSummary {
                name: g.name().to_string(),
                state: g.state().to_string(),
                protocol: g.protocol().to_string(),
                protocol_type: g.protocol_type().to_string(),
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    })
    .await
}

/// 解析 consumer protocol 的 assignment 字节（version + topics + partitions）。
fn parse_assignment(bytes: &[u8]) -> Vec<PartitionAssignment> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    let read_i16 = |pos: &mut usize| -> Option<i16> {
        if *pos + 2 > bytes.len() {
            return None;
        }
        let value = i16::from_be_bytes([bytes[*pos], bytes[*pos + 1]]);
        *pos += 2;
        Some(value)
    };
    let read_i32 = |pos: &mut usize| -> Option<i32> {
        if *pos + 4 > bytes.len() {
            return None;
        }
        let value = i32::from_be_bytes([
            bytes[*pos],
            bytes[*pos + 1],
            bytes[*pos + 2],
            bytes[*pos + 3],
        ]);
        *pos += 4;
        Some(value)
    };
    let read_string = |pos: &mut usize| -> Option<String> {
        let len = read_i16(pos)?;
        if len < 0 || *pos + len as usize > bytes.len() {
            return None;
        }
        let value = String::from_utf8_lossy(&bytes[*pos..*pos + len as usize]).to_string();
        *pos += len as usize;
        Some(value)
    };

    let _version = match read_i16(&mut pos) {
        Some(v) => v,
        None => return out,
    };
    let topic_count = match read_i32(&mut pos) {
        Some(v) if (0..=10_000).contains(&v) => v,
        _ => return out,
    };
    for _ in 0..topic_count {
        let topic = match read_string(&mut pos) {
            Some(t) => t,
            None => return out,
        };
        let partition_count = match read_i32(&mut pos) {
            Some(v) if (0..=100_000).contains(&v) => v,
            _ => return out,
        };
        for _ in 0..partition_count {
            match read_i32(&mut pos) {
                Some(partition) => out.push(PartitionAssignment { topic: topic.clone(), partition }),
                None => return out,
            }
        }
        // user_data: int32 length + bytes
        match read_i32(&mut pos) {
            Some(len) if len >= 0 && pos + len as usize <= bytes.len() => {
                pos += len as usize;
            }
            _ => return out,
        }
    }
    out
}

pub async fn group_detail(c: ClusterConfig, group: String) -> Result<GroupDetail, String> {
    blocking("groupDetail", move || {
        let consumer = build_consumer(&c)?;
        let groups = consumer
            .fetch_group_list(Some(&group), META_TIMEOUT)
            .map_err(|e| conn_error(&c, "读取消费者组详情", e))?;
        let mut detail = GroupDetail {
            name: group.clone(),
            state: String::new(),
            protocol: String::new(),
            protocol_type: String::new(),
            members: Vec::new(),
        };
        if let Some(g) = groups.groups().iter().find(|g| g.name() == group) {
            detail.state = g.state().to_string();
            detail.protocol = g.protocol().to_string();
            detail.protocol_type = g.protocol_type().to_string();
            for member in g.members() {
                let assignments = member
                    .assignment()
                    .map(parse_assignment)
                    .unwrap_or_default();
                detail.members.push(GroupMember {
                    member_id: member.id().to_string(),
                    client_id: member.client_id().to_string(),
                    client_host: member.client_host().to_string(),
                    assignments,
                });
            }
        }
        detail.members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
        Ok(detail)
    })
    .await
}

// ────────────────────────────────────────────────────────────────────────────
// 消息浏览 / 生产
// ────────────────────────────────────────────────────────────────────────────

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64[((triple >> 18) & 0x3f) as usize] as char);
        out.push(B64[((triple >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            B64[((triple >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[(triple & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// 返回（base64, 文本, 是否截断, 原始长度）。
fn decode_preview(bytes: &[u8], max_bytes: usize) -> (String, String, bool, i64) {
    let truncated = bytes.len() > max_bytes;
    let slice = &bytes[..bytes.len().min(max_bytes)];
    (
        base64_encode(slice),
        String::from_utf8_lossy(slice).to_string(),
        truncated,
        bytes.len() as i64,
    )
}

fn message_record(message: &rdkafka::message::BorrowedMessage<'_>, max_bytes: usize) -> MessageRecord {
    let key = message.key().unwrap_or_default();
    let value = message.payload().unwrap_or_default();
    let (key_base64, key_text, key_truncated, _) = decode_preview(key, max_bytes);
    let (value_base64, value_text, value_truncated, value_length) =
        decode_preview(value, max_bytes);
    let mut headers = Vec::new();
    if let Some(headers_raw) = message.headers() {
        for header in headers_raw.iter() {
            let (value_base64, value_text, _, _) =
                decode_preview(header.value.unwrap_or_default(), max_bytes);
            headers.push(MessageHeader {
                key: header.key.to_string(),
                value_base64,
                value_text,
            });
        }
    }
    MessageRecord {
        topic: message.topic().to_string(),
        partition: message.partition(),
        offset: message.offset(),
        timestamp_ms: message.timestamp().to_millis(),
        key_base64,
        key_text,
        value_base64,
        value_text,
        value_length,
        headers,
        truncated: key_truncated || value_truncated,
    }
}

fn resolve_partitions(
    consumer: &BaseConsumer,
    topic: &str,
    partitions: &[i32],
) -> Result<Vec<i32>, String> {
    if !partitions.is_empty() {
        return Ok(partitions.to_vec());
    }
    let md = consumer
        .fetch_metadata(Some(topic), META_TIMEOUT)
        .map_err(|e| format!("读取 topic {topic} 元数据失败：{e}"))?;
    let t = md
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| format!("topic 不存在：{topic}"))?;
    Ok(t.partitions().iter().map(|p| p.id()).collect())
}

pub async fn browse(
    c: ClusterConfig,
    topic: String,
    partitions: Vec<i32>,
    start: String,
    spec_offset: i64,
    per_part: i32,
    limit: i32,
    max_value_bytes: i32,
) -> Result<BrowseResult, String> {
    let per_part = per_part.clamp(1, 10_000);
    let limit = limit.clamp(1, 100_000);
    let max_value_bytes = max_value_bytes.clamp(64, 1024 * 1024) as usize;
    blocking("browse", move || {
        let consumer = build_consumer(&c)?;
        let parts = resolve_partitions(&consumer, &topic, &partitions)?;
        if parts.is_empty() {
            return Err(format!("topic {topic} 没有可读分区"));
        }

        let start_mode = start.trim().to_uppercase();
        let mut tpl = TopicPartitionList::new();

        if start_mode == "TIMESTAMP" {
            let mut timestamps = TopicPartitionList::new();
            for partition in &parts {
                timestamps
                    .add_partition_offset(&topic, *partition, Offset::Offset(spec_offset.max(0)))
                    .map_err(|e| format!("构造时间戳查询失败：{e}"))?;
            }
            let resolved = consumer
                .offsets_for_times(timestamps, Duration::from_secs(10))
                .map_err(|e| format!("按时间戳定位 offset 失败：{e}"))?;
            for element in resolved.elements() {
                let offset = match element.offset() {
                    Offset::Offset(value) if value >= 0 => Offset::Offset(value),
                    _ => Offset::Beginning,
                };
                tpl.add_partition_offset(&topic, element.partition(), offset)
                    .map_err(|e| format!("构造分区分配失败：{e}"))?;
            }
        } else {
            for partition in &parts {
                let offset = match start_mode.as_str() {
                    "OLDEST" | "EARLIEST" => Offset::Beginning,
                    "SPECIFIC" => Offset::Offset(spec_offset.max(0)),
                    // NEWEST：从尾部往前 per_part 条
                    _ => {
                        let (_, high) = consumer
                            .fetch_watermarks(&topic, *partition, META_TIMEOUT)
                            .map_err(|e| format!("读取 {topic}[{partition}] 水位失败：{e}"))?;
                        Offset::Offset((high - per_part as i64).max(0))
                    }
                };
                tpl.add_partition_offset(&topic, *partition, offset)
                    .map_err(|e| format!("构造分区分配失败：{e}"))?;
            }
        }

        consumer
            .assign(&tpl)
            .map_err(|e| format!("分配分区失败：{e}"))?;

        let mut messages = Vec::new();
        let mut counts: HashMap<i32, i32> = HashMap::new();
        let mut done: HashSet<i32> = HashSet::new();
        let idle_limit = parts.len().clamp(2, 20);
        let mut idle = 0usize;

        while messages.len() < limit as usize && done.len() < parts.len() {
            match consumer.poll(Duration::from_millis(300)) {
                Some(Ok(message)) => {
                    idle = 0;
                    let partition = message.partition();
                    let count = counts.entry(partition).or_insert(0);
                    *count += 1;
                    messages.push(message_record(&message, max_value_bytes));
                    if *count >= per_part {
                        done.insert(partition);
                    }
                }
                Some(Err(err)) => return Err(format!("拉取消息失败：{err}")),
                None => {
                    idle += 1;
                    if idle >= idle_limit {
                        break;
                    }
                }
            }
        }

        messages.sort_by_key(|m| (m.partition, m.offset));
        // 命中总上限，或有分区达到单分区上限（后面还有消息）都算截断。
        let truncated = messages.len() >= limit as usize || !done.is_empty();
        Ok(BrowseResult {
            messages,
            truncated,
            partitions: parts,
        })
    })
    .await
}

pub async fn produce(
    c: ClusterConfig,
    topic: String,
    key: String,
    value: String,
    partition: i32,
) -> Result<ProduceResult, String> {
    if topic.trim().is_empty() {
        return Err("topic 不能为空".into());
    }
    let producer = build_producer(&c)?;
    let mut record = FutureRecord::to(&topic).payload(value.as_bytes());
    if !key.is_empty() {
        record = record.key(key.as_bytes());
    }
    if partition >= 0 {
        record = record.partition(partition);
    }
    let (partition, offset) = producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(err, _)| format!("生产消息到 {topic} 失败：{err}"))?;
    Ok(ProduceResult { partition, offset })
}

// ────────────────────────────────────────────────────────────────────────────
// Topic 管理
// ────────────────────────────────────────────────────────────────────────────

pub async fn create_topic(
    c: ClusterConfig,
    name: String,
    partitions: i32,
    replication: i32,
) -> Result<bool, String> {
    if name.trim().is_empty() {
        return Err("topic 名称不能为空".into());
    }
    if partitions <= 0 {
        return Err("partitions 必须大于 0".into());
    }
    if replication <= 0 {
        return Err("replication 必须大于 0（按集群副本数填写）".into());
    }
    let client = admin(&c)?;
    let topic = NewTopic::new(
        &name,
        partitions,
        TopicReplication::Fixed(replication),
    );
    let options = AdminOptions::new().request_timeout(Some(Duration::from_secs(15)));
    let results = client
        .create_topics(&[topic], &options)
        .await
        .map_err(|e| format!("创建 topic 失败：{e}"))?;
    for result in results {
        if let Err((what, code)) = result {
            return Err(format!("创建 topic {name} 失败：{what}（{code:?}）"));
        }
    }
    Ok(true)
}

pub async fn delete_topic(c: ClusterConfig, name: String) -> Result<bool, String> {
    if name.trim().is_empty() {
        return Err("topic 名称不能为空".into());
    }
    let client = admin(&c)?;
    let options = AdminOptions::new().request_timeout(Some(Duration::from_secs(15)));
    let results = client
        .delete_topics(&[name.as_str()], &options)
        .await
        .map_err(|e| format!("删除 topic 失败：{e}"))?;
    for result in results {
        if let Err((what, code)) = result {
            return Err(format!("删除 topic {name} 失败：{what}（{code:?}）"));
        }
    }
    Ok(true)
}

pub async fn increase_partitions(
    c: ClusterConfig,
    name: String,
    new_count: i32,
) -> Result<bool, String> {
    if name.trim().is_empty() {
        return Err("topic 名称不能为空".into());
    }
    if new_count <= 0 {
        return Err("newCount 必须大于 0".into());
    }
    let client = admin(&c)?;
    let partitions = NewPartitions::new(&name, new_count as usize);
    let options = AdminOptions::new().request_timeout(Some(Duration::from_secs(15)));
    let results = client
        .create_partitions(&[partitions], &options)
        .await
        .map_err(|e| format!("增加分区失败：{e}"))?;
    for result in results {
        if let Err((what, code)) = result {
            return Err(format!("增加分区 {name} 失败：{what}（{code:?}）"));
        }
    }
    Ok(true)
}

// ────────────────────────────────────────────────────────────────────────────
// 健康巡检
// ────────────────────────────────────────────────────────────────────────────

pub async fn scan_health(c: ClusterConfig) -> Result<HealthReport, String> {
    blocking("health", move || {
        let consumer = build_consumer(&c)?;
        let md = consumer
            .fetch_metadata(None, META_TIMEOUT)
            .map_err(|e| conn_error(&c, "健康巡检读取元数据", e))?;
        let live: HashSet<i32> = md.brokers().iter().map(|b| b.id()).collect();
        let mut report = HealthReport {
            scanned_topics: 0,
            scanned_partitions: 0,
            issues: Vec::new(),
            topic_errors: Vec::new(),
            missing_brokers: Vec::new(),
        };
        let mut missing: HashSet<i32> = HashSet::new();

        for topic in md.topics() {
            report.scanned_topics += 1;
            if let Some(err) = topic.error() {
                report.topic_errors.push(TopicIssue {
                    topic: topic.name().to_string(),
                    err: format!("{err:?}"),
                });
            }
            for partition in topic.partitions() {
                report.scanned_partitions += 1;
                let replicas = partition.replicas().to_vec();
                let isr = partition.isr().to_vec();
                let offline_reps: Vec<i32> = replicas
                    .iter()
                    .copied()
                    .filter(|r| !live.contains(r))
                    .collect();
                let mut labels = Vec::new();
                if partition.leader() < 0 {
                    labels.push("离线".to_string());
                }
                if isr.len() < replicas.len() {
                    labels.push("副本不足".to_string());
                }
                if !offline_reps.is_empty() {
                    labels.push("离线副本".to_string());
                }
                if partition.leader() >= 0 && !isr.contains(&partition.leader()) {
                    labels.push("Leader 不在 ISR".to_string());
                }
                if !labels.is_empty() {
                    report.issues.push(PartitionIssue {
                        topic: topic.name().to_string(),
                        partition: partition.id(),
                        leader: partition.leader(),
                        replicas: replicas.clone(),
                        isr,
                        offline_reps: offline_reps.clone(),
                        err: if partition.leader() < 0 {
                            "离线".into()
                        } else {
                            String::new()
                        },
                        labels,
                    });
                }
                for replica in replicas {
                    if !live.contains(&replica) {
                        missing.insert(replica);
                    }
                }
            }
        }

        report
            .issues
            .sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));
        report.topic_errors.sort_by(|a, b| a.topic.cmp(&b.topic));
        report.missing_brokers = missing.into_iter().collect();
        report.missing_brokers.sort();
        Ok(report)
    })
    .await
}

// ────────────────────────────────────────────────────────────────────────────
// Kerberos
// ────────────────────────────────────────────────────────────────────────────

pub async fn kinit(c: ClusterConfig) -> Result<KinitResult, String> {
    blocking("kinit", move || {
        if !c.uses_sasl() || c.sasl_mechanism()? != SaslMechanism::Gssapi {
            return Err("当前集群不是 SASL/GSSAPI，无需 kinit".into());
        }
        if !c.krb.auto_kinit {
            return Err("未勾选 autoKinit（krb.autoKinit），无可执行的取票配置".into());
        }
        let ccache = config::ccache_file(&c);
        match config::apply_gssapi_env(&c)? {
            Some(message) => Ok(KinitResult {
                ok: true,
                principal: c.krb.principal.clone(),
                ccache_path: ccache,
                message,
            }),
            None => Ok(KinitResult {
                ok: false,
                principal: c.krb.principal.clone(),
                ccache_path: ccache,
                message: "未执行取票（缺少 keytab 或带 realm 的 principal）".into(),
            }),
        }
    })
    .await
}
