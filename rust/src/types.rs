//! 返回给 Node 侧的数据结构（napi 自动 snake_case → camelCase）。
//!
//! 集群连接配置**不在这里**：配置以 JSON 字符串跨边界传入，由 `config.rs`
//! 用 serde 解析（兼容 connections.json 的 PascalCase 与 dsh 设置里的 camelCase）。

use napi_derive::napi;

/// 一条 Kafka 配置项（topic / broker 动态配置）。
#[napi(object)]
pub struct ConfigEntry {
    pub name: String,
    pub value: String,
    pub default_value: String,
    pub origin: String,
    pub docs: String,
    pub synthetic: bool,
    pub read_only: bool,
    pub is_sensitive: bool,
    pub source: i32,
}

/// Topic 概要。
#[napi(object)]
pub struct TopicSummary {
    pub name: String,
    pub internal: bool,
    pub partition_count: i32,
    pub replication: i32,
    pub usable: bool,
    pub unusable_reason: String,
    pub offline_partitions: i32,
    pub under_replicated: i32,
}

/// 单个分区的明细。
#[napi(object)]
pub struct PartitionDetail {
    pub partition: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub offline_reps: Vec<i32>,
    pub earliest: i64,
    pub latest: i64,
    pub size: i64,
    pub err: String,
}

/// Topic 详情（分区明细）。
#[napi(object)]
pub struct TopicDetail {
    pub topic: TopicSummary,
    pub partitions: Vec<PartitionDetail>,
}

/// Broker 信息。
#[napi(object)]
pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: String,
    pub is_controller: bool,
    pub is_seed: bool,
}

/// 消费者组概要。
#[napi(object)]
pub struct GroupSummary {
    pub name: String,
    pub state: String,
    pub protocol: String,
    pub protocol_type: String,
}

/// 成员的分区分配。
#[napi(object)]
pub struct PartitionAssignment {
    pub topic: String,
    pub partition: i32,
}

/// 消费者组成员。
#[napi(object)]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignments: Vec<PartitionAssignment>,
}

/// 消费者组详情。
#[napi(object)]
pub struct GroupDetail {
    pub name: String,
    pub state: String,
    pub protocol: String,
    pub protocol_type: String,
    pub members: Vec<GroupMember>,
}

/// 分区水位（earliest / latest offset）。
#[napi(object)]
pub struct Watermarks {
    pub topic: String,
    pub partition: i32,
    pub low: i64,
    pub high: i64,
}

/// 消息头。二进制值同时给出 base64 与有损 UTF-8 文本。
#[napi(object)]
pub struct MessageHeader {
    pub key: String,
    pub value_base64: String,
    pub value_text: String,
}

/// 一条消息（key/value 按预览上限截断）。
#[napi(object)]
pub struct MessageRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key_base64: String,
    pub key_text: String,
    pub value_base64: String,
    pub value_text: String,
    pub value_length: i64,
    pub headers: Vec<MessageHeader>,
    pub truncated: bool,
}

/// 浏览消息结果。
#[napi(object)]
pub struct BrowseResult {
    pub messages: Vec<MessageRecord>,
    pub truncated: bool,
    pub partitions: Vec<i32>,
}

/// 生产消息结果。
#[napi(object)]
pub struct ProduceResult {
    pub partition: i32,
    pub offset: i64,
}

/// 异常 topic。
#[napi(object)]
pub struct TopicIssue {
    pub topic: String,
    pub err: String,
}

/// 异常分区。
#[napi(object)]
pub struct PartitionIssue {
    pub topic: String,
    pub partition: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub offline_reps: Vec<i32>,
    pub err: String,
    pub labels: Vec<String>,
}

/// 健康巡检报告。
#[napi(object)]
pub struct HealthReport {
    pub scanned_topics: i32,
    pub scanned_partitions: i32,
    pub issues: Vec<PartitionIssue>,
    pub topic_errors: Vec<TopicIssue>,
    pub missing_brokers: Vec<i32>,
}

/// 集群标识。
#[napi(object)]
pub struct ClusterIdentity {
    pub cluster_id: String,
    pub controller_id: i32,
}

/// Kerberos 取票结果。
#[napi(object)]
pub struct KinitResult {
    pub ok: bool,
    pub principal: String,
    pub ccache_path: String,
    pub message: String,
}
