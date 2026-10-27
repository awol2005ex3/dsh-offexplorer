//! dsh-offexplorer 的原生半：把 offexplorer_rs 的 Kafka 能力以 napi 函数暴露给 Node。
//!
//! - 集群配置以 JSON 字符串传入（兼容 connections.json 的 PascalCase），由 `config.rs` 解析校验。
//! - 所有函数都是 async，返回 Promise；阻塞操作内部走 `spawn_blocking`。
//! - 错误一律 `Result<T, napi::Error>`，message 原样透传到宿主与 agent。

use napi::bindgen_prelude::*;
use napi_derive::napi;

mod config;
mod kafka;
mod types;

use crate::config::ClusterConfig;
use crate::types::{
    BrokerInfo, BrowseResult, ClusterIdentity, ConfigEntry, GroupDetail, GroupSummary, HealthReport,
    KinitResult, ProduceResult, TopicDetail, TopicSummary, Watermarks,
};

fn parse(json: &str) -> Result<ClusterConfig> {
    config::parse_cluster(json).map_err(|e| Error::new(Status::InvalidArg, e))
}

fn failure(stage: &str, message: String) -> Error {
    Error::new(Status::GenericFailure, format!("{stage}: {message}"))
}

/// 原生模块版本（诊断用）。
#[napi]
pub fn native_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// 握手测试：只做一次 metadata 往返。
#[napi]
pub async fn ping(cluster_json: String) -> Result<bool> {
    let cluster = parse(&cluster_json)?;
    kafka::ping(cluster).await.map_err(|e| failure("ping", e))
}

/// 集群 id 与 controller broker id。
#[napi]
pub async fn cluster_info(cluster_json: String) -> Result<ClusterIdentity> {
    let cluster = parse(&cluster_json)?;
    kafka::cluster_identity(cluster)
        .await
        .map_err(|e| failure("clusterInfo", e))
}

/// broker 列表。
#[napi]
pub async fn list_brokers(cluster_json: String) -> Result<Vec<BrokerInfo>> {
    let cluster = parse(&cluster_json)?;
    kafka::list_brokers(cluster)
        .await
        .map_err(|e| failure("listBrokers", e))
}

/// topic 概要列表。
#[napi]
pub async fn list_topics(cluster_json: String) -> Result<Vec<TopicSummary>> {
    let cluster = parse(&cluster_json)?;
    kafka::list_topics(cluster)
        .await
        .map_err(|e| failure("listTopics", e))
}

/// topic 分区明细（leader / 副本 / ISR / 水位）。
#[napi]
pub async fn topic_detail(cluster_json: String, topic: String) -> Result<TopicDetail> {
    let cluster = parse(&cluster_json)?;
    kafka::topic_detail(cluster, topic)
        .await
        .map_err(|e| failure("topicDetail", e))
}

/// topic 动态配置。
#[napi]
pub async fn topic_configs(cluster_json: String, topic: String) -> Result<Vec<ConfigEntry>> {
    let cluster = parse(&cluster_json)?;
    kafka::topic_configs(cluster, topic)
        .await
        .map_err(|e| failure("topicConfigs", e))
}

/// broker 动态配置。
#[napi]
pub async fn broker_configs(cluster_json: String, broker_id: i32) -> Result<Vec<ConfigEntry>> {
    let cluster = parse(&cluster_json)?;
    kafka::broker_configs(cluster, broker_id)
        .await
        .map_err(|e| failure("brokerConfigs", e))
}

/// 消费者组列表。
#[napi]
pub async fn list_groups(cluster_json: String) -> Result<Vec<GroupSummary>> {
    let cluster = parse(&cluster_json)?;
    kafka::list_groups(cluster)
        .await
        .map_err(|e| failure("listGroups", e))
}

/// 消费者组详情（成员与分区分配）。
#[napi]
pub async fn group_detail(cluster_json: String, group: String) -> Result<GroupDetail> {
    let cluster = parse(&cluster_json)?;
    kafka::group_detail(cluster, group)
        .await
        .map_err(|e| failure("groupDetail", e))
}

/// 分区水位。
#[napi]
pub async fn fetch_watermarks(
    cluster_json: String,
    topic: String,
    partition: i32,
) -> Result<Watermarks> {
    let cluster = parse(&cluster_json)?;
    kafka::fetch_watermarks(cluster, topic, partition)
        .await
        .map_err(|e| failure("watermarks", e))
}

/// 浏览消息。
#[napi]
#[allow(clippy::too_many_arguments)]
pub async fn browse(
    cluster_json: String,
    topic: String,
    partitions: Vec<i32>,
    start: String,
    spec_offset: i64,
    per_part: i32,
    limit: i32,
    max_value_bytes: i32,
) -> Result<BrowseResult> {
    let cluster = parse(&cluster_json)?;
    kafka::browse(
        cluster,
        topic,
        partitions,
        start,
        spec_offset,
        per_part,
        limit,
        max_value_bytes,
    )
    .await
    .map_err(|e| failure("browse", e))
}

/// 生产一条消息。`partition` < 0 时由分区器决定。
#[napi]
pub async fn produce(
    cluster_json: String,
    topic: String,
    key: String,
    value: String,
    partition: i32,
) -> Result<ProduceResult> {
    let cluster = parse(&cluster_json)?;
    kafka::produce(cluster, topic, key, value, partition)
        .await
        .map_err(|e| failure("produce", e))
}

/// 创建 topic。
#[napi]
pub async fn create_topic(
    cluster_json: String,
    name: String,
    partitions: i32,
    replication: i32,
) -> Result<bool> {
    let cluster = parse(&cluster_json)?;
    kafka::create_topic(cluster, name, partitions, replication)
        .await
        .map_err(|e| failure("createTopic", e))
}

/// 删除 topic。
#[napi]
pub async fn delete_topic(cluster_json: String, name: String) -> Result<bool> {
    let cluster = parse(&cluster_json)?;
    kafka::delete_topic(cluster, name)
        .await
        .map_err(|e| failure("deleteTopic", e))
}

/// 增加 topic 分区数。
#[napi]
pub async fn increase_partitions(
    cluster_json: String,
    name: String,
    new_count: i32,
) -> Result<bool> {
    let cluster = parse(&cluster_json)?;
    kafka::increase_partitions(cluster, name, new_count)
        .await
        .map_err(|e| failure("increasePartitions", e))
}

/// 健康巡检：离线分区 / 副本不足 / 离线副本。
#[napi]
pub async fn scan_health(cluster_json: String) -> Result<HealthReport> {
    let cluster = parse(&cluster_json)?;
    kafka::scan_health(cluster)
        .await
        .map_err(|e| failure("health", e))
}

/// 按集群配置执行一次 Kerberos 取票。
#[napi]
pub async fn kinit(cluster_json: String) -> Result<KinitResult> {
    let cluster = parse(&cluster_json)?;
    kafka::kinit(cluster)
        .await
        .map_err(|e| failure("kinit", e))
}
