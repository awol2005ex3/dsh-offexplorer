/**
 * 原生模块（rust/ → lib/native/index.cjs）的手写类型声明。
 *
 * 与 `rust/src/types.rs` 的 napi 导出、`rust/src/lib.rs` 的 `#[napi]` 函数一一对应。
 * napi 宏自动把 snake_case 字段/函数名转成 camelCase；`Option<T>` 变成 `T | null`。
 *
 * 集群配置不经过 napi 对象，而是 JSON 字符串（见 `settings.ts` 的 KafkaCluster），
 * 原生侧 `config.rs` 会再做一次规范化与校验。
 */

export interface ConfigEntry {
  name: string
  value: string
  defaultValue: string
  origin: string
  docs: string
  synthetic: boolean
  readOnly: boolean
  isSensitive: boolean
  source: number
}

export interface TopicSummary {
  name: string
  internal: boolean
  partitionCount: number
  replication: number
  usable: boolean
  unusableReason: string
  offlinePartitions: number
  underReplicated: number
}

export interface PartitionDetail {
  partition: number
  leader: number
  leaderEpoch: number
  replicas: number[]
  isr: number[]
  offlineReps: number[]
  earliest: number
  latest: number
  size: number
  err: string
}

export interface TopicDetail {
  topic: TopicSummary
  partitions: PartitionDetail[]
}

export interface BrokerInfo {
  nodeId: number
  host: string
  port: number
  rack: string
  isController: boolean
  isSeed: boolean
}

export interface GroupSummary {
  name: string
  state: string
  protocol: string
  protocolType: string
}

export interface PartitionAssignment {
  topic: string
  partition: number
}

export interface GroupMember {
  memberId: string
  clientId: string
  clientHost: string
  assignments: PartitionAssignment[]
}

export interface GroupDetail extends GroupSummary {
  members: GroupMember[]
}

export interface Watermarks {
  topic: string
  partition: number
  low: number
  high: number
}

export interface MessageHeader {
  key: string
  valueBase64: string
  valueText: string
}

export interface MessageRecord {
  topic: string
  partition: number
  offset: number
  /** napi 的 Option<i64> 在运行时可能表现为 null 或缺省 */
  timestampMs?: number | null
  keyBase64: string
  keyText: string
  valueBase64: string
  valueText: string
  valueLength: number
  headers: MessageHeader[]
  truncated: boolean
}

export interface BrowseResult {
  messages: MessageRecord[]
  truncated: boolean
  partitions: number[]
}

export interface ProduceResult {
  partition: number
  offset: number
}

export interface TopicIssue {
  topic: string
  err: string
}

export interface PartitionIssue {
  topic: string
  partition: number
  leader: number
  replicas: number[]
  isr: number[]
  offlineReps: number[]
  err: string
  labels: string[]
}

export interface HealthReport {
  scannedTopics: number
  scannedPartitions: number
  issues: PartitionIssue[]
  topicErrors: TopicIssue[]
  missingBrokers: number[]
}

export interface ClusterIdentity {
  clusterId: string
  controllerId: number
}

export interface KinitResult {
  ok: boolean
  principal: string
  ccachePath: string
  message: string
}

/** 原生模块函数面（clusterJson 为 `JSON.stringify(KafkaCluster)`）。 */
export interface DshOffexplorerNative {
  nativeVersion(): string
  ping(clusterJson: string): Promise<boolean>
  clusterInfo(clusterJson: string): Promise<ClusterIdentity>
  listBrokers(clusterJson: string): Promise<BrokerInfo[]>
  listTopics(clusterJson: string): Promise<TopicSummary[]>
  topicDetail(clusterJson: string, topic: string): Promise<TopicDetail>
  topicConfigs(clusterJson: string, topic: string): Promise<ConfigEntry[]>
  brokerConfigs(clusterJson: string, brokerId: number): Promise<ConfigEntry[]>
  listGroups(clusterJson: string): Promise<GroupSummary[]>
  groupDetail(clusterJson: string, group: string): Promise<GroupDetail>
  fetchWatermarks(clusterJson: string, topic: string, partition: number): Promise<Watermarks>
  browse(
    clusterJson: string,
    topic: string,
    partitions: number[],
    start: string,
    specOffset: number,
    perPart: number,
    limit: number,
    maxValueBytes: number,
  ): Promise<BrowseResult>
  produce(
    clusterJson: string,
    topic: string,
    key: string,
    value: string,
    partition: number,
  ): Promise<ProduceResult>
  createTopic(
    clusterJson: string,
    name: string,
    partitions: number,
    replication: number,
  ): Promise<boolean>
  deleteTopic(clusterJson: string, name: string): Promise<boolean>
  increasePartitions(clusterJson: string, name: string, newCount: number): Promise<boolean>
  scanHealth(clusterJson: string): Promise<HealthReport>
  kinit(clusterJson: string): Promise<KinitResult>
}
