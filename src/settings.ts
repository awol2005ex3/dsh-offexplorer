import z from '@deepseek-ai/schemastery'

/**
 * dsh ≥ 0.1.7 的 volatile 配置引用：只依赖 `get()`，兼容旧版普通值与测试替身。
 */
export interface VolatileRef<T> {
  get(): T
}

export type MaybeVolatile<T> = T | VolatileRef<T>

/** Kerberos 设置（与原生侧 `config::KerberosConfig` 对应）。 */
export interface KerberosSettings {
  krb5ConfPath: string
  realm: string
  credSource: string
  ccachePath: string
  keytabPath: string
  principal: string
  username: string
  password: string
  autoKinit: boolean
  kinitPath: string
  service: string
  hostTemplate: string
  resolve: string
  disablePafxFast: boolean
  forceCanonical: boolean
}

/** TLS 设置（与原生侧 `config::TlsConfig` 对应）。 */
export interface TlsSettings {
  caPemPath: string
  clientCertPemPath: string
  clientKeyPemPath: string
  serverName: string
  insecureSkipVerify: boolean
}

/**
 * 一个 Kafka 集群连接（camelCase，跨 napi 边界的规范形状）。
 *
 * 也兼容 Offset Explorer connections.json 的 PascalCase 字段，
 * 解析时在 {@link normalizeCluster} 里统一。
 */
export interface KafkaCluster {
  id: string
  name: string
  group: string
  description: string
  bootstrapServers: string[]
  kafkaVersion: string
  security: string
  sasl: string
  krb: KerberosSettings
  plainUser: string
  plainPass: string
  scramAuthzId: string
  tls: TlsSettings
  schemaRegistryUrl: string
  maxMessageBytes: number
  allowAutoCreateTopic: boolean
  consumerGroup: string
  dialTimeoutMs: number
  requestTimeoutMs: number
  extraConfig: string
}

/** 每次请求现读的普通值配置快照。 */
export interface OffexplorerSettings {
  clusters: KafkaCluster[]
  clusterError?: string
  defaultCluster: string
  maxRows: number
  maxPerPartition: number
  maxValueBytes: number
}

export interface OffexplorerConfig {
  clusters?: MaybeVolatile<string>
  defaultCluster?: MaybeVolatile<string>
  maxRows?: MaybeVolatile<number>
  maxPerPartition?: MaybeVolatile<number>
  maxValueBytes?: MaybeVolatile<number>
}

export const DEFAULTS = {
  clusters: '[]',
  defaultCluster: '',
  maxRows: 100,
  maxPerPartition: 50,
  maxValueBytes: 4096,
}

export const OffexplorerConfigSchema = z.object({
  clusters: z.string().default(DEFAULTS.clusters).volatile().description(
    'Kafka 集群列表（JSON 数组）。示例：[{"id":"prod","bootstrapServers":["broker1:9092"],' +
    '"security":"SASL_PLAINTEXT","sasl":"GSSAPI","krb":{"principal":"user@REALM",' +
    '"keytabPath":"C:/keys/user.keytab","autoKinit":true}}]。' +
    '也兼容 Offset Explorer connections.json 的 PascalCase 字段。',
  ),
  defaultCluster: z.string().default(DEFAULTS.defaultCluster).volatile().description(
    '默认集群 id（工具/面板省略 cluster 参数时使用）。留空取列表第一个',
  ),
  maxRows: z.number().default(DEFAULTS.maxRows).volatile().description(
    '浏览消息的默认总条数上限',
  ),
  maxPerPartition: z.number().default(DEFAULTS.maxPerPartition).volatile().description(
    '浏览消息时每个分区的默认条数上限',
  ),
  maxValueBytes: z.number().default(DEFAULTS.maxValueBytes).volatile().description(
    '单条消息 key/value 预览的默认字节上限（base64 与文本都按此截断）',
  ),
})

function unwrap<T>(value: MaybeVolatile<T> | undefined | null): T | undefined {
  if (value === undefined || value === null) return undefined
  const ref = value as VolatileRef<T>
  if (typeof ref.get === 'function') {
    try {
      return ref.get()
    } catch {
      return undefined
    }
  }
  return value as T
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined
}

/** 依次取第一个存在的键（兼容 camelCase / PascalCase / snake_case）。 */
function pick(row: Record<string, unknown>, ...keys: string[]): unknown {
  for (const key of keys) {
    if (row[key] !== undefined && row[key] !== null) return row[key]
  }
  return undefined
}

function pickString(row: Record<string, unknown>, ...keys: string[]): string {
  const value = pick(row, ...keys)
  return typeof value === 'string' ? value.trim() : ''
}

function pickNumber(row: Record<string, unknown>, fallback: number, ...keys: string[]): number {
  const value = pick(row, ...keys)
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback
}

function pickBool(row: Record<string, unknown>, fallback: boolean, ...keys: string[]): boolean {
  const value = pick(row, ...keys)
  if (typeof value === 'boolean') return value
  if (typeof value === 'string') {
    const text = value.trim().toLowerCase()
    if (text === 'true' || text === '1') return true
    if (text === 'false' || text === '0') return false
  }
  return fallback
}

function servers(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.filter((v): v is string => typeof v === 'string').map((v) => v.trim()).filter(Boolean)
  }
  if (typeof value === 'string') {
    return value.split(/[,;\s]+/).map((v) => v.trim()).filter(Boolean)
  }
  return []
}

function normalizeKerberos(value: unknown): KerberosSettings {
  const row = asRecord(value) ?? {}
  return {
    krb5ConfPath: pickString(row, 'krb5ConfPath', 'Krb5ConfPath', 'KRB5ConfPath'),
    realm: pickString(row, 'realm', 'Realm'),
    credSource: pickString(row, 'credSource', 'CredSource') || 'ccache',
    ccachePath: pickString(row, 'ccachePath', 'CCachePath', 'CcachePath'),
    keytabPath: pickString(row, 'keytabPath', 'KeytabPath'),
    principal: pickString(row, 'principal', 'Principal'),
    username: pickString(row, 'username', 'Username'),
    password: pickString(row, 'password', 'Password'),
    autoKinit: pickBool(row, false, 'autoKinit', 'AutoKinit'),
    kinitPath: pickString(row, 'kinitPath', 'KinitPath'),
    service: pickString(row, 'service', 'Service'),
    hostTemplate: pickString(row, 'hostTemplate', 'HostTemplate'),
    resolve: pickString(row, 'resolve', 'Resolve') || 'literal',
    disablePafxFast: pickBool(row, false, 'disablePafxFast', 'DisablePAFXFast'),
    forceCanonical: pickBool(row, false, 'forceCanonical', 'ForceCanonical'),
  }
}

function normalizeTls(value: unknown): TlsSettings {
  const row = asRecord(value) ?? {}
  return {
    caPemPath: pickString(row, 'caPemPath', 'CAPemPath', 'CaPemPath'),
    clientCertPemPath: pickString(row, 'clientCertPemPath', 'ClientCertPemPath'),
    clientKeyPemPath: pickString(row, 'clientKeyPemPath', 'ClientKeyPemPath'),
    serverName: pickString(row, 'serverName', 'ServerName'),
    insecureSkipVerify: pickBool(row, false, 'insecureSkipVerify', 'InsecureSkipVerify'),
  }
}

function normalizeCluster(row: Record<string, unknown>): KafkaCluster | undefined {
  const bootstrapServers = servers(pick(row, 'bootstrapServers', 'BootstrapServers', 'bootstrap_servers'))
  if (bootstrapServers.length === 0) return undefined
  const name = pickString(row, 'name', 'Name')
  const id = pickString(row, 'id', 'Id', 'ID')
    || name
    || bootstrapServers[0].replace(/[^\w.-]+/g, '-')
  return {
    id,
    name,
    group: pickString(row, 'group', 'Group'),
    description: pickString(row, 'description', 'Description'),
    bootstrapServers,
    kafkaVersion: pickString(row, 'kafkaVersion', 'KafkaVersion'),
    security: pickString(row, 'security', 'Security', 'securityType') || 'PLAINTEXT',
    sasl: pickString(row, 'sasl', 'SASL', 'saslMechanism') || 'GSSAPI',
    krb: normalizeKerberos(pick(row, 'krb', 'Krb', 'kerberos')),
    plainUser: pickString(row, 'plainUser', 'PlainUser'),
    plainPass: pickString(row, 'plainPass', 'PlainPass'),
    scramAuthzId: pickString(row, 'scramAuthzId', 'SCRAMAuthzID'),
    tls: normalizeTls(pick(row, 'tls', 'TLS')),
    schemaRegistryUrl: pickString(row, 'schemaRegistryUrl', 'SchemaRegistryURL'),
    maxMessageBytes: pickNumber(row, 1024 * 1024, 'maxMessageBytes', 'MaxMessageBytes'),
    allowAutoCreateTopic: pickBool(row, false, 'allowAutoCreateTopic', 'AllowAutoCreateTopic'),
    consumerGroup: pickString(row, 'consumerGroup', 'ConsumerGroup'),
    dialTimeoutMs: pickNumber(row, 10_000, 'dialTimeoutMs', 'DialTimeoutMs'),
    requestTimeoutMs: pickNumber(row, 20_000, 'requestTimeoutMs', 'RequestTimeoutMs'),
    extraConfig: pickString(row, 'extraConfig', 'ExtraConfig'),
  }
}

export function parseClusters(raw: string | undefined): { clusters: KafkaCluster[]; error?: string } {
  const text = (raw ?? '').trim()
  if (!text) return { clusters: [] }
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (err) {
    return {
      clusters: [],
      error: `clusters JSON 解析失败: ${err instanceof Error ? err.message : String(err)}`,
    }
  }
  if (!Array.isArray(parsed)) return { clusters: [], error: 'clusters 必须是 JSON 数组' }

  const clusters: KafkaCluster[] = []
  const seen = new Set<string>()
  const problems: string[] = []
  parsed.forEach((item, index) => {
    const row = asRecord(item)
    if (!row) {
      problems.push(`第 ${index + 1} 项不是对象`)
      return
    }
    const cluster = normalizeCluster(row)
    if (!cluster) {
      problems.push(`第 ${index + 1} 项缺少 bootstrapServers`)
      return
    }
    let id = cluster.id
    for (let n = 2; seen.has(id); n += 1) id = `${cluster.id}-${n}`
    seen.add(id)
    clusters.push({ ...cluster, id })
  })
  return { clusters, error: problems.length > 0 ? problems.join('；') : undefined }
}

function numberIn(value: number | undefined, fallback: number, min: number, max: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) return fallback
  return Math.min(max, Math.max(min, Math.floor(value)))
}

/**
 * 取一份普通值配置快照。每次请求现读（volatile 引用值随时可能被设置页改写）。
 */
export function readSettings(config?: OffexplorerConfig | null): OffexplorerSettings {
  const source: OffexplorerConfig = config ?? {}
  const { clusters, error } = parseClusters(unwrap(source.clusters))
  const defaultCluster = unwrap(source.defaultCluster)?.trim() ?? ''
  return {
    clusters,
    clusterError: error,
    defaultCluster: defaultCluster || clusters[0]?.id || '',
    maxRows: numberIn(unwrap(source.maxRows), DEFAULTS.maxRows, 1, 100_000),
    maxPerPartition: numberIn(unwrap(source.maxPerPartition), DEFAULTS.maxPerPartition, 1, 10_000),
    maxValueBytes: numberIn(unwrap(source.maxValueBytes), DEFAULTS.maxValueBytes, 64, 1024 * 1024),
  }
}
