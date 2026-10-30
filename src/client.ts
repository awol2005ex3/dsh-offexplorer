/**
 * dsh-offexplorer · 浏览器端 bundle 模块体。
 *
 * 构建契约同 dsh-hdfs/src/client.ts：本文件经 tsc 编译后，由
 * scripts/wrap-client.mjs 包上闭包工厂外壳（无 ESM 语法，module.exports 返回
 * { name, inject, apply }）。
 *
 * 功能：
 * 1. Kafka 浏览器（侧边栏入口 + 浮动面板）：主题 / 分区 / 消息 / 消费者组 /
 *    Broker / 健康巡检 / 生产消息 / Kerberos 取票；
 * 2. 配置页签（settings.plugins.tab，ns=offexplorer）：clusters JSON 编辑 +
 *    校验 + 解析预览，优先走官方 configForms（volatile 热更新），RPC 只读兜底。
 *
 * ⚠ RPC 通道固定 /api：宿主端是 connection.fetch.register() 注册的精确路由。
 * ⚠ .node 原生模块只能活在宿主进程里，这里永远经由 RPC 桥访问 Kafka。
 */

const PLUGIN_ID = 'dsh-offexplorer'
const SETTINGS_NS = 'offexplorer'
const RPC_CHANNEL = '/api'
const RPC_PREFIX = 'offexplorer/'

declare var module: { exports: unknown }
declare function require(id: string): unknown

const win = window as unknown as { __dshOffexplorerMounted?: boolean }
const doc = document

/* ── 工具函数 ─────────────────────────────────────────────────────────── */

type ElProps = { style?: string; [key: string]: unknown }

function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  props?: ElProps,
  children?: (Node | string)[],
): HTMLElementTagNameMap[K] {
  const node = doc.createElement(tag)
  if (props) {
    for (const [key, value] of Object.entries(props)) {
      if (key === 'style') node.setAttribute('style', value as string)
      else (node as unknown as Record<string, unknown>)[key] = value
    }
  }
  if (children) {
    for (const c of children) node.append(typeof c === 'string' ? doc.createTextNode(c) : c)
  }
  return node
}

function fmtBytes(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return '0 B'
  if (n < 1024) return `${n} B`
  const units = ['KB', 'MB', 'GB', 'TB', 'PB']
  let v = n
  let i = -1
  do { v /= 1024; i += 1 } while (v >= 1024 && i < units.length - 1)
  return `${v.toFixed(v >= 100 ? 0 : 1)} ${units[i]}`
}

function fmtTime(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms) || ms <= 0) return '-'
  try { return new Date(ms).toLocaleString() } catch { return String(ms) }
}

function errText(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

/** 安全地取一个服务（cordis 4 严格 inject：属性访问会抛，get 不受限制）。 */
function service(ctx: any, name: string): any {
  try {
    if (typeof ctx?.get === 'function') return ctx.get(name)
    return ctx?.[name]
  } catch {
    return undefined
  }
}

function short(text: string, max: number): string {
  if (!text) return ''
  return text.length > max ? `${text.slice(0, max)}…` : text
}

/* ── RPC ──────────────────────────────────────────────────────────────── */

interface RpcResponse {
  ok: boolean
  value?: unknown
  error?: { code: string; message: string; details?: unknown }
}

interface ClusterRow {
  id: string
  name?: string
  group?: string
  description?: string
  bootstrapServers: string[]
  security: string
  sasl: string
}

interface ClusterListValue {
  clusters: ClusterRow[]
  defaultCluster: string
  clusterError?: string
  defaults?: { maxRows: number; maxPerPartition: number; maxValueBytes: number }
}

interface TopicSummary {
  name: string
  internal: boolean
  partitionCount: number
  replication: number
  usable: boolean
  unusableReason: string
  offlinePartitions: number
  underReplicated: number
}

interface PartitionDetail {
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

interface TopicDetail {
  topic: TopicSummary
  partitions: PartitionDetail[]
}

interface ConfigEntry {
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

interface GroupSummary {
  name: string
  state: string
  protocol: string
  protocolType: string
}

interface GroupMember {
  memberId: string
  clientId: string
  clientHost: string
  assignments: Array<{ topic: string; partition: number }>
}

interface GroupDetail extends GroupSummary {
  members: GroupMember[]
}

interface BrokerInfo {
  nodeId: number
  host: string
  port: number
  rack: string
  isController: boolean
  isSeed: boolean
}

interface MessageHeader {
  key: string
  valueBase64: string
  valueText: string
}

interface MessageRecord {
  topic: string
  partition: number
  offset: number
  timestampMs?: number | null
  keyBase64: string
  keyText: string
  valueBase64: string
  valueText: string
  valueLength: number
  headers: MessageHeader[]
  truncated: boolean
}

interface BrowseResult {
  messages: MessageRecord[]
  truncated: boolean
  partitions: number[]
}

interface HealthReport {
  scannedTopics: number
  scannedPartitions: number
  issues: Array<{
    topic: string
    partition: number
    leader: number
    replicas: number[]
    isr: number[]
    offlineReps: number[]
    err: string
    labels: string[]
  }>
  topicErrors: Array<{ topic: string; err: string }>
  missingBrokers: number[]
}

async function callRpc(conn: any, endpoint: string, args?: Record<string, unknown>): Promise<unknown> {
  const res = (await conn.rpc.call(RPC_CHANNEL, `${RPC_PREFIX}${endpoint}`, args ? { args } : {})) as RpcResponse
  if (!res || res.ok !== true) {
    throw new Error(res?.error?.message ?? 'rpc error')
  }
  return res.value
}

/* ── 配置读写通道 ──────────────────────────────────────────────────────── */

interface SettingsSnapshot {
  clusters?: string
  defaultCluster?: string
  maxRows?: number
  maxPerPartition?: number
  maxValueBytes?: number
}

interface SettingsChannel {
  read(): Promise<SettingsSnapshot>
  write(patch: Record<string, unknown>): Promise<void>
  /** configForms 可写；RPC 退路只能看到宿主解析后的只读快照。 */
  writable: boolean
}

/** 退路：宿主 /api/offexplorer/clusters 只读快照（不含密钥字段，且不可写）。 */
function rpcSettingsChannel(conn: any): SettingsChannel | undefined {
  if (!conn || !conn.rpc || typeof conn.rpc.call !== 'function') return undefined
  return {
    writable: false,
    read: async () => {
      const value = (await callRpc(conn, 'clusters', {})) as ClusterListValue
      const rows = (value.clusters ?? []).map((c) => ({
        id: c.id,
        name: c.name ?? '',
        group: c.group ?? '',
        description: c.description ?? '',
        bootstrapServers: c.bootstrapServers,
        security: c.security,
        sasl: c.sasl,
      }))
      return {
        clusters: JSON.stringify(rows, null, 2),
        defaultCluster: value.defaultCluster ?? '',
        maxRows: value.defaults?.maxRows,
        maxPerPartition: value.defaults?.maxPerPartition,
        maxValueBytes: value.defaults?.maxValueBytes,
      }
    },
    write: async () => {
      throw new Error('当前 dsh 未提供 settings 表单服务（configForms），无法落盘配置')
    },
  }
}

/** 首选：官方 configForms（设置页同款，volatile 字段落盘 + 热更新）。 */
function configFormsChannel(ctx: any): SettingsChannel | undefined {
  const forms = service(ctx, 'configForms')
  if (!forms || typeof forms.get !== 'function') return undefined

  function ensure(): boolean {
    try {
      const mirror = typeof forms.describe === 'function' ? forms.describe() : undefined
      if (!mirror || typeof mirror.getSnapshot !== 'function') return false
      return mirror.getSnapshot()?.view?.namespaces?.some((row: any) => row?.ns === SETTINGS_NS) === true
    } catch {
      return false
    }
  }

  if (!ensure()) return undefined

  const form = (): any => forms.get(SETTINGS_NS)
  async function waitReady(): Promise<any> {
    const snap = form().getSnapshot()
    if (snap?.status === 'ready') return snap
    return new Promise((resolve, reject) => {
      let done = false
      const timer = setTimeout(() => {
        if (done) return
        done = true
        off?.()
        reject(new Error('读取配置超时（宿主配置镜像未就绪）'))
      }, 15000)
      const off = typeof form().subscribe === 'function' ? form().subscribe(check) : undefined
      function check(): void {
        if (done) return
        const s = form().getSnapshot()
        if (s?.status === 'ready') {
          done = true
          clearTimeout(timer)
          off?.()
          resolve(s)
        } else if (s?.status === 'unavailable') {
          done = true
          clearTimeout(timer)
          off?.()
          reject(new Error('配置服务不可用'))
        }
      }
    })
  }

  return {
    writable: true,
    read: async () => {
      const snap = await waitReady()
      const value = snap.value ?? {}
      let clusters = value.clusters
      if (Array.isArray(clusters)) clusters = JSON.stringify(clusters, null, 2)
      if (typeof clusters !== 'string') clusters = '[]'
      return {
        clusters,
        defaultCluster: String(value.defaultCluster ?? ''),
        maxRows: Number(value.maxRows ?? 100),
        maxPerPartition: Number(value.maxPerPartition ?? 50),
        maxValueBytes: Number(value.maxValueBytes ?? 4096),
      }
    },
    write: async (patch) => {
      const snap = await waitReady()
      const ops: Array<Record<string, unknown>> = []
      for (const [key, value] of Object.entries(patch)) ops.push({ op: 'set', path: [key], value })
      await form().mutate(ops, snap.revision)
    },
  }
}

/** 每次读写都重新探测：配置镜像异步就绪，启动时失败不代表以后不可用。 */
function createChannel(ctx: any, conn: any): SettingsChannel {
  const resolve = (): SettingsChannel | undefined =>
    configFormsChannel(ctx) ?? rpcSettingsChannel(conn)
  const required = (): SettingsChannel => {
    const channel = resolve()
    if (!channel) throw new Error('没有可用的配置读写通道（configForms / RPC 均不可用）')
    return channel
  }
  return {
    get writable(): boolean {
      try {
        return resolve()?.writable === true
      } catch {
        return false
      }
    },
    read: () => required().read(),
    write: (patch) => required().write(patch),
  }
}

/* ── 样式 ─────────────────────────────────────────────────────────────── */

const PANEL_CSS =
  'color:var(--fg,#1f2328);font-size:13px;line-height:1.5;display:flex;flex-direction:column;gap:8px;'
const INPUT_CSS =
  'box-sizing:border-box;padding:5px 8px;font-size:12.5px;font-family:inherit;' +
  'border:1px solid rgba(127,127,127,.4);border-radius:6px;background:var(--bg,#fff);color:inherit;'
const BTN_CSS =
  'padding:5px 12px;font-size:12.5px;cursor:pointer;border:1px solid rgba(127,127,127,.4);' +
  'background:var(--bg,#f6f8fa);color:inherit;border-radius:6px;'
const BTN_PRIMARY_CSS =
  'padding:5px 12px;font-size:12.5px;cursor:pointer;border:none;background:#1f6feb;color:#fff;border-radius:6px;'
const TABLE_CSS = 'width:100%;border-collapse:collapse;font-size:12.5px;'
const CELL_CSS = 'padding:4px 8px;border-bottom:1px solid rgba(127,127,127,.2);text-align:left;white-space:nowrap;'
const PREVIEW_CSS =
  'max-height:280px;overflow:auto;padding:8px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;' +
  'font-size:12px;white-space:pre-wrap;word-break:break-all;border:1px solid rgba(127,127,127,.3);' +
  'border-radius:6px;background:var(--bg,#f6f8fa);margin:0;'
const STATUS_OK = 'color:#1a7f37;'
const STATUS_ERR = 'color:#cf222e;'
const TAB_ACTIVE = 'border-bottom:2px solid #1f6feb;color:#1f6feb;font-weight:600;'
const TAB_IDLE = 'border-bottom:2px solid transparent;color:inherit;'

interface PanelHandle {
  root: HTMLElement
  refresh(): void
  setChrome(mode: boolean | 'browse' | 'config'): void
}

interface TableRow {
  cells: Array<Node | string>
  onClick?: () => void
}

function renderTable(table: HTMLTableElement, head: string[], rows: TableRow[]): void {
  table.textContent = ''
  const headRow = el('tr')
  for (const label of head) headRow.append(el('th', { textContent: label, style: CELL_CSS }))
  table.append(el('thead', {}, [headRow]))
  const tbody = el('tbody')
  for (const row of rows) {
    const tr = el('tr', row.onClick ? { style: 'cursor:pointer;' } : undefined)
    for (const cell of row.cells) {
      const td = el('td', { style: CELL_CSS })
      td.append(typeof cell === 'string' ? doc.createTextNode(cell) : cell)
      tr.append(td)
    }
    if (row.onClick) tr.addEventListener('click', row.onClick)
    tbody.append(tr)
  }
  if (rows.length === 0) {
    const tr = el('tr')
    const td = el('td', { textContent: '(空)', style: CELL_CSS + 'color:var(--fg-muted,#57606a);' })
    td.setAttribute('colspan', String(head.length))
    tr.append(td)
    tbody.append(tr)
  }
  table.append(tbody)
}

/* ── 集群草稿（表单 ⇄ 宿主 JSON） ──────────────────────────────────────── */

type JsonRow = Record<string, unknown>

const SECURITY_TYPES = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']
const SASL_MECHANISMS = ['GSSAPI', 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']

interface KrbDraft {
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

interface TlsDraft {
  caPemPath: string
  clientCertPemPath: string
  clientKeyPemPath: string
  serverName: string
  insecureSkipVerify: boolean
}

interface ClusterDraft {
  id: string
  name: string
  group: string
  description: string
  bootstrap: string
  kafkaVersion: string
  security: string
  sasl: string
  krb: KrbDraft
  plainUser: string
  plainPass: string
  scramAuthzId: string
  tls: TlsDraft
  schemaRegistryUrl: string
  maxMessageBytes: number
  allowAutoCreateTopic: boolean
  consumerGroup: string
  dialTimeoutMs: number
  requestTimeoutMs: number
  /** key=value 每行一条；保存时转成 extraConfig 的 JSON 对象。 */
  extraConfig: string
}

function rowOf(value: unknown): JsonRow | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as JsonRow)
    : undefined
}

function getString(row: JsonRow, ...keys: string[]): string {
  for (const key of keys) {
    const value = row[key]
    if (typeof value === 'string') return value.trim()
    if (typeof value === 'number') return String(value)
  }
  return ''
}

function getNumber(row: JsonRow, fallback: number, ...keys: string[]): number {
  for (const key of keys) {
    const value = row[key]
    if (typeof value === 'number' && Number.isFinite(value)) return value
    if (typeof value === 'string' && value.trim() && Number.isFinite(Number(value))) return Number(value)
  }
  return fallback
}

function getBool(row: JsonRow, fallback: boolean, ...keys: string[]): boolean {
  for (const key of keys) {
    const value = row[key]
    if (typeof value === 'boolean') return value
    if (typeof value === 'string') {
      const text = value.trim().toLowerCase()
      if (text === 'true' || text === '1') return true
      if (text === 'false' || text === '0') return false
    }
  }
  return fallback
}

function serversText(value: unknown): string {
  if (Array.isArray(value)) {
    return value.filter((v): v is string => typeof v === 'string').map((v) => v.trim()).filter(Boolean).join(', ')
  }
  if (typeof value === 'string') return value.trim()
  return ''
}

function splitServers(text: string): string[] {
  return text.split(/[,;\s]+/).map((s) => s.trim()).filter(Boolean)
}

function usesSasl(security: string): boolean {
  return security === 'SASL_PLAINTEXT' || security === 'SASL_SSL'
}

function usesTls(security: string): boolean {
  return security === 'SSL' || security === 'SASL_SSL'
}

function blankDraft(): ClusterDraft {
  return {
    id: '',
    name: '',
    group: '',
    description: '',
    bootstrap: '',
    kafkaVersion: '',
    security: 'PLAINTEXT',
    sasl: 'GSSAPI',
    krb: {
      krb5ConfPath: '',
      realm: '',
      credSource: 'ccache',
      ccachePath: '',
      keytabPath: '',
      principal: '',
      username: '',
      password: '',
      autoKinit: false,
      kinitPath: '',
      service: '',
      hostTemplate: '',
      resolve: 'literal',
      disablePafxFast: false,
      forceCanonical: false,
    },
    plainUser: '',
    plainPass: '',
    scramAuthzId: '',
    tls: {
      caPemPath: '',
      clientCertPemPath: '',
      clientKeyPemPath: '',
      serverName: '',
      insecureSkipVerify: false,
    },
    schemaRegistryUrl: '',
    maxMessageBytes: 1024 * 1024,
    allowAutoCreateTopic: false,
    consumerGroup: '',
    dialTimeoutMs: 10_000,
    requestTimeoutMs: 20_000,
    extraConfig: '',
  }
}

/** 兼容 camelCase / PascalCase（Offset Explorer connections.json）。 */
function draftFromRow(row: JsonRow): ClusterDraft {
  const draft = blankDraft()
  const krb = rowOf(row.krb ?? row.Krb ?? row.kerberos) ?? {}
  const tls = rowOf(row.tls ?? row.TLS) ?? {}
  draft.id = getString(row, 'id', 'Id', 'ID')
  draft.name = getString(row, 'name', 'Name')
  draft.group = getString(row, 'group', 'Group')
  draft.description = getString(row, 'description', 'Description')
  draft.bootstrap = serversText(row.bootstrapServers ?? row.BootstrapServers ?? row.bootstrap_servers)
  draft.kafkaVersion = getString(row, 'kafkaVersion', 'KafkaVersion')
  draft.security = (getString(row, 'security', 'Security', 'securityType') || 'PLAINTEXT').toUpperCase()
  draft.sasl = (getString(row, 'sasl', 'SASL', 'saslMechanism') || 'GSSAPI').toUpperCase()
  draft.krb = {
    krb5ConfPath: getString(krb, 'krb5ConfPath', 'KRB5ConfPath', 'Krb5ConfPath'),
    realm: getString(krb, 'realm', 'Realm'),
    credSource: getString(krb, 'credSource', 'CredSource') || 'ccache',
    ccachePath: getString(krb, 'ccachePath', 'CCachePath', 'CcachePath'),
    keytabPath: getString(krb, 'keytabPath', 'KeytabPath'),
    principal: getString(krb, 'principal', 'Principal'),
    username: getString(krb, 'username', 'Username'),
    password: getString(krb, 'password', 'Password'),
    autoKinit: getBool(krb, false, 'autoKinit', 'AutoKinit'),
    kinitPath: getString(krb, 'kinitPath', 'KinitPath'),
    service: getString(krb, 'service', 'Service'),
    hostTemplate: getString(krb, 'hostTemplate', 'HostTemplate'),
    resolve: getString(krb, 'resolve', 'Resolve') || 'literal',
    disablePafxFast: getBool(krb, false, 'disablePafxFast', 'DisablePAFXFast'),
    forceCanonical: getBool(krb, false, 'forceCanonical', 'ForceCanonical'),
  }
  draft.plainUser = getString(row, 'plainUser', 'PlainUser')
  draft.plainPass = getString(row, 'plainPass', 'PlainPass')
  draft.scramAuthzId = getString(row, 'scramAuthzId', 'SCRAMAuthzID')
  draft.tls = {
    caPemPath: getString(tls, 'caPemPath', 'CAPemPath', 'CaPemPath'),
    clientCertPemPath: getString(tls, 'clientCertPemPath', 'ClientCertPemPath'),
    clientKeyPemPath: getString(tls, 'clientKeyPemPath', 'ClientKeyPemPath'),
    serverName: getString(tls, 'serverName', 'ServerName'),
    insecureSkipVerify: getBool(tls, false, 'insecureSkipVerify', 'InsecureSkipVerify'),
  }
  draft.schemaRegistryUrl = getString(row, 'schemaRegistryUrl', 'SchemaRegistryURL')
  draft.maxMessageBytes = getNumber(row, 1024 * 1024, 'maxMessageBytes', 'MaxMessageBytes')
  draft.allowAutoCreateTopic = getBool(row, false, 'allowAutoCreateTopic', 'AllowAutoCreateTopic')
  draft.consumerGroup = getString(row, 'consumerGroup', 'ConsumerGroup')
  draft.dialTimeoutMs = getNumber(row, 10_000, 'dialTimeoutMs', 'DialTimeoutMs')
  draft.requestTimeoutMs = getNumber(row, 20_000, 'requestTimeoutMs', 'RequestTimeoutMs')
  draft.extraConfig = extraConfigToLines(getString(row, 'extraConfig', 'ExtraConfig'))
  return draft
}

/** 表单草稿 → 宿主 clusters 里的一项（camelCase 规范形状）。 */
function draftToRow(draft: ClusterDraft): JsonRow {
  return {
    id: draft.id,
    name: draft.name,
    group: draft.group,
    description: draft.description,
    bootstrapServers: splitServers(draft.bootstrap),
    kafkaVersion: draft.kafkaVersion,
    security: draft.security,
    sasl: draft.sasl,
    krb: { ...draft.krb },
    plainUser: draft.plainUser,
    plainPass: draft.plainPass,
    scramAuthzId: draft.scramAuthzId,
    tls: { ...draft.tls },
    schemaRegistryUrl: draft.schemaRegistryUrl,
    maxMessageBytes: draft.maxMessageBytes,
    allowAutoCreateTopic: draft.allowAutoCreateTopic,
    consumerGroup: draft.consumerGroup,
    dialTimeoutMs: draft.dialTimeoutMs,
    requestTimeoutMs: draft.requestTimeoutMs,
    extraConfig: linesToExtraConfig(draft.extraConfig).json,
  }
}

function cloneDraft(draft: ClusterDraft): ClusterDraft {
  return JSON.parse(JSON.stringify(draft)) as ClusterDraft
}

/** 生成 id 片段；纯中文等无法转写的内容返回空串，交给调用方换下一个候选。 */
function slugId(text: string): string {
  return text.trim().replace(/[^\w.-]+/g, '-').replace(/^-+|-+$/g, '')
}

function extraConfigToLines(raw: string): string {
  const text = raw.trim()
  if (!text) return ''
  try {
    const parsed: unknown = JSON.parse(text)
    const obj = rowOf(parsed)
    if (!obj) return text
    return Object.entries(obj)
      .map(([key, value]) => `${key}=${typeof value === 'string' ? value : JSON.stringify(value)}`)
      .join('\n')
  } catch {
    return text
  }
}

function linesToExtraConfig(text: string): { json: string; error?: string } {
  const lines = text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean)
  const obj: Record<string, string> = {}
  for (const line of lines) {
    const index = line.indexOf('=')
    if (index <= 0) return { json: '', error: `“${line}” 不是 key=value 形式` }
    obj[line.slice(0, index).trim()] = line.slice(index + 1).trim()
  }
  return { json: Object.keys(obj).length > 0 ? JSON.stringify(obj) : '' }
}

function validateDraft(draft: ClusterDraft): string | undefined {
  if (!draft.name.trim()) return '集群名称不能为空'
  if (splitServers(draft.bootstrap).length === 0) return 'Bootstrap Servers 不能为空'
  if (!SECURITY_TYPES.includes(draft.security)) return `安全类型无效：${draft.security}`
  if (usesSasl(draft.security)) {
    if (!SASL_MECHANISMS.includes(draft.sasl)) return `SASL 机制无效：${draft.sasl}`
    if (draft.sasl === 'GSSAPI') {
      if (draft.krb.autoKinit) {
        if (!draft.krb.keytabPath.trim()) return '勾选“自动 kinit”后必须填写 keytab 路径'
        if (!draft.krb.principal.includes('@')) return '自动 kinit 需要带 realm 的 principal（user@REALM）'
      }
    } else {
      if (!draft.plainUser.trim()) return `${draft.sasl} 需要填写用户名`
      if (!draft.plainPass) return `${draft.sasl} 需要填写口令`
    }
  }
  if (usesTls(draft.security)) {
    const hasCert = draft.tls.clientCertPemPath.trim().length > 0
    const hasKey = draft.tls.clientKeyPemPath.trim().length > 0
    if (hasCert !== hasKey) return 'TLS 客户端证书与私钥必须同时填写'
  }
  if (!(draft.maxMessageBytes > 0)) return '最大消息字节必须为正数'
  return undefined
}

/* ── 面板 ─────────────────────────────────────────────────────────────── */

function buildPanel(channel: SettingsChannel, rpcConn: any): PanelHandle {
  const status = el('div', { style: 'font-size:12.5px;', textContent: '' })
  const setStatus = (text: string, isError = false): void => {
    status.textContent = text
    status.setAttribute('style', `font-size:12.5px;${isError ? STATUS_ERR : STATUS_OK}`)
  }

  let chromeMode: boolean | 'browse' | 'config' = false
  let defaults = { maxRows: 100, maxPerPartition: 50, maxValueBytes: 4096 }

  /* 工具栏 */
  const clusterSelect = el('select', { style: INPUT_CSS + 'min-width:180px;' }) as HTMLSelectElement
  const pingBtn = el('button', { type: 'button', textContent: '测试连接', style: BTN_CSS }) as HTMLButtonElement
  const kinitBtn = el('button', { type: 'button', textContent: '🔑 取票', style: BTN_CSS }) as HTMLButtonElement
  const refreshBtn = el('button', { type: 'button', textContent: '⟳ 刷新', style: BTN_CSS }) as HTMLButtonElement
  const kinitStatus = el('span', { style: 'font-size:12px;color:var(--fg-muted,#57606a);', textContent: '' })

  /* 数据视图 */
  const tabBar = el('div', { style: 'display:flex;gap:4px;border-bottom:1px solid rgba(127,127,127,.25);' })
  const tabDefs: Array<{ key: string; label: string }> = [
    { key: 'topics', label: '主题' },
    { key: 'groups', label: '消费者组' },
    { key: 'brokers', label: 'Brokers' },
    { key: 'health', label: '健康巡检' },
  ]
  const tabButtons: Record<string, HTMLButtonElement> = {}
  for (const def of tabDefs) {
    const btn = el('button', {
      type: 'button',
      textContent: def.label,
      style: 'padding:5px 10px;background:transparent;border:none;cursor:pointer;font-size:12.5px;' + TAB_IDLE,
    }) as HTMLButtonElement
    btn.addEventListener('click', () => void switchTab(def.key))
    tabButtons[def.key] = btn
    tabBar.append(btn)
  }
  let activeTab = 'topics'

  const viewHost = el('div', { style: 'display:flex;flex-direction:column;gap:8px;min-height:120px;' })

  /* —— 主题：列表页 / 详情页分开 —— */
  const topicsView = el('div', { style: 'display:flex;flex-direction:column;gap:8px;' })

  const topicListView = el('div', { style: 'display:flex;flex-direction:column;gap:8px;' })
  const topicFilter = el('input', {
    type: 'search',
    placeholder: '过滤主题（支持子串）',
    style: INPUT_CSS + 'width:240px;',
  }) as HTMLInputElement
  const topicsWrap = el('div', { style: 'overflow:auto;max-height:400px;' })
  const topicsTable = el('table', { style: TABLE_CSS })
  topicsWrap.append(topicsTable)
  topicListView.append(
    el('div', { style: 'display:flex;gap:8px;align-items:center;flex-wrap:wrap;' }, [topicFilter]),
    topicsWrap,
  )

  const topicDetailView = el('div', { style: 'display:none;flex-direction:column;gap:8px;' })
  const topicBackBtn = el('button', {
    type: 'button',
    textContent: '← 主题列表',
    style: BTN_CSS + 'flex-shrink:0;',
  }) as HTMLButtonElement
  const topicTitle = el('div', { style: 'font-weight:700;font-size:13.5px;' })
  const topicMeta = el('div', { style: 'font-size:12px;color:var(--fg-muted,#57606a);' })
  const partitionsWrap = el('div', { style: 'overflow:auto;max-height:220px;' })
  const partitionsTable = el('table', { style: TABLE_CSS })
  partitionsWrap.append(partitionsTable)

  const topicConfigsBox = el('details', { style: 'font-size:12.5px;' })
  topicConfigsBox.append(el('summary', { textContent: 'Topic 配置', style: 'cursor:pointer;font-weight:600;' }))
  const topicConfigsWrap = el('div', { style: 'overflow:auto;max-height:200px;margin-top:4px;' })
  const topicConfigsTable = el('table', { style: TABLE_CSS })
  topicConfigsWrap.append(topicConfigsTable)
  topicConfigsBox.append(topicConfigsWrap)

  /* 浏览消息 */
  const startSelect = el('select', { style: INPUT_CSS }) as HTMLSelectElement
  for (const mode of ['NEWEST', 'OLDEST', 'SPECIFIC', 'TIMESTAMP']) {
    startSelect.append(el('option', { value: mode, textContent: mode }))
  }
  const offsetInput = el('input', {
    type: 'number',
    placeholder: '起始 offset',
    style: INPUT_CSS + 'width:120px;display:none;',
  }) as HTMLInputElement
  const timestampInput = el('input', {
    type: 'datetime-local',
    style: INPUT_CSS + 'display:none;',
  }) as HTMLInputElement
  const partitionsInput = el('input', {
    type: 'text',
    placeholder: '分区（逗号分隔，空=全部）',
    style: INPUT_CSS + 'width:190px;',
  }) as HTMLInputElement
  const perPartInput = el('input', {
    type: 'number',
    title: '每个分区最多取多少条',
    style: INPUT_CSS + 'width:80px;',
  }) as HTMLInputElement
  const limitInput = el('input', {
    type: 'number',
    title: '总条数上限',
    style: INPUT_CSS + 'width:90px;',
  }) as HTMLInputElement
  const browseBtn = el('button', { type: 'button', textContent: '拉取消息', style: BTN_PRIMARY_CSS }) as HTMLButtonElement

  const messagesWrap = el('div', { style: 'overflow:auto;max-height:300px;' })
  const messagesTable = el('table', { style: TABLE_CSS })
  messagesWrap.append(messagesTable)
  const messageDetail = el('pre', { style: PREVIEW_CSS + 'display:none;' })

  /* 生产消息 */
  const produceKey = el('input', { type: 'text', placeholder: 'key（可空）', style: INPUT_CSS + 'width:180px;' }) as HTMLInputElement
  const produceValue = el('textarea', {
    rows: '2',
    placeholder: 'value（文本）',
    style: INPUT_CSS + 'width:100%;font-family:ui-monospace,monospace;',
  }) as HTMLTextAreaElement
  const producePartition = el('input', {
    type: 'number',
    placeholder: '分区（可空）',
    style: INPUT_CSS + 'width:120px;',
  }) as HTMLInputElement
  const produceBtn = el('button', { type: 'button', textContent: '发送消息', style: BTN_PRIMARY_CSS }) as HTMLButtonElement

  topicDetailView.append(
    el('div', { style: 'display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;' }, [
      topicBackBtn,
      topicTitle,
      topicMeta,
    ]),
    partitionsWrap,
    el('div', { style: 'display:flex;gap:8px;align-items:center;flex-wrap:wrap;' }, [
      el('span', { textContent: '起始' }),
      startSelect,
      offsetInput,
      timestampInput,
      partitionsInput,
      el('span', { textContent: '每分区' }),
      perPartInput,
      el('span', { textContent: '总数' }),
      limitInput,
      browseBtn,
    ]),
    messagesWrap,
    messageDetail,
    el('div', { style: 'border-top:1px dashed rgba(127,127,127,.3);padding-top:8px;display:flex;flex-direction:column;gap:6px;' }, [
      el('span', { style: 'font-weight:600;', textContent: '生产消息' }),
      el('div', { style: 'display:flex;gap:8px;align-items:center;flex-wrap:wrap;' }, [
        produceKey,
        producePartition,
        el('span', { style: 'font-size:12px;color:var(--fg-muted,#57606a);', textContent: '分区留空 = 由分区器决定' }),
      ]),
      produceValue,
      el('div', {}, [produceBtn]),
    ]),
    topicConfigsBox,
  )

  topicsView.append(topicListView, topicDetailView)

  /* —— 消费者组 —— */
  const groupsView = el('div', { style: 'display:none;flex-direction:column;gap:8px;' })
  const groupsWrap = el('div', { style: 'overflow:auto;max-height:260px;' })
  const groupsTable = el('table', { style: TABLE_CSS })
  groupsWrap.append(groupsTable)
  const groupDetailBox = el('div', {
    style: 'display:none;border-top:1px solid rgba(127,127,127,.25);padding-top:8px;flex-direction:column;gap:8px;',
  })
  const groupTitle = el('div', { style: 'font-weight:700;' })
  const groupMembersWrap = el('div', { style: 'overflow:auto;max-height:260px;' })
  const groupMembersTable = el('table', { style: TABLE_CSS })
  groupMembersWrap.append(groupMembersTable)
  groupDetailBox.append(groupTitle, groupMembersWrap)
  groupsView.append(groupsWrap, groupDetailBox)

  /* —— Brokers —— */
  const brokersView = el('div', { style: 'display:none;flex-direction:column;gap:8px;' })
  const brokersWrap = el('div', { style: 'overflow:auto;max-height:260px;' })
  const brokersTable = el('table', { style: TABLE_CSS })
  brokersWrap.append(brokersTable)
  const brokerDetailBox = el('div', {
    style: 'display:none;border-top:1px solid rgba(127,127,127,.25);padding-top:8px;flex-direction:column;gap:8px;',
  })
  const brokerTitle = el('div', { style: 'font-weight:700;' })
  const brokerConfigsWrap = el('div', { style: 'overflow:auto;max-height:280px;' })
  const brokerConfigsTable = el('table', { style: TABLE_CSS })
  brokerConfigsWrap.append(brokerConfigsTable)
  brokerDetailBox.append(brokerTitle, brokerConfigsWrap)
  brokersView.append(brokersWrap, brokerDetailBox)

  /* —— 健康 —— */
  const healthView = el('div', { style: 'display:none;flex-direction:column;gap:8px;' })
  const healthSummary = el('div', { style: 'display:flex;gap:14px;flex-wrap:wrap;font-size:12.5px;' })
  const healthWrap = el('div', { style: 'overflow:auto;max-height:320px;' })
  const healthTable = el('table', { style: TABLE_CSS })
  healthWrap.append(healthTable)
  const healthTopicErrors = el('pre', { style: PREVIEW_CSS + 'display:none;' })
  healthView.append(healthSummary, healthWrap, healthTopicErrors)

  viewHost.append(topicsView, groupsView, brokersView, healthView)

  /* —— 配置区：表单（对齐原版 Offset Explorer 连接对话框） —— */
  const cfgStatus = el('div', { style: 'font-size:12px;color:var(--fg-muted,#57606a);' })
  const cfgDirtyMark = el('span', { style: 'font-size:12px;color:#9a6700;' })

  const sectionTitle = (text: string): HTMLElement =>
    el('div', {
      textContent: text,
      style: 'font-weight:700;font-size:12px;margin-top:4px;color:var(--fg-muted,#57606a);',
    })
  const fieldGrid = (children: HTMLElement[]): HTMLElement =>
    el('div', { style: 'display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px 12px;' }, children)
  function formField(label: string, control: HTMLElement, hint?: string): HTMLElement {
    const wrap = el('label', { style: 'display:flex;flex-direction:column;gap:3px;font-size:12px;min-width:0;' })
    wrap.append(el('span', { textContent: label, style: 'font-weight:600;' }), control)
    if (hint) {
      wrap.append(el('span', { textContent: hint, style: 'color:var(--fg-muted,#57606a);font-size:11.5px;' }))
    }
    return wrap
  }
  const textField = (placeholder = '', type = 'text'): HTMLInputElement =>
    el('input', { type, placeholder, spellcheck: 'false', style: INPUT_CSS }) as HTMLInputElement
  const numberField = (min: number): HTMLInputElement =>
    el('input', { type: 'number', min: String(min), style: INPUT_CSS }) as HTMLInputElement
  function selectField(options: string[]): HTMLSelectElement {
    const select = el('select', { style: INPUT_CSS }) as HTMLSelectElement
    for (const option of options) select.append(el('option', { value: option, textContent: option }))
    return select
  }
  function checkField(label: string): { root: HTMLElement; input: HTMLInputElement } {
    const input = el('input', { type: 'checkbox' }) as HTMLInputElement
    const root = el('label', { style: 'display:flex;gap:6px;align-items:center;font-size:12px;cursor:pointer;' }, [
      input,
      el('span', { textContent: label }),
    ])
    return { root, input }
  }

  /* 浏览默认值 */
  const maxRowsInput = numberField(1)
  const maxPerPartInput = numberField(1)
  const maxBytesInput = numberField(64)
  const defaultClusterSelect = selectField([])

  /* 集群编辑表单字段 */
  const eName = textField('例如 prod-kafka')
  const eId = textField('留空自动生成')
  const eGroup = textField('可选，用于分组')
  const eDesc = textField('可选')
  const eBootstrap = textField('broker1:9092, broker2:9092')
  const eSecurity = selectField(SECURITY_TYPES)
  const eSasl = selectField(SASL_MECHANISMS)
  const eAutoCreate = checkField('自动创建 topic')
  const eConsumerGroup = textField('留空自动生成')
  const eDialTimeout = numberField(1)
  const eRequestTimeout = numberField(1)
  const eMaxBytes = numberField(1)

  const eKrb5 = textField('krb5.conf 路径')
  const eCcache = textField('ccache 路径')
  const eKeytab = textField('keytab 路径')
  const ePrincipal = textField('user@REALM')
  const eKinit = textField('留空自动探测')
  const eService = textField('默认 kafka')
  const eAutoKinit = checkField('自动执行 kinit 取票')

  const ePlainUser = textField('用户名')
  const ePlainPass = textField('口令', 'password')
  const eScramAuthz = textField('可选')

  const eCa = textField('CA 证书 PEM 路径')
  const eCert = textField('客户端证书 PEM 路径')
  const eKey = textField('客户端私钥 PEM 路径')
  const eSni = textField('默认取 broker 主机名')
  const eInsecure = checkField('跳过证书校验（仅测试）')

  const eExtra = el('textarea', {
    rows: '3',
    spellcheck: 'false',
    placeholder: 'key=value，每行一条，例如 ssl.ca.location=/path/ca.pem',
    style: INPUT_CSS + 'width:100%;font-family:ui-monospace,monospace;',
  }) as HTMLTextAreaElement

  const saslField = formField('SASL 机制', eSasl)
  const krbSection = el('div', { style: 'display:none;flex-direction:column;gap:8px;' }, [
    sectionTitle('Kerberos'),
    fieldGrid([
      formField('krb5.conf 路径', eKrb5),
      formField('ccache 路径', eCcache, '留空按 principal 生成临时 ccache'),
      formField('keytab 路径', eKeytab),
      formField('principal', ePrincipal, '自动 kinit 时必须带 @REALM'),
      formField('kinit 路径', eKinit),
      formField('SPN 服务名', eService, '默认 kafka'),
    ]),
    eAutoKinit.root,
  ])
  const plainSection = el('div', { style: 'display:none;flex-direction:column;gap:8px;' }, [
    sectionTitle('SASL 口令'),
    fieldGrid([
      formField('用户名', ePlainUser),
      formField('口令', ePlainPass),
      formField('SCRAM authzId', eScramAuthz),
    ]),
  ])
  const tlsSection = el('div', { style: 'display:none;flex-direction:column;gap:8px;' }, [
    sectionTitle('TLS'),
    fieldGrid([
      formField('CA 证书', eCa),
      formField('客户端证书', eCert),
      formField('客户端私钥', eKey),
      formField('SNI 主机名', eSni),
    ]),
    eInsecure.root,
  ])

  const eTitle = el('div', { style: 'font-weight:700;font-size:13px;' })
  const eOkBtn = el('button', { type: 'button', textContent: '确定', style: BTN_PRIMARY_CSS }) as HTMLButtonElement
  const eCancelBtn = el('button', { type: 'button', textContent: '取消', style: BTN_CSS }) as HTMLButtonElement

  const editorBox = el('div', {
    style: 'display:none;flex-direction:column;gap:8px;border:1px solid rgba(127,127,127,.35);' +
      'border-radius:8px;padding:10px;background:var(--bg,#f6f8fa);',
  }, [
    eTitle,
    sectionTitle('基本'),
    fieldGrid([
      formField('集群名称', eName),
      formField('集群 id', eId, '省略时按名称 / 首个 broker 生成'),
      formField('分组', eGroup),
      formField('备注', eDesc),
    ]),
    formField('Bootstrap Servers', eBootstrap, '多个 broker 用逗号分隔'),
    fieldGrid([
      formField('最大消息字节', eMaxBytes),
      formField('连接超时（毫秒）', eDialTimeout),
      formField('请求超时（毫秒）', eRequestTimeout),
      formField('Consumer Group', eConsumerGroup),
    ]),
    eAutoCreate.root,
    sectionTitle('安全'),
    fieldGrid([formField('安全类型', eSecurity), saslField]),
    krbSection,
    plainSection,
    tlsSection,
    sectionTitle('高级'),
    formField('额外 librdkafka 配置（key=value）', eExtra, '每行一条；留空即不追加'),
    el('div', { style: 'display:flex;gap:8px;justify-content:flex-end;' }, [eOkBtn, eCancelBtn]),
  ])

  /* 集群列表与操作 */
  const clusterTable = el('table', { style: TABLE_CSS })
  const clusterTableWrap = el('div', { style: 'overflow:auto;max-height:240px;' })
  clusterTableWrap.append(clusterTable)

  const addClusterBtn = el('button', { type: 'button', textContent: '＋ 新建集群', style: BTN_CSS }) as HTMLButtonElement
  const saveConfigBtn = el('button', { type: 'button', textContent: '保存配置', style: BTN_PRIMARY_CSS }) as HTMLButtonElement
  const reloadConfigBtn = el('button', { type: 'button', textContent: '重新加载', style: BTN_CSS }) as HTMLButtonElement
  addClusterBtn.addEventListener('click', () => openEditor(-1, false))
  saveConfigBtn.addEventListener('click', () => void saveConfig())
  reloadConfigBtn.addEventListener('click', () => void refreshConfig())
  eSecurity.addEventListener('change', syncEditorSections)
  eSasl.addEventListener('change', syncEditorSections)
  eOkBtn.addEventListener('click', applyEditor)
  eCancelBtn.addEventListener('click', closeEditor)

  const configZone = el('div', {
    style: 'border-top:1px solid rgba(127,127,127,.25);margin-top:8px;padding-top:8px;display:flex;flex-direction:column;gap:8px;',
  }, [
    sectionTitle('浏览默认值'),
    fieldGrid([
      formField('最大行数', maxRowsInput),
      formField('每分区条数', maxPerPartInput),
      formField('单条消息字节上限', maxBytesInput),
      formField('默认集群', defaultClusterSelect),
    ]),
    sectionTitle('集群连接'),
    el('div', { style: 'display:flex;gap:8px;align-items:center;flex-wrap:wrap;' }, [
      addClusterBtn,
      saveConfigBtn,
      reloadConfigBtn,
      cfgDirtyMark,
    ]),
    clusterTableWrap,
    editorBox,
    cfgStatus,
  ])

  /* 标题栏 */
  let maximized = false
  const closeBtn = el('button', {
    type: 'button',
    title: '关闭',
    textContent: '✕',
    style: 'margin-left:auto;padding:2px 8px;font-size:13px;cursor:pointer;' +
      'border:1px solid rgba(127,127,127,.3);background:transparent;color:inherit;border-radius:6px;display:none;',
  }) as HTMLButtonElement
  closeBtn.addEventListener('click', () => { root.style.display = 'none' })
  const maximizeBtn = el('button', {
    type: 'button',
    title: '最大化 / 还原',
    textContent: '⤢',
    style: 'margin-left:8px;padding:2px 8px;font-size:13px;cursor:pointer;' +
      'border:1px solid rgba(127,127,127,.3);background:transparent;color:inherit;border-radius:6px;display:none;',
  }) as HTMLButtonElement

  const headingLabel = el('span', { textContent: '🪵 Kafka 浏览器', style: 'user-select:none;' })
  const headingRow = el('div', {
    style: 'display:flex;align-items:center;font-weight:700;font-size:14px;flex-shrink:0;',
  }, [headingLabel, maximizeBtn, closeBtn])

  const browseZone = el('div', { style: 'display:flex;flex-direction:column;gap:8px;' }, [
    el('div', { style: 'display:flex;gap:8px;align-items:center;flex-wrap:wrap;' }, [
      el('span', { style: 'font-weight:600;', textContent: '集群' }),
      clusterSelect,
      pingBtn,
      kinitBtn,
      refreshBtn,
      kinitStatus,
    ]),
    tabBar,
    viewHost,
  ])

  const root = el('div', { style: PANEL_CSS }, [headingRow, status, browseZone, configZone])

  /* ── 数据加载 ───────────────────────────────────────────────────────── */

  function activeCluster(): string {
    return clusterSelect.value || ''
  }

  async function loadClusters(): Promise<void> {
    const value = (await callRpc(rpcConn, 'clusters', {})) as ClusterListValue
    const previous = clusterSelect.value
    clusterSelect.textContent = ''
    for (const c of value.clusters ?? []) {
      const label = `${c.id}${c.name && c.name !== c.id ? `（${c.name}）` : ''} · ${c.security}`
      clusterSelect.append(el('option', { value: c.id, textContent: label }))
    }
    if (previous) clusterSelect.value = previous
    if (!clusterSelect.value && (value.clusters?.length ?? 0) > 0) {
      clusterSelect.value = value.defaultCluster || value.clusters[0]!.id
    }
    if (value.defaults) {
      defaults = value.defaults
      if (!perPartInput.value) perPartInput.value = String(value.defaults.maxPerPartition)
      if (!limitInput.value) limitInput.value = String(value.defaults.maxRows)
    }
    if (value.clusterError) setStatus(`⚠ 配置解析失败：${value.clusterError}`, true)
  }

  async function ping(): Promise<void> {
    setStatus('连接测试中…')
    try {
      const id = activeCluster()
      await callRpc(rpcConn, 'ping', { cluster: id })
      setStatus(`✔ 集群 ${id} 连接正常`)
    } catch (err) {
      setStatus(`✘ ${errText(err)}`, true)
    }
  }

  async function kinit(): Promise<void> {
    const id = activeCluster()
    setStatus(`Kerberos 取票中（${id}）…`)
    try {
      const value = (await callRpc(rpcConn, 'kinit', { cluster: id })) as {
        ok: boolean
        principal: string
        ccachePath: string
        message: string
      }
      kinitStatus.textContent = value.ok ? `✔ ${value.principal}` : '○ 未取票'
      setStatus(`${value.ok ? '✔' : '○'} ${value.message}${value.ccachePath ? `（${value.ccachePath}）` : ''}`, !value.ok)
    } catch (err) {
      kinitStatus.textContent = ''
      setStatus(`✘ ${errText(err)}`, true)
    }
  }

  /* 主题列表 */
  let allTopics: TopicSummary[] = []
  let selectedTopic = ''

  function renderTopics(): void {
    const needle = topicFilter.value.trim().toLowerCase()
    const rows: TableRow[] = []
    for (const t of allTopics) {
      if (needle && !t.name.toLowerCase().includes(needle)) continue
      const flags: string[] = []
      if (t.internal) flags.push('内部')
      if (t.offlinePartitions > 0) flags.push(`离线分区 ${t.offlinePartitions}`)
      if (t.underReplicated > 0) flags.push(`副本不足 ${t.underReplicated}`)
      if (!t.usable) flags.push(t.unusableReason || '不可用')
      rows.push({
        cells: [
          t.name,
          String(t.partitionCount),
          String(t.replication),
          flags.join('；') || '—',
        ],
        onClick: () => void openTopic(t.name),
      })
    }
    renderTable(topicsTable, ['主题', '分区数', '副本数', '状态'], rows)
  }

  async function loadTopics(): Promise<void> {
    setStatus(`加载主题列表（${activeCluster()}）…`)
    try {
      const value = (await callRpc(rpcConn, 'topics', { cluster: activeCluster() })) as { topics: TopicSummary[] }
      allTopics = value.topics ?? []
      renderTopics()
      setStatus(`✔ ${allTopics.length} 个主题`)
    } catch (err) {
      allTopics = []
      renderTopics()
      setStatus(`✘ ${errText(err)}`, true)
    }
  }

  function showTopicList(): void {
    selectedTopic = ''
    topicDetailView.style.display = 'none'
    topicListView.style.display = 'flex'
  }

  async function openTopic(name: string): Promise<void> {
    selectedTopic = name
    topicListView.style.display = 'none'
    topicDetailView.style.display = 'flex'
    topicTitle.textContent = `📄 ${name}`
    topicMeta.textContent = '加载中…'
    partitionsTable.textContent = ''
    topicConfigsTable.textContent = ''
    messagesTable.textContent = ''
    messageDetail.style.display = 'none'
    try {
      const detail = (await callRpc(rpcConn, 'topicDetail', {
        cluster: activeCluster(),
        topic: name,
      })) as TopicDetail
      const t = detail.topic
      topicMeta.textContent =
        `${t.partitionCount} 分区 · 副本 ${t.replication}` +
        (t.internal ? ' · 内部主题' : '') +
        (t.offlinePartitions > 0 ? ` · 离线分区 ${t.offlinePartitions}` : '') +
        (t.underReplicated > 0 ? ` · 副本不足 ${t.underReplicated}` : '')
      const rows: TableRow[] = detail.partitions.map((p) => ({
        cells: [
          String(p.partition),
          p.leader >= 0 ? String(p.leader) : '离线',
          String(p.earliest),
          String(p.latest),
          String(p.latest - p.earliest),
          fmtBytes(p.size),
          p.isr.join(','),
          p.replicas.join(','),
          p.err || '—',
        ],
      }))
      renderTable(
        partitionsTable,
        ['分区', 'Leader', '最早 offset', '最新 offset', '消息数', '大小', 'ISR', '副本', '错误'],
        rows,
      )
      setStatus(`✔ 已加载 ${name}（${detail.partitions.length} 分区）`)
    } catch (err) {
      topicMeta.textContent = ''
      setStatus(`✘ ${errText(err)}`, true)
    }
    void loadTopicConfigs(name)
  }

  async function loadTopicConfigs(name: string): Promise<void> {
    try {
      const value = (await callRpc(rpcConn, 'topicConfigs', {
        cluster: activeCluster(),
        topic: name,
      })) as { configs: ConfigEntry[] }
      const rows: TableRow[] = (value.configs ?? []).map((c) => ({
        cells: [
          c.name,
          c.isSensitive ? '(敏感)' : c.value || '—',
          c.source === 5 ? '默认' : '覆盖',
          c.readOnly ? '只读' : '',
        ],
      }))
      renderTable(topicConfigsTable, ['配置项', '值', '来源', '读写'], rows)
    } catch (err) {
      renderTable(topicConfigsTable, ['配置项', '值'], [{ cells: [`读取失败：${errText(err)}`] }])
    }
  }

  async function reloadTopicView(): Promise<void> {
    if (selectedTopic) await openTopic(selectedTopic)
    else await loadTopics()
  }

  function parsePartitions(raw: string): number[] {
    return raw
      .split(/[,\s]+/)
      .map((s) => s.trim())
      .filter(Boolean)
      .map(Number)
      .filter((n) => Number.isInteger(n) && n >= 0)
  }

  async function browseMessages(): Promise<void> {
    if (!selectedTopic) return
    const start = startSelect.value || 'NEWEST'
    const offset = start === 'SPECIFIC' ? Number(offsetInput.value || 0) : 0
    if (start === 'TIMESTAMP') {
      const ms = timestampInput.value ? new Date(timestampInput.value).getTime() : 0
      if (!Number.isFinite(ms) || ms <= 0) {
        setStatus('✘ 请选择起始时间', true)
        return
      }
      // 复用 offset 参数传递 epoch ms（宿主按 TIMESTAMP 模式解释）
      await runBrowse(start, ms)
      return
    }
    await runBrowse(start, Number.isFinite(offset) ? offset : 0)
  }

  async function runBrowse(start: string, offset: number): Promise<void> {
    setStatus(`拉取 ${selectedTopic} 消息…`)
    messagesTable.textContent = ''
    messageDetail.style.display = 'none'
    try {
      const value = (await callRpc(rpcConn, 'browse', {
        cluster: activeCluster(),
        topic: selectedTopic,
        start,
        offset,
        partitions: parsePartitions(partitionsInput.value),
        perPartition: Number(perPartInput.value || defaults.maxPerPartition),
        limit: Number(limitInput.value || defaults.maxRows),
        maxValueBytes: defaults.maxValueBytes,
      })) as BrowseResult
      const rows: TableRow[] = (value.messages ?? []).map((m) => ({
        cells: [
          String(m.partition),
          String(m.offset),
          fmtTime(m.timestampMs),
          short(m.keyText || (m.keyBase64 ? `<base64 ${m.keyBase64.length}>` : ''), 24) || '—',
          short(m.valueText || (m.valueBase64 ? `<base64 ${m.valueBase64.length}>` : ''), 90) || '—',
        ],
        onClick: () => showMessage(m),
      }))
      renderTable(messagesTable, ['分区', 'offset', '时间', 'key', 'value'], rows)
      setStatus(
        `✔ ${value.messages?.length ?? 0} 条消息` +
          (value.truncated ? '（已截断，可调大总数/减少每分区条数）' : ''),
      )
    } catch (err) {
      setStatus(`✘ ${errText(err)}`, true)
    }
  }

  function showMessage(m: MessageRecord): void {
    const headers = (m.headers ?? [])
      .map((h) => `${h.key}=${h.valueText || `<base64 ${h.valueBase64.length}>`}`)
      .join('\n')
    const lines = [
      `topic=${m.topic} partition=${m.partition} offset=${m.offset}`,
      `time=${fmtTime(m.timestampMs)} length=${m.valueLength}${m.truncated ? ' (已截断)' : ''}`,
      '',
      `key(text): ${m.keyText || '(二进制)'}`,
      m.keyBase64 ? `key(base64): ${m.keyBase64}` : '',
      '',
      `value(text):`,
      m.valueText || '(二进制)',
      m.valueBase64 ? `\nvalue(base64): ${m.valueBase64}` : '',
      headers ? `\nheaders:\n${headers}` : '',
    ]
    messageDetail.textContent = lines.filter((l) => l !== '').join('\n')
    messageDetail.style.display = ''
  }

  async function produceMessage(): Promise<void> {
    if (!selectedTopic) return
    const value = produceValue.value
    setStatus(`发送到 ${selectedTopic}…`)
    try {
      const partitionRaw = producePartition.value.trim()
      const result = (await callRpc(rpcConn, 'produce', {
        cluster: activeCluster(),
        topic: selectedTopic,
        key: produceKey.value,
        value,
        partition: partitionRaw ? Number(partitionRaw) : -1,
      })) as { partition: number; offset: number }
      setStatus(`✔ 已发送：partition=${result.partition}, offset=${result.offset}`)
    } catch (err) {
      setStatus(`✘ ${errText(err)}`, true)
    }
  }

  /* 消费者组 */
  async function loadGroups(): Promise<void> {
    setStatus('加载消费者组…')
    groupDetailBox.style.display = 'none'
    try {
      const value = (await callRpc(rpcConn, 'groups', { cluster: activeCluster() })) as { groups: GroupSummary[] }
      const rows: TableRow[] = (value.groups ?? []).map((g) => ({
        cells: [g.name, g.state || '—', g.protocol || '—', g.protocolType || '—'],
        onClick: () => void openGroup(g.name),
      }))
      renderTable(groupsTable, ['消费者组', '状态', '协议', '协议类型'], rows)
      setStatus(`✔ ${value.groups?.length ?? 0} 个消费者组`)
    } catch (err) {
      renderTable(groupsTable, ['消费者组', '状态', '协议', '协议类型'], [])
      setStatus(`✘ ${errText(err)}`, true)
    }
  }

  async function openGroup(name: string): Promise<void> {
    groupDetailBox.style.display = 'flex'
    groupTitle.textContent = `👥 ${name}`
    groupMembersTable.textContent = ''
    try {
      const detail = (await callRpc(rpcConn, 'groupDetail', {
        cluster: activeCluster(),
        group: name,
      })) as GroupDetail
      groupTitle.textContent = `👥 ${name}（${detail.state || '未知状态'} · ${detail.members?.length ?? 0} 成员）`
      const rows: TableRow[] = (detail.members ?? []).map((m) => ({
        cells: [
          m.memberId,
          m.clientId || '—',
          m.clientHost || '—',
          (m.assignments ?? []).map((a) => `${a.topic}[${a.partition}]`).join(', ') || '—',
        ],
      }))
      renderTable(groupMembersTable, ['成员', 'client.id', '主机', '分配'], rows)
    } catch (err) {
      groupTitle.textContent = `👥 ${name}`
      renderTable(groupMembersTable, ['成员', 'client.id', '主机', '分配'], [{ cells: [errText(err)] }])
    }
  }

  /* Brokers */
  async function loadBrokers(): Promise<void> {
    setStatus('加载 broker 列表…')
    brokerDetailBox.style.display = 'none'
    try {
      const value = (await callRpc(rpcConn, 'brokers', { cluster: activeCluster() })) as { brokers: BrokerInfo[] }
      const rows: TableRow[] = (value.brokers ?? []).map((b) => ({
        cells: [
          String(b.nodeId),
          `${b.host}:${b.port}`,
          b.rack || '—',
          b.isController ? 'controller' : '',
          b.isSeed ? 'seed' : '',
        ],
        onClick: () => void openBroker(b.nodeId),
      }))
      renderTable(brokersTable, ['NodeId', '地址', 'Rack', '角色', ''], rows)
      setStatus(`✔ ${value.brokers?.length ?? 0} 个 broker`)
    } catch (err) {
      renderTable(brokersTable, ['NodeId', '地址', 'Rack', '角色', ''], [])
      setStatus(`✘ ${errText(err)}`, true)
    }
  }

  async function openBroker(nodeId: number): Promise<void> {
    brokerDetailBox.style.display = 'flex'
    brokerTitle.textContent = `⚙ Broker ${nodeId} 配置`
    brokerConfigsTable.textContent = ''
    try {
      const value = (await callRpc(rpcConn, 'brokerConfigs', {
        cluster: activeCluster(),
        brokerId: nodeId,
      })) as { configs: ConfigEntry[] }
      const rows: TableRow[] = (value.configs ?? []).map((c) => ({
        cells: [c.name, c.isSensitive ? '(敏感)' : c.value || '—', c.readOnly ? '只读' : '', c.docs || ''],
      }))
      renderTable(brokerConfigsTable, ['配置项', '值', '读写', '说明'], rows)
    } catch (err) {
      renderTable(brokerConfigsTable, ['配置项', '值'], [{ cells: [errText(err)] }])
    }
  }

  /* 健康 */
  async function loadHealth(): Promise<void> {
    setStatus('健康巡检中…')
    healthSummary.textContent = ''
    healthTopicErrors.style.display = 'none'
    try {
      const value = (await callRpc(rpcConn, 'health', { cluster: activeCluster() })) as HealthReport
      healthSummary.append(
        el('span', { textContent: `扫描 topic：${value.scannedTopics}` }),
        el('span', { textContent: `扫描分区：${value.scannedPartitions}` }),
        el('span', {
          textContent: `异常分区：${value.issues?.length ?? 0}`,
          style: (value.issues?.length ?? 0) > 0 ? STATUS_ERR : STATUS_OK,
        }),
        el('span', {
          textContent: `topic 错误：${value.topicErrors?.length ?? 0}`,
          style: (value.topicErrors?.length ?? 0) > 0 ? STATUS_ERR : '',
        }),
        el('span', {
          textContent: `缺失 broker：${value.missingBrokers?.length ? value.missingBrokers.join(',') : '无'}`,
          style: value.missingBrokers?.length ? STATUS_ERR : '',
        }),
      )
      const rows: TableRow[] = (value.issues ?? []).map((i) => ({
        cells: [
          i.topic,
          String(i.partition),
          i.leader >= 0 ? String(i.leader) : '离线',
          i.offlineReps?.length ? i.offlineReps.join(',') : '—',
          (i.labels ?? []).join('、'),
        ],
      }))
      renderTable(healthTable, ['Topic', '分区', 'Leader', '离线副本', '问题'], rows)
      if ((value.topicErrors?.length ?? 0) > 0) {
        healthTopicErrors.textContent = value.topicErrors.map((e) => `${e.topic}: ${e.err}`).join('\n')
        healthTopicErrors.style.display = ''
      }
      setStatus(
        (value.issues?.length ?? 0) === 0 && (value.topicErrors?.length ?? 0) === 0
          ? '✔ 未发现异常'
          : `⚠ 发现 ${value.issues?.length ?? 0} 个异常分区`,
        (value.issues?.length ?? 0) > 0,
      )
    } catch (err) {
      setStatus(`✘ ${errText(err)}`, true)
    }
  }

  /* Tab 切换 */
  async function switchTab(key: string): Promise<void> {
    activeTab = key
    for (const def of tabDefs) {
      tabButtons[def.key]!.setAttribute(
        'style',
        'padding:5px 10px;background:transparent;border:none;cursor:pointer;font-size:12.5px;' +
          (def.key === key ? TAB_ACTIVE : TAB_IDLE),
      )
    }
    topicsView.style.display = key === 'topics' ? 'flex' : 'none'
    groupsView.style.display = key === 'groups' ? 'flex' : 'none'
    brokersView.style.display = key === 'brokers' ? 'flex' : 'none'
    healthView.style.display = key === 'health' ? 'flex' : 'none'
    if (key === 'topics') await reloadTopicView()
    else if (key === 'groups') await loadGroups()
    else if (key === 'brokers') await loadBrokers()
    else if (key === 'health') await loadHealth()
  }

  /* 配置（表单 ⇄ clusters JSON） */
  let clusterDrafts: ClusterDraft[] = []
  let editingIndex = -1
  let editingSource: ClusterDraft | null = null
  let configDirty = false

  function setCfgStatus(text: string, isError = false): void {
    cfgStatus.textContent = text
    cfgStatus.setAttribute(
      'style',
      'font-size:12px;' + (isError ? STATUS_ERR : 'color:var(--fg-muted,#57606a);'),
    )
  }

  function renderDirty(): void {
    cfgDirtyMark.textContent = configDirty ? '● 有未保存的修改' : ''
  }

  function markDirty(): void {
    configDirty = true
    renderDirty()
  }

  function positiveInt(raw: string, fallback: number): number {
    const value = Number(raw)
    return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback
  }

  function parseClustersText(raw: unknown): { rows: JsonRow[]; error?: string } {
    if (Array.isArray(raw)) {
      const rows = raw.map(rowOf).filter((row): row is JsonRow => row !== undefined)
      return { rows }
    }
    if (typeof raw !== 'string' || raw.trim().length === 0) return { rows: [] }
    try {
      const parsed: unknown = JSON.parse(raw)
      if (!Array.isArray(parsed)) return { rows: [], error: 'clusters 不是数组' }
      const rows = parsed.map(rowOf).filter((row): row is JsonRow => row !== undefined)
      const skipped = parsed.length - rows.length
      return { rows, error: skipped > 0 ? `${skipped} 项不是对象，已跳过` : undefined }
    } catch (err) {
      return { rows: [], error: `clusters 解析失败：${errText(err)}` }
    }
  }

  function renderDefaultOptions(saved?: string): void {
    const current = saved ?? defaultClusterSelect.value
    defaultClusterSelect.textContent = ''
    defaultClusterSelect.append(el('option', { value: '', textContent: '（取第一个）' }))
    for (const draft of clusterDrafts) {
      if (!draft.id) continue
      defaultClusterSelect.append(el('option', { value: draft.id, textContent: draft.id }))
    }
    const exists = Array.from(defaultClusterSelect.options).some((option) => option.value === current)
    defaultClusterSelect.value = exists ? current : ''
  }

  function renderConfigList(savedDefault?: string): void {
    const rows: TableRow[] = clusterDrafts.map((draft, index) => {
      const actions = el('div', { style: 'display:flex;gap:6px;' })
      const action = (label: string, handler: () => void, danger = false): void => {
        const btn = el('button', {
          type: 'button',
          textContent: label,
          style: BTN_CSS + 'padding:2px 8px;font-size:11.5px;' + (danger ? 'color:#cf222e;' : ''),
        }) as HTMLButtonElement
        btn.addEventListener('click', handler)
        actions.append(btn)
      }
      action('编辑', () => openEditor(index, false))
      action('复制', () => openEditor(index, true))
      action('删除', () => removeCluster(index), true)
      const security = usesSasl(draft.security) ? `${draft.security} / ${draft.sasl}` : draft.security
      return {
        cells: [
          draft.name || '(未命名)',
          draft.id || '—',
          draft.bootstrap || '(未填写)',
          security,
          actions,
        ],
      }
    })
    renderTable(clusterTable, ['名称', 'id', 'Bootstrap Servers', '安全', '操作'], rows)
    renderDefaultOptions(savedDefault)
  }

  function syncEditorSections(): void {
    const security = eSecurity.value
    const sasl = usesSasl(security)
    saslField.style.display = sasl ? '' : 'none'
    krbSection.style.display = sasl && eSasl.value === 'GSSAPI' ? 'flex' : 'none'
    plainSection.style.display = sasl && eSasl.value !== 'GSSAPI' ? 'flex' : 'none'
    tlsSection.style.display = usesTls(security) ? 'flex' : 'none'
  }

  function fillEditor(draft: ClusterDraft): void {
    eName.value = draft.name
    eId.value = draft.id
    eGroup.value = draft.group
    eDesc.value = draft.description
    eBootstrap.value = draft.bootstrap
    eSecurity.value = draft.security
    eSasl.value = draft.sasl
    eAutoCreate.input.checked = draft.allowAutoCreateTopic
    eConsumerGroup.value = draft.consumerGroup
    eDialTimeout.value = String(draft.dialTimeoutMs)
    eRequestTimeout.value = String(draft.requestTimeoutMs)
    eMaxBytes.value = String(draft.maxMessageBytes)
    eKrb5.value = draft.krb.krb5ConfPath
    eCcache.value = draft.krb.ccachePath
    eKeytab.value = draft.krb.keytabPath
    ePrincipal.value = draft.krb.principal
    eKinit.value = draft.krb.kinitPath
    eService.value = draft.krb.service
    eAutoKinit.input.checked = draft.krb.autoKinit
    ePlainUser.value = draft.plainUser
    ePlainPass.value = draft.plainPass
    eScramAuthz.value = draft.scramAuthzId
    eCa.value = draft.tls.caPemPath
    eCert.value = draft.tls.clientCertPemPath
    eKey.value = draft.tls.clientKeyPemPath
    eSni.value = draft.tls.serverName
    eInsecure.input.checked = draft.tls.insecureSkipVerify
    eExtra.value = draft.extraConfig
    syncEditorSections()
  }

  function openEditor(index: number, copy: boolean): void {
    if (!channel.writable) {
      setCfgStatus('✘ 当前为只读模式（宿主未提供配置表单服务），无法编辑', true)
      return
    }
    const source = index >= 0 ? clusterDrafts[index] : undefined
    const draft = source ? cloneDraft(source) : blankDraft()
    if (copy) {
      draft.name = draft.name ? `${draft.name}（副本）` : draft.name
      draft.id = `${draft.id || slugId(draft.name) || 'cluster'}-copy`
    }
    editingIndex = copy ? -1 : index
    editingSource = draft
    eTitle.textContent = copy
      ? `复制集群：${source?.name || source?.id || ''}`
      : index >= 0
        ? `编辑集群：${source?.name || source?.id || ''}`
        : '新建集群'
    fillEditor(draft)
    editorBox.style.display = 'flex'
    eName.focus()
  }

  function closeEditor(): void {
    editorBox.style.display = 'none'
    editingIndex = -1
    editingSource = null
  }

  /** 表单当前值 → 草稿；未在表单出现的字段从编辑前的原始配置里保留。 */
  function collectDraft(): ClusterDraft {
    const draft = editingSource ? cloneDraft(editingSource) : blankDraft()
    draft.name = eName.value.trim()
    draft.id = eId.value.trim() || slugId(draft.name) || slugId(splitServers(eBootstrap.value)[0] ?? '') || 'cluster'
    draft.group = eGroup.value.trim()
    draft.description = eDesc.value.trim()
    draft.bootstrap = eBootstrap.value.trim()
    draft.security = eSecurity.value
    draft.sasl = eSasl.value
    draft.allowAutoCreateTopic = eAutoCreate.input.checked
    draft.consumerGroup = eConsumerGroup.value.trim()
    draft.dialTimeoutMs = positiveInt(eDialTimeout.value, 10_000)
    draft.requestTimeoutMs = positiveInt(eRequestTimeout.value, 20_000)
    draft.maxMessageBytes = positiveInt(eMaxBytes.value, 1024 * 1024)
    draft.krb = {
      ...draft.krb,
      krb5ConfPath: eKrb5.value.trim(),
      ccachePath: eCcache.value.trim(),
      keytabPath: eKeytab.value.trim(),
      principal: ePrincipal.value.trim(),
      kinitPath: eKinit.value.trim(),
      service: eService.value.trim(),
      autoKinit: eAutoKinit.input.checked,
    }
    draft.plainUser = ePlainUser.value.trim()
    draft.plainPass = ePlainPass.value
    draft.scramAuthzId = eScramAuthz.value.trim()
    draft.tls = {
      ...draft.tls,
      caPemPath: eCa.value.trim(),
      clientCertPemPath: eCert.value.trim(),
      clientKeyPemPath: eKey.value.trim(),
      serverName: eSni.value.trim(),
      insecureSkipVerify: eInsecure.input.checked,
    }
    draft.extraConfig = eExtra.value
    return draft
  }

  function applyEditor(): void {
    const draft = collectDraft()
    const problem = validateDraft(draft)
    if (problem) {
      setCfgStatus(`✘ ${problem}`, true)
      return
    }
    const extra = linesToExtraConfig(draft.extraConfig)
    if (extra.error) {
      setCfgStatus(`✘ 高级配置：${extra.error}`, true)
      return
    }
    const taken = new Set(
      clusterDrafts.filter((_, index) => index !== editingIndex).map((row) => row.id),
    )
    const baseId = draft.id
    for (let n = 2; taken.has(draft.id); n += 1) draft.id = `${baseId}-${n}`
    if (editingIndex >= 0) clusterDrafts[editingIndex] = draft
    else clusterDrafts.push(draft)
    closeEditor()
    renderConfigList()
    markDirty()
    setCfgStatus(`✔ ${draft.name} 已加入列表，点“保存配置”写入宿主`)
  }

  function removeCluster(index: number): void {
    const draft = clusterDrafts[index]
    if (!draft) return
    const label = draft.name || draft.id
    if (!window.confirm(`删除集群“${label}”？保存配置后生效。`)) return
    clusterDrafts.splice(index, 1)
    if (editingIndex === index) closeEditor()
    else if (editingIndex > index) editingIndex -= 1
    renderConfigList()
    markDirty()
    setCfgStatus(`已删除 ${label}，点“保存配置”生效`)
  }

  async function refreshConfig(): Promise<void> {
    try {
      const value = await channel.read()
      const parsed = parseClustersText(value.clusters)
      clusterDrafts = parsed.rows.map(draftFromRow)
      maxRowsInput.value = String(value.maxRows ?? defaults.maxRows)
      maxPerPartInput.value = String(value.maxPerPartition ?? defaults.maxPerPartition)
      maxBytesInput.value = String(value.maxValueBytes ?? defaults.maxValueBytes)
      renderConfigList(value.defaultCluster ?? '')
      closeEditor()
      configDirty = false
      renderDirty()
      addClusterBtn.disabled = !channel.writable
      saveConfigBtn.disabled = !channel.writable
      if (parsed.error) setCfgStatus(`⚠ ${parsed.error}`, true)
      else if (!channel.writable) setCfgStatus('当前为只读模式（宿主未提供配置表单服务）', true)
      else setCfgStatus('')
    } catch (err) {
      clusterDrafts = []
      renderConfigList('')
      closeEditor()
      setCfgStatus(`读取配置失败：${errText(err)}`, true)
    }
  }

  async function saveConfig(): Promise<void> {
    if (!channel.writable) {
      setCfgStatus('✘ 当前为只读模式（宿主未提供配置表单服务），无法保存', true)
      return
    }
    const problems: string[] = []
    const rows: JsonRow[] = []
    clusterDrafts.forEach((draft, index) => {
      const problem = validateDraft(draft)
      if (problem) problems.push(`第 ${index + 1} 个集群（${draft.name || draft.id}）：${problem}`)
      const extra = linesToExtraConfig(draft.extraConfig)
      if (extra.error) problems.push(`第 ${index + 1} 个集群高级配置：${extra.error}`)
      rows.push(draftToRow(draft))
    })
    if (problems.length > 0) {
      setCfgStatus(`✘ ${problems.join('；')}`, true)
      return
    }
    const maxRows = positiveInt(maxRowsInput.value, defaults.maxRows)
    const maxPerPartition = positiveInt(maxPerPartInput.value, defaults.maxPerPartition)
    const maxValueBytes = positiveInt(maxBytesInput.value, defaults.maxValueBytes)
    try {
      await channel.write({
        clusters: JSON.stringify(rows, null, 2),
        defaultCluster: defaultClusterSelect.value,
        maxRows,
        maxPerPartition,
        maxValueBytes,
      })
      defaults = { maxRows, maxPerPartition, maxValueBytes }
      configDirty = false
      renderDirty()
      setCfgStatus('✔ 配置已保存（热更新，无需重启）')
      await loadClusters()
      if (activeTab === 'topics') await reloadTopicView()
    } catch (err) {
      setCfgStatus(`保存失败：${errText(err)}`, true)
    }
  }

  /* 事件 */
  startSelect.addEventListener('change', () => {
    offsetInput.style.display = startSelect.value === 'SPECIFIC' ? '' : 'none'
    timestampInput.style.display = startSelect.value === 'TIMESTAMP' ? '' : 'none'
  })
  topicFilter.addEventListener('input', () => renderTopics())
  topicBackBtn.addEventListener('click', () => showTopicList())
  browseBtn.addEventListener('click', () => void browseMessages())
  produceBtn.addEventListener('click', () => void produceMessage())
  pingBtn.addEventListener('click', () => void ping())
  kinitBtn.addEventListener('click', () => void kinit())
  refreshBtn.addEventListener('click', () => {
    if (chromeMode === 'config') void refreshConfig()
    else if (activeTab === 'topics') void reloadTopicView()
    else void switchTab(activeTab)
  })
  clusterSelect.addEventListener('change', () => {
    showTopicList()
    void switchTab(activeTab)
  })
  maximizeBtn.addEventListener('click', () => {
    maximized = !maximized
    if (maximized) {
      root.style.left = '24px'
      root.style.top = '24px'
      root.style.right = '24px'
      root.style.bottom = '24px'
      root.style.width = 'auto'
      root.style.height = 'auto'
      root.style.maxHeight = 'none'
      maximizeBtn.textContent = '⤣'
    } else {
      root.style.left = 'auto'
      root.style.top = 'auto'
      root.style.right = '16px'
      root.style.bottom = '16px'
      root.style.width = '1000px'
      root.style.height = '640px'
      root.style.maxHeight = '88vh'
      maximizeBtn.textContent = '⤢'
    }
  })

  /* 布局裁剪 */
  const handle: PanelHandle = {
    root,
    refresh(): void {
      if (chromeMode === 'config') {
        void refreshConfig()
        return
      }
      void (async () => {
        try {
          await loadClusters()
          await refreshConfig().catch(() => {})
          await switchTab(activeTab)
        } catch (err) {
          setStatus(`✘ ${errText(err)}`, true)
        }
      })()
    },
    setChrome(mode: boolean | 'browse' | 'config') {
      chromeMode = mode
      const browseOnly = mode === 'browse'
      const configOnly = mode === 'config'
      browseZone.style.display = browseOnly || mode === true || mode === false ? 'flex' : 'none'
      configZone.style.display = configOnly || mode === true || mode === false ? 'flex' : 'none'
      closeBtn.style.display = browseOnly ? '' : 'none'
      maximizeBtn.style.display = browseOnly ? '' : 'none'
      if (browseOnly) {
        root.style.cssText =
          PANEL_CSS +
          'position:fixed;right:16px;bottom:16px;z-index:2147483646;width:1000px;height:640px;max-height:88vh;' +
          'overflow:auto;padding:14px;background:var(--bg,#fff);border:1px solid rgba(127,127,127,.35);' +
          'border-radius:10px;box-shadow:0 8px 28px rgba(0,0,0,.25);'
      } else if (configOnly) {
        root.style.cssText = PANEL_CSS
      } else {
        root.style.cssText = PANEL_CSS
      }
    },
  }
  return handle
}

/* ── 挂载：侧边栏按钮 + 设置页插槽 + 浮动兜底 ─────────────────────────── */

const SIDEBAR_FOOTER_SLOT = 'sidebar.footer.action'

function createNavIcon(React: any): any {
  return function NavIcon(props: Record<string, unknown>) {
    return React.createElement(
      'svg',
      Object.assign(
        { width: 16, height: 16, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, 'aria-hidden': true },
        props,
      ),
      React.createElement('rect', { x: 3, y: 4, width: 18, height: 6, rx: 2 }),
      React.createElement('rect', { x: 3, y: 14, width: 18, height: 6, rx: 2 }),
      React.createElement('circle', { cx: 7, cy: 7, r: 0.6 }),
      React.createElement('circle', { cx: 7, cy: 17, r: 0.6 }),
    )
  }
}

function createSidebarButton(React: any, handle: PanelHandle): any {
  const Icon = createNavIcon(React)
  return function KafkaSidebarButton({ wide }: { wide?: boolean }): any {
    const [open, setOpen] = React.useState(false)
    React.useEffect(function () {
      // 打开时显示浮动面板；面板 DOM 由 apply 预挂到 body（纯 DOM，不经过 React）
      handle.root.style.display = open ? 'flex' : 'none'
      if (open) handle.refresh()
    }, [open])
    return React.createElement(
      'button',
      {
        type: 'button',
        title: 'Kafka 浏览器',
        'data-dsh-offexplorer-sidebar-toggle': 'true',
        'aria-expanded': open ? 'true' : 'false',
        onClick: () => setOpen((v: boolean) => !v),
        style: {
          display: 'flex', alignItems: 'center', gap: wide === false ? '0' : '8px',
          justifyContent: wide === false ? 'center' : 'flex-start',
          width: '100%', boxSizing: 'border-box', margin: '2px 0', padding: '8px 10px',
          fontSize: '13px', cursor: 'pointer',
          border: '1px solid rgba(127,127,127,.25)',
          background: open ? 'rgba(127,127,127,.15)' : 'transparent',
          color: 'inherit', borderRadius: '8px',
        },
      },
      React.createElement(Icon, null),
      wide === false ? null : 'Kafka',
    )
  }
}

function tryRegisterSidebarEntry(ctx: any, handle: PanelHandle): boolean {
  const slots = service(ctx, 'slots')
  if (!slots || typeof slots.inject !== 'function') return false
  let React: any
  try { React = require('react') } catch { return false }
  if (!React || typeof React.createElement !== 'function') return false
  try {
    ctx.effect(() => slots.inject(SIDEBAR_FOOTER_SLOT, () => {
      const Button = createSidebarButton(React, handle)
      return slots.register(
        { name: SIDEBAR_FOOTER_SLOT, id: PLUGIN_ID, order: 51, label: 'Kafka' },
        Button,
      )
    }), 'offexplorer: sidebar footer button')
    return true
  } catch (err) {
    console.warn('[dsh-offexplorer] sidebar footer registration failed:', err)
    return false
  }
}

function settingsSlotOptions(): Array<[string, Record<string, unknown>]> {
  const label = (): string => 'Offset Explorer'
  return [
    ['settings.plugins.tab', { id: SETTINGS_NS, order: 21, label }],
    ['settings.plugin.item', { key: SETTINGS_NS, label }],
    ['settings.section', { id: PLUGIN_ID, label, title: 'Kafka 配置', icon: (React: any) => createNavIcon(React) }],
  ]
}

function createSettingsComponent(React: any, handle: PanelHandle): any {
  return function KafkaSettings() {
    const ref = React.useRef(null)
    React.useEffect(function () {
      const node = ref.current as HTMLElement | null
      if (!node) return
      const wrapper = doc.createElement('div')
      wrapper.setAttribute('data-dsh-offexplorer-settings', 'true')
      wrapper.append(handle.root)
      node.append(wrapper)
      handle.setChrome('config')
      handle.refresh()
    }, [])
    return React.createElement('div', { ref })
  }
}

function tryRegisterSettingsSlot(ctx: any, handle: PanelHandle): boolean {
  const register = (slots: any): boolean => {
    if (!slots || typeof slots.inject !== 'function') return false
    let React: any
    try { React = require('react') } catch { return false }
    if (!React || typeof React.createElement !== 'function') return false

    const options = settingsSlotOptions()
    const priority = new Map(options.map(([name], index) => [name, index]))
    const mountedAt = new Set<string>()
    const declared = (name: string): boolean => {
      if (typeof slots.specDynamic !== 'function') return false
      try { return Boolean(slots.specDynamic(name)) } catch { return false }
    }
    const Comp = createSettingsComponent(React, handle)
    let mounted = false

    for (const [name, opts] of options) {
      try {
        slots.inject(name, () => {
          for (const [other] of options) {
            if (priority.get(other)! < priority.get(name)! && declared(other)) return
          }
          if (mountedAt.size > 0 && !mountedAt.has(name)) return
          const slotOpts = Object.assign({ name }, opts)
          if (name === 'settings.section' && typeof opts.icon === 'function') {
            slotOpts.icon = opts.icon(React)
          }
          const dispose = slots.register(slotOpts, Comp)
          mountedAt.add(name)
          mounted = true
          return () => {
            mountedAt.delete(name)
            if (typeof dispose === 'function') dispose()
          }
        })
      } catch { /* 单个插槽注入失败不影响其余候选 */ }
    }
    return mounted
  }

  let mounted = false
  try {
    if (typeof ctx?.inject === 'function') {
      ctx.inject(['slots'], (scope: any) => { if (register(scope?.slots)) mounted = true })
    }
    if (register(service(ctx, 'slots'))) mounted = true
  } catch { /* 回退到浮动启动器 */ }
  return mounted
}

/* ── 客户端插件契约 ────────────────────────────────────────────────────── */

function apply(ctx: any): void {
  if (win.__dshOffexplorerMounted === true) return
  const conn = service(ctx, 'connection')
  if (!conn || !conn.rpc || typeof conn.rpc.call !== 'function') return
  win.__dshOffexplorerMounted = true

  const channel = createChannel(ctx, conn)
  let entryCount = 0

  // 浏览面板：浮动、纯 DOM，由侧边栏按钮切换显示
  const browseHandle = buildPanel(channel, conn)
  browseHandle.root.id = 'dsh-offexplorer-browse-panel'
  browseHandle.setChrome('browse')
  browseHandle.root.style.display = 'none'
  doc.body.append(browseHandle.root)
  if (tryRegisterSidebarEntry(ctx, browseHandle)) entryCount += 1

  // 配置面板：设置页插槽专用实例（与浏览面板分离，避免 DOM 被搬走）
  const configHandle = buildPanel(channel, conn)
  if (tryRegisterSettingsSlot(ctx, configHandle)) entryCount += 1

  if (entryCount === 0) {
    const panel = browseHandle.root
    const launcher = el('button', {
      textContent: '🪵 Kafka',
      id: 'dsh-offexplorer-launcher',
      style: 'position:fixed;left:16px;bottom:16px;z-index:2147483647;padding:9px 14px;' +
        'font-size:13px;background:#1f6feb;color:#fff;border:none;border-radius:8px;' +
        'box-shadow:0 2px 8px rgba(0,0,0,.3);margin:0;cursor:pointer;',
    }) as HTMLButtonElement
    launcher.addEventListener('click', () => {
      if (panel.style.display === 'none') {
        panel.style.display = 'flex'
        browseHandle.refresh()
      } else {
        panel.style.display = 'none'
      }
    })
    doc.body.append(launcher)
  }
}

module.exports = { name: PLUGIN_ID, inject: ['connection', 'slots'], apply }
