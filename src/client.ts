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

export interface SettingsSnapshot {
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
    read: async () => {
      const snap = await waitReady()
      const value = snap.value ?? {}
      let clusters = value.clusters
      if (Array.isArray(clusters)) clusters = JSON.stringify(clusters, null, 2)
      if (typeof clusters !== 'string') clusters = '[]'
      return { clusters, defaultCluster: String(value.defaultCluster ?? '') }
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
  return { read: () => required().read(), write: (patch) => required().write(patch) }
}

/* ── 样式 ─────────────────────────────────────────────────────────────── */

const PANEL_CSS =
  'color:var(--fg,#1f2328);font-size:13px;line-height:1.5;display:flex;flex-direction:column;gap:8px;'
const LABEL_CSS = 'display:block;font-weight:600;margin-top:4px;'
const HINT_CSS = 'display:block;color:var(--fg-muted,#57606a);font-size:12px;margin:2px 0;'
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

  /* —— 主题 —— */
  const topicsView = el('div', { style: 'display:flex;flex-direction:column;gap:8px;' })
  const topicFilter = el('input', {
    type: 'search',
    placeholder: '过滤主题（支持子串）',
    style: INPUT_CSS + 'width:240px;',
  }) as HTMLInputElement
  const topicsWrap = el('div', { style: 'overflow:auto;max-height:260px;' })
  const topicsTable = el('table', { style: TABLE_CSS })
  topicsWrap.append(topicsTable)

  const topicDetailBox = el('div', {
    style: 'display:none;border-top:1px solid rgba(127,127,127,.25);padding-top:8px;' +
      'flex-direction:column;gap:8px;',
  })
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

  topicDetailBox.append(
    el('div', { style: 'display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;' }, [topicTitle, topicMeta]),
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

  topicsView.append(
    el('div', { style: 'display:flex;gap:8px;align-items:center;flex-wrap:wrap;' }, [topicFilter]),
    topicsWrap,
    topicDetailBox,
  )

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

  /* —— 配置区 —— */
  const clustersInput = el('textarea', {
    rows: '8',
    spellcheck: 'false',
    style: INPUT_CSS + 'width:100%;font-family:ui-monospace,monospace;',
  }) as HTMLTextAreaElement
  const defaultClusterInput = el('input', { type: 'text', style: INPUT_CSS + 'width:200px;', placeholder: '留空取第一个' }) as HTMLInputElement
  const saveBtn = el('button', { type: 'button', textContent: '保存集群配置', style: BTN_PRIMARY_CSS }) as HTMLButtonElement
  const validateBtn = el('button', { type: 'button', textContent: '校验 JSON', style: BTN_CSS }) as HTMLButtonElement
  const configPreviewWrap = el('div', { style: 'overflow:auto;max-height:200px;' })
  const configPreviewTable = el('table', { style: TABLE_CSS })
  configPreviewWrap.append(configPreviewTable)
  const configMessage = el('div', { style: 'font-size:12px;color:var(--fg-muted,#57606a);' })

  const configZone = el('div', {
    style: 'border-top:1px solid rgba(127,127,127,.25);margin-top:8px;padding-top:8px;display:flex;flex-direction:column;gap:4px;',
  }, [
    el('span', { style: LABEL_CSS, textContent: 'Kafka 集群配置（JSON 数组）' }),
    el('span', {
      style: HINT_CSS,
      textContent:
        '[{"id":"local","name":"本地","bootstrapServers":["127.0.0.1:9092"],"security":"PLAINTEXT",' +
        '"sasl":"GSSAPI","krb":{"principal":"user@REALM","keytabPath":"C:/keys/user.keytab","autoKinit":true}}]；' +
        '也兼容 Offset Explorer connections.json 的 PascalCase 字段',
    }),
    clustersInput,
    el('span', { style: LABEL_CSS, textContent: '默认集群 id' }),
    el('span', { style: HINT_CSS, textContent: '面板与 agent 工具省略 cluster 参数时使用；留空取第一个' }),
    defaultClusterInput,
    el('div', { style: 'display:flex;gap:8px;align-items:center;margin-top:6px;' }, [saveBtn, validateBtn]),
    configMessage,
    el('span', { style: LABEL_CSS, textContent: '解析预览' }),
    configPreviewWrap,
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

  async function openTopic(name: string): Promise<void> {
    selectedTopic = name
    topicDetailBox.style.display = 'flex'
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
    if (key === 'topics') await loadTopics()
    else if (key === 'groups') await loadGroups()
    else if (key === 'brokers') await loadBrokers()
    else if (key === 'health') await loadHealth()
  }

  /* 配置 */
  function parseClusterRows(raw: string): Array<Record<string, unknown>> {
    const parsed = JSON.parse(raw)
    if (!Array.isArray(parsed)) throw new Error('必须是 JSON 数组')
    return parsed as Array<Record<string, unknown>>
  }

  function renderConfigPreview(rows: Array<Record<string, unknown>>): void {
    const table: TableRow[] = rows.map((row) => {
      const servers = Array.isArray(row.bootstrapServers)
        ? (row.bootstrapServers as unknown[]).join(',')
        : typeof row.BootstrapServers === 'string'
          ? row.BootstrapServers
          : ''
      return {
        cells: [
          String(row.id ?? row.Id ?? row.name ?? row.Name ?? '(未命名)'),
          servers || '(缺少 bootstrapServers)',
          String(row.security ?? row.Security ?? 'PLAINTEXT'),
          String(row.sasl ?? row.SASL ?? 'GSSAPI'),
        ],
      }
    })
    renderTable(configPreviewTable, ['id', 'bootstrap servers', 'security', 'sasl'], table)
  }

  async function refreshConfig(): Promise<void> {
    try {
      const value = await channel.read()
      if (typeof value.clusters === 'string') clustersInput.value = value.clusters
      defaultClusterInput.value = value.defaultCluster ?? ''
      try {
        renderConfigPreview(parseClusterRows(clustersInput.value || '[]'))
      } catch {
        renderTable(configPreviewTable, ['id', 'bootstrap servers', 'security', 'sasl'], [])
      }
      configMessage.textContent = ''
      configMessage.setAttribute('style', 'font-size:12px;color:var(--fg-muted,#57606a);')
    } catch (err) {
      clustersInput.value = clustersInput.value || '[]'
      configMessage.textContent = `读取配置失败：${errText(err)}`
      configMessage.setAttribute('style', 'font-size:12px;' + STATUS_ERR)
    }
  }

  function validateConfig(): Array<Record<string, unknown>> {
    const rows = parseClusterRows(clustersInput.value.trim() || '[]')
    rows.forEach((row, index) => {
      const servers = row.bootstrapServers ?? row.BootstrapServers
      const ok =
        (Array.isArray(servers) && servers.length > 0) ||
        (typeof servers === 'string' && servers.trim().length > 0)
      if (!ok) throw new Error(`第 ${index + 1} 项缺少 bootstrapServers`)
      const security = String(row.security ?? row.Security ?? 'PLAINTEXT').toUpperCase()
      if (!['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'].includes(security)) {
        throw new Error(`第 ${index + 1} 项 security 无效：${security}`)
      }
    })
    return rows
  }

  async function saveConfig(): Promise<void> {
    let rows: Array<Record<string, unknown>>
    try {
      rows = validateConfig()
    } catch (err) {
      configMessage.textContent = `clusters JSON 无效：${errText(err)}`
      configMessage.setAttribute('style', 'font-size:12px;' + STATUS_ERR)
      return
    }
    const raw = clustersInput.value.trim() || '[]'
    try {
      await channel.write({ clusters: raw, defaultCluster: defaultClusterInput.value.trim() })
      configMessage.textContent = '✔ 配置已保存（volatile 热更新，无需重启）'
      configMessage.setAttribute('style', 'font-size:12px;' + STATUS_OK)
      renderConfigPreview(rows)
      await loadClusters()
      if (activeTab === 'topics') await loadTopics()
    } catch (err) {
      configMessage.textContent = `保存失败：${errText(err)}`
      configMessage.setAttribute('style', 'font-size:12px;' + STATUS_ERR)
    }
  }

  /* 事件 */
  startSelect.addEventListener('change', () => {
    offsetInput.style.display = startSelect.value === 'SPECIFIC' ? '' : 'none'
    timestampInput.style.display = startSelect.value === 'TIMESTAMP' ? '' : 'none'
  })
  topicFilter.addEventListener('input', () => renderTopics())
  browseBtn.addEventListener('click', () => void browseMessages())
  produceBtn.addEventListener('click', () => void produceMessage())
  pingBtn.addEventListener('click', () => void ping())
  kinitBtn.addEventListener('click', () => void kinit())
  refreshBtn.addEventListener('click', () => {
    if (chromeMode === 'config') void refreshConfig()
    else if (activeTab === 'topics') void loadTopics()
    else void switchTab(activeTab)
  })
  validateBtn.addEventListener('click', () => {
    try {
      renderConfigPreview(validateConfig())
      configMessage.textContent = '✔ JSON 格式正确'
      configMessage.setAttribute('style', 'font-size:12px;' + STATUS_OK)
    } catch (err) {
      configMessage.textContent = `✘ ${errText(err)}`
      configMessage.setAttribute('style', 'font-size:12px;' + STATUS_ERR)
    }
  })
  saveBtn.addEventListener('click', () => void saveConfig())
  clusterSelect.addEventListener('change', () => {
    selectedTopic = ''
    topicDetailBox.style.display = 'none'
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
