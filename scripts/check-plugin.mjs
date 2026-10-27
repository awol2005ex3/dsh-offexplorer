/**
 * 宿主半冒烟检查：模拟 cordis 环境加载 lib/index.js，
 * 验证四大导出、路由注册、信封回填、集群解析与工具注册。
 *
 * 不连接 Kafka：只调用无需集群的端点，并用未知集群验证错误路径。
 */
import { pathToFileURL } from 'node:url'

const pluginUrl = new URL('../lib/index.js', import.meta.url)

const failures = []
const check = (label, cond) => {
  console.log(`${cond ? '✔' : '✘'} ${label}`)
  if (!cond) failures.push(label)
}

// ── 加载插件主入口 ──
let plugin
try {
  plugin = await import(pluginUrl.href)
} catch (err) {
  console.error('✘ lib/index.js 加载失败:', err)
  process.exit(1)
}

check('name 导出 = dsh-offexplorer', plugin.name === 'dsh-offexplorer')
check(
  'inject 含 connection + tools',
  Array.isArray(plugin.inject) &&
    plugin.inject.includes('connection') &&
    plugin.inject.includes('tools'),
)
check(
  'Config schema 存在（schemastery schema 为函数形态）',
  typeof plugin.Config === 'object' || typeof plugin.Config === 'function',
)
check('apply 是函数', typeof plugin.apply === 'function')

// ── 模拟宿主环境 ──
const registeredRoutes = []
const registeredTools = []
const disposers = []

const fakeConnection = {
  fetch: {
    register(route) {
      registeredRoutes.push(route)
      const remove = async () => {
        const i = registeredRoutes.indexOf(route)
        if (i >= 0) registeredRoutes.splice(i, 1)
      }
      disposers.push(remove)
      return remove
    },
  },
}

const fakeTools = {
  register(tool) {
    registeredTools.push(tool)
    return () => {
      const i = registeredTools.indexOf(tool)
      if (i >= 0) registeredTools.splice(i, 1)
    }
  },
}

const fakeCtx = {
  // cordis 的 logger 是可调用访问器：ctx.logger('name').info(...)
  logger: (name) => ({
    info: () => {},
    warn: (msg, ...args) => console.log(`  [warn:${name}]`, msg, ...args),
    error: (msg, ...args) => console.log(`  [error:${name}]`, msg, ...args),
  }),
  get: (name) =>
    name === 'connection' ? fakeConnection : name === 'tools' ? fakeTools : undefined,
  effect: (fn, label) => {
    const dispose = fn()
    disposers.push(() => dispose?.())
    return label
  },
}

// 两个集群：一个 PascalCase（Offset Explorer connections.json 形态），一个 camelCase。
const clusters = [
  {
    Id: 'test1',
    Name: 'Test 1',
    BootstrapServers: '127.0.0.1:9092',
    Security: 'PLAINTEXT',
    SASL: 'GSSAPI',
  },
  {
    id: 'bad',
    name: 'Bad',
    bootstrapServers: ['127.0.0.1:1'],
    security: 'PLAINTEXT',
  },
]
const config = {
  clusters: { get: () => JSON.stringify(clusters) },
  defaultCluster: { get: () => 'test1' },
  maxRows: { get: () => 100 },
  maxPerPartition: { get: () => 50 },
  maxValueBytes: { get: () => 4096 },
}

// ── apply ──
try {
  plugin.apply(fakeCtx, config)
} catch (err) {
  console.error('✘ apply 抛错:', err)
  process.exit(1)
}

check('注册了 19 条 /api/offexplorer/* 路由', registeredRoutes.length === 19)
check(
  '路由全部在 /api/offexplorer/ 下',
  registeredRoutes.every((r) => r.path.startsWith('/api/offexplorer/')),
)
check(
  '路由全部 POST + buffered',
  registeredRoutes.every(
    (r) => r.methods.length === 1 && r.methods[0] === 'POST' && r.requestBody === 'buffered',
  ),
)
check('注册了 1 个 kafka 工具', registeredTools.length === 1 && registeredTools[0].name === 'kafka')
check(
  'kafka 工具的 endpoint 是枚举（与路由清单一致）',
  Array.isArray(registeredTools[0]?.parameters?.properties?.endpoint?.enum) &&
    registeredTools[0].parameters.properties.endpoint.enum.length === registeredRoutes.length,
)

// ── 通过 Fetch 路由调用 RPC（绕过 HTTP，直接调 fetch handler） ──
const route = (path) => registeredRoutes.find((r) => r.path === path)
const callRoute = async (path, args) => {
  const r = route(path)
  if (!r) throw new Error(`route ${path} not found`)
  const request = new Request(`http://localhost${path}`, {
    method: 'POST',
    body: JSON.stringify({
      type: 'client-request',
      rpcId: 'test-1',
      method: 'x',
      payload: { args },
    }),
  })
  const response = await r.fetch(request)
  if (response.status !== 200) {
    throw new Error(`HTTP ${response.status}: ${await response.text()}`)
  }
  const body = await response.json()
  if (body.type !== 'server-response' || body.rpcId !== 'test-1') {
    throw new Error(`bad envelope: ${JSON.stringify(body)}`)
  }
  return body.result
}

// version / clusters（无需真实集群）
const version = await callRoute('/api/offexplorer/version', {})
check('version 返回原生版本或 unavailable', version.ok && typeof version.value?.native === 'string')

const clusterList = await callRoute('/api/offexplorer/clusters', {})
check('clusters 返回 2 个集群', clusterList.ok && clusterList.value?.clusters?.length === 2)
check('clusters.defaultCluster = test1', clusterList.value?.defaultCluster === 'test1')
check(
  'PascalCase 集群 → 规范化 bootstrapServers',
  clusterList.value?.clusters?.[0]?.id === 'test1' &&
    Array.isArray(clusterList.value?.clusters?.[0]?.bootstrapServers) &&
    clusterList.value.clusters[0].bootstrapServers[0] === '127.0.0.1:9092',
)

// 错误路径：未知集群（在原生模块检查之前解析，错误确定）
const badCluster = await callRoute('/api/offexplorer/topics', { cluster: 'nope' })
check(
  '未知集群 → ok:false + code/message/details',
  !badCluster.ok &&
    badCluster.error?.code &&
    badCluster.error?.message &&
    'details' in badCluster.error &&
    /集群 "nope" 不存在/.test(badCluster.error.message),
)

// 错误路径：不可达集群（127.0.0.1:1，仅本机连接被拒，不触达任何真实集群）
const unreachable = await callRoute('/api/offexplorer/ping', { cluster: 'bad' })
check(
  '不可达集群 → ok:false + 可读错误',
  !unreachable.ok && typeof unreachable.error?.message === 'string',
)

// 信封畸形：HTTP 400
const malformed = route('/api/offexplorer/version')
const badResp = await malformed.fetch(
  new Request('http://localhost/api/offexplorer/version', {
    method: 'POST',
    body: 'not-json',
  }),
)
check('畸形信封 → HTTP 400', badResp.status === 400)

// 工具 execute 烟测（clusters 不需要集群连接）
const tool = registeredTools[0]
const exec = { signal: undefined }
const toolOk = await tool.execute({ endpoint: 'clusters', args: {} }, exec)
check('工具 clusters 调用成功', toolOk.ok === true && toolOk.result.includes('test1'))

const toolBad = await tool.execute({ endpoint: 'topics', args: { cluster: 'nope' } }, exec)
check('工具错误 → ok:false + 错误消息', toolBad.ok === false && /集群 "nope" 不存在/.test(toolBad.result))

let unknownRejected = false
try {
  await tool.execute({ endpoint: 'nope', args: {} }, exec)
} catch {
  unknownRejected = true
}
check('工具拒绝未知 endpoint', unknownRejected)

// ── disposer 幂等 ──
for (const d of disposers) await d()
check('disposer 解除后路由清空', registeredRoutes.length === 0)

// ── 结论 ──
if (failures.length > 0) {
  console.error(`\n${failures.length} 项检查失败`)
  process.exit(1)
}
console.log('\n全部检查通过')
