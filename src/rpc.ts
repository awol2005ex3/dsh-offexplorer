/**
 * dsh-offexplorer 宿主半 RPC：把面板 / agent 的请求桥接到 Rust 原生模块。
 *
 * 通道契约（harness 0.1.7）：
 * - 插件 RPC = `connection.fetch.register()` 注册 `/api/offexplorer/<endpoint>` 精确路由；
 * - 请求 `{ type:'client-request', rpcId, method, payload }` →
 *   `HTTP 200` + `{ type:'server-response', rpcId, result }`；
 * - result 必须是 `{ ok:true, value }` 或 `{ ok:false, error:{ code, message, details } }`；
 * - 业务错误一律 HTTP 200 + 错误分支，只有信封畸形才回 400。
 */
import type { DshOffexplorerNative } from './native-types.js'
import type { KafkaCluster, OffexplorerSettings } from './settings.js'

/** 宿主 connection.fetch 的最小形状（见 harness connection/src/rpc.ts）。 */
export interface HostConnectionFetch {
  register(route: {
    path: string
    methods: readonly ('GET' | 'HEAD' | 'POST')[]
    requestBody: 'buffered' | 'streaming'
    fetch: (request: Request) => Promise<Response>
  }): () => Promise<void>
}

/** 载体中立的 RPC 结果（严格信封要求 code/details）。 */
export interface RpcResult {
  ok: boolean
  value?: unknown
  error?: { code: string; message: string; details: object }
}

/** 精确 Fetch 路由的 base（必须在 /api 下）。 */
export const RPC_BASE = '/api/offexplorer'
/** 端点前缀（rpc.call 的 method 字段）。 */
export const RPC_PREFIX = 'offexplorer/'
/** 暴露为精确路由的端点清单。 */
export const RPC_ENDPOINTS = [
  'version',
  'clusters',
  'kinit',
  'ping',
  'clusterInfo',
  'brokers',
  'topics',
  'topicDetail',
  'topicConfigs',
  'brokerConfigs',
  'groups',
  'groupDetail',
  'watermarks',
  'browse',
  'produce',
  'createTopic',
  'deleteTopic',
  'increasePartitions',
  'health',
] as const

export type RpcEndpoint = (typeof RPC_ENDPOINTS)[number]

interface ClientRequestEnvelope {
  type?: unknown
  rpcId?: unknown
  method?: unknown
  payload?: unknown
}

function failure(err: unknown): RpcResult {
  const message = err instanceof Error ? err.message : String(err)
  return { ok: false, error: { code: 'bad-request', message, details: {} } }
}

function ok(value: unknown): RpcResult {
  return { ok: true, value }
}

/** 宽松参数读取；最终校验在原生侧（Rust config.rs）。 */
function str(args: Record<string, unknown>, key: string, fallback = ''): string {
  const v = args[key]
  return typeof v === 'string' ? v : fallback
}

function num(args: Record<string, unknown>, key: string, fallback: number): number {
  const v = args[key]
  return typeof v === 'number' && Number.isFinite(v) ? v : fallback
}

function int(args: Record<string, unknown>, key: string, fallback: number): number {
  return Math.floor(num(args, key, fallback))
}

function strArray(args: Record<string, unknown>, key: string): string[] {
  const v = args[key]
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string') : []
}

function intArray(args: Record<string, unknown>, key: string): number[] {
  const v = args[key]
  return Array.isArray(v)
    ? v.filter((x): x is number => typeof x === 'number' && Number.isFinite(x)).map(Math.floor)
    : []
}

function required(value: string, label: string): string {
  const text = value.trim()
  if (!text) throw new Error(`${label} 不能为空`)
  return text
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value))
}

/**
 * 构造 RPC 分发器。
 *
 * @param native 原生模块；null（加载失败）时所有集群端点返回可读错误
 * @param settings 现读配置快照提供者（volatile 引用值随时在变）
 */
export function createRpcDispatcher(
  native: DshOffexplorerNative | null,
  settings: () => OffexplorerSettings,
): (endpoint: string, args: Record<string, unknown>) => Promise<RpcResult> {
  /** 按 id（或默认集群）解析集群；找不到时给出含候选清单的可读错误。 */
  function resolveCluster(args: Record<string, unknown>): KafkaCluster {
    const snap = settings()
    if (snap.clusters.length === 0) {
      throw new Error(
        snap.clusterError
          ? `未配置可用集群：${snap.clusterError}（设置 → 插件 → Offset Explorer）`
          : '未配置 Kafka 集群（设置 → 插件 → Offset Explorer，clusters 为 JSON 数组）',
      )
    }
    const wanted = str(args, 'cluster') || snap.defaultCluster
    const found = snap.clusters.find((c) => c.id === wanted)
    if (found) return found
    throw new Error(`集群 "${wanted}" 不存在。可用: ${snap.clusters.map((c) => c.id).join(', ')}`)
  }

  return async function dispatch(endpoint, args): Promise<RpcResult> {
    if (endpoint === 'version') {
      return ok({ native: native ? native.nativeVersion() : 'unavailable' })
    }
    if (endpoint === 'clusters') {
      const snap = settings()
      return ok({
        clusters: snap.clusters.map((c) => ({
          id: c.id,
          name: c.name,
          group: c.group,
          description: c.description,
          bootstrapServers: c.bootstrapServers,
          security: c.security,
          sasl: c.sasl,
        })),
        defaultCluster: snap.defaultCluster,
        clusterError: snap.clusterError,
        defaults: {
          maxRows: snap.maxRows,
          maxPerPartition: snap.maxPerPartition,
          maxValueBytes: snap.maxValueBytes,
        },
      })
    }

    // 先解析集群：即使原生模块缺席，未知集群也给出确定的「集群不存在」错误。
    let cluster: KafkaCluster
    try {
      cluster = resolveCluster(args)
    } catch (err) {
      return failure(err)
    }
    if (!native) {
      return failure(
        new Error('dsh-offexplorer 原生模块未加载（宿主启动日志应有警告）；请先 npm run build:native 并重启'),
      )
    }

    const clusterJson = JSON.stringify(cluster)

    try {
      switch (endpoint) {
        case 'kinit':
          return ok(await native.kinit(clusterJson))

        case 'ping':
          return ok({ cluster: cluster.id, ok: await native.ping(clusterJson) })

        case 'clusterInfo':
          return ok(await native.clusterInfo(clusterJson))

        case 'brokers':
          return ok({ cluster: cluster.id, brokers: await native.listBrokers(clusterJson) })

        case 'topics':
          return ok({ cluster: cluster.id, topics: await native.listTopics(clusterJson) })

        case 'topicDetail':
          return ok(await native.topicDetail(clusterJson, required(str(args, 'topic'), 'topic')))

        case 'topicConfigs':
          return ok({
            topic: str(args, 'topic'),
            configs: await native.topicConfigs(
              clusterJson,
              required(str(args, 'topic'), 'topic'),
            ),
          })

        case 'brokerConfigs': {
          const brokerId = int(args, 'brokerId', -1)
          if (brokerId < 0) throw new Error('brokerId 必须 >= 0')
          return ok({ brokerId, configs: await native.brokerConfigs(clusterJson, brokerId) })
        }

        case 'groups':
          return ok({ cluster: cluster.id, groups: await native.listGroups(clusterJson) })

        case 'groupDetail':
          return ok(await native.groupDetail(clusterJson, required(str(args, 'group'), 'group')))

        case 'watermarks': {
          const topic = required(str(args, 'topic'), 'topic')
          const partition = int(args, 'partition', -1)
          if (partition < 0) throw new Error('partition 必须 >= 0')
          return ok(await native.fetchWatermarks(clusterJson, topic, partition))
        }

        case 'browse': {
          const snap = settings()
          const topic = required(str(args, 'topic'), 'topic')
          const start = (str(args, 'start', 'NEWEST') || 'NEWEST').toUpperCase()
          const perPart = clamp(int(args, 'perPartition', snap.maxPerPartition), 1, 10_000)
          const limit = clamp(int(args, 'limit', snap.maxRows), 1, 100_000)
          const maxValueBytes = clamp(
            int(args, 'maxValueBytes', snap.maxValueBytes),
            64,
            1024 * 1024,
          )
          return ok(
            await native.browse(
              clusterJson,
              topic,
              intArray(args, 'partitions'),
              start,
              Math.max(0, int(args, 'offset', 0)),
              perPart,
              limit,
              maxValueBytes,
            ),
          )
        }

        case 'produce': {
          const topic = required(str(args, 'topic'), 'topic')
          const value = str(args, 'value')
          const key = str(args, 'key')
          const partition = int(args, 'partition', -1)
          return ok(await native.produce(clusterJson, topic, key, value, partition))
        }

        case 'createTopic': {
          const name = required(str(args, 'name'), 'name')
          const partitions = int(args, 'partitions', 1)
          const replication = int(args, 'replication', 1)
          if (partitions <= 0) throw new Error('partitions 必须 > 0')
          if (replication <= 0) throw new Error('replication 必须 > 0')
          return ok(await native.createTopic(clusterJson, name, partitions, replication))
        }

        case 'deleteTopic':
          return ok(await native.deleteTopic(clusterJson, required(str(args, 'name'), 'name')))

        case 'increasePartitions': {
          const name = required(str(args, 'name'), 'name')
          const newCount = int(args, 'newCount', 0)
          if (newCount <= 0) throw new Error('newCount 必须 > 0')
          return ok(await native.increasePartitions(clusterJson, name, newCount))
        }

        case 'health':
          return ok(await native.scanHealth(clusterJson))

        default:
          return failure(new Error(`unknown endpoint ${endpoint}`))
      }
    } catch (err) {
      return failure(err)
    }
  }
}

/**
 * 为每个端点注册精确 Fetch 路由，并回填 server-response 信封。
 *
 * @returns 解除全部路由的 disposer（幂等）
 */
export function registerRpcRoutes(
  connection: unknown,
  dispatch: (endpoint: string, args: Record<string, unknown>) => Promise<RpcResult>,
  log: (message: string) => void,
): () => void {
  const registry = (connection as { fetch?: HostConnectionFetch } | undefined)?.fetch
  if (!registry || typeof registry.register !== 'function') {
    log('connection.fetch registry unavailable; RPC routes disabled')
    return () => {}
  }

  const answer = (endpoint: string) => async (request: Request): Promise<Response> => {
    let body: ClientRequestEnvelope
    try {
      body = (await request.json()) as ClientRequestEnvelope
    } catch {
      return new Response('malformed rpc envelope', { status: 400 })
    }
    if (body?.type !== 'client-request' || typeof body.rpcId !== 'string') {
      return new Response('malformed rpc envelope', { status: 400 })
    }
    const rpcId = body.rpcId
    try {
      const payload = (body.payload ?? {}) as { args?: Record<string, unknown> }
      const args = payload.args ?? {}
      const result = await dispatch(endpoint, args)
      return new Response(JSON.stringify({ type: 'server-response', rpcId, result }), {
        headers: { 'content-type': 'application/json' },
      })
    } catch (err) {
      return new Response(
        `offexplorer rpc failed: ${err instanceof Error ? err.message : String(err)}`,
        { status: 500 },
      )
    }
  }

  const disposers: Array<() => unknown> = []
  for (const endpoint of RPC_ENDPOINTS) {
    try {
      const remove = registry.register({
        path: `${RPC_BASE}/${endpoint}`,
        methods: ['POST'],
        requestBody: 'buffered',
        fetch: answer(endpoint),
      })
      disposers.push(remove)
    } catch (err) {
      log(`failed to register route ${RPC_BASE}/${endpoint}: ${String(err)}`)
    }
  }

  return () => {
    for (const dispose of disposers) {
      try {
        void dispose()
      } catch {
        // 单个路由解除失败不阻断其余清理
      }
    }
  }
}
