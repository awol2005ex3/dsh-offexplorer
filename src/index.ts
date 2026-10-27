// @dsh-version 0.1.7-rc.2
/**
 * dsh-offexplorer · 宿主半（Node / Cordis 插件）。
 *
 * 职责：
 * 1. 加载 rust/ 构建出的 napi 原生模块（lib/native/index.cjs → .node）；
 * 2. 经 `connection.fetch.register()` 注册 `/api/offexplorer/<endpoint>` 精确路由；
 * 3. 注册 `kafka` agent 工具（复用 RPC 分发器，面板与工具走同一条校验路径）。
 *
 * .node 模块永远不能进浏览器；所有 Kafka 能力都通过宿主 RPC 暴露。
 */
import type { Context } from '@deepseek-ai/cordis'

import {
  OffexplorerConfigSchema,
  readSettings,
  type OffexplorerConfig,
} from './settings.js'
import { tryNative } from './native.js'
import { createRpcDispatcher, registerRpcRoutes } from './rpc.js'
import { createKafkaTool } from './tool.js'

/** Plugin id — must be unique within the composed plugin tree. */
export const name = 'dsh-offexplorer'

/** Declare the host services this plugin needs; the loader waits for them. */
export const inject = ['connection', 'tools'] as const

/**
 * 插件配置 schema（dsh ≥ 0.1.7 的配置载体）。
 * volatile 字段改动不重挂插件；本插件所有请求都现读配置，无需监听也能生效。
 */
export const Config = OffexplorerConfigSchema

/**
 * 配置命名空间 = profile 条目 id（本插件的 bundle patch 用 `id: offexplorer`）。
 */
function settingsNamespace(ctx: Context): string {
  const fiber = (ctx as unknown as { fiber?: { entry?: { options?: { id?: string } } } }).fiber
  return fiber?.entry?.options?.id ?? 'offexplorer'
}

export function apply(ctx: Context, config: OffexplorerConfig): void {
  const logger = ctx.logger('offexplorer')

  // ── 原生模块 ──────────────────────────────────────────────────────────────
  // 加载失败不炸插件：RPC 与工具返回可读错误，其余功能照常工作。
  const native = tryNative()
  if (native) {
    logger.info('dsh-offexplorer: native module loaded, version %s', native.nativeVersion())
  } else {
    logger.warn(
      'dsh-offexplorer: native module unavailable; run `npm run build:native`. All cluster operations will fail.',
    )
  }

  // volatile 配置现读快照器：每次请求现取，引用不变、值在变。
  const settings = () => readSettings(config)

  // ── RPC 分发器（面板 + agent 工具共用） ──────────────────────────────────
  const dispatch = createRpcDispatcher(native ?? null, settings)

  // ── 注册 Fetch 精确路由（ctx.effect 保证 HMR/卸载可逆） ───────────────────
  const connection = ctx.get('connection')
  ctx.effect(() => {
    const dispose = registerRpcRoutes(connection, dispatch, (m) =>
      logger.warn('dsh-offexplorer: %s', m),
    )
    logger.info('dsh-offexplorer: /api/offexplorer/* routes registered (ns=%s)', settingsNamespace(ctx))
    return dispose
  }, 'offexplorer: rpc routes')

  // ── 注册 agent 工具 ──────────────────────────────────────────────────────
  ctx.effect(() => {
    // tools 服务由 loader 提供；ctx.get 兼容测试替身，属性访问只适用于真实 cordis。
    const tools =
      (ctx as unknown as { get?: (n: string) => unknown }).get?.('tools') ??
      (ctx as unknown as { tools?: { register: unknown } }).tools
    const registry = tools as { register: (t: unknown) => unknown } | undefined
    const tool = createKafkaTool(async (endpoint, args) => {
      const result = await dispatch(endpoint, args)
      if (result.ok) return result.value
      throw new Error(result.error?.message ?? 'offexplorer rpc failed')
    })
    if (!registry || typeof registry.register !== 'function') {
      logger.warn('dsh-offexplorer: tools registry unavailable; agent tool not registered')
      return () => {}
    }
    logger.info('dsh-offexplorer: kafka agent tool registered')
    const dispose = registry.register(tool) as () => void
    return () => {
      dispose()
    }
  }, 'offexplorer: agent tool')
}
