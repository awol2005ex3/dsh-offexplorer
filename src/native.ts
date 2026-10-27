/**
 * 原生模块加载器（仅宿主半使用）。
 *
 * napi 构建产物在 `lib/native/`：
 *   - `index.cjs`：CJS 绑定（根 package.json 是 "type": "module"，所以必须是 .cjs）；
 *   - `dsh-offexplorer-native.<triple>.node`：真正的原生二进制。
 *
 * 用 `createRequire(import.meta.url)` 相对编译产物定位：运行时本文件位于
 * `lib/native.js`，所以 `./native/index.cjs` 即 `lib/native/index.cjs`。
 */
import { createRequire } from 'node:module'
import type { DshOffexplorerNative } from './native-types.js'

let cached: DshOffexplorerNative | null = null
let cachedError: Error | null = null

/** 惰性加载原生模块；进程内只加载一次，失败信息保持一致。 */
export function loadNative(): DshOffexplorerNative {
  if (cached) return cached
  if (cachedError) throw cachedError
  try {
    const require = createRequire(import.meta.url)
    cached = require('./native/index.cjs') as DshOffexplorerNative
    return cached
  } catch (err) {
    cachedError = new Error(
      `dsh-offexplorer 原生模块加载失败（请先执行 npm run build:native）: ${
        err instanceof Error ? err.message : String(err)
      }`,
    )
    throw cachedError
  }
}

/** 原生模块是否可用（不抛错版本，供启动日志与 version 端点使用）。 */
export function tryNative(): DshOffexplorerNative | undefined {
  try {
    return loadNative()
  } catch {
    return undefined
  }
}
