/**
 * 原生模块加载烟测：验证 .node 二进制能被 Node 加载且函数面完整。
 * 不连接任何集群，只做本地断言。
 */
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const failures = []
const check = (label, cond) => {
  console.log(`${cond ? '✔' : '✘'} ${label}`)
  if (!cond) failures.push(label)
}

let native
try {
  native = require('../lib/native/index.cjs')
} catch (err) {
  console.error('✘ lib/native/index.cjs 加载失败:', err.message)
  console.error('  请先执行: npm run build:native')
  process.exit(1)
}

const EXPECTED = [
  'nativeVersion',
  'ping',
  'clusterInfo',
  'listBrokers',
  'listTopics',
  'topicDetail',
  'topicConfigs',
  'brokerConfigs',
  'listGroups',
  'groupDetail',
  'fetchWatermarks',
  'browse',
  'produce',
  'createTopic',
  'deleteTopic',
  'increasePartitions',
  'scanHealth',
  'kinit',
]

for (const fn of EXPECTED) {
  check(`导出 ${fn}`, typeof native[fn] === 'function')
}

const version = native.nativeVersion()
check('nativeVersion 返回字符串', typeof version === 'string')
console.log(`  ↳ 原生版本: ${version}`)

// 配置校验路径（不建立连接）：PascalCase 也要被规范化后报出可读错误。
let validation = ''
try {
  await native.ping(JSON.stringify({ BootstrapServers: [] }))
} catch (err) {
  validation = err instanceof Error ? err.message : String(err)
}
check('空 bootstrapServers → 校验错误（含 PascalCase 兼容）', /bootstrapServers/.test(validation))

if (failures.length > 0) {
  console.error(`\n${failures.length} 项检查失败`)
  process.exit(1)
}
console.log('\n全部检查通过')
