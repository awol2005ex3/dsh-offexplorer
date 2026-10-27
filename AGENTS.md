# AGENTS.md — dsh-offexplorer

dsh 插件，移植 `D:\workspace_rust\offexplorer_rs` 的 Kafka 能力。
参考实现：`D:\workspace_node\dsh-hdfs`（同样的 rust napi + 宿主 RPC 分层）。
dsh 宿主源码：`D:\workspace_node\deepseek-harness`。

## 分层与硬约束

- **Rust 原生半**（`rust/`）：所有 Kafka I/O。`rdkafka` 无状态使用——每次调用新建
  client；阻塞调用必须 `spawn_blocking`。集群配置以 JSON 字符串跨 napi 边界，
  `config.rs` 先规范化键名（兼容 PascalCase）再校验。
- **TS 宿主半**（`src/`）：`connection.fetch.register()` 精确路由
  `/api/offexplorer/*`；`rpc.handle` 对 out-of-tree 插件不可用。信封与
  `{ok,value}|{ok,error:{code,message,details}}` 契约不可省略 code/details。
- **`.node` 只允许宿主加载**：`createRequire(import.meta.url)` →
  `lib/native/index.cjs`。浏览器半（若有）只能走 RPC，绝不能 import 原生模块。
- **配置每次现读**：`settings.ts` 的 volatile 字段随时被设置页改写；不要在
  `apply` 时缓存快照。patch 按 id 整体替换 config，不深合并。
- **原生模块缺席不影响 apply**：`tryNative()` 失败只打警告，端点返回可读错误。

## 构建 / 验证

```bash
npm run build:native   # rust/ → lib/native/index.cjs + .node
npm run build          # tsc → lib/
npm run check          # check:native + check:plugin（均不连集群）
```

`napi build` 必须在 `rust/` 目录下执行，且带 `--js index.cjs --output-dir ../lib/native`。
Windows 上 `.node` 会被运行中的宿主锁住，重新构建前先停宿主。

## 约定

- 端点清单只维护在 `src/rpc.ts` 的 `RPC_ENDPOINTS`；工具描述与冒烟脚本从这里取。
- 新增原生函数：`types.rs` 结构 → `kafka.rs` 实现 → `lib.rs` `#[napi]` →
  `src/native-types.ts` → `src/rpc.ts` →（必要时）`src/tool.ts` 描述。
- 冒烟脚本用假数据 / 假 ctx；**永远不要从本机连接生产集群**。
- keytab、密码、ccache 不入库；本地集群配置用 `*.local.json`（已 gitignore）。
- 不要用 sed/awk/perl 重写源文件；用 Edit / Write。
