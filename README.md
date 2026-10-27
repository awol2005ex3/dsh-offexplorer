# dsh-offexplorer

DeepSeek Harness (dsh) 插件：把 [offexplorer_rs](../../workspace_rust/offexplorer_rs) 的 Kafka
浏览 / 管理能力移植进 dsh。Rust 半基于 `rdkafka`，经 napi-rs 暴露给 Node 宿主，
再以 `/api/offexplorer/*` 精确 Fetch 路由和一个 `kafka` agent 工具对外提供。

## 结构

```
src/
  index.ts         插件入口：name / inject / Config / apply
  settings.ts      Config schema + 集群 JSON 规范化（兼容 connections.json PascalCase）
  native.ts        原生模块加载（createRequire → lib/native/index.cjs）
  native-types.ts  原生函数面手写类型
  rpc.ts           RPC 分发器 + connection.fetch.register 路由
  tool.ts          kafka agent 工具
rust/
  src/config.rs    集群配置解析 / 校验 / kinit / GSSAPI 环境
  src/kafka.rs     rdkafka 封装（无状态，每次调用新建客户端）
  src/types.rs     napi 返回结构
  src/lib.rs       #[napi] 导出
  vendor/rdkafka-sys  Windows MIT Kerberos 补丁版（见下）
scripts/
  check-native.mjs 原生模块加载冒烟
  check-plugin.mjs 宿主半冒烟（假 cordis ctx，不连集群）
```

## 构建

```bash
npm install
npm run build:native   # cargo + napi，产物 lib/native/index.cjs 与 .node
npm run build          # tsc → lib/
npm run check          # check:native + check:plugin
```

`build:native` 需要 Rust 工具链、CMake（librdkafka 用 `cmake-build` 特性现场编译）。
Windows 上首次构建会编译 vendored 的 librdkafka，耗时较长。

## 配置

插件配置（设置 → 插件 → Offset Explorer）：

```json
{
  "clusters": "[{\"id\":\"prod\",\"name\":\"生产\",\"bootstrapServers\":[\"broker1:9092\"],\"security\":\"SASL_PLAINTEXT\",\"sasl\":\"GSSAPI\",\"krb\":{\"principal\":\"user@REALM\",\"keytabPath\":\"C:/keys/user.keytab\",\"autoKinit\":true}}]",
  "defaultCluster": "prod",
  "maxRows": 100,
  "maxPerPartition": 50,
  "maxValueBytes": 4096
}
```

- `clusters` 是 JSON 字符串（schemastery 无数组对象编辑能力），元素兼容
  Offset Explorer `connections.json` 的 PascalCase 字段（`BootstrapServers` /
  `SASL` / `TLS` / `CCachePath` / `CAPemPath` …）。
- `security`：`PLAINTEXT` | `SSL` | `SASL_PLAINTEXT` | `SASL_SSL`；`sasl`：
  `GSSAPI` | `PLAIN` | `SCRAM-SHA-256` | `SCRAM-SHA-512`。
- keytab / 密码等敏感信息只放在 dsh 配置里，不要提交进仓库。

## RPC 端点

全部为 `POST /api/offexplorer/<endpoint>`，信封
`{type:'client-request',rpcId,method,payload:{args}}` →
`{type:'server-response',rpcId,result}`，`result` 为
`{ok:true,value}` 或 `{ok:false,error:{code,message,details}}`。

| endpoint | 说明 |
| --- | --- |
| `version` / `clusters` | 原生版本 / 集群清单（无需集群连接） |
| `kinit` | 按集群配置执行一次 Kerberos 取票 |
| `ping` / `clusterInfo` / `brokers` | 连通性、cluster id + controller、broker 列表 |
| `topics` / `topicDetail` / `topicConfigs` / `brokerConfigs` | topic 列表 / 分区明细 / 动态配置 |
| `groups` / `groupDetail` | 消费者组列表 / 成员与分配 |
| `watermarks` | 单分区 earliest / latest |
| `browse` | 拉取消息（`start`: OLDEST / NEWEST / SPECIFIC / TIMESTAMP） |
| `produce` / `createTopic` / `deleteTopic` / `increasePartitions` | 写入与管理 |
| `health` | 离线分区 / 副本不足 / 离线副本巡检 |

## Agent 工具

宿主半注册一个名为 `kafka` 的 agent 工具，参数为 `{ endpoint, args, cluster? }`，
直接复用上面的 RPC 分发器（与面板同一条校验路径）。省略 `cluster` 时用默认集群。
`endpoint` 在 schema 里是枚举，传错会被参数校验直接拒绝。

```
列出已配置的 Kafka 集群          → {endpoint:"clusters"}
orders 有哪些分区                → {endpoint:"topicDetail",args:{topic:"orders"}}
看看 orders 最新 20 条消息       → {endpoint:"browse",args:{topic:"orders",start:"NEWEST",limit:20}}
检查集群健康                     → {endpoint:"health"}
往 orders 发一条 hello           → {endpoint:"produce",args:{topic:"orders",value:"hello"}}
```

`produce` / `createTopic` / `deleteTopic` / `increasePartitions` 会改动集群，
工具描述里要求模型先与用户确认。GSSAPI 集群在 `autoKinit` 打开时自动 kinit。

## Windows Kerberos 说明

`rust/vendor/rdkafka-sys` 是打过补丁的 rdkafka-sys 4.10（librdkafka 2.12.1）：
新增 `rdkafka_sasl_mit.c`，当环境变量 `RDKAFKA_SASL_MIT=1` 时使用 MIT Kerberos
（gssapi64.dll）而不是 SSPI，避免 SSPI 把 SPN 大写导致认证失败。

宿主进程会设置：

- `RDKAFKA_SASL_MIT=1`（Windows GSSAPI）；
- `KRB5_CONFIG`（若配置了 `krb.krb5ConfPath`）；
- `KRB5CCNAME=FILE:<ccache>`（`autoKinit` 或 `kinit` 端点成功后）。

`autoKinit` 使用 `kinit -c FILE:<ccache> -k -t <keytab> <principal>`。
默认 ccache 为 `%TEMP%\dsh_offexplorer_krb5cc_<principal>`。

## 安全 / 开发约定

- 冒烟脚本只做本地断言，**不连接任何集群**；联调请使用本地或测试集群。
- TLS / SCRAM 相关配置项已归一化传递，但当前构建未启用 rdkafka 的 `ssl` /
  `sasl` 扩展特性（与 offexplorer_rs 的构建面保持一致），连接时若服务端强制
  TLS 会得到明确的客户端错误。
- 原生模块加载失败不会让 `apply` 抛错：`version` / `clusters` 仍可用，其余端点
  返回可读错误。
