/**
 * dsh-offexplorer 的 agent 工具。
 *
 * 与 dsh-hdfs 相同的取舍：不暴露十几个细粒度工具，而是「一个 kafka 代理工具 +
 * 结构化 endpoint 参数」，执行体复用 RPC 分发器，与面板走完全相同的校验路径。
 */
import { defineTool } from '@deepseek-ai/dsh-tools'
import { RPC_ENDPOINTS } from './rpc.js'

export type RpcDispatch = (endpoint: string, args: Record<string, unknown>) => Promise<unknown>

/** 输出 JSON 截断上限（工具结果过大会挤爆上下文）。 */
const OUTPUT_LIMIT = 256 * 1024

export function createKafkaTool(dispatch: RpcDispatch) {
  return defineTool({
    name: 'kafka',
    description: [
      'Operate Kafka clusters configured in dsh-offexplorer (设置 → 插件 → Offset Explorer).',
      'Call {endpoint:"clusters"} first to list configured cluster ids and the default.',
      'Pass `endpoint` plus `args`; omit `cluster` to use the default cluster.',
      'Required args — clusters/version/ping/clusterInfo/brokers/topics/groups/health/kinit: none;',
      'topicDetail/topicConfigs: {topic}; watermarks: {topic,partition};',
      'browse: {topic, partitions?, start?: OLDEST|NEWEST|SPECIFIC|TIMESTAMP, offset?, limit?, perPartition?, maxValueBytes?};',
      'brokerConfigs: {brokerId}; groupDetail: {group}; produce: {topic,value, key?, partition?};',
      'createTopic: {name,partitions,replication}; deleteTopic: {name}; increasePartitions: {name,newCount}.',
      'Examples: {endpoint:"topics",args:{}} · {endpoint:"browse",args:{topic:"orders",start:"NEWEST",limit:20}} ·',
      '{endpoint:"produce",args:{topic:"orders",value:"hello"}}.',
      'produce/createTopic/deleteTopic/increasePartitions mutate the cluster — confirm with the user first.',
      'GSSAPI clusters run kinit automatically when autoKinit is set.',
    ].join(' '),
    parameters: {
      endpoint: {
        type: 'string',
        required: true,
        enum: [...RPC_ENDPOINTS],
        description: 'RPC endpoint to call.',
      },
      args: {
        type: 'object',
        additionalProperties: true,
        description:
          'Endpoint arguments. See required-args list in the tool description ' +
          '(topic, group, brokerId, partition, partitions, start, offset, limit, perPartition, ' +
          'maxValueBytes, name, newCount, partitions, replication, value, key).',
      },
      cluster: {
        type: 'string',
        description: 'Cluster id from endpoint "clusters". Defaults to the plugin default cluster.',
      },
    },
    output: {
      schema: {
        type: 'object',
        additionalProperties: false,
        properties: {
          endpoint: { type: 'string' },
          ok: { type: 'boolean' },
          result: { type: 'string', description: 'JSON result (truncated to 256KB)' },
        },
      },
      render: (_args, value) => [
        {
          type: 'text',
          text: value.ok
            ? `kafka ${value.endpoint} → ${value.result}`
            : `kafka ${value.endpoint} failed → ${value.result}`,
        },
      ],
    },
    async execute(args, exec) {
      if (exec.signal?.aborted) throw new Error('Cancelled')
      const endpoint = String(args.endpoint ?? '')
      const rpcArgs: Record<string, unknown> =
        typeof args.args === 'object' && args.args !== null
          ? { ...(args.args as Record<string, unknown>) }
          : {}
      if (typeof args.cluster === 'string' && args.cluster) rpcArgs.cluster = args.cluster

      try {
        const value = await dispatch(endpoint, rpcArgs)
        let json = JSON.stringify(value, null, 2)
        if (json.length > OUTPUT_LIMIT) {
          json = `${json.slice(0, OUTPUT_LIMIT)}\n…[truncated ${json.length - OUTPUT_LIMIT} chars]`
        }
        return { endpoint, ok: true, result: json }
      } catch (err) {
        // 错误也走结构化返回（ok:false），模型能读到完整错误消息并自行纠正参数。
        return {
          endpoint,
          ok: false,
          result: err instanceof Error ? err.message : String(err),
        }
      }
    },
  })
}
