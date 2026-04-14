# Pure Ray Timeline Benchmark

这个脚本是一个**不依赖 TransferQueue** 的双机 Ray benchmark。

它测的是这条纯 Ray 链路：

1. 机器 A 上的 `WriterActor` 构造 payload
2. 机器 A 上调用 `ray.put(...)`
3. 机器 B 上的 `ReaderActor` 通过 `ray.get(object_ref)` 拉取 payload
4. 脚本导出 Ray object transfer trace
5. 脚本额外导出一份 CSV summary，方便按 sweep 看表

默认情况下：

- 总 payload 会被切成 `8` 个等大小 chunk
- Writer 会对这 `8` 个 chunk 分别做 `ray.put(...)`
- Reader 会对这 `8` 个 `ObjectRef` 一次性 `ray.get(...)`

脚本文件：

- [pure_ray_timeline_benchmark.py](/Users/humphrey/Documents/github/tq_test/TransferQueue/scripts/pure_ray_timeline_benchmark.py:1)

## 重要说明

你已经说明了：

- 机器是 NPU 机器
- 但是这次 benchmark **不需要走 NPU tensor 路径**
- 不管是 Ray 还是 TQ，都按 **CPU 路径** 来测

所以这个脚本现在的默认模式是：

```bash
--payload-kind cpu-torch
```

也就是：

- 用 CPU 上的 `torch.Tensor` 构造 payload
- 把总 payload 均分成多个 chunk
- 对每个 chunk 执行 `ray.put(...)`
- 在远端通过 `ray.get([...])` 一次性读取所有 chunk

如果你想更贴近 verl 里 rollout 的 `DataProto` 形态，也可以用：

```bash
--payload-kind verl-dataproto
```

这个模式会把一个 `TensorDict` 再包一层 `DataProtoLike`，并通过对象的自定义 `__getstate__` / `__setstate__` 走更接近 verl 的序列化路径。

## 默认 Sweep

默认会按 2 倍递增从 `16MB` 一路测到 `32GB`：

- `16MB`
- `32MB`
- `64MB`
- `128MB`
- `256MB`
- `512MB`
- `1GB`
- `2GB`
- `4GB`
- `8GB`
- `16GB`
- `32GB`

对应参数：

```bash
--start-mb 16 --end-gb 32 --multiplier 2
```

如果想测到 `64GB`：

```bash
--end-gb 64
```

## 结果里有什么

每个 size / round 会输出：

- `writer_create_seconds`
  Writer 在机器 A 构造 payload 的时间
- `writer_put_seconds`
  Writer 在机器 A 执行 `ray.put(...)` 的时间
- `reader_consume_seconds`
  Reader 在机器 B 执行 `ray.get(...)` 并拿到 payload 的时间
- `end_to_end_seconds`
  从发起写入到 Reader 消费完成的整段 wall-clock 时间
- `writer_payload_bytes`
  payload 字节数
- `num_chunks`
  当前总 payload 被拆成多少个 chunk
- `object_transfer_timeline_file`
  原始 Ray object transfer trace 文件路径
- `summary_csv`
  sweep 的汇总表，适合直接用表格软件看
- `timeline_summary.transfer_send.total_ms`
  object transfer trace 中 `transfer_send` 的总时长
- `timeline_summary.transfer_receive.total_ms`
  object transfer trace 中 `transfer_receive` 的总时长
- `timeline_summary.receive_pull_request.total_ms`
  object transfer trace 中 `receive_pull_request` 的总时长

## 关于 object transfer trace 统计

这里要特别说明一下：

- 原始 object transfer trace 文件是 Ray 真实导出的 tracing 数据
- CSV 里展示的是最直接的三类 object manager 事件：
  - `transfer_send`
  - `transfer_receive`
  - `receive_pull_request`

所以建议你这样用：

1. 先看 CSV 里的 send / receive / pull 汇总
2. 如果某个 size 很关键，再打开对应的 `object_transfer_timeline_file` 做细看

也就是说：

- `object_transfer_timeline_file` 是原始依据
- `timeline_summary.*` 是方便批量 sweep 比较的整理结果

## 最适合 sweep 的看法

如果你是从 `16MB` 一路 sweep 到 `32GB` / `64GB`，最方便的顺序是：

1. 先看脚本导出的 CSV
2. 用 CSV 找到异常 size
3. 再打开对应那一行里的 `object_transfer_timeline_file`

CSV 里会直接给你这些列：

- `payload_human`
- `round`
- `writer_create_seconds`
- `writer_put_seconds`
- `reader_consume_seconds`
- `end_to_end_seconds`
- `end_to_end_gbps`
- `transfer_send_ms`
- `transfer_receive_ms`
- `receive_pull_request_ms`
- `object_transfer_timeline_file`

所以如果你只是想扫一眼 sweep 结果，**优先看 CSV 就够了**。

## 前置条件

两台机器都需要：

1. 安装好 Ray
2. 安装好脚本运行依赖，比如 `numpy`，默认模式下还需要 `torch`
3. 都加入同一个 Ray 集群
4. 都能访问这个仓库目录

## 启动 Ray

假设：

- 机器 A IP: `10.0.0.1`
- 机器 B IP: `10.0.0.2`

为了让 object transfer trace 更完整，**建议在启动 Ray 之前**，两边都先设置：

```bash
export RAY_PROFILING=1
export RAY_task_events_report_interval_ms=0
```

然后在机器 A 上启动 head：

```bash
ray start --head --resources='{"node:10.0.0.1": 1}'
```

在机器 B 上加入集群：

```bash
ray start --address="10.0.0.1:6379" --resources='{"node:10.0.0.2": 1}'
```

## 推荐启动方式

CPU 路径推荐命令如下：

```bash
python scripts/pure_ray_timeline_benchmark.py \
  --writer-ip 10.0.0.1 \
  --reader-ip 10.0.0.2 \
  --payload-kind cpu-torch \
  --chunks 8 \
  --start-mb 16 \
  --end-gb 32 \
  --multiplier 2 \
  --rounds 1 \
  --timeline-dir ray_object_transfer_outputs \
  --summary-csv pure_ray_timeline_benchmark.csv \
  --output pure_ray_timeline_benchmark.json
```

如果你想继续到 `64GB`：

```bash
python scripts/pure_ray_timeline_benchmark.py \
  --writer-ip 10.0.0.1 \
  --reader-ip 10.0.0.2 \
  --payload-kind cpu-torch \
  --chunks 8 \
  --start-mb 16 \
  --end-gb 64 \
  --multiplier 2 \
  --rounds 1 \
  --timeline-dir ray_object_transfer_outputs_64g \
  --summary-csv pure_ray_timeline_benchmark_64g.csv \
  --output pure_ray_timeline_benchmark_64g.json
```

如果你不传 `--summary-csv`，脚本会默认生成：

```bash
<output 同名>.csv
```

如果你不传 `--timeline-dir`：

- 脚本仍然会输出 `writer_put_seconds`
- 脚本仍然会输出 `reader_consume_seconds`
- 脚本仍然会输出 `end_to_end_seconds`
- 但是不会导出 object transfer trace

## 可选参数

- `--payload-kind`
  可选 `cpu-numpy` / `cpu-torch` / `npu-torch`
- `--dtype`
  默认 `float32`
- `--chunks`
  每个总 payload 切成多少个等大小 chunk，默认 `8`
- `--rounds`
  每个 size 测几轮
- `--size-list-mb`
  自定义 size 列表，比如 `16,32,64,128,256`
- `--timeline-dir`
  object transfer trace 文件输出目录；如果不传，就不会导出 trace
- `--summary-csv`
  CSV 汇总表输出路径
- `--stop-on-error`
  某个 size 出错时立即停止

## 怎么看 object transfer trace

推荐方式：

1. 跑完脚本后，先打开 CSV
2. 找到你想看的那一行
3. 取这一行里的 `object_transfer_timeline_file`
4. 打开 [Perfetto UI](https://ui.perfetto.dev/)
5. 把 `object_transfer_timeline_file` 拖进去

如果你想用 Chrome 老的 tracing 页面，也可以：

```bash
chrome://tracing
```

然后导入对应的 `object_transfer_timeline_file`

## object transfer trace 查看建议

在 trace 里优先看：

- `transfer_send`
- `transfer_receive`
- `receive_pull_request`

建议做法：

1. 先用 CSV 定位异常 size
2. 再打开该 size 对应的 object transfer trace
3. 在 `chrome://tracing` 里打开 `View Options`
4. 勾选 `Flow events`
5. 再看对象在两台机器之间的 send / receive / pull 连线

## 和 TQ 脚本的关系

如果你想做对比：

- [dual_node_meta_benchmark.py](/Users/humphrey/Documents/github/tq_test/TransferQueue/scripts/dual_node_meta_benchmark.py:1)
  测的是 TQ 路径
- [pure_ray_timeline_benchmark.py](/Users/humphrey/Documents/github/tq_test/TransferQueue/scripts/pure_ray_timeline_benchmark.py:1)
  测的是纯 Ray 路径

这样你可以比较：

- TQ 的 `put/get`
- 纯 Ray 的 `ray.put/ray.get`
- 再结合 Ray object transfer trace 看 send / receive / pull 的传输行为

## 当前验证情况

我这边只做了脚本级别的实现和静态检查思路整理，还没有在当前环境做真实运行验证，因为当前环境没有安装 `ray`。
