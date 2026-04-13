# Dual-Node TransferQueue Meta Benchmark

这个脚本用于测试下面这条双机链路：

1. 机器 A 上的 `WriterActor` 构造数据
2. 机器 A 调用 `TransferQueueClient.put(...)`
3. 数据写入机器 B 上的 `SimpleStorageUnit`
4. 机器 A 把 `BatchMeta` 通过 Ray 发送给机器 B 上的 `ReaderActor`
5. 机器 B 基于这份 `BatchMeta` 从 `SimpleStorageUnit` 读取数据

这个脚本现在按“**一个总 payload 会被均分成多个 chunk/sample**”来测。

默认情况下：

- `--chunks` 默认等于 `--shards`
- 也就是总 payload 会均分成和 `SimpleStorageUnit` 数量相同的 sample
- 比如 `--shards 8` 时，总 payload 会切成 `8` 个等大小 sample

脚本文件：

- [dual_node_meta_benchmark.py](/Users/humphrey/Documents/github/tq_test/TransferQueue/scripts/dual_node_meta_benchmark.py:1)

## 默认 Sweep

默认参数下，这个脚本会按 2 倍递增做完整 sweep：

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

也就是：

```bash
--start-mb 16 --end-gb 32 --multiplier 2
```

如果你想继续测到 `64GB`，直接改成：

```bash
--end-gb 64
```

## 记录的指标

每个 size / round 会输出并保存这些关键指标：

- `put_seconds`
  机器 A 把 payload 写入机器 B 上 `SimpleStorageUnit` 的时间
- `metadata_transfer_seconds`
  `BatchMeta` 从机器 A 经 Ray 发送到机器 B 的时间
- `read_seconds`
  机器 B 基于 `BatchMeta` 从 `SimpleStorageUnit` 取回数据的时间
- `payload_bytes`
  实际 payload 大小
- `metadata_ray_bytes`
  `BatchMeta` 经 `cloudpickle` 序列化后的大小，可作为 Ray 传输数据量的近似值
- `put_gbps`
  按 payload 大小计算的写入吞吐
- `read_gbps`
  按 payload 大小计算的读取吞吐
- `summary_csv`
  sweep 的汇总表，适合直接用表格软件看

## 前置条件

在两台机器上都需要准备好：

1. 安装项目依赖，至少要有 `ray`、`torch`、`tensordict`、`omegaconf`
2. 两台机器都能访问同一个 Ray 集群
3. 启动 Ray 并给节点打上 `node:<IP>` 资源标签
4. 两台机器都能访问这个仓库代码

## 启动 Ray

假设：

- 机器 A IP: `10.0.0.1`
- 机器 B IP: `10.0.0.2`

在机器 A 上启动 head：

```bash
ray start --head --resources='{"node:10.0.0.1": 1}'
```

在机器 B 上加入集群：

```bash
ray start --address="10.0.0.1:6379" --resources='{"node:10.0.0.2": 1}'
```

## 最常用启动方式

最典型的是：

- `WriterActor` 在机器 A
- `TransferQueueController` 在机器 A
- `SimpleStorageUnit` 在机器 B
- `ReaderActor` 在机器 B

命令如下：

```bash
python scripts/dual_node_meta_benchmark.py \
  --writer-ip 10.0.0.1 \
  --storage-ip 10.0.0.2 \
  --reader-ip 10.0.0.2 \
  --controller-ip 10.0.0.1 \
  --start-mb 16 \
  --end-gb 32 \
  --multiplier 2 \
  --shards 8 \
  --chunks 8 \
  --rounds 1 \
  --summary-csv dual_node_meta_benchmark.csv \
  --output dual_node_meta_benchmark.json
```

如果你想直接测到 `64GB`：

```bash
python scripts/dual_node_meta_benchmark.py \
  --writer-ip 10.0.0.1 \
  --storage-ip 10.0.0.2 \
  --reader-ip 10.0.0.2 \
  --controller-ip 10.0.0.1 \
  --start-mb 16 \
  --end-gb 64 \
  --multiplier 2 \
  --shards 8 \
  --chunks 8 \
  --rounds 1 \
  --summary-csv dual_node_meta_benchmark_64g.csv \
  --output dual_node_meta_benchmark_64g.json
```

## 可选参数

- `--writer-ip`
  Writer 所在节点，通常是机器 A
- `--storage-ip`
  `SimpleStorageUnit` 所在节点，通常是机器 B
- `--reader-ip`
  Reader 所在节点，默认等于 `--storage-ip`
- `--controller-ip`
  Controller 所在节点，默认等于 `--writer-ip`
- `--shards`
  机器 B 上启动多少个 `SimpleStorageUnit`，默认 `8`
- `--chunks`
  每个总 payload 切成多少个等大小 sample，默认等于 `--shards`
- `--rounds`
  每个 payload size 测几轮
- `--summary-csv`
  CSV 汇总表输出路径
- `--size-list-mb`
  自定义 size 列表，比如 `16,32,64,128,256`
- `--stop-on-error`
  遇到某个 size 失败时立即停止

## 自定义 Sweep

如果不想用默认的 2 倍递增 sweep，可以手工指定：

```bash
python scripts/dual_node_meta_benchmark.py \
  --writer-ip 10.0.0.1 \
  --storage-ip 10.0.0.2 \
  --size-list-mb 16,32,64,128,256,512,1024,2048 \
  --output custom_sizes.json
```

注意：

- `size-list-mb` 的单位是 MB
- 每个 size 会先切成 `chunks` 个等大小 sample，再写入 TQ
- 为了切分干净，payload size 需要能被 `chunks` 整除

## 输出结果

脚本会输出一个 JSON 文件，默认叫：

```bash
dual_node_meta_benchmark.json
```

JSON 里主要有三部分：

- `config`
  原始启动参数
- `resolved_config`
  脚本解析后的实际配置
- `results`
  每个 size / round 的测试结果

如果你不传 `--summary-csv`，脚本会默认生成：

```bash
<output 同名>.csv
```

## 关于 `metadata_ray_bytes`

这里的 `metadata_ray_bytes` 不是 Ray 内部网络层的精确抓包值，而是：

- 对 `BatchMeta` 做 `cloudpickle.dumps(...)`
- 取序列化后的字节数

它适合用来回答这个问题：

“通过 Ray 传的 `BatchMeta` 大概有多大，和真正 payload 相比差多少？”

## 注意事项

- `SimpleStorageUnit` 是内存存储，测到 `32GB` 或 `64GB` 时，两边机器都要有比较充足的内存余量
- 默认 `--shards 8 --chunks 8` 时，总 payload 会被切成 8 个 sample，分发到 8 个 `SimpleStorageUnit`
- `put_seconds` 和 `read_seconds` 都包含了 TQ 自身序列化、ZMQ 通信、内存拷贝等开销，不是纯裸网络时间
- `metadata_transfer_seconds` 是 `WriterActor -> ReaderActor.accept_metadata(...) -> ack` 的端到端时间，不是仅网络层时间
- 我这边只做了脚本语法校验，没在当前环境做真实双机运行，因为当前环境没有安装 `ray`
