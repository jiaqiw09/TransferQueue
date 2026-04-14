# Dispatch Broadcast Compare

这个脚本专门对应下面这条 `dispatch only` 拓扑：

- 服务器 A 做主控
- `TQ` 的 writer / controller 在服务器 A
- `TQ` 的 `SimpleStorageUnit` 可以按配置放在服务器 B，或者按 `4/4` 分布在 A/B
- 服务器 B 上起 `8` 个 worker
- 这 `8` 个 worker 读的是同一份逻辑 payload
- 只看 `dispatch`，不看 `collect`

脚本文件：

- [dispatch_broadcast_compare.py](/Users/humphrey/Documents/github/tq_test/TransferQueue/scripts/dispatch_only/dispatch_broadcast_compare.py:1)

## 两条对比链路

这份脚本里源头构造的是 `DataProto` 语义 payload：

- `Ray` 侧直接传 `DataProto-like`
- `TQ` 侧先把 `DataProto-like.batch` 写进 TQ，再把 `meta_info/non_tensor_batch` 放进 `BatchMeta.extra_info`

所以对齐的是：

- Ray: `DataProto`
- TQ: `DataProto -> TQ storage + BatchMeta`

### TQ

1. 服务器 A 把一份逻辑 payload 写入 TQ
2. payload 会按 `--chunks` 拆成内部 sample，方便落到 A 上的多个 `SimpleStorageUnit`
3. 服务器 A 上的 single controller 像 `ROLL` 原生那样，对服务器 B 上的 `8` 个 worker 分别发起 actor method 调用
4. 这 `8` 次调用都带着同一个 `BatchMeta` 作为 top-level 参数
5. worker 在方法内部基于这份 `BatchMeta` 从 TQ 读取数据，并 materialize 成 `DataProto` 语义 payload

也就是说：

- 逻辑上只写一次数据
- 控制面上仍然是 repeated actor-arg dispatch
- `8` 个 worker 读的是同一份数据
- 很适合验证你说的“同一份 dispatch 数据，TQ 只写一份”的路径

### Ray

1. 服务器 A 构造一份完整 payload
2. 服务器 A 像 `ROLL` 原生 `DP_MP_COMPUTE` 那样，对 `8` 个 worker 分别发起 actor method 调用
3. 这 `8` 次调用都带着同一个大 `DataProto-like` 作为 top-level 参数
4. 服务器 B 上 `8` 个 worker 分别接收并消费这份 payload

也就是说：

- Ray 这边不是脚本手写 `ray.put` `8` 次
- 但从 `ROLL` 原生 dispatch 的实际效果看，等价于把同一份大参数重复分发给 `8` 个 worker
- 这比“显式 `ray.put` `8` 份”更贴近你现在想对比的 no-TQ dispatch 行为

## 时间统计定义

考虑到服务器 A / 服务器 B 的系统时间可能不一致，这个脚本**不依赖跨机器绝对时间戳**，只用下面两类时间：

- 单侧本地 elapsed duration
- 远端 worker 自己返回的本地 elapsed duration

这样不会因为两台机器时钟不同步，把总耗时算歪。

### TQ 侧

- `tq_put_seconds`
  服务器 A 把 payload 写入 TQ 的时间
- `tq_controller_submit_seconds`
  single controller 在服务器 A 上，把同一个 `BatchMeta` 提交给 `8` 个 worker 的时间
- `tq_all_workers_complete_seconds`
  single controller 在服务器 A 上等待 `8` 个 worker 全部完成并返回结果的时间
- `tq_worker_read_max_seconds`
  `8` 个 worker 里最慢那个 worker 端 `get_data(...)` 时间
- `tq_all_workers_effective_gbps`
  按 `8` 个 worker 的总读取字节数计算的聚合吞吐

### Ray 侧

- `ray_controller_submit_seconds`
  single controller 在服务器 A 上，把同一个大参数提交给 `8` 个 worker 的时间
- `ray_all_workers_complete_seconds`
  single controller 在服务器 A 上等待 `8` 个 worker 全部完成并返回结果的时间
- `ray_worker_handler_max_seconds`
  `8` 个 worker 里最慢那个 worker 方法体内处理时间
- `ray_all_workers_effective_gbps`
  按 `8` 个 worker 的总读取字节数计算的聚合吞吐

## 默认 sweep

默认就是你要的 2 倍递增：

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

## 推荐命令

假设：

- 服务器 A: `10.0.0.1`
- 服务器 B: `10.0.0.2`

### TQ 全部写到 B 上的 8 个 unit

这也是脚本默认值：

```bash
python scripts/dispatch_only/dispatch_broadcast_compare.py \
  --server-a-ip 10.0.0.1 \
  --server-b-ip 10.0.0.2 \
  --benchmarks tq \
  --num-workers 8 \
  --shards 8 \
  --tq-storage-layout all_b \
  --chunks 8 \
  --start-mb 16 \
  --end-gb 32 \
  --multiplier 2 \
  --rounds 1 \
  --output dispatch_broadcast_compare.json \
  --summary-csv dispatch_broadcast_compare.csv
```

### TQ 按 4 个在 A、4 个在 B

```bash
python scripts/dispatch_only/dispatch_broadcast_compare.py \
  --server-a-ip 10.0.0.1 \
  --server-b-ip 10.0.0.2 \
  --benchmarks tq \
  --num-workers 8 \
  --shards 8 \
  --tq-storage-layout split_ab \
  --chunks 8 \
  --start-mb 16 \
  --end-gb 32 \
  --multiplier 2 \
  --rounds 1 \
  --output dispatch_broadcast_compare_split_ab.json \
  --summary-csv dispatch_broadcast_compare_split_ab.csv
```

如果你想先只压小一点的点位：

```bash
python scripts/dispatch_only/dispatch_broadcast_compare.py \
  --server-a-ip 10.0.0.1 \
  --server-b-ip 10.0.0.2 \
  --benchmarks tq \
  --num-workers 8 \
  --shards 8 \
  --tq-storage-layout all_b \
  --chunks 8 \
  --size-list-mb 16,32,64,128,256,512,1024 \
  --rounds 1 \
  --output dispatch_broadcast_compare_small.json \
  --summary-csv dispatch_broadcast_compare_small.csv
```

## 参数说明

- `--server-a-ip`
  writer/controller/storage 所在服务器，也就是主控机
- `--server-b-ip`
  `8` 个 reader worker 所在服务器
- `--num-workers`
  远端 worker 数，默认 `8`
- `--shards`
  TQ 的 `SimpleStorageUnit` 数量，默认 `8`
- `--tq-storage-layout`
  TQ 存储布局，支持 `all_a`、`all_b`、`split_ab`
- `--tq-storage-ip-list`
  显式指定每个 shard 的 IP，优先级高于 `--tq-storage-layout`
- `--chunks`
  一份逻辑 payload 内部拆成多少个 sample，默认 `8`
- `--rounds`
  每个 payload size 跑几轮
- `--size-list-mb`
  手工指定测试点位
- `--benchmarks`
  选择跑 `both`、`tq` 或 `ray`，默认 `both`

## 输出文件

脚本会输出：

- 一份 JSON
- 一份 CSV

CSV 里最核心的列就是：

- `tq_put_seconds`
- `tq_controller_submit_seconds`
- `tq_all_workers_complete_seconds`
- `ray_controller_submit_seconds`
- `ray_all_workers_complete_seconds`

## 注意事项

- `32GB` 点位在 Ray 侧等价于 `8` 份完整 payload 的 put/get，对内存压力会非常大
- `32GB` 点位在现在这版原生 Ray 风格里，等价于 single controller 把同一份 `32GB` 级别的大参数重复提交给 `8` 个 worker，对内存和对象存储压力同样会非常大
- `TQ` 侧虽然逻辑上只写一份，但 `8` 个 worker 并发读完整 payload，服务器 A 和服务器 B 都需要充足内存
- 为了能按 `chunks` 均分，payload size 需要能被 `chunks` 整除
- 脚本默认用的是 `bf16` payload，也就是每个元素 `2 Bytes`
- 我这里把 bulk data transport 主要体现在：
  - TQ: `tq_all_workers_complete_seconds`
  - Ray: `ray_all_workers_complete_seconds`
- `tq_controller_submit_seconds` 只表示 controller 提交 `BatchMeta` 调用，不代表 bulk payload 网络传输
- 对 Ray 原生 dispatch 来说，worker 方法开始执行时参数通常已经完成反序列化，所以 worker 端局部时间只能作为 handler 时间参考；真正更可信的总指标是 controller 看到的 `ray_all_workers_complete_seconds`
