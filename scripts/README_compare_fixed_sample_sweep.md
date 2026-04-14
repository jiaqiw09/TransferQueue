# Compare Fixed Sample Sweep

这个脚本用于同时跑：

- TQ benchmark
- Pure Ray benchmark

并把两边结果汇总成一张对比表。

脚本文件：

- [compare_fixed_sample_sweep.py](/Users/humphrey/Documents/github/tq_test/TransferQueue/scripts/compare_fixed_sample_sweep.py:1)

## 默认设定

- 每个 sample / chunk 固定 `256MB`
- sample count 默认 sweep：
  - `1`
  - `2`
  - `4`
  - `8`
  - `16`
  - `32`
  - `64`
  - `128`
- TQ 默认：
  - `8` 个 `SimpleStorageUnit`
  - `chunks = sample_count`
- Ray 默认：
  - `chunks = sample_count`
- 如果你想更贴近 verl rollout 的 `DataProto`，可以把 Ray 改成：
  - `--ray-payload-kind verl-dataproto`

所以：

- `1 sample` = `256MB`
- `2 sample` = `512MB`
- `4 sample` = `1GB`
- `8 sample` = `2GB`
- `16 sample` = `4GB`
- `32 sample` = `8GB`
- `64 sample` = `16GB`
- `128 sample` = `32GB`

## 输出内容

脚本会输出：

- 一份总 JSON
- 一份总 CSV
- 每个 sample_count 对应的 TQ 原始 JSON/CSV
- 每个 sample_count 对应的 Ray 原始 JSON/CSV
- 如果传了 `--ray-timeline-dir`，还会额外输出每个 sample_count 对应的 Ray object transfer trace

## 推荐启动方式

```bash
python scripts/compare_fixed_sample_sweep.py \
  --writer-ip 10.0.0.1 \
  --storage-ip 10.0.0.2 \
  --reader-ip 10.0.0.2 \
  --controller-ip 10.0.0.1 \
  --sample-size-mb 256 \
  --tq-shards 8 \
  --rounds 1 \
  --ray-payload-kind verl-dataproto \
  --artifacts-dir compare_fixed_sample_artifacts \
  --output-json compare_fixed_sample_sweep.json \
  --output-csv compare_fixed_sample_sweep.csv
```

## 自定义 sample count

如果你想手工指定 sample count：

```bash
python scripts/compare_fixed_sample_sweep.py \
  --writer-ip 10.0.0.1 \
  --storage-ip 10.0.0.2 \
  --reader-ip 10.0.0.2 \
  --controller-ip 10.0.0.1 \
  --sample-size-mb 256 \
  --sample-count-list 1,2,4,8,16,32,64,128 \
  --tq-shards 8 \
  --rounds 1
```

## 对比表里有什么

合并后的 CSV 里会有这些主要列：

- `sample_count`
- `sample_size_mb`
- `payload_mb`
- `payload_human`
- `tq_put_seconds`
- `tq_transfer_seconds`
- `tq_read_seconds`
- `tq_three_stage_total_seconds`
- `ray_put_seconds`
- `ray_get_seconds`
- `ray_two_stage_total_seconds`
- `ray_end_to_end_seconds`

## 前置条件

先按你原来的方式启动 Ray 集群。

如果你还要看 Ray object transfer trace，可以额外传：

```bash
--ray-timeline-dir ray_object_transfer_compare_outputs
```

这时建议在两台机器上都先设置：

```bash
export RAY_PROFILING=1
export RAY_task_events_report_interval_ms=0
```

然后再启动 `ray start ...`
