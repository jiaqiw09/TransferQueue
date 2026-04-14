# TQ Fixed Sample Split Storage Sweep

这个脚本只跑 TQ。

测试形态固定为：

- 每个 sample 固定 `256MB`
- sample count 从 `1` 一直到 `128`
- 一共 `8` 个 `SimpleStorageUnit`
- `4` 个放在服务器 A
- `4` 个放在服务器 B

脚本文件：

- [tq_fixed_sample_split_storage_sweep.py](/Users/humphrey/Documents/github/tq_test/TransferQueue/scripts/tq_fixed_sample_split_storage_sweep.py:1)

## 默认 Sweep

- `1 sample` = `256MB`
- `2 sample` = `512MB`
- `4 sample` = `1GB`
- `8 sample` = `2GB`
- `16 sample` = `4GB`
- `32 sample` = `8GB`
- `64 sample` = `16GB`
- `128 sample` = `32GB`

## 拓扑

默认拓扑是：

- Writer 在服务器 A
- Controller 在服务器 A
- Reader 在服务器 B
- `SimpleStorageUnit` 共 `8` 个
- storage 分布是：
  - `A,A,A,A,B,B,B,B`

## 推荐启动方式

```bash
python scripts/tq_fixed_sample_split_storage_sweep.py \
  --server-a-ip 10.0.0.1 \
  --server-b-ip 10.0.0.2 \
  --sample-size-mb 256 \
  --sample-count-list 1,2,4,8,16,32,64,128 \
  --shards 8 \
  --rounds 1 \
  --artifacts-dir tq_fixed_sample_split_storage_artifacts \
  --output-json tq_fixed_sample_split_storage_sweep.json \
  --output-csv tq_fixed_sample_split_storage_sweep.csv
```

## 输出内容

总 CSV 会有这些主要列：

- `sample_count`
- `sample_size_mb`
- `payload_mb`
- `put_seconds`
- `metadata_transfer_seconds`
- `read_seconds`
- `three_stage_total_seconds`
- `put_gbps`
- `read_gbps`
- `storage_ip_list`

每个 sample count 还会保留一份原始 TQ JSON/CSV。

## 可选拓扑

如果你想改 Writer / Reader / Controller 的位置，也可以显式传：

```bash
--writer-ip ...
--reader-ip ...
--controller-ip ...
```
