# Ray

```bash
export RAY_PROFILING=1
export RAY_task_events_report_interval_ms=0
ray start --head --resources='{"node:10.0.0.1": 1}'
```

```bash
export RAY_PROFILING=1
export RAY_task_events_report_interval_ms=0
ray start --address="10.0.0.1:6379" --resources='{"node:10.0.0.2": 1}'
```

# TQ

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

# Pure Ray

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

# Compare Fixed 256MB Samples

```bash
python scripts/compare_fixed_sample_sweep.py \
  --writer-ip 10.0.0.1 \
  --storage-ip 10.0.0.2 \
  --reader-ip 10.0.0.2 \
  --controller-ip 10.0.0.1 \
  --sample-size-mb 256 \
  --sample-count-list 1,2,4,8,16,32,64,128 \
  --tq-shards 8 \
  --rounds 1 \
  --ray-payload-kind cpu-torch \
  --artifacts-dir compare_fixed_sample_artifacts \
  --output-json compare_fixed_sample_sweep.json \
  --output-csv compare_fixed_sample_sweep.csv
```

# TQ Fixed 256MB Samples With Split Storage

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
