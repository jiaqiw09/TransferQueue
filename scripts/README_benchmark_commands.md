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
  --rounds 1 \
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
  --rounds 1 \
  --output dual_node_meta_benchmark_64g.json
```

# Pure Ray

```bash
python scripts/pure_ray_timeline_benchmark.py \
  --writer-ip 10.0.0.1 \
  --reader-ip 10.0.0.2 \
  --payload-kind cpu-torch \
  --start-mb 16 \
  --end-gb 32 \
  --multiplier 2 \
  --rounds 1 \
  --timeline-dir ray_timeline_outputs \
  --summary-csv pure_ray_timeline_benchmark.csv \
  --output pure_ray_timeline_benchmark.json
```

```bash
python scripts/pure_ray_timeline_benchmark.py \
  --writer-ip 10.0.0.1 \
  --reader-ip 10.0.0.2 \
  --payload-kind cpu-torch \
  --start-mb 16 \
  --end-gb 64 \
  --multiplier 2 \
  --rounds 1 \
  --timeline-dir ray_timeline_outputs_64g \
  --summary-csv pure_ray_timeline_benchmark_64g.csv \
  --output pure_ray_timeline_benchmark_64g.json
```
