#!/usr/bin/env python3
# Copyright 2025 Huawei Technologies Co., Ltd. All Rights Reserved.
# Copyright 2025 The TransferQueue Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import copy
import csv
import gc
import io
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import ray
import ray.cloudpickle as cloudpickle
import torch
from omegaconf import OmegaConf
from tensordict import TensorDict, TensorDictBase

repo_root = Path(__file__).resolve().parents[2]
sys.path.append(str(repo_root))

from transfer_queue import TransferQueueClient  # noqa: E402
from transfer_queue.controller import TransferQueueController  # noqa: E402
from transfer_queue.metadata import BatchMeta  # noqa: E402
from transfer_queue.storage.simple_backend import SimpleStorageUnit  # noqa: E402
from transfer_queue.utils.zmq_utils import process_zmq_server_info  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MB = 1024**2
GB = 1024**3
DTYPE = torch.bfloat16
DTYPE_BYTES = torch.tensor([], dtype=DTYPE).element_size()


def format_bytes(num_bytes: int) -> str:
    if num_bytes >= GB:
        return f"{num_bytes / GB:.2f} GB"
    if num_bytes >= MB:
        return f"{num_bytes / MB:.2f} MB"
    if num_bytes >= 1024:
        return f"{num_bytes / 1024:.2f} KB"
    return f"{num_bytes} B"


def bytes_to_gbps(num_bytes: int, seconds: float) -> float:
    if seconds <= 0:
        return 0.0
    return (num_bytes * 8) / seconds / 1e9


def build_size_sweep(start_mb: int, end_gb: int, multiplier: int) -> list[int]:
    start_bytes = start_mb * MB
    end_bytes = end_gb * GB
    if start_bytes <= 0:
        raise ValueError("start_mb must be positive")
    if end_bytes < start_bytes:
        raise ValueError("end_gb must be >= start_mb")
    if multiplier < 2:
        raise ValueError("multiplier must be >= 2")

    sizes = []
    current = start_bytes
    while current <= end_bytes:
        sizes.append(current)
        current *= multiplier
    return sizes


def parse_size_list_mb(size_list_mb: str) -> list[int]:
    values = []
    for chunk in size_list_mb.split(","):
        value = chunk.strip()
        if not value:
            continue
        values.append(int(value) * MB)
    if not values:
        raise ValueError("size-list-mb did not contain any valid sizes")
    return values


def parse_ip_list(ip_list: str) -> list[str]:
    values = []
    for chunk in ip_list.split(","):
        value = chunk.strip()
        if not value:
            continue
        values.append(value)
    if not values:
        raise ValueError("storage-ip-list did not contain any valid IPs")
    return values


def build_storage_ip_list(server_a_ip: str, server_b_ip: str, shards: int, mode: str) -> list[str]:
    if mode == "all_a":
        return [server_a_ip] * shards
    if mode == "all_b":
        return [server_b_ip] * shards
    if mode == "split_ab":
        if shards % 2 != 0:
            raise ValueError("split_ab requires an even shard count")
        half = shards // 2
        return [server_a_ip] * half + [server_b_ip] * half
    raise ValueError(f"Unsupported storage layout mode: {mode}")


def build_tq_config(controller_info: Any, storage_unit_infos: Any, num_storage_units: int) -> Any:
    config = OmegaConf.create(
        {
            "num_data_storage_units": num_storage_units,
            "num_data_controllers": 1,
        },
        flags={"allow_objects": True},
    )
    config.controller_info = controller_info
    config.storage_unit_infos = storage_unit_infos
    return config


def compute_chunk_layout(payload_bytes: int, num_chunks: int) -> tuple[int, int]:
    if num_chunks <= 0:
        raise ValueError("num_chunks must be positive")
    if payload_bytes % num_chunks != 0:
        raise ValueError(f"payload_bytes={payload_bytes} must be divisible by num_chunks={num_chunks}")
    chunk_bytes = payload_bytes // num_chunks
    if chunk_bytes % DTYPE_BYTES != 0:
        raise ValueError(
            f"chunk_bytes={chunk_bytes} must be divisible by dtype size {DTYPE_BYTES}. "
            f"Adjust payload size or num_chunks={num_chunks}."
        )
    return num_chunks, chunk_bytes // DTYPE_BYTES


def tensor_payload_nbytes(batch: TensorDictBase | TensorDict | None) -> int:
    if batch is None:
        return 0
    total = 0
    for _, value in batch.items():
        if isinstance(value, torch.Tensor):
            if value.is_nested:
                total += sum(item.numel() * item.element_size() for item in value.unbind())
            else:
                total += value.numel() * value.element_size()
    return total


def tensor_payload_checksum(batch: TensorDictBase | TensorDict | None) -> float:
    if batch is None:
        return 0.0
    total = 0.0
    for _, value in batch.items():
        if isinstance(value, torch.Tensor):
            total += float(value.reshape(-1)[: min(value.numel(), 1024)].float().sum().item())
    return total


def summarize_worker_times(values: list[float], prefix: str) -> dict[str, Any]:
    if not values:
        return {
            f"{prefix}_min_seconds": 0.0,
            f"{prefix}_max_seconds": 0.0,
            f"{prefix}_avg_seconds": 0.0,
        }
    return {
        f"{prefix}_min_seconds": min(values),
        f"{prefix}_max_seconds": max(values),
        f"{prefix}_avg_seconds": sum(values) / len(values),
    }


@dataclass
class DataProtoLike:
    batch: TensorDict
    non_tensor_batch: dict[str, Any] = field(default_factory=dict)
    meta_info: dict[str, Any] = field(default_factory=dict)

    def __getstate__(self):
        buffer = io.BytesIO()
        batch = self.batch
        if hasattr(batch, "contiguous"):
            batch = batch.contiguous()
        if hasattr(batch, "consolidate"):
            batch = batch.consolidate()
        torch.save(batch, buffer)
        return buffer.getvalue(), self.non_tensor_batch, self.meta_info

    def __setstate__(self, state):
        batch_bytes, non_tensor_batch, meta_info = state
        buffer = io.BytesIO(batch_bytes)
        try:
            batch = torch.load(buffer, weights_only=False)
        except TypeError:
            batch = torch.load(buffer)
        self.batch = batch
        self.non_tensor_batch = non_tensor_batch
        self.meta_info = meta_info


def pack_extra_info(payload: DataProtoLike) -> dict[str, Any]:
    return {
        "meta_info": copy.deepcopy(payload.meta_info),
        "non_tensor_batch": copy.deepcopy(payload.non_tensor_batch),
    }


def unpack_extra_info(extra_info: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any]]:
    extra_info = extra_info or {}
    return (
        copy.deepcopy(extra_info.get("meta_info", {})),
        copy.deepcopy(extra_info.get("non_tensor_batch", {})),
    )


def build_dataproto_payload(payload_bytes: int, num_chunks: int, fill_value: float) -> tuple[DataProtoLike, int, int]:
    batch_size, elems_per_chunk = compute_chunk_layout(payload_bytes, num_chunks)
    tensor = torch.full((batch_size, elems_per_chunk), fill_value, dtype=DTYPE)
    batch = TensorDict({"payload": tensor}, batch_size=(batch_size,))
    payload = DataProtoLike(
        batch=batch,
        non_tensor_batch={"sample_ids": list(range(batch_size))},
        meta_info={
            "sample_count": batch_size,
            "chunk_num_elements": elems_per_chunk,
            "payload_bytes": payload_bytes,
            "dtype": str(DTYPE),
        },
    )
    return payload, batch_size, elems_per_chunk


def create_storage_units(storage_ips: list[str], storage_unit_size: int) -> dict[int, Any]:
    storage_units = {}
    for rank, storage_ip in enumerate(storage_ips):
        storage_units[rank] = SimpleStorageUnit.options(
            num_cpus=1,
            resources={f"node:{storage_ip}": 0.001},
            runtime_env={"env_vars": {"OMP_NUM_THREADS": "2"}},
        ).remote(storage_unit_size=storage_unit_size)
    return storage_units


@ray.remote(num_cpus=1)
class TQDispatchWriter:
    def __init__(self, tq_config: Any, num_chunks: int, fill_value: float = 1.0):
        self.client = TransferQueueClient(client_id="dispatch-writer", controller_info=tq_config.controller_info)
        self.client.initialize_storage_manager(manager_type="AsyncSimpleStorageManager", config=tq_config)
        self.num_chunks = num_chunks
        self.fill_value = fill_value

    def put_payload(self, payload_bytes: int, partition_id: str) -> dict[str, Any]:
        create_start = time.perf_counter()
        payload, batch_size, elems_per_chunk = build_dataproto_payload(payload_bytes, self.num_chunks, self.fill_value)
        create_seconds = time.perf_counter() - create_start
        actual_payload_bytes = tensor_payload_nbytes(payload.batch)

        put_start = time.perf_counter()
        metadata = self.client.put(data=payload.batch, partition_id=partition_id)
        put_seconds = time.perf_counter() - put_start
        metadata.extra_info = pack_extra_info(payload)

        metadata_bytes = len(cloudpickle.dumps(metadata))
        checksum = tensor_payload_checksum(payload.batch)
        del payload
        gc.collect()

        return {
            "metadata": metadata,
            "payload_bytes": actual_payload_bytes,
            "batch_size": batch_size,
            "chunk_num_elements": elems_per_chunk,
            "num_chunks": batch_size,
            "create_seconds": create_seconds,
            "put_seconds": put_seconds,
            "metadata_ray_bytes_single": metadata_bytes,
            "checksum": checksum,
        }

    def clear_partition(self, partition_id: str) -> None:
        self.client.clear_partition(partition_id)
        gc.collect()


@ray.remote(num_cpus=1)
class TQDispatchReader:
    def __init__(self, tq_config: Any, worker_idx: int):
        self.client = TransferQueueClient(client_id=f"dispatch-reader-{worker_idx}", controller_info=tq_config.controller_info)
        self.client.initialize_storage_manager(manager_type="AsyncSimpleStorageManager", config=tq_config)
        self.worker_idx = worker_idx
    
    def consume_metadata(self, metadata: BatchMeta) -> dict[str, Any]:
        local_metadata = copy.deepcopy(metadata)
        start = time.perf_counter()
        batch = self.client.get_data(local_metadata)
        read_seconds = time.perf_counter() - start
        meta_info, non_tensor_batch = unpack_extra_info(getattr(local_metadata, "extra_info", None))
        payload = DataProtoLike(batch=batch, non_tensor_batch=non_tensor_batch, meta_info=meta_info)
        payload_bytes = tensor_payload_nbytes(payload.batch)
        checksum = tensor_payload_checksum(payload.batch)
        shape_summary = {
            key: list(value.shape) for key, value in payload.batch.items() if isinstance(value, torch.Tensor)
        }

        del payload
        gc.collect()

        return {
            "worker_idx": self.worker_idx,
            "partition_id": getattr(local_metadata, "partition_ids", [None])[0] if getattr(local_metadata, "partition_ids", None) else None,
            "read_seconds": read_seconds,
            "payload_bytes": payload_bytes,
            "checksum": checksum,
            "shape_summary": shape_summary,
        }


@ray.remote(num_cpus=1)
class RayDispatchWriter:
    def __init__(self, num_chunks: int, fill_value: float = 1.0):
        self.num_chunks = num_chunks
        self.fill_value = fill_value

    def build_payload(self, payload_bytes: int) -> dict[str, Any]:
        create_start = time.perf_counter()
        payload, batch_size, elems_per_chunk = build_dataproto_payload(payload_bytes, self.num_chunks, self.fill_value)
        create_seconds = time.perf_counter() - create_start
        actual_payload_bytes = tensor_payload_nbytes(payload.batch)
        checksum = tensor_payload_checksum(payload.batch)

        return {
            "payload": payload,
            "payload_bytes_single": actual_payload_bytes,
            "batch_size": batch_size,
            "chunk_num_elements": elems_per_chunk,
            "num_chunks": batch_size,
            "create_seconds": create_seconds,
            "checksum": checksum,
        }


@ray.remote(num_cpus=1)
class RayDispatchReader:
    def __init__(self, worker_idx: int):
        self.worker_idx = worker_idx

    def consume_payload(self, payload: Any) -> dict[str, Any]:
        start = time.perf_counter()

        batch = payload.batch if isinstance(payload, DataProtoLike) else payload
        if not isinstance(batch, TensorDictBase):
            raise TypeError(f"Unexpected Ray payload type: {type(batch)}")

        payload_bytes = tensor_payload_nbytes(batch)
        checksum = tensor_payload_checksum(batch)
        shape_summary = {key: list(value.shape) for key, value in batch.items() if isinstance(value, torch.Tensor)}
        handler_seconds = time.perf_counter() - start

        del payload
        gc.collect()

        return {
            "worker_idx": self.worker_idx,
            "handler_seconds": handler_seconds,
            "payload_bytes": payload_bytes,
            "checksum": checksum,
            "shape_summary": shape_summary,
        }


def build_tq_result(
    raw_write: dict[str, Any],
    controller_submit_seconds: float,
    worker_reads: list[dict[str, Any]],
    worker_count: int,
    payload_bytes: int,
    payload_human: str,
    partition_id: str,
) -> dict[str, Any]:
    read_seconds = [item["read_seconds"] for item in worker_reads]
    aggregate_read_bytes = sum(item["payload_bytes"] for item in worker_reads)
    result = {
        "mode": "tq",
        "payload_bytes": payload_bytes,
        "payload_human": payload_human,
        "partition_id": partition_id,
        "worker_count": worker_count,
        "num_chunks": raw_write["num_chunks"],
        "batch_size": raw_write["batch_size"],
        "chunk_num_elements": raw_write["chunk_num_elements"],
        "create_seconds": raw_write["create_seconds"],
        "put_seconds": raw_write["put_seconds"],
        "controller_submit_seconds": controller_submit_seconds,
        "all_workers_complete_seconds": 0.0,
        "all_workers_read_aggregate_bytes": aggregate_read_bytes,
        "metadata_ray_bytes_single": raw_write["metadata_ray_bytes_single"],
        "metadata_ray_bytes_total": raw_write["metadata_ray_bytes_single"] * worker_count,
        "writer_checksum": raw_write["checksum"],
        "worker_checksums": [item["checksum"] for item in worker_reads],
        "worker_payload_bytes": [item["payload_bytes"] for item in worker_reads],
        "worker_read_seconds": read_seconds,
        "shape_summary": worker_reads[0]["shape_summary"] if worker_reads else {},
    }
    result.update(summarize_worker_times(read_seconds, "worker_read"))
    return result


def build_ray_result(
    raw_payload: dict[str, Any],
    controller_submit_seconds: float,
    worker_receives: list[dict[str, Any]],
    worker_count: int,
    payload_bytes: int,
    payload_human: str,
) -> dict[str, Any]:
    handler_seconds = [item["handler_seconds"] for item in worker_receives]
    aggregate_receive_bytes = sum(item["payload_bytes"] for item in worker_receives)
    result = {
        "mode": "ray",
        "payload_bytes": payload_bytes,
        "payload_human": payload_human,
        "worker_count": worker_count,
        "num_chunks": raw_payload["num_chunks"],
        "batch_size": raw_payload["batch_size"],
        "chunk_num_elements": raw_payload["chunk_num_elements"],
        "create_seconds": raw_payload["create_seconds"],
        "controller_submit_seconds": controller_submit_seconds,
        "all_workers_complete_seconds": 0.0,
        "all_workers_aggregate_bytes": aggregate_receive_bytes,
        "payload_bytes_total_dispatched_estimate": raw_payload["payload_bytes_single"] * worker_count,
        "writer_checksum": raw_payload["checksum"],
        "worker_checksums": [item["checksum"] for item in worker_receives],
        "worker_payload_bytes": [item["payload_bytes"] for item in worker_receives],
        "worker_handler_seconds": handler_seconds,
        "shape_summary": worker_receives[0]["shape_summary"] if worker_receives else {},
    }
    result.update(summarize_worker_times(handler_seconds, "worker_handler"))
    return result


def build_compare_row(result: dict[str, Any]) -> dict[str, Any]:
    tq = result["tq"]
    ray_result = result["ray"]
    return {
        "payload_bytes": result["payload_bytes"],
        "payload_human": result["payload_human"],
        "round": result["round"],
        "worker_count": result["worker_count"],
        "tq_put_seconds": tq.get("put_seconds"),
        "tq_controller_submit_seconds": tq.get("controller_submit_seconds"),
        "tq_all_workers_complete_seconds": tq.get("all_workers_complete_seconds"),
        "tq_dispatch_total_seconds": tq.get("dispatch_total_seconds"),
        "tq_metadata_ray_bytes_single": tq.get("metadata_ray_bytes_single"),
        "tq_metadata_ray_bytes_total": tq.get("metadata_ray_bytes_total"),
        "tq_worker_read_max_seconds": tq.get("worker_read_max_seconds"),
        "tq_worker_read_avg_seconds": tq.get("worker_read_avg_seconds"),
        "tq_all_workers_effective_gbps": tq.get("all_workers_effective_gbps"),
        "ray_controller_submit_seconds": ray_result.get("controller_submit_seconds"),
        "ray_all_workers_complete_seconds": ray_result.get("all_workers_complete_seconds"),
        "ray_dispatch_total_seconds": ray_result.get("dispatch_total_seconds"),
        "ray_payload_bytes_total_dispatched_estimate": ray_result.get("payload_bytes_total_dispatched_estimate"),
        "ray_worker_handler_max_seconds": ray_result.get("worker_handler_max_seconds"),
        "ray_worker_handler_avg_seconds": ray_result.get("worker_handler_avg_seconds"),
        "ray_all_workers_effective_gbps": ray_result.get("all_workers_effective_gbps"),
    }


def build_summary_csv_rows(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for result in results:
        if "error" in result:
            rows.append(
                {
                    "payload_bytes": result.get("payload_bytes"),
                    "payload_human": result.get("payload_human"),
                    "round": result.get("round"),
                    "status": "error",
                    "error": result.get("error"),
                }
            )
            continue

        if "tq" in result and "ray" in result:
            row = build_compare_row(result)
        else:
            row = {
                "payload_bytes": result.get("payload_bytes"),
                "payload_human": result.get("payload_human"),
                "round": result.get("round"),
                "worker_count": result.get("worker_count"),
                "tq_put_seconds": result.get("tq", {}).get("put_seconds") if "tq" in result else "",
                "tq_controller_submit_seconds": result.get("tq", {}).get("controller_submit_seconds") if "tq" in result else "",
                "tq_all_workers_complete_seconds": result.get("tq", {}).get("all_workers_complete_seconds") if "tq" in result else "",
                "tq_dispatch_total_seconds": result.get("tq", {}).get("dispatch_total_seconds") if "tq" in result else "",
                "tq_metadata_ray_bytes_single": result.get("tq", {}).get("metadata_ray_bytes_single") if "tq" in result else "",
                "tq_metadata_ray_bytes_total": result.get("tq", {}).get("metadata_ray_bytes_total") if "tq" in result else "",
                "tq_worker_read_max_seconds": result.get("tq", {}).get("worker_read_max_seconds") if "tq" in result else "",
                "tq_worker_read_avg_seconds": result.get("tq", {}).get("worker_read_avg_seconds") if "tq" in result else "",
                "tq_all_workers_effective_gbps": result.get("tq", {}).get("all_workers_effective_gbps") if "tq" in result else "",
                "ray_controller_submit_seconds": result.get("ray", {}).get("controller_submit_seconds") if "ray" in result else "",
                "ray_all_workers_complete_seconds": result.get("ray", {}).get("all_workers_complete_seconds") if "ray" in result else "",
                "ray_dispatch_total_seconds": result.get("ray", {}).get("dispatch_total_seconds") if "ray" in result else "",
                "ray_payload_bytes_total_dispatched_estimate": result.get("ray", {}).get("payload_bytes_total_dispatched_estimate") if "ray" in result else "",
                "ray_worker_handler_max_seconds": result.get("ray", {}).get("worker_handler_max_seconds") if "ray" in result else "",
                "ray_worker_handler_avg_seconds": result.get("ray", {}).get("worker_handler_avg_seconds") if "ray" in result else "",
                "ray_all_workers_effective_gbps": result.get("ray", {}).get("all_workers_effective_gbps") if "ray" in result else "",
            }
        row["status"] = "ok"
        row["error"] = ""
        rows.append(row)
    return rows


def write_summary_csv(csv_path: str, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "payload_bytes",
        "payload_human",
        "round",
        "status",
        "worker_count",
        "tq_put_seconds",
        "tq_controller_submit_seconds",
        "tq_all_workers_complete_seconds",
        "tq_dispatch_total_seconds",
        "tq_metadata_ray_bytes_single",
        "tq_metadata_ray_bytes_total",
        "tq_worker_read_max_seconds",
        "tq_worker_read_avg_seconds",
        "tq_all_workers_effective_gbps",
        "ray_controller_submit_seconds",
        "ray_all_workers_complete_seconds",
        "ray_dispatch_total_seconds",
        "ray_payload_bytes_total_dispatched_estimate",
        "ray_worker_handler_max_seconds",
        "ray_worker_handler_avg_seconds",
        "ray_all_workers_effective_gbps",
        "error",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dispatch-only dual-node compare benchmark: one TQ write + 8 remote reads vs native-Roll-style Ray repeated actor-arg dispatch to 8 workers"
    )
    parser.add_argument("--server-a-ip", type=str, required=True, help="Server A IP. Writer/controller/storage all live here.")
    parser.add_argument("--server-b-ip", type=str, required=True, help="Server B IP. All dispatch workers live here.")
    parser.add_argument("--num-workers", type=int, default=8, help="Number of remote readers/workers on server B")
    parser.add_argument("--shards", type=int, default=8, help="Number of TQ SimpleStorageUnit actors on server A")
    parser.add_argument(
        "--tq-storage-layout",
        type=str,
        default="all_b",
        choices=["all_a", "all_b", "split_ab"],
        help="Where to place TQ SimpleStorageUnit actors: all on A, all on B, or split evenly across A/B",
    )
    parser.add_argument(
        "--tq-storage-ip-list",
        type=str,
        default=None,
        help="Optional comma-separated explicit TQ storage placement, one IP per shard. Overrides --tq-storage-layout.",
    )
    parser.add_argument(
        "--chunks",
        type=int,
        default=8,
        help="Split one logical payload into this many internal samples so TQ can spread storage across shards",
    )
    parser.add_argument("--start-mb", type=int, default=16, help="Sweep start size in MB")
    parser.add_argument("--end-gb", type=int, default=32, help="Sweep end size in GB")
    parser.add_argument("--multiplier", type=int, default=2, help="Sweep multiplier between consecutive points")
    parser.add_argument(
        "--size-list-mb",
        type=str,
        default=None,
        help="Optional comma-separated override for exact sweep sizes in MB, e.g. 16,32,64,128",
    )
    parser.add_argument("--rounds", type=int, default=1, help="Benchmark rounds per payload size")
    parser.add_argument("--fill-value", type=float, default=1.0, help="Constant used to materialize payload tensors")
    parser.add_argument(
        "--benchmarks",
        type=str,
        default="both",
        choices=["both", "tq", "ray"],
        help="Which branches to run: both, tq only, or ray only",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="dispatch_broadcast_compare.json",
        help="Output JSON path",
    )
    parser.add_argument(
        "--summary-csv",
        type=str,
        default=None,
        help="Optional CSV summary path. Defaults to <output_basename>.csv",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop the sweep immediately on the first failing size",
    )
    args = parser.parse_args()

    if args.chunks <= 0:
        raise ValueError("--chunks must be positive")
    if args.shards <= 0:
        raise ValueError("--shards must be positive")
    if args.num_workers <= 0:
        raise ValueError("--num-workers must be positive")

    summary_csv = args.summary_csv or str(Path(args.output).with_suffix(".csv"))
    sweep_sizes = parse_size_list_mb(args.size_list_mb) if args.size_list_mb else build_size_sweep(
        start_mb=args.start_mb,
        end_gb=args.end_gb,
        multiplier=args.multiplier,
    )
    max_payload_bytes = max(sweep_sizes)
    max_batch_size, _ = compute_chunk_layout(max_payload_bytes, args.chunks)
    storage_unit_size = max(1, (max_batch_size + args.shards - 1) // args.shards)
    storage_ips = (
        parse_ip_list(args.tq_storage_ip_list)
        if args.tq_storage_ip_list
        else build_storage_ip_list(args.server_a_ip, args.server_b_ip, args.shards, args.tq_storage_layout)
    )
    if len(storage_ips) != args.shards:
        raise ValueError(f"Expected {args.shards} storage IPs, got {len(storage_ips)}")

    cwd = os.getcwd()
    if not ray.is_initialized():
        ray.init(address="auto", runtime_env={"working_dir": cwd})

    logger.info(
        "Dispatch benchmark topology: server_a=%s server_b=%s workers=%s tq_shards=%s chunks=%s tq_storage_ips=%s",
        args.server_a_ip,
        args.server_b_ip,
        args.num_workers,
        args.shards,
        args.chunks,
        storage_ips,
    )
    logger.info("Sweep sizes: %s", ", ".join(format_bytes(size) for size in sweep_sizes))

    controller = None
    storage_units = None
    tq_writer = None
    tq_readers = []
    ray_writer = None
    ray_readers = []
    results = []

    try:
        controller = TransferQueueController.options(resources={f"node:{args.server_a_ip}": 0.001}).remote()
        storage_units = create_storage_units(storage_ips, storage_unit_size)
        controller_info = process_zmq_server_info(controller)
        storage_unit_infos = process_zmq_server_info(storage_units)
        tq_config = build_tq_config(controller_info, storage_unit_infos, args.shards)

        tq_writer = TQDispatchWriter.options(
            resources={f"node:{args.server_a_ip}": 0.001},
            runtime_env={"env_vars": {"OMP_NUM_THREADS": "2"}},
        ).remote(tq_config, args.chunks, args.fill_value)
        tq_readers = [
            TQDispatchReader.options(
                resources={f"node:{args.server_b_ip}": 0.001},
                runtime_env={"env_vars": {"OMP_NUM_THREADS": "2"}},
            ).remote(tq_config, worker_idx)
            for worker_idx in range(args.num_workers)
        ]
        ray_writer = RayDispatchWriter.options(
            resources={f"node:{args.server_a_ip}": 0.001},
            runtime_env={"env_vars": {"OMP_NUM_THREADS": "2"}},
        ).remote(args.chunks, args.fill_value)
        ray_readers = [
            RayDispatchReader.options(
                resources={f"node:{args.server_b_ip}": 0.001},
                runtime_env={"env_vars": {"OMP_NUM_THREADS": "2"}},
            ).remote(worker_idx)
            for worker_idx in range(args.num_workers)
        ]

        run_tq = args.benchmarks in ("both", "tq")
        run_ray = args.benchmarks in ("both", "ray")

        for payload_bytes in sweep_sizes:
            payload_human = format_bytes(payload_bytes)
            for round_idx in range(args.rounds):
                partition_id = f"dispatch_broadcast_{payload_bytes}_{round_idx}"
                logger.info(
                    "Running payload=%s round=%s/%s partition=%s",
                    payload_human,
                    round_idx + 1,
                    args.rounds,
                    partition_id,
                )
                try:
                    tq_result = None
                    if run_tq:
                        tq_write = ray.get(tq_writer.put_payload.remote(payload_bytes, partition_id))
                        tq_meta = tq_write.pop("metadata")

                        tq_submit_start = time.perf_counter()
                        tq_result_refs = [reader.consume_metadata.remote(tq_meta) for reader in tq_readers]
                        tq_controller_submit_seconds = time.perf_counter() - tq_submit_start

                        tq_wait_start = time.perf_counter()
                        tq_worker_reads = ray.get(tq_result_refs)
                        tq_all_workers_complete_seconds = time.perf_counter() - tq_wait_start

                        ray.get(tq_writer.clear_partition.remote(partition_id))

                        tq_result = build_tq_result(
                            raw_write=tq_write,
                            controller_submit_seconds=tq_controller_submit_seconds,
                            worker_reads=tq_worker_reads,
                            worker_count=args.num_workers,
                            payload_bytes=payload_bytes,
                            payload_human=payload_human,
                            partition_id=partition_id,
                        )
                        tq_result["all_workers_complete_seconds"] = tq_all_workers_complete_seconds
                        tq_result["dispatch_total_seconds"] = (
                            tq_result["put_seconds"]
                            + tq_result["controller_submit_seconds"]
                            + tq_result["all_workers_complete_seconds"]
                        )
                        tq_result["all_workers_effective_gbps"] = bytes_to_gbps(
                            tq_result["all_workers_read_aggregate_bytes"],
                            tq_result["all_workers_complete_seconds"],
                        )

                    ray_result = None
                    if run_ray:
                        ray_payload = ray.get(ray_writer.build_payload.remote(payload_bytes))
                        payload_obj = ray_payload.pop("payload")
                        ray_submit_start = time.perf_counter()
                        ray_result_refs = [reader.consume_payload.remote(payload_obj) for reader in ray_readers]
                        ray_controller_submit_seconds = time.perf_counter() - ray_submit_start

                        ray_wait_start = time.perf_counter()
                        ray_worker_reads = ray.get(ray_result_refs)
                        ray_all_workers_complete_seconds = time.perf_counter() - ray_wait_start

                        ray_result = build_ray_result(
                            raw_payload=ray_payload,
                            controller_submit_seconds=ray_controller_submit_seconds,
                            worker_receives=ray_worker_reads,
                            worker_count=args.num_workers,
                            payload_bytes=payload_bytes,
                            payload_human=payload_human,
                        )
                        ray_result["all_workers_complete_seconds"] = ray_all_workers_complete_seconds
                        ray_result["dispatch_total_seconds"] = (
                            ray_result["controller_submit_seconds"] + ray_result["all_workers_complete_seconds"]
                        )
                        ray_result["all_workers_effective_gbps"] = bytes_to_gbps(
                            ray_result["all_workers_aggregate_bytes"],
                            ray_result["all_workers_complete_seconds"],
                        )

                    result = {
                        "round": round_idx + 1,
                        "payload_bytes": payload_bytes,
                        "payload_human": payload_human,
                        "worker_count": args.num_workers,
                        "server_a_ip": args.server_a_ip,
                        "server_b_ip": args.server_b_ip,
                        "shards": args.shards,
                        "chunks": args.chunks,
                        "tq_storage_ips": storage_ips,
                    }
                    if tq_result is not None:
                        result["tq"] = tq_result
                    if ray_result is not None:
                        result["ray"] = ray_result
                    if tq_result is not None and ray_result is not None:
                        result["compare"] = build_compare_row(
                            {
                                "round": round_idx + 1,
                                "payload_bytes": payload_bytes,
                                "payload_human": payload_human,
                                "worker_count": args.num_workers,
                                "tq": tq_result,
                                "ray": ray_result,
                            }
                        )
                    results.append(result)

                    if tq_result is not None and ray_result is not None:
                        logger.info(
                            "Done payload=%s | TQ put=%.4fs submit=%.4fs wait_all=%.4fs | Ray submit=%.4fs wait_all=%.4fs",
                            payload_human,
                            tq_result["put_seconds"],
                            tq_result["controller_submit_seconds"],
                            tq_result["all_workers_complete_seconds"],
                            ray_result["controller_submit_seconds"],
                            ray_result["all_workers_complete_seconds"],
                        )
                    elif tq_result is not None:
                        logger.info(
                            "Done payload=%s | TQ put=%.4fs submit=%.4fs wait_all=%.4fs",
                            payload_human,
                            tq_result["put_seconds"],
                            tq_result["controller_submit_seconds"],
                            tq_result["all_workers_complete_seconds"],
                        )
                    elif ray_result is not None:
                        logger.info(
                            "Done payload=%s | Ray submit=%.4fs wait_all=%.4fs",
                            payload_human,
                            ray_result["controller_submit_seconds"],
                            ray_result["all_workers_complete_seconds"],
                        )
                except Exception as exc:
                    error_result = {
                        "round": round_idx + 1,
                        "payload_bytes": payload_bytes,
                        "payload_human": payload_human,
                        "worker_count": args.num_workers,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                    results.append(error_result)
                    logger.exception("Dispatch benchmark failed for payload=%s round=%s", payload_human, round_idx + 1)
                    if args.stop_on_error:
                        raise

        with open(args.output, "w") as f:
            json.dump(
                {
                    "config": vars(args),
                    "resolved_config": {
                        "writer_ip": args.server_a_ip,
                        "controller_ip": args.server_a_ip,
                        "storage_ips": storage_ips,
                        "reader_ip": args.server_b_ip,
                    },
                    "results": results,
                },
                f,
                indent=2,
            )
        csv_rows = build_summary_csv_rows(results)
        write_summary_csv(summary_csv, csv_rows)
        logger.info("Results saved to %s", args.output)
        logger.info("CSV summary saved to %s", summary_csv)
    finally:
        if ray.is_initialized():
            ray.shutdown()


if __name__ == "__main__":
    main()
