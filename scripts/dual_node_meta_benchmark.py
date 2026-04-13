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
import csv
import gc
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import ray
import ray.cloudpickle as cloudpickle
import torch
from omegaconf import OmegaConf
from tensordict import TensorDict

repo_root = Path(__file__).resolve().parent.parent
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
DTYPE = torch.float32
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


def tensor_payload_nbytes(batch: TensorDict) -> int:
    total = 0
    for _, value in batch.items():
        if isinstance(value, torch.Tensor):
            if value.is_nested:
                total += sum(item.numel() * item.element_size() for item in value.unbind())
            else:
                total += value.numel() * value.element_size()
    return total


@ray.remote(num_cpus=1)
class ReaderActor:
    def __init__(self, tq_config: Any):
        self.client = TransferQueueClient(client_id="reader", controller_info=tq_config.controller_info)
        self.client.initialize_storage_manager(manager_type="AsyncSimpleStorageManager", config=tq_config)
        self.pending_metadata: BatchMeta | None = None
        self.pending_partition_id: str | None = None

    def accept_metadata(self, metadata: BatchMeta, partition_id: str) -> dict[str, Any]:
        self.pending_metadata = metadata
        self.pending_partition_id = partition_id
        return {
            "metadata_samples": metadata.size,
            "field_names": metadata.field_names,
        }

    def read_pending(self) -> dict[str, Any]:
        if self.pending_metadata is None:
            raise RuntimeError("No pending BatchMeta on reader. Call accept_metadata first.")

        start = time.perf_counter()
        batch = self.client.get_data(self.pending_metadata)
        read_seconds = time.perf_counter() - start
        retrieved_payload_bytes = tensor_payload_nbytes(batch)

        shape_summary = {key: list(value.shape) for key, value in batch.items() if isinstance(value, torch.Tensor)}

        del batch
        gc.collect()

        return {
            "read_seconds": read_seconds,
            "retrieved_payload_bytes": retrieved_payload_bytes,
            "shape_summary": shape_summary,
            "partition_id": self.pending_partition_id,
        }

    def clear_pending_partition(self) -> None:
        if self.pending_partition_id is None:
            return
        self.client.clear_partition(self.pending_partition_id)
        self.pending_metadata = None
        self.pending_partition_id = None
        gc.collect()


@ray.remote(num_cpus=1)
class WriterActor:
    def __init__(self, tq_config: Any, num_chunks: int, fill_value: float = 1.0):
        self.client = TransferQueueClient(client_id="writer", controller_info=tq_config.controller_info)
        self.client.initialize_storage_manager(manager_type="AsyncSimpleStorageManager", config=tq_config)
        self.num_chunks = num_chunks
        self.fill_value = fill_value

    def _create_payload(self, payload_bytes: int) -> tuple[TensorDict, int, int]:
        batch_size, elems_per_chunk = compute_chunk_layout(payload_bytes, self.num_chunks)
        tensor = torch.full((batch_size, elems_per_chunk), self.fill_value, dtype=DTYPE)
        batch = TensorDict({"payload": tensor}, batch_size=(batch_size,))
        return batch, batch_size, elems_per_chunk

    def run_single(self, reader: Any, payload_bytes: int, partition_id: str) -> dict[str, Any]:
        create_start = time.perf_counter()
        batch, batch_size, elems_per_chunk = self._create_payload(payload_bytes)
        create_seconds = time.perf_counter() - create_start
        actual_payload_bytes = tensor_payload_nbytes(batch)

        put_start = time.perf_counter()
        metadata = self.client.put(data=batch, partition_id=partition_id)
        put_seconds = time.perf_counter() - put_start

        metadata_bytes = len(cloudpickle.dumps(metadata))

        del batch
        gc.collect()

        meta_transfer_start = time.perf_counter()
        ack = ray.get(reader.accept_metadata.remote(metadata, partition_id))
        meta_transfer_seconds = time.perf_counter() - meta_transfer_start

        read_summary = ray.get(reader.read_pending.remote())
        ray.get(reader.clear_pending_partition.remote())

        return {
            "partition_id": partition_id,
            "batch_size": batch_size,
            "chunk_num_elements": elems_per_chunk,
            "num_chunks": batch_size,
            "payload_bytes": actual_payload_bytes,
            "create_seconds": create_seconds,
            "put_seconds": put_seconds,
            "metadata_ray_bytes": metadata_bytes,
            "metadata_transfer_seconds": meta_transfer_seconds,
            "metadata_samples": ack["metadata_samples"],
            "read_seconds": read_summary["read_seconds"],
            "retrieved_payload_bytes": read_summary["retrieved_payload_bytes"],
            "shape_summary": read_summary["shape_summary"],
        }


def create_storage_units(target_ip: str, num_shards: int, storage_unit_size: int) -> dict[int, Any]:
    storage_units = {}
    for rank in range(num_shards):
        storage_units[rank] = SimpleStorageUnit.options(
            num_cpus=1,
            resources={f"node:{target_ip}": 0.001},
            runtime_env={"env_vars": {"OMP_NUM_THREADS": "2"}},
        ).remote(storage_unit_size=storage_unit_size)
    return storage_units


def summarize_result(result: dict[str, Any]) -> dict[str, Any]:
    payload_bytes = result["payload_bytes"]
    metadata_bytes = result["metadata_ray_bytes"]
    total_seconds = (
        result["put_seconds"] + result["metadata_transfer_seconds"] + result["read_seconds"]
    )
    return {
        **result,
        "payload_human": format_bytes(payload_bytes),
        "metadata_ray_human": format_bytes(metadata_bytes),
        "metadata_to_payload_ratio": metadata_bytes / payload_bytes if payload_bytes else 0.0,
        "put_gbps": bytes_to_gbps(payload_bytes, result["put_seconds"]),
        "read_gbps": bytes_to_gbps(payload_bytes, result["read_seconds"]),
        "metadata_transfer_mbps": (metadata_bytes / MB) / result["metadata_transfer_seconds"]
        if result["metadata_transfer_seconds"] > 0
        else 0.0,
        "total_seconds": total_seconds,
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

        rows.append(
            {
                "payload_bytes": result["payload_bytes"],
                "payload_human": result["payload_human"],
                "round": result["round"],
                "status": "ok",
                "num_chunks": result["num_chunks"],
                "shards": result["shards"],
                "create_seconds": result["create_seconds"],
                "put_seconds": result["put_seconds"],
                "metadata_transfer_seconds": result["metadata_transfer_seconds"],
                "read_seconds": result["read_seconds"],
                "total_seconds": result["total_seconds"],
                "put_gbps": result["put_gbps"],
                "read_gbps": result["read_gbps"],
                "metadata_ray_bytes": result["metadata_ray_bytes"],
                "metadata_ray_human": result["metadata_ray_human"],
                "metadata_to_payload_ratio": result["metadata_to_payload_ratio"],
                "chunk_num_elements": result["chunk_num_elements"],
                "batch_size": result["batch_size"],
                "partition_id": result["partition_id"],
                "error": "",
            }
        )
    return rows


def write_summary_csv(csv_path: str, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "payload_bytes",
        "payload_human",
        "round",
        "status",
        "num_chunks",
        "shards",
        "create_seconds",
        "put_seconds",
        "metadata_transfer_seconds",
        "read_seconds",
        "total_seconds",
        "put_gbps",
        "read_gbps",
        "metadata_ray_bytes",
        "metadata_ray_human",
        "metadata_to_payload_ratio",
        "chunk_num_elements",
        "batch_size",
        "partition_id",
        "error",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dual-node TransferQueue benchmark: writer put -> Ray BatchMeta transfer -> reader get_data"
    )
    parser.add_argument("--writer-ip", type=str, required=True, help="Node IP for WriterActor (machine A)")
    parser.add_argument("--storage-ip", type=str, required=True, help="Node IP for SimpleStorageUnit actors (machine B)")
    parser.add_argument(
        "--reader-ip",
        type=str,
        default=None,
        help="Node IP for ReaderActor. Defaults to --storage-ip so read happens on machine B.",
    )
    parser.add_argument(
        "--controller-ip",
        type=str,
        default=None,
        help="Node IP for TransferQueueController. Defaults to --writer-ip.",
    )
    parser.add_argument("--shards", type=int, default=8, help="Number of SimpleStorageUnit actors on storage node")
    parser.add_argument(
        "--chunks",
        type=int,
        default=None,
        help="Split each total payload into this many equal chunks/samples. Defaults to --shards.",
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
    parser.add_argument(
        "--output",
        type=str,
        default="dual_node_meta_benchmark.json",
        help="Output JSON path",
    )
    parser.add_argument(
        "--summary-csv",
        type=str,
        default=None,
        help="Optional CSV summary path. Defaults to <output_basename>.csv",
    )
    parser.add_argument(
        "--fill-value",
        type=float,
        default=1.0,
        help="Constant value used to materialize the payload tensor",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop the sweep immediately on the first failing size",
    )

    args = parser.parse_args()

    reader_ip = args.reader_ip or args.storage_ip
    controller_ip = args.controller_ip or args.writer_ip
    num_chunks = args.chunks or args.shards
    summary_csv = args.summary_csv or str(Path(args.output).with_suffix(".csv"))
    sweep_sizes = parse_size_list_mb(args.size_list_mb) if args.size_list_mb else build_size_sweep(
        start_mb=args.start_mb,
        end_gb=args.end_gb,
        multiplier=args.multiplier,
    )
    max_payload_bytes = max(sweep_sizes)
    max_batch_size, _ = compute_chunk_layout(max_payload_bytes, num_chunks)
    storage_unit_size = max(1, (max_batch_size + args.shards - 1) // args.shards)

    cwd = os.getcwd()
    if not ray.is_initialized():
        ray.init(address="auto", runtime_env={"working_dir": cwd})

    logger.info(
        "Benchmark topology: writer=%s controller=%s storage=%s reader=%s shards=%s",
        args.writer_ip,
        controller_ip,
        args.storage_ip,
        reader_ip,
        args.shards,
    )
    logger.info(
        "Sweep sizes: %s",
        ", ".join(format_bytes(size) for size in sweep_sizes),
    )
    logger.info(
        "Chunk mode: payload split into %s equal samples, per-shard storage capacity: %s samples",
        num_chunks,
        storage_unit_size,
    )

    controller = None
    storage_units = None
    writer = None
    reader = None
    results = []

    try:
        controller = TransferQueueController.options(resources={f"node:{controller_ip}": 0.001}).remote()
        storage_units = create_storage_units(args.storage_ip, args.shards, storage_unit_size)
        controller_info = process_zmq_server_info(controller)
        storage_unit_infos = process_zmq_server_info(storage_units)
        tq_config = build_tq_config(controller_info, storage_unit_infos, args.shards)

        writer = WriterActor.options(
            resources={f"node:{args.writer_ip}": 0.001},
            runtime_env={"env_vars": {"OMP_NUM_THREADS": "2"}},
        ).remote(tq_config, num_chunks, args.fill_value)
        reader = ReaderActor.options(
            resources={f"node:{reader_ip}": 0.001},
            runtime_env={"env_vars": {"OMP_NUM_THREADS": "2"}},
        ).remote(tq_config)

        for payload_bytes in sweep_sizes:
            for round_idx in range(args.rounds):
                partition_id = f"dual_node_{payload_bytes}_{round_idx}"
                logger.info(
                    "Running payload=%s round=%s/%s partition=%s",
                    format_bytes(payload_bytes),
                    round_idx + 1,
                    args.rounds,
                    partition_id,
                )
                try:
                    raw_result = ray.get(writer.run_single.remote(reader, payload_bytes, partition_id))
                    result = summarize_result(raw_result)
                    result.update(
                        {
                            "round": round_idx + 1,
                            "writer_ip": args.writer_ip,
                            "storage_ip": args.storage_ip,
                            "reader_ip": reader_ip,
                            "controller_ip": controller_ip,
                            "shards": args.shards,
                            "chunks": num_chunks,
                        }
                    )
                    results.append(result)
                    logger.info(
                        "Done payload=%s | put=%.4fs (%.2f Gbps) | meta=%.4fs (%s) | read=%.4fs (%.2f Gbps)",
                        result["payload_human"],
                        result["put_seconds"],
                        result["put_gbps"],
                        result["metadata_transfer_seconds"],
                        result["metadata_ray_human"],
                        result["read_seconds"],
                        result["read_gbps"],
                    )
                except Exception as exc:
                    error_result = {
                        "payload_bytes": payload_bytes,
                        "payload_human": format_bytes(payload_bytes),
                        "round": round_idx + 1,
                        "partition_id": partition_id,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                    results.append(error_result)
                    logger.exception("Benchmark failed for payload=%s round=%s", format_bytes(payload_bytes), round_idx + 1)
                    if args.stop_on_error:
                        raise

        with open(args.output, "w") as f:
            json.dump(
                {
                    "config": vars(args),
                    "resolved_config": {
                        "reader_ip": reader_ip,
                        "controller_ip": controller_ip,
                        "chunks": num_chunks,
                        "storage_unit_size": storage_unit_size,
                        "sweep_sizes_bytes": sweep_sizes,
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
