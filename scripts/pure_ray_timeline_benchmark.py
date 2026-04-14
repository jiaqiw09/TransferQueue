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

import io
import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import ray
from ray.util import get_node_ip_address
from tensordict import TensorDict, TensorDictBase

repo_root = Path(__file__).resolve().parent.parent
sys.path.append(str(repo_root))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MB = 1024**2
GB = 1024**3

SERIALIZE_KEYWORDS = (
    "serialize",
    "serialization",
    "pickle",
    "cloudpickle",
    "dumps",
    "dump",
    "msgpack",
)
DESERIALIZE_KEYWORDS = (
    "deserialize",
    "deserialization",
    "unpickle",
    "loads",
    "load",
)
TRANSFER_KEYWORDS = (
    "transfer",
    "object_manager",
    "pull",
    "push",
    "send",
    "receive",
    "fetch",
    "transport",
)


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


@dataclass
class DataProtoLike:
    """Small verl-style payload wrapper used to mimic rollout DataProto transport."""

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
        torch = import_torch_modules()[0]
        torch.save(batch, buffer)
        return buffer.getvalue(), self.non_tensor_batch, self.meta_info

    def __setstate__(self, state):
        batch_bytes, non_tensor_batch, meta_info = state
        buffer = io.BytesIO(batch_bytes)
        torch = import_torch_modules()[0]
        try:
            batch = torch.load(buffer, weights_only=False)
        except TypeError:
            batch = torch.load(buffer)
        self.batch = batch
        self.non_tensor_batch = non_tensor_batch
        self.meta_info = meta_info


def compute_chunk_sizes(payload_bytes: int, num_chunks: int, itemsize: int) -> tuple[int, int]:
    if num_chunks <= 0:
        raise ValueError("num_chunks must be positive")
    if payload_bytes % num_chunks != 0:
        raise ValueError(f"payload_bytes={payload_bytes} must be divisible by num_chunks={num_chunks}")
    chunk_bytes = payload_bytes // num_chunks
    if chunk_bytes % itemsize != 0:
        raise ValueError(
            f"chunk_bytes={chunk_bytes} must be divisible by dtype size {itemsize}. "
            f"Adjust payload size or num_chunks={num_chunks}."
        )
    return num_chunks, chunk_bytes // itemsize


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


def sanitize_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value)


def import_torch_modules():
    try:
        import torch
    except ImportError as exc:
        raise RuntimeError("torch is required for torch-based payload modes") from exc

    try:
        import torch_npu  # noqa: F401
    except ImportError:
        torch_npu = None
    else:
        torch_npu = sys.modules.get("torch_npu")

    return torch, torch_npu


def compute_bytes_for_payload(payload: Any) -> int:
    if isinstance(payload, np.ndarray):
        return int(payload.nbytes)

    try:
        torch, _ = import_torch_modules()
    except RuntimeError:
        torch = None

    if torch is not None and isinstance(payload, torch.Tensor):
        return int(payload.numel() * payload.element_size())

    if isinstance(payload, TensorDictBase):
        return sum(compute_bytes_for_payload(value) for _, value in payload.items())

    if isinstance(payload, DataProtoLike):
        return compute_bytes_for_payload(payload.batch)

    if isinstance(payload, dict):
        return sum(compute_bytes_for_payload(value) for value in payload.values())

    if isinstance(payload, (list, tuple)):
        return sum(compute_bytes_for_payload(value) for value in payload)

    raise TypeError(f"Unsupported payload type for size calculation: {type(payload)}")


def iter_tensor_leaves(payload: Any):
    try:
        torch, _ = import_torch_modules()
    except RuntimeError:
        torch = None

    if torch is not None and isinstance(payload, torch.Tensor):
        yield payload
        return
    if isinstance(payload, np.ndarray):
        yield payload
        return
    if isinstance(payload, TensorDictBase):
        for _, value in payload.items():
            yield from iter_tensor_leaves(value)
        return
    if isinstance(payload, DataProtoLike):
        yield from iter_tensor_leaves(payload.batch)
        return
    if isinstance(payload, dict):
        for value in payload.values():
            yield from iter_tensor_leaves(value)
        return
    if isinstance(payload, (list, tuple)):
        for value in payload:
            yield from iter_tensor_leaves(value)


def tensor_checksum(payload: Any) -> float:
    try:
        torch, _ = import_torch_modules()
    except RuntimeError:
        torch = None

    total = 0.0
    for leaf in iter_tensor_leaves(payload):
        if isinstance(leaf, np.ndarray):
            total += float(np.sum(leaf[: min(leaf.size, 1024)], dtype=np.float64))
        elif torch is not None and isinstance(leaf, torch.Tensor):
            total += float(leaf[: min(leaf.numel(), 1024)].float().sum().item())
    return total


def first_tensor_leaf(payload: Any) -> Any:
    for leaf in iter_tensor_leaves(payload):
        return leaf
    return None


def load_timeline_events(timeline_path: str) -> list[dict[str, Any]]:
    with open(timeline_path) as f:
        payload = json.load(f)
    if isinstance(payload, dict):
        if "traceEvents" in payload and isinstance(payload["traceEvents"], list):
            return payload["traceEvents"]
        if "events" in payload and isinstance(payload["events"], list):
            return payload["events"]
    if isinstance(payload, list):
        return payload
    raise ValueError(f"Unsupported timeline format in {timeline_path}")


def event_overlaps_window(event: dict[str, Any], start_us: float, end_us: float) -> bool:
    event_start = float(event.get("ts", 0.0))
    event_duration = float(event.get("dur", 0.0))
    event_end = event_start + event_duration
    return event_start <= end_us and event_end >= start_us


def flatten_event_text(event: dict[str, Any]) -> str:
    parts = [
        str(event.get("name", "")),
        str(event.get("cat", "")),
        str(event.get("ph", "")),
        json.dumps(event.get("args", {}), sort_keys=True, default=str),
    ]
    return " ".join(parts).lower()


def classify_event(event: dict[str, Any]) -> str | None:
    text = flatten_event_text(event)
    if any(keyword in text for keyword in DESERIALIZE_KEYWORDS):
        return "deserialize"
    if any(keyword in text for keyword in SERIALIZE_KEYWORDS):
        return "serialize"
    if any(keyword in text for keyword in TRANSFER_KEYWORDS):
        return "transfer"
    return None


def dump_object_transfer_timeline(timeline_path: str) -> None:
    if hasattr(ray, "object_transfer_timeline"):
        ray.object_transfer_timeline(filename=timeline_path)
        return

    import ray._private.state as ray_state

    if hasattr(ray_state, "object_transfer_timeline"):
        ray_state.object_transfer_timeline(filename=timeline_path)
        return

    raise RuntimeError("Current Ray version does not expose object_transfer_timeline(...)")


def summarize_object_transfer_trace(timeline_path: str) -> dict[str, Any]:
    events = load_timeline_events(timeline_path)
    transfer_send = []
    transfer_receive = []
    receive_pull_request = []
    other = []

    for event in events:
        name = str(event.get("name", ""))
        duration_us = float(event.get("dur", 0.0))
        if name == "transfer_send":
            transfer_send.append(duration_us)
        elif name == "transfer_receive":
            transfer_receive.append(duration_us)
        elif name == "receive_pull_request":
            receive_pull_request.append(duration_us)
        else:
            other.append(duration_us)

    def pack(values: list[float]) -> dict[str, Any]:
        return {
            "event_count": len(values),
            "total_us": sum(values),
            "total_ms": sum(values) / 1000.0,
        }

    return {
        "total_events": len(events),
        "transfer_send": pack(transfer_send),
        "transfer_receive": pack(transfer_receive),
        "receive_pull_request": pack(receive_pull_request),
        "other": pack(other),
    }


def warn_if_timeline_env_missing() -> None:
    profiling = os.getenv("RAY_PROFILING")
    report_interval = os.getenv("RAY_task_events_report_interval_ms")
    if profiling != "1" or report_interval != "0":
        logger.warning(
            "Timeline usually needs RAY_PROFILING=1 and RAY_task_events_report_interval_ms=0 "
            "to be set before starting Ray on every node."
        )


def empty_transfer_summary() -> dict[str, Any]:
    return {
        "total_events": 0,
        "transfer_send": {"event_count": 0, "total_us": 0.0, "total_ms": 0.0},
        "transfer_receive": {"event_count": 0, "total_us": 0.0, "total_ms": 0.0},
        "receive_pull_request": {"event_count": 0, "total_us": 0.0, "total_ms": 0.0},
        "other": {"event_count": 0, "total_us": 0.0, "total_ms": 0.0},
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

        timeline_summary = result["timeline_summary"]
        rows.append(
            {
                "payload_bytes": result["payload_bytes"],
                "payload_human": result["payload_human"],
                "round": result["round"],
                "status": "ok",
                "num_chunks": result["num_chunks"],
                "writer_create_seconds": result["writer_create_seconds"],
                "writer_put_seconds": result["writer_put_seconds"],
                "reader_consume_seconds": result["reader_consume_seconds"],
                "end_to_end_seconds": result["end_to_end_seconds"],
                "end_to_end_gbps": result["end_to_end_gbps"],
                "writer_payload_bytes": result["writer_payload_bytes"],
                "writer_node_ip": result.get("writer_node_ip", ""),
                "reader_node_ip": result.get("reader_node_ip", ""),
                "writer_device": result["writer_device"],
                "reader_device": result["reader_device"],
                "transfer_send_ms": timeline_summary["transfer_send"]["total_ms"],
                "transfer_receive_ms": timeline_summary["transfer_receive"]["total_ms"],
                "receive_pull_request_ms": timeline_summary["receive_pull_request"]["total_ms"],
                "transfer_send_events": timeline_summary["transfer_send"]["event_count"],
                "transfer_receive_events": timeline_summary["transfer_receive"]["event_count"],
                "receive_pull_request_events": timeline_summary["receive_pull_request"]["event_count"],
                "object_transfer_timeline_file": result["object_transfer_timeline_file"],
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
        "writer_create_seconds",
        "writer_put_seconds",
        "reader_consume_seconds",
        "end_to_end_seconds",
        "end_to_end_gbps",
        "writer_payload_bytes",
        "writer_node_ip",
        "reader_node_ip",
        "writer_device",
        "reader_device",
        "transfer_send_ms",
        "transfer_receive_ms",
        "receive_pull_request_ms",
        "transfer_send_events",
        "transfer_receive_events",
        "receive_pull_request_events",
        "object_transfer_timeline_file",
        "error",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


@ray.remote(num_cpus=1)
class WriterActor:
    def __init__(
        self,
        payload_kind: str,
        dtype: str,
        fill_value: float,
        tensor_transport: str | None,
        npu_device: int,
        num_chunks: int,
    ):
        self.payload_kind = payload_kind
        self.dtype_name = dtype
        self.fill_value = fill_value
        self.tensor_transport = tensor_transport
        self.npu_device = npu_device
        self.num_chunks = num_chunks
        self.numpy_dtype = np.dtype(dtype) if payload_kind == "cpu-numpy" else None
        self.torch = None

        if payload_kind in ("cpu-torch", "npu-torch", "verl-dataproto"):
            self.torch, _ = import_torch_modules()
            if payload_kind == "npu-torch":
                if not hasattr(self.torch, "npu") or not self.torch.npu.is_available():
                    raise RuntimeError("payload-kind=npu-torch requires torch.npu to be available")
                self.torch.npu.set_device(npu_device)

    def get_runtime_info(self) -> dict[str, Any]:
        return {
            "node_ip": get_node_ip_address(),
        }

    def _create_payload(self, payload_bytes: int):
        if self.payload_kind == "verl-dataproto":
            assert self.torch is not None
            torch = self.torch
            torch_dtype = getattr(torch, self.dtype_name, None)
            if torch_dtype is None:
                raise ValueError(f"Unsupported torch dtype: {self.dtype_name}")
            itemsize = torch.tensor([], dtype=torch_dtype).element_size()
            num_chunks, chunk_num_elements = compute_chunk_sizes(payload_bytes, self.num_chunks, itemsize)

            response_ids = torch.full(
                (num_chunks, chunk_num_elements),
                self.fill_value,
                dtype=torch_dtype,
                device="cpu",
            )
            batch = TensorDict({"response_ids": response_ids}, batch_size=(num_chunks,))
            non_tensor_batch = {
                "raw_prompt": [f"prompt_{idx}" for idx in range(num_chunks)],
                "sample_ids": list(range(num_chunks)),
            }
            meta_info = {
                "kind": "verl-roll",
                "sample_count": num_chunks,
                "chunk_num_elements": chunk_num_elements,
            }
            payload = DataProtoLike(batch=batch, non_tensor_batch=non_tensor_batch, meta_info=meta_info)
            return payload, int(chunk_num_elements), str(torch_dtype), [chunk_num_elements], str(response_ids.device)

        if self.payload_kind == "cpu-numpy":
            assert self.numpy_dtype is not None
            num_chunks, chunk_num_elements = compute_chunk_sizes(payload_bytes, self.num_chunks, self.numpy_dtype.itemsize)
            payloads = [
                np.full((chunk_num_elements,), self.fill_value, dtype=self.numpy_dtype) for _ in range(num_chunks)
            ]
            return payloads, int(chunk_num_elements), str(self.numpy_dtype), [chunk_num_elements], "cpu"

        assert self.torch is not None
        torch = self.torch
        torch_dtype = getattr(torch, self.dtype_name, None)
        if torch_dtype is None:
            raise ValueError(f"Unsupported torch dtype: {self.dtype_name}")
        itemsize = torch.tensor([], dtype=torch_dtype).element_size()
        num_chunks, chunk_num_elements = compute_chunk_sizes(payload_bytes, self.num_chunks, itemsize)

        device = "cpu"
        if self.payload_kind == "npu-torch":
            device = f"npu:{self.npu_device}"
        payloads = [
            torch.full((chunk_num_elements,), self.fill_value, dtype=torch_dtype, device=device) for _ in range(num_chunks)
        ]
        return payloads, int(chunk_num_elements), str(torch_dtype), [chunk_num_elements], str(payloads[0].device)

    def create_payload_and_put(self, payload_bytes: int) -> dict[str, Any]:
        create_start = time.perf_counter()
        payloads, chunk_num_elements, dtype_name, shape, device = self._create_payload(payload_bytes)
        create_seconds = time.perf_counter() - create_start

        put_start = time.perf_counter()
        if self.payload_kind == "verl-dataproto":
            payload = payloads
            object_ref = ray.put(payload)
            object_refs = [object_ref]
            payload_sample_count = int(payload.meta_info.get("sample_count", 1))
        else:
            object_refs = []
            for payload in payloads:
                if (
                    self.payload_kind == "npu-torch"
                    and self.tensor_transport
                    and self.torch is not None
                    and isinstance(payload, self.torch.Tensor)
                ):
                    object_ref = ray.put(payload, _tensor_transport=self.tensor_transport)
                else:
                    object_ref = ray.put(payload)
                object_refs.append(object_ref)
            payload_sample_count = len(object_refs)
        put_seconds = time.perf_counter() - put_start
        payload_bytes_actual = compute_bytes_for_payload(payloads)

        return {
            "object_refs": object_refs,
            "object_ref": object_refs[0] if len(object_refs) == 1 else None,
            "payload_bytes": payload_bytes_actual,
            "chunk_num_elements": chunk_num_elements,
            "num_chunks": payload_sample_count,
            "dtype": dtype_name,
            "shape": shape,
            "device": device,
            "create_seconds": create_seconds,
            "put_seconds": put_seconds,
        }


@ray.remote(num_cpus=1)
class ReaderActor:
    def __init__(self, payload_kind: str, npu_device: int):
        self.payload_kind = payload_kind
        self.npu_device = npu_device
        self.torch = None
        if payload_kind in ("cpu-torch", "npu-torch"):
            self.torch, _ = import_torch_modules()
            if payload_kind == "npu-torch":
                if not hasattr(self.torch, "npu") or not self.torch.npu.is_available():
                    raise RuntimeError("payload-kind=npu-torch requires torch.npu to be available")
                self.torch.npu.set_device(npu_device)

    def get_runtime_info(self) -> dict[str, Any]:
        return {
            "node_ip": get_node_ip_address(),
        }

    def consume_object_ref(self, payload_packet: dict[str, Any]) -> dict[str, Any]:
        receive_start = time.perf_counter()
        if "object_ref" in payload_packet and payload_packet["object_ref"] is not None:
            payload = ray.get(payload_packet["object_ref"])
            payloads = [payload.batch] if isinstance(payload, DataProtoLike) else [payload]
        else:
            payloads = ray.get(payload_packet["object_refs"])
        receive_seconds = time.perf_counter() - receive_start

        first_payload = payloads[0]
        if isinstance(first_payload, np.ndarray):
            checksum = float(sum(np.sum(payload[: min(payload.size, 1024)], dtype=np.float64) for payload in payloads))
            shape = list(first_payload.shape)
            dtype = str(first_payload.dtype)
            device = "cpu"
        elif isinstance(first_payload, TensorDictBase):
            checksum = tensor_checksum(first_payload)
            tensor_leaf = first_tensor_leaf(first_payload)
            if tensor_leaf is None:
                raise RuntimeError("TensorDict payload did not contain any tensor leaves")
            shape = list(tensor_leaf.shape)
            dtype = str(tensor_leaf.dtype)
            device = str(tensor_leaf.device)
        else:
            assert self.torch is not None
            torch = self.torch
            if not isinstance(first_payload, torch.Tensor):
                raise TypeError(f"Unexpected payload type: {type(first_payload)}")
            checksum = float(sum(payload[: min(payload.numel(), 1024)].float().sum().item() for payload in payloads))
            shape = list(first_payload.shape)
            dtype = str(first_payload.dtype)
            device = str(first_payload.device)

        return {
            "payload_bytes": sum(compute_bytes_for_payload(payload) for payload in payloads),
            "checksum": checksum,
            "shape": shape,
            "dtype": dtype,
            "device": device,
            "consume_seconds": receive_seconds,
        }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pure Ray dual-node transfer benchmark with object transfer tracing"
    )
    parser.add_argument("--writer-ip", type=str, required=True, help="Node IP for WriterActor (machine A)")
    parser.add_argument("--reader-ip", type=str, required=True, help="Node IP for ReaderActor (machine B)")
    parser.add_argument("--chunks", type=int, default=8, help="Split each total payload into this many equal chunks")
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
        "--payload-kind",
        type=str,
        default="cpu-torch",
        choices=["cpu-numpy", "cpu-torch", "npu-torch", "verl-dataproto"],
        help="Payload materialization mode. Default is cpu-torch for CPU-path benchmarking.",
    )
    parser.add_argument(
        "--dtype",
        type=str,
        default="float32",
        help="Payload dtype. cpu-numpy uses NumPy dtype names; torch modes use torch dtype names.",
    )
    parser.add_argument("--fill-value", type=float, default=1.0, help="Constant used to materialize the payload")
    parser.add_argument(
        "--tensor-transport",
        type=str,
        default="nixl",
        help="Tensor transport passed to ray.put(..., _tensor_transport=...). Only used for npu-torch.",
    )
    parser.add_argument("--writer-npu-device", type=int, default=0, help="NPU device id on writer node")
    parser.add_argument("--reader-npu-device", type=int, default=0, help="NPU device id on reader node")
    parser.add_argument(
        "--timeline-dir",
        type=str,
        default=None,
        help="Optional directory to store raw Ray object transfer trace dumps. If omitted, no trace is dumped.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="pure_ray_timeline_benchmark.json",
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

    warn_if_timeline_env_missing()

    sweep_sizes = parse_size_list_mb(args.size_list_mb) if args.size_list_mb else build_size_sweep(
        start_mb=args.start_mb,
        end_gb=args.end_gb,
        multiplier=args.multiplier,
    )
    timeline_dir = Path(args.timeline_dir) if args.timeline_dir else None
    if timeline_dir is not None:
        timeline_dir.mkdir(parents=True, exist_ok=True)
    summary_csv = args.summary_csv or str(Path(args.output).with_suffix(".csv"))

    cwd = os.getcwd()
    if not ray.is_initialized():
        ray.init(address="auto", runtime_env={"working_dir": cwd})

    logger.info(
        "Benchmark topology: writer=%s reader=%s",
        args.writer_ip,
        args.reader_ip,
    )
    logger.info(
        "Sweep sizes: %s",
        ", ".join(format_bytes(size) for size in sweep_sizes),
    )

    writer = None
    reader = None
    results = []

    try:
        writer = WriterActor.options(
            resources={f"node:{args.writer_ip}": 0.001},
            runtime_env={"env_vars": {"OMP_NUM_THREADS": "2"}},
        ).remote(
            args.payload_kind,
            args.dtype,
            args.fill_value,
            args.tensor_transport,
            args.writer_npu_device,
            args.chunks,
        )
        reader = ReaderActor.options(
            resources={f"node:{args.reader_ip}": 0.001},
            runtime_env={"env_vars": {"OMP_NUM_THREADS": "2"}},
        ).remote(args.payload_kind, args.reader_npu_device)
        writer_runtime = ray.get(writer.get_runtime_info.remote())
        reader_runtime = ray.get(reader.get_runtime_info.remote())
        logger.info(
            "Resolved runtime nodes: writer_node_ip=%s reader_node_ip=%s",
            writer_runtime["node_ip"],
            reader_runtime["node_ip"],
        )

        for payload_bytes in sweep_sizes:
            for round_idx in range(args.rounds):
                payload_human = format_bytes(payload_bytes)
                logger.info(
                    "Running payload=%s round=%s/%s",
                    payload_human,
                    round_idx + 1,
                    args.rounds,
                )

                try:
                    produce_start = time.perf_counter()
                    payload_ref = writer.create_payload_and_put.remote(payload_bytes)
                    consume_ref = reader.consume_object_ref.remote(payload_ref)
                    consume_summary = ray.get(consume_ref)
                    end_to_end_seconds = time.perf_counter() - produce_start

                    payload_packet = ray.get(payload_ref)

                    timeline_path = None
                    if timeline_dir is not None:
                        timeline_filename = (
                            f"object_transfer_timeline_{sanitize_name(payload_human)}_round{round_idx + 1}.json"
                        )
                        timeline_path = timeline_dir / timeline_filename
                        dump_object_transfer_timeline(str(timeline_path))
                        timeline_summary = summarize_object_transfer_trace(str(timeline_path))
                    else:
                        timeline_summary = empty_transfer_summary()

                    result = {
                        "round": round_idx + 1,
                        "writer_ip": args.writer_ip,
                        "reader_ip": args.reader_ip,
                        "payload_bytes": payload_bytes,
                        "payload_human": payload_human,
                        "object_transfer_timeline_file": str(timeline_path) if timeline_path is not None else "",
                        "end_to_end_seconds": end_to_end_seconds,
                        "end_to_end_gbps": bytes_to_gbps(payload_bytes, end_to_end_seconds),
                        "writer_node_ip": writer_runtime["node_ip"],
                        "reader_node_ip": reader_runtime["node_ip"],
                        "writer_device": payload_packet["device"],
                        "writer_dtype": payload_packet["dtype"],
                        "writer_shape": payload_packet["shape"],
                        "writer_chunk_num_elements": payload_packet["chunk_num_elements"],
                        "num_chunks": payload_packet["num_chunks"],
                        "writer_create_seconds": payload_packet["create_seconds"],
                        "writer_put_seconds": payload_packet["put_seconds"],
                        "writer_payload_bytes": payload_packet["payload_bytes"],
                        "reader_payload_bytes": consume_summary["payload_bytes"],
                        "reader_checksum": consume_summary["checksum"],
                        "reader_shape": consume_summary["shape"],
                        "reader_dtype": consume_summary["dtype"],
                        "reader_device": consume_summary["device"],
                        "reader_consume_seconds": consume_summary["consume_seconds"],
                        "timeline_summary": timeline_summary,
                    }
                    results.append(result)

                    logger.info(
                        "Done payload=%s | e2e=%.4fs (%.2f Gbps) | writer_node=%s reader_node=%s | send=%.2fms receive=%.2fms pull=%.2fms",
                        payload_human,
                        result["end_to_end_seconds"],
                        result["end_to_end_gbps"],
                        result["writer_node_ip"],
                        result["reader_node_ip"],
                        timeline_summary["transfer_send"]["total_ms"],
                        timeline_summary["transfer_receive"]["total_ms"],
                        timeline_summary["receive_pull_request"]["total_ms"],
                    )
                except Exception as exc:
                    error_result = {
                        "round": round_idx + 1,
                        "payload_bytes": payload_bytes,
                        "payload_human": payload_human,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                    results.append(error_result)
                    logger.exception("Benchmark failed for payload=%s round=%s", payload_human, round_idx + 1)
                    if args.stop_on_error:
                        raise

        with open(args.output, "w") as f:
            json.dump(
                {
                    "config": vars(args),
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
