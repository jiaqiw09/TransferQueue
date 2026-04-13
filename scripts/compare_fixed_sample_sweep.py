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
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_sample_count_list(value: str) -> list[int]:
    counts = []
    for chunk in value.split(","):
        stripped = chunk.strip()
        if not stripped:
            continue
        count = int(stripped)
        if count <= 0:
            raise ValueError("sample counts must be positive")
        counts.append(count)
    if not counts:
        raise ValueError("sample-count-list did not contain any valid counts")
    return counts


def default_sample_counts() -> list[int]:
    return [1, 2, 4, 8, 16, 32, 64, 128]


def format_mb(mb: int) -> str:
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    return f"{mb:.2f} MB"


def run_command(cmd: list[str], cwd: Path) -> None:
    logger.info("Running: %s", " ".join(cmd))
    completed = subprocess.run(cmd, cwd=str(cwd), check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}: {' '.join(cmd)}")


def load_json(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


def build_compare_row(
    sample_count: int,
    sample_size_mb: int,
    round_idx: int,
    tq_result: dict[str, Any],
    ray_result: dict[str, Any],
    tq_json: Path,
    ray_json: Path,
) -> dict[str, Any]:
    payload_mb = sample_count * sample_size_mb
    payload_bytes = tq_result["payload_bytes"] if "payload_bytes" in tq_result else ray_result["payload_bytes"]
    return {
        "sample_count": sample_count,
        "sample_size_mb": sample_size_mb,
        "payload_mb": payload_mb,
        "payload_human": format_mb(payload_mb),
        "round": round_idx,
        "payload_bytes": payload_bytes,
        "tq_create_seconds": tq_result.get("create_seconds"),
        "tq_put_seconds": tq_result.get("put_seconds"),
        "tq_metadata_transfer_seconds": tq_result.get("metadata_transfer_seconds"),
        "tq_read_seconds": tq_result.get("read_seconds"),
        "tq_total_seconds": tq_result.get("total_seconds"),
        "tq_put_gbps": tq_result.get("put_gbps"),
        "tq_read_gbps": tq_result.get("read_gbps"),
        "tq_metadata_ray_bytes": tq_result.get("metadata_ray_bytes"),
        "ray_create_seconds": ray_result.get("writer_create_seconds"),
        "ray_put_seconds": ray_result.get("writer_put_seconds"),
        "ray_read_seconds": ray_result.get("reader_consume_seconds"),
        "ray_total_seconds": ray_result.get("end_to_end_seconds"),
        "ray_end_to_end_gbps": ray_result.get("end_to_end_gbps"),
        "ray_transfer_send_ms": ray_result.get("timeline_summary", {}).get("transfer_send", {}).get("total_ms"),
        "ray_transfer_receive_ms": ray_result.get("timeline_summary", {}).get("transfer_receive", {}).get("total_ms"),
        "ray_receive_pull_request_ms": ray_result.get("timeline_summary", {})
        .get("receive_pull_request", {})
        .get("total_ms"),
        "ray_writer_node_ip": ray_result.get("writer_node_ip"),
        "ray_reader_node_ip": ray_result.get("reader_node_ip"),
        "ray_object_transfer_timeline_file": ray_result.get("object_transfer_timeline_file"),
        "tq_result_json": str(tq_json),
        "ray_result_json": str(ray_json),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "sample_count",
        "sample_size_mb",
        "payload_mb",
        "payload_human",
        "round",
        "payload_bytes",
        "tq_create_seconds",
        "tq_put_seconds",
        "tq_metadata_transfer_seconds",
        "tq_read_seconds",
        "tq_total_seconds",
        "tq_put_gbps",
        "tq_read_gbps",
        "tq_metadata_ray_bytes",
        "ray_create_seconds",
        "ray_put_seconds",
        "ray_read_seconds",
        "ray_total_seconds",
        "ray_end_to_end_gbps",
        "ray_transfer_send_ms",
        "ray_transfer_receive_ms",
        "ray_receive_pull_request_ms",
        "ray_writer_node_ip",
        "ray_reader_node_ip",
        "ray_object_transfer_timeline_file",
        "tq_result_json",
        "ray_result_json",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run TQ and pure Ray benchmarks with fixed sample size and compare them in a single table"
    )
    parser.add_argument("--writer-ip", type=str, required=True, help="Writer node IP")
    parser.add_argument("--storage-ip", type=str, required=True, help="TQ storage node IP")
    parser.add_argument("--reader-ip", type=str, required=True, help="Reader node IP")
    parser.add_argument("--controller-ip", type=str, default=None, help="TQ controller node IP, defaults to writer-ip")
    parser.add_argument("--sample-size-mb", type=int, default=256, help="Fixed sample/chunk size in MB")
    parser.add_argument(
        "--sample-count-list",
        type=str,
        default=None,
        help="Comma-separated sample counts, e.g. 1,2,4,8,16,32,64,128. Defaults to powers of two up to 128.",
    )
    parser.add_argument("--tq-shards", type=int, default=8, help="Number of TQ SimpleStorageUnit shards")
    parser.add_argument("--rounds", type=int, default=1, help="Rounds per sample count")
    parser.add_argument(
        "--ray-payload-kind",
        type=str,
        default="cpu-torch",
        choices=["cpu-numpy", "cpu-torch", "npu-torch"],
        help="Payload mode for pure Ray benchmark",
    )
    parser.add_argument(
        "--ray-timeline-dir",
        type=str,
        default="ray_object_transfer_compare_outputs",
        help="Directory for pure Ray object transfer traces",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=str,
        default="compare_fixed_sample_artifacts",
        help="Directory to store per-run raw JSON/CSV outputs",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="compare_fixed_sample_sweep.json",
        help="Combined comparison JSON output path",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default="compare_fixed_sample_sweep.csv",
        help="Combined comparison CSV output path",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    scripts_dir = repo_root / "scripts"
    controller_ip = args.controller_ip or args.writer_ip
    sample_counts = (
        parse_sample_count_list(args.sample_count_list) if args.sample_count_list else default_sample_counts()
    )

    artifacts_dir = Path(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    ray_timeline_dir = Path(args.ray_timeline_dir)
    ray_timeline_dir.mkdir(parents=True, exist_ok=True)

    compare_rows = []
    raw_results = []

    for sample_count in sample_counts:
        total_mb = sample_count * args.sample_size_mb
        logger.info(
            "Running comparison for sample_count=%s sample_size=%sMB total_payload=%s",
            sample_count,
            args.sample_size_mb,
            format_mb(total_mb),
        )

        tq_output_json = artifacts_dir / f"tq_samples_{sample_count}.json"
        tq_output_csv = artifacts_dir / f"tq_samples_{sample_count}.csv"
        ray_output_json = artifacts_dir / f"ray_samples_{sample_count}.json"
        ray_output_csv = artifacts_dir / f"ray_samples_{sample_count}.csv"
        per_count_timeline_dir = ray_timeline_dir / f"samples_{sample_count}"
        per_count_timeline_dir.mkdir(parents=True, exist_ok=True)

        tq_cmd = [
            sys.executable,
            str(scripts_dir / "dual_node_meta_benchmark.py"),
            "--writer-ip",
            args.writer_ip,
            "--storage-ip",
            args.storage_ip,
            "--reader-ip",
            args.reader_ip,
            "--controller-ip",
            controller_ip,
            "--size-list-mb",
            str(total_mb),
            "--shards",
            str(args.tq_shards),
            "--chunks",
            str(sample_count),
            "--rounds",
            str(args.rounds),
            "--summary-csv",
            str(tq_output_csv),
            "--output",
            str(tq_output_json),
        ]
        run_command(tq_cmd, repo_root)
        tq_payload = load_json(tq_output_json)

        ray_cmd = [
            sys.executable,
            str(scripts_dir / "pure_ray_timeline_benchmark.py"),
            "--writer-ip",
            args.writer_ip,
            "--reader-ip",
            args.reader_ip,
            "--payload-kind",
            args.ray_payload_kind,
            "--size-list-mb",
            str(total_mb),
            "--chunks",
            str(sample_count),
            "--rounds",
            str(args.rounds),
            "--timeline-dir",
            str(per_count_timeline_dir),
            "--summary-csv",
            str(ray_output_csv),
            "--output",
            str(ray_output_json),
        ]
        run_command(ray_cmd, repo_root)
        ray_payload = load_json(ray_output_json)

        tq_results = tq_payload.get("results", [])
        ray_results = ray_payload.get("results", [])
        if len(tq_results) != len(ray_results):
            raise RuntimeError(
                f"Result count mismatch for sample_count={sample_count}: TQ={len(tq_results)} Ray={len(ray_results)}"
            )

        paired_results = []
        for tq_result, ray_result in zip(tq_results, ray_results, strict=True):
            if "error" in tq_result:
                raise RuntimeError(f"TQ benchmark failed for sample_count={sample_count}: {tq_result['error']}")
            if "error" in ray_result:
                raise RuntimeError(f"Ray benchmark failed for sample_count={sample_count}: {ray_result['error']}")

            round_idx = int(tq_result["round"])
            compare_row = build_compare_row(
                sample_count,
                args.sample_size_mb,
                round_idx,
                tq_result,
                ray_result,
                tq_output_json,
                ray_output_json,
            )
            compare_rows.append(compare_row)
            paired_results.append(
                {
                    "round": round_idx,
                    "tq": tq_result,
                    "ray": ray_result,
                    "compare": compare_row,
                }
            )

        raw_results.append(
            {
                "sample_count": sample_count,
                "sample_size_mb": args.sample_size_mb,
                "payload_mb": total_mb,
                "tq_json": str(tq_output_json),
                "ray_json": str(ray_output_json),
                "runs": paired_results,
            }
        )

    output_json = Path(args.output_json)
    output_csv = Path(args.output_csv)
    with open(output_json, "w") as f:
        json.dump(
            {
                "config": vars(args),
                "sample_counts": sample_counts,
                "results": raw_results,
            },
            f,
            indent=2,
        )
    write_csv(output_csv, compare_rows)
    logger.info("Comparison JSON saved to %s", output_json)
    logger.info("Comparison CSV saved to %s", output_csv)


if __name__ == "__main__":
    main()
