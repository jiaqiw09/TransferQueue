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


def build_storage_ip_list(server_a_ip: str, server_b_ip: str, shards: int) -> list[str]:
    if shards % 2 != 0:
        raise ValueError("This script expects an even shard count so storage can be split evenly across two servers.")
    half = shards // 2
    return [server_a_ip] * half + [server_b_ip] * half


def run_command(cmd: list[str], cwd: Path) -> None:
    logger.info("Running: %s", " ".join(cmd))
    completed = subprocess.run(cmd, cwd=str(cwd), check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}: {' '.join(cmd)}")


def load_json(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


def build_row(
    sample_count: int,
    sample_size_mb: int,
    round_idx: int,
    result: dict[str, Any],
    result_json: Path,
    storage_ip_list: list[str],
) -> dict[str, Any]:
    payload_mb = sample_count * sample_size_mb
    return {
        "sample_count": sample_count,
        "sample_size_mb": sample_size_mb,
        "payload_mb": payload_mb,
        "payload_human": format_mb(payload_mb),
        "round": round_idx,
        "payload_bytes": result.get("payload_bytes"),
        "chunks": result.get("chunks"),
        "shards": result.get("shards"),
        "storage_ip_list": ",".join(storage_ip_list),
        "put_seconds": result.get("put_seconds"),
        "metadata_transfer_seconds": result.get("metadata_transfer_seconds"),
        "read_seconds": result.get("read_seconds"),
        "three_stage_total_seconds": result.get("total_seconds"),
        "put_gbps": result.get("put_gbps"),
        "read_gbps": result.get("read_gbps"),
        "metadata_ray_bytes": result.get("metadata_ray_bytes"),
        "result_json": str(result_json),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "sample_count",
        "sample_size_mb",
        "payload_mb",
        "payload_human",
        "round",
        "payload_bytes",
        "chunks",
        "shards",
        "storage_ip_list",
        "put_seconds",
        "metadata_transfer_seconds",
        "read_seconds",
        "three_stage_total_seconds",
        "put_gbps",
        "read_gbps",
        "metadata_ray_bytes",
        "result_json",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TQ-only fixed-sample sweep with 8 SimpleStorageUnits split evenly across server A and server B"
    )
    parser.add_argument("--server-a-ip", type=str, required=True, help="Server A IP. Default writer/controller side.")
    parser.add_argument("--server-b-ip", type=str, required=True, help="Server B IP. Default reader side.")
    parser.add_argument("--writer-ip", type=str, default=None, help="Writer node IP. Defaults to server-a-ip.")
    parser.add_argument("--reader-ip", type=str, default=None, help="Reader node IP. Defaults to server-b-ip.")
    parser.add_argument("--controller-ip", type=str, default=None, help="Controller node IP. Defaults to writer-ip.")
    parser.add_argument("--sample-size-mb", type=int, default=256, help="Fixed sample size in MB")
    parser.add_argument(
        "--sample-count-list",
        type=str,
        default=None,
        help="Comma-separated sample counts. Defaults to 1,2,4,8,16,32,64,128.",
    )
    parser.add_argument("--shards", type=int, default=8, help="Total number of SimpleStorageUnit actors")
    parser.add_argument("--rounds", type=int, default=1, help="Rounds per sample count")
    parser.add_argument(
        "--artifacts-dir",
        type=str,
        default="tq_fixed_sample_split_storage_artifacts",
        help="Directory to store per-run raw JSON/CSV outputs",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="tq_fixed_sample_split_storage_sweep.json",
        help="Combined JSON output path",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default="tq_fixed_sample_split_storage_sweep.csv",
        help="Combined CSV output path",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    scripts_dir = repo_root / "scripts"
    writer_ip = args.writer_ip or args.server_a_ip
    reader_ip = args.reader_ip or args.server_b_ip
    controller_ip = args.controller_ip or writer_ip
    sample_counts = (
        parse_sample_count_list(args.sample_count_list) if args.sample_count_list else default_sample_counts()
    )
    storage_ip_list = build_storage_ip_list(args.server_a_ip, args.server_b_ip, args.shards)

    artifacts_dir = Path(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    raw_results = []

    for sample_count in sample_counts:
        total_mb = sample_count * args.sample_size_mb
        logger.info(
            "Running TQ sweep for sample_count=%s sample_size=%sMB total_payload=%s storage_layout=%s",
            sample_count,
            args.sample_size_mb,
            format_mb(total_mb),
            storage_ip_list,
        )

        tq_output_json = artifacts_dir / f"tq_split_storage_samples_{sample_count}.json"
        tq_output_csv = artifacts_dir / f"tq_split_storage_samples_{sample_count}.csv"
        tq_cmd = [
            sys.executable,
            str(scripts_dir / "dual_node_meta_benchmark.py"),
            "--writer-ip",
            writer_ip,
            "--storage-ip",
            args.server_b_ip,
            "--storage-ip-list",
            ",".join(storage_ip_list),
            "--reader-ip",
            reader_ip,
            "--controller-ip",
            controller_ip,
            "--size-list-mb",
            str(total_mb),
            "--shards",
            str(args.shards),
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
        tq_results = tq_payload.get("results", [])

        per_count_runs = []
        for result in tq_results:
            if "error" in result:
                raise RuntimeError(f"TQ benchmark failed for sample_count={sample_count}: {result['error']}")

            round_idx = int(result["round"])
            row = build_row(sample_count, args.sample_size_mb, round_idx, result, tq_output_json, storage_ip_list)
            rows.append(row)
            per_count_runs.append(
                {
                    "round": round_idx,
                    "result": result,
                    "summary": row,
                }
            )

        raw_results.append(
            {
                "sample_count": sample_count,
                "sample_size_mb": args.sample_size_mb,
                "payload_mb": total_mb,
                "storage_ip_list": storage_ip_list,
                "tq_json": str(tq_output_json),
                "runs": per_count_runs,
            }
        )

    output_json = Path(args.output_json)
    output_csv = Path(args.output_csv)
    with open(output_json, "w") as f:
        json.dump(
            {
                "config": vars(args),
                "resolved_config": {
                    "writer_ip": writer_ip,
                    "reader_ip": reader_ip,
                    "controller_ip": controller_ip,
                    "storage_ip_list": storage_ip_list,
                    "sample_counts": sample_counts,
                },
                "results": raw_results,
            },
            f,
            indent=2,
        )
    write_csv(output_csv, rows)
    logger.info("Combined JSON saved to %s", output_json)
    logger.info("Combined CSV saved to %s", output_csv)


if __name__ == "__main__":
    main()
