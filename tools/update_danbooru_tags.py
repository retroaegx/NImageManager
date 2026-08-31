#!/usr/bin/env python3
"""Merge the latest public Danbooru tags into NImageManager's bundled dictionaries."""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


API_ROOT = "https://danbooru.donmai.us"
USER_AGENT = "NImageManager-tag-updater/1.0 (https://github.com/retroaegx/NImageManager)"
PAGE_SIZE = 1000
SOURCE_NAME = "danbooru_api"


def fetch_json(path: str, params: dict[str, str | int], *, retries: int = 7) -> list[dict[str, Any]]:
    url = f"{API_ROOT}{path}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                payload = json.load(response)
            if not isinstance(payload, list):
                raise RuntimeError(f"Unexpected response from {path}: {type(payload).__name__}")
            return payload
        except urllib.error.HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504} or attempt + 1 >= retries:
                raise
            retry_after = error.headers.get("Retry-After")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else min(30.0, 1.5 * (2**attempt))
        except (urllib.error.URLError, TimeoutError):
            if attempt + 1 >= retries:
                raise
            delay = min(30.0, 1.5 * (2**attempt))
        print(f"retrying {path} in {delay:.1f}s", file=sys.stderr, flush=True)
        time.sleep(delay)
    raise AssertionError("unreachable")


def fetch_all_tags(min_post_count: int, delay: float) -> dict[str, dict[str, Any]]:
    tags: dict[str, dict[str, Any]] = {}
    cursor: str | None = None
    batch_number = 0
    received = 0
    while True:
        params: dict[str, str | int] = {
            "limit": PAGE_SIZE,
            "only": "id,name,post_count,category,is_deprecated",
            "search[order]": "id_desc",
            "search[post_count]": f"{min_post_count}..",
        }
        if cursor:
            params["page"] = cursor
        batch = fetch_json(
            "/tags.json",
            params,
        )
        batch_number += 1
        received += len(batch)
        for item in batch:
            name = str(item.get("name") or "").strip()
            post_count = int(item.get("post_count") or 0)
            if not name or bool(item.get("is_deprecated")) or post_count < min_post_count:
                continue
            tags[name] = {
                "category": int(item.get("category") or 0),
                "post_count": post_count,
            }
        if batch_number == 1 or batch_number % 25 == 0 or len(batch) < PAGE_SIZE:
            print(
                f"tags batch={batch_number} received={received} unique={len(tags)}",
                flush=True,
            )
        if len(batch) < PAGE_SIZE:
            break
        last_id = int(batch[-1].get("id") or 0)
        if last_id <= 0:
            raise RuntimeError("Danbooru tags cursor is missing")
        next_cursor = f"b{last_id}"
        if next_cursor == cursor:
            raise RuntimeError("Danbooru tags cursor did not advance")
        cursor = next_cursor
        time.sleep(delay)
    return tags


def fetch_all_active_aliases(delay: float) -> dict[str, str]:
    aliases: dict[str, str] = {}
    cursor: str | None = None
    batch_number = 0
    received = 0
    while True:
        params: dict[str, str | int] = {
            "limit": PAGE_SIZE,
            "only": "id,antecedent_name,consequent_name,status",
            "search[status]": "active",
            "search[order]": "id_desc",
        }
        if cursor:
            params["page"] = cursor
        batch = fetch_json(
            "/tag_aliases.json",
            params,
        )
        batch_number += 1
        received += len(batch)
        for item in batch:
            if str(item.get("status") or "") != "active":
                continue
            alias = str(item.get("antecedent_name") or "").strip()
            canonical = str(item.get("consequent_name") or "").strip()
            if alias and canonical and alias != canonical:
                aliases[alias] = canonical
        if batch_number == 1 or batch_number % 25 == 0 or len(batch) < PAGE_SIZE:
            print(
                f"aliases batch={batch_number} received={received} unique={len(aliases)}",
                flush=True,
            )
        if len(batch) < PAGE_SIZE:
            break
        last_id = int(batch[-1].get("id") or 0)
        if last_id <= 0:
            raise RuntimeError("Danbooru aliases cursor is missing")
        next_cursor = f"b{last_id}"
        if next_cursor == cursor:
            raise RuntimeError("Danbooru aliases cursor did not advance")
        cursor = next_cursor
        time.sleep(delay)
    return aliases


def split_pipe(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split("|") if part.strip()]


def load_master(path: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            tag = str(row.get("tag") or "").strip()
            if not tag:
                continue
            rows[tag] = {
                "tag": tag,
                "category": int(row.get("category") or 0),
                "post_count": int(row.get("post_count") or 0),
                "sources": split_pipe(str(row.get("sources") or "")),
                "aliases": split_pipe(str(row.get("aliases") or "")),
            }
    return rows


def load_aliases(path: Path) -> dict[str, str]:
    aliases: dict[str, str] = {}
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            alias = str(row.get("alias") or "").strip()
            canonical = str(row.get("canonical") or "").strip()
            if alias and canonical and alias != canonical:
                aliases[alias] = canonical
    return aliases


def load_exact_quality_tags(path: Path) -> set[str]:
    tags: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.reader(handle):
            if not row:
                continue
            tag = str(row[0] or "").strip()
            category = str(row[1] if len(row) > 1 else "").strip()
            if tag and "*" not in tag and category == "5":
                tags.add(tag)
    return tags


def merge_data(
    master: dict[str, dict[str, Any]],
    aliases: dict[str, str],
    danbooru_tags: dict[str, dict[str, Any]],
    danbooru_aliases: dict[str, str],
    quality_tags: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, str]], dict[str, int]]:
    before_tags = len(master)
    before_aliases = len(aliases)
    updated = 0
    added = 0

    # Preserve aliases embedded in the five-column master even if an old alias gzip omitted one.
    for canonical, row in master.items():
        for alias in row["aliases"]:
            if alias and alias != canonical:
                aliases.setdefault(alias, canonical)

    for tag, incoming in danbooru_tags.items():
        row = master.get(tag)
        if row is None:
            master[tag] = {
                "tag": tag,
                "category": incoming["category"],
                "post_count": incoming["post_count"],
                "sources": [SOURCE_NAME],
                "aliases": [],
            }
            added += 1
            continue
        old_category = int(row["category"])
        old_count = int(row["post_count"])
        row["category"] = int(incoming["category"])
        row["post_count"] = max(old_count, int(incoming["post_count"]))
        if SOURCE_NAME not in row["sources"]:
            row["sources"].append(SOURCE_NAME)
        if old_category != row["category"] or old_count != row["post_count"]:
            updated += 1

    # Danbooru is authoritative for currently active Danbooru aliases.
    aliases.update(danbooru_aliases)
    aliases = {alias: canonical for alias, canonical in aliases.items() if alias and canonical and alias != canonical}

    grouped_aliases: dict[str, set[str]] = defaultdict(set)
    for alias, canonical in aliases.items():
        grouped_aliases[canonical].add(alias)

    for tag, row in master.items():
        if tag in quality_tags:
            row["category"] = 5
            if "extra-quality-tags.csv" not in row["sources"]:
                row["sources"].append("extra-quality-tags.csv")
        row["sources"] = list(dict.fromkeys(row["sources"]))
        row["aliases"] = sorted(grouped_aliases.get(tag, set()))

    master_rows = sorted(master.values(), key=lambda row: (-int(row["post_count"]), str(row["tag"])))
    alias_rows = [
        {"alias": alias, "canonical": canonical}
        for alias, canonical in sorted(aliases.items(), key=lambda item: item[0])
    ]
    stats = {
        "master_before": before_tags,
        "master_after": len(master_rows),
        "master_added": added,
        "master_updated": updated,
        "aliases_before": before_aliases,
        "aliases_after": len(alias_rows),
        "aliases_added_net": len(alias_rows) - before_aliases,
        "danbooru_tags": len(danbooru_tags),
        "danbooru_active_aliases": len(danbooru_aliases),
    }
    return master_rows, alias_rows, stats


def csv_bytes(rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fieldnames})
    return buffer.getvalue().encode("utf-8")


def gzip_bytes(payload: bytes) -> bytes:
    output = io.BytesIO()
    with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0, compresslevel=9) as handle:
        handle.write(payload)
    return output.getvalue()


def atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--min-post-count", type=int, default=25)
    parser.add_argument("--request-delay", type=float, default=0.15)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    tags_dir = repo / "server" / "assets" / "tags"
    master_csv = tags_dir / "tag_master.csv" / "tag_master.csv"
    master_gz = tags_dir / "tag_master.csv.gz"
    alias_gz = tags_dir / "tag_alias.csv.gz"
    quality_csv = tags_dir / "extra-quality-tags.csv"

    master = load_master(master_csv)
    aliases = load_aliases(alias_gz)
    quality_tags = load_exact_quality_tags(quality_csv)
    print(f"existing master={len(master)} aliases={len(aliases)} exact_quality={len(quality_tags)}", flush=True)

    danbooru_tags = fetch_all_tags(args.min_post_count, args.request_delay)
    danbooru_aliases = fetch_all_active_aliases(args.request_delay)
    master_rows, alias_rows, stats = merge_data(master, aliases, danbooru_tags, danbooru_aliases, quality_tags)

    master_payload = csv_bytes(
        (
            {
                "tag": row["tag"],
                "category": row["category"],
                "post_count": row["post_count"],
                "sources": "|".join(row["sources"]),
                "aliases": "|".join(row["aliases"]),
            }
            for row in master_rows
        ),
        ["tag", "category", "post_count", "sources", "aliases"],
    )
    alias_payload = csv_bytes(alias_rows, ["alias", "canonical"])

    atomic_write(master_csv, master_payload)
    atomic_write(master_gz, gzip_bytes(master_payload))
    atomic_write(alias_gz, gzip_bytes(alias_payload))

    report = {
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "api_root": API_ROOT,
        "minimum_post_count": args.min_post_count,
        **stats,
        "master_csv_bytes": len(master_payload),
        "alias_csv_bytes": len(alias_payload),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
