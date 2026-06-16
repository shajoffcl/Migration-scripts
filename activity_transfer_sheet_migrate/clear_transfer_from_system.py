"""
Prune Japan transfers in the transfer DB so only CMS sheet transfers remain.

Keeps every transfer whose ``jarvis_id`` appears in
``content/transfer_static_migrate_sheet.csv`` (``ID`` column).

Deletes (and removes slabs for) all other Japan transfers:

  - within_city_transfer: ``country.name`` == "Japan" and jarvis_id not in sheet
  - across_city_transfer: ``from_country.name`` and ``to_country.name`` == "Japan"
    and jarvis_id not in sheet

Deletion order: slabs by ``transfer_id``, then transfer documents.

Toggle DRY_RUN at the top to preview without writing.
"""

from __future__ import annotations

import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# ----------------------------- Run configuration ----------------------------
DRY_RUN = False

THIS_DIR = Path(__file__).resolve().parent
STATIC_TRANSFER_CSV = THIS_DIR / "content" / "transfer_static_migrate_sheet.csv"
REPORTS_DIR = THIS_DIR / "reports"

MONGO_URI = os.getenv("MONGO_URI")
TRANSFER_DB = os.getenv("TRANSFER_DB") or "ht_transfer_db"

JAPAN_COUNTRY_NAME = "Japan"

REPORT_HEADERS = [
    "jarvis_id",
    "collection",
    "transfer_id",
    "title",
    "slab_count",
    "status",
    "reason",
    "mode",
]


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def normalize_code(value: Any) -> str:
    return str(value or "").strip().upper()


def load_allowed_jarvis_ids(path: Path, log: logging.Logger) -> set[str]:
    """Jarvis IDs to keep — every ``ID`` from the static migrate sheet."""
    if not path.is_file():
        raise FileNotFoundError(f"Static transfer sheet not found: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    if "ID" not in df.columns:
        raise ValueError(f"Column 'ID' missing in {path}")
    allowed: set[str] = set()
    for raw in df["ID"]:
        code = normalize_code(raw)
        if code:
            allowed.add(code)
    log.info("Allowlist: %d jarvis_ids from %s", len(allowed), path.name)
    for code in sorted(allowed):
        log.info("  KEEP %s", code)
    return allowed


def get_db():
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI env var is not set")
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        tlsAllowInvalidCertificates=True,
        tlsAllowInvalidHostnames=True,
    )
    db = client[TRANSFER_DB]
    return (
        client,
        db["within_city_transfer"],
        db["across_city_transfer"],
        db["within_city_transfer_slab"],
        db["across_city_transfer_slab"],
    )


def japan_within_filter(allowed: set[str]) -> dict[str, Any]:
    return {
        "country.name": JAPAN_COUNTRY_NAME,
        "jarvis_id": {"$nin": list(allowed)},
    }


def japan_across_filter(allowed: set[str]) -> dict[str, Any]:
    return {
        "from_country.name": JAPAN_COUNTRY_NAME,
        "to_country.name": JAPAN_COUNTRY_NAME,
        "jarvis_id": {"$nin": list(allowed)},
    }


def find_japan_transfers_to_remove(
    coll,
    query: dict[str, Any],
) -> list[dict[str, Any]]:
    return list(
        coll.find(
            query,
            {"_id": 1, "jarvis_id": 1, "title": 1, "country": 1, "from_country": 1, "to_country": 1},
        )
    )


def write_report(rows: list[dict[str, Any]], dry_run: bool, log: logging.Logger) -> str:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    mode = "dry_run" if dry_run else "live"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"transfer_clear_{mode}_{timestamp}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in REPORT_HEADERS})
    log.info("Report written: %s", path)
    return str(path)


def append_doc_reports(
    docs: list[dict[str, Any]],
    collection: str,
    slab_coll,
    report_rows: list[dict[str, Any]],
    dry_run: bool,
    mode_label: str,
    reason: str,
    log: logging.Logger,
) -> list[ObjectId]:
    transfer_ids: list[ObjectId] = []
    for doc in docs:
        tid = doc["_id"]
        transfer_ids.append(tid)
        sc = slab_coll.count_documents({"transfer_id": tid})
        report_rows.append(
            {
                "jarvis_id": doc.get("jarvis_id", ""),
                "collection": collection,
                "transfer_id": str(tid),
                "title": doc.get("title", ""),
                "slab_count": sc,
                "status": "queued_delete" if dry_run else "deleted",
                "reason": reason,
                "mode": mode_label,
            }
        )
        log.info(
            "[%s] %s %s jarvis_id=%s slabs=%d title=%r",
            "DRY RUN" if dry_run else "DELETE",
            collection,
            tid,
            doc.get("jarvis_id"),
            sc,
            doc.get("title", ""),
        )
    return transfer_ids


def run(dry_run: bool) -> None:
    setup_logging()
    log = logging.getLogger("clear-transfer")
    log.info("DRY_RUN=%s | DB=%s | country=%s", dry_run, TRANSFER_DB, JAPAN_COUNTRY_NAME)

    allowed = load_allowed_jarvis_ids(STATIC_TRANSFER_CSV, log)
    if not allowed:
        log.warning("Allowlist is empty. Aborting to avoid mass delete.")
        return

    client, within_coll, across_coll, within_slab_coll, across_slab_coll = get_db()
    mode_label = "dry_run" if dry_run else "live"
    report_rows: list[dict[str, Any]] = []

    within_query = japan_within_filter(allowed)
    across_query = japan_across_filter(allowed)

    log.info("Within delete filter: %s", within_query)
    log.info("Across delete filter: %s", across_query)

    within_docs = find_japan_transfers_to_remove(within_coll, within_query)
    across_docs = find_japan_transfers_to_remove(across_coll, across_query)

    within_ids = append_doc_reports(
        within_docs,
        "within_city_transfer",
        within_slab_coll,
        report_rows,
        dry_run,
        mode_label,
        f"japan within, jarvis_id not in static sheet ({len(allowed)} allowed)",
        log,
    )
    across_ids = append_doc_reports(
        across_docs,
        "across_city_transfer",
        across_slab_coll,
        report_rows,
        dry_run,
        mode_label,
        f"japan to/from japan across, jarvis_id not in static sheet",
        log,
    )

    within_keep_count = within_coll.count_documents(
        {"country.name": JAPAN_COUNTRY_NAME, "jarvis_id": {"$in": list(allowed)}}
    )
    across_keep_count = across_coll.count_documents(
        {
            "from_country.name": JAPAN_COUNTRY_NAME,
            "to_country.name": JAPAN_COUNTRY_NAME,
            "jarvis_id": {"$in": list(allowed)},
        }
    )

    log.info("Summary:")
    log.info("  allowlist (sheet):              %d", len(allowed))
    log.info("  within_city_transfer DELETE:    %d", len(within_docs))
    log.info("  across_city_transfer DELETE:    %d", len(across_docs))
    log.info("  within_city_transfer KEEP:      %d", within_keep_count)
    log.info("  across_city_transfer KEEP:      %d", across_keep_count)

    if not dry_run:
        if within_ids:
            r1 = within_slab_coll.delete_many({"transfer_id": {"$in": within_ids}})
            r2 = within_coll.delete_many({"_id": {"$in": within_ids}})
            log.info(
                "Within: deleted %d slabs, %d transfers",
                r1.deleted_count,
                r2.deleted_count,
            )
        if across_ids:
            r1 = across_slab_coll.delete_many({"transfer_id": {"$in": across_ids}})
            r2 = across_coll.delete_many({"_id": {"$in": across_ids}})
            log.info(
                "Across: deleted %d slabs, %d transfers",
                r1.deleted_count,
                r2.deleted_count,
            )
    else:
        log.info("DRY RUN: no deletes performed. Set DRY_RUN=False to remove.")

    write_report(report_rows, dry_run=dry_run, log=log)
    client.close()
    log.info("Done.")


if __name__ == "__main__":
    run(dry_run=DRY_RUN)
