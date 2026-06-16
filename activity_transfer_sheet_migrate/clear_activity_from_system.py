"""
Prune Japan activities in the activity DB so only CMS sheet activities remain.

Keeps every activity whose ``provider_info.cms.activityCode`` appears in
``content/activity_static_migrate_sheet_v2.csv`` (``ID`` column).

Deletes (and removes images + prices for) all other Japan activities:

  - activity: ``country.name`` == "Japan" and cms activityCode not in sheet

Deletion order: activity_price and activity_image by ``activity_id``, then
activity documents.

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
STATIC_ACTIVITY_CSV = THIS_DIR / "content" / "activity_static_migrate_sheet_v2.csv"
REPORTS_DIR = THIS_DIR / "reports"

MONGO_URI = os.getenv("MONGO_URI")
ACTIVITIES_DB = os.getenv("ACTIVITIES_DB") or "ht_activity_db"

JAPAN_COUNTRY_NAME = "Japan"

REPORT_HEADERS = [
    "activity_code",
    "activity_id",
    "title",
    "price_count",
    "image_count",
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


def load_allowed_activity_codes(path: Path, log: logging.Logger) -> set[str]:
    """Activity codes to keep — every ``ID`` from the static migrate sheet."""
    if not path.is_file():
        raise FileNotFoundError(f"Static activity sheet not found: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    if "ID" not in df.columns:
        raise ValueError(f"Column 'ID' missing in {path}")
    allowed: set[str] = set()
    for raw in df["ID"]:
        code = normalize_code(raw)
        if code:
            allowed.add(code)
    log.info("Allowlist: %d activity codes from %s", len(allowed), path.name)
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
    db = client[ACTIVITIES_DB]
    return (
        client,
        db["activity"],
        db["activity_image"],
        db["activity_price"],
    )


def japan_activity_filter(allowed: set[str]) -> dict[str, Any]:
    return {
        "country.name": JAPAN_COUNTRY_NAME,
        "provider_info.cms.activityCode": {"$nin": list(allowed)},
    }


def find_japan_activities_to_remove(
    activity_coll,
    query: dict[str, Any],
) -> list[dict[str, Any]]:
    return list(
        activity_coll.find(
            query,
            {
                "_id": 1,
                "title": 1,
                "provider_info.cms.activityCode": 1,
                "provider_info.hotelbeds.activityCode": 1,
                "country": 1,
            },
        )
    )


def activity_code_from_doc(doc: dict[str, Any]) -> str:
    cms = (doc.get("provider_info") or {}).get("cms") or {}
    code = cms.get("activityCode")
    if code:
        return str(code)
    hb = (doc.get("provider_info") or {}).get("hotelbeds") or {}
    return str(hb.get("activityCode") or "")


def write_report(rows: list[dict[str, Any]], dry_run: bool, log: logging.Logger) -> str:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    mode = "dry_run" if dry_run else "live"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"activity_clear_{mode}_{timestamp}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in REPORT_HEADERS})
    log.info("Report written: %s", path)
    return str(path)


def append_doc_reports(
    docs: list[dict[str, Any]],
    image_coll,
    price_coll,
    report_rows: list[dict[str, Any]],
    dry_run: bool,
    mode_label: str,
    reason: str,
    log: logging.Logger,
) -> list[ObjectId]:
    activity_ids: list[ObjectId] = []
    for doc in docs:
        aid = doc["_id"]
        activity_ids.append(aid)
        price_count = price_coll.count_documents({"activity_id": aid})
        image_count = image_coll.count_documents({"activity_id": aid})
        report_rows.append(
            {
                "activity_code": activity_code_from_doc(doc),
                "activity_id": str(aid),
                "title": doc.get("title", ""),
                "price_count": price_count,
                "image_count": image_count,
                "status": "queued_delete" if dry_run else "deleted",
                "reason": reason,
                "mode": mode_label,
            }
        )
        log.info(
            "[%s] activity %s code=%s prices=%d images=%d title=%r",
            "DRY RUN" if dry_run else "DELETE",
            aid,
            activity_code_from_doc(doc),
            price_count,
            image_count,
            doc.get("title", ""),
        )
    return activity_ids


def run(dry_run: bool) -> None:
    setup_logging()
    log = logging.getLogger("clear-activity")
    log.info("DRY_RUN=%s | DB=%s | country=%s", dry_run, ACTIVITIES_DB, JAPAN_COUNTRY_NAME)

    allowed = load_allowed_activity_codes(STATIC_ACTIVITY_CSV, log)
    if not allowed:
        log.warning("Allowlist is empty. Aborting to avoid mass delete.")
        return

    client, activity_coll, image_coll, price_coll = get_db()
    mode_label = "dry_run" if dry_run else "live"
    report_rows: list[dict[str, Any]] = []

    delete_query = japan_activity_filter(allowed)
    log.info("Activity delete filter: %s", delete_query)

    docs_to_remove = find_japan_activities_to_remove(activity_coll, delete_query)
    activity_ids = append_doc_reports(
        docs_to_remove,
        image_coll,
        price_coll,
        report_rows,
        dry_run,
        mode_label,
        f"japan activity, cms activityCode not in static sheet ({len(allowed)} allowed)",
        log,
    )

    keep_count = activity_coll.count_documents(
        {
            "country.name": JAPAN_COUNTRY_NAME,
            "provider_info.cms.activityCode": {"$in": list(allowed)},
        }
    )

    log.info("Summary:")
    log.info("  allowlist (sheet):     %d", len(allowed))
    log.info("  activity DELETE:       %d", len(docs_to_remove))
    log.info("  activity KEEP:         %d", keep_count)

    if not dry_run and activity_ids:
        r_prices = price_coll.delete_many({"activity_id": {"$in": activity_ids}})
        r_images = image_coll.delete_many({"activity_id": {"$in": activity_ids}})
        r_activities = activity_coll.delete_many({"_id": {"$in": activity_ids}})
        log.info(
            "Deleted %d prices, %d images, %d activities",
            r_prices.deleted_count,
            r_images.deleted_count,
            r_activities.deleted_count,
        )
    elif dry_run:
        log.info("DRY RUN: no deletes performed. Set DRY_RUN=False to remove.")

    write_report(report_rows, dry_run=dry_run, log=log)
    client.close()
    log.info("Done.")


if __name__ == "__main__":
    run(dry_run=DRY_RUN)
