"""
Replace TRANSFER items in Japan package itineraries using transfers_to_replace.csv.

For each CSV row we locate the package_itinerary by **slug** (derived from
``package_itinerary_title``: lowercase, strip special chars, spaces to hyphens),
then the item at (day_index, itinerary_index). If module == TRANSFER and
module_id matches the **old** or **new** code in the sheet, we replace it
(idempotent refresh when already migrated). Lookup uses ``new_cms_activity_code``
in
within_city_transfer or across_city_transfer.

Only package itineraries whose package_id belongs to a bookable Japan package
(destination_slug=japan) are updated.

Toggle DRY_RUN at the top to preview without writing.
"""

from __future__ import annotations

import csv
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# ----------------------------- Run configuration ----------------------------
DRY_RUN = False

THIS_DIR = Path(__file__).resolve().parent
REPLACE_CSV = THIS_DIR / "content" / "transfers_to_replace.csv"
REPORTS_DIR = THIS_DIR / "reports"

MONGO_URI = os.getenv("MONGO_URI")
ITINERARY_DB = os.getenv("ITINERARY_DB") or "ht_itinerary_db"
TRANSFER_DB = os.getenv("TRANSFER_DB") or "ht_transfer_db"

TARGET_DESTINATION_SLUG = "japan"

SHARING_BASIS_TO_TRANSFER_TYPE = {
    "private": "PRIVATE",
    "sharing": "SHARED",
    "shared": "SHARED",
    "none": "PRIVATE",
}

TRANSFER_TYPE_CAR_BUS_TRAIN_MAPPING = {
    "car": "TAXI",
    "rail": "RAIL",
    "train": "RAIL",
    "bus": "BUS",
    "coach": "BUS",
    "ferry": "FERRY",
    "speed_boat": "SPEED_BOAT",
    "sea_plane": "SEA_PLANE",
}


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_code(value: Any) -> str:
    return clean_str(value).upper()


def normalize_csv_row(row: dict[str, Any]) -> dict[str, str]:
    """Strip keys/values from a CSV DictReader row."""
    return {clean_str(k): clean_str(v) for k, v in row.items()}


def title_to_slug(title: str) -> str:
    """Convert package itinerary title to slug (env-agnostic lookup key).

    Rules: lowercase, remove punctuation/symbols (no hyphen from them), then
    spaces become hyphens.

    Example: ``Japan's Scenic Trio`` -> ``japans-scenic-trio``
    """
    text = clean_str(title).lower()
    if not text:
        return ""
    # Drop special characters entirely (apostrophe, &, etc.) — do not insert hyphens.
    text = re.sub(r"[^a-z0-9\s]", "", text)
    # Collapse whitespace runs to a single hyphen between words.
    text = re.sub(r"\s+", "-", text.strip())
    return text.strip("-")


# ----------------------------- DB -------------------------------------------

def get_clients() -> tuple[MongoClient, Any, Any, Any, Any]:
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI env var is not set")
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        tlsAllowInvalidCertificates=True,
        tlsAllowInvalidHostnames=True,
    )
    itinerary_db = client[ITINERARY_DB]
    transfer_db = client[TRANSFER_DB]
    return (
        client,
        itinerary_db["package"],
        itinerary_db["package_itinerary"],
        transfer_db["within_city_transfer"],
        transfer_db["across_city_transfer"],
    )


# ------------------------- Transfer lookup cache ----------------------------

class TransferLookup:
    def __init__(self, within_coll, across_coll) -> None:
        self.within_coll = within_coll
        self.across_coll = across_coll
        self._cache: dict[str, Optional[tuple[str, dict]]] = {}

    def get(self, jarvis_id: str) -> Optional[tuple[str, dict]]:
        code = normalize_code(jarvis_id)
        if not code:
            return None
        if code in self._cache:
            return self._cache[code]
        doc = self.within_coll.find_one({"jarvis_id": code})
        if doc:
            self._cache[code] = ("within", doc)
            return self._cache[code]
        doc = self.across_coll.find_one({"jarvis_id": code})
        if doc:
            self._cache[code] = ("across", doc)
            return self._cache[code]
        self._cache[code] = None
        return None


def type_from_hero_image(hero_image: str) -> str:
    if not hero_image:
        return "TAXI"
    name = hero_image.split("/")[-1].split(".")[0].lower()
    if "car" in name or "taxi" in name or "private" in name:
        return "TAXI"
    if "bus" in name or "coach" in name or "shuttle" in name:
        return "BUS"
    if "rail" in name or "train" in name:
        return "RAIL"
    return "TAXI"


def build_itinerary_transfer_item(
    transfer_doc: dict[str, Any],
    collection_kind: str,
) -> dict[str, Any]:
    """Build a package_itinerary TRANSFER item from a transfer DB document."""
    jarvis_id = clean_str(transfer_doc.get("jarvis_id"))
    sharing_raw = clean_str(transfer_doc.get("sharing_basis")).lower()
    transfer_type = SHARING_BASIS_TO_TRANSFER_TYPE.get(
        sharing_raw, sharing_raw.upper() or "PRIVATE"
    )
    hero_image = clean_str(transfer_doc.get("hero_image"))

    if collection_kind == "within":
        pick_up = clean_str(transfer_doc.get("pickup"))
        drop_off = clean_str(transfer_doc.get("dropoff"))
        transfer_category = "WITHIN"
        vehicle_type = type_from_hero_image(hero_image)
    else:
        pick_up = clean_str(transfer_doc.get("t_from"))
        drop_off = clean_str(transfer_doc.get("t_to"))
        transfer_category = "ACROSS"
        category = clean_str(transfer_doc.get("category")).lower()
        vehicle_type = TRANSFER_TYPE_CAR_BUS_TRAIN_MAPPING.get(
            category, type_from_hero_image(hero_image)
        )

    sub_type = (
        "AIRPORT_TRANSFER"
        if "airport" in pick_up.lower() or "airport" in drop_off.lower()
        else None
    )

    item: dict[str, Any] = {
        "module": "TRANSFER",
        "module_id": jarvis_id,
        "_id": transfer_doc["_id"],
        "title": clean_str(transfer_doc.get("title")),
        "transfer_type": transfer_type,
        "type": vehicle_type,
        "from_location": pick_up,
        "to_location": drop_off,
        "transfer_category": transfer_category,
        "image_url": hero_image or "https://cdn.holidaytribe.ai/website/transfer/private-taxi.png",
    }
    if sub_type:
        item["sub_type"] = sub_type
    return item


# ------------------------------ CSV loading ---------------------------------

def load_replacement_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = normalize_csv_row(raw)
            # Header in sheet may be " new_cms_activity_code" (leading space).
            new_code = row.get("new_cms_activity_code", "")
            if not new_code:
                for key, val in row.items():
                    if "new_cms" in key.lower() and "code" in key.lower():
                        new_code = val
                        break
            title = row.get("package_itinerary_title", "")
            if not title or not new_code:
                continue
            row["new_cms_activity_code"] = normalize_code(new_code)
            row["module_id"] = normalize_code(row.get("module_id", ""))
            row["package_itinerary_slug"] = title_to_slug(title)
            rows.append(row)
    return rows


def fetch_japan_package_ids(package_coll) -> set[Any]:
    cursor = package_coll.find(
        {"destination_slug": TARGET_DESTINATION_SLUG, "is_bookable": True},
        {"_id": 1},
    )
    ids = {doc["_id"] for doc in cursor}
    return ids


def fetch_japan_itineraries_by_slug(
    itinerary_coll,
    japan_package_ids: set[Any],
) -> dict[str, dict[str, Any]]:
    """Index Japan package_itinerary docs by slug for env-agnostic lookup."""
    if not japan_package_ids:
        return {}
    by_slug: dict[str, dict[str, Any]] = {}
    cursor = itinerary_coll.find(
        {"package_id": {"$in": list(japan_package_ids)}},
        {"_id": 1, "slug": 1, "title": 1, "package_id": 1, "day_wise_details": 1},
    )
    for doc in cursor:
        slug = clean_str(doc.get("slug"))
        if slug:
            by_slug[slug] = doc
        # Also index by slug derived from title (handles CSV title vs stored slug drift).
        title_slug = title_to_slug(doc.get("title") or "")
        if title_slug and title_slug not in by_slug:
            by_slug[title_slug] = doc
    return by_slug


# --------------------------------- Report -----------------------------------

REPORT_HEADERS = [
    "transfer_code_old",
    "transfer_code_new",
    "package_itinerary_slug",
    "package_itinerary_id",
    "package_itinerary_title",
    "day_index",
    "itinerary_index",
    "status",
    "reason",
    "old_module_id_found",
    "new_title",
    "transfer_collection",
    "mode",
]


def write_report(rows: list[dict[str, Any]], dry_run: bool, log: logging.Logger) -> str:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    mode = "dry_run" if dry_run else "live"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"transfer_itinerary_replace_{mode}_{timestamp}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in REPORT_HEADERS})
    log.info("Report written: %s", path)
    return str(path)


# ------------------------------ Main pipeline -------------------------------

def run(dry_run: bool) -> None:
    setup_logging()
    log = logging.getLogger("replace-transfer-itinerary")
    log.info("Loading replacements from %s (dry_run=%s)", REPLACE_CSV, dry_run)

    replacement_rows = load_replacement_rows(REPLACE_CSV)
    log.info("Replacement rows loaded: %d", len(replacement_rows))

    client, package_coll, itinerary_coll, within_coll, across_coll = get_clients()
    japan_package_ids = fetch_japan_package_ids(package_coll)
    log.info("Japan bookable package ids: %d", len(japan_package_ids))

    itineraries_by_slug = fetch_japan_itineraries_by_slug(itinerary_coll, japan_package_ids)
    log.info("Japan package_itinerary docs indexed by slug: %d", len(itineraries_by_slug))

    transfer_lookup = TransferLookup(within_coll, across_coll)

    # Group replacements by slug for one read + one write per doc.
    by_slug: dict[str, list[dict[str, str]]] = {}
    for row in replacement_rows:
        slug = row["package_itinerary_slug"]
        by_slug.setdefault(slug, []).append(row)

    report_rows: list[dict[str, Any]] = []
    counts = {
        "replaced": 0,
        "skipped": 0,
        "itineraries_updated": 0,
    }
    mode_label = "dry_run" if dry_run else "live"

    for slug, rows_for_doc in by_slug.items():
        itinerary_doc = itineraries_by_slug.get(slug)
        if not itinerary_doc:
            title = rows_for_doc[0].get("package_itinerary_title", "")
            reason = f"package_itinerary not found for slug={slug!r} (title={title!r})"
            for row in rows_for_doc:
                report_rows.append(_report_row(row, "skipped", reason, mode_label))
            counts["skipped"] += len(rows_for_doc)
            log.warning("SKIP slug=%s -> %s", slug, reason)
            continue

        itinerary_oid = itinerary_doc["_id"]
        itinerary_id_str = str(itinerary_oid)

        day_wise_details = itinerary_doc.get("day_wise_details") or []
        if not isinstance(day_wise_details, list):
            for row in rows_for_doc:
                report_rows.append(_report_row(row, "skipped", "invalid day_wise_details", mode_label))
            counts["skipped"] += len(rows_for_doc)
            continue

        doc_changed = False

        for row in rows_for_doc:
            report = _report_row(row, "", "", mode_label)
            report["package_itinerary_id"] = itinerary_id_str
            report["package_itinerary_slug"] = slug
            old_code = row["module_id"]
            new_code = row["new_cms_activity_code"]

            try:
                day_index = int(row["day_index"])
                itinerary_index = int(row["itinerary_index"])
            except (TypeError, ValueError):
                report["status"] = "skipped"
                report["reason"] = "invalid day_index or itinerary_index"
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            if day_index < 1 or itinerary_index < 1:
                report["status"] = "skipped"
                report["reason"] = "day_index and itinerary_index are 1-based"
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            day_idx = day_index - 1
            item_idx = itinerary_index - 1

            if day_idx >= len(day_wise_details):
                report["status"] = "skipped"
                report["reason"] = f"day_index {day_index} out of range (days={len(day_wise_details)})"
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            day_detail = day_wise_details[day_idx]
            if not isinstance(day_detail, dict):
                report["status"] = "skipped"
                report["reason"] = "day detail is not an object"
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            itinerary_items = day_detail.get("itinerary") or []
            if not isinstance(itinerary_items, list):
                report["status"] = "skipped"
                report["reason"] = "itinerary is not a list"
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            if item_idx >= len(itinerary_items):
                report["status"] = "skipped"
                report["reason"] = (
                    f"itinerary_index {itinerary_index} out of range "
                    f"(items={len(itinerary_items)})"
                )
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            item = itinerary_items[item_idx]
            if not isinstance(item, dict):
                report["status"] = "skipped"
                report["reason"] = "itinerary item is not an object"
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            if item.get("module") != "TRANSFER":
                report["status"] = "skipped"
                report["reason"] = f"module is {item.get('module')!r}, expected TRANSFER"
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            found_module_id = normalize_code(item.get("module_id"))
            report["old_module_id_found"] = found_module_id

            if found_module_id == old_code:
                match_kind = "old_code"
            elif found_module_id == new_code:
                match_kind = "new_code"
            else:
                report["status"] = "skipped"
                report["reason"] = (
                    f"module_id mismatch: expected old={old_code} or new={new_code}, "
                    f"found {found_module_id}"
                )
                report_rows.append(report)
                counts["skipped"] += 1
                log.warning(
                    "SKIP %s (%s) day=%s idx=%s -> %s",
                    slug, itinerary_id_str, day_index, itinerary_index, report["reason"],
                )
                continue

            lookup = transfer_lookup.get(new_code)
            if not lookup:
                report["status"] = "skipped"
                report["reason"] = f"new transfer {new_code} not found in within/across collections"
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            collection_kind, transfer_doc = lookup
            new_item = build_itinerary_transfer_item(transfer_doc, collection_kind)
            itinerary_items[item_idx] = new_item
            doc_changed = True

            report["status"] = (
                f"queued_replace_{match_kind}" if dry_run else f"replaced_{match_kind}"
            )
            report["reason"] = (
                f"matched on {match_kind} ({found_module_id})"
                if match_kind == "new_code"
                else ""
            )
            report["new_title"] = new_item.get("title", "")
            report["transfer_collection"] = (
                "within_city_transfer" if collection_kind == "within" else "across_city_transfer"
            )
            report_rows.append(report)
            counts["replaced"] += 1
            log.info(
                "%s %s (%s) day=%s idx=%s [%s]: %s -> %s (%s)",
                "QUEUE" if dry_run else "REPLACE",
                slug,
                itinerary_id_str,
                day_index,
                itinerary_index,
                match_kind,
                found_module_id,
                new_code,
                report["transfer_collection"],
            )

        if doc_changed and not dry_run:
            itinerary_coll.update_one(
                {"_id": itinerary_oid},
                {"$set": {"day_wise_details": day_wise_details, "updated_at": utcnow()}},
            )
            counts["itineraries_updated"] += 1
            log.info("Updated package_itinerary slug=%s _id=%s", slug, itinerary_id_str)
        elif doc_changed:
            counts["itineraries_updated"] += 1

    log.info("Summary:")
    log.info("  replacements applied:     %d", counts["replaced"])
    log.info("  rows skipped:             %d", counts["skipped"])
    log.info("  itineraries touched:    %d", counts["itineraries_updated"])

    write_report(report_rows, dry_run=dry_run, log=log)
    client.close()

    if dry_run:
        log.info("DRY RUN complete. Set DRY_RUN=False to write changes.")


def _report_row(
    row: dict[str, str],
    status: str,
    reason: str,
    mode: str,
) -> dict[str, Any]:
    return {
        "transfer_code_old": row.get("module_id", ""),
        "transfer_code_new": row.get("new_cms_activity_code", ""),
        "package_itinerary_slug": row.get("package_itinerary_slug", ""),
        "package_itinerary_id": "",
        "package_itinerary_title": row.get("package_itinerary_title", ""),
        "day_index": row.get("day_index", ""),
        "itinerary_index": row.get("itinerary_index", ""),
        "status": status,
        "reason": reason,
        "old_module_id_found": "",
        "new_title": "",
        "transfer_collection": "",
        "mode": mode,
    }


if __name__ == "__main__":
    run(dry_run=DRY_RUN)
