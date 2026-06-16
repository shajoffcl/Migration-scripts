"""
Replace ACTIVITY (or TRANSFER) items in Japan package itineraries using
activity_to_replace.csv.

For each CSV row we locate the package_itinerary by ``package_itinerary_id``
(preferred) or slug derived from ``package_itinerary_title``, then the item at
(day_index, itinerary_index). If module == ACTIVITY and module_id matches the
**old** Hotelbeds/CMS code or the **new** CMS code in the sheet, we replace it
(idempotent refresh when already migrated).

The new item is resolved with a three-tier fallback:
  1. activity collection   (provider_info.cms.activityCode / hotelbeds.activityCode)
  2. within_city_transfer  (jarvis_id)
  3. across_city_transfer  (jarvis_id)

This means a row in the activity sheet whose new_cms_activity_code turns out to
be a transfer jarvis_id will be migrated to a TRANSFER item automatically.

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
REPLACE_CSV = THIS_DIR / "content" / "activity_to_replace.csv"
REPORTS_DIR = THIS_DIR / "reports"

MONGO_URI = os.getenv("MONGO_URI")
ITINERARY_DB = os.getenv("ITINERARY_DB") or "ht_itinerary_db"
ACTIVITIES_DB = os.getenv("ACTIVITIES_DB") or "ht_activity_db"
TRANSFER_DB = os.getenv("TRANSFER_DB") or "ht_transfer_db"

TARGET_DESTINATION_SLUG = "japan"


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


def normalize_module_id(value: Any) -> str:
    """Normalize module_id for comparison (case-insensitive, trim whitespace/newlines)."""
    return clean_str(value).upper()


def normalize_csv_row(row: dict[str, Any]) -> dict[str, str]:
    return {clean_str(k): clean_str(v) for k, v in row.items()}


def title_to_slug(title: str) -> str:
    text = clean_str(title).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", "-", text.strip())
    return text.strip("-")


# ----------------------------- Transfer helpers -----------------------------

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


# ----------------------------- DB -------------------------------------------

def get_clients() -> tuple[MongoClient, Any, Any, Any, Any, Any]:
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI env var is not set")
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        tlsAllowInvalidCertificates=True,
        tlsAllowInvalidHostnames=True,
    )
    itinerary_db = client[ITINERARY_DB]
    activity_db = client[ACTIVITIES_DB]
    transfer_db = client[TRANSFER_DB]
    return (
        client,
        itinerary_db["package"],
        itinerary_db["package_itinerary"],
        activity_db["activity"],
        transfer_db["within_city_transfer"],
        transfer_db["across_city_transfer"],
    )


# -------------------- Unified item lookup cache (activity → within → across) ---

class ItemLookup:
    """Resolve a new_cms_activity_code with a three-tier fallback:
    1. activity collection  (provider_info.cms.activityCode / hotelbeds.activityCode)
    2. within_city_transfer (jarvis_id)
    3. across_city_transfer (jarvis_id)

    Returns a ``(collection_kind, doc)`` tuple or ``None``.
    ``collection_kind`` is one of: ``"activity"``, ``"within"``, ``"across"``.
    """

    def __init__(self, activity_coll, within_coll, across_coll) -> None:
        self.activity_coll = activity_coll
        self.within_coll = within_coll
        self.across_coll = across_coll
        self._cache: dict[str, Optional[tuple[str, dict]]] = {}

    def get(self, code: str) -> Optional[tuple[str, dict]]:
        raw = clean_str(code)
        if not raw:
            return None
        cache_key = raw.upper()
        if cache_key in self._cache:
            return self._cache[cache_key]

        result: Optional[tuple[str, dict]] = None

        # 1. Activity collection
        doc = self.activity_coll.find_one({"provider_info.cms.activityCode": raw})
        if not doc:
            doc = self.activity_coll.find_one({"provider_info.cms.activityCode": cache_key})
        if not doc:
            doc = self.activity_coll.find_one({"provider_info.hotelbeds.activityCode": raw})
        if not doc:
            doc = self.activity_coll.find_one({"provider_info.hotelbeds.activityCode": cache_key})
        if doc:
            result = ("activity", doc)

        # 2. within_city_transfer
        if result is None:
            doc = self.within_coll.find_one({"jarvis_id": cache_key})
            if not doc:
                doc = self.within_coll.find_one({"jarvis_id": raw})
            if doc:
                result = ("within", doc)

        # 3. across_city_transfer
        if result is None:
            doc = self.across_coll.find_one({"jarvis_id": cache_key})
            if not doc:
                doc = self.across_coll.find_one({"jarvis_id": raw})
            if doc:
                result = ("across", doc)

        self._cache[cache_key] = result
        self._cache[raw] = result
        return result


def activity_code_from_doc(activity_doc: dict[str, Any]) -> str:
    provider_info = activity_doc.get("provider_info") or {}
    cms_code = clean_str((provider_info.get("cms") or {}).get("activityCode"))
    if cms_code:
        return cms_code
    hb_code = clean_str((provider_info.get("hotelbeds") or {}).get("activityCode"))
    return hb_code


def first_image_url(activity_doc: dict[str, Any]) -> str:
    images = activity_doc.get("images") or []
    if not images or not isinstance(images[0], dict):
        return ""
    return clean_str(images[0].get("img_url"))


def build_itinerary_activity_item(
    activity_doc: dict[str, Any],
    new_code: str,
    old_item: dict[str, Any],
) -> dict[str, Any]:
    """Build a package_itinerary ACTIVITY item from an activity DB document."""
    module_id = activity_code_from_doc(activity_doc) or clean_str(new_code)

    new_item: dict[str, Any] = {
        "module": "ACTIVITY",
        "isCmsActivity": True,
        "module_id": module_id,
        "_id": activity_doc["_id"],
        "name": clean_str(activity_doc.get("title") or activity_doc.get("seller_title")),
        "inclusion": activity_doc.get("inclusions") or [],
        "exclusion": activity_doc.get("exclusions") or [],
        "cancellation": clean_str(activity_doc.get("cancellation")),
        "type": activity_doc.get("type"),
        "tags": activity_doc.get("tags") or [],
        "description": clean_str(activity_doc.get("description")),
        "image_url": first_image_url(activity_doc),
        "duration": activity_doc.get("duration") if activity_doc.get("duration") is not None else 0,
    }

    if "is_paid" in old_item:
        new_item["is_paid"] = old_item["is_paid"]

    return new_item


# ------------------------------ CSV loading ---------------------------------

def load_replacement_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = normalize_csv_row(raw)
            new_code = row.get("new_cms_activity_code", "")
            if not new_code:
                for key, val in row.items():
                    if "new_cms" in key.lower() and "code" in key.lower():
                        new_code = val
                        break

            title = row.get("package_itinerary_title", "")
            module_id = clean_str(row.get("module_id", ""))
            itinerary_id = clean_str(row.get("package_itinerary_id", ""))

            if not title or not new_code or not module_id:
                continue

            row["new_cms_activity_code"] = clean_str(new_code)
            row["module_id"] = module_id
            row["package_itinerary_id"] = itinerary_id
            row["package_itinerary_slug"] = title_to_slug(title)
            rows.append(row)
    return rows


def fetch_japan_package_ids(package_coll) -> set[Any]:
    cursor = package_coll.find(
        {"destination_slug": TARGET_DESTINATION_SLUG, "is_bookable": True},
        {"_id": 1},
    )
    return {doc["_id"] for doc in cursor}


def fetch_japan_itineraries(
    itinerary_coll,
    japan_package_ids: set[Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Index Japan package_itinerary docs by _id string and slug."""
    by_id: dict[str, dict[str, Any]] = {}
    by_slug: dict[str, dict[str, Any]] = {}
    if not japan_package_ids:
        return by_id, by_slug

    cursor = itinerary_coll.find(
        {"package_id": {"$in": list(japan_package_ids)}},
        {"_id": 1, "slug": 1, "title": 1, "package_id": 1, "day_wise_details": 1},
    )
    for doc in cursor:
        by_id[str(doc["_id"])] = doc
        slug = clean_str(doc.get("slug"))
        if slug:
            by_slug[slug] = doc
        title_slug = title_to_slug(doc.get("title") or "")
        if title_slug:
            by_slug.setdefault(title_slug, doc)
    return by_id, by_slug


def resolve_itinerary_doc(
    row: dict[str, str],
    by_id: dict[str, dict[str, Any]],
    by_slug: dict[str, dict[str, Any]],
) -> Optional[dict[str, Any]]:
    itinerary_id = row.get("package_itinerary_id", "")
    if itinerary_id and itinerary_id in by_id:
        return by_id[itinerary_id]
    slug = row.get("package_itinerary_slug", "")
    if slug and slug in by_slug:
        return by_slug[slug]
    return None


# --------------------------------- Report -----------------------------------

REPORT_HEADERS = [
    "activity_code_old",
    "activity_code_new",
    "match_type",
    "package_itinerary_id",
    "package_itinerary_slug",
    "package_itinerary_title",
    "day_index",
    "itinerary_index",
    "status",
    "reason",
    "old_module_id_found",
    "resolved_collection",
    "new_title",
    "new_activity_id",
    "mode",
]


def write_report(rows: list[dict[str, Any]], dry_run: bool, log: logging.Logger) -> str:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    mode = "dry_run" if dry_run else "live"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"activity_itinerary_replace_{mode}_{timestamp}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in REPORT_HEADERS})
    log.info("Report written: %s", path)
    return str(path)


def _report_row(
    row: dict[str, str],
    status: str,
    reason: str,
    mode: str,
) -> dict[str, Any]:
    return {
        "activity_code_old": row.get("module_id", ""),
        "activity_code_new": row.get("new_cms_activity_code", ""),
        "match_type": row.get("match_type", ""),
        "package_itinerary_id": row.get("package_itinerary_id", ""),
        "package_itinerary_slug": row.get("package_itinerary_slug", ""),
        "package_itinerary_title": row.get("package_itinerary_title", ""),
        "day_index": row.get("day_index", ""),
        "itinerary_index": row.get("itinerary_index", ""),
        "status": status,
        "reason": reason,
        "old_module_id_found": "",
        "resolved_collection": "",
        "new_title": "",
        "new_activity_id": "",
        "mode": mode,
    }


# ------------------------------ Main pipeline -------------------------------

def run(dry_run: bool) -> None:
    setup_logging()
    log = logging.getLogger("replace-activity-itinerary")
    log.info("Loading replacements from %s (dry_run=%s)", REPLACE_CSV, dry_run)

    replacement_rows = load_replacement_rows(REPLACE_CSV)
    log.info("Replacement rows loaded: %d", len(replacement_rows))

    client, package_coll, itinerary_coll, activity_coll, within_coll, across_coll = get_clients()
    japan_package_ids = fetch_japan_package_ids(package_coll)
    log.info("Japan bookable package ids: %d", len(japan_package_ids))

    itineraries_by_id, itineraries_by_slug = fetch_japan_itineraries(
        itinerary_coll, japan_package_ids
    )
    log.info(
        "Japan package_itinerary docs indexed: %d by id, %d by slug",
        len(itineraries_by_id),
        len(itineraries_by_slug),
    )

    item_lookup = ItemLookup(activity_coll, within_coll, across_coll)

    # Group by itinerary doc key (prefer package_itinerary_id).
    by_itinerary_key: dict[str, list[dict[str, str]]] = {}
    for row in replacement_rows:
        key = row.get("package_itinerary_id") or row["package_itinerary_slug"]
        by_itinerary_key.setdefault(key, []).append(row)

    report_rows: list[dict[str, Any]] = []
    counts = {
        "replaced": 0,
        "skipped": 0,
        "itineraries_updated": 0,
    }
    mode_label = "dry_run" if dry_run else "live"

    for itinerary_key, rows_for_doc in by_itinerary_key.items():
        itinerary_doc = resolve_itinerary_doc(
            rows_for_doc[0], itineraries_by_id, itineraries_by_slug
        )
        if not itinerary_doc:
            title = rows_for_doc[0].get("package_itinerary_title", "")
            reason = (
                f"package_itinerary not found for id/slug={itinerary_key!r} "
                f"(title={title!r})"
            )
            for row in rows_for_doc:
                report_rows.append(_report_row(row, "skipped", reason, mode_label))
            counts["skipped"] += len(rows_for_doc)
            log.warning("SKIP key=%s -> %s", itinerary_key, reason)
            continue

        itinerary_oid = itinerary_doc["_id"]
        itinerary_id_str = str(itinerary_oid)
        slug = clean_str(itinerary_doc.get("slug")) or rows_for_doc[0].get(
            "package_itinerary_slug", ""
        )

        day_wise_details = itinerary_doc.get("day_wise_details") or []
        if not isinstance(day_wise_details, list):
            for row in rows_for_doc:
                report_rows.append(
                    _report_row(row, "skipped", "invalid day_wise_details", mode_label)
                )
            counts["skipped"] += len(rows_for_doc)
            continue

        doc_changed = False

        for row in rows_for_doc:
            report = _report_row(row, "", "", mode_label)
            report["package_itinerary_id"] = itinerary_id_str
            report["package_itinerary_slug"] = slug

            old_code = row["module_id"]
            new_code = row["new_cms_activity_code"]
            old_code_norm = normalize_module_id(old_code)
            new_code_norm = normalize_module_id(new_code)

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
                report["reason"] = (
                    f"day_index {day_index} out of range (days={len(day_wise_details)})"
                )
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

            item_module = item.get("module")
            if item_module not in ("ACTIVITY", "TRANSFER"):
                report["status"] = "skipped"
                report["reason"] = f"module is {item_module!r}, expected ACTIVITY or TRANSFER"
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            found_module_id = clean_str(item.get("module_id"))
            found_module_id_norm = normalize_module_id(found_module_id)
            report["old_module_id_found"] = found_module_id

            if found_module_id_norm == old_code_norm:
                match_kind = "old_code"
            elif found_module_id_norm == new_code_norm:
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
                    slug,
                    itinerary_id_str,
                    day_index,
                    itinerary_index,
                    report["reason"],
                )
                continue

            lookup = item_lookup.get(new_code)
            if not lookup:
                report["status"] = "skipped"
                report["reason"] = (
                    f"new code {new_code} not found in activity, "
                    "within_city_transfer, or across_city_transfer collections"
                )
                report_rows.append(report)
                counts["skipped"] += 1
                continue

            collection_kind, resolved_doc = lookup

            if collection_kind == "activity":
                new_item = build_itinerary_activity_item(resolved_doc, new_code, item)
                new_title = new_item.get("name", "")
            else:
                new_item = build_itinerary_transfer_item(resolved_doc, collection_kind)
                new_title = new_item.get("title", "")

            itinerary_items[item_idx] = new_item
            doc_changed = True

            collection_label = {
                "activity": "activity",
                "within": "within_city_transfer",
                "across": "across_city_transfer",
            }[collection_kind]

            report["status"] = (
                f"queued_replace_{match_kind}" if dry_run else f"replaced_{match_kind}"
            )
            report["reason"] = (
                f"matched on {match_kind} ({found_module_id})"
                if match_kind == "new_code"
                else ""
            )
            report["resolved_collection"] = collection_label
            report["new_title"] = new_title
            report["new_activity_id"] = str(resolved_doc["_id"])
            report_rows.append(report)
            counts["replaced"] += 1

            resolved_code = (
                activity_code_from_doc(resolved_doc)
                if collection_kind == "activity"
                else clean_str(resolved_doc.get("jarvis_id"))
            ) or new_code
            log.info(
                "%s %s (%s) day=%s idx=%s [%s]: %s -> %s (%s)",
                "QUEUE" if dry_run else "REPLACE",
                slug,
                itinerary_id_str,
                day_index,
                itinerary_index,
                match_kind,
                found_module_id,
                resolved_code,
                collection_label,
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
    log.info("  replacements applied:  %d", counts["replaced"])
    log.info("  rows skipped:          %d", counts["skipped"])
    log.info("  itineraries touched:   %d", counts["itineraries_updated"])

    write_report(report_rows, dry_run=dry_run, log=log)
    client.close()

    if dry_run:
        log.info("DRY RUN complete. Set DRY_RUN=False to write changes.")


if __name__ == "__main__":
    run(dry_run=DRY_RUN)
