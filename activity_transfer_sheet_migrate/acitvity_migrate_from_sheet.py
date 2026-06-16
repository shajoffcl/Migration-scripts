"""
Migrate activities from the static + pricing CSV sheets into three Mongo
collections in the activity DB:

  - activity        (one doc per CSV row, generated _id)
  - activity_image  (one doc per activity using the same image as the hero)
  - activity_price  (rows from the pricing CSV mapped to slab schema)

Toggle DRY_RUN below to switch between dry-run and actual inserts.
"""

import csv
import logging
import math
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# ----------------------------- Run configuration ----------------------------
# Flip this to False when you want to actually write to the DB.
DRY_RUN = False

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ACTIVITY_CSV = os.path.join(THIS_DIR, "content", "activity_static_migrate_sheet_v2.csv")
PRICING_CSV = os.path.join(THIS_DIR, "content", "act_and_transfer_pricing_v2.csv")
REPORTS_DIR = os.path.join(THIS_DIR, "reports")

MONGO_URI = os.getenv("MONGO_URI")
ACTIVITIES_DB = os.getenv("ACTIVITIES_DB") or "ht_activity_db"
LOCATION_DB = os.getenv("LOCATION_DB") or "ht_location_db"

IMAGE_BASE_URL = "https://cdn.holidaytribe.ai/website/activity"


# ----------------------------- Allowed values -------------------------------

ALLOWED_TRANSFER = {None, "", "private", "shared"}

ALLOWED_TAGS = {
    "Air, Helicopter & Balloon Tours",
    "Amusement Parks",
    "Art & culture",
    "Bundle",
    "City tours",
    "Classes",
    "Cruise & Water Tours",
    "Day Trips & Excursions",
    "Gastronomy & nightlife",
    "Outdoor activities & Adventure",
    "Shows, sports and special events",
    "Spa & Wellness",
    "Specialist tours",
    "Tickets & Attraction Passes",
    "Tour & activities",
    "Transport & Rentals",
    "Travel Services",
    "Zoo, Aquarium & Nature",
    "Shows"
}

ALLOWED_TRAVEL_THEME = {
    "ADVENTURE", "BEACH", "CITY", "CULTURE", "FESTIVAL", "FOOD", "HISTORY",
    "LUXURY", "MOUNTAIN", "NATURE", "NIGHT_LIFE", "RELAXATION", "ROMANTIC",
    "SELF_DRIVE", "SHOPPING", "SNOW", "SPIRITUAL", "WILDLIFE", "THRILL_SEEKER",
    "WATER_BABY", "FASHIONISTA", "LIFESTYLE_CURATOR",
}

ALLOWED_TRAVEL_GROUP = {"FAMILY", "COUPLE", "FRIENDS", "SOLO"}

# Map the cased forms that show up in the sheet to the enum values.
TRAVEL_GROUP_ALIASES = {
    "FAMILY": "FAMILY",
    "COUPLE": "COUPLE",
    "COUPLES": "COUPLE",
    "FRIEND": "FRIENDS",
    "FRIENDS": "FRIENDS",
    "SOLO": "SOLO",
}

ALLOWED_CMS_ACTIVITY_TYPE = {
    "ACTIVITY_WITH_ONLY_TICKET",
    "ACTIVITY_WITH_PRIVATE_TRANSFER",
    "ACTIVITY_WITH_SHARED_TRANSFER",
}

# -------------------------- Cache price configuration -----------------------

TICKET_AGE_CONFIG = 30
EXPECTED_NUMBER_OF_PEOPLE = 2

# Temporary forex mapping (currency -> INR). Matches transfer_price.py rates.
CURRENCY_TO_INR = {
    "INR": 1.0,
    "IDR": 0.0054,
    "CHF": 124.2386,
    "AUD": 69.9576,
    "MYR": 25.2865,
    "AED": 26.9757,
    "EUR": 114.6699,
    "THB": 3.0694,
    "SGD": 76.6732,
    "USD": 97.5307,
    "GBP": 133.4665,
}

FALLBACK_CACHE_PRICE = {"price": 0.0, "markup_price": 0.0}

# When the sheet city is "Nationwide", map the activity to every city in that country.
NATIONWIDE_CITY_NAME = "nationwide"


# ------------------------------- Utilities ----------------------------------

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def clean_str(value: Any) -> str:
    """Trim and normalize a CSV cell to a plain string. NaN/None -> ''."""
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def split_lines(value: Any) -> list[str]:
    """Split a multiline cell on newlines, drop blanks, trim each item."""
    text = clean_str(value)
    if not text:
        return []
    parts = re.split(r"[\r\n]+", text)
    return [p.strip() for p in parts if p.strip()]


def split_csv_list(value: Any) -> list[str]:
    """Split a comma-separated cell into trimmed items."""
    text = clean_str(value)
    if not text:
        return []
    return [p.strip() for p in text.split(",") if p.strip()]


def normalize_transfer(value: Any) -> Optional[str]:
    """Normalize transfer value. 'none' -> '' which is allowed."""
    text = clean_str(value).lower()
    if text in {"", "none"}:
        return ""
    if text in {"private", "shared"}:
        return text
    return None  # signal "invalid" by returning sentinel handled by caller


def build_image_cdn_url(image_code: str) -> str:
    """Build the CDN URL from the image code in the sheet's images column."""
    return f"{IMAGE_BASE_URL}/{clean_str(image_code)}.webp"


def parse_sheet_images(raw: Any) -> Optional[dict[str, str]]:
    """Parse `[{img_url=CODE, img_alt=TEXT}]` from the sheet images column."""
    text = clean_str(raw)
    if not text:
        return None

    url_match = re.search(r"img_url=([^,}\]]+)", text, flags=re.IGNORECASE)
    if not url_match:
        return None

    image_code = url_match.group(1).strip()
    alt_match = re.search(r"img_alt=([^}\]]+)", text, flags=re.IGNORECASE)
    img_alt = alt_match.group(1).strip() if alt_match else ""

    return {
        "img_url": build_image_cdn_url(image_code),
        "img_alt": img_alt,
    }


def resolve_hero_image(row: dict[str, Any], activity_code: str, title: str) -> dict[str, str]:
    """Resolve hero image from the sheet; fall back to activity code if missing."""
    parsed = parse_sheet_images(row.get("images"))
    if parsed:
        return parsed
    return {
        "img_url": build_image_cdn_url(activity_code),
        "img_alt": title,
    }


# ----------------------------- DB connections -------------------------------

def get_db_clients() -> tuple[MongoClient, Any, Any]:
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI env var is not set")
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        tlsAllowInvalidCertificates=True,
        tlsAllowInvalidHostnames=True,
    )
    activity_db = client[ACTIVITIES_DB]
    location_db = client[LOCATION_DB]
    return client, activity_db, location_db


# --------------------------- Lookups & validations --------------------------

def lookup_city(city_collection, name: str) -> Optional[dict]:
    if not name:
        return None
    return city_collection.find_one({"name": name})


def lookup_country(country_collection, name: str) -> Optional[dict]:
    if not name:
        return None
    return country_collection.find_one({"name": name})


def lookup_cities_by_country(city_collection, country_id: ObjectId) -> list[dict]:
    """Return all cities belonging to the given country."""
    return list(city_collection.find({"country_id": country_id}))


def lookup_cities(
    city_collection,
    raw: str,
    country_doc: Optional[dict] = None,
) -> tuple[list[dict], list[str]]:
    """Resolve a comma-separated list of city names against the city collection.

    If a token is "Nationwide", expand it to every city in ``country_doc``'s country.
    Tokens that match the country name (e.g. "Nationwide, Japan") are ignored.

    Returns (found_docs, missing_names). The order of found_docs mirrors the order
    in the sheet, with nationwide cities appended in DB order.
    """
    names = split_csv_list(raw)
    found: list[dict] = []
    missing: list[str] = []
    seen_ids: set[ObjectId] = set()
    country_name_lower = clean_str(country_doc.get("name") if country_doc else "").lower()

    for name in names:
        if name.lower() == NATIONWIDE_CITY_NAME:
            if not country_doc:
                missing.append(name)
                continue
            nationwide_cities = lookup_cities_by_country(city_collection, country_doc["_id"])
            if not nationwide_cities:
                missing.append(name)
                continue
            for doc in nationwide_cities:
                if doc["_id"] not in seen_ids:
                    seen_ids.add(doc["_id"])
                    found.append(doc)
            continue

        if country_name_lower and name.lower() == country_name_lower:
            continue

        doc = lookup_city(city_collection, name)
        if doc:
            if doc["_id"] not in seen_ids:
                seen_ids.add(doc["_id"])
                found.append(doc)
        else:
            missing.append(name)
    return found, missing


def validate_tags(raw: list[str]) -> tuple[list[str], list[str]]:
    """Returns (valid_tags, invalid_tags). 'none' is treated as no tag."""
    valid: list[str] = []
    invalid: list[str] = []
    for tag in raw:
        if tag.lower() == "none":
            continue
        if tag in ALLOWED_TAGS:
            valid.append(tag)
        else:
            invalid.append(tag)
    return valid, invalid


def validate_travel_theme(raw: list[str]) -> tuple[list[str], list[str]]:
    valid: list[str] = []
    invalid: list[str] = []
    for theme in raw:
        upper = theme.upper().replace(" ", "_")
        if upper in ALLOWED_TRAVEL_THEME:
            if upper not in valid:
                valid.append(upper)
        else:
            invalid.append(theme)
    return valid, invalid


def validate_travel_group(raw: list[str]) -> tuple[list[str], list[str]]:
    valid: list[str] = []
    invalid: list[str] = []
    for group in raw:
        upper = group.upper().strip()
        mapped = TRAVEL_GROUP_ALIASES.get(upper)
        if mapped and mapped in ALLOWED_TRAVEL_GROUP:
            if mapped not in valid:
                valid.append(mapped)
        else:
            invalid.append(group)
    return valid, invalid


# --------------------------- Cache price computation ------------------------

def _normalize_currency(raw: Any) -> str:
    if raw is None:
        return "INR"
    cur = str(raw).strip().upper()
    if "-" in cur:
        cur = cur.split("-", 1)[0].strip()
    return cur or "INR"


def _round_off(value: float, decimals: int = 2) -> float:
    return round(value, decimals)


def _threshold_in_range(
    slab: dict[str, Any],
    value: int,
    min_key: str = "min_threshold",
    max_key: str = "max_threshold",
) -> bool:
    min_val = slab.get(min_key)
    max_val = slab.get(max_key)
    if min_val is None or max_val is None:
        return False
    return min_val <= value <= max_val


def compute_total_price(slabs: list[dict[str, Any]]) -> float:
    """Mirror CMSCachePriceProvider.computeTotalPrice using activity_price slabs."""
    ticket_slabs = [s for s in slabs if s.get("slab_type") == "ticket"]
    group_slabs = [s for s in slabs if s.get("slab_type") == "group"]
    transfer_slabs = [s for s in slabs if s.get("slab_type") == "transfer"]

    total_price = 0.0

    if ticket_slabs:
        valid_slabs = [
            s for s in ticket_slabs
            if s.get("price_per_person") is not None and s["price_per_person"] > 0
        ]
        for slab in valid_slabs:
            if _threshold_in_range(slab, TICKET_AGE_CONFIG):
                total_price += slab["price_per_person"]
                break

    if group_slabs:
        valid_slabs = [
            s for s in group_slabs
            if s.get("price_per_person") is not None and s["price_per_person"] > 0
        ]
        matching_slab = next(
            (
                s for s in valid_slabs
                if _threshold_in_range(s, EXPECTED_NUMBER_OF_PEOPLE)
            ),
            None,
        )
        if matching_slab:
            total_price += matching_slab["price_per_person"]

    if transfer_slabs:
        valid_slabs = [
            s for s in transfer_slabs
            if s.get("price_per_vehicle") is not None and s["price_per_vehicle"] > 0
        ]
        matching_slab = next(
            (
                s for s in valid_slabs
                if _threshold_in_range(s, EXPECTED_NUMBER_OF_PEOPLE)
            ),
            None,
        )
        if matching_slab:
            total_price += _round_off(
                matching_slab["price_per_vehicle"] / EXPECTED_NUMBER_OF_PEOPLE, 2
            )

    return total_price


def get_forex_rate(currency: str) -> Optional[float]:
    normalized = _normalize_currency(currency)
    if normalized == "INR":
        return 1.0
    return CURRENCY_TO_INR.get(normalized)


def load_activity_markup_by_country(markup_collection: Any) -> dict[str, float]:
    """Load {country_id_str: markup_percentage} from activity_markup collection."""
    markup_by_country: dict[str, float] = {}
    for doc in markup_collection.find({}):
        country = doc.get("country")
        if not isinstance(country, dict):
            continue
        country_id = country.get("_id")
        pct = doc.get("markup_percentage")
        if country_id is None or pct is None:
            continue
        try:
            markup_by_country[str(country_id)] = float(pct)
        except (TypeError, ValueError):
            continue
    return markup_by_country


def build_cache_price(
    price_slabs: list[dict[str, Any]],
    country_id: Optional[ObjectId] = None,
    markup_by_country: Optional[dict[str, float]] = None,
) -> dict[str, float]:
    """Compute cache_price in INR from pricing slabs (CMSCachePriceProvider logic)."""
    if not price_slabs:
        return dict(FALLBACK_CACHE_PRICE)

    base_price = compute_total_price(price_slabs)
    if base_price <= 0:
        return dict(FALLBACK_CACHE_PRICE)

    currency = price_slabs[0].get("currency")
    if not currency:
        return dict(FALLBACK_CACHE_PRICE)

    forex_rate = get_forex_rate(currency)
    if forex_rate is None:
        return dict(FALLBACK_CACHE_PRICE)

    price_inr = _round_off(base_price * forex_rate, 2)
    markup_pct = None
    if country_id and markup_by_country:
        markup_pct = markup_by_country.get(str(country_id))
    markup_price = (
        _round_off(price_inr * (markup_pct / 100.0), 2)
        if markup_pct is not None
        else 0.0
    )
    return {"price": price_inr, "markup_price": markup_price}


# ---------------------------- Document builders -----------------------------

def build_activity_doc(
    row: dict[str, Any],
    activity_id: ObjectId,
    city_docs: list[dict],
    country_doc: dict,
    cache_price: Optional[dict[str, float]] = None,
    hero_image: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    activity_code = clean_str(row.get("ID"))
    title = clean_str(row.get("title"))
    seller_title = clean_str(row.get("seller_title")) or title
    description = clean_str(row.get("description"))
    additional_details = clean_str(row.get("additional_details"))
    cancellation = clean_str(row.get("cancellation"))
    cms_activity_type = clean_str(row.get("cms_activity_type"))
    transfer = normalize_transfer(row.get("transfer"))

    image = hero_image or resolve_hero_image(row, activity_code, title)

    inclusions = split_lines(row.get("inclusions"))
    exclusions = split_lines(row.get("exclusions"))

    tags, _ = validate_tags(split_csv_list(row.get("tags")))
    travel_theme, _ = validate_travel_theme(split_csv_list(row.get("travel_theme")))
    travel_group, _ = validate_travel_group(split_csv_list(row.get("travel_group")))

    now = utcnow()
    return {
        "_id": activity_id,
        "providers": ["cms"],
        "provider_info": {"cms": {"activityCode": activity_code}},
        "title": title,
        "description": description if description else title,
        "duration": 0,
        "images": [{"img_url": image["img_url"], "img_alt": image["img_alt"]}],
        "tags": tags,
        "type": "",
        "transfer": transfer,
        "pickup_instructions": "",
        "additional_details": additional_details,
        "city": [{"_id": c["_id"], "name": c["name"]} for c in city_docs],
        "country": {"_id": country_doc["_id"], "name": country_doc["name"]},
        "destination": {"_id": None, "name": None},
        "day_time": [],
        "inclusions": inclusions,
        "exclusions": exclusions,
        "travel_theme": travel_theme,
        "cancellation": cancellation,
        "travel_group": travel_group,
        "starting_point": None,
        "ending_point": None,
        "highlights": [],
        "seller_title": seller_title,
        "cms_activity_type": cms_activity_type,
        "cms_status": 1,
        "cache_price": cache_price or dict(FALLBACK_CACHE_PRICE),
        "created_at": now,
        "updated_at": now,
    }


def build_image_doc(activity_id: ObjectId, hero_image: dict[str, str]) -> dict[str, Any]:
    now = utcnow()
    return {
        "_id": ObjectId(),
        "img_url": hero_image["img_url"],
        "img_alt": "",
        "activity_id": activity_id,
        "created_at": now,
        "updated_at": now,
    }


def build_price_doc(activity_id: ObjectId, row: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Build a single activity_price doc from a pricing CSV row."""
    currency = clean_str(row.get("currency")) or "INR"
    slab_type_raw = clean_str(row.get("slab_type")).lower()
    if slab_type_raw not in {"ticket", "transfer", "group"}:
        slab_type_raw = "ticket"

    vehicle_type_raw = clean_str(row.get("vehicle_type")).lower()
    vehicle_type: Optional[str] = vehicle_type_raw if vehicle_type_raw and vehicle_type_raw != "none" else None

    def _to_int(v: Any) -> Optional[int]:
        s = clean_str(v)
        if s == "":
            return None
        try:
            return int(float(s))
        except ValueError:
            return None

    def _to_num(v: Any) -> Optional[float]:
        s = clean_str(v)
        if s == "":
            return None
        try:
            return float(s)
        except ValueError:
            return None

    min_threshold = _to_int(row.get("min_threshold"))
    max_threshold = _to_int(row.get("max_threshold"))
    price_per_person = _to_num(row.get("price_per_person"))
    price_per_vehicle = _to_num(row.get("price_per_vehicle"))

    return {
        "_id": ObjectId(),
        "activity_id": activity_id,
        "vehicle_type": vehicle_type,
        "slab_type": slab_type_raw,
        "min_threshold": min_threshold,
        "max_threshold": max_threshold,
        "price_per_vehicle": price_per_vehicle,
        "price_per_person": price_per_person,
        "currency": currency,
        "price_per_person_in_inr": None,
        "price_per_vehicle_in_inr": None,
    }


# ------------------------------ Main pipeline -------------------------------

def read_activity_rows(path: str) -> list[dict[str, Any]]:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return df.to_dict(orient="records")


def read_price_rows(path: str) -> dict[str, list[dict[str, Any]]]:
    """Returns {activity_code -> [price_row, ...]}."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = clean_str(row.get("Activity ID"))
            if not code:
                continue
            grouped.setdefault(code, []).append(row)
    return grouped


def validate_row(row: dict[str, Any]) -> tuple[bool, list[str]]:
    """Returns (is_valid, list_of_reasons)."""
    reasons: list[str] = []
    activity_code = clean_str(row.get("ID"))
    if not activity_code:
        reasons.append("missing ID")
    if not clean_str(row.get("title")):
        reasons.append("missing title")

    transfer = normalize_transfer(row.get("transfer"))
    if transfer is None:
        reasons.append(f"invalid transfer={row.get('transfer')!r}")

    cms_type = clean_str(row.get("cms_activity_type"))
    if cms_type not in ALLOWED_CMS_ACTIVITY_TYPE:
        reasons.append(f"invalid cms_activity_type={cms_type!r}")

    tags, invalid_tags = validate_tags(split_csv_list(row.get("tags")))
    if invalid_tags:
        reasons.append(f"invalid tags={invalid_tags}")

    _, invalid_themes = validate_travel_theme(split_csv_list(row.get("travel_theme")))
    if invalid_themes:
        reasons.append(f"invalid travel_theme={invalid_themes}")

    _, invalid_groups = validate_travel_group(split_csv_list(row.get("travel_group")))
    if invalid_groups:
        reasons.append(f"invalid travel_group={invalid_groups}")

    return (len(reasons) == 0), reasons


REPORT_HEADERS = [
    "activity_code",
    "title",
    "status",
    "reason",
    "activity_id",
    "city_csv",
    "city_resolved",
    "country_csv",
    "country_resolved",
    "price_rows",
    "cache_price",
    "cache_price_markup",
    "mode",
]


def write_report(rows: list[dict[str, Any]], dry_run: bool, log: logging.Logger) -> str:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    mode = "dry_run" if dry_run else "live"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(REPORTS_DIR, f"activity_migration_report_{mode}_{timestamp}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in REPORT_HEADERS})
    log.info("Report written: %s", path)
    return path


def run(dry_run: bool) -> None:
    setup_logging()
    log = logging.getLogger("migrate")
    log.info("Reading CSVs (dry_run=%s)", dry_run)

    activity_rows = read_activity_rows(ACTIVITY_CSV)
    price_groups = read_price_rows(PRICING_CSV)
    log.info("Activity rows: %d | Pricing groups: %d", len(activity_rows), len(price_groups))

    client, activity_db, location_db = get_db_clients()
    activity_coll = activity_db["activity"]
    activity_image_coll = activity_db["activity_image"]
    activity_price_coll = activity_db["activity_price"]
    activity_markup_coll = activity_db["activity_markup"]
    city_coll = location_db["city"]
    country_coll = location_db["country"]

    markup_by_country = load_activity_markup_by_country(activity_markup_coll)
    log.info("Loaded activity markup for %d countries", len(markup_by_country))

    # New activities to insert (no existing doc by this activityCode).
    insert_activity_docs: list[dict[str, Any]] = []
    # Existing activities to replace in place (preserves _id + created_at).
    update_activity_docs: list[dict[str, Any]] = []
    # Activity ids whose image + price docs should be wiped and re-inserted.
    update_activity_ids: list[ObjectId] = []
    # Image and price docs to insert; they reference whichever _id we settled on.
    image_docs: list[dict[str, Any]] = []
    price_docs: list[dict[str, Any]] = []
    report_rows: list[dict[str, Any]] = []

    counts = {
        "queued_insert": 0,
        "queued_update": 0,
        "skipped_validation": 0,
        "skipped_location": 0,
    }
    mode_label = "dry_run" if dry_run else "live"

    for row in activity_rows:
        code = clean_str(row.get("ID"))
        title = clean_str(row.get("title"))
        city_raw = clean_str(row.get("city"))
        country_name = clean_str(row.get("country"))
        if not code:
            continue

        report_row: dict[str, Any] = {
            "activity_code": code,
            "title": title,
            "status": "",
            "reason": "",
            "activity_id": "",
            "city_csv": city_raw,
            "city_resolved": "",
            "country_csv": country_name,
            "country_resolved": "",
            "price_rows": 0,
            "mode": mode_label,
        }

        is_valid, reasons = validate_row(row)
        if not is_valid:
            report_row["status"] = "skipped_validation"
            report_row["reason"] = "; ".join(reasons)
            report_rows.append(report_row)
            counts["skipped_validation"] += 1
            log.warning("SKIP %s -> validation failed: %s", code, "; ".join(reasons))
            continue

        country_doc = lookup_country(country_coll, country_name)
        city_docs, missing_cities = lookup_cities(city_coll, city_raw, country_doc=country_doc)
        report_row["city_resolved"] = ", ".join(c["name"] for c in city_docs)
        report_row["country_resolved"] = country_doc["name"] if country_doc else ""

        if not city_docs or missing_cities or not country_doc:
            reason = (
                f"city missing={missing_cities} resolved={[c['name'] for c in city_docs]}, "
                f"country found={bool(country_doc)}"
            )
            report_row["status"] = "skipped_location"
            report_row["reason"] = reason
            report_rows.append(report_row)
            counts["skipped_location"] += 1
            log.warning("SKIP %s -> %s", code, reason)
            continue

        existing = activity_coll.find_one(
            {"provider_info.cms.activityCode": code},
            {"_id": 1, "created_at": 1},
        )
        is_update = existing is not None
        activity_id = existing["_id"] if is_update else ObjectId()

        prices_for_code = price_groups.get(code, [])
        activity_price_docs_for_code: list[dict[str, Any]] = []
        for p_row in prices_for_code:
            p_doc = build_price_doc(activity_id, p_row)
            if p_doc:
                price_docs.append(p_doc)
                activity_price_docs_for_code.append(p_doc)

        cache_price = build_cache_price(
            activity_price_docs_for_code,
            country_id=country_doc["_id"],
            markup_by_country=markup_by_country,
        )
        hero_image = resolve_hero_image(row, code, title)
        activity_doc = build_activity_doc(
            row, activity_id, city_docs, country_doc,
            cache_price=cache_price, hero_image=hero_image,
        )
        if is_update and existing.get("created_at"):
            activity_doc["created_at"] = existing["created_at"]

        image_doc = build_image_doc(activity_id, hero_image)
        image_docs.append(image_doc)

        if is_update:
            update_activity_docs.append(activity_doc)
            update_activity_ids.append(activity_id)
            counts["queued_update"] += 1
            report_row["status"] = "queued_update" if dry_run else "updated"
        else:
            insert_activity_docs.append(activity_doc)
            counts["queued_insert"] += 1
            report_row["status"] = "queued_insert" if dry_run else "inserted"

        report_row["activity_id"] = str(activity_id)
        report_row["price_rows"] = len(prices_for_code)
        report_row["cache_price"] = cache_price.get("price", 0)
        report_row["cache_price_markup"] = cache_price.get("markup_price", 0)
        report_rows.append(report_row)
        log.info(
            "%s %s -> activity=1, image=1, prices=%d",
            ("QUEUE_UPDATE" if is_update else "QUEUE_INSERT") if dry_run else (
                "UPDATE" if is_update else "INSERT"
            ),
            code,
            len(prices_for_code),
        )

    log.info("Summary:")
    log.info("  activities to insert: %d", len(insert_activity_docs))
    log.info("  activities to update: %d", len(update_activity_docs))
    log.info("  images to insert:     %d", len(image_docs))
    log.info("  prices to insert:     %d", len(price_docs))
    log.info("  skipped (validation): %d", counts["skipped_validation"])
    log.info("  skipped (location):   %d", counts["skipped_location"])

    if dry_run:
        log.info("DRY RUN: skipping all writes. Set DRY_RUN=False at the top of the file to insert.")
        write_report(report_rows, dry_run=True, log=log)
        client.close()
        return

    # 1. Insert brand-new activities.
    if insert_activity_docs:
        result = activity_coll.insert_many(insert_activity_docs, ordered=False)
        log.info("Inserted %d new activities", len(result.inserted_ids))

    # 2. Replace existing activity docs in place.
    if update_activity_docs:
        for doc in update_activity_docs:
            activity_coll.replace_one({"_id": doc["_id"]}, doc)
        log.info("Replaced %d existing activities", len(update_activity_docs))

    # 3. Wipe old image + price docs for the activities we are updating,
    #    then insert the fresh set.
    if update_activity_ids:
        img_del = activity_image_coll.delete_many({"activity_id": {"$in": update_activity_ids}})
        prc_del = activity_price_coll.delete_many({"activity_id": {"$in": update_activity_ids}})
        log.info(
            "Cleaned old docs for %d updated activities: images=%d, prices=%d",
            len(update_activity_ids), img_del.deleted_count, prc_del.deleted_count,
        )

    if image_docs:
        result = activity_image_coll.insert_many(image_docs, ordered=False)
        log.info("Inserted %d images", len(result.inserted_ids))
    if price_docs:
        result = activity_price_coll.insert_many(price_docs, ordered=False)
        log.info("Inserted %d prices", len(result.inserted_ids))

    write_report(report_rows, dry_run=False, log=log)
    client.close()
    log.info("Done.")


if __name__ == "__main__":
    run(dry_run=DRY_RUN)
