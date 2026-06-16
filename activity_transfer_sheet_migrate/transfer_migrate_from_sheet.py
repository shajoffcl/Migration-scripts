"""
Migrate transfers from the static + pricing CSV sheets into four Mongo
collections in the transfer DB:

  - within_city_transfer        (one doc per CSV row whose Within/InterCity = "Within City")
  - across_city_transfer        (one doc per CSV row whose Within/InterCity = "Intercity";
    city lookup uses From/To, then falls back to from_city/to_city columns)
  - within_city_transfer_slab   (pricing rows for within-city transfers)
  - across_city_transfer_slab   (pricing rows for across-city transfers)

``cache_price`` on each transfer doc is computed from its slabs (same logic as
``db_updation_scripts/transfer_changes/transfer_price.py``): ticket slab for age
30 + vehicle slab for 2 pax (price/2), converted to INR, plus country markup.

For each CSV row we look for an existing transfer by jarvis_id:
  - If it exists in the *correct* target collection -> replace the doc in
    place (preserves _id + created_at) and wipe + re-insert its slabs.
  - If it exists in the *other* collection -> delete the old doc and its
    slabs, then insert into the correct target.
  - Otherwise -> plain insert.

Toggle DRY_RUN below to switch between dry-run and actual writes.
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
TRANSFER_CSV = os.path.join(THIS_DIR, "content", "transfer_static_migrate_sheet_v2.csv")
PRICING_CSV = os.path.join(THIS_DIR, "content", "act_and_transfer_pricing_v2.csv")
REPORTS_DIR = os.path.join(THIS_DIR, "reports")

MONGO_URI = os.getenv("MONGO_URI")
TRANSFER_DB = os.getenv("TRANSFER_DB") or "ht_transfer_db"
LOCATION_DB = os.getenv("LOCATION_DB") or "ht_location_db"

TRANSFER_IMAGE_BASE_URL = "https://cdn.holidaytribe.ai/website/activity"

# cache_price (same rules as db_updation_scripts/transfer_changes/transfer_price.py)
TICKET_AGE_CONFIG = 30
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
MARKUP_FIELDS = (
    "markup_percentage",
    "markup_percent",
    "markup",
    "percentage",
    "percent",
)
FALLBACK_CACHE_PRICE = {"price": 0.0, "markup_price": 0.0}


# ----------------------------- Allowed values -------------------------------

ALLOWED_SHARING_BASIS = {"none", "private", "sharing"}

# Maps the raw value from the `transfer` column of the sheet to one of the
# allowed sharing_basis values.
SHARING_BASIS_ALIASES = {
    "": "none",
    "none": "none",
    "private": "private",
    "shared": "sharing",
    "sharing": "sharing",
}

# Map "Transfer Type" column (Train / Group Shuttle / Private / etc.)
# to a (medium, category) tuple for across_city_transfer schema.
TRANSFER_TYPE_TO_MEDIUM_CATEGORY = {
    "train": ("rail", "train"),
    "group shuttle": ("road", "bus"),
    "shared shuttle": ("road", "bus"),
    "shuttle": ("road", "bus"),
    "private": ("road", "car"),
    "ferry": ("water", "ferry"),
    "speed boat": ("water", "speed_boat"),
    "sea plane": ("air", "sea_plane"),
}

TRANSFER_TYPE_TO_SHARING_BASIS = {
    "private": "private",
    "train": "sharing",
    "group shuttle" : "sharing",
}


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


def extract_short_id(code: str) -> str:
    """Return the part after the underscore, e.g. TRA_57347 -> 57347."""
    code = clean_str(code)
    if "_" in code:
        return code.split("_", 1)[1]
    return code


def is_within_city(value: Any) -> bool:
    """True if the Within/InterCity column says it's a within-city transfer."""
    v = clean_str(value).lower()
    return "within" in v


def build_transfer_image_cdn_url(image_code: str) -> str:
    """Build CDN hero URL from an image code (sheet img_url or transfer ID)."""
    return f"{TRANSFER_IMAGE_BASE_URL}/{clean_str(image_code)}.webp"


def parse_sheet_images_code(raw: Any) -> Optional[str]:
    """Parse image code from ``[{img_url=CODE, img_alt=TEXT}]`` in the images column."""
    text = clean_str(raw)
    if not text:
        return None
    url_match = re.search(r"img_url=([^,}\]]+)", text, flags=re.IGNORECASE)
    if not url_match:
        return None
    return url_match.group(1).strip()


def resolve_hero_image(row: dict[str, Any], transfer_code: str) -> str:
    """Hero image URL: sheet images column first, else transfer jarvis ID."""
    image_code = parse_sheet_images_code(row.get("images"))
    if image_code:
        return build_transfer_image_cdn_url(image_code)
    return build_transfer_image_cdn_url(transfer_code)


def normalize_sharing_basis(value: Any) -> Optional[str]:
    """Map the sheet's `transfer` column to one of {none, private, sharing}.

    Returns None if the value is invalid.
    """
    v = clean_str(value).lower()
    mapped = SHARING_BASIS_ALIASES.get(v)
    if mapped in ALLOWED_SHARING_BASIS:
        return mapped
    return None


def map_medium_category(transfer_type_raw: Any) -> tuple[str, str]:
    """Map a free-form 'Transfer Type' value to (medium, category)."""
    t = clean_str(transfer_type_raw).lower()
    if t in TRANSFER_TYPE_TO_MEDIUM_CATEGORY:
        return TRANSFER_TYPE_TO_MEDIUM_CATEGORY[t]
    if "train" in t or "rail" in t:
        return "rail", "train"
    if "shuttle" in t or "bus" in t or "coach" in t:
        return "road", "bus"
    if "ferry" in t:
        return "water", "ferry"
    if "private" in t or "car" in t or "taxi" in t:
        return "road", "car"
    return "road", "car"


def parse_cancellation_policy(value: Any) -> str:
    """Convert a CSV cancellation cell into the stored string schema.

    Known inputs and their outputs:
      ''                  -> ''
      'Non-Refundable'    -> "{'valid_before': '0 Days', 'percentage': 0}"
      'D-1' / 'D-7' / ... -> "{'valid_before': '<n> Days', 'percentage': 100}"
    Anything we don't recognise is returned as the empty string so we don't
    pollute the DB with random text.
    """
    v = clean_str(value)
    if not v:
        return ""
    lower = v.lower()
    if "non" in lower and "refund" in lower:
        return "{'valid_before': '0 Days', 'percentage': 0}"
    m = re.match(r"^\s*d[-\s]?(\d+)\s*$", lower)
    if m:
        days = int(m.group(1))
        return f"{{'valid_before': '{days} Days', 'percentage': 100}}"
    return ""


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
    transfer_db = client[TRANSFER_DB]
    location_db = client[LOCATION_DB]
    return client, transfer_db, location_db


# --------------------------- Lookups & caches -------------------------------

class LocationCache:
    """Small in-memory cache to avoid repeated lookups for the same city/country."""

    def __init__(self, city_coll, country_coll) -> None:
        self.city_coll = city_coll
        self.country_coll = country_coll
        self._city_cache: dict[str, Optional[dict]] = {}
        self._country_cache: dict[str, Optional[dict]] = {}

    def city(self, name: str) -> Optional[dict]:
        name = (name or "").strip()
        if not name:
            return None
        if name in self._city_cache:
            return self._city_cache[name]
        doc = self.city_coll.find_one({"name": name})
        self._city_cache[name] = doc
        return doc

    def cities(self, raw: str) -> tuple[list[dict], list[str]]:
        names = split_csv_list(raw)
        found: list[dict] = []
        missing: list[str] = []
        seen_ids: set[ObjectId] = set()
        for name in names:
            doc = self.city(name)
            if doc:
                if doc["_id"] not in seen_ids:
                    seen_ids.add(doc["_id"])
                    found.append(doc)
            else:
                missing.append(name)
        return found, missing

    def country(self, name: str) -> Optional[dict]:
        name = (name or "").strip()
        if not name:
            return None
        if name in self._country_cache:
            return self._country_cache[name]
        doc = self.country_coll.find_one({"name": name})
        self._country_cache[name] = doc
        return doc


def resolve_across_city_side(
    cache: LocationCache,
    row: dict[str, Any],
    side: str,
    code: str,
    log: logging.Logger,
) -> tuple[Optional[dict], str]:
    """Resolve a city for intercity transfers: try From/To first, then from_city/to_city.

    Returns (city_doc, lookup_source) where lookup_source is one of
    ``from``, ``to``, ``from_city``, ``to_city``, or ``""`` if not found.
    """
    if side == "from":
        primary_col, fallback_col = "From", "from_city"
    else:
        primary_col, fallback_col = "To", "to_city"

    primary_name = clean_str(row.get(primary_col))
    if primary_name:
        doc = cache.city(primary_name)
        if doc:
            return doc, primary_col.lower()

    fallback_name = clean_str(row.get(fallback_col))
    if fallback_name:
        doc = cache.city(fallback_name)
        if doc:
            log.info(
                "%s: %s not found via %s=%r; fallback %s=%r -> %s",
                code,
                side,
                primary_col,
                primary_name,
                fallback_col,
                fallback_name,
                doc["name"],
            )
            return doc, fallback_col

    return None, ""


# ---------------------------- Document builders -----------------------------

def build_within_city_doc(
    row: dict[str, Any],
    transfer_id: ObjectId,
    city_doc: dict,
    country_doc: dict,
    sharing_basis: str,
) -> dict[str, Any]:
    code = clean_str(row.get("ID"))
    title = clean_str(row.get("title"))
    description = clean_str(row.get("description")) or title
    pickup = clean_str(row.get("From"))
    dropoff = clean_str(row.get("To"))

    inclusions = split_lines(row.get("inclusions"))
    exclusions = split_lines(row.get("exclusions"))
    cancellation_policy = parse_cancellation_policy(row.get("cancellation"))

    hero_image = resolve_hero_image(row, code)

    now = utcnow()
    return {
        "_id": transfer_id,
        "id": extract_short_id(code),
        "title": title,
        "description": description,
        "dropoff": dropoff,
        "pickup": pickup,
        "transfer_type": "within",
        "inclusions": inclusions,
        "exclusion": exclusions,
        "cancellation_policy": cancellation_policy,
        "sharing_basis": sharing_basis,
        "jarvis_id": code,
        "city": {"_id": city_doc["_id"], "name": city_doc["name"]},
        "country": {"_id": country_doc["_id"], "name": country_doc["name"]},
        "hero_image": hero_image,
        "cache_price": {"price": 0, "markup_price": 0},
        "created_at": now,
        "updated_at": now,
    }


def build_across_city_doc(
    row: dict[str, Any],
    transfer_id: ObjectId,
    from_city_doc: dict,
    to_city_doc: dict,
    from_country_doc: dict,
    to_country_doc: dict,
    sharing_basis: str,
) -> dict[str, Any]:
    code = clean_str(row.get("ID"))
    title = clean_str(row.get("title"))
    description = clean_str(row.get("description")) or title
    t_from = clean_str(row.get("From"))
    t_to = clean_str(row.get("To"))

    medium, category = map_medium_category(row.get("Transfer Type"))

    inclusions = split_lines(row.get("inclusions"))
    exclusions = split_lines(row.get("exclusions"))
    cancellation_policy = parse_cancellation_policy(row.get("cancellation"))

    hero_image = resolve_hero_image(row, code)

    now = utcnow()
    return {
        "_id": transfer_id,
        "id": extract_short_id(code),
        "title": title,
        "description": description,
        "t_from": t_from,
        "t_to": t_to,
        "medium": medium,
        "category": category,
        "inclusions": inclusions,
        "exclusion": exclusions,
        "cancellation_policy": cancellation_policy,
        "sharing_basis": sharing_basis,
        "jarvis_id": code,
        "is_within_city": False,
        "from_country": {"_id": from_country_doc["_id"], "name": from_country_doc["name"]},
        "to_country": {"_id": to_country_doc["_id"], "name": to_country_doc["name"]},
        "from_city": {"_id": from_city_doc["_id"], "name": from_city_doc["name"]},
        "to_city": {"_id": to_city_doc["_id"], "name": to_city_doc["name"]},
        "hero_image": hero_image,
        "cache_price": {"price": 0, "markup_price": 0},
        "created_at": now,
        "updated_at": now,
    }


def build_slab_doc(transfer_id: ObjectId, row: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Build a slab doc from a pricing CSV row.

    Sheet -> DB mapping:
      slab_type "transfer" -> "vehicle"; min_threshold/max_threshold -> min_pax/max_pax
      slab_type "ticket"   -> "ticket";  min_threshold/max_threshold -> min_age/max_age

    Returns None if the row can't be parsed into a useful slab.
    """
    currency = clean_str(row.get("currency")) or "USD"
    raw_slab_type = clean_str(row.get("slab_type")).lower()

    vehicle_type_raw = clean_str(row.get("vehicle_type")).lower()
    vehicle_type = "" if vehicle_type_raw in {"", "none"} else vehicle_type_raw

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

    if raw_slab_type == "transfer":
        slab_type = "vehicle"
        price = price_per_vehicle
        min_age, max_age = 0, 99
        min_pax = min_threshold if min_threshold is not None else 0
        max_pax = max_threshold if max_threshold is not None else 99
    elif raw_slab_type == "ticket":
        slab_type = "ticket"
        price = price_per_person
        min_age = min_threshold if min_threshold is not None else 0
        max_age = max_threshold if max_threshold is not None else 99
        min_pax, max_pax = 0, 99
        # tickets aren't tied to a vehicle.
        vehicle_type = ""
    else:
        return None

    if price is None:
        return None

    return {
        "_id": ObjectId(),
        "slab_type": slab_type,
        "price": price,
        "min_age": min_age,
        "max_age": max_age,
        "min_pax": min_pax,
        "max_pax": max_pax,
        "vehicle_type": vehicle_type,
        "currency": currency,
        "transfer_id": transfer_id,
    }


# --------------------------- cache_price from slabs -------------------------

def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _normalize_currency(raw: Any) -> str:
    if raw is None:
        return "INR"
    cur = str(raw).strip().upper()
    if "-" in cur:
        cur = cur.split("-", 1)[0].strip()
    return cur or "INR"


def _country_id_key(country: Any) -> Optional[str]:
    if not isinstance(country, dict):
        return None
    country_id = country.get("_id") or country.get("country_id")
    if not country_id:
        return None
    return str(country_id)


def load_transfer_markup_by_country(markup_collection: Any) -> dict[str, float]:
    markup_by_country: dict[str, float] = {}
    for doc in markup_collection.find({}):
        country_key = _country_id_key(doc.get("country"))
        if not country_key:
            continue
        pct = None
        for field in MARKUP_FIELDS:
            pct = _to_float(doc.get(field))
            if pct is not None:
                break
        if pct is None:
            continue
        markup_by_country[country_key] = pct
    return markup_by_country


def _country_key_for_transfer(transfer_doc: dict[str, Any], is_within_table: bool) -> Optional[str]:
    if is_within_table:
        return _country_id_key(transfer_doc.get("country"))
    return _country_id_key(transfer_doc.get("from_country"))


def _ticket_price_component(slabs: list[dict[str, Any]]) -> float:
    ticket_slabs = [s for s in slabs if s.get("slab_type") == "ticket"]
    valid = [s for s in ticket_slabs if (_to_float(s.get("price")) or 0) > 0]
    if not valid:
        return 0.0

    for slab in valid:
        min_age = _to_float(slab.get("min_age")) or 0
        max_age = _to_float(slab.get("max_age"))
        if max_age is None:
            max_age = min_age
        if min_age <= TICKET_AGE_CONFIG <= max_age:
            return _to_float(slab.get("price")) or 0.0
    return 0.0


def _transfer_price_component(slabs: list[dict[str, Any]]) -> float:
    vehicle_slabs = [s for s in slabs if s.get("slab_type") == "vehicle"]
    valid = [s for s in vehicle_slabs if (_to_float(s.get("price")) or 0) > 0]
    if not valid:
        return 0.0

    applicable_for_two = []
    for slab in valid:
        min_threshold = _to_float(slab.get("min_pax")) or 0.0
        max_threshold = _to_float(slab.get("max_pax"))
        if max_threshold is None:
            max_threshold = min_threshold
        if min_threshold <= 2 <= max_threshold:
            applicable_for_two.append(slab)

    if applicable_for_two:
        chosen_slab = min(
            applicable_for_two,
            key=lambda s: (
                _to_float(s.get("price")) or float("inf"),
                _to_float(s.get("max_pax")) or float("inf"),
                _to_float(s.get("min_pax")) or float("inf"),
            ),
        )
    else:
        chosen_slab = min(
            valid,
            key=lambda s: (
                abs((_to_float(s.get("min_pax")) or 0.0) - 2.0),
                abs(
                    (
                        _to_float(s.get("max_pax"))
                        or (_to_float(s.get("min_pax")) or 0.0)
                    )
                    - 2.0
                ),
                _to_float(s.get("price")) or float("inf"),
            ),
        )

    vehicle_price = _to_float(chosen_slab.get("price")) or 0.0
    return round(vehicle_price / 2.0, 2)


def compute_base_price(slabs: list[dict[str, Any]]) -> tuple[float, str]:
    if not slabs:
        return 0.0, "INR"
    currency = _normalize_currency(slabs[0].get("currency"))
    total_price = _ticket_price_component(slabs) + _transfer_price_component(slabs)
    return total_price, currency


def to_inr(amount: float, currency: str) -> Optional[float]:
    rate = CURRENCY_TO_INR.get(_normalize_currency(currency))
    if rate is None:
        return None
    return round(amount * rate, 2)


def build_cache_price(
    transfer_doc: dict[str, Any],
    slabs: list[dict[str, Any]],
    is_within_table: bool,
    markup_by_country: dict[str, float],
) -> dict[str, float]:
    """Compute cache_price from slabs (ticket + vehicle/2, INR + country markup)."""
    base_price, currency = compute_base_price(slabs)
    if base_price <= 0:
        return dict(FALLBACK_CACHE_PRICE)

    price_inr = to_inr(base_price, currency)
    if price_inr is None:
        return dict(FALLBACK_CACHE_PRICE)

    country_key = _country_key_for_transfer(transfer_doc, is_within_table)
    markup_pct = markup_by_country.get(country_key) if country_key else None
    markup_price = (
        round(price_inr * (markup_pct / 100.0), 2) if markup_pct is not None else 0.0
    )
    return {"price": price_inr, "markup_price": markup_price}


def apply_cache_price(
    transfer_doc: dict[str, Any],
    slabs: list[dict[str, Any]],
    is_within_table: bool,
    markup_by_country: dict[str, float],
) -> dict[str, float]:
    """Set transfer_doc['cache_price'] from slabs and return the value."""
    cache_price = build_cache_price(transfer_doc, slabs, is_within_table, markup_by_country)
    transfer_doc["cache_price"] = cache_price
    return cache_price


# ------------------------------ Main pipeline -------------------------------

def read_transfer_rows(path: str) -> list[dict[str, Any]]:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return df.to_dict(orient="records")


def read_price_rows(path: str) -> dict[str, list[dict[str, Any]]]:
    """Returns {transfer_code -> [price_row, ...]}."""
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
    """Cheap pre-DB validation. Returns (is_valid, reasons)."""
    reasons: list[str] = []
    code = clean_str(row.get("ID"))
    if not code:
        reasons.append("missing ID")
    if not clean_str(row.get("title")):
        reasons.append("missing title")
    if not clean_str(row.get("From")):
        reasons.append("missing From")
    if not clean_str(row.get("To")):
        reasons.append("missing To")
    if not clean_str(row.get("country")):
        reasons.append("missing country")
    if not clean_str(row.get("city")):
        reasons.append("missing city")

    sharing = TRANSFER_TYPE_TO_SHARING_BASIS.get(row.get("Transfer Type","").lower())
    if sharing is None:
        reasons.append(f"invalid transfer={row.get('transfer')!r}")

    within_inter = clean_str(row.get("Within/InterCity"))
    if not within_inter:
        reasons.append("missing Within/InterCity")

    return (len(reasons) == 0), reasons


# --------------------------------- Report -----------------------------------

REPORT_HEADERS = [
    "transfer_code",
    "title",
    "status",
    "reason",
    "transfer_id",
    "target_collection",
    "existing_collection",
    "from_csv",
    "to_csv",
    "from_city_resolved",
    "to_city_resolved",
    "from_city_lookup",
    "to_city_lookup",
    "city_csv",
    "city_resolved",
    "country_csv",
    "country_resolved",
    "price_rows",
    "slab_count",
    "cache_price",
    "cache_markup_price",
    "mode",
]


def write_report(rows: list[dict[str, Any]], dry_run: bool, log: logging.Logger) -> str:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    mode = "dry_run" if dry_run else "live"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(REPORTS_DIR, f"transfer_migration_report_{mode}_{timestamp}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in REPORT_HEADERS})
    log.info("Report written: %s", path)
    return path


# --------------------------- Existing-doc lookup ----------------------------

def find_existing(within_coll, across_coll, code: str) -> Optional[tuple[str, dict]]:
    """Return (collection_name, doc) for the first match by jarvis_id."""
    doc = within_coll.find_one({"jarvis_id": code}, {"_id": 1, "created_at": 1})
    if doc:
        return "within_city_transfer", doc
    doc = across_coll.find_one({"jarvis_id": code}, {"_id": 1, "created_at": 1})
    if doc:
        return "across_city_transfer", doc
    return None


# ------------------------------ Run pipeline --------------------------------

def run(dry_run: bool) -> None:
    setup_logging()
    log = logging.getLogger("transfer-migrate")
    log.info("Reading CSVs (dry_run=%s)", dry_run)

    transfer_rows = read_transfer_rows(TRANSFER_CSV)
    price_groups = read_price_rows(PRICING_CSV)
    log.info(
        "Transfer rows: %d | Pricing groups: %d",
        len(transfer_rows), len(price_groups),
    )

    client, transfer_db, location_db = get_db_clients()
    within_coll = transfer_db["within_city_transfer"]
    across_coll = transfer_db["across_city_transfer"]
    within_slab_coll = transfer_db["within_city_transfer_slab"]
    across_slab_coll = transfer_db["across_city_transfer_slab"]
    markup_by_country = load_transfer_markup_by_country(transfer_db["transfer_markup"])
    log.info("Loaded transfer markup for %d countries", len(markup_by_country))
    cache = LocationCache(location_db["city"], location_db["country"])

    # New docs to insert (no existing doc by jarvis_id in the target collection).
    insert_within_docs: list[dict[str, Any]] = []
    insert_across_docs: list[dict[str, Any]] = []
    # Existing docs to replace in place (keeps _id + created_at).
    update_within_docs: list[dict[str, Any]] = []
    update_across_docs: list[dict[str, Any]] = []
    # Docs that were in the *other* collection and need to be deleted there.
    moved_within_to_across: list[ObjectId] = []
    moved_across_to_within: list[ObjectId] = []
    # transfer_ids whose slabs should be wiped before re-inserting.
    refresh_within_ids: list[ObjectId] = []
    refresh_across_ids: list[ObjectId] = []
    # Slab docs to insert.
    within_slab_docs: list[dict[str, Any]] = []
    across_slab_docs: list[dict[str, Any]] = []

    report_rows: list[dict[str, Any]] = []
    counts = {
        "queued_insert": 0,
        "queued_update": 0,
        "queued_move": 0,
        "skipped_validation": 0,
        "skipped_location": 0,
        "no_prices": 0,
        "cache_price_zero": 0,
    }
    mode_label = "dry_run" if dry_run else "live"

    for row in transfer_rows:
        code = clean_str(row.get("ID"))
        if not code:
            continue
        title = clean_str(row.get("title"))
        from_csv = clean_str(row.get("From"))
        to_csv = clean_str(row.get("To"))
        city_csv = clean_str(row.get("city"))
        country_csv = clean_str(row.get("country"))

        report_row: dict[str, Any] = {
            "transfer_code": code,
            "title": title,
            "status": "",
            "reason": "",
            "transfer_id": "",
            "target_collection": "",
            "existing_collection": "",
            "from_csv": from_csv,
            "to_csv": to_csv,
            "from_city_resolved": "",
            "to_city_resolved": "",
            "from_city_lookup": "",
            "to_city_lookup": "",
            "city_csv": city_csv,
            "city_resolved": "",
            "country_csv": country_csv,
            "country_resolved": "",
            "price_rows": len(price_groups.get(code, [])),
            "slab_count": 0,
            "cache_price": "",
            "cache_markup_price": "",
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

        sharing_basis = TRANSFER_TYPE_TO_SHARING_BASIS.get(row.get("Transfer Type","").lower())
        if sharing_basis is None:
            report_row["status"] = "skipped_validation"
            report_row["reason"] = "sharing_basis recheck failed"
            report_rows.append(report_row)
            counts["skipped_validation"] += 1
            continue

        country_doc = cache.country(country_csv)
        report_row["country_resolved"] = country_doc["name"] if country_doc else ""
        if not country_doc:
            reason = f"country='{country_csv}' not found"
            report_row["status"] = "skipped_location"
            report_row["reason"] = reason
            report_rows.append(report_row)
            counts["skipped_location"] += 1
            log.warning("SKIP %s -> %s", code, reason)
            continue

        within = is_within_city(row.get("Within/InterCity"))
        target_coll_name = "within_city_transfer" if within else "across_city_transfer"
        report_row["target_collection"] = target_coll_name

        # Resolve location-related entities for the chosen target schema.
        transfer_doc: Optional[dict[str, Any]]
        if within:
            city_docs, missing_cities = cache.cities(city_csv)
            report_row["city_resolved"] = ", ".join(c["name"] for c in city_docs)
            if not city_docs:
                reason = (
                    f"within: no city resolved from '{city_csv}', "
                    f"missing={missing_cities}"
                )
                report_row["status"] = "skipped_location"
                report_row["reason"] = reason
                report_rows.append(report_row)
                counts["skipped_location"] += 1
                log.warning("SKIP %s -> %s", code, reason)
                continue
            if missing_cities:
                log.warning(
                    "PARTIAL %s -> within: cities not found %s, using %s",
                    code, missing_cities, city_docs[0]["name"],
                )
            primary_city_doc = city_docs[0]
        else:
            from_city_doc, from_lookup = resolve_across_city_side(
                cache, row, "from", code, log
            )
            to_city_doc, to_lookup = resolve_across_city_side(
                cache, row, "to", code, log
            )
            from_city = from_city_doc
            to_city = to_city_doc
            report_row["from_city_resolved"] = from_city["name"] if from_city else ""
            report_row["to_city_resolved"] = to_city["name"] if to_city else ""
            report_row["from_city_lookup"] = from_lookup
            report_row["to_city_lookup"] = to_lookup
            if not from_city or not to_city:
                from_fb = clean_str(row.get("from_city"))
                to_fb = clean_str(row.get("to_city"))
                reason = (
                    f"across: from From={from_csv!r} from_city={from_fb!r} "
                    f"found={bool(from_city)} (via {from_lookup or 'n/a'}), "
                    f"to To={to_csv!r} to_city={to_fb!r} "
                    f"found={bool(to_city)} (via {to_lookup or 'n/a'})"
                )
                report_row["status"] = "skipped_location"
                report_row["reason"] = reason
                report_rows.append(report_row)
                counts["skipped_location"] += 1
                log.warning("SKIP %s -> %s", code, reason)
                continue

        # Look up existing doc by jarvis_id in either transfer collection.
        existing = find_existing(within_coll, across_coll, code)
        existing_coll_name: Optional[str] = None
        existing_id: Optional[ObjectId] = None
        existing_created_at = None
        if existing:
            existing_coll_name, existing_doc = existing
            existing_id = existing_doc["_id"]
            existing_created_at = existing_doc.get("created_at")
            report_row["existing_collection"] = existing_coll_name

        # Decide the operation kind: insert / update / move.
        if existing_id is None:
            op = "insert"
            transfer_id: ObjectId = ObjectId()
        elif existing_coll_name == target_coll_name:
            op = "update"
            transfer_id = existing_id
        else:
            op = "move"
            transfer_id = existing_id
        report_row["transfer_id"] = str(transfer_id)

        # Build the new doc.
        if within:
            transfer_doc = build_within_city_doc(
                row=row,
                transfer_id=transfer_id,
                city_doc=primary_city_doc,
                country_doc=country_doc,
                sharing_basis=sharing_basis,
            )
        else:
            transfer_doc = build_across_city_doc(
                row=row,
                transfer_id=transfer_id,
                from_city_doc=from_city,         # type: ignore[arg-type]
                to_city_doc=to_city,             # type: ignore[arg-type]
                from_country_doc=country_doc,
                to_country_doc=country_doc,
                sharing_basis=sharing_basis,
            )

        if op != "insert" and existing_created_at is not None:
            transfer_doc["created_at"] = existing_created_at

        # Route into the right buckets.
        if op == "insert":
            if within:
                insert_within_docs.append(transfer_doc)
            else:
                insert_across_docs.append(transfer_doc)
            counts["queued_insert"] += 1
            report_row["status"] = "queued_insert" if dry_run else "inserted"
        elif op == "update":
            if within:
                update_within_docs.append(transfer_doc)
                refresh_within_ids.append(transfer_id)
            else:
                update_across_docs.append(transfer_doc)
                refresh_across_ids.append(transfer_id)
            counts["queued_update"] += 1
            report_row["status"] = "queued_update" if dry_run else "updated"
        else:  # move
            # The doc is in the *other* collection. Delete it from there
            # (and its old slabs) and insert a fresh doc in the right place.
            if within:
                # currently in across, moving to within
                moved_across_to_within.append(transfer_id)
                insert_within_docs.append(transfer_doc)
            else:
                # currently in within, moving to across
                moved_within_to_across.append(transfer_id)
                insert_across_docs.append(transfer_doc)
            counts["queued_move"] += 1
            report_row["status"] = "queued_move" if dry_run else "moved"

        # Build slabs for this transfer, then derive cache_price from them.
        transfer_slabs: list[dict[str, Any]] = []
        for p_row in price_groups.get(code, []):
            slab_doc = build_slab_doc(transfer_id, p_row)
            if slab_doc:
                transfer_slabs.append(slab_doc)
                if within:
                    within_slab_docs.append(slab_doc)
                else:
                    across_slab_docs.append(slab_doc)

        slab_count = len(transfer_slabs)
        report_row["slab_count"] = slab_count

        if slab_count == 0:
            counts["no_prices"] += 1
            log.warning("WARN %s -> no usable price rows", code)

        cache_price = apply_cache_price(
            transfer_doc, transfer_slabs, within, markup_by_country
        )
        report_row["cache_price"] = cache_price["price"]
        report_row["cache_markup_price"] = cache_price["markup_price"]
        if cache_price["price"] <= 0:
            counts["cache_price_zero"] += 1

        report_rows.append(report_row)
        log.info(
            "%s %s -> %s + %d slabs, cache_price=%s",
            (
                {"insert": "QUEUE_INSERT", "update": "QUEUE_UPDATE", "move": "QUEUE_MOVE"}[op]
                if dry_run
                else {"insert": "INSERT", "update": "UPDATE", "move": "MOVE"}[op]
            ),
            code,
            target_coll_name,
            slab_count,
            cache_price,
        )

    # ----------------------------- Summary -----------------------------
    log.info("Summary:")
    log.info("  within transfers to insert:  %d", len(insert_within_docs))
    log.info("  across transfers to insert:  %d", len(insert_across_docs))
    log.info("  within transfers to update:  %d", len(update_within_docs))
    log.info("  across transfers to update:  %d", len(update_across_docs))
    log.info("  moves within->across:        %d", len(moved_within_to_across))
    log.info("  moves across->within:        %d", len(moved_across_to_within))
    log.info("  within slabs to insert:      %d", len(within_slab_docs))
    log.info("  across slabs to insert:      %d", len(across_slab_docs))
    log.info("  skipped (validation):        %d", counts["skipped_validation"])
    log.info("  skipped (location):          %d", counts["skipped_location"])
    log.info("  warning (no prices):         %d", counts["no_prices"])
    log.info("  cache_price fallback (0):    %d", counts["cache_price_zero"])

    if dry_run:
        log.info("DRY RUN: skipping all writes. Set DRY_RUN=False at the top of the file to insert.")
        if insert_within_docs:
            log.info("Sample within_city_transfer (insert):\n%s", insert_within_docs[0])
        if insert_across_docs:
            log.info("Sample across_city_transfer (insert):\n%s", insert_across_docs[0])
        if within_slab_docs:
            log.info("Sample within_city_transfer_slab:\n%s", within_slab_docs[0])
        if across_slab_docs:
            log.info("Sample across_city_transfer_slab:\n%s", across_slab_docs[0])
        write_report(report_rows, dry_run=True, log=log)
        client.close()
        return

    # 1. Inserts.
    if insert_within_docs:
        result = within_coll.insert_many(insert_within_docs, ordered=False)
        log.info("Inserted %d within_city_transfer docs", len(result.inserted_ids))
    if insert_across_docs:
        result = across_coll.insert_many(insert_across_docs, ordered=False)
        log.info("Inserted %d across_city_transfer docs", len(result.inserted_ids))

    # 2. In-place replacements for existing docs in the same target collection.
    if update_within_docs:
        for doc in update_within_docs:
            within_coll.replace_one({"_id": doc["_id"]}, doc)
        log.info("Replaced %d within_city_transfer docs", len(update_within_docs))
    if update_across_docs:
        for doc in update_across_docs:
            across_coll.replace_one({"_id": doc["_id"]}, doc)
        log.info("Replaced %d across_city_transfer docs", len(update_across_docs))

    # 3. Cross-collection moves: delete the old doc and its slabs.
    if moved_within_to_across:
        del_doc = within_coll.delete_many({"_id": {"$in": moved_within_to_across}})
        del_slab = within_slab_coll.delete_many(
            {"transfer_id": {"$in": moved_within_to_across}}
        )
        log.info(
            "Moved %d docs out of within_city_transfer: deleted docs=%d, slabs=%d",
            len(moved_within_to_across), del_doc.deleted_count, del_slab.deleted_count,
        )
    if moved_across_to_within:
        del_doc = across_coll.delete_many({"_id": {"$in": moved_across_to_within}})
        del_slab = across_slab_coll.delete_many(
            {"transfer_id": {"$in": moved_across_to_within}}
        )
        log.info(
            "Moved %d docs out of across_city_transfer: deleted docs=%d, slabs=%d",
            len(moved_across_to_within), del_doc.deleted_count, del_slab.deleted_count,
        )

    # 4. Wipe slabs for in-place updates so we can re-insert fresh ones.
    if refresh_within_ids:
        del_slab = within_slab_coll.delete_many(
            {"transfer_id": {"$in": refresh_within_ids}}
        )
        log.info(
            "Cleaned old within slabs for %d updated transfers: deleted=%d",
            len(refresh_within_ids), del_slab.deleted_count,
        )
    if refresh_across_ids:
        del_slab = across_slab_coll.delete_many(
            {"transfer_id": {"$in": refresh_across_ids}}
        )
        log.info(
            "Cleaned old across slabs for %d updated transfers: deleted=%d",
            len(refresh_across_ids), del_slab.deleted_count,
        )

    # 5. Fresh slab inserts (covers inserts, updates, and moves).
    if within_slab_docs:
        result = within_slab_coll.insert_many(within_slab_docs, ordered=False)
        log.info("Inserted %d within_city_transfer_slab docs", len(result.inserted_ids))
    if across_slab_docs:
        result = across_slab_coll.insert_many(across_slab_docs, ordered=False)
        log.info("Inserted %d across_city_transfer_slab docs", len(result.inserted_ids))

    write_report(report_rows, dry_run=False, log=log)
    client.close()
    log.info("Done.")


if __name__ == "__main__":
    run(dry_run=DRY_RUN)
