"""
Migrate packages from the static package CSV sheets into one Mongo
collections in the package_itinerary DB:

- package_itinerary (one doc multiple CSV rows based on unique package_name)

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
import random

import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


# ----------------------------- Run configuration ----------------------------
# Flip this to False when you want to actually write to the DB.
DRY_RUN = False

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PACKAGE_CSV = os.path.join(THIS_DIR, "content", "Indonesia_Holiday_Packages_v5.xlsx - Pkg.csv")
METADATA_CSV = os.path.join(THIS_DIR, "content", "Indonesia_Holiday_Packages_v5_meta.xlsx - Meta.csv")
REPORTS_DIR = os.path.join(THIS_DIR, "reports")

MONGO_URI = os.getenv("MONGO_URI")
ITINERARY_DB = os.getenv("ITINERARY_DB") or "ht_itinerary_db"
LOCATION_DB = os.getenv("LOCATION_DB") or "ht_location_db"
ACTIVITIES_DB = os.getenv("ACTIVITIES_DB") or "ht_activity_db"
TRANSFER_DB = os.getenv("TRANSFER_DB") or "ht_transfer_db"
HOTEL_DB = os.getenv("HOTEL_DB") or "ht_hotel_db"

PACKAGE_IMAGE_BASE_URL = "https://cdn.holidaytribe.ai/website/package"


# ----------------------------- Data mappings -------------------------------
CITY_MAPPING = {
    "Insel Nusa Penida": "Nusa Penida"
}


# --------------------------- Lookups & caches -------------------------------

class LocationCache:
    """Small in-memory cache to avoid repeated lookups for the same city/country."""

    def __init__(self, city_coll, country_coll, continent_coll, destination_coll) -> None:
        self.city_coll = city_coll
        self.country_coll = country_coll
        self.continent_coll = continent_coll
        self.destination_coll = destination_coll
        self._city_cache: dict[str, Optional[dict]] = {}
        self._country_cache: dict[str, Optional[dict]] = {}
        self._continent_cache: dict[str, Optional[dict]] = {}
        self._destination_cache: dict[str, Optional[dict]] = {}

    def city(self, name: str) -> Optional[dict]:
        name = (name or "").strip()
        name = CITY_MAPPING.get(name, name) # Map the city name to the actual city name
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
    
    def countryById(self, id: ObjectId) -> Optional[dict]:
        if not id:
            return None
        if id in self._country_cache:
            return self._country_cache[id]
        doc = self.country_coll.find_one({"_id": id})
        self._country_cache[id] = doc
        return doc
    
    def continent(self, name: str) -> Optional[dict]:
        name = (name or "").strip()
        if not name:
            return None
        if name in self._continent_cache:
            return self._continent_cache[name]
        doc = self.continent_coll.find_one({"name": name})
        self._continent_cache[name] = doc
        return doc
    
    def continentById(self, id: ObjectId) -> Optional[dict]:
        if not id:
            return None
        if id in self._continent_cache:
            return self._continent_cache[id]
        doc = self.continent_coll.find_one({"_id": id})
        self._continent_cache[id] = doc
        return doc

    def destination(self, name: str) -> Optional[dict]:
        name = (name or "").strip()
        if not name:
            return None
        if name in self._destination_cache:
            return self._destination_cache[name]
        doc = self.destination_coll.find_one({"name": name})
        self._destination_cache[name] = doc
        return doc


class ActivityCache:
    """Small in-memory cache to avoid repeated lookups for the same activity."""

    def __init__(self, activity_coll) -> None:
        self.activity_coll = activity_coll
        self._activity_cache: dict[str, Optional[dict]] = {}

    def activity(self, code: str) -> Optional[dict]:
        code = (code or "").strip()
        if not code:
            return None
        if code in self._activity_cache:
            return self._activity_cache[code]
        doc = self.activity_coll.find_one({"provider_info.cms.activityCode": code})
        self._activity_cache[code] = doc
        return doc

    def activityById(self, id: ObjectId) -> Optional[dict]:
        if not id:
            return None
        if id in self._activity_cache:
            return self._activity_cache[id]
        doc = self.activity_coll.find_one({"_id": id})
        self._activity_cache[id] = doc
        return doc
class TransferCache:
    """Small in-memory cache to avoid repeated lookups for the same transfer."""

    def __init__(self, transfer_coll) -> None:
        self.transfer_coll = transfer_coll
        self._transfer_cache: dict[str, Optional[dict]] = {}

    def transfer(self, code: str) -> Optional[dict]:
        code = (code or "").strip()
        if not code:
            return None
        if code in self._transfer_cache:
            return self._transfer_cache[code]
        doc = self.transfer_coll.find_one({"jarvis_id": code})
        self._transfer_cache[code] = doc
        return doc

    def transferById(self, id: ObjectId) -> Optional[dict]:
        if not id:
            return None
        if id in self._transfer_cache:
            return self._transfer_cache[id]
        doc = self.transfer_coll.find_one({"_id": id})
        self._transfer_cache[id] = doc
        return doc


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


def split_csv_list(value: Any, delimiter: str = ",") -> list[str]:
    """Split a comma-separated cell into trimmed items."""
    text = clean_str(value)
    if not text:
        return []
    return [p.strip() for p in text.split(delimiter) if p.strip()]


def extract_short_id(code: str) -> str:
    """Return the part after the underscore, e.g. TRA_57347 -> 57347."""
    code = clean_str(code)
    if "_" in code:
        return code.split("_", 1)[1]
    return code


def parse_sheet_images_code(raw: Any) -> Optional[str]:
    """Parse image code from ``[{img_url=CODE, img_alt=TEXT}]`` in the images column."""
    text = clean_str(raw)
    if not text:
        return None
    url_match = re.search(r"img_url=([^,}\]]+)", text, flags=re.IGNORECASE)
    if not url_match:
        return None
    return url_match.group(1).strip()


def build_image_url(image_code: str) -> str:
    return f"{PACKAGE_IMAGE_BASE_URL}/{image_code}.jpg"

def build_travel_theme(raw: str) -> list[str]:
    return split_csv_list(raw)

def build_travel_group(raw: str) -> list[str]:
    return split_csv_list(raw)

def build_highlights(raw: str) -> list[str]:
    return split_csv_list(raw, delimiter="|")

def build_slug(package_name: str) -> str:
    text = clean_str(package_name).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", "-", text.strip())
    return text.strip("-")

# ----------------------------- DB connections -------------------------------

def get_db_clients() -> tuple[MongoClient, Any, Any, Any, Any, Any]:
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI env var is not set")
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        tlsAllowInvalidCertificates=True,
        tlsAllowInvalidHostnames=True,
    )
    itinerary_db = client[ITINERARY_DB]
    location_db = client[LOCATION_DB]
    activities_db = client[ACTIVITIES_DB]
    transfer_db = client[TRANSFER_DB]
    hotel_db = client[HOTEL_DB]
    return client, itinerary_db, location_db, activities_db, transfer_db, hotel_db


# ------------------------------ Main pipeline -------------------------------

def read_package_metadata_rows(path: str) -> dict[str, list[dict[str, Any]]]:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return df.to_dict(orient="records")

def read_package_itinerary_rows(path: str) -> list[dict[str, Any]]:
    """Returns {package_name -> [package_itinerary_row, ...]}."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            package_name = clean_str(row.get("Package Name"))
            if not package_name:
                continue
            grouped.setdefault(package_name, []).append(row)
    return grouped


# --------------------------------- Report -----------------------------------

REPORT_HEADERS = [
    "package_name",
    "status",
    "reason",
    "mode",
]


def write_report(rows: list[dict[str, Any]], dry_run: bool, log: logging.Logger) -> str:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    mode = "dry_run" if dry_run else "live"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(REPORTS_DIR, f"package_migration_report_{mode}_{timestamp}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in REPORT_HEADERS})
    log.info("Report written: %s", path)
    return path


# ------------------------------ Run pipeline --------------------------------

class PackageMigrate:
    def __init__(self, dry_run: bool) -> None:
        self.dry_run = dry_run
        setup_logging()
        self.log = logging.getLogger("package-migrate")
        self.package_metadata_rows = read_package_metadata_rows(METADATA_CSV)
        self.package_itinerary_rows = read_package_itinerary_rows(PACKAGE_CSV)
        self.client, self.itinerary_db, self.location_db, self.activities_db, self.transfer_db, self.hotel_db = get_db_clients()
        self.package_itinerary_coll = self.itinerary_db["package_itinerary_v1"]
        self.country_coll = self.location_db["country"]
        self.city_coll = self.location_db["city"]
        self.continent_coll = self.location_db["continent"]
        self.destination_coll = self.location_db["destination"]
        self.activity_coll = self.activities_db["activity"]
        self.transfer_coll = self.transfer_db["within_city_transfer"]
        self.hotel_coll = self.hotel_db["hotel_v2"]
        self.location_cache = LocationCache(self.city_coll, self.country_coll, self.continent_coll, self.destination_coll)
        self.activity_cache = ActivityCache(self.activity_coll)
        self.transfer_cache = TransferCache(self.transfer_coll)
        self.report_rows: list[dict[str, Any]] = []
        self.insert_package_docs: list[dict[str, Any]] = []
    
    def build_activity_item(self, row: dict[str, Any]) -> dict[str, Any]:
        code = clean_str(row.get("Code"))
        if not code:
            raise ValueError(f"Activity code is required: {row}")
        activity_doc = self.activity_cache.activity(code)
        if not activity_doc:
            raise ValueError(f"Activity not found: {code}")
        return {
            "_id": activity_doc["_id"],
            "module": "ACTIVITY",
            "module_id": code,
            "name": activity_doc["title"],
            "tags": activity_doc["tags"],
            "description": activity_doc["description"],
            "is_paid": True,
            "why_this": None, # AI support required for this field
            "image_url": activity_doc["images"][0]["img_url"],
            "duration": activity_doc["duration"],
            "isCmsActivity": True,
            "inclusion": activity_doc["inclusions"],
            "exclusion": activity_doc["exclusions"],
            "cancellation": activity_doc["cancellation"],
        }

    def build_transfer_item(self, row: dict[str, Any]) -> dict[str, Any]:
        code = clean_str(row.get("Code"))
        if not code:
            raise ValueError(f"Transfer code is required: {row}")
        transfer_doc = self.transfer_cache.transfer(code)
        if not transfer_doc:
            raise ValueError(f"Transfer not found: {code}")
        return {
            "_id": transfer_doc["_id"],
            "module": "TRANSFER",
            "module_id": code,
            "title": transfer_doc["title"],
            "transfer_type": transfer_doc["transfer_type"],
            "image_url": transfer_doc["hero_image"],
            "type": None, # TODO: add type
            "sub_type": None, # TODO: add sub type
            "from_location": transfer_doc["pickup"],
            "to_location": transfer_doc["dropoff"],
            "transfer_category": 'WITHIN',
            "cancellation": transfer_doc["cancellation_policy"],
        }
    
    def build_hotel_item(self, row: dict[str, Any]) -> dict[str, Any]:
        # filter, city_id and star_rating and is_recommended + sorting by price return random one from top 5
        city_name = row["City"]
        star_rating = 3 # TODO: add star rating
        duration_in_days = row["No of Nights"]

        city_doc = self.location_cache.city(city_name)
        if not city_doc:
            raise ValueError(f"City not found: {city_name}")
        hotel_docs = list(self.hotel_coll.find({"city.city_id": city_doc["_id"], "star_rating": {"$gte": star_rating}, "is_recommended": True}).sort("price", 1).limit(5))
        if not hotel_docs:
            raise ValueError(f"No hotel found for city: {city_name}")

        hotel_doc = random.choice(hotel_docs)
        return {
            "_id": hotel_doc["_id"],
            "module": "HOTEL_CHECK_IN",
            "module_id": None,
            "name": hotel_doc["name"],
            "locality": None, # TODO: add locality
            "locality_id": None, # TODO: add locality id
            "star_rating": hotel_doc["star_rating"],
            "duration_in_days": duration_in_days,
            "image_url": hotel_doc["hero_image"][0]["href"],
            "description": None, # TODO: add description
        }

    def build_itinerary_item(self, row: dict[str, Any]) -> dict[str, Any]:
        item_type = row["Type"].upper()
        match item_type:
            case "ACTIVITY":
                return self.build_activity_item(row)
            case "TRANSFER":
                # return self.build_transfer_item(row)
                return {}
            case "HOTEL":
                return self.build_hotel_item(row)
            case _:
                raise ValueError(f"Invalid item type: {item_type}")

    def build_destinations(self, cities: list[str]) -> dict[str, Any]:
        destinations: dict[str, Any] = {}
        for city in cities:
            destination_doc = self.location_cache.destination(city)
            if not destination_doc:
                raise ValueError(f"Destination not found: {city}")
            
            country_id = destination_doc["country_id"]
            country_doc = self.location_cache.countryById(country_id)
            if not country_doc:
                raise ValueError(f"Country not found: {country_id}")

            destinations[destination_doc["name"]] = {
                "destination_id": destination_doc["_id"],
                "name": destination_doc["name"],
                "image_url": None,
                "tags": [],
                "summary": None,
                "city_id": destination_doc["city_id"],
                "country": {
                    "_id": country_doc["_id"],
                    "name": country_doc["name"],
                }
            }
        return destinations

    def build_destination_with_country_and_continent(self, cities: list[str]) -> dict[str, Any]:
        destinations: list[dict[str, Any]] = []
        countries: list[dict[str, Any]] = []
        continents: list[dict[str, Any]] = []
        for city in cities:
            destination_doc = self.location_cache.destination(city)
            if not destination_doc:
                raise ValueError(f"Destination not found: {city}")
            country_id = destination_doc["country_id"]
            country_doc = self.location_cache.countryById(country_id)
            if not country_doc:
                raise ValueError(f"Country not found: {country_id}")
            continent_id = country_doc["continent_id"]
            continent_doc = self.location_cache.continentById(continent_id)
            if not continent_doc:
                raise ValueError(f"Continent not found: {continent_id}")
            destinations.append({
                "_id": destination_doc["_id"],
                "name": destination_doc["name"]
            })
            countries.append({
                "_id": country_doc["_id"],
                "name": country_doc["name"],
            })
            continents.append({
                "_id": continent_doc["_id"],
                "name": continent_doc["name"],
            })
        return {
            "destinations": destinations,
            "countries": countries,
            "continents": continents,
        }
    
    def build_day_wise_details(self, package_itinerary_row: list[dict[str, Any]]) -> list[dict[str, Any]]:
        day_wise_dict: dict[int, dict[str, Any]] = {}

        for row in package_itinerary_row:
            day_index = int(row["Day"])
            existing_day_details = day_wise_dict.get(day_index, None)

            if existing_day_details:
                itinerary_items: list[dict[str, Any]] = existing_day_details["itinerary"]
                itinerary_item = self.build_itinerary_item(row)
                itinerary_items.append(itinerary_item)
            else:
                city_doc = self.location_cache.city(row["City"])
                if not city_doc:
                    raise ValueError(f"City not found: {row['City']}")

                itinerary_items: list[dict[str, Any]] = []
                itinerary_item = self.build_itinerary_item(row)
                itinerary_items.append(itinerary_item)

                day_wise_dict[day_index] = {
                    "city": city_doc["name"],
                    "city_id": city_doc["_id"],
                    "description": None, # AI support required for this field
                    "itinerary": itinerary_items,
                    "pace": None, # AI support required for this field
                }
        return list(day_wise_dict.values())
    
    def build_package_itinerary_doc(self, package_metadata_row: dict[str, Any], day_wise_details: list[dict[str, Any]]) -> dict[str, Any]:
        slug = build_slug(package_metadata_row["Package_Name"])
        unique_cities = set()
        for day_wise_detail in day_wise_details:
            unique_cities.add(day_wise_detail["city"])
        destination_with_country_and_continent = self.build_destination_with_country_and_continent(unique_cities)
        return {
            "title": package_metadata_row["Package_Name"],
            "user_type": "ADMIN",
            "number_of_days": int(package_metadata_row["Duration_Days"]),
            "number_of_nights": int(package_metadata_row["Duration_Nights"]),
            "travel_theme": build_travel_theme(package_metadata_row["Themes"]),
            "travel_group": build_travel_group(package_metadata_row["Travel Group"]),
            "budget": 0.0, # AI support required for this field
            "recommendation_score": None, # AI support required for this field
            "is_bookable": True,
            "is_customisable": True,
            "overall_pace": None, # AI support required for this field
            "highlights": build_highlights(package_metadata_row["Package Highlights (usp)"]),
            "img_url": build_image_url(slug),
            "img_alt": package_metadata_row["Package_Name"],
            "seo_info": None, # AI support required for this field
            "day_wise_details": day_wise_details,
            "travel_insurance": None, # TODO: add travel insurance
            "visa_assistance": None, # TODO: add visa assistance
            "destination": destination_with_country_and_continent["destinations"],
            "destinations": self.build_destinations(unique_cities),
            "countries": destination_with_country_and_continent["countries"],
            "continents": destination_with_country_and_continent["continents"],
            "slug": slug,
            "package_id": None,
            "created_at": utcnow(),
            "updated_at": utcnow(),
        }

    def run(self) -> None:
        self.log.info("Reading CSVs (dry_run=%s)", self.dry_run)
        self.log.info("Package metadata rows: %d | Package itinerary rows: %d", len(self.package_metadata_rows), len(self.package_itinerary_rows))

        if len(self.package_metadata_rows) != len(self.package_itinerary_rows):
            raise ValueError("Package metadata rows and package itinerary rows are not equal")

        for package_metadata_row in self.package_metadata_rows:
            try:
                package_name = package_metadata_row["Package_Name"]
                package_itinerary_row = self.package_itinerary_rows[package_name]
                day_wise_details = self.build_day_wise_details(package_itinerary_row)
                package_doc = self.build_package_itinerary_doc(package_metadata_row, day_wise_details)
                self.insert_package_docs.append(package_doc)
                self.report_rows.append({ 
                    "package_name": package_name,
                    "status": "success",
                    "reason": "",
                    "mode": "dry_run" if self.dry_run else "live",
                })
            except Exception as e:
                self.report_rows.append({
                    "package_name": package_metadata_row["Package_Name"],
                    "status": "error",
                    "reason": str(e),
                    "mode": "dry_run" if self.dry_run else "live",
                })
                self.log.error("Error building package itinerary doc: %s", e)
                continue

        self.log.info("Summary:")
        self.log.info("Inserted %d package docs", len(self.insert_package_docs))
        self.log.info("Report rows: %d", len(self.report_rows))

        if self.dry_run:
            self.log.info("DRY RUN: skipping all writes. Set DRY_RUN=False at the top of the file to insert.")
            if self.insert_package_docs:
                self.log.info("Sample package doc (insert):\n%s", self.insert_package_docs[0])
            write_report(self.report_rows, dry_run=True, log=self.log)
            self.client.close()
            return

        if self.insert_package_docs:
            result = self.package_itinerary_coll.insert_many(self.insert_package_docs, ordered=False)
            self.log.info("Inserted %d package docs", len(result.inserted_ids))
        write_report(self.report_rows, dry_run=False, log=self.log)
        self.client.close()
        self.log.info("Done.")


if __name__ == "__main__":
    PackageMigrate(dry_run=DRY_RUN).run()
