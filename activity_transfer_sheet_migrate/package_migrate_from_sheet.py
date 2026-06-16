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

import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

"""
{
  "_id": {
    "$oid": "6a2ab843ae797d9a7c92dca2"
  },
  "title": "Bali Adventure & Relaxation",
  "user_type": "ADMIN",
  "number_of_days": 9,
  "number_of_nights": 8,
  "travel_theme": [
    "ADVENTURE",
    "LUXURY",
    "RELAXATION"
  ],
  "travel_group": [
    "COUPLE"
  ],
  "budget": 41049,
  "travel_insurance": {
    "_id": {
      "$oid": "69834d8d81abb9c40913a348"
    },
    "module_id": null,
    "name": "Travel Insurance",
    "description": "",
    "country_code": null,
    "estimated_price": null,
    "cancellation": "100% Non- refundable"
  },
  "visa_assistance": [
    {
      "_id": {
        "$oid": "69834d1f9a50a48a5d5f6dec"
      },
      "module_id": "",
      "name": "Travel VISA",
      "description": "",
      "inclusion": "Visa\nOnly Valid for Indian Passport Holders",
      "exclusion": "",
      "cancellation": "Non Refundable",
      "estimated_price": null
    }
  ],
  "destinations": {
    "Kuta": {
      "destination_id": {
        "$oid": "68ea52813b8fe3fccd59a149"
      },
      "name": "Kuta",
      "image_url": "https://cdn.holidaytribe.ai/website/destination/kuta.webp",
      "tags": [],
      "summary": "",
      "city_id": {
        "$oid": "68ea52813b8fe3fccd59a148"
      },
      "country": {
        "name": "Indonesia",
        "_id": {
          "$oid": "6a2ab843ae797d9a7c92dc9e"
        }
      }
    },
    "Ubud": {
      "destination_id": {
        "$oid": "694bf97e5bb003240b7662c8"
      },
      "name": "Ubud",
      "image_url": "https://cdn.holidaytribe.ai/website/destination/ubud.webp",
      "tags": [],
      "summary": "",
      "city_id": {
        "$oid": "694bf9795bb003240b7662c7"
      },
      "country": {
        "name": "Indonesia",
        "_id": {
          "$oid": "6a2ab843ae797d9a7c92dc9f"
        }
      }
    },
    "Bali": {
      "destination_id": {
        "$oid": "68a4ca48b81169ffd5923591"
      },
      "name": "Bali",
      "image_url": "https://cdn.holidaytribe.ai/website/destination/Bali.webp",
      "tags": [],
      "summary": "",
      "city_id": {
        "$oid": "68a4c755b10a45236e75962f"
      },
      "country": {
        "name": "Indonesia",
        "_id": {
          "$oid": "6a2ab843ae797d9a7c92dca0"
        }
      }
    }
  },
  "destination": [
    {
      "name": "Kuta",
      "_id": {
        "$oid": "6a2ab843ae797d9a7c92dc99"
      }
    },
    {
      "name": "Ubud",
      "_id": {
        "$oid": "6a2ab843ae797d9a7c92dc9a"
      }
    },
    {
      "name": "Bali",
      "_id": {
        "$oid": "6a2ab843ae797d9a7c92dc9b"
      }
    },
    {
      "name": "Indonesia",
      "_id": {
        "$oid": "6a2ab843ae797d9a7c92dc9c"
      }
    },
    {
      "name": "Asia",
      "_id": {
        "$oid": "6a2ab843ae797d9a7c92dc9d"
      }
    }
  ],
  "day_wise_details": [
    {
      "city": "Kuta",
      "city_id": {
        "$oid": "68ea52813b8fe3fccd59a148"
      },
      "description": "Arrive in Kuta and check-in to The Kana Kuta after a private transfer from DPS Airport.",
      "itinerary": [
        {
          "_id": {
            "$oid": "68ecea6f103b93ff55de9775"
          },
          "module": "TRANSFER",
          "module_id": "",
          "title": "One Way Private Transfer from DPS Airport to Kuta Hotel",
          "transfer_type": "PRIVATE",
          "image_url": "https://cdn.holidaytribe.ai/website/transfer/private-taxi.png",
          "type": "TAXI",
          "sub_type": "AIRPORT_TRANSFER",
          "from_location": "DPS Airport",
          "to_location": "Kuta Hotel",
          "transfer_category": "WITHIN",
          "cancellation": "{'valid_before': '7 Days', 'percentage': 100}",
          "estimated_price": null,
          "estimated_markup_price": null
        },
        {
          "_id": {
            "$oid": "696e0affa7a775afc497fc4b"
          },
          "module": "HOTEL_CHECK_IN",
          "module_id": null,
          "name": "The Kana Kuta",
          "locality": "Jl. Setiabudi no. 8",
          "locality_id": null,
          "star_rating": 4,
          "image_url": "https://i.travelapi.com/lodging/8000000/7250000/7249800/7249735/479d451f_b.jpg",
          "duration_in_days": 3,
          "description": null,
          "estimated_price": null,
          "estimated_markup_price": null
        }
      ],
      "pace": null
    },
    {
      "city": "Kuta",
      "city_id": {
        "$oid": "68ea52813b8fe3fccd59a148"
      },
      "description": "Embark on a thrilling Mount Batur Sunrise Jeep Tour with a return private transfer.",
      "itinerary": [
        {
          "_id": {
            "$oid": "68d18464c48cd546b35fb96b"
          },
          "module": "ACTIVITY",
          "module_id": "ACT_55528",
          "name": "Mount Batur Sunrise Jeep Tour with Return Private Transfer",
          "type": "",
          "tags": [],
          "description": "",
          "is_paid": true,
          "why_this": null,
          "image_url": "https://cdn.holidaytribe.ai/website/activity/ACT_55528_7.jpg",
          "duration": 0,
          "isCmsActivity": null,
          "inclusion": [
            "Mount Batur Sunrise Jeep Tour",
            "Return Private Transfer"
          ],
          "exclusion": [],
          "cancellation": "D-7",
          "estimated_price": null,
          "estimated_markup_price": null
        }
      ],
      "pace": null
    },
    {
      "city": "Kuta",
      "city_id": {
        "$oid": "68ea52813b8fe3fccd59a148"
      },
      "description": "Enjoy a leisurely day at your hotel in Kuta with time for relaxation and exploration.",
      "itinerary": [],
      "pace": null
    },
    {
      "city": "Bali",
      "city_id": {
        "$oid": "68a4c755b10a45236e75962f"
      },
      "description": "",
      "itinerary": [
        {
          "_id": {
            "$oid": "689c31aef958e8ea47aac76c"
          },
          "module": "ACTIVITY",
          "module_id": "ACT_54234",
          "name": "Half Day Private Car at Disposal for 4 Hours",
          "type": "",
          "tags": [],
          "description": "Half Day Private Car at Disposal for 4 Hours",
          "is_paid": true,
          "why_this": "",
          "image_url": "https://cdn.holidaytribe.ai/website/activity/ACT_54234.webp",
          "duration": 5,
          "isCmsActivity": null,
          "inclusion": [
            "Half Day Private Car at Disposal for 4 Hours with Limited Kilometers"
          ],
          "exclusion": [
            "Out Side City Limits"
          ],
          "cancellation": "D-7",
          "estimated_price": null,
          "estimated_markup_price": null
        }
      ],
      "pace": null
    },
    {
      "city": "Bali",
      "city_id": {
        "$oid": "68a4c755b10a45236e75962f"
      },
      "description": "",
      "itinerary": [],
      "pace": null
    },
    {
      "city": "Ubud",
      "city_id": {
        "$oid": "694bf9795bb003240b7662c7"
      },
      "description": "Bid farewell to Kuta, transfer to Ubud, and check-in to The Mansion Resort Hotel & Spa.",
      "itinerary": [
        {
          "_id": {
            "$oid": "68e7cb0e6b76504213b2208d"
          },
          "module": "TRANSFER",
          "module_id": "",
          "title": "One Way Private Transfer from Kuta Hotel to Ubud Hotel",
          "transfer_type": "PRIVATE",
          "image_url": "https://cdn.holidaytribe.ai/website/transfer/private-taxi.png",
          "type": "TAXI",
          "sub_type": null,
          "from_location": "Kuta",
          "to_location": "Ubud",
          "transfer_category": "ACROSS",
          "cancellation": "{'valid_before': '7 Days', 'percentage': 100}",
          "estimated_price": null,
          "estimated_markup_price": null
        }
      ],
      "pace": null
    },
    {
      "city": "Ubud",
      "city_id": {
        "$oid": "694bf9795bb003240b7662c7"
      },
      "description": "Explore the scenic Git Git Water Fall with a return private transfer.",
      "itinerary": [
        {
          "_id": {
            "$oid": "689c31aef958e8ea47aac713"
          },
          "module": "ACTIVITY",
          "module_id": "ACT_54109",
          "name": "Git Git Water Fall with Return Private Transfer",
          "type": "",
          "tags": [],
          "description": "The Gitgit Waterfalls is a beautiful tourist destination in the northern part of Bali. Nestled among lush green trees and clove plantations, the splendid waterfalls are located in Gitgit village, Singaraja district &constantly emit natural water perennially.The 35-40m cascade drops beautifully in a pool that has a tiny shrine in it. It’s hard to get an idea of how big the waterfall is until you’re up close and feel the thundering mist. It’s a huge waterfall. Visitors are allowed to swim in the cool waters of the pool to relax after a tiresome yet picturesque hike to the waterfall.",
          "is_paid": true,
          "why_this": null,
          "image_url": "https://cdn.holidaytribe.ai/website/activity/ACT_54109_1.jpg",
          "duration": 8,
          "isCmsActivity": null,
          "inclusion": [
            "Git Git Water Fall",
            "Return Private Transfer"
          ],
          "exclusion": [
            "Anything which is not mentioned in inclusion"
          ],
          "cancellation": "D-7",
          "estimated_price": null,
          "estimated_markup_price": null
        }
      ],
      "pace": null
    },
    {
      "city": "Ubud",
      "city_id": {
        "$oid": "694bf9795bb003240b7662c7"
      },
      "description": "Experience the adrenaline rush of a 1-hour ATV Quad Bike ride with private transfer.",
      "itinerary": [
        {
          "_id": {
            "$oid": "689c31aef958e8ea47aac863"
          },
          "module": "ACTIVITY",
          "module_id": "ACT_54986",
          "name": "1 Hour ATV Quad Bike with Private Transfer ",
          "type": "",
          "tags": [],
          "description": "For a hard core adventurer or a novice, off-roading is the most appealing word & what better than Quad Biking while in Bali. Ride, off the beaten track during your Bali vacation on your own ATV (All-Terrain Vehicle). Follow an experienced guide along a challenging track, suitable for all skill levels.No driving license required & safety is of utmost priority here. Glide past a picturesque landscape of rice fields, bamboo forest, and lush riverside flanked by traditional Balinese villages. It is an ultimate thrill for a couple of hours; you will just want to keep it going.",
          "is_paid": true,
          "why_this": null,
          "image_url": "https://cdn.holidaytribe.ai/website/activity/ACT_54986_1.jpg",
          "duration": 2,
          "isCmsActivity": null,
          "inclusion": [
            "Welcome Drink, Lunch, \r\r\r\r\r\n\r\r\r\r\r\n\r\r\r\r\r\nCoffee or Tea or Mineral Water\r\r\r\r\r\n\r\r\r\r\r\n\r\r\r\r\r\nShower facilities, Toiletries, Locker & Insurance.\r\r\r\r\r\n\r\r\r\r\r\n\r\r\r\r\r\nSingle-1 Hour\r\r\r\r\r\n\r\r\r\r\r\n\r\r\r\r\r\nAll options including: ATV ride experience, professional guide, all safety gear(helmets and boots)\r\r\r\r\r\n\r\r\r\r\r\n\r\r\r\r\r\nPrivate Transfers"
          ],
          "exclusion": [
            "Anything which is not mentioned in inclusion"
          ],
          "cancellation": "D-7",
          "estimated_price": null,
          "estimated_markup_price": null
        }
      ],
      "pace": null
    },
    {
      "city": "Ubud",
      "city_id": {
        "$oid": "694bf9795bb003240b7662c7"
      },
      "description": "Transfer to your next destination after checking out from The Mansion Resort Hotel & Spa.",
      "itinerary": [
        {
          "_id": {
            "$oid": "68ecea6f103b93ff55de9779"
          },
          "module": "TRANSFER",
          "module_id": "",
          "title": "One Way Private Transfer",
          "transfer_type": "PRIVATE",
          "image_url": "https://cdn.holidaytribe.ai/website/transfer/private-taxi.png",
          "type": "TAXI",
          "sub_type": "AIRPORT_TRANSFER",
          "from_location": "Ubud Hotel",
          "to_location": "DPS Airport",
          "transfer_category": "WITHIN",
          "cancellation": "{'valid_before': '7 Days', 'percentage': 100}",
          "estimated_price": null,
          "estimated_markup_price": null
        }
      ],
      "pace": null
    }
  ],
  "seo_info": {
    "title": "Bali Adventure & Relaxation",
    "meta_title": "Bali Adventure Tour Package | HolidayTribe",
    "meta_description": "Explore Bali's adventure & relaxation. 7N stay in Kuta & Ubud, Jeep Tour, Water Fall visit & ATV Quad Bike. Book now!",
    "meta_robots": "index, follow",
    "og_title": "Experience Bali's Best: Adventure & Relaxation",
    "og_description": "Discover Bali's thrill & serenity. Enjoy Jeep Tours, Water Falls & more in Kuta & Ubud.",
    "og_type": "article",
    "og_image_url": ""
  },
  "img_url": "https://cdn.holidaytribe.ai/website/package/memorable-bali-with-friends.webp",
  "img_alt": "Breathtaking holiday package showcasing stunning Bali landscapes and luxurious accommodations.",
  "is_bookable": true,
  "is_customisable": true,
  "highlights": [
    "Witness the breathtaking sunrise on Mount Batur",
    "Discover the natural beauty of Git Git Water Fall",
    "Enjoy the comfort and amenities of The Kana Kuta and The Mansion Resort Hotel & Spa",
    "Explore the cultural and scenic wonders of Ubud"
  ],
  "overall_pace": "RELAXED",
  "recommendation_score": null,
  "countries": [
    {
      "_id": {
        "$oid": "68a4c280662c8b665bf3b263"
      },
      "name": "Indonesia"
    }
  ],
  "continents": [
    {
      "_id": {
        "$oid": "689fa4ae06bc95e3b52da1a9"
      },
      "name": "Asia"
    }
  ],
  "max_flexible_days": null,
  "slug": "bali-adventure-relaxation",
  "package_id": {
    "$oid": "6a2ab843ae797d9a7c92dca1"
  },
  "created_at": {
    "$date": "2026-06-11T18:59:39.083Z"
  },
  "updated_at": {
    "$date": "2026-06-11T18:59:39.083Z"
  }
}
"""


# ----------------------------- Run configuration ----------------------------
# Flip this to False when you want to actually write to the DB.
DRY_RUN = True



THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PACKAGE_CSV = os.path.join(THIS_DIR, "content", "Indonesia_Holiday_Packages_v5.xlsx - Pkg.csv")
METADATA_CSV = os.path.join(THIS_DIR, "content", "Indonesia_Holiday_Packages_v5_meta.xlsx - Meta.csv")
REPORTS_DIR = os.path.join(THIS_DIR, "reports")

MONGO_URI = os.getenv("MONGO_URI")
PACKAGE_ITINERARY_DB = os.getenv("PACKAGE_ITINERARY_DB") or "ht_package_itinerary_db"
LOCATION_DB = os.getenv("LOCATION_DB") or "ht_location_db"
ACTIVITIES_DB = os.getenv("ACTIVITIES_DB") or "ht_activity_db"
TRANSFER_DB = os.getenv("TRANSFER_DB") or "ht_transfer_db"

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

def build_destinations(cities: list[str], location_cache: LocationCache) -> dict[str, Any]:
    destinations: dict[str, Any] = {}
    for city in cities:
        destination_doc = location_cache.destination(city)
        if not destination_doc:
            raise ValueError(f"Destination not found: {city}")
        
        country_id = destination_doc["country_id"]
        country_doc = location_cache.countryById(country_id)
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

def build_destination_with_country_and_continent(cities: list[str], location_cache: LocationCache) -> dict[str, Any]:
    destinations: list[dict[str, Any]] = []
    countries: list[dict[str, Any]] = []
    continents: list[dict[str, Any]] = []
    for city in cities:
        destination_doc = location_cache.destination(city)
        if not destination_doc:
            raise ValueError(f"Destination not found: {city}")
        country_id = destination_doc["country_id"]
        country_doc = location_cache.countryById(country_id)
        if not country_doc:
            raise ValueError(f"Country not found: {country_id}")
        continent_id = country_doc["continent_id"]
        continent_doc = location_cache.continentById(continent_id)
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


def build_package_itinerary_doc(package_metadata_row: dict[str, Any], day_wise_details: list[dict[str, Any]], location_cache: LocationCache) -> dict[str, Any]:
    slug = build_slug(package_metadata_row["Package_Name"])
    unique_cities = set()
    for day_wise_detail in day_wise_details:
        unique_cities.add(day_wise_detail["city"])
    destinations, countries, continents = build_destination_with_country_and_continent(unique_cities, location_cache)
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
        "destination": [*destinations, *countries, *continents],
        "destinations": build_destinations(unique_cities, location_cache),
        "countries": countries,
        "continents": continents,
        "slug": slug,
        "package_id": None,
        "created_at": utcnow(),
        "updated_at": utcnow(),
    }

def build_activity_item(row: dict[str, Any], activity_cache: ActivityCache) -> dict[str, Any]:
    code = clean_str(row.get("Code"))
    if not code:
        raise ValueError(f"Activity code is required: {row}")
    activity_doc = activity_cache.activity(code)
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

def build_transfer_item(row: dict[str, Any], transfer_cache: TransferCache) -> dict[str, Any]:
    code = clean_str(row.get("Code"))
    if not code:
        raise ValueError(f"Transfer code is required: {row}")
    transfer_doc = transfer_cache.transfer(code)
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

def build_hotel_item(row: dict[str, Any]) -> dict[str, Any]:
  # filter, city_id and star_rating and is_recommended + sorting by price return random one from top 5
    return {
        "module": "HOTEL",
        "module_id": 123,
        "name": "Hotel 123",
    }

def build_itinerary_item(row: dict[str, Any], activity_cache: ActivityCache, transfer_cache: TransferCache) -> dict[str, Any]:
    item_type = row["Type"].upper()
    match item_type:
        case "ACTIVITY":
            return build_activity_item(row, activity_cache)
        case "TRANSFER":
            return build_transfer_item(row, transfer_cache)
        case "HOTEL":
            return build_hotel_item(row)
        case _:
            raise ValueError(f"Invalid item type: {item_type}")


def build_day_wise_details(package_itinerary_row: list[dict[str, Any]], location_cache: LocationCache, activity_cache: ActivityCache, transfer_cache: TransferCache) -> list[dict[str, Any]]:
    day_wise_dict: dict[int, dict[str, Any]] = {}

    for row in package_itinerary_row:
        day_index = int(row["Day"])
        existing_day_details = day_wise_dict.get(day_index, None)

        if existing_day_details:
            itinerary_items: list[dict[str, Any]] = existing_day_details["itinerary"]
            itinerary_item = build_itinerary_item(row, activity_cache, transfer_cache)
            itinerary_items.append(itinerary_item)
        else:
            city_doc = location_cache.city(row["City"])
            if not city_doc:
                raise ValueError(f"City not found: {row['City']}")

            itinerary_items: list[dict[str, Any]] = []
            itinerary_item = build_itinerary_item(row, activity_cache, transfer_cache)
            itinerary_items.append(itinerary_item)

            day_wise_dict[day_index] = {
                "city": city_doc["name"],
                "city_id": city_doc["_id"],
                "description": None, # AI support required for this field
                "itinerary": itinerary_items,
                "pace": None, # AI support required for this field
            }
    return list(day_wise_dict.values())

# ----------------------------- DB connections -------------------------------

def get_db_clients() -> tuple[MongoClient, Any, Any, Any, Any]:
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI env var is not set")
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        tlsAllowInvalidCertificates=True,
        tlsAllowInvalidHostnames=True,
    )
    package_itinerary_db = client[PACKAGE_ITINERARY_DB]
    location_db = client[LOCATION_DB]
    activities_db = client[ACTIVITIES_DB]
    transfer_db = client[TRANSFER_DB]
    return client, package_itinerary_db, location_db, activities_db, transfer_db


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

    return (len(reasons) == 0), reasons


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

def run(dry_run: bool) -> None:
    setup_logging()
    log = logging.getLogger("package-migrate")
    log.info("Reading CSVs (dry_run=%s)", dry_run)

    package_metadata_rows = read_package_metadata_rows(METADATA_CSV)
    package_itinerary_rows = read_package_itinerary_rows(PACKAGE_CSV)

    log.info(
        "Package metadata rows: %d | Package itinerary rows: %d",
        len(package_metadata_rows), len(package_itinerary_rows),
    )

    if len(package_metadata_rows) != len(package_itinerary_rows):
        log.error("Package metadata rows: %d | Package itinerary rows: %d", len(package_metadata_rows), len(package_itinerary_rows))
        return

    client, package_itinerary_db, location_db, activities_db, transfer_db = get_db_clients()

    package_itinerary_coll = package_itinerary_db["package_itinerary"]
    country_coll = location_db["country"]
    city_coll = location_db["city"]
    continent_coll = location_db["continent"]
    destination_coll = location_db["destination"]

    activity_coll = activities_db["activity"]
    transfer_coll = transfer_db["within_city_transfer"]

    location_cache = LocationCache(city_coll, country_coll, continent_coll, destination_coll)
    activity_cache = ActivityCache(activity_coll)
    transfer_cache = TransferCache(transfer_coll)

    report_rows: list[dict[str, Any]] = []

    insert_package_docs: list[dict[str, Any]] = []
    for package_metadata_row in package_metadata_rows:
        try:
            package_name = package_metadata_row["Package_Name"]
            package_itinerary_row = package_itinerary_rows[package_name]
            day_wise_details = build_day_wise_details(package_itinerary_row, location_cache, activity_cache, transfer_cache)
            package_doc = build_package_itinerary_doc(package_metadata_row, day_wise_details, location_cache)
            insert_package_docs.append(package_doc)
            report_rows.append({
                "package_name": package_name,
                "status": "success",
                "reason": "",
                "mode": "dry_run" if dry_run else "live",
            })
        except Exception as e:
            log.error(f"Error processing package: {e}")
            report_rows.append({
                "package_name": package_name,
                "status": "error",
                "reason": str(e),
                "mode": "dry_run" if dry_run else "live",
            })
            continue

        if dry_run:
            continue


    # ----------------------------- Summary -----------------------------
    log.info("Summary:")

    if dry_run:
        log.info("DRY RUN: skipping all writes. Set DRY_RUN=False at the top of the file to insert.")
        if insert_package_docs:
            log.info("Sample package doc (insert):\n%s", insert_package_docs[0])
        write_report(report_rows, dry_run=True, log=log)
        client.close()
        return
    if insert_package_docs:
        result = package_itinerary_coll.insert_many(insert_package_docs, ordered=False)
        log.info("Inserted %d package docs", len(result.inserted_ids))

    write_report(report_rows, dry_run=False, log=log)
    client.close()
    log.info("Done.")


if __name__ == "__main__":
    run(dry_run=DRY_RUN)
