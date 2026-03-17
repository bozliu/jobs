"""
Fetch and normalize regional employment-by-occupation data from ILOSTAT.

The output is a deterministic JSON artifact committed to the repo so the
frontend and Vercel build can work without live network access.

Usage:
    python fetch_regional_data.py
    uv run python fetch_regional_data.py
"""

from __future__ import annotations

import csv
import io
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

import httpx

ILO_INDICATOR_URL = "https://rplumber.ilo.org/data/indicator/"
ILO_DATASET_URL = "https://ilostat.ilo.org/data/"
OUTPUT_FILE = "regional_employment.json"

COUNTRIES = {
    "AUS": {"name": "Australia", "class_prefixes": ("OCU_ISCO08_",)},
    "CHN": {"name": "China", "class_prefixes": ("OCU_ISCO88_",)},
    "IND": {"name": "India", "class_prefixes": ("OCU_ISCO08_",)},
    "JPN": {"name": "Japan", "class_prefixes": ("OCU_ISCO08_",)},
    "DEU": {"name": "Germany", "class_prefixes": ("OCU_ISCO08_",)},
    "GBR": {"name": "United Kingdom", "class_prefixes": ("OCU_ISCO08_",)},
    "FRA": {"name": "France", "class_prefixes": ("OCU_ISCO08_",)},
    "ITA": {"name": "Italy", "class_prefixes": ("OCU_ISCO08_",)},
    "ESP": {"name": "Spain", "class_prefixes": ("OCU_ISCO08_",)},
}

REGIONS = {
    "asia": {
        "label": "Asia",
        "countries": ["CHN", "JPN", "IND", "AUS"],
        "year_strategy": "latest_per_country",
        "note": (
            "Asia is merged from the latest country-level occupation year available in ILOSTAT. "
            "China is currently only published there in older ISCO-88 occupational splits, so this "
            "regional view uses mixed country years instead of one shared year."
        ),
    },
    "europe": {
        "label": "Europe",
        "countries": ["DEU", "GBR", "FRA", "ITA", "ESP"],
        "year_strategy": "latest_shared",
        "note": (
            "Europe is merged from the latest common occupation year shared across the selected countries in ILOSTAT."
        ),
    },
}


def load_group_definitions() -> list[dict]:
    with open("regional_occupations.json") as file:
        return json.load(file)


def fetch_country_rows(client: httpx.Client, country_code: str) -> list[dict]:
    response = client.get(
        ILO_INDICATOR_URL,
        params={
            "id": "EMP_TEMP_SEX_OCU_NB",
            "ref_area": country_code,
            "sex": "SEX_T",
        },
        timeout=60,
    )
    response.raise_for_status()
    return list(csv.DictReader(io.StringIO(response.text)))


def extract_group_code(row: dict, prefixes: tuple[str, ...]) -> str | None:
    classif = row["classif1"]
    for prefix in prefixes:
        if not classif.startswith(prefix):
            continue
        suffix = classif[len(prefix) :]
        if suffix in {"TOTAL", "X"}:
            return None
        if suffix.isdigit():
            return suffix
    return None


def filter_country_rows(rows: list[dict], country_code: str) -> list[dict]:
    prefixes = COUNTRIES[country_code]["class_prefixes"]
    filtered = []
    for row in rows:
        group_code = extract_group_code(row, prefixes)
        if group_code is None:
            continue
        value = row.get("obs_value")
        if not value:
            continue
        filtered.append(
            {
                "country": country_code,
                "country_name": COUNTRIES[country_code]["name"],
                "group_code": group_code,
                "year": int(row["time"]),
                "jobs": int(round(float(value) * 1000)),
                "source": row.get("source") or "",
                "classification": row["classif1"].split("_")[1],
                "indicator": row.get("indicator") or "EMP_TEMP_SEX_OCU_NB",
            }
        )
    return filtered


def select_country_year(rows: list[dict], strategy: str, shared_year: int | None) -> int:
    years = sorted({row["year"] for row in rows})
    if not years:
        raise ValueError("No occupation years available")
    if strategy == "latest_shared":
        if shared_year is None:
            raise ValueError("Missing shared year for latest_shared strategy")
        return shared_year
    return years[-1]


def compute_shared_year(country_rows: dict[str, list[dict]]) -> int:
    year_sets = []
    for rows in country_rows.values():
        years = {row["year"] for row in rows}
        if not years:
            raise ValueError("Cannot compute shared year with empty country data")
        year_sets.append(years)
    shared = set.intersection(*year_sets)
    if not shared:
        raise ValueError("No shared year found across countries")
    return max(shared)


def build_region(region_id: str, country_rows: dict[str, list[dict]], groups: list[dict]) -> dict:
    config = REGIONS[region_id]
    selected_rows = {code: country_rows[code] for code in config["countries"]}
    shared_year = compute_shared_year(selected_rows) if config["year_strategy"] == "latest_shared" else None

    country_metadata = []
    by_group: dict[str, list[dict]] = defaultdict(list)

    for code, rows in selected_rows.items():
        chosen_year = select_country_year(rows, config["year_strategy"], shared_year)
        chosen_rows = [row for row in rows if row["year"] == chosen_year]
        total_jobs = sum(row["jobs"] for row in chosen_rows)
        classification = sorted({row["classification"] for row in chosen_rows})[0]
        sources = sorted({row["source"] for row in chosen_rows if row["source"]})
        country_metadata.append(
            {
                "code": code,
                "name": COUNTRIES[code]["name"],
                "year": chosen_year,
                "classification": classification,
                "jobs": total_jobs,
                "sources": sources,
            }
        )
        for row in chosen_rows:
            by_group[row["group_code"]].append(row)

    occupations = []
    for group in groups:
        breakdown = []
        total_jobs = 0
        for code in group["classCodes"]:
            for row in by_group.get(code, []):
                breakdown.append(
                    {
                        "country": row["country_name"],
                        "countryCode": row["country"],
                        "jobs": row["jobs"],
                        "year": row["year"],
                        "classification": row["classification"],
                        "source": row["source"],
                    }
                )
                total_jobs += row["jobs"]
        if not breakdown:
            continue
        breakdown.sort(key=lambda item: item["jobs"], reverse=True)
        occupations.append(
            {
                "slug": group["slug"],
                "title": group["title"],
                "groupCode": group["classCodes"][0],
                "jobs": total_jobs,
                "countryBreakdown": breakdown,
                "source": {
                    "label": "ILOSTAT employment by occupation",
                    "url": ILO_DATASET_URL,
                    "indicator": "EMP_TEMP_SEX_OCU_NB",
                },
            }
        )

    occupations.sort(key=lambda item: item["jobs"], reverse=True)
    year_label = (
        str(shared_year)
        if shared_year is not None
        else " / ".join(f"{item['name']} {item['year']}" for item in country_metadata)
    )

    return {
        "id": region_id,
        "label": config["label"],
        "yearStrategy": config["year_strategy"],
        "year": shared_year,
        "yearLabel": year_label,
        "note": config["note"],
        "countries": country_metadata,
        "occupations": occupations,
        "source": {
            "label": "ILOSTAT employment by occupation",
            "url": ILO_DATASET_URL,
            "indicator": "EMP_TEMP_SEX_OCU_NB",
        },
    }


def main() -> None:
    groups = load_group_definitions()
    client = httpx.Client(follow_redirects=True)
    try:
        country_rows = {}
        for code in COUNTRIES:
            raw_rows = fetch_country_rows(client, code)
            filtered = filter_country_rows(raw_rows, code)
            if not filtered:
                raise ValueError(f"No occupation rows found for {code}")
            country_rows[code] = filtered
    finally:
        client.close()

    payload = {
        "generatedAt": None,
        "source": {
            "label": "ILOSTAT employment by occupation",
            "url": ILO_DATASET_URL,
            "indicator": "EMP_TEMP_SEX_OCU_NB",
        },
        "regions": {
            region_id: build_region(region_id, country_rows, groups)
            for region_id in REGIONS
        },
    }

    generated_at = datetime.now(timezone.utc).isoformat()
    existing_payload = None
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as file:
            existing_payload = json.load(file)

    def normalized(data: dict) -> dict:
        stripped = json.loads(json.dumps(data))
        stripped.pop("generatedAt", None)
        return stripped

    if existing_payload and normalized(existing_payload) == normalized(payload):
        generated_at = existing_payload.get("generatedAt") or generated_at

    payload["generatedAt"] = generated_at

    with open(OUTPUT_FILE, "w") as file:
        json.dump(payload, file, indent=2)

    print(f"Wrote regional employment data to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
