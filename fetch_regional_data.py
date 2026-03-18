"""
Fetch and normalize regional employment-by-occupation data.

This pipeline keeps country-level inputs in official ILOSTAT major groups, then
projects them into the same 342 canonical occupation slugs used by the US view.
The canonical projection is deterministic and committed to the repo so the
frontend and Vercel build can work without live network access.

Usage:
    python fetch_regional_data.py
    uv run python fetch_regional_data.py
"""

from __future__ import annotations

import csv
import io
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import httpx

from regional_taxonomy import FAMILY_DEFINITIONS, build_us_family_weights

CATALOG_FILE = Path("regional_source_catalog.json")
US_OCCUPATIONS_FILE = Path("occupations.csv")
CROSSWALK_FILE = Path("regional_crosswalk.json")
EMPLOYMENT_FILE = Path("regional_employment.json")

NATIVE_GROUP_FAMILIES = {
    "CN01": ("1",),
    "CN02": ("2", "3"),
    "CN04": ("4",),
    "CN05": ("5",),
    "CN06": ("6",),
    "CN78": ("7", "8"),
    "CN99": ("9",),
}


def load_json(path: Path) -> dict:
    with path.open() as file:
        return json.load(file)


def load_us_rows() -> list[dict]:
    with US_OCCUPATIONS_FILE.open() as file:
        return list(csv.DictReader(file))


def country_configs_by_code(catalog: dict) -> dict[str, dict]:
    return {country["code"]: country for country in catalog["countries"]}


def extract_live_native_code(classif: str, prefix: str) -> str | None:
    if not classif.startswith(prefix):
        return None
    suffix = classif[len(prefix) :]
    if suffix in {"TOTAL", "X"}:
        return None
    if suffix in FAMILY_DEFINITIONS:
        return suffix
    return None


def fetch_live_country_rows(client: httpx.Client, catalog: dict, country_cfg: dict) -> list[dict]:
    response = client.get(
        catalog["apiUrl"],
        params={
            "id": catalog["indicator"],
            "ref_area": country_cfg["code"],
            "sex": "SEX_T",
        },
        timeout=60,
    )
    response.raise_for_status()
    rows = []
    for raw in csv.DictReader(io.StringIO(response.text)):
        native_code = extract_live_native_code(raw.get("classif1", ""), country_cfg["classificationPrefix"])
        if native_code is None or not raw.get("obs_value"):
            continue
        rows.append(
            {
                "countryCode": country_cfg["code"],
                "countryName": country_cfg["name"],
                "year": int(raw["time"]),
                "nativeCode": native_code,
                "nativeLabel": FAMILY_DEFINITIONS[native_code],
                "jobs": int(round(float(raw["obs_value"]) * 1000)),
                "source": raw.get("source") or "",
                "classification": country_cfg["nativeCodeSystem"].split()[0].replace("-", ""),
                "nativeCodeSystem": country_cfg["nativeCodeSystem"],
                "ingestMode": country_cfg["ingestMode"],
                "indicator": catalog["indicator"],
            }
        )
    return rows


def load_cached_country_rows(catalog: dict, country_cfg: dict) -> list[dict]:
    cache_path = Path(country_cfg["cachePath"])
    with cache_path.open() as file:
        rows = []
        for raw in csv.DictReader(file):
            native_code = raw["nativeCode"]
            if native_code not in FAMILY_DEFINITIONS:
                continue
            rows.append(
                {
                    "countryCode": country_cfg["code"],
                    "countryName": country_cfg["name"],
                    "year": int(raw["year"]),
                    "nativeCode": native_code,
                    "nativeLabel": FAMILY_DEFINITIONS[native_code],
                    "jobs": int(raw["jobs"]),
                    "source": raw.get("source") or "",
                    "classification": raw.get("classification") or country_cfg["nativeCodeSystem"].split()[0].replace("-", ""),
                    "nativeCodeSystem": country_cfg["nativeCodeSystem"],
                    "ingestMode": country_cfg["ingestMode"],
                    "indicator": catalog["indicator"],
                    "sourceUrl": raw.get("sourceUrl") or country_cfg.get("sourceUrl") or catalog["sourceUrl"],
                    "sourceYearLabel": raw.get("sourceYearLabel") or str(raw["year"]),
                    "sourceDetail": raw.get("sourceDetail") or country_cfg.get("sourceNote") or "",
                }
            )
    return rows


def load_country_rows(catalog: dict, client: httpx.Client) -> dict[str, list[dict]]:
    countries = country_configs_by_code(catalog)
    rows_by_country = {}
    for code, country_cfg in countries.items():
        if country_cfg["ingestMode"] in {"cached_official_extract", "manual_curated_official_mix"}:
            rows = load_cached_country_rows(catalog, country_cfg)
        else:
            rows = fetch_live_country_rows(client, catalog, country_cfg)
        if not rows:
            raise ValueError(f"No occupation rows found for {code}")
        rows_by_country[code] = rows
    return rows_by_country


def compute_shared_year(country_rows: dict[str, list[dict]], country_codes: list[str]) -> int:
    year_sets = []
    for code in country_codes:
        years = {row["year"] for row in country_rows[code]}
        if not years:
            raise ValueError(f"Cannot compute shared year for {code} with no data")
        year_sets.append(years)
    shared = set.intersection(*year_sets)
    if not shared:
        raise ValueError("No shared year found across countries")
    return max(shared)


def choose_region_years(region_cfg: dict, region_country_codes: list[str], country_rows: dict[str, list[dict]]) -> tuple[dict[str, int], int | None, str]:
    if region_cfg["yearStrategy"] == "latest_shared":
        shared_year = compute_shared_year(country_rows, region_country_codes)
        return {code: shared_year for code in region_country_codes}, shared_year, str(shared_year)

    selected = {}
    labels = []
    for code in region_country_codes:
        years = sorted({row["year"] for row in country_rows[code]})
        if not years:
            raise ValueError(f"No occupation years available for {code}")
        selected_year = years[-1]
        selected[code] = selected_year
        country_name = next(row["countryName"] for row in country_rows[code] if row["year"] == selected_year)
        labels.append(f"{country_name} {selected_year}")
    return selected, None, " / ".join(labels)


def family_baseline_confidence(family_weights: dict[str, list[dict]]) -> dict[str, float]:
    baselines = {}
    for family_code, weights in family_weights.items():
        total_weight = sum(item["weight"] for item in weights) or 1.0
        average = sum(item["weight"] * item["assignmentConfidence"] for item in weights) / total_weight
        baselines[family_code] = round(average, 2)
    return baselines


def occupation_weights_for_native_code(native_code: str, family_weights: dict[str, list[dict]]) -> list[dict]:
    family_codes = NATIVE_GROUP_FAMILIES.get(native_code, (native_code,))
    combined = []
    for family_code in family_codes:
        combined.extend(family_weights.get(family_code, []))
    if not combined:
        raise KeyError(f"No family weights found for native code {native_code}")

    total_weight = sum(item["weight"] for item in combined) or 1.0
    return [
        {
            **item,
            "weight": item["weight"] / total_weight,
        }
        for item in combined
    ]


def source_quality_factor(country_cfg: dict) -> float:
    factor = 0.88
    if "ISCO-88" in country_cfg["nativeCodeSystem"]:
        factor -= 0.08
    if country_cfg["ingestMode"] == "cached_official_extract":
        factor -= 0.04
    if country_cfg["ingestMode"] == "manual_curated_official_mix":
        factor -= 0.02
    return max(0.6, factor)


def build_country_crosswalk(country_cfg: dict, selected_rows: list[dict], family_weights: dict[str, list[dict]]) -> dict:
    total_jobs = 0
    mappings = []
    quality_factor = source_quality_factor(country_cfg)

    for row in sorted(selected_rows, key=lambda item: item["nativeCode"]):
        occupation_weights = occupation_weights_for_native_code(row["nativeCode"], family_weights)
        total_weight = sum(item["weight"] for item in occupation_weights) or 1.0
        normalized_weights = []
        average_assignment_confidence = 0.0

        for item in occupation_weights:
            normalized_weight = item["weight"] / total_weight
            normalized_weights.append(
                {
                    "slug": item["slug"],
                    "title": item["title"],
                    "weight": normalized_weight,
                    "assignmentConfidence": item["assignmentConfidence"],
                    "assignmentReason": item["assignmentReason"],
                }
            )
            average_assignment_confidence += normalized_weight * item["assignmentConfidence"]

        mapping_type = "direct" if len(normalized_weights) == 1 else "weighted_crosswalk"
        mapping_confidence = round(min(0.99, average_assignment_confidence * quality_factor), 2)
        total_jobs += row["jobs"]
        mappings.append(
            {
                "nativeCode": row["nativeCode"],
                "nativeLabel": row["nativeLabel"],
                "jobs": row["jobs"],
                "year": row["year"],
                "classification": row["classification"],
                "mappingType": mapping_type,
                "mappingConfidence": mapping_confidence,
                "occupationWeights": normalized_weights,
            }
        )

    selected_year = selected_rows[0]["year"]
    return {
        "code": country_cfg["code"],
        "name": country_cfg["name"],
        "region": country_cfg["region"],
        "ingestMode": country_cfg["ingestMode"],
        "nativeCodeSystem": country_cfg["nativeCodeSystem"],
        "fileFormat": country_cfg["fileFormat"],
        "refreshCadence": country_cfg["refreshCadence"],
        "sourceYear": selected_year,
        "sourceYearLabel": selected_rows[0].get("sourceYearLabel") or str(selected_year),
        "classification": selected_rows[0]["classification"],
        "jobs": total_jobs,
        "cachePath": country_cfg.get("cachePath"),
        "sourceLabel": country_cfg.get("sourceLabel") or "ILOSTAT employment by occupation",
        "sourceUrl": country_cfg.get("sourceUrl"),
        "publicationTitle": country_cfg.get("publicationTitle"),
        "publicationYear": country_cfg.get("publicationYear"),
        "tableTitle": country_cfg.get("tableTitle"),
        "sourceNote": country_cfg.get("sourceNote"),
        "mappings": mappings,
    }


def allocate_jobs(total_jobs: int, occupation_weights: list[dict]) -> list[dict]:
    if total_jobs <= 0:
        return [{"slug": item["slug"], "jobs": 0} for item in occupation_weights]

    total_weight = sum(item["weight"] for item in occupation_weights) or 1.0
    interim = []
    allocated = 0
    for index, item in enumerate(occupation_weights):
        raw_jobs = total_jobs * (item["weight"] / total_weight)
        floored_jobs = int(raw_jobs)
        allocated += floored_jobs
        interim.append(
            {
                "slug": item["slug"],
                "jobs": floored_jobs,
                "fraction": raw_jobs - floored_jobs,
                "index": index,
            }
        )

    remainder = total_jobs - allocated
    for item in sorted(interim, key=lambda value: (-value["fraction"], value["index"]))[:remainder]:
        item["jobs"] += 1

    return [{"slug": item["slug"], "jobs": item["jobs"]} for item in interim]


def write_json_preserving_timestamp(path: Path, payload: dict) -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    existing_payload = None
    if path.exists():
        existing_payload = load_json(path)

    def normalized(data: dict) -> dict:
        stripped = json.loads(json.dumps(data))
        stripped.pop("generatedAt", None)
        return stripped

    if existing_payload and normalized(existing_payload) == normalized(payload):
        generated_at = existing_payload.get("generatedAt") or generated_at

    payload["generatedAt"] = generated_at
    with path.open("w") as file:
        json.dump(payload, file, indent=2)
        file.write("\n")


def main() -> None:
    catalog = load_json(CATALOG_FILE)
    countries = country_configs_by_code(catalog)
    us_rows = load_us_rows()
    family_weights, assignments = build_us_family_weights(us_rows)
    family_baselines = family_baseline_confidence(family_weights)

    with httpx.Client(follow_redirects=True) as client:
        all_country_rows = load_country_rows(catalog, client)

    region_selection = {}
    for region_id, region_cfg in catalog["regions"].items():
        region_selection[region_id] = choose_region_years(region_cfg, region_cfg["countries"], all_country_rows)

    crosswalk_countries = []
    country_crosswalk_by_code = {}
    for region_id, region_cfg in catalog["regions"].items():
        selected_years, _, _ = region_selection[region_id]
        for code in region_cfg["countries"]:
            country_cfg = countries[code]
            chosen_rows = [row for row in all_country_rows[code] if row["year"] == selected_years[code]]
            country_crosswalk = build_country_crosswalk(country_cfg, chosen_rows, family_weights)
            crosswalk_countries.append(country_crosswalk)
            country_crosswalk_by_code[code] = country_crosswalk

    crosswalk_payload = {
        "generatedAt": None,
        "source": {
            "label": catalog["sourceLabel"],
            "url": catalog["sourceUrl"],
            "indicator": catalog["indicator"],
        },
        "canonicalTaxonomy": {
            "label": "US BLS occupations",
            "slugCount": len(us_rows),
        },
        "countries": crosswalk_countries,
    }

    regions = {}
    for region_id, region_cfg in catalog["regions"].items():
        region_country_codes = region_cfg["countries"]
        selected_years, shared_year, year_label = region_selection[region_id]

        country_payloads = []
        occupation_map = {}
        for row in us_rows:
            family_code = assignments[row["slug"]]["familyCode"]
            family_members = family_weights[family_code]
            occupation_map[row["slug"]] = {
                "slug": row["slug"],
                "title": row["title"],
                "category": row["category"],
                "jobs": 0,
                "sourceYear": shared_year,
                "sourceYearLabel": year_label,
                "countryBreakdown": [],
                "mappingType": "direct" if len(family_members) == 1 else "weighted_crosswalk",
                "mappingConfidence": family_baselines[family_code],
                "source": {
                    "label": catalog["sourceLabel"],
                    "url": catalog["sourceUrl"],
                    "indicator": catalog["indicator"],
                },
                "_confidenceWeightedJobs": 0.0,
                "_familyCode": family_code,
            }

        for code in region_country_codes:
            country_cfg = countries[code]
            chosen_year = selected_years[code]
            country_crosswalk = country_crosswalk_by_code[code]
            country_payloads.append(
                {
                    key: value
                    for key, value in country_crosswalk.items()
                    if key != "mappings"
                }
            )

            for mapping in country_crosswalk["mappings"]:
                allocations = allocate_jobs(mapping["jobs"], mapping["occupationWeights"])
                for allocation in allocations:
                    if allocation["jobs"] <= 0:
                        continue
                    occupation = occupation_map[allocation["slug"]]
                    occupation["jobs"] += allocation["jobs"]
                    occupation["_confidenceWeightedJobs"] += allocation["jobs"] * mapping["mappingConfidence"]
                    occupation["countryBreakdown"].append(
                        {
                            "country": country_cfg["name"],
                            "countryCode": country_cfg["code"],
                            "jobs": allocation["jobs"],
                            "year": chosen_year,
                            "sourceYear": chosen_year,
                            "classification": mapping["classification"],
                            "source": country_cfg.get("sourceLabel") or catalog["sourceLabel"],
                            "ingestMode": country_cfg["ingestMode"],
                            "mappingType": mapping["mappingType"],
                            "mappingConfidence": mapping["mappingConfidence"],
                            "nativeCode": mapping["nativeCode"],
                            "nativeLabel": mapping["nativeLabel"],
                        }
                    )

        occupations = []
        for occupation in occupation_map.values():
            if occupation["jobs"] > 0:
                occupation["mappingConfidence"] = round(
                    occupation["_confidenceWeightedJobs"] / occupation["jobs"],
                    2,
                )
            occupation["countryBreakdown"].sort(key=lambda item: item["jobs"], reverse=True)
            occupation.pop("_confidenceWeightedJobs", None)
            occupation.pop("_familyCode", None)
            occupations.append(occupation)

        occupations.sort(key=lambda item: (-item["jobs"], item["title"]))

        live_countries = [item["name"] for item in country_payloads if item["ingestMode"] == "live_ilostat_api"]
        manual_countries = [item["name"] for item in country_payloads if item["ingestMode"] == "manual_curated_official_mix"]
        cached_countries = [item["name"] for item in country_payloads if item["ingestMode"] == "cached_official_extract"]
        if manual_countries:
            freshness_summary = f"{len(live_countries)} live ILOSTAT feeds + {len(manual_countries)} checked-in official national extract"
        elif cached_countries:
            freshness_summary = f"{len(live_countries)} live ILOSTAT feeds + {len(cached_countries)} cached official extract"
        else:
            freshness_summary = f"{len(live_countries)} live ILOSTAT feeds"

        regions[region_id] = {
            "id": region_id,
            "label": region_cfg["label"],
            "yearStrategy": region_cfg["yearStrategy"],
            "year": shared_year,
            "yearLabel": year_label,
            "note": region_cfg["note"],
            "countries": country_payloads,
            "freshness": {
                "summary": freshness_summary,
                "liveCountries": live_countries,
                "manualCountries": manual_countries,
                "cachedCountries": cached_countries,
                "hasCachedSources": bool(cached_countries),
                "hasManualSources": bool(manual_countries),
            },
            "occupations": occupations,
            "source": {
                "label": catalog["sourceLabel"],
                "url": catalog["sourceUrl"],
                "indicator": catalog["indicator"],
            },
        }

    employment_payload = {
        "generatedAt": None,
        "source": {
            "label": catalog["sourceLabel"],
            "url": catalog["sourceUrl"],
            "indicator": catalog["indicator"],
        },
        "regions": regions,
    }

    write_json_preserving_timestamp(CROSSWALK_FILE, crosswalk_payload)
    write_json_preserving_timestamp(EMPLOYMENT_FILE, employment_payload)
    print(f"Wrote regional crosswalk to {CROSSWALK_FILE}")
    print(f"Wrote regional employment to {EMPLOYMENT_FILE}")


if __name__ == "__main__":
    main()
