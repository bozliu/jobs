"""
Build a structured JSON payload for the static website.

The output is designed for a narrative, data-rich frontend. In addition to the
occupation records used by the treemap, it includes summary metrics, category
snapshots, and curated story slices derived from the BLS data and AI exposure
scores.

Usage:
    python build_site_data.py
    uv run python build_site_data.py
"""

from __future__ import annotations

import csv
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

REPO_URL = "https://github.com/bozliu/jobs"
BLS_URL = "https://www.bls.gov/ooh/"
LARGE_OCCUPATION_MIN_JOBS = 150_000

CATEGORY_LABELS = {
    "architecture-and-engineering": "Architecture & Engineering",
    "arts-and-design": "Arts & Design",
    "building-and-grounds-cleaning": "Building & Grounds Cleaning",
    "business-and-financial": "Business & Financial",
    "community-and-social-service": "Community & Social Service",
    "computer-and-information-technology": "Computer & Information Technology",
    "construction-and-extraction": "Construction & Extraction",
    "education-training-and-library": "Education, Training & Library",
    "entertainment-and-sports": "Entertainment & Sports",
    "farming-fishing-and-forestry": "Farming, Fishing & Forestry",
    "food-preparation-and-serving": "Food Preparation & Serving",
    "healthcare": "Healthcare",
    "installation-maintenance-and-repair": "Installation, Maintenance & Repair",
    "legal": "Legal",
    "life-physical-and-social-science": "Life, Physical & Social Science",
    "management": "Management",
    "math": "Math",
    "media-and-communication": "Media & Communication",
    "military": "Military",
    "office-and-administrative-support": "Office & Administrative Support",
    "personal-care-and-service": "Personal Care & Service",
    "production": "Production",
    "protective-service": "Protective Service",
    "sales": "Sales",
    "transportation-and-material-moving": "Transportation & Material Moving",
}

DEGREE_FORWARD_EDUCATION = {
    "Bachelor's degree",
    "Master's degree",
    "Doctoral or professional degree",
}

PHYSICAL_CATEGORIES = {
    "building-and-grounds-cleaning",
    "construction-and-extraction",
    "farming-fishing-and-forestry",
    "food-preparation-and-serving",
    "installation-maintenance-and-repair",
    "personal-care-and-service",
    "production",
    "transportation-and-material-moving",
}


def weighted_average(records: list[dict], key: str) -> float | None:
    weighted_total = 0.0
    total_jobs = 0
    for record in records:
        value = record.get(key)
        jobs = record.get("jobs") or 0
        if value is None or jobs <= 0:
            continue
        weighted_total += value * jobs
        total_jobs += jobs
    if total_jobs == 0:
        return None
    return round(weighted_total / total_jobs, 2)


def dominant_education(records: list[dict]) -> str | None:
    education_jobs: dict[str, int] = defaultdict(int)
    for record in records:
        education = record.get("education")
        jobs = record.get("jobs") or 0
        if not education or jobs <= 0:
            continue
        education_jobs[education] += jobs
    if not education_jobs:
        return None
    return max(education_jobs.items(), key=lambda item: item[1])[0]


def make_occupation_snapshot(record: dict | None) -> dict | None:
    if not record:
        return None
    return {
        "title": record["title"],
        "slug": record["slug"],
        "category": record["category"],
        "categoryLabel": record["category_label"],
        "jobs": record["jobs"],
        "pay": record["pay"],
        "outlook": record["outlook"],
        "outlookDesc": record["outlook_desc"],
        "education": record["education"],
        "exposure": record["exposure"],
        "url": record["url"],
    }


def select_top(records: list[dict], key: str, limit: int = 1, reverse: bool = True) -> list[dict]:
    eligible = [record for record in records if record.get(key) is not None]
    return sorted(
        eligible,
        key=lambda record: (record.get(key), record.get("jobs") or 0),
        reverse=reverse,
    )[:limit]


def select_large(records: list[dict]) -> list[dict]:
    return [record for record in records if (record.get("jobs") or 0) >= LARGE_OCCUPATION_MIN_JOBS]


def build_categories(records: list[dict], total_jobs: int) -> list[dict]:
    by_category: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        by_category[record["category"]].append(record)

    categories = []
    for slug, items in by_category.items():
        jobs = sum(item["jobs"] or 0 for item in items)
        average_pay = weighted_average(items, "pay")
        average_outlook = weighted_average(items, "outlook")
        average_exposure = weighted_average(items, "exposure")
        top_jobs = select_top(items, "jobs", limit=3)
        top_growth = select_top(items, "outlook", limit=1)
        categories.append(
            {
                "slug": slug,
                "label": CATEGORY_LABELS.get(slug, slug.replace("-", " ").title()),
                "occupationCount": len(items),
                "jobs": jobs,
                "shareOfJobs": round((jobs / total_jobs) * 100, 2) if total_jobs else 0,
                "averagePay": average_pay,
                "averageOutlook": average_outlook,
                "averageExposure": average_exposure,
                "dominantEducation": dominant_education(items),
                "topOccupations": [make_occupation_snapshot(item) for item in top_jobs],
                "fastestGrowingOccupation": make_occupation_snapshot(top_growth[0]) if top_growth else None,
            }
        )

    return sorted(categories, key=lambda category: category["jobs"], reverse=True)


def build_stories(records: list[dict], categories: list[dict]) -> list[dict]:
    growth_leaders = select_top(records, "outlook", limit=5)
    pay_leaders = select_top(records, "pay", limit=5)

    exposure_frontline = sorted(
        [
            record
            for record in select_large(records)
            if record.get("exposure") is not None
            and record.get("education") in DEGREE_FORWARD_EDUCATION
        ],
        key=lambda record: (record["exposure"], record["jobs"] or 0),
        reverse=True,
    )[:5]

    resilient_frontline = sorted(
        [
            record
            for record in select_large(records)
            if record.get("exposure") is not None and record["category"] in PHYSICAL_CATEGORIES
        ],
        key=lambda record: (record["exposure"], -(record["jobs"] or 0)),
    )[:5]

    salary_categories = sorted(
        [category for category in categories if category["averagePay"] is not None],
        key=lambda category: category["averagePay"],
        reverse=True,
    )[:4]

    return [
        {
            "id": "growth-engines",
            "eyebrow": "Story 01",
            "title": "Growth is clustering at the technical, clinical, and energy edge.",
            "description": (
                f"The fastest line in the dataset is {growth_leaders[0]['title']} at {growth_leaders[0]['outlook']}% projected growth. "
                f"The rest of the top tier shows a clear pattern: renewable infrastructure, advanced care, "
                f"and analytics-heavy roles are expanding faster than the market baseline."
            ),
            "metricLabel": "Fastest growth",
            "metricValue": growth_leaders[0]["outlook"],
            "metricFormat": "percent",
            "metricContext": growth_leaders[0]["title"],
            "occupations": [make_occupation_snapshot(record) for record in growth_leaders],
        },
        {
            "id": "pay-gravity",
            "eyebrow": "Story 02",
            "title": "The highest wages concentrate in leadership, medicine, and technical specialization.",
            "description": (
                f"The top salary in the dataset is {pay_leaders[0]['title']} at ${pay_leaders[0]['pay']:,}. "
                f"Yet the broader pattern matters more: high-compensation work tends to sit at the intersection "
                f"of credentialing, managerial leverage, or scarce technical expertise."
            ),
            "metricLabel": "Highest pay",
            "metricValue": pay_leaders[0]["pay"],
            "metricFormat": "currency",
            "metricContext": pay_leaders[0]["title"],
            "occupations": [make_occupation_snapshot(record) for record in pay_leaders],
            "categories": salary_categories,
        },
        {
            "id": "ai-frontline",
            "eyebrow": "Story 03",
            "title": "AI pressure is strongest in high-volume digital and administrative work.",
            "description": (
                f"On the high-exposure end, the signal from {exposure_frontline[0]['title']} is clear, but scale is the real story. "
                f"Roles built around documents, communication, coding, or repetitive screen-based workflows are "
                f"where current AI systems can already reshape throughput."
            ),
            "metricLabel": "Most exposed large role",
            "metricValue": exposure_frontline[0]["exposure"],
            "metricFormat": "score",
            "metricContext": exposure_frontline[0]["title"],
            "occupations": [make_occupation_snapshot(record) for record in exposure_frontline],
        },
        {
            "id": "embodied-work",
            "eyebrow": "Story 04",
            "title": "Embodied work remains the hardest frontier for automation to cross.",
            "description": (
                f"At the low-exposure end of the map, the pattern around {resilient_frontline[0]['title']} sets the tone. "
                f"Physical presence, dexterity, and messy real-world environments still create durable friction "
                f"for AI-first substitution, even in very large job categories."
            ),
            "metricLabel": "Lowest exposure at scale",
            "metricValue": resilient_frontline[0]["exposure"],
            "metricFormat": "score",
            "metricContext": resilient_frontline[0]["title"],
            "occupations": [make_occupation_snapshot(record) for record in resilient_frontline],
        },
    ]


def build_summary(records: list[dict], categories: list[dict]) -> dict:
    total_jobs = sum(record["jobs"] or 0 for record in records)
    large_records = select_large(records)

    fastest_growth = select_top(records, "outlook", limit=1)[0]
    highest_pay = select_top(records, "pay", limit=1)[0]
    largest_role = select_top(records, "jobs", limit=1)[0]
    most_exposed_large = sorted(
        [record for record in large_records if record.get("exposure") is not None],
        key=lambda record: (record["exposure"], record["jobs"] or 0),
        reverse=True,
    )[0]
    most_resilient_large = sorted(
        [record for record in large_records if record.get("exposure") is not None],
        key=lambda record: (record["exposure"], -(record["jobs"] or 0)),
    )[0]
    largest_category = categories[0]
    large_categories = [category for category in categories if category["jobs"] >= 5_000_000]
    most_exposed_category = max(
        [
            category
            for category in (large_categories or categories)
            if category["averageExposure"] is not None
        ],
        key=lambda category: category["averageExposure"],
    )

    return {
        "repoUrl": REPO_URL,
        "sourceUrl": BLS_URL,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "occupationsCount": len(records),
        "categoryCount": len(categories),
        "totalJobs": total_jobs,
        "weightedAveragePay": weighted_average(records, "pay"),
        "weightedAverageOutlook": weighted_average(records, "outlook"),
        "weightedAverageExposure": weighted_average(records, "exposure"),
        "heroStats": [
            {
                "label": "Jobs mapped",
                "value": total_jobs,
                "format": "jobs",
                "detail": "2024 BLS employment across the entire map",
            },
            {
                "label": "Occupations",
                "value": len(records),
                "format": "count",
                "detail": "roles represented in the Occupational Outlook Handbook",
            },
            {
                "label": "Career families",
                "value": len(categories),
                "format": "count",
                "detail": "major BLS groupings rendered into one explorer",
            },
            {
                "label": "Average AI exposure",
                "value": weighted_average(records, "exposure"),
                "format": "score",
                "detail": "job-weighted exposure across the dataset",
            },
        ],
        "highlights": [
            {
                "title": "Largest employment base",
                "value": largest_category["jobs"],
                "format": "jobs",
                "context": largest_category["label"],
                "detail": f"{largest_category['shareOfJobs']}% of all jobs in the dataset",
            },
            {
                "title": "Fastest projected growth",
                "value": fastest_growth["outlook"],
                "format": "percent",
                "context": fastest_growth["title"],
                "detail": fastest_growth["category_label"],
            },
            {
                "title": "Highest annual pay",
                "value": highest_pay["pay"],
                "format": "currency",
                "context": highest_pay["title"],
                "detail": highest_pay["category_label"],
            },
            {
                "title": "Most exposed category at scale",
                "value": most_exposed_category["averageExposure"],
                "format": "score",
                "context": most_exposed_category["label"],
                "detail": "average exposure weighted by employment",
            },
        ],
        "largestOccupation": make_occupation_snapshot(largest_role),
        "fastestGrowingOccupation": make_occupation_snapshot(fastest_growth),
        "highestPayOccupation": make_occupation_snapshot(highest_pay),
        "mostExposedLargeOccupation": make_occupation_snapshot(most_exposed_large),
        "mostResilientLargeOccupation": make_occupation_snapshot(most_resilient_large),
    }


def load_records() -> list[dict]:
    with open("scores.json") as file:
        scores_list = json.load(file)
    scores = {score["slug"]: score for score in scores_list}

    with open("occupations.csv") as file:
        rows = list(csv.DictReader(file))

    records = []
    for row in rows:
        slug = row["slug"]
        score = scores.get(slug, {})
        category = row["category"]
        records.append(
            {
                "title": row["title"],
                "slug": slug,
                "category": category,
                "category_label": CATEGORY_LABELS.get(category, category.replace("-", " ").title()),
                "pay": int(row["median_pay_annual"]) if row["median_pay_annual"] else None,
                "jobs": int(row["num_jobs_2024"]) if row["num_jobs_2024"] else None,
                "outlook": int(row["outlook_pct"]) if row["outlook_pct"] else None,
                "outlook_desc": row["outlook_desc"],
                "education": row["entry_education"],
                "exposure": score.get("exposure"),
                "exposure_rationale": score.get("rationale"),
                "url": row.get("url", ""),
            }
        )

    return sorted(records, key=lambda record: record["jobs"] or 0, reverse=True)


def main() -> None:
    records = load_records()
    total_jobs = sum(record["jobs"] or 0 for record in records)
    categories = build_categories(records, total_jobs)
    summary = build_summary(records, categories)
    payload = {
        "generatedAt": None,
        "summary": summary,
        "stories": build_stories(records, categories),
        "categories": categories,
        "occupations": records,
    }

    generated_at = datetime.now(timezone.utc).isoformat()
    existing_payload = None
    existing_path = "site/data.json"
    if os.path.exists(existing_path):
        with open(existing_path) as file:
            existing_payload = json.load(file)

    def normalized(data: dict) -> dict:
        stripped = json.loads(json.dumps(data))
        stripped.pop("generatedAt", None)
        stripped.get("summary", {}).pop("generatedAt", None)
        return stripped

    if existing_payload and normalized(existing_payload) == normalized(payload):
        generated_at = existing_payload.get("generatedAt") or generated_at

    payload["generatedAt"] = generated_at
    payload["summary"]["generatedAt"] = generated_at

    os.makedirs("site", exist_ok=True)
    with open("site/data.json", "w") as file:
        json.dump(payload, file, indent=2)

    print(f"Wrote structured payload for {len(records)} occupations to site/data.json")
    print(f"Total jobs represented: {total_jobs:,}")


if __name__ == "__main__":
    main()
