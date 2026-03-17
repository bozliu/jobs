"""
Build a multi-view JSON payload for the static website.

The output powers a single-page explorer with three geography views:
- US: detailed BLS occupations
- Asia: merged regional ILOSTAT occupation groups
- Europe: merged regional ILOSTAT occupation groups

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
ILOSTAT_URL = "https://ilostat.ilo.org/data/"
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
    "international-major-groups": "International Major Groups",
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

US_PROMPT_TEXT = """You are an expert analyst evaluating how exposed different occupations are to AI. You will be given a detailed description of an occupation from the Bureau of Labor Statistics.

Rate the occupation's overall AI Exposure on a scale from 0 to 10.

AI Exposure measures: how much will AI reshape this occupation? Consider both direct effects (AI automating tasks currently done by humans) and indirect effects (AI making each worker so productive that fewer are needed).

A key signal is whether the job's work product is fundamentally digital. If the job can be done entirely from a home office on a computer — writing, coding, analyzing, communicating — then AI exposure is inherently high (7+), because AI capabilities in digital domains are advancing rapidly. Even if today's AI can't handle every aspect of such a job, the trajectory is steep and the ceiling is very high. Conversely, jobs requiring physical presence, manual skill, or real-time human interaction in the physical world have a natural barrier to AI exposure.

Use these anchors to calibrate your score:

- 0–1: Minimal exposure. The work is almost entirely physical, hands-on, or requires real-time human presence in unpredictable environments. AI has essentially no impact on daily work.
- 2–3: Low exposure. Mostly physical or interpersonal work. AI might help with minor peripheral tasks but doesn't touch the core job.
- 4–5: Moderate exposure. A mix of physical/interpersonal work and knowledge work. AI can meaningfully assist with the information-processing parts but a substantial share of the job still requires human presence.
- 6–7: High exposure. Predominantly knowledge work with some need for human judgment, relationships, or physical presence.
- 8–9: Very high exposure. The job is almost entirely done on a computer.
- 10: Maximum exposure. Routine information processing, fully digital, with no physical component.

Respond with ONLY a JSON object in this exact format:
{"exposure": <0-10>, "rationale": "<2-3 sentences explaining the key factors>"}"""


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
    snapshot = {
        "title": record["title"],
        "slug": record["slug"],
        "category": record["category"],
        "categoryLabel": record["category_label"],
        "jobs": record["jobs"],
        "pay": record.get("pay"),
        "outlook": record.get("outlook"),
        "outlookDesc": record.get("outlook_desc"),
        "education": record.get("education"),
        "exposure": record.get("exposure"),
        "url": record.get("url"),
    }
    if record.get("countryBreakdown"):
        snapshot["countryBreakdown"] = record["countryBreakdown"]
    if record.get("mappingType"):
        snapshot["mappingType"] = record["mappingType"]
    if record.get("mappingConfidence") is not None:
        snapshot["mappingConfidence"] = record["mappingConfidence"]
    if record.get("sourceYearLabel"):
        snapshot["sourceYearLabel"] = record["sourceYearLabel"]
    return snapshot


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


def build_us_summary(records: list[dict], categories: list[dict]) -> dict:
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
        [category for category in (large_categories or categories) if category["averageExposure"] is not None],
        key=lambda category: category["averageExposure"],
    )

    return {
        "repoUrl": REPO_URL,
        "sourceUrl": BLS_URL,
        "sourceLabel": "Bureau of Labor Statistics Occupational Outlook Handbook",
        "generatedAt": None,
        "occupationsCount": len(records),
        "visibleOccupationsCount": len(records),
        "categoryCount": len(categories),
        "countriesCount": 1,
        "countries": [{"name": "United States", "code": "USA", "year": 2024}],
        "year": 2024,
        "yearLabel": "2024",
        "yearStrategy": "single_year",
        "freshnessSummary": "Single BLS 2024 release",
        "methodologyNote": "US occupations are taken directly from the Occupational Outlook Handbook. Area shows employment; color shows the selected BLS or AI layer.",
        "totalJobs": total_jobs,
        "weightedAveragePay": weighted_average(records, "pay"),
        "weightedAverageOutlook": weighted_average(records, "outlook"),
        "weightedAverageExposure": weighted_average(records, "exposure"),
        "introHtml": [
            'This is a research tool that visualizes <b>342 occupations</b> from the <a href="https://www.bls.gov/ooh/">Bureau of Labor Statistics Occupational Outlook Handbook</a>, covering <b>143M jobs</b> across the US economy. Each rectangle&apos;s <b>area</b> is proportional to total employment. <b>Color</b> shows the selected metric: projected growth outlook, median pay, education requirements, or AI exposure.',
            'The <a class="repo-link" href="https://github.com/bozliu/jobs">source code</a> includes scrapers, parsers, and a prompt-driven scoring pipeline. The Digital AI Exposure layer is one example: it estimates how much current AI, which is primarily digital, may reshape each occupation.',
            'A high exposure score does <em>not</em> mean a role disappears. It only means AI is likely to materially change the throughput, workflow, or structure of the job.'
        ],
        "promptText": US_PROMPT_TEXT,
        "showPrompt": True,
        "largestOccupation": make_occupation_snapshot(largest_role),
        "fastestGrowingOccupation": make_occupation_snapshot(fastest_growth),
        "highestPayOccupation": make_occupation_snapshot(highest_pay),
        "mostExposedLargeOccupation": make_occupation_snapshot(most_exposed_large),
        "mostResilientLargeOccupation": make_occupation_snapshot(most_resilient_large),
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
    }


def load_us_records() -> list[dict]:
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


def build_us_view() -> dict:
    records = load_us_records()
    total_jobs = sum(record["jobs"] or 0 for record in records)
    categories = build_categories(records, total_jobs)
    summary = build_us_summary(records, categories)
    return {
        "id": "us",
        "label": "US",
        "heading": "US Job Market Visualizer",
        "countries": ["United States"],
        "year": 2024,
        "yearLabel": "2024",
        "yearStrategy": "single_year",
        "availableModes": ["outlook", "pay", "education", "exposure"],
        "defaultMode": "outlook",
        "summary": summary,
        "stories": build_stories(records, categories),
        "categories": categories,
        "occupations": records,
    }


def build_regional_summary(region: dict, occupations: list[dict], categories: list[dict]) -> dict:
    active_occupations = [item for item in occupations if item["jobs"] > 0] or occupations
    total_jobs = sum(item["jobs"] for item in active_occupations)
    largest = max(active_occupations, key=lambda item: item["jobs"])
    large_records = select_large(active_occupations) or active_occupations
    exposure_records = [item for item in large_records if item.get("exposure") is not None] or active_occupations
    most_exposed = max(exposure_records, key=lambda item: (item["exposure"], item["jobs"]))
    most_resilient = min(exposure_records, key=lambda item: (item["exposure"], -item["jobs"]))
    countries = region["countries"]
    country_names = ", ".join(item["name"] for item in countries)
    live_categories = [category for category in categories if category["jobs"] > 0] or categories
    largest_category = live_categories[0]
    top_country = max(countries, key=lambda item: item["jobs"])
    freshness_summary = region["freshness"]["summary"]

    if region["yearStrategy"] == "latest_shared":
        note = f"Built from the latest shared ILOSTAT occupation year across {country_names}."
    else:
        note = region["note"]

    return {
        "repoUrl": REPO_URL,
        "sourceUrl": region["source"]["url"],
        "sourceLabel": region["source"]["label"],
        "generatedAt": None,
        "occupationsCount": len(occupations),
        "visibleOccupationsCount": len(active_occupations),
        "categoryCount": len(categories),
        "countriesCount": len(countries),
        "countries": countries,
        "year": region["year"],
        "yearLabel": region["yearLabel"],
        "yearStrategy": region["yearStrategy"],
        "note": note,
        "freshness": region["freshness"],
        "freshnessSummary": freshness_summary,
        "methodologyNote": (
            "Regional employment stays official at the ILOSTAT major-group level, then gets projected into the "
            "same 342 occupation slugs as the US map using deterministic employment-weighted crosswalks."
        ),
        "totalJobs": total_jobs,
        "weightedAveragePay": None,
        "weightedAverageOutlook": None,
        "weightedAverageExposure": weighted_average(active_occupations, "exposure"),
        "introHtml": [
            f'This view merges official employment-by-occupation data from <a href="{ILOSTAT_URL}">ILOSTAT</a> for <b>{country_names}</b>, then reallocates those country totals into the same <b>342 canonical occupation labels</b> used by the US map.',
            'In these regional maps, <b>area</b> represents merged employment and <b>color</b> represents the same Digital AI Exposure score used in the US view. That creates taxonomy parity without pretending the region also has US-quality pay, education, or outlook estimates.',
            f"{note} <b>{freshness_summary}.</b>",
        ],
        "promptText": None,
        "showPrompt": False,
        "largestOccupation": make_occupation_snapshot(largest),
        "largestCategory": largest_category,
        "topCountry": {
            "name": top_country["name"],
            "code": top_country["code"],
            "jobs": top_country["jobs"],
            "sourceYear": top_country["sourceYear"],
        },
        "mostExposedLargeOccupation": make_occupation_snapshot(most_exposed),
        "mostResilientLargeOccupation": make_occupation_snapshot(most_resilient),
        "highlights": [
            {
                "title": "Visible occupations",
                "value": len(active_occupations),
                "format": "count",
                "context": "non-zero tiles in the treemap",
                "detail": f"{len(occupations)} canonical occupations remain in the payload",
            },
            {
                "title": "Largest category",
                "value": largest_category["jobs"],
                "format": "jobs",
                "context": largest_category["label"],
                "detail": f"{largest_category['shareOfJobs']}% of the mapped regional jobs",
            },
            {
                "title": "Most exposed large role",
                "value": most_exposed["exposure"],
                "format": "score",
                "context": most_exposed["title"],
                "detail": f"{format(most_exposed['jobs'], ',')} mapped regional jobs",
            },
            {
                "title": "Largest country input",
                "value": top_country["jobs"],
                "format": "jobs",
                "context": top_country["name"],
                "detail": f"source year {top_country['sourceYear']}",
            },
        ],
    }


def load_regional_views(us_records: list[dict]) -> dict[str, dict]:
    with open("regional_employment.json") as file:
        employment_payload = json.load(file)
    us_by_slug = {record["slug"]: record for record in us_records}

    views = {}
    for region_id, region in employment_payload["regions"].items():
        occupations = []
        for item in region["occupations"]:
            us_record = us_by_slug.get(item["slug"])
            if not us_record:
                continue
            occupations.append(
                {
                    "title": us_record["title"],
                    "slug": us_record["slug"],
                    "category": us_record["category"],
                    "category_label": us_record["category_label"],
                    "pay": None,
                    "jobs": item["jobs"],
                    "outlook": None,
                    "outlook_desc": "",
                    "education": None,
                    "exposure": us_record["exposure"],
                    "exposure_rationale": us_record["exposure_rationale"],
                    "countryBreakdown": item["countryBreakdown"],
                    "source": item["source"],
                    "sourceYear": item["sourceYear"],
                    "sourceYearLabel": item["sourceYearLabel"],
                    "mappingType": item["mappingType"],
                    "mappingConfidence": item["mappingConfidence"],
                    "url": "",
                }
            )
        occupations.sort(key=lambda record: record["jobs"], reverse=True)
        total_jobs = sum(record["jobs"] or 0 for record in occupations)
        categories = build_categories(occupations, total_jobs)
        summary = build_regional_summary(region, occupations, categories)
        views[region_id] = {
            "id": region_id,
            "label": region["label"],
            "heading": f"{region['label']} Job Market Visualizer",
            "countries": [item["name"] for item in region["countries"]],
            "year": region["year"],
            "yearLabel": region["yearLabel"],
            "yearStrategy": region["yearStrategy"],
            "availableModes": ["exposure"],
            "defaultMode": "exposure",
            "summary": summary,
            "stories": [],
            "categories": categories,
            "occupations": occupations,
        }

    return views


def main() -> None:
    us_view = build_us_view()
    regional_views = load_regional_views(us_view["occupations"])

    payload = {
        "generatedAt": None,
        "defaultRegion": "us",
        "views": {
            "us": us_view,
            "asia": regional_views["asia"],
            "europe": regional_views["europe"],
        },
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
        for view in stripped.get("views", {}).values():
            view.get("summary", {}).pop("generatedAt", None)
        return stripped

    if existing_payload and normalized(existing_payload) == normalized(payload):
        generated_at = existing_payload.get("generatedAt") or generated_at

    payload["generatedAt"] = generated_at
    for view in payload["views"].values():
        view["summary"]["generatedAt"] = generated_at

    os.makedirs("site", exist_ok=True)
    with open(existing_path, "w") as file:
        json.dump(payload, file, indent=2)

    total_us_jobs = payload["views"]["us"]["summary"]["totalJobs"]
    print(f"Wrote multi-view payload to {existing_path}")
    print(f"US jobs represented: {total_us_jobs:,}")


if __name__ == "__main__":
    main()
