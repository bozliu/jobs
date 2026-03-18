"""
Helpers for mapping the US canonical occupation taxonomy into broad
international occupation families used for regional crosswalk allocation.
"""

from __future__ import annotations

from collections import defaultdict

FAMILY_DEFINITIONS = {
    "0": "Armed forces occupations",
    "1": "Managers",
    "2": "Professionals",
    "3": "Technicians and associate professionals",
    "4": "Clerical support workers",
    "5": "Service and sales workers",
    "6": "Skilled agricultural, forestry and fishery workers",
    "7": "Craft and related trades workers",
    "8": "Plant and machine operators, and assemblers",
    "9": "Elementary occupations",
    "CN01": "Unit leaders and managers",
    "CN02": "Professional and technical personnel",
    "CN04": "Clerical and related personnel",
    "CN05": "Social production and life service personnel",
    "CN06": "Agricultural production and auxiliary personnel",
    "CN78": "Production, manufacturing, operators, and related personnel",
    "CN99": "Other employed persons",
}


def _contains(text: str, phrases: tuple[str, ...]) -> bool:
    return any(phrase in text for phrase in phrases)


def assign_occupation_family(row: dict) -> dict:
    title = row["title"].lower()
    slug = row["slug"]
    category = row["category"]

    if category == "military":
        return {"familyCode": "0", "confidence": 0.98, "reason": "military category"}

    if "manager" in title or "managers" in title or category == "management":
        return {"familyCode": "1", "confidence": 0.95, "reason": "managerial title or category"}

    if category == "office-and-administrative-support":
        return {"familyCode": "4", "confidence": 0.92, "reason": "office and administrative support"}

    if category == "building-and-grounds-cleaning":
        return {"familyCode": "9", "confidence": 0.9, "reason": "elementary cleaning and grounds work"}

    if category == "farming-fishing-and-forestry":
        return {"familyCode": "6", "confidence": 0.92, "reason": "agriculture, forestry, or fishing"}

    if category == "food-preparation-and-serving":
        return {"familyCode": "5", "confidence": 0.9, "reason": "service occupation"}

    if category == "sales":
        return {"familyCode": "5", "confidence": 0.9, "reason": "sales occupation"}

    if category == "personal-care-and-service":
        return {"familyCode": "5", "confidence": 0.9, "reason": "personal service occupation"}

    if category == "protective-service":
        return {"familyCode": "5", "confidence": 0.83, "reason": "protective services align to service workers"}

    if category == "construction-and-extraction":
        return {"familyCode": "7", "confidence": 0.91, "reason": "construction and extraction trade work"}

    if category == "installation-maintenance-and-repair":
        if _contains(title, ("calibration", "telecommunications technician")):
            return {"familyCode": "3", "confidence": 0.78, "reason": "technical instrumentation role"}
        return {"familyCode": "7", "confidence": 0.88, "reason": "installation, maintenance, and repair trade work"}

    if category == "production":
        if _contains(
            title,
            (
                "assemblers",
                "machine",
                "operators",
                "boiler",
                "power plant",
                "wastewater",
                "water treatment",
                "quality control inspector",
                "semiconductor processing",
            ),
        ):
            return {"familyCode": "8", "confidence": 0.84, "reason": "operator or machine-oriented production role"}
        return {"familyCode": "7", "confidence": 0.8, "reason": "craft-oriented production role"}

    if category == "transportation-and-material-moving":
        if _contains(title, ("flight attendants",)):
            return {"familyCode": "5", "confidence": 0.85, "reason": "transport service role"}
        if _contains(title, ("air traffic controllers", "pilots")):
            return {"familyCode": "3", "confidence": 0.84, "reason": "specialized transport control role"}
        if _contains(title, ("hand laborers", "material movers")):
            return {"familyCode": "9", "confidence": 0.86, "reason": "elementary moving labor"}
        return {"familyCode": "8", "confidence": 0.84, "reason": "transport operator or driver"}

    if category == "healthcare":
        if _contains(title, ("assistant", "aide", "home health", "personal care", "massage therapist")):
            return {"familyCode": "5", "confidence": 0.83, "reason": "care support or personal service role"}
        if _contains(
            title,
            (
                "technologist",
                "technician",
                "hygienist",
                "phlebotomist",
                "medical records",
                "medical transcription",
                "health information",
                "licensed practical",
                "licensed vocational",
                "optician",
            ),
        ):
            return {"familyCode": "3", "confidence": 0.82, "reason": "health technician or associate role"}
        return {"familyCode": "2", "confidence": 0.86, "reason": "health professional role"}

    if category == "education-training-and-library":
        if _contains(title, ("teacher assistants", "library technicians", "tutors")):
            return {"familyCode": "3", "confidence": 0.79, "reason": "education support role"}
        if _contains(title, ("administrators", "principals")):
            return {"familyCode": "1", "confidence": 0.9, "reason": "education management role"}
        return {"familyCode": "2", "confidence": 0.86, "reason": "education professional role"}

    if category == "business-and-financial":
        if _contains(title, ("purchasing managers",)):
            return {"familyCode": "1", "confidence": 0.93, "reason": "managerial business role"}
        if _contains(title, ("sales agents",)):
            return {"familyCode": "5", "confidence": 0.7, "reason": "sales-oriented business role"}
        return {"familyCode": "2", "confidence": 0.82, "reason": "business or finance professional role"}

    if category == "arts-and-design":
        if _contains(title, ("photographers",)):
            return {"familyCode": "3", "confidence": 0.67, "reason": "practical visual-media role"}
        return {"familyCode": "2", "confidence": 0.78, "reason": "creative professional role"}

    if category == "entertainment-and-sports":
        if _contains(title, ("producers and directors", "music directors and composers")):
            return {"familyCode": "2", "confidence": 0.72, "reason": "creative direction role"}
        return {"familyCode": "3", "confidence": 0.68, "reason": "performing, coaching, or sports role"}

    if category == "media-and-communication":
        if _contains(title, ("technicians",)):
            return {"familyCode": "3", "confidence": 0.78, "reason": "broadcast or production technician"}
        return {"familyCode": "2", "confidence": 0.82, "reason": "media or communications professional"}

    if category in {
        "architecture-and-engineering",
        "community-and-social-service",
        "computer-and-information-technology",
        "legal",
        "life-physical-and-social-science",
        "math",
    }:
        return {"familyCode": "2", "confidence": 0.9, "reason": f"{category} defaults to professional work"}

    return {"familyCode": "2", "confidence": 0.6, "reason": "fallback professional mapping"}


def build_us_family_weights(rows: list[dict]) -> tuple[dict[str, list[dict]], dict[str, dict]]:
    family_members: dict[str, list[dict]] = defaultdict(list)
    assignments: dict[str, dict] = {}

    for row in rows:
        assignment = assign_occupation_family(row)
        assignments[row["slug"]] = assignment
        family_members[assignment["familyCode"]].append(row)

    family_weights: dict[str, list[dict]] = {}
    for family_code, members in family_members.items():
        total_jobs = sum(int(member["num_jobs_2024"] or 0) for member in members)
        size = len(members)
        weights = []
        for member in sorted(members, key=lambda item: item["slug"]):
            jobs = int(member["num_jobs_2024"] or 0)
            weight = (jobs / total_jobs) if total_jobs > 0 else (1 / size if size else 0)
            weights.append(
                {
                    "slug": member["slug"],
                    "title": member["title"],
                    "weight": weight,
                    "assignmentConfidence": assignments[member["slug"]]["confidence"],
                    "assignmentReason": assignments[member["slug"]]["reason"],
                }
            )
        family_weights[family_code] = weights

    return family_weights, assignments
