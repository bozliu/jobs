"""
Validate regional crosswalk and employment outputs before publishing.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path


def load_us_slugs() -> set[str]:
    with open("occupations.csv") as file:
        return {row["slug"] for row in csv.DictReader(file)}


def main() -> None:
    us_slugs = load_us_slugs()
    with open("regional_source_catalog.json") as file:
        catalog = json.load(file)
    with open("regional_crosswalk.json") as file:
        crosswalk = json.load(file)
    with open("regional_employment.json") as file:
        employment = json.load(file)

    china_cfg = next(country for country in catalog["countries"] if country["code"] == "CHN")
    assert china_cfg["cachePath"] != "regional_sources/china_ilostat_2005_major_groups.csv", "China still points at the 2005 ILOSTAT fallback"
    assert Path(china_cfg["cachePath"]).exists(), f"Missing China extract {china_cfg['cachePath']}"

    for country in crosswalk["countries"]:
        assert country["sourceYear"] is not None, f"Missing sourceYear for {country['code']}"
        if country["code"] == "CHN":
            assert country["sourceYear"] >= 2023, "China sourceYear did not advance beyond the old fallback"
            assert "2005" not in (country.get("sourceYearLabel") or ""), "China source label still references 2005"
        for mapping in country["mappings"]:
            total = sum(item["weight"] for item in mapping["occupationWeights"])
            assert math.isclose(total, 1.0, rel_tol=0, abs_tol=1e-9), (
                f"Weights for {country['code']} {mapping['nativeCode']} sum to {total}"
            )
            for item in mapping["occupationWeights"]:
                assert item["slug"] in us_slugs, f"Unknown US slug {item['slug']}"

    for region_id, region in employment["regions"].items():
        occupations = region["occupations"]
        assert len(occupations) == len(us_slugs), f"{region_id} does not contain all canonical occupations"
        region_slugs = {item["slug"] for item in occupations}
        assert region_slugs == us_slugs, f"{region_id} slug set does not match US canonical taxonomy"
        for country in region["countries"]:
            assert country["sourceYear"] is not None, f"Missing sourceYear for {region_id}/{country['code']}"
        for item in occupations:
            assert "mappingType" in item and item["mappingType"], f"Missing mappingType for {item['slug']}"
            assert "mappingConfidence" in item, f"Missing mappingConfidence for {item['slug']}"
            assert "sourceYearLabel" in item, f"Missing sourceYearLabel for {item['slug']}"
            if item["jobs"] > 0:
                assert item["countryBreakdown"], f"Missing countryBreakdown for populated occupation {item['slug']}"

    print("Regional data validation passed.")


if __name__ == "__main__":
    main()
