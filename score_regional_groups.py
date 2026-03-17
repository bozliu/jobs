"""
Score broad international occupation groups for AI exposure using an LLM.

This mirrors the occupation-level scoring pipeline but works on the canonical
regional occupation groups used for Asia and Europe views.

Usage:
    uv run python score_regional_groups.py
"""

from __future__ import annotations

import argparse
import json
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

DEFAULT_MODEL = "google/gemini-3-flash-preview"
OUTPUT_FILE = "regional_scores.json"
API_URL = "https://openrouter.ai/api/v1/chat/completions"

SYSTEM_PROMPT = """\
You are an expert analyst evaluating how exposed different occupation groups are to AI.
You will be given the title and definition of a broad occupation group from the
international ISCO classification.

Rate the occupation group's overall AI Exposure on a scale from 0 to 10.

AI Exposure measures: how much will AI reshape this occupation group? Consider
both direct effects (AI automating tasks currently done by humans) and indirect
effects (AI making each worker more productive so fewer workers are needed).

A key signal is whether the work product is fundamentally digital. Occupation
groups centered on documents, screens, analysis, communication, coding, or
routine information processing should score high. Groups centered on physical
presence, dexterity, outdoor work, or real-time human interaction should score
lower.

Respond with ONLY a JSON object in this exact format:
{
  "exposure": <0-10>,
  "rationale": "<2-3 sentences explaining the key factors>"
}
"""


def score_group(client: httpx.Client, model: str, group: dict) -> dict:
    response = client.post(
        API_URL,
        headers={"Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": f"Title: {group['title']}\nDefinition: {group['description']}",
                },
            ],
            "temperature": 0.2,
        },
        timeout=60,
    )
    response.raise_for_status()
    content = response.json()["choices"][0]["message"]["content"].strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()
    return json.loads(content)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=DEFAULT_MODEL)
    args = parser.parse_args()

    with open("regional_occupations.json") as file:
        groups = json.load(file)

    client = httpx.Client()
    results = []
    try:
        for group in groups:
            result = score_group(client, args.model, group)
            results.append(
                {
                    "slug": group["slug"],
                    "title": group["title"],
                    **result,
                }
            )
    finally:
        client.close()

    with open(OUTPUT_FILE, "w") as file:
        json.dump(results, file, indent=2)

    print(f"Wrote {len(results)} regional exposure scores to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
