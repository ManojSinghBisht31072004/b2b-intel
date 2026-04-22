"""
B2B Job Intelligence Scraper
Scrapes RemoteOK public API for job postings to generate company hiring signals.
Handles pagination, missing fields, and failures gracefully.
"""

import requests
import json
import time
import logging
from datetime import datetime
from pathlib import Path

# ── Logging setup ────────────────────────────────────────────────────────────
import sys
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", encoding="utf-8"),
        logging.StreamHandler(stream=open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1))
    ]
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
REMOTEOK_API   = "https://remoteok.com/api"
HN_WHOISHIRING = "https://hacker-news.firebaseio.com/v0/item/{}.json"
HN_ALGOLIA     = "https://hn.algolia.com/api/v1/search?query=who+is+hiring&tags=story&hitsPerPage=1"

HEADERS = {
    "User-Agent": "B2B-Intel-Bot/1.0 (Data Engineering Assignment)"
}
OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


# ── Helpers ──────────────────────────────────────────────────────────────────
def safe_get(url: str, retries: int = MAX_RETRIES) -> dict | list | None:
    """HTTP GET with retry + graceful failure."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            log.warning(f"HTTP error on attempt {attempt}: {e}")
        except requests.exceptions.ConnectionError as e:
            log.warning(f"Connection error on attempt {attempt}: {e}")
        except requests.exceptions.Timeout:
            log.warning(f"Timeout on attempt {attempt}")
        except json.JSONDecodeError as e:
            log.warning(f"JSON decode error: {e}")
            return None

        if attempt < retries:
            log.info(f"Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    log.error(f"All {retries} attempts failed for {url}")
    return None


def extract_field(obj: dict, *keys, default=""):
    """Safely extract nested fields with a default."""
    for key in keys:
        val = obj.get(key, default)
        if val and val != "false" and val is not False:
            return str(val).strip()
    return default


# ── Source 1: RemoteOK API ────────────────────────────────────────────────────
def scrape_remoteok() -> list[dict]:
    """
    Scrape RemoteOK public JSON API.
    First item is always a legal notice — we skip it.
    Handles pagination via built-in full dataset return.
    """
    log.info("Scraping RemoteOK API...")
    data = safe_get(REMOTEOK_API)

    if not data or not isinstance(data, list):
        log.error("RemoteOK returned empty/invalid data")
        return []

    jobs = data[1:]  # skip legal notice at index 0
    log.info(f"RemoteOK: raw {len(jobs)} jobs fetched")

    results = []
    for job in jobs:
        if not isinstance(job, dict):
            continue

        results.append({
            "source":       "remoteok",
            "job_id":       extract_field(job, "id", "slug"),
            "title":        extract_field(job, "position"),
            "company":      extract_field(job, "company"),
            "company_url":  extract_field(job, "company_logo_url", "url"),
            "location":     extract_field(job, "location", default="Remote"),
            "tags":         job.get("tags", []),
            "salary_min":   job.get("salary_min", None),
            "salary_max":   job.get("salary_max", None),
            "description":  extract_field(job, "description"),
            "apply_url":    extract_field(job, "apply_url", "url"),
            "posted_at":    extract_field(job, "date"),
            "scraped_at":   datetime.utcnow().isoformat(),
        })

    log.info(f"RemoteOK: {len(results)} valid jobs extracted")
    return results


# ── Source 2: HN Who Is Hiring (Algolia API) ─────────────────────────────────
def scrape_hn_hiring(max_comments: int = 200) -> list[dict]:
    """
    Scrape the latest Hacker News 'Who Is Hiring' thread via public APIs.
    Uses Algolia search to find the latest thread, then Firebase HN API for comments.
    max_comments: how many top comments (job postings) to fetch.
    """
    log.info("Scraping HN Who Is Hiring...")

    # Step 1: Find latest 'Who Is Hiring' story
    search = safe_get(HN_ALGOLIA)
    if not search or not search.get("hits"):
        log.error("Could not find HN Who Is Hiring thread")
        return []

    story_id = search["hits"][0]["objectID"]
    log.info(f"HN thread found: story id={story_id}")

    # Step 2: Get story metadata + top comment IDs
    story = safe_get(HN_WHOISHIRING.format(story_id))
    if not story:
        return []

    comment_ids = story.get("kids", [])[:max_comments]
    log.info(f"HN: fetching {len(comment_ids)} comments...")

    results = []
    for cid in comment_ids:
        comment = safe_get(HN_WHOISHIRING.format(cid))
        if not comment or comment.get("deleted") or comment.get("dead"):
            continue

        text = comment.get("text", "")
        if not text or len(text) < 50:
            continue

        results.append({
            "source":      "hn_hiring",
            "job_id":      str(cid),
            "title":       _parse_hn_title(text),
            "company":     _parse_hn_company(text),
            "company_url": "",
            "location":    _parse_hn_location(text),
            "tags":        _parse_hn_tags(text),
            "salary_min":  None,
            "salary_max":  None,
            "description": text,
            "apply_url":   f"https://news.ycombinator.com/item?id={cid}",
            "posted_at":   datetime.utcfromtimestamp(
                               comment.get("time", 0)).isoformat(),
            "scraped_at":  datetime.utcnow().isoformat(),
        })

        time.sleep(0.1)  # be polite to HN API

    log.info(f"HN: {len(results)} job postings extracted")
    return results


def _parse_hn_title(text: str) -> str:
    """Extract job title from HN posting (first line usually has it)."""
    first = text.split("<p>")[0].replace("<b>", "").replace("</b>", "")
    return first[:120].strip() if first else "Unknown Role"


def _parse_hn_company(text: str) -> str:
    """Extract company name — typically the bold first word."""
    import re
    match = re.search(r"<b>([^<]+)</b>", text)
    return match.group(1).strip() if match else "Unknown Company"


def _parse_hn_location(text: str) -> str:
    """Try to extract location keywords from HN post."""
    import re
    loc_patterns = [
        r"(Remote|On-?site|Hybrid|New York|San Francisco|London|Berlin|"
        r"Toronto|Singapore|Bangalore|Mumbai|Delhi|Hyderabad)[^<,\n]{0,30}"
    ]
    for pat in loc_patterns:
        match = re.search(pat, text, re.IGNORECASE)
        if match:
            return match.group(0).strip()
    return "Remote"


def _parse_hn_tags(text: str) -> list[str]:
    """Extract tech keywords mentioned in job post."""
    TECH_KEYWORDS = [
        "Python", "JavaScript", "TypeScript", "React", "Node.js", "Go",
        "Rust", "Java", "Kotlin", "Swift", "PostgreSQL", "MySQL", "MongoDB",
        "Redis", "Kafka", "AWS", "GCP", "Azure", "Docker", "Kubernetes",
        "Machine Learning", "AI", "LLM", "Data Engineering", "FastAPI",
        "Django", "Flask", "Next.js", "GraphQL", "REST", "Terraform",
        "Spark", "dbt", "Airflow", "Snowflake", "BigQuery"
    ]
    found = []
    text_lower = text.lower()
    for kw in TECH_KEYWORDS:
        if kw.lower() in text_lower:
            found.append(kw)
    return found


# ── Main ──────────────────────────────────────────────────────────────────────
def run_scraper() -> list[dict]:
    all_jobs = []

    # Source 1: RemoteOK
    remoteok_jobs = scrape_remoteok()
    all_jobs.extend(remoteok_jobs)

    # Source 2: HN Who Is Hiring
    hn_jobs = scrape_hn_hiring(max_comments=150)
    all_jobs.extend(hn_jobs)

    # Save raw output
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_file = OUTPUT_DIR / f"jobs_raw_{timestamp}.json"
    with open(out_file, "w") as f:
        json.dump(all_jobs, f, indent=2, default=str)

    # Also save as 'latest' for the pipeline
    latest_file = OUTPUT_DIR / "jobs_latest.json"
    with open(latest_file, "w") as f:
        json.dump(all_jobs, f, indent=2, default=str)

    log.info(f"✅ Scraping complete. Total jobs: {len(all_jobs)}")
    log.info(f"   Saved to: {out_file}")
    return all_jobs


if __name__ == "__main__":
    run_scraper()