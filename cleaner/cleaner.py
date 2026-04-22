"""
Data Cleaning Pipeline
Cleans raw scraped job data:
 - Removes duplicates
 - Normalises company names, locations, tags
 - Handles missing values
 - Standardises date formats
 - Documents every decision
"""

import json
import re
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
import sys
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("cleaner.log", encoding="utf-8"),
        logging.StreamHandler(stream=open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1))
    ]
)
log = logging.getLogger(__name__)

RAW_DIR     = Path("data/raw")
CLEAN_DIR   = Path("data/clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)


# ── DECISION LOG (every cleaning choice documented here) ─────────────────────
DECISIONS = {
    "duplicate_strategy":   "Keep first occurrence, drop rest by job_id",
    "missing_company":      "Fill with 'Unknown Company'",
    "missing_title":        "Fill with 'Unknown Role'",
    "missing_location":     "Fill with 'Remote' (majority of scraped jobs are remote)",
    "missing_description":  "Fill with empty string — not required for core analytics",
    "salary_missing":       "Keep as None/null — don't impute; salary varies wildly",
    "tags_empty":           "Leave as empty list — not all jobs have tags",
    "date_normalisation":   "Parse to ISO 8601 UTC. Invalid dates → scraped_at fallback",
    "company_normalisation":"Strip HTML, lowercase, title-case, remove Inc/Ltd/LLC",
    "title_normalisation":  "Strip HTML entities, truncate to 200 chars",
    "location_normalisation":"Map common abbreviations (SF, NYC, etc.) to full names",
    "description_cleaning": "Strip HTML tags, collapse whitespace, truncate to 2000 chars",
    "tags_normalisation":   "Deduplicate, sort alphabetically, max 20 tags per job",
}


# ── Text helpers ──────────────────────────────────────────────────────────────
HTML_TAG_RE    = re.compile(r"<[^>]+>")
WHITESPACE_RE  = re.compile(r"\s+")
HTML_ENTITY_RE = re.compile(r"&[a-z]+;|&#\d+;")

def strip_html(text: str) -> str:
    if not text:
        return ""
    text = HTML_TAG_RE.sub(" ", str(text))
    text = HTML_ENTITY_RE.sub(" ", text)
    return WHITESPACE_RE.sub(" ", text).strip()


COMPANY_NOISE = re.compile(
    r"\b(inc|llc|ltd|limited|corp|corporation|co|gmbh|pvt|private|public)\b\.?",
    re.IGNORECASE
)

def normalise_company(name: str) -> str:
    """Decision: strip HTML, remove legal suffixes, title-case."""
    name = strip_html(name)
    name = COMPANY_NOISE.sub("", name)
    name = re.sub(r"[^\w\s\-&.]", "", name)   # keep alphanumeric + basic punctuation
    name = WHITESPACE_RE.sub(" ", name).strip()
    return name.title() if name else "Unknown Company"


LOCATION_MAP = {
    "sf":            "San Francisco, CA",
    "nyc":           "New York, NY",
    "ny":            "New York, NY",
    "la":            "Los Angeles, CA",
    "dc":            "Washington, DC",
    "uk":            "United Kingdom",
    "us":            "United States",
    "usa":           "United States",
    "bay area":      "San Francisco, CA",
    "silicon valley":"San Francisco, CA",
}

def normalise_location(loc: str) -> str:
    """Decision: map abbreviations, title-case, default to Remote."""
    if not loc or loc.strip() == "":
        return "Remote"
    loc_clean = strip_html(loc).strip()
    lower = loc_clean.lower()
    for abbr, full in LOCATION_MAP.items():
        if lower == abbr or lower.startswith(abbr + " "):
            return full
    return loc_clean.title()[:100]


def normalise_title(title: str) -> str:
    """Decision: strip HTML, truncate to 200 chars."""
    t = strip_html(title)
    return t[:200] if t else "Unknown Role"


def normalise_description(desc: str) -> str:
    """Decision: strip HTML, collapse whitespace, truncate to 2000 chars."""
    d = strip_html(desc)
    return d[:2000] if d else ""


def normalise_tags(tags) -> list[str]:
    """Decision: deduplicate, sort, cap at 20."""
    if not tags:
        return []
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except Exception:
            tags = [t.strip() for t in tags.split(",")]
    cleaned = sorted(set(str(t).strip() for t in tags if t))
    return cleaned[:20]


def parse_date(date_str: str, fallback: str) -> str:
    """Decision: parse various formats to ISO 8601. Use fallback on failure."""
    if not date_str:
        return fallback or datetime.utcnow().isoformat()
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%a, %d %b %Y %H:%M:%S %z",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str[:25], fmt).isoformat()
        except ValueError:
            continue
    # Timestamp integer?
    try:
        return datetime.utcfromtimestamp(int(date_str)).isoformat()
    except Exception:
        pass
    log.debug(f"Could not parse date '{date_str}', using fallback")
    return fallback or datetime.utcnow().isoformat()


# ── Main cleaning logic ───────────────────────────────────────────────────────
def clean(raw_jobs: list[dict]) -> pd.DataFrame:
    log.info(f"Starting clean. Input: {len(raw_jobs)} jobs")

    df = pd.DataFrame(raw_jobs)
    before = len(df)

    # 1. Convert tags to string temporarily so pandas can hash them for dedup
    if "tags" in df.columns:
        df["tags"] = df["tags"].apply(
            lambda t: json.dumps(t) if isinstance(t, list) else (t or "[]")
        )

    # 2. Drop exact duplicates (now safe — no unhashable list columns)
    df.drop_duplicates(inplace=True)

    # 3. Deduplicate by job_id (keep first)
    if "job_id" in df.columns:
        df.drop_duplicates(subset=["job_id"], keep="first", inplace=True)
    log.info(f"Duplicates removed: {before - len(df)}")

    # 4. Parse tags back to list now
    if "tags" in df.columns:
        df["tags"] = df["tags"].apply(
            lambda t: json.loads(t) if isinstance(t, str) else []
        )

    # 3. Clean each column
    df["company"]     = df.get("company",     pd.Series()).fillna("").apply(normalise_company)
    df["title"]       = df.get("title",       pd.Series()).fillna("").apply(normalise_title)
    df["location"]    = df.get("location",    pd.Series()).fillna("").apply(normalise_location)
    df["description"] = df.get("description", pd.Series()).fillna("").apply(normalise_description)
    df["tags"]        = df.get("tags",        pd.Series()).apply(normalise_tags)
    df["apply_url"]   = df.get("apply_url",   pd.Series()).fillna("")
    df["company_url"] = df.get("company_url", pd.Series()).fillna("")
    df["source"]      = df.get("source",      pd.Series()).fillna("unknown")

    # 4. Normalise dates
    df["scraped_at"] = df.get("scraped_at", pd.Series()).fillna(datetime.utcnow().isoformat())
    df["posted_at"]  = df.apply(
        lambda r: parse_date(str(r.get("posted_at", "")), str(r.get("scraped_at", ""))),
        axis=1
    )

    # 5. Salary — keep as nullable float
    for col in ["salary_min", "salary_max"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = None

    # 6. Add derived columns useful for B2B analytics
    df["is_remote"]      = df["location"].str.lower().str.contains("remote")
    df["tag_count"]      = df["tags"].apply(len)
    df["has_salary"]     = df["salary_min"].notna() | df["salary_max"].notna()
    df["scrape_date"]    = pd.to_datetime(df["scraped_at"]).dt.date.astype(str)

    # 7. Drop rows with no useful data
    df = df[df["company"] != "Unknown Company"].copy()

    log.info(f"✅ Cleaning complete. Output: {len(df)} jobs")
    return df


def run_cleaner(input_file: str = None) -> pd.DataFrame:
    # Load latest raw file if not specified
    if input_file:
        path = Path(input_file)
    else:
        path = RAW_DIR / "jobs_latest.json"

    if not path.exists():
        log.error(f"Raw data file not found: {path}")
        return pd.DataFrame()

    with open(path) as f:
        raw = json.load(f)

    df = clean(raw)

    # Save cleaned outputs
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path  = CLEAN_DIR / f"jobs_clean_{timestamp}.csv"
    json_path = CLEAN_DIR / "jobs_clean_latest.json"

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=2, default_handler=str)

    log.info(f"Saved: {csv_path}")
    log.info(f"Saved: {json_path}")

    # Print cleaning decisions
    log.info("\n📋 CLEANING DECISIONS:")
    for k, v in DECISIONS.items():
        log.info(f"  {k}: {v}")

    return df


if __name__ == "__main__":
    run_cleaner()