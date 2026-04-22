"""
Pipeline Orchestrator
Runs: Scrape → Clean → Store in one command.
Called by GitHub Actions cron or manually.
"""

import sys
import logging
from pathlib import Path

# Make sure sibling packages are importable
sys.path.insert(0, str(Path(__file__).parent))

from scraper.scraper import run_scraper
from cleaner.cleaner import run_cleaner
from database.db import init_db, upsert_jobs, log_run, get_session

import sys
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(stream=open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1))
    ]
)
log = logging.getLogger(__name__)


def run_pipeline():
    log.info("=" * 60)
    log.info("🚀 B2B Intel Pipeline Starting")
    log.info("=" * 60)

    session = get_session()
    status = "success"
    error_msg = None
    jobs_new = 0

    try:
        # Step 1: Scrape
        log.info("\n📡 STEP 1: Scraping...")
        raw_jobs = run_scraper()
        log.info(f"   → {len(raw_jobs)} raw jobs collected")

        # Step 2: Clean
        log.info("\n🧹 STEP 2: Cleaning...")
        df = run_cleaner()
        log.info(f"   → {len(df)} clean jobs ready")

        if df.empty:
            log.warning("No data to store after cleaning.")
            return

        # Step 3: Store
        log.info("\n💾 STEP 3: Storing in database...")
        init_db()
        jobs_new, skipped = upsert_jobs(df, session)
        log.info(f"   → {jobs_new} new jobs inserted, {skipped} duplicates skipped")

    except Exception as e:
        status = "error"
        error_msg = str(e)
        log.error(f"Pipeline failed: {e}", exc_info=True)

    finally:
        log_run(
            session,
            jobs_found=len(raw_jobs) if "raw_jobs" in locals() else 0,
            jobs_new=jobs_new,
            status=status,
            error_msg=error_msg,
        )
        session.close()

    log.info("\n" + "=" * 60)
    log.info(f"✅ Pipeline complete. Status: {status}")
    log.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()