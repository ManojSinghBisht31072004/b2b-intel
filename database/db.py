"""
Database layer — SQLite (local dev) or Supabase PostgreSQL (production).
Uses SQLAlchemy ORM for compatibility with both.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import (
    create_engine, Column, String, Float, Boolean,
    Integer, Text, DateTime, JSON, text
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session

log = logging.getLogger(__name__)

# ── Engine ────────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{Path(__file__).parent.parent}/data/b2b_intel.db"
)

# Supabase uses postgres:// but SQLAlchemy needs postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
    echo=False,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


# ── Models ────────────────────────────────────────────────────────────────────
class Job(Base):
    __tablename__ = "jobs"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    job_id      = Column(String(255), unique=True, nullable=False, index=True)
    source      = Column(String(50),  index=True)
    title       = Column(String(255))
    company     = Column(String(255), index=True)
    company_url = Column(String(500))
    location    = Column(String(200), index=True)
    tags        = Column(JSON,  default=list)
    salary_min  = Column(Float, nullable=True)
    salary_max  = Column(Float, nullable=True)
    description = Column(Text)
    apply_url   = Column(String(500))
    posted_at   = Column(DateTime, nullable=True)
    scraped_at  = Column(DateTime, default=datetime.utcnow)
    is_remote   = Column(Boolean, default=True)
    tag_count   = Column(Integer, default=0)
    has_salary  = Column(Boolean, default=False)
    scrape_date = Column(String(20))


class ScraperRun(Base):
    """Audit log for every pipeline run."""
    __tablename__ = "scraper_runs"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    run_at      = Column(DateTime, default=datetime.utcnow)
    jobs_found  = Column(Integer, default=0)
    jobs_new    = Column(Integer, default=0)
    jobs_updated= Column(Integer, default=0)
    status      = Column(String(20), default="success")
    error_msg   = Column(Text, nullable=True)


# ── Init ──────────────────────────────────────────────────────────────────────
def init_db():
    """Create all tables if they don't exist."""
    Base.metadata.create_all(engine)
    log.info("✅ Database tables created/verified")


# ── CRUD ──────────────────────────────────────────────────────────────────────
def upsert_jobs(df, session: Session) -> tuple[int, int]:
    """
    Insert new jobs, skip existing ones (by job_id).
    Returns (new_count, skipped_count).
    """
    new_count = 0
    skipped   = 0

    for _, row in df.iterrows():
        existing = session.query(Job).filter_by(job_id=str(row["job_id"])).first()
        if existing:
            skipped += 1
            continue

        try:
            tags = row.get("tags", [])
            if isinstance(tags, str):
                tags = json.loads(tags) if tags.startswith("[") else []

            job = Job(
                job_id      = str(row["job_id"]),
                source      = str(row.get("source", "")),
                title       = str(row.get("title", "")),
                company     = str(row.get("company", "")),
                company_url = str(row.get("company_url", "")),
                location    = str(row.get("location", "")),
                tags        = tags,
                salary_min  = row.get("salary_min") or None,
                salary_max  = row.get("salary_max") or None,
                description = str(row.get("description", "")),
                apply_url   = str(row.get("apply_url", "")),
                posted_at   = _safe_date(row.get("posted_at")),
                scraped_at  = _safe_date(row.get("scraped_at")),
                is_remote   = bool(row.get("is_remote", True)),
                tag_count   = int(row.get("tag_count", 0)),
                has_salary  = bool(row.get("has_salary", False)),
                scrape_date = str(row.get("scrape_date", "")),
            )
            session.add(job)
            new_count += 1
        except Exception as e:
            log.warning(f"Failed to insert job {row.get('job_id')}: {e}")
            session.rollback()
            continue

    session.commit()
    return new_count, skipped


def log_run(session: Session, jobs_found=0, jobs_new=0,
            jobs_updated=0, status="success", error_msg=None):
    run = ScraperRun(
        jobs_found=jobs_found,
        jobs_new=jobs_new,
        jobs_updated=jobs_updated,
        status=status,
        error_msg=error_msg,
    )
    session.add(run)
    session.commit()


def _safe_date(val) -> datetime | None:
    if not val or str(val) in ("nan", "None", ""):
        return None
    try:
        return datetime.fromisoformat(str(val)[:19])
    except Exception:
        return None


def get_session() -> Session:
    return SessionLocal()
