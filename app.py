"""
Root-level FastAPI entry point for Render deployment.
All code is self-contained here — no cross-folder imports needed.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from collections import Counter
from typing import Optional

from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import (
    create_engine, Column, String, Float, Boolean,
    Integer, Text, DateTime, JSON, func, desc
)
from sqlalchemy.orm import declarative_base, sessionmaker

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── Database ──────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./b2b_intel.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class Job(Base):
    __tablename__ = "jobs"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    job_id      = Column(String(255), unique=True, nullable=False, index=True)
    source      = Column(String(50))
    title       = Column(String(255))
    company     = Column(String(255), index=True)
    company_url = Column(String(500))
    location    = Column(String(200))
    tags        = Column(JSON, default=list)
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
    __tablename__ = "scraper_runs"
    id           = Column(Integer, primary_key=True, autoincrement=True)
    run_at       = Column(DateTime, default=datetime.utcnow)
    jobs_found   = Column(Integer, default=0)
    jobs_new     = Column(Integer, default=0)
    jobs_updated = Column(Integer, default=0)
    status       = Column(String(20), default="success")
    error_msg    = Column(Text, nullable=True)


Base.metadata.create_all(engine)
log.info("Database tables ready")

# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI(
    title="B2B Job Intelligence API",
    description="Real-time hiring signals for B2B sales & market research",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    return SessionLocal()


def job_to_dict(j):
    return {
        "id":         j.id,
        "job_id":     j.job_id,
        "source":     j.source,
        "title":      j.title,
        "company":    j.company,
        "location":   j.location,
        "tags":       j.tags or [],
        "salary_min": j.salary_min,
        "salary_max": j.salary_max,
        "apply_url":  j.apply_url,
        "posted_at":  j.posted_at.isoformat() if j.posted_at else None,
        "scraped_at": j.scraped_at.isoformat() if j.scraped_at else None,
        "is_remote":  j.is_remote,
        "has_salary": j.has_salary,
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/")
def health():
    return {
        "status": "ok",
        "service": "B2B Job Intelligence API",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/api/stats")
def get_stats():
    db = get_db()
    try:
        total_jobs      = db.query(func.count(Job.id)).scalar() or 0
        total_companies = db.query(func.count(func.distinct(Job.company))).scalar() or 0
        remote_jobs     = db.query(func.count(Job.id)).filter(Job.is_remote == True).scalar() or 0
        with_salary     = db.query(func.count(Job.id)).filter(Job.has_salary == True).scalar() or 0
        since_yesterday = datetime.utcnow() - timedelta(hours=24)
        new_today       = db.query(func.count(Job.id)).filter(Job.scraped_at >= since_yesterday).scalar() or 0
        last_run        = db.query(ScraperRun).order_by(desc(ScraperRun.run_at)).first()
        return {
            "total_jobs":        total_jobs,
            "total_companies":   total_companies,
            "remote_jobs":       remote_jobs,
            "jobs_with_salary":  with_salary,
            "new_last_24h":      new_today,
            "last_pipeline_run": last_run.run_at.isoformat() if last_run else None,
            "last_run_status":   last_run.status if last_run else None,
        }
    finally:
        db.close()


@app.get("/api/jobs")
def get_jobs(
    page:     int           = Query(1, ge=1),
    limit:    int           = Query(20, ge=1, le=100),
    company:  Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    tag:      Optional[str] = Query(None),
    remote:   Optional[bool]= Query(None),
    source:   Optional[str] = Query(None),
):
    db = get_db()
    try:
        q = db.query(Job)
        if company:  q = q.filter(Job.company.ilike(f"%{company}%"))
        if location: q = q.filter(Job.location.ilike(f"%{location}%"))
        if remote is not None: q = q.filter(Job.is_remote == remote)
        if source:   q = q.filter(Job.source == source)
        if tag:      q = q.filter(Job.tags.contains(tag))
        total = q.count()
        jobs  = q.order_by(desc(Job.scraped_at)).offset((page-1)*limit).limit(limit).all()
        return {
            "total": total, "page": page, "limit": limit,
            "pages": (total + limit - 1) // limit,
            "jobs":  [job_to_dict(j) for j in jobs],
        }
    finally:
        db.close()


@app.get("/api/companies")
def get_companies(
    min_jobs: int = Query(1, ge=1),
    limit:    int = Query(50, ge=1, le=200),
):
    db = get_db()
    try:
        results = (
            db.query(Job.company, func.count(Job.id).label("job_count"),
                     func.max(Job.scraped_at).label("last_seen"))
            .group_by(Job.company)
            .having(func.count(Job.id) >= min_jobs)
            .order_by(desc("job_count"))
            .limit(limit).all()
        )
        companies = []
        for row in results:
            jobs = db.query(Job).filter(Job.company == row.company).all()
            all_tags = []
            for j in jobs:
                if isinstance(j.tags, list):
                    all_tags.extend(j.tags)
            top_tags = [t for t, _ in Counter(all_tags).most_common(5)]
            companies.append({
                "company":   row.company,
                "job_count": row.job_count,
                "top_tags":  top_tags,
                "last_seen": row.last_seen.isoformat() if row.last_seen else None,
                "is_hot":    row.job_count >= 3,
            })
        return {"total": len(companies), "companies": companies}
    finally:
        db.close()


@app.get("/api/trends")
def get_trends(days: int = Query(30, ge=1, le=365)):
    db = get_db()
    try:
        since = datetime.utcnow() - timedelta(days=days)
        jobs  = db.query(Job).filter(Job.scraped_at >= since).all()
        all_tags, all_titles, locations = [], [], []
        for j in jobs:
            if isinstance(j.tags, list): all_tags.extend(j.tags)
            if j.title:    all_titles.append(j.title)
            if j.location: locations.append(j.location)

        top_tags = [{"tag": t, "count": c} for t, c in Counter(all_tags).most_common(20)]
        top_locs = [{"location": l, "count": c} for l, c in Counter(locations).most_common(10)]

        ROLES = {
            "Engineering":       ["engineer", "developer", "backend", "frontend"],
            "Data / ML":         ["data", "machine learning", "ml", "ai", "analyst"],
            "DevOps / Infra":    ["devops", "sre", "cloud", "infrastructure"],
            "Product / Design":  ["product", "design", "ux", "manager"],
            "Sales / Marketing": ["sales", "marketing", "growth"],
        }
        role_counts = Counter()
        for title in all_titles:
            t = title.lower()
            for cat, kws in ROLES.items():
                if any(kw in t for kw in kws):
                    role_counts[cat] += 1
                    break

        return {
            "period_days":    days,
            "total_jobs":     len(jobs),
            "top_tags":       top_tags,
            "top_locations":  top_locs,
            "role_breakdown": [{"role": r, "count": c} for r, c in role_counts.most_common()],
        }
    finally:
        db.close()


@app.post("/api/refresh")
def refresh(background_tasks: BackgroundTasks):
    def run():
        try:
            import subprocess, sys
            subprocess.run([sys.executable, "pipeline.py"], check=True)
        except Exception as e:
            log.error(f"Pipeline failed: {e}")
    background_tasks.add_task(run)
    return {"status": "started"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)