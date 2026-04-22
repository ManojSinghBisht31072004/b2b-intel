"""
FastAPI Backend — B2B Job Intelligence API
Endpoints:
  GET /                    → health check
  GET /api/jobs            → paginated job listings with filters
  GET /api/companies       → company hiring signals
  GET /api/trends          → tech stack & role trends
  GET /api/stats           → dashboard summary stats
  POST /api/refresh        → trigger pipeline manually
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter
from typing import Optional

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import func, desc, text

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import init_db, get_session, Job, ScraperRun

log = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="B2B Job Intelligence API",
    description="Real-time hiring signals for B2B sales, recruiting & market research",
    version="1.0.0",
    docs_url="/docs",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()
    log.info("Database initialised")


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/")
def health():
    return {
        "status": "ok",
        "service": "B2B Job Intelligence API",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
    }


# ── Jobs endpoint ─────────────────────────────────────────────────────────────
@app.get("/api/jobs")
def get_jobs(
    page:     int      = Query(1,   ge=1,   description="Page number"),
    limit:    int      = Query(20,  ge=1, le=100, description="Jobs per page"),
    company:  Optional[str] = Query(None, description="Filter by company name"),
    location: Optional[str] = Query(None, description="Filter by location"),
    tag:      Optional[str] = Query(None, description="Filter by tech tag"),
    remote:   Optional[bool]= Query(None, description="Filter remote-only"),
    source:   Optional[str] = Query(None, description="Filter by source (remoteok/hn_hiring)"),
):
    """
    Paginated job listings with optional filters.
    Business use: Sales teams finding target companies actively hiring.
    """
    db = get_session()
    try:
        q = db.query(Job)

        if company:
            q = q.filter(Job.company.ilike(f"%{company}%"))
        if location:
            q = q.filter(Job.location.ilike(f"%{location}%"))
        if remote is not None:
            q = q.filter(Job.is_remote == remote)
        if source:
            q = q.filter(Job.source == source)
        if tag:
            # JSON contains check — works on SQLite & Postgres
            q = q.filter(Job.tags.contains(tag))

        total = q.count()
        jobs  = q.order_by(desc(Job.scraped_at)).offset((page-1)*limit).limit(limit).all()

        return {
            "total": total,
            "page":  page,
            "limit": limit,
            "pages": (total + limit - 1) // limit,
            "jobs":  [_job_to_dict(j) for j in jobs],
        }
    finally:
        db.close()


# ── Companies endpoint ────────────────────────────────────────────────────────
@app.get("/api/companies")
def get_companies(
    min_jobs: int = Query(1, ge=1, description="Minimum job postings"),
    limit:    int = Query(50, ge=1, le=200),
):
    """
    Company hiring signals — who is hiring the most.
    Business use: Lead generation, investor signals, competitive intelligence.
    """
    db = get_session()
    try:
        results = (
            db.query(
                Job.company,
                func.count(Job.id).label("job_count"),
                func.max(Job.scraped_at).label("last_seen"),
                func.min(Job.posted_at).label("first_seen"),
            )
            .group_by(Job.company)
            .having(func.count(Job.id) >= min_jobs)
            .order_by(desc("job_count"))
            .limit(limit)
            .all()
        )

        companies = []
        for row in results:
            # Get all tags for this company
            jobs = db.query(Job).filter(Job.company == row.company).all()
            all_tags = []
            for j in jobs:
                if isinstance(j.tags, list):
                    all_tags.extend(j.tags)
            top_tags = [t for t, _ in Counter(all_tags).most_common(5)]

            companies.append({
                "company":    row.company,
                "job_count":  row.job_count,
                "top_tags":   top_tags,
                "last_seen":  row.last_seen.isoformat() if row.last_seen else None,
                "first_seen": row.first_seen.isoformat() if row.first_seen else None,
                "is_hot":     row.job_count >= 3,
            })

        return {"total": len(companies), "companies": companies}
    finally:
        db.close()


# ── Trends endpoint ───────────────────────────────────────────────────────────
@app.get("/api/trends")
def get_trends(days: int = Query(30, ge=1, le=365)):
    """
    Tech stack & role trends from the last N days.
    Business use: Market research, training providers, tool vendors.
    """
    db = get_session()
    try:
        since = datetime.utcnow() - timedelta(days=days)
        jobs  = db.query(Job).filter(Job.scraped_at >= since).all()

        all_tags   = []
        all_titles = []
        locations  = []

        for j in jobs:
            if isinstance(j.tags, list):
                all_tags.extend(j.tags)
            if j.title:
                all_titles.append(j.title)
            if j.location:
                locations.append(j.location)

        # Top tech tags
        tag_counts = Counter(all_tags)
        top_tags   = [{"tag": t, "count": c} for t, c in tag_counts.most_common(20)]

        # Top locations
        loc_counts  = Counter(locations)
        top_locs    = [{"location": l, "count": c} for l, c in loc_counts.most_common(10)]

        # Role categories (simple keyword bucketing)
        ROLE_KEYWORDS = {
            "Engineering":        ["engineer", "developer", "programmer", "swe", "backend", "frontend"],
            "Data / ML":          ["data", "machine learning", "ml", "ai", "analyst", "scientist"],
            "DevOps / Infra":     ["devops", "sre", "infrastructure", "cloud", "platform"],
            "Product / Design":   ["product", "design", "ux", "ui", "manager"],
            "Sales / Marketing":  ["sales", "marketing", "growth", "revenue"],
        }
        role_counts = Counter()
        for title in all_titles:
            t_lower = title.lower()
            for cat, kws in ROLE_KEYWORDS.items():
                if any(kw in t_lower for kw in kws):
                    role_counts[cat] += 1
                    break

        top_roles = [{"role": r, "count": c} for r, c in role_counts.most_common()]

        return {
            "period_days":  days,
            "total_jobs":   len(jobs),
            "top_tags":     top_tags,
            "top_locations":top_locs,
            "role_breakdown":top_roles,
        }
    finally:
        db.close()


# ── Stats endpoint ────────────────────────────────────────────────────────────
@app.get("/api/stats")
def get_stats():
    """Summary statistics for the dashboard."""
    db = get_session()
    try:
        total_jobs     = db.query(func.count(Job.id)).scalar()
        total_companies= db.query(func.count(func.distinct(Job.company))).scalar()
        remote_jobs    = db.query(func.count(Job.id)).filter(Job.is_remote == True).scalar()
        with_salary    = db.query(func.count(Job.id)).filter(Job.has_salary == True).scalar()

        last_run = db.query(ScraperRun).order_by(desc(ScraperRun.run_at)).first()

        # Jobs added in last 24 hrs
        since_yesterday = datetime.utcnow() - timedelta(hours=24)
        new_today = db.query(func.count(Job.id)).filter(
            Job.scraped_at >= since_yesterday
        ).scalar()

        return {
            "total_jobs":       total_jobs,
            "total_companies":  total_companies,
            "remote_jobs":      remote_jobs,
            "jobs_with_salary": with_salary,
            "new_last_24h":     new_today,
            "last_pipeline_run":last_run.run_at.isoformat() if last_run else None,
            "last_run_status":  last_run.status if last_run else None,
        }
    finally:
        db.close()


# ── Manual trigger ────────────────────────────────────────────────────────────
@app.post("/api/refresh")
def refresh_pipeline(background_tasks: BackgroundTasks):
    """Trigger the pipeline manually (runs in background)."""
    def run():
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from pipeline import run_pipeline
            run_pipeline()
        except Exception as e:
            log.error(f"Manual pipeline run failed: {e}")

    background_tasks.add_task(run)
    return {"status": "started", "message": "Pipeline running in background"}


# ── Helpers ───────────────────────────────────────────────────────────────────
def _job_to_dict(j: Job) -> dict:
    return {
        "id":          j.id,
        "job_id":      j.job_id,
        "source":      j.source,
        "title":       j.title,
        "company":     j.company,
        "company_url": j.company_url,
        "location":    j.location,
        "tags":        j.tags or [],
        "salary_min":  j.salary_min,
        "salary_max":  j.salary_max,
        "apply_url":   j.apply_url,
        "posted_at":   j.posted_at.isoformat() if j.posted_at else None,
        "scraped_at":  j.scraped_at.isoformat() if j.scraped_at else None,
        "is_remote":   j.is_remote,
        "has_salary":  j.has_salary,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
