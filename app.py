"""
Standalone FastAPI app for Render.
DB connection is lazy — only connects when a request comes in.
"""
import os, logging, sys
from datetime import datetime, timedelta
from collections import Counter
from typing import Optional

from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import (
    create_engine, Column, String, Float, Boolean,
    Integer, Text, DateTime, JSON, func, desc, event
)
from sqlalchemy.orm import declarative_base, sessionmaker

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Ensure root directory is in path for pipeline imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── Database — lazy connection ────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./b2b_intel.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Add SSL for Supabase
if "supabase" in DATABASE_URL and "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"
log.info(f"DB URL prefix: {DATABASE_URL[:30]}...")

IS_SQLITE = "sqlite" in DATABASE_URL

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if IS_SQLITE else {},
    pool_pre_ping=True,
    # Don't connect until first request
    pool_recycle=300,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

# ── Models ────────────────────────────────────────────────────────────────────
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
    id         = Column(Integer, primary_key=True, autoincrement=True)
    run_at     = Column(DateTime, default=datetime.utcnow)
    jobs_found = Column(Integer, default=0)
    jobs_new   = Column(Integer, default=0)
    status     = Column(String(20), default="success")
    error_msg  = Column(Text, nullable=True)

# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI(title="B2B Job Intelligence API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

@app.on_event("startup")
def startup():
    """Create tables on startup — actual DB connect happens here."""
    try:
        Base.metadata.create_all(engine)
        log.info("DB tables ready")
    except Exception as e:
        log.error(f"DB init error: {e}")
        # Don't crash — app still serves /health

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def job_dict(j):
    return {
        "id": j.id, "job_id": j.job_id, "source": j.source,
        "title": j.title, "company": j.company, "location": j.location,
        "tags": j.tags or [], "salary_min": j.salary_min,
        "salary_max": j.salary_max, "apply_url": j.apply_url,
        "posted_at":  j.posted_at.isoformat()  if j.posted_at  else None,
        "scraped_at": j.scraped_at.isoformat() if j.scraped_at else None,
        "is_remote": j.is_remote, "has_salary": j.has_salary,
    }

# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/")
def health():
    return {
        "status": "ok",
        "service": "B2B Job Intelligence API",
        "db": DATABASE_URL[:30] + "...",
        "time": datetime.utcnow().isoformat(),
    }

@app.get("/api/stats")
def stats():
    db = SessionLocal()
    try:
        total_jobs      = db.query(func.count(Job.id)).scalar() or 0
        total_companies = db.query(func.count(func.distinct(Job.company))).scalar() or 0
        remote_jobs     = db.query(func.count(Job.id)).filter(Job.is_remote == True).scalar() or 0
        since_24h       = datetime.utcnow() - timedelta(hours=24)
        new_today       = db.query(func.count(Job.id)).filter(Job.scraped_at >= since_24h).scalar() or 0
        last_run        = db.query(ScraperRun).order_by(desc(ScraperRun.run_at)).first()
        return {
            "total_jobs": total_jobs, "total_companies": total_companies,
            "remote_jobs": remote_jobs, "new_last_24h": new_today,
            "last_pipeline_run": last_run.run_at.isoformat() if last_run else None,
            "last_run_status":   last_run.status if last_run else None,
        }
    except Exception as e:
        log.error(f"Stats error: {e}")
        return {"error": str(e)}
    finally:
        db.close()

@app.get("/api/jobs")
def jobs(
    page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=100),
    company: Optional[str] = Query(None), location: Optional[str] = Query(None),
    tag: Optional[str] = Query(None), remote: Optional[bool] = Query(None),
    source: Optional[str] = Query(None),
):
    db = SessionLocal()
    try:
        q = db.query(Job)
        if company:  q = q.filter(Job.company.ilike(f"%{company}%"))
        if location: q = q.filter(Job.location.ilike(f"%{location}%"))
        if remote is not None: q = q.filter(Job.is_remote == remote)
        if source:   q = q.filter(Job.source == source)
        if tag:      q = q.filter(Job.tags.contains(tag))
        total = q.count()
        rows  = q.order_by(desc(Job.scraped_at)).offset((page-1)*limit).limit(limit).all()
        return {
            "total": total, "page": page, "limit": limit,
            "pages": (total+limit-1)//limit,
            "jobs":  [job_dict(j) for j in rows],
        }
    except Exception as e:
        log.error(f"Jobs error: {e}")
        return {"error": str(e), "jobs": []}
    finally:
        db.close()

@app.get("/api/companies")
def companies(min_jobs: int = Query(1, ge=1), limit: int = Query(50, ge=1, le=200)):
    db = SessionLocal()
    try:
        rows = (
            db.query(Job.company, func.count(Job.id).label("cnt"),
                     func.max(Job.scraped_at).label("last_seen"))
            .group_by(Job.company)
            .having(func.count(Job.id) >= min_jobs)
            .order_by(desc("cnt")).limit(limit).all()
        )
        out = []
        for r in rows:
            js   = db.query(Job).filter(Job.company == r.company).all()
            tags = []
            for j in js:
                if isinstance(j.tags, list): tags.extend(j.tags)
            out.append({
                "company": r.company, "job_count": r.cnt,
                "top_tags": [t for t,_ in Counter(tags).most_common(5)],
                "last_seen": r.last_seen.isoformat() if r.last_seen else None,
                "is_hot": r.cnt >= 3,
            })
        return {"total": len(out), "companies": out}
    except Exception as e:
        log.error(f"Companies error: {e}")
        return {"error": str(e), "companies": []}
    finally:
        db.close()

@app.get("/api/trends")
def trends(days: int = Query(30, ge=1, le=365)):
    db = SessionLocal()
    try:
        since = datetime.utcnow() - timedelta(days=days)
        js    = db.query(Job).filter(Job.scraped_at >= since).all()
        tags, titles, locs = [], [], []
        for j in js:
            if isinstance(j.tags, list): tags.extend(j.tags)
            if j.title:    titles.append(j.title)
            if j.location: locs.append(j.location)
        ROLES = {
            "Engineering": ["engineer","developer","backend","frontend"],
            "Data / ML":   ["data","ml","ai","analyst","scientist"],
            "DevOps":      ["devops","sre","cloud","infra"],
            "Product":     ["product","design","manager"],
            "Sales":       ["sales","marketing","growth"],
        }
        rc = Counter()
        for t in titles:
            tl = t.lower()
            for cat, kws in ROLES.items():
                if any(k in tl for k in kws): rc[cat] += 1; break
        return {
            "period_days":    days, "total_jobs": len(js),
            "top_tags":       [{"tag":t,"count":c} for t,c in Counter(tags).most_common(20)],
            "top_locations":  [{"location":l,"count":c} for l,c in Counter(locs).most_common(10)],
            "role_breakdown": [{"role":r,"count":c} for r,c in rc.most_common()],
        }
    except Exception as e:
        log.error(f"Trends error: {e}")
        return {"error": str(e)}
    finally:
        db.close()

@app.post("/api/refresh")
def refresh(bg: BackgroundTasks):
    def run():
        try:
            from pipeline import run_pipeline
            run_pipeline()
        except Exception as e:
            log.error(f"Pipeline error: {e}")
    bg.add_task(run)
    return {"status": "started"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)