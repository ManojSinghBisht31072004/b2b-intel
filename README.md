# 📡 B2B Intel — Hiring Intelligence Platform

> **Data Engineering Intern Assignment**
> Automatically collect, clean, and surface B2B hiring signals from public job boards.

---

## 🧠 The Problem Being Solved

Businesses — especially sales teams, recruiters, and investors — need to know:
- **Which companies are actively hiring** (growth signal)
- **What tech stacks companies use** (tool/service targeting)
- **Which roles are in demand** (market trends)

This data is publicly available but scattered across dozens of job boards. Manually tracking it is impossible at scale.

**B2B Intel** solves this by:
1. Automatically scraping public job postings (RemoteOK + HN Who Is Hiring)
2. Cleaning and storing them in a structured database
3. Exposing a live dashboard + REST API that a business user can actually use

Think of it as a mini version of Apollo.io or Bombora — built in a weekend.

---

## 🏗️ Architecture

```
[RemoteOK API] ──┐
                  ├──► [Scraper] ──► [Cleaner] ──► [SQLite/Postgres DB]
[HN Hiring API] ─┘                                         │
                                                            ▼
                                               [FastAPI REST API]
                                                            │
                                                            ▼
                                               [Live HTML Dashboard]

[GitHub Actions] ──► runs entire pipeline daily at 06:00 UTC automatically
```

---

## 📁 Project Structure

```
b2b-intel/
├── scraper/
│   └── scraper.py          # Scrapes RemoteOK + HN Who Is Hiring
├── cleaner/
│   └── cleaner.py          # Cleans raw data, documents decisions
├── database/
│   └── db.py               # SQLAlchemy models + CRUD helpers
├── api/
│   ├── main.py             # FastAPI app with all endpoints
│   └── ai_layer.py         # AI bonus: company intelligence summaries
├── frontend/
│   └── index.html          # Live dashboard (no framework needed)
├── .github/
│   └── workflows/
│       └── pipeline.yml    # GitHub Actions cron schedule
├── pipeline.py             # Master orchestrator (Scrape → Clean → Store)
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🚀 Running Locally (3 steps)

### Step 1 — Clone & Install

```bash
git clone https://github.com/YOUR_USERNAME/b2b-intel.git
cd b2b-intel

python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Step 2 — Configure Environment

```bash
cp .env.example .env
# Open .env — the defaults work for local dev (SQLite, no API key needed)
```

### Step 3 — Run Everything

**Option A: One command (full pipeline + API)**
```bash
# Terminal 1: Run the pipeline once to populate the database
python pipeline.py

# Terminal 2: Start the API
cd api && uvicorn main:app --reload --port 8000

# Terminal 3: Open the dashboard
open frontend/index.html      # or just double-click the file
```

**Option B: Step by step**
```bash
python scraper/scraper.py     # scrape → data/raw/
python cleaner/cleaner.py     # clean  → data/clean/
python pipeline.py            # store into database
```

---

## 🌐 API Endpoints

Base URL: `http://localhost:8000`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Health check |
| GET | `/docs` | Interactive API docs (Swagger UI) |
| GET | `/api/stats` | Dashboard summary stats |
| GET | `/api/jobs` | Paginated job listings with filters |
| GET | `/api/companies` | Company hiring signals |
| GET | `/api/trends` | Tech stack & role trends |
| POST | `/api/refresh` | Manually trigger pipeline |

### Example Requests

```bash
# All jobs
curl http://localhost:8000/api/jobs

# Filter by company
curl http://localhost:8000/api/jobs?company=Stripe

# Remote Python jobs
curl "http://localhost:8000/api/jobs?tag=Python&remote=true"

# Top hiring companies
curl http://localhost:8000/api/companies?min_jobs=2

# Tech trends last 7 days
curl http://localhost:8000/api/trends?days=7
```

---

## 🔄 Automation (GitHub Actions)

The pipeline runs **automatically every day at 06:00 UTC** via GitHub Actions.

**Setup:**
1. Push this repo to GitHub
2. Go to `Settings → Secrets → Actions`
3. Add `DATABASE_URL` secret (your Supabase connection string)
4. Optionally add `OPENAI_API_KEY` for AI summaries
5. The workflow runs automatically — no manual intervention needed

To trigger manually: `Actions tab → B2B Intel Pipeline → Run workflow`

---

## ☁️ Deployment

### Backend (Render — free tier)
1. Create account at [render.com](https://render.com)
2. New → Web Service → connect your GitHub repo
3. Build command: `pip install -r requirements.txt`
4. Start command: `cd api && uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Add environment variable: `DATABASE_URL` = your Supabase URL
6. Deploy → copy the URL (e.g. `https://b2b-intel.onrender.com`)

### Frontend (any static host)
- Update `API` variable in `frontend/index.html` to your Render URL
- Deploy to Vercel, Netlify, or GitHub Pages (it's just one HTML file)

### Database (Supabase — free tier)
1. Create project at [supabase.com](https://supabase.com)
2. Settings → Database → copy the connection string
3. Set as `DATABASE_URL` in Render + GitHub Secrets

---

## 🧹 Cleaning Decisions (documented)

| Decision | Rationale |
|----------|-----------|
| Deduplicate by `job_id` | Same job can appear across multiple scrape runs |
| Missing company → "Unknown Company" then filtered out | Jobs without company data have no B2B value |
| Missing location → "Remote" | Majority of scraped jobs are remote-first |
| Strip HTML from descriptions | Raw data contains HTML tags from job board |
| Salary kept as null if missing | Imputing salary would be misleading |
| Tags deduplicated + sorted | Consistent format for trend analysis |
| Dates → ISO 8601 UTC | Standardised format for DB storage |
| Company legal suffixes removed | "Stripe Inc." and "Stripe" are the same company |

---

## 🤖 AI/ML Bonus Layer

**File:** `api/ai_layer.py`

### What it does
Generates a natural language "Company Intelligence Brief" for each hiring company using GPT-3.5-turbo. Given a company's job postings, it produces:
- A 2-3 sentence description of what they're building
- Key business signals (e.g. "Expanding into ML", "Building data platform")
- A growth score (1-10)
- A recommended sales pitch

### Why this approach
- LLMs excel at synthesising unstructured job description text
- Gives sales reps instant context without reading 10 job posts
- Cost: ~$0.002 per company summary (GPT-3.5-turbo)

### Trade-offs considered
| Option | Pros | Cons |
|--------|------|------|
| GPT-3.5-turbo (chosen) | High quality, fast | ~$0.002/summary, needs API key |
| Local Ollama/Mistral | Free, private | Slower, lower quality |
| spaCy NER | Very fast, free | Misses nuance, no summarisation |
| Rule-based (fallback) | Zero cost, always works | Basic output only |

**The fallback:** If no OpenAI key is set, the system uses a rule-based summariser that still extracts meaningful signals from tag frequency.

---

## ⚙️ Required Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | SQLite path (default) or Supabase PostgreSQL URL |
| `OPENAI_API_KEY` | No | For AI summaries — uses rule-based fallback if not set |

---

## 📊 Data Sources

| Source | Type | Rate limit | Notes |
|--------|------|------------|-------|
| RemoteOK | Public JSON API | ~1 req/sec | Free, no auth needed |
| HN Who Is Hiring | HN + Algolia APIs | ~10 req/sec | Free, no auth needed |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Scraping | Python, requests |
| Cleaning | pandas |
| Database | SQLite (dev) / PostgreSQL via Supabase (prod) |
| ORM | SQLAlchemy |
| API | FastAPI + uvicorn |
| Frontend | Vanilla HTML/CSS/JS |
| Automation | GitHub Actions (cron) |
| Hosting | Render (API) + Vercel (frontend) |
| AI Bonus | OpenAI GPT-3.5-turbo |

---

## 🔮 What I'd Add With More Time

- Playwright scraper for sites that block simple requests
- Email alerts when a target company posts a new job
- Embedding-based company similarity clustering
- Webhook to push new signals to Slack
- Historical trend charts (Chart.js)

---

*Built for Data Engineering Intern Assignment — total time: ~10 hours*
