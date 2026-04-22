"""
AI/ML Bonus Layer — Company Intelligence Summarizer
Uses OpenAI GPT to generate natural language hiring intelligence reports
from job posting data.

WHY THIS APPROACH:
- LLMs are excellent at synthesizing unstructured job description text
- Gives sales reps and analysts instant context without reading 10 job posts
- Trade-off: API cost per company (~$0.002 per summary) vs. value delivered

TRADE-OFFS CONSIDERED:
- Could use local models (e.g. Ollama/Mistral) → free but slower, less quality
- Could use rule-based NLP (spaCy) → fast but misses nuance
- GPT-3.5-turbo chosen: best cost/quality ratio for short summarisation tasks
- Summaries are cached in DB to avoid re-generating on every request
"""

import os
import json
import logging
from datetime import datetime

import requests

log = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_URL     = "https://api.openai.com/v1/chat/completions"


def summarise_company(company: str, jobs: list[dict]) -> dict:
    """
    Generate an AI intelligence brief for a company based on their job postings.
    
    Returns a dict with:
      - summary: 2-3 sentence natural language description
      - signals: list of business signals detected
      - growth_score: 1-10 estimate of hiring velocity
      - recommended_pitch: what a salesperson might say to this company
    """
    if not OPENAI_API_KEY:
        return _fallback_summary(company, jobs)

    # Build context from jobs
    job_summaries = []
    for j in jobs[:5]:  # max 5 jobs to keep prompt short
        tags = ", ".join(j.get("tags", [])[:5])
        job_summaries.append(
            f"- {j.get('title', 'Unknown Role')} | Location: {j.get('location', '')} | Skills: {tags}"
        )

    job_text = "\n".join(job_summaries) if job_summaries else "No job details available."
    job_count = len(jobs)

    prompt = f"""You are a B2B market intelligence analyst. Analyse the following hiring data for {company} and provide a brief intelligence report.

Company: {company}
Total open roles: {job_count}
Recent job postings:
{job_text}

Respond ONLY with a valid JSON object in this exact format:
{{
  "summary": "2-3 sentence description of what this company appears to be building/doing based on their hiring",
  "signals": ["signal 1", "signal 2", "signal 3"],
  "growth_score": 7,
  "growth_label": "High Growth",
  "recommended_pitch": "One sentence a salesperson could use when cold-calling this company"
}}

growth_score should be 1-10 where 10 = very fast growth, based on number and diversity of roles.
growth_label should be one of: Early Stage, Scaling, High Growth, Enterprise Hiring, Unknown"""

    try:
        response = requests.post(
            OPENAI_URL,
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       "gpt-3.5-turbo",
                "max_tokens":  400,
                "temperature": 0.3,
                "messages":    [{"role": "user", "content": prompt}],
            },
            timeout=20,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]

        # Strip markdown fences if present
        content = content.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        result  = json.loads(content)
        result["generated_at"] = datetime.utcnow().isoformat()
        result["company"]      = company
        result["source"]       = "openai"
        return result

    except Exception as e:
        log.warning(f"OpenAI summarisation failed for {company}: {e}")
        return _fallback_summary(company, jobs)


def _fallback_summary(company: str, jobs: list[dict]) -> dict:
    """
    Rule-based fallback when OpenAI is not available.
    Uses tag frequency to infer what the company does.
    """
    from collections import Counter
    all_tags = []
    for j in jobs:
        all_tags.extend(j.get("tags", []))

    top_tags = [t for t, _ in Counter(all_tags).most_common(3)]
    job_count = len(jobs)

    if job_count >= 5:
        growth_score = 8
        growth_label = "High Growth"
    elif job_count >= 3:
        growth_score = 6
        growth_label = "Scaling"
    else:
        growth_score = 4
        growth_label = "Early Stage"

    tech_str = ", ".join(top_tags) if top_tags else "various technologies"

    return {
        "company":      company,
        "summary":      f"{company} is actively hiring across {job_count} role(s), with a focus on {tech_str}. Their hiring pattern suggests active product development.",
        "signals":      [f"Hiring for {tech_str}", f"{job_count} open roles detected", "Active on job boards"],
        "growth_score": growth_score,
        "growth_label": growth_label,
        "recommended_pitch": f"We help companies like {company} scale their {tech_str} infrastructure — worth a 15-min call?",
        "generated_at": datetime.utcnow().isoformat(),
        "source":       "rule-based-fallback",
    }


def batch_summarise(companies_data: list[dict], max_companies: int = 10) -> list[dict]:
    """Summarise multiple companies. Caps at max_companies to control API costs."""
    results = []
    for company_data in companies_data[:max_companies]:
        company = company_data.get("company", "")
        jobs    = company_data.get("jobs", [])
        log.info(f"Summarising: {company}")
        summary = summarise_company(company, jobs)
        results.append(summary)
    return results
