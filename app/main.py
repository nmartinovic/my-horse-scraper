# app/main.py

import sys
import asyncio
import logging

from fastapi import FastAPI, BackgroundTasks, status
from fastapi.responses import HTMLResponse
from sqlmodel import Session, select, delete

from app.db import engine
from app.models import Race, RaceDetail, ScrapeLog
from app.scrapers.daily import run_daily_scrape, reschedule_jobs
from app.scheduler import scheduler
from app.scheduler_refresh import setup_hourly_refresh, trigger_manual_refresh
from app.git_operations import daily_git_commit  # NEW
from datetime import datetime

# On Windows, use SelectorEventLoopPolicy everywhere (so playwright can spawn its subprocesses)
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s │ %(message)s",
)
logger = logging.getLogger("app.main")

app = FastAPI(title="Horse Racing Dashboard")


def setup_daily_git_job():
    """Setup the daily git commit job at 00:00 UTC"""
    try:
        # Remove existing job if it exists
        existing_job = scheduler.get_job("daily_git_commit")
        if existing_job:
            scheduler.remove_job("daily_git_commit")
        
        # Add daily git commit job at 00:00 UTC
        scheduler.add_job(
            run_git_commit_job,
            trigger="cron",
            hour=0,
            minute=0,
            timezone="UTC",
            id="daily_git_commit",
            replace_existing=True,
            misfire_grace_time=300  # 5 minutes grace time
        )
        
        logger.info("Daily git commit job scheduled for 00:00 UTC")
        
    except Exception as e:
        logger.error("Failed to setup daily git job: %s", e)


def run_git_commit_job():
    """Wrapper function to run git commit and log results"""
    logger.info("Starting daily git commit job")
    
    try:
        result = daily_git_commit()
        
        # Log the operation to database
        log_entry = ScrapeLog(
            job_type="git_commit",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            status=result["status"],
            message=result["message"]
        )
        
        with Session(engine) as session:
            session.add(log_entry)
            session.commit()
        
        if result["status"] == "ok":
            logger.info("Git commit completed: %s", result["message"])
        elif result["status"] == "disabled":
            logger.info("Git auto-commit is disabled")
        else:
            logger.error("Git commit failed: %s", result["message"])
            
    except Exception as e:
        logger.exception("Error in git commit job")
        
        # Log the error
        log_entry = ScrapeLog(
            job_type="git_commit",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            status="error",
            message=f"Unexpected error: {str(e)}"
        )
        
        with Session(engine) as session:
            session.add(log_entry)
            session.commit()


@app.on_event("startup")
async def _start_scheduler():
    scheduler.start()
    logger.info("Scheduler started")
    
    # Set up the hourly refresh system
    setup_hourly_refresh()
    logger.info("Hourly refresh system initialized")
    
    # Set up daily git commit job
    setup_daily_git_job()
    logger.info("Daily git commit job initialized")


@app.on_event("shutdown")
async def _stop_scheduler():
    scheduler.shutdown(wait=False)
    logger.info("Scheduler shutdown")


def _run_daily_in_thread():
    asyncio.run(run_daily_scrape())


@app.post("/api/scrape/daily/run", status_code=status.HTTP_202_ACCEPTED)
async def trigger_daily_scrape(background_tasks: BackgroundTasks):
    logger.info("→ trigger_daily_scrape endpoint called")
    background_tasks.add_task(_run_daily_in_thread)
    return {"status": "daily scrape scheduled"}


@app.post("/api/reschedule", status_code=status.HTTP_202_ACCEPTED)
async def trigger_reschedule():
    logger.info("→ reschedule endpoint called")
    reschedule_jobs()
    return {"status": "rescheduled all jobs"}


@app.post("/api/refresh", status_code=status.HTTP_202_ACCEPTED)
async def trigger_refresh():
    """Manually trigger a smart database refresh that respects race timing."""
    logger.info("→ manual refresh endpoint called")
    trigger_manual_refresh()
    return {"status": "refresh scheduled at optimal time"}


# NEW: Manual git commit endpoint
@app.post("/api/git/commit", status_code=status.HTTP_202_ACCEPTED)
async def trigger_git_commit():
    """Manually trigger a git commit of CSV data."""
    logger.info("→ manual git commit endpoint called")
    
    def run_commit():
        run_git_commit_job()
    
    import threading
    thread = threading.Thread(target=run_commit)
    thread.start()
    
    return {"status": "git commit scheduled"}


@app.get("/api/races")
def get_races():
    with Session(engine) as session:
        return session.exec(select(Race)).all()


@app.get("/api/race_details")
def get_race_details():
    with Session(engine) as session:
        return session.exec(select(RaceDetail)).all()


@app.get("/api/scrape_logs")
def get_scrape_logs():
    with Session(engine) as session:
        logs = session.exec(
            select(ScrapeLog).order_by(ScrapeLog.started_at.desc()).limit(50)
        ).all()
        return logs


@app.post("/api/db/clear")
def clear_database():
    with Session(engine) as session:
        session.exec(delete(RaceDetail))
        session.exec(delete(Race))
        session.commit()
    return {"status": "cleared"}


@app.get("/api/jobs")
def list_jobs():
    jobs = scheduler.get_jobs()
    return [
        {
            "id": job.id,
            "race_id": job.args[0] if job.args else None,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "job_type": (
                "git_commit" if job.id == "daily_git_commit" else
                "refresh" if "refresh" in job.id else 
                "race_scrape" if job.id.startswith("race_") else 
                "other"
            )
        }
        for job in jobs
    ]


@app.get("/", response_class=HTMLResponse)
def dashboard():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>Horse Racing Dashboard</title>
        <style>
            table { border-collapse: collapse; width: 100%; margin: 20px 0; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f4f4f4; }
            pre { max-width: 250px; max-height: 200px; overflow: auto; font-size: 11px; }
            button { margin: 5px; padding: 8px 16px; }
            .json-column { width: 20%; }
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1, h2 { color: #333; }
            .refresh-btn { background-color: #4CAF50; color: white; }
            .git-btn { background-color: #f39c12; color: white; }
            .job-type-refresh { background-color: #e8f5e8; }
            .job-type-race { background-color: #fff3e0; }
            .job-type-git { background-color: #fdf2e9; }
            .status-ok { color: green; font-weight: bold; }
            .status-error { color: red; font-weight: bold; }
        </style>
    </head>
    <body>
        <h1>Horse Racing Dashboard</h1>
        <button onclick="clearDb()">Clear DB</button>
        <button onclick="runDaily()">Run Daily Scrape</button>
        <button onclick="reschedule()">Reschedule Jobs</button>
        <button onclick="triggerRefresh()" class="refresh-btn">Smart Refresh</button>
        <button onclick="triggerGitCommit()" class="git-btn">Git Commit</button>

        <h2>Races</h2>
        <table id="races-table">
          <thead><tr>
            <th>ID</th><th>Unibet ID</th><th>Name</th><th>Meeting</th><th>Time</th>
          </tr></thead>
          <tbody></tbody>
        </table>

        <h2>Race Details</h2>
        <table id="details-table">
          <thead><tr>
            <th>ID</th>
            <th>Race ID</th>
            <th class="json-column">Scraped Data</th>
            <th class="json-column">Prediction Request</th>
            <th class="json-column">Prediction Response</th>
            <th class="json-column">Betting Request</th>
          </tr></thead>
          <tbody></tbody>
        </table>

        <h2>Scheduled Jobs</h2>
        <table id="jobs-table">
          <thead><tr>
            <th>Job ID</th><th>Type</th><th>Race ID</th><th>Next Run</th>
          </tr></thead>
          <tbody></tbody>
        </table>

        <h2>Recent Logs</h2>
        <table id="logs-table">
          <thead><tr>
            <th>Type</th><th>Started</th><th>Status</th><th>Message</th>
          </tr></thead>
          <tbody></tbody>
        </table>

        <script>
        async function fetchData(path) {
            const res = await fetch(path);
            return res.json();
        }
        
        function formatJSON(data) {
            if (!data) return '';
            try {
                if (typeof data === 'string') {
                    return JSON.stringify(JSON.parse(data), null, 2);
                } else {
                    return JSON.stringify(data, null, 2);
                }
            } catch {
                return data.toString();
            }
        }
        
        async function loadAll() {
            await Promise.all([loadRaces(), loadDetails(), loadJobs(), loadLogs()]);
        }
        
        async function loadRaces() {
            const races = await fetchData('/api/races');
            const tbody = document.querySelector('#races-table tbody');
            tbody.innerHTML = '';
            races.forEach(r => {
                const dt = new Date(r.race_time);
                const formatted = dt.toLocaleString(undefined, { timeZoneName: 'short' });
                const tr = document.createElement('tr');
                tr.innerHTML = `
                  <td>${r.id}</td>
                  <td>${r.unibet_id}</td>
                  <td>${r.name}</td>
                  <td>${r.meeting}</td>
                  <td>${formatted}</td>
                `;
                tbody.appendChild(tr);
            });
        }
        
        async function loadDetails() {
            const details = await fetchData('/api/race_details');
            const tbody = document.querySelector('#details-table tbody');
            tbody.innerHTML = '';
            details.forEach(d => {
                const scrapedData = formatJSON(d.bookmarklet_json);
                const requestData = formatJSON(d.prediction_request);
                const responseData = formatJSON(d.prediction_response);
                const bettingData = formatJSON(d.betting_request);
                
                const tr = document.createElement('tr');
                tr.innerHTML = `
                  <td>${d.id}</td>
                  <td>${d.race_id}</td>
                  <td><pre>${scrapedData}</pre></td>
                  <td><pre>${requestData}</pre></td>
                  <td><pre>${responseData}</pre></td>
                  <td><pre>${bettingData}</pre></td>
                `;
                tbody.appendChild(tr);
            });
        }
        
        async function loadJobs() {
            const jobs = await fetchData('/api/jobs');
            const tbody = document.querySelector('#jobs-table tbody');
            tbody.innerHTML = '';
            jobs.forEach(j => {
                let formatted = '—';
                if (j.next_run_time) {
                    const dt = new Date(j.next_run_time);
                    formatted = dt.toLocaleString(undefined, { timeZoneName: 'short' });
                }
                const tr = document.createElement('tr');
                tr.className = `job-type-${j.job_type}`;
                tr.innerHTML = `
                  <td>${j.id}</td>
                  <td>${j.job_type}</td>
                  <td>${j.race_id || '—'}</td>
                  <td>${formatted}</td>
                `;
                tbody.appendChild(tr);
            });
        }
        
        async function loadLogs() {
            const logs = await fetchData('/api/scrape_logs');
            const tbody = document.querySelector('#logs-table tbody');
            tbody.innerHTML = '';
            logs.forEach(l => {
                const dt = new Date(l.started_at);
                const formatted = dt.toLocaleString(undefined, { timeZoneName: 'short' });
                const tr = document.createElement('tr');
                tr.innerHTML = `
                  <td>${l.job_type}</td>
                  <td>${formatted}</td>
                  <td class="status-${l.status}">${l.status}</td>
                  <td>${l.message || '—'}</td>
                `;
                tbody.appendChild(tr);
            });
        }
        
        async function clearDb() {
            await fetch('/api/db/clear', { method: 'POST' });
            loadAll();
        }
        
        async function runDaily() {
            await fetch('/api/scrape/daily/run', { method: 'POST' });
            alert('Daily scrape scheduled');
            loadAll();
        }
        
        async function reschedule() {
            await fetch('/api/reschedule', { method: 'POST' });
            alert('Reschedule triggered');
            loadAll();
        }
        
        async function triggerRefresh() {
            await fetch('/api/refresh', { method: 'POST' });
            alert('Smart refresh scheduled at optimal time');
            loadAll();
        }
        
        async function triggerGitCommit() {
            await fetch('/api/git/commit', { method: 'POST' });
            alert('Git commit scheduled');
            loadAll();
        }
        
        window.onload = loadAll;
        setInterval(loadAll, 30000); // Refresh every 30 seconds
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)