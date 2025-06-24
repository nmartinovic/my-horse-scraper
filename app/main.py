# app/main.py

import sys
import asyncio
import logging

from fastapi import FastAPI, BackgroundTasks, status
from fastapi.responses import HTMLResponse
from sqlmodel import Session, select, delete

from app.db import engine
from app.models import Race, RaceDetail
from app.scrapers.daily import run_daily_scrape, reschedule_jobs
from app.scheduler import scheduler

# On Windows, use SelectorEventLoopPolicy everywhere (so playwright can spawn its subprocesses)
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s │ %(message)s",
)
logger = logging.getLogger("app.main")

app = FastAPI(title="Horse Racing Dashboard")


@app.on_event("startup")
async def _start_scheduler():
    scheduler.start()
    logger.info("Scheduler started")


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


@app.get("/api/races")
def get_races():
    with Session(engine) as session:
        return session.exec(select(Race)).all()


@app.get("/api/race_details")
def get_race_details():
    with Session(engine) as session:
        return session.exec(select(RaceDetail)).all()


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
        </style>
    </head>
    <body>
        <h1>Horse Racing Dashboard</h1>
        <button onclick="clearDb()">Clear DB</button>
        <button onclick="runDaily()">Run Daily Scrape</button>
        <button onclick="reschedule()">Reschedule Jobs</button>

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

        <h2>Scheduled Scrapes</h2>
        <table id="jobs-table">
          <thead><tr>
            <th>Job ID</th><th>Race ID</th><th>Next Run</th>
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
            await Promise.all([loadRaces(), loadDetails(), loadJobs()]);
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
                tr.innerHTML = `
                  <td>${j.id}</td>
                  <td>${j.race_id}</td>
                  <td>${formatted}</td>
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
        
        window.onload = loadAll;
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)