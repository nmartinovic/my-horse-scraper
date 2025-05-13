# app/scheduler.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from datetime import timezone

from app.db import engine

def get_scheduler() -> AsyncIOScheduler:
    """
    Create and return our singleton AsyncIO scheduler,
    backed by the SQLite DB, running entirely in UTC.
    """
    jobstores = {
        "default": SQLAlchemyJobStore(engine=engine)
    }
    return AsyncIOScheduler(
        jobstores=jobstores,
        timezone=timezone.utc,     # ← force UTC everywhere
    )

# module‐level singleton
scheduler = get_scheduler()
