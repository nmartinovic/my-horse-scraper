# app/scheduler_refresh.py - Fixed version with proper timezone handling

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlmodel import Session, select, delete
from apscheduler.triggers.cron import CronTrigger

from app.db import engine
from app.models import Race, RaceDetail
from app.scrapers.daily import run_daily_scrape, _schedule_per_race_jobs
from app.scheduler import scheduler

logger = logging.getLogger(__name__)

def find_next_safe_refresh_time() -> Optional[datetime]:
    """
    Find the next time when no race will start within 3 minutes.
    Returns None if we can refresh now, or a datetime when it's safe to refresh.
    """
    now_utc = datetime.now(timezone.utc)
    safety_buffer = timedelta(minutes=3)
    
    with Session(engine) as session:
        # Get all upcoming races in the next 24 hours
        upcoming_races = session.exec(
            select(Race).where(
                Race.race_time > now_utc,
                Race.race_time <= now_utc + timedelta(hours=24)
            ).order_by(Race.race_time)
        ).all()
        
        if not upcoming_races:
            # No upcoming races, safe to refresh now
            logger.info("🟢 No upcoming races found, safe to refresh immediately")
            return None
        
        # Check if any race starts within the next 3 minutes
        next_race = upcoming_races[0]
        
        # Ensure race_time is timezone-aware - convert if needed
        if next_race.race_time.tzinfo is None:
            # Race time is naive, assume it's in Europe/Paris and convert to UTC
            from zoneinfo import ZoneInfo
            race_time_aware = next_race.race_time.replace(tzinfo=ZoneInfo("Europe/Paris"))
            race_time_utc = race_time_aware.astimezone(timezone.utc)
        else:
            race_time_utc = next_race.race_time.astimezone(timezone.utc)
        
        if race_time_utc > now_utc + safety_buffer:
            # Next race is more than 3 minutes away, safe to refresh now
            minutes_until = (race_time_utc - now_utc).total_seconds() / 60
            logger.info("🟢 Next race in %.1f minutes, safe to refresh immediately", minutes_until)
            return None
        
        logger.info("🟡 Race starts within 3 minutes, finding safe window...")
        
        # Find the next safe window after all conflicting races
        for i, race in enumerate(upcoming_races):
            # Convert race time to UTC if needed
            if race.race_time.tzinfo is None:
                from zoneinfo import ZoneInfo
                race_start_aware = race.race_time.replace(tzinfo=ZoneInfo("Europe/Paris"))
                race_start_utc = race_start_aware.astimezone(timezone.utc)
            else:
                race_start_utc = race.race_time.astimezone(timezone.utc)
            
            race_end_buffer = race_start_utc + timedelta(minutes=10)  # Assume race + scraping takes ~10 min
            
            # Check if there's a gap before the next race
            if i + 1 < len(upcoming_races):
                next_race_obj = upcoming_races[i + 1]
                
                # Convert next race time to UTC if needed
                if next_race_obj.race_time.tzinfo is None:
                    from zoneinfo import ZoneInfo
                    next_race_aware = next_race_obj.race_time.replace(tzinfo=ZoneInfo("Europe/Paris"))
                    next_race_start_utc = next_race_aware.astimezone(timezone.utc)
                else:
                    next_race_start_utc = next_race_obj.race_time.astimezone(timezone.utc)
                
                gap_duration = next_race_start_utc - race_end_buffer
                
                if gap_duration >= safety_buffer:
                    # Found a safe window
                    logger.info("🟢 Found safe window after race %d at %s UTC", race.id, race_end_buffer.isoformat())
                    return race_end_buffer
            else:
                # This is the last race, schedule refresh after it
                logger.info("🟢 Scheduling refresh after last race at %s UTC", race_end_buffer.isoformat())
                return race_end_buffer
        
        # If no safe window found in 24h, just schedule for tomorrow
        tomorrow = now_utc.replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=1)
        logger.info("🟡 No safe window found, scheduling for tomorrow at %s UTC", tomorrow.isoformat())
        return tomorrow

async def clear_db_and_refresh():
    """Clear database, run daily scrape, and reschedule jobs."""
    logger.info("🧹 Starting database clear and refresh")
    
    try:
        # Clear existing data
        with Session(engine) as session:
            deleted_details = session.exec(delete(RaceDetail)).rowcount
            deleted_races = session.exec(delete(Race)).rowcount
            session.commit()
        logger.info("✅ Database cleared: %d races, %d details", deleted_races, deleted_details)
        
        # Run daily scrape
        await run_daily_scrape()
        logger.info("✅ Daily scrape completed")
        
        # Jobs are automatically scheduled by run_daily_scrape via _schedule_per_race_jobs
        logger.info("✅ Database refresh completed successfully")
        
    except Exception as e:
        logger.exception("❌ Database refresh failed: %s", e)

def schedule_next_refresh():
    """Schedule the next database refresh at the optimal time."""
    try:
        safe_time = find_next_safe_refresh_time()
        
        if safe_time is None:
            # Safe to refresh now, schedule it immediately
            logger.info("🔄 Scheduling immediate database refresh")
            scheduler.add_job(
                clear_db_and_refresh,
                trigger="date",
                run_date=datetime.now(timezone.utc) + timedelta(seconds=5),
                id="db_refresh_immediate",
                replace_existing=True,
                misfire_grace_time=300  # 5 minutes grace
            )
        else:
            # Schedule for the safe time
            logger.info("⏰ Scheduling database refresh for %s UTC", safe_time.isoformat())
            scheduler.add_job(
                clear_db_and_refresh,
                trigger="date", 
                run_date=safe_time,
                id="db_refresh_delayed",
                replace_existing=True,
                misfire_grace_time=300  # 5 minutes grace
            )
    except Exception as e:
        logger.exception("❌ Failed to schedule refresh: %s", e)

def hourly_refresh_check():
    """
    Called every hour to check if we should refresh the database.
    This is the main entry point for the hourly refresh system.
    """
    logger.info("🕐 Hourly refresh check triggered")
    
    try:
        # Remove any existing refresh jobs to avoid duplicates
        for job_id in ["db_refresh_immediate", "db_refresh_delayed"]:
            try:
                scheduler.remove_job(job_id)
                logger.debug("Removed existing job: %s", job_id)
            except:
                pass  # Job might not exist
        
        # Schedule the next refresh
        schedule_next_refresh()
        
    except Exception as e:
        logger.exception("❌ Hourly refresh check failed: %s", e)

def setup_hourly_refresh():
    """Set up the hourly refresh check job. Call this during app startup."""
    logger.info("🔧 Setting up hourly refresh system")
    
    try:
        # Schedule hourly checks
        scheduler.add_job(
            hourly_refresh_check,
            trigger=CronTrigger(minute=0),  # Run at the top of every hour
            id="hourly_refresh_check",
            replace_existing=True,
            misfire_grace_time=300  # 5 minutes grace
        )
        
        # Also run an initial check 30 seconds after startup
        scheduler.add_job(
            hourly_refresh_check,
            trigger="date",
            run_date=datetime.now(timezone.utc) + timedelta(seconds=30),
            id="initial_refresh_check",
            replace_existing=True
        )
        
        logger.info("✅ Hourly refresh system configured")
        
    except Exception as e:
        logger.exception("❌ Failed to setup hourly refresh: %s", e)

# Helper function to manually trigger refresh (for API endpoint)
def trigger_manual_refresh():
    """Manually trigger a database refresh, respecting race timing."""
    logger.info("🔄 Manual refresh triggered")
    schedule_next_refresh()