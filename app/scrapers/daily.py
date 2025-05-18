# app/scrapers/daily.py

from __future__ import annotations
import sys
import asyncio
import logging
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from apscheduler.triggers.date import DateTrigger
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout
from sqlmodel import Session, select

from app.config import get_settings
from app.db import engine
from app.models import Race, ScrapeLog
from app.scrapers.race import run_race_scrape

logger = logging.getLogger(__name__)
settings = get_settings()

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


async def _dismiss_cookies(page: Page) -> None:
    for sel in (
        "button#onetrust-accept-btn-handler",
        "button.ot-sdk-btn",
        "button:has-text('Tout accepter')",
        "button:has-text('Accepter')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
    ):
        try:
            await page.locator(sel).click(timeout=2_000)
            return
        except PWTimeout:
            continue


async def _extract_races(page: Page) -> List[Dict[str, Any]]:
    await _dismiss_cookies(page)

    await page.wait_for_selector(
        "ul.races-list li.race[data-betting-race-id]",
        state="attached",
        timeout=60_000,
    )


    raw = await page.eval_on_selector_all(
    "ul.races-list li.race[data-betting-race-id]",
    """
    nodes => nodes.map(li => {
        const id = li.getAttribute('data-betting-race-id');
        const epoch = li.getAttribute('data-betting-race-time');
        
        const meetingTitle = li.querySelector('h3.meeting-title')?.textContent?.trim() || "";
        const raceTitle = li.querySelector('h4.race-title')?.textContent?.trim() || "";

        const rankSpan = li.querySelector('.rank');
        const meetingRank = rankSpan.querySelector('.meetingrank')?.textContent?.trim() || "";
        const courseNumberMatch = rankSpan.textContent.trim().match(/C(\\d+)/);
        const courseNumber = courseNumberMatch ? 'C' + courseNumberMatch[1] : "";

        const dateObj = new Date(parseInt(epoch));
        const day = String(dateObj.getDate()).padStart(2, '0');
        const month = String(dateObj.getMonth() + 1).padStart(2, '0');
        const year = dateObj.getFullYear();
        const urlDate = `${day}-${month}-${year}`;

        const slugify = str => str
            .normalize('NFD').replace(/[\\u0300-\\u036f]/g, '')
            .replace(/[^a-zA-Z0-9]+/g, '-').toLowerCase()
            .replace(/^-+|-+$/g, '');

        const locationSlug = slugify(meetingTitle);
        const raceNameSlug = slugify(raceTitle);

        const url = `https://www.unibet.fr/turf/race/${urlDate}-${meetingRank}-${courseNumber}-${locationSlug}-${raceNameSlug}.html`;

        return {
            unibet_id: id,
            epoch_ms: epoch,
            url: url,
            meeting: meetingTitle,
            name: raceTitle,
            distance: li.querySelector('span.distance')?.textContent?.trim() || ""
        };
    })
    """,
)


    races: List[Dict[str, Any]] = []
    for r in raw:
        try:
            logger.info(f"Processing race: {r}")
            race_utc = datetime.fromtimestamp(int(r["epoch_ms"]) / 1000, tz=timezone.utc)
            race_local = race_utc.astimezone(ZoneInfo("Europe/Paris"))
            races.append({
                "unibet_id": r["unibet_id"],
                "name": r["name"],
                "meeting": r["meeting"],
                "race_time": race_local,
                "url": r["url"],
                "surface": None,
                "distance_m": int(r["distance"].rstrip("m")) if r["distance"].endswith("m") else None,
                "scraped_at": datetime.utcnow(),
            })
        except Exception as exc:
            logger.exception(f"Failed processing race {r['unibet_id']}: {exc}")
    return races


def _schedule_per_race_jobs(races: List[Race]) -> None:
    from app.scheduler import scheduler

    for race in races:
        race_start_utc = race.race_time.astimezone(timezone.utc)
        run_time_utc = race_start_utc - timedelta(minutes=3)

        now_utc = datetime.now(timezone.utc)
        if run_time_utc <= now_utc:
            continue

        job_id = f"race_{race.id}"
        if scheduler.get_job(job_id):
            continue

        scheduler.add_job(
            run_race_scrape,
            trigger="date",
            run_date=run_time_utc,
            args=[race.id],
            id=job_id,
            replace_existing=True,
            misfire_grace_time=60,
        )

        logger.info(
            "[scheduler] race %d scheduled at %s UTC (3m before local %s)",
            race.id,
            run_time_utc.isoformat(),
            race.race_time.isoformat(),
        )


async def run_daily_scrape() -> None:
    log = ScrapeLog(job_type="daily", started_at=datetime.utcnow(), status="ok")
    logger.info("→ daily scrape starting")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not settings.SHOW_BROWSER)
        try:
            page = await browser.new_page(ignore_https_errors=True)
            logger.info("Opening programme page…")
            await page.goto("https://www.unibet.fr/turf/programme", timeout=60_000)

            raw = await _extract_races(page)
            logger.info("Parsed %d races", len(raw))

            saved: List[Race] = []
            with Session(engine) as sess:
                for data in raw:
                    db_race = sess.exec(
                        select(Race).where(Race.unibet_id == data["unibet_id"])
                    ).first() or Race(**data)
                    for k, v in data.items():
                        setattr(db_race, k, v)
                    sess.add(db_race)
                    saved.append(db_race)
                sess.commit()
                for r in saved:
                    sess.refresh(r)

            _schedule_per_race_jobs(saved)

            log.message = f"Saved {len(saved)} races"
            logger.info(log.message)

        except Exception:
            log.status = "error"
            log.message = "daily scrape error"
            logger.exception("Daily scrape failed")

        finally:
            log.finished_at = datetime.utcnow()
            with Session(engine) as sess:
                sess.add(log)
                sess.commit()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(run_daily_scrape())