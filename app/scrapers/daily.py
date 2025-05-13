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

# On Windows, use the Proactor policy so Playwright’s subprocesses work
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

    # wait for the list to be attached
    await page.wait_for_selector(
        "ul.races-list li.race[data-betting-race-id]",
        state="attached",
        timeout=60_000,
    )

    # grab real URLs off each <a>, not just build from the ID
    raw = await page.eval_on_selector_all(
        "ul.races-list li.race[data-betting-race-id]",
        """
        nodes => nodes.map(li => {
            const id    = li.getAttribute('data-betting-race-id');
            const epoch = li.getAttribute('data-betting-race-time');
            // look for the anchor inside, if present
            const anchor = li.querySelector('a[href*="/turf/course/"]');
            const url = anchor
                ? anchor.href
                : `${window.location.origin}/turf/course/${id}`;
            return {
                unibet_id: id,
                epoch_ms:  epoch,
                url:       url,
                meeting:   li.querySelector('h3.meeting-title')?.textContent?.trim() || "",
                name:      li.querySelector('h4.race-title')?.textContent?.trim() || "",
                distance:  li.querySelector('span.distance')?.textContent?.trim() || ""
            };
        })
        """,
    )
    logger.debug("raw races from page → %O", raw)

    races: List[Dict[str, Any]] = []
    for r in raw:
        try:
            # parse the epoch (ms) *as UTC*, then convert to Europe/Paris
            race_utc = datetime.fromtimestamp(int(r["epoch_ms"]) / 1000, tz=timezone.utc)
            race_local = race_utc.astimezone(ZoneInfo("Europe/Paris"))
        except Exception as exc:
            logger.warning("Skipping bad epoch %r for %s: %s", r["epoch_ms"], r["unibet_id"], exc)
            continue

        races.append({
            "unibet_id":  r["unibet_id"],
            "name":       r["name"],
            "meeting":    r["meeting"],
            "race_time":  race_local,              # tz-aware Europe/Paris
            "url":        r["url"],
            "surface":    None,
            "distance_m": int(r["distance"].rstrip("m")) if r["distance"].endswith("m") else None,
            "scraped_at": datetime.utcnow(),
        })

    return races


def _schedule_per_race_jobs(races: List[Race]) -> None:
    """
    For each Race (tz-aware Europe/Paris), schedule its scrape
    exactly 3 minutes before its start, *in UTC*.
    """
    from app.scheduler import scheduler

    for race in races:
        # 1) Take the stored Paris time, convert to UTC
        race_start_utc = race.race_time.astimezone(timezone.utc)
        # 2) Subtract 3 minutes *in UTC*
        run_time_utc = race_start_utc - timedelta(minutes=3)

        now_utc = datetime.now(timezone.utc)
        if run_time_utc <= now_utc:
            continue  # already in the past

        job_id = f"race_{race.id}"
        if scheduler.get_job(job_id):
            continue  # don't double-schedule

        scheduler.add_job(
            run_race_scrape,
            trigger="date",
            run_date=run_time_utc,    # tz-aware UTC datetime
            args=[race.id],
            id=job_id,
            replace_existing=True,
            misfire_grace_time=60,
        )

        logger.info(
            "[scheduler] race %d scheduled at %s UTC  (3m before local %s)",
            race.id,
            run_time_utc.isoformat(),
            race.race_time.isoformat(),
        )


async def run_daily_scrape() -> None:
    """
    Fetch today’s programme, upsert into DB, then schedule each per-race job.
    """
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
