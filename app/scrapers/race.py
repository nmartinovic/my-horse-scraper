# app/scrapers/race.py

import sys
import asyncio
import logging

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from sqlmodel import Session, select

from app.db import engine
from app.models import Race, RaceDetail

# ─── configure logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s %(levelname)7s %(name)s │ %(message)s",
    level=logging.DEBUG,
)
log = logging.getLogger(__name__)


def _scrape_sync(race_id: int):
    """Synchronously scrape one race by running the bookmarklet JS on its page."""
    log.info("→ _scrape_sync starting for race_id=%d", race_id)

    # On Windows, ensure ProactorEventLoopPolicy so playwright can spawn subprocesses
    if sys.platform.startswith("win"):
        policy = asyncio.get_event_loop_policy()
        if not isinstance(policy, asyncio.WindowsProactorEventLoopPolicy):
            log.debug("Setting WindowsProactorEventLoopPolicy")
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # 1) Load URL from DB
    with Session(engine) as sess:
        race = sess.exec(select(Race).where(Race.id == race_id)).one()
    url = race.url
    log.info("→ Scraping race %d @ %s", race_id, url)

    try:
        with sync_playwright() as p:
            log.debug("Launching headless Chromium")
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            log.debug("Navigating to page (DOMContentLoaded)")
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            # 2) Run your bookmarklet logic in-page and return the text blob
            log.debug("Running bookmarklet extraction")
            js = r"""
            () => {
              const titleElem = document.querySelector('.race-head-title.ui-mainview-block');
              const metaElem  = document.querySelector('.race-meta.ui-mainview-block');
              const runnersEls = document.querySelectorAll('.runners-list');
              const trackElem = document.querySelector('.ui-left');

              const title   = titleElem ? titleElem.innerText.trim() : "race_data";
              const meta    = metaElem  ? metaElem.innerText.trim()  : "No meta found";
              const runners = runnersEls.length
                              ? Array.from(runnersEls).map(el => el.outerHTML).join("\n")
                              : "No runners found";
              const track   = trackElem ? trackElem.innerText.trim() : "Track not found";

              return `Title: ${title}\n\nMeta: ${meta}\n\nRunners (HTML):\n${runners}\n\nTrack: ${track}`;
            }
            """
            try:
                content = page.evaluate(js)
                log.info("✔ Bookmarklet JS returned %d characters", len(content))
            except PlaywrightTimeoutError:
                log.error("⏰ Timeout running bookmarklet on race %d", race_id)
                content = None

            # 3) Persist into RaceDetail
            if content:
                with Session(engine) as sess:
                    sess.add(RaceDetail(race_id=race_id, bookmarklet_json=content))
                    sess.commit()
                log.info("✅ Saved RaceDetail for race %d", race_id)
            else:
                log.error("❌ No content extracted for race %d", race_id)

            browser.close()

    except NotImplementedError as nie:
        log.error("🔴 Playwright subprocess creation failed: %s", nie)
        log.error("   └─ Ensure ProactorEventLoopPolicy on Windows before starting uvicorn")
    except Exception:
        log.exception("🐛 Unexpected error in _scrape_sync for race %d", race_id)


async def run_race_scrape(race_id: int):
    """APS-scheduler entrypoint: delegate to _scrape_sync in a thread."""
    log.info("Scheduling scrape for race %d", race_id)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _scrape_sync, race_id)


if __name__ == "__main__":
    import argparse

    # Windows event-loop fix again if run as a script
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    parser = argparse.ArgumentParser(description="Scrape one race detail by DB ID")
    parser.add_argument("race_id", type=int, help="Race.id to scrape")
    args = parser.parse_args()

    _scrape_sync(args.race_id)
