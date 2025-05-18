# app/scrapers/race.py

import sys
import asyncio
import logging
import httpx
import json
import math
import pprint

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

PREDICT_URL = (
    "http://127.0.0.1:8080/api/predict/html"
    "?win_strategy=mid_odds_balanced"
    "&include_place_bets=true"
    "&include_deuzio_bets=true"
    "&place_strategy=conservative_power"
    "&deuzio_strategy=aggressive"
    "&place_min_probability=0.25"
    "&place_min_ev=2.5"
    "&deuzio_min_probability=0.10"
    "&deuzio_min_ev=1.0"
    "&bankroll=103"
)

FORWARD_URL = "http://127.0.0.1:5173/place-bets"

def round_down(amount: float) -> int:
    return max(int(amount), 2)

def _scrape_sync(race_id: int):
    log.info("→ _scrape_sync starting for race_id=%d", race_id)

    if sys.platform.startswith("win"):
        policy = asyncio.get_event_loop_policy()
        if not isinstance(policy, asyncio.WindowsProactorEventLoopPolicy):
            log.debug("Setting WindowsProactorEventLoopPolicy")
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    with Session(engine) as sess:
        race = sess.exec(select(Race).where(Race.id == race_id)).one()
    url = race.url
    log.info("→ Scraping race %d @ %s", race_id, url)

    try:
        with sync_playwright() as p:
            log.debug("Launching headless Chromium")
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            log.debug("Navigating to page (networkidle)")
            page.goto(url, wait_until="networkidle", timeout=60_000)

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

            prediction_response = None

            if content:
                try:
                    headers = {"Content-Type": "text/html; charset=utf-8"}
                    resp = httpx.post(PREDICT_URL, content=content.encode("utf-8"), headers=headers, timeout=10.0)
                    resp.raise_for_status()
                    prediction_response = resp.text
                    log.info("📬 Sent race %d to prediction server (status %d)", race_id, resp.status_code)

                    parsed = json.loads(prediction_response)

                    recommendations = parsed.get("recommendations", [])
                    for r in recommendations:
                        original = r.get("bet_amount", 0)
                        r["bet_amount"] = round_down(original)

                    summary = parsed.get("summary", {})
                    summary.setdefault("boulot_bets", 0)

                    forwarded_payload = {
                        "race_url": url,
                        "recommendations": recommendations,
                        "summary": summary,
                    }

                    log.debug("📦 Forwarding payload to place-bets:\n%s", pprint.pformat(forwarded_payload))
                    forward_resp = httpx.post(FORWARD_URL, json=forwarded_payload, timeout=10.0)
                    try:
                        forward_resp.raise_for_status()
                    except httpx.HTTPStatusError as e:
                        log.error("❌ Failed with %s\nResponse body:\n%s", e, forward_resp.text)
                        raise
                    log.info("🚀 Forwarded prediction to %s (status %d)", FORWARD_URL, forward_resp.status_code)

                except Exception as e:
                    log.exception("❌ Failed during prediction or forwarding for race %d: %s", race_id, e)

                with Session(engine) as sess:
                    race_detail = RaceDetail(
                        race_id=race_id,
                        bookmarklet_json=content,
                        prediction_response=prediction_response,
                        race_url=url,
                    )
                    sess.add(race_detail)
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
    log.info("Scheduling scrape for race %d", race_id)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _scrape_sync, race_id)

if __name__ == "__main__":
    import argparse

    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    parser = argparse.ArgumentParser(description="Scrape one race detail by DB ID")
    parser.add_argument("race_id", type=int, help="Race.id to scrape")
    args = parser.parse_args()

    _scrape_sync(args.race_id)
