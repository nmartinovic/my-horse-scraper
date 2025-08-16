# app/scrapers/race.py

import sys
import asyncio
import logging
import httpx
import json
import pprint
import csv
import os
from datetime import datetime
from pathlib import Path

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
    "http://127.0.0.1:8080/predict"
)

FORWARD_URL = "http://127.0.0.1:5173/place-bets"

# CSV file paths (in project root)
CSV_FILE_PATH = Path(__file__).parent.parent.parent / "race_data_log.csv"
RUNNERS_CSV_PATH = Path(__file__).parent.parent.parent / "race_runners_log.csv"

def save_to_csv(race_id: int, data_type: str, data: dict, timestamp: str):
    """
    Save data to CSV file with each request/response as a separate row
    
    Args:
        race_id: The race ID (can be int or string)
        data_type: Type of data ('prediction_request', 'prediction_response', 'betting_request')
        data: The actual data (dict or string)
        timestamp: ISO timestamp string
    """
    try:
        # Ensure CSV file exists with headers
        file_exists = CSV_FILE_PATH.exists()
        
        with open(CSV_FILE_PATH, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['timestamp', 'race_id', 'data_type', 'data_json']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
                log.info("Created new CSV file: %s", CSV_FILE_PATH)
            
            # Convert data to JSON string if it's a dict
            if isinstance(data, dict):
                data_json = json.dumps(data, ensure_ascii=False)
            else:
                data_json = str(data)
            
            # Write the row
            writer.writerow({
                'timestamp': timestamp,
                'race_id': race_id,
                'data_type': data_type,
                'data_json': data_json
            })
            
        log.info("Saved %s for race %s to CSV", data_type, race_id)
        
    except Exception as e:
        log.error("Failed to save to CSV: %s", e)

def save_runners_to_csv(runners_data: dict, timestamp: str):
    """
    Save individual runner data to structured CSV file
    
    Args:
        runners_data: The race data dict containing runners array
        timestamp: ISO timestamp string
    """
    try:
        runners = runners_data.get('runners', [])
        if not runners:
            log.warning("No runners data to save to CSV")
            return
        
        # Check if CSV file exists
        file_exists = RUNNERS_CSV_PATH.exists()
        
        # Define CSV headers matching your specification
        headers = [
            'Race_ID', 'Title', 'Meta', 'Track', 'Place', 'Number', 'Horse_Name', 
            'Jockey', 'Age_Sex', 'Equipment', 'Weight', 'Times', 
            'Odds_Morning', 'Odds_Live', 'Trainer', 'Distance', 'Musique', 'Additional_Info'
        ]
        
        with open(RUNNERS_CSV_PATH, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
                log.info("Created new runners CSV file: %s", RUNNERS_CSV_PATH)
            
            # Write each runner as a separate row
            for runner in runners:
                writer.writerow({
                    'Race_ID': runner.get('race_id', ''),
                    'Title': runner.get('title', ''),
                    'Meta': runner.get('meta', ''),
                    'Track': runner.get('track', ''),
                    'Place': runner.get('place', ''),
                    'Number': runner.get('number', ''),
                    'Horse_Name': runner.get('horse_name', ''),
                    'Jockey': runner.get('jockey', ''),
                    'Age_Sex': runner.get('age_sex', ''),
                    'Equipment': runner.get('equipment', ''),
                    'Weight': runner.get('weight', ''),
                    'Times': runner.get('times', ''),
                    'Odds_Morning': runner.get('odds_morning', ''),
                    'Odds_Live': runner.get('odds_live', ''),
                    'Trainer': runner.get('trainer', ''),
                    'Distance': runner.get('distance', ''),
                    'Musique': runner.get('musique', ''),
                    'Additional_Info': runner.get('additional_info', '')
                })
        
        log.info("Saved %d runners to CSV for race %s", len(runners), runners_data.get('race_info', {}).get('race_id', 'unknown'))
        
    except Exception as e:
        log.error("Failed to save runners to CSV: %s", e)

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

            # First try to click on "Tableau des partants" if it exists
            try:
                tableau_button = page.locator("span.text:has-text('Tableau des partants')")
                if tableau_button.count() > 0:
                    tableau_button.click()
                    page.wait_for_timeout(2000)  # Wait for content to load
            except Exception as e:
                log.warning("Could not click 'Tableau des partants': %s", e)

            log.debug("Running runners extraction JavaScript")
            
            # Enhanced JavaScript that extracts structured runner data
            js = """
            () => {
                // Race title block
                let title = document
                  .querySelector('.race-head-title.ui-mainview-block')
                  ?.innerText.trim() || "race_data";

                // Meta info block (date, track, etc.)
                let meta = document
                  .querySelector('.race-meta.ui-mainview-block')
                  ?.innerText.trim() || "No meta found";

                // Track name
                let track = document
                  .querySelector('.ui-left')
                  ?.innerText.trim() || "Track not found";

                // Generate race_id from URL
                let race_id = '';
                const currentUrl = window.location.href;
                const urlMatch = currentUrl.match(/race\/([^\/]+?)(?:\.html)?(?:\?|$)/);
                if (urlMatch) {
                    race_id = urlMatch[1];
                } else {
                    const titlePart = title.replace(/[^a-zA-Z0-9]/g, '').substring(0, 20);
                    const trackPart = track.replace(/[^a-zA-Z0-9]/g, '');
                    race_id = `${titlePart}_${trackPart}`.toLowerCase();
                }
                
                let runners = [];
                
                // Method 1: Parse structured runner list - prioritize the detailed table
                let runnerItems = document.querySelectorAll('ul.runners-list:not(.bottom-list) li.runner-item');
                
                // Find the best list with most columns
                const allLists = document.querySelectorAll('ul.runners-list');
                let bestList = null;
                let maxColumns = 0;
                
                allLists.forEach(list => {
                    const legendItem = list.querySelector('li.legend');
                    if (legendItem) {
                        const columnCount = legendItem.querySelectorAll('div, span').length;
                        if (columnCount > maxColumns) {
                            maxColumns = columnCount;
                            bestList = list;
                        }
                    }
                });
                
                if (bestList) {
                    runnerItems = bestList.querySelectorAll('li.runner-item');
                }
                
                // Collect race results data
                const resultsData = {};
                const resultsTables = document.querySelectorAll('ul.runners-list:not(.bottom-list):not(:has(.betrunners-legend))');
                
                resultsTables.forEach(table => {
                    const resultItems = table.querySelectorAll('li.runner-item');
                    resultItems.forEach(item => {
                        const numberEl = item.querySelector('.rank span');
                        const placeEl = item.querySelector('.position span');
                        const timeEl = item.querySelector('.time span');
                        const reductionEl = item.querySelector('.reduction.more span');
                        
                        if (numberEl) {
                            const number = numberEl.innerText.trim();
                            resultsData[number] = {
                                place: placeEl ? placeEl.innerText.trim() : '',
                                times: ''
                            };
                            
                            if (reductionEl && reductionEl.innerText.trim() && reductionEl.innerText.trim() !== '-') {
                                resultsData[number].times = reductionEl.innerText.trim();
                            }
                            if (timeEl && timeEl.innerText.trim() && timeEl.innerText.trim() !== '-') {
                                if (resultsData[number].times) {
                                    resultsData[number].times += ' / ' + timeEl.innerText.trim();
                                } else {
                                    resultsData[number].times = timeEl.innerText.trim();
                                }
                            }
                        }
                    });
                });
                
                // Check bottom-list for DNF runners
                const bottomList = document.querySelector('ul.runners-list.bottom-list');
                if (bottomList) {
                    const bottomItems = bottomList.querySelectorAll('li.runner-item');
                    bottomItems.forEach(item => {
                        const numberEl = item.querySelector('.rank span');
                        const placeEl = item.querySelector('.position span, .position small');
                        
                        if (numberEl) {
                            const number = numberEl.innerText.trim();
                            resultsData[number] = {
                                place: placeEl ? placeEl.innerText.trim() : '-',
                                times: '-'
                            };
                        }
                    });
                }
                
                runnerItems.forEach((item, index) => {
                    let runner = {
                        race_id: race_id,
                        title: title,
                        meta: meta,
                        track: track,
                        place: '',
                        number: '',
                        horse_name: '',
                        jockey: '',
                        age_sex: '',
                        equipment: '',
                        weight: '',
                        times: '',
                        odds_morning: '',
                        odds_live: '',
                        trainer: '',
                        distance: '',
                        musique: '',
                        additional_info: ''
                    };
                    
                    // Extract finishing place
                    const positionEl = item.querySelector('.position span');
                    if (positionEl) {
                        const positionText = positionEl.innerText.trim();
                        if (positionText.includes('er') || positionText.includes('e')) {
                            runner.place = positionText;
                        } else if (positionText === 'DAI') {
                            runner.place = 'DAI';
                        }
                    }
                    
                    // Extract horse number and merge with results
                    const numberEl = item.querySelector('.rank .number, .rank span');
                    if (numberEl) {
                        runner.number = numberEl.innerText.trim();
                        
                        if (resultsData[runner.number]) {
                            runner.place = resultsData[runner.number].place;
                            if (!runner.times) {
                                runner.times = resultsData[runner.number].times;
                            }
                        }
                    }
                    
                    // Extract horse name
                    const horseEl = item.querySelector('.horse-name, .info-horse');
                    if (horseEl) {
                        runner.horse_name = horseEl.innerText.trim();
                    }
                    
                    // Extract jockey name
                    const jockeyEl = item.querySelector('.jockey-name, .info-jockey');
                    if (jockeyEl) {
                        runner.jockey = jockeyEl.innerText.trim();
                    }
                    
                    // Extract age/sex
                    const ageEl = item.querySelector('.age.more');
                    if (ageEl) {
                        runner.age_sex = ageEl.innerText.trim();
                    }
                    
                    // Extract equipment (shoes)
                    const equipmentEl = item.querySelector('.shoes.more .icon-shoes, .shoes.more span');
                    if (equipmentEl) {
                        const equipmentClass = equipmentEl.className;
                        if (equipmentClass.includes('FORE')) runner.equipment = 'FORE';
                        else if (equipmentClass.includes('HIND')) runner.equipment = 'HIND';
                        else if (equipmentClass.includes('BOTH')) runner.equipment = 'BOTH';
                        else if (equipmentEl.innerText.trim()) runner.equipment = equipmentEl.innerText.trim();
                    }
                    
                    // Extract weight
                    const weightEl = item.querySelector('.weight.more, .poids.more');
                    if (weightEl) {
                        runner.weight = weightEl.innerText.trim();
                    }
                    
                    // Extract trainer
                    const trainerEl = item.querySelector('.trainer.more');
                    if (trainerEl) {
                        runner.trainer = trainerEl.innerText.trim();
                    }
                    
                    // Extract distance
                    const distanceEl = item.querySelector('.distance.more');
                    if (distanceEl) {
                        runner.distance = distanceEl.innerText.trim();
                    }
                    
                    // Extract musique (performance history)
                    const musiqueEl = item.querySelector('.musique.more, .info-musique');
                    if (musiqueEl) {
                        runner.musique = musiqueEl.innerText.trim();
                    }
                    
                    // Extract odds - morning and live prices
                    const pricesContainer = item.querySelector('.prices');
                    if (pricesContainer) {
                        const morningPrice = pricesContainer.querySelector('.price-morning');
                        const livePrice = pricesContainer.querySelector('.price-live');
                        
                        if (morningPrice) runner.odds_morning = morningPrice.innerText.trim();
                        if (livePrice) runner.odds_live = livePrice.innerText.trim();
                        
                        if (!runner.odds_morning && !runner.odds_live) {
                            const anyPrice = pricesContainer.querySelector('span');
                            if (anyPrice) runner.odds_live = anyPrice.innerText.trim();
                        }
                    }
                    
                    // Extract times for finished races
                    const reductionEl = item.querySelector('.reduction.more span');
                    const timeEl = item.querySelector('.time span');
                    
                    if (reductionEl && reductionEl.innerText.trim() && reductionEl.innerText.trim() !== '-') {
                        runner.times = reductionEl.innerText.trim();
                    }
                    if (timeEl && timeEl.innerText.trim() && timeEl.innerText.trim() !== '-') {
                        if (runner.times) {
                            runner.times += ' / ' + timeEl.innerText.trim();
                        } else {
                            runner.times = timeEl.innerText.trim();
                        }
                    }
                    
                    // Store raw data
                    runner.additional_info = item.innerText.replace(/\\s+/g, ' ').trim();
                    
                    // Fallback parsing if core data missing
                    if (!runner.horse_name && runner.additional_info) {
                        const cleanText = runner.additional_info;
                        const numberMatch = cleanText.match(/^(\\d+)\\s+([A-Z\\s]+?)(?=[A-Z][a-z])/);
                        if (numberMatch) {
                            if (!runner.number) runner.number = numberMatch[1];
                            if (!runner.horse_name) runner.horse_name = numberMatch[2].trim();
                        }
                        
                        const ageMatch = cleanText.match(/([FHM]\\/\\d+)/);
                        if (ageMatch && !runner.age_sex) {
                            runner.age_sex = ageMatch[1];
                        }
                        
                        const distanceMatch = cleanText.match(/(\\d+m)/);
                        if (distanceMatch && !runner.distance) {
                            runner.distance = distanceMatch[1];
                        }
                        
                        const oddsMatch = cleanText.match(/([\\d.]+)(?:\\s+([\\d.]+))?\\s*$/);
                        if (oddsMatch) {
                            if (oddsMatch[2]) {
                                if (!runner.odds_morning) runner.odds_morning = oddsMatch[1];
                                if (!runner.odds_live) runner.odds_live = oddsMatch[2];
                            } else {
                                if (!runner.odds_live) runner.odds_live = oddsMatch[1];
                            }
                        }
                    }
                    
                    // Only add if we have a horse name
                    if (runner.horse_name && runner.horse_name.length > 1) {
                        runners.push(runner);
                    }
                });
                
                // Fallback text parsing if no structured data
                if (runners.length === 0) {
                    const runnerLists = document.querySelectorAll('.runners-list');
                    let consolidatedText = '';
                    
                    runnerLists.forEach(runnerList => {
                        consolidatedText += runnerList.innerText + '\\n';
                    });
                    
                    const lines = consolidatedText.split(/\\n/).filter(line => line.trim());
                    
                    lines.forEach(line => {
                        const horseMatch = line.match(/([A-Z][A-Z\\s]{2,}?)([A-Z][a-z]+(?:\\s+[A-Z][a-z]*)*)/);
                        
                        if (horseMatch) {
                            runners.push({
                                race_id: race_id,
                                title: title,
                                meta: meta,
                                track: track,
                                place: '',
                                number: '',
                                horse_name: horseMatch[1].trim(),
                                jockey: horseMatch[2].trim(),
                                age_sex: '',
                                equipment: '',
                                weight: '',
                                times: '',
                                odds_morning: '',
                                odds_live: '',
                                trainer: '',
                                distance: '',
                                musique: '',
                                additional_info: line.trim()
                            });
                        }
                    });
                }

                // Remove duplicates
                const uniqueRunners = [];
                const seenHorses = new Set();
                
                runners.forEach(runner => {
                    if (runner.horse_name && !seenHorses.has(runner.horse_name)) {
                        seenHorses.add(runner.horse_name);
                        uniqueRunners.push(runner);
                    }
                });

                return {
                    race_info: {
                        race_id: race_id,
                        title: title,
                        meta: meta,
                        track: track,
                        url: window.location.href
                    },
                    runners: uniqueRunners,
                    scraped_at: new Date().toISOString()
                };
            }
            """
            
            try:
                race_data = page.evaluate(js)
                log.info("✔ Runners extraction returned %d runners", len(race_data.get('runners', [])))
            except PlaywrightTimeoutError:
                log.error("⏰ Timeout running runners extraction on race %d", race_id)
                race_data = None

            prediction_response = None
            prediction_request = None
            forwarded_payload = None
            timestamp = datetime.utcnow().isoformat() + "Z"

            if race_data and race_data.get('runners'):
                try:
                    # Store the request data that we're about to send
                    prediction_request = race_data
                    
                    # Extract the scraped race_id for CSV logging
                    scraped_race_id = race_data.get('race_info', {}).get('race_id', str(race_id))
                    
                    # Save prediction request to CSV (existing functionality)
                    save_to_csv(scraped_race_id, "prediction_request", prediction_request, timestamp)
                    
                    # NEW: Save individual runners to structured CSV
                    save_runners_to_csv(race_data, timestamp)
                    
                    # Send structured JSON data instead of HTML
                    headers = {"Content-Type": "application/json"}
                    resp = httpx.post(PREDICT_URL, json=race_data, headers=headers, timeout=10.0)
                    resp.raise_for_status()
                    prediction_response = resp.text
                    log.info("📬 Sent race %d to prediction server (status %d)", race_id, resp.status_code)

                    # Parse response and save to CSV
                    parsed = json.loads(prediction_response)
                    save_to_csv(scraped_race_id, "prediction_response", parsed, timestamp)

                    recommendations = parsed.get("recommendations", [])
                    
                    # Process recommendations to match betting server format
                    for r in recommendations:
                        # Add race_id
                        r["race_id"] = race.unibet_id
                        
                        # Rename bet_amount to bet_percentage
                        if "bet_amount" in r:
                            r["bet_percentage"] = r.pop("bet_amount")
                        
                        # Find horse_number from scraped data by matching horse_name
                        horse_name = r.get("horse_name", "")
                        horse_number = None
                        
                        # Look up horse number from the scraped runners data
                        for runner in race_data.get("runners", []):
                            if runner.get("horse_name", "").strip().upper() == horse_name.strip().upper():
                                horse_number = runner.get("number", "")
                                break
                        
                        if horse_number:
                            r["horse_number"] = int(horse_number) if horse_number.isdigit() else horse_number
                        else:
                            log.warning("⚠️ Could not find horse_number for '%s'", horse_name)
                        
                        # Remove extra fields that betting server doesn't need
                        fields_to_remove = ["confidence", "edge", "estimated_place_odds", "kelly_fraction", "strategy", "win_odds"]
                        for field in fields_to_remove:
                            r.pop(field, None)

                    summary = parsed.get("summary", {})
                    summary.setdefault("boulot_bets", 0)
                    
                    # Count bet types for the summary
                    win_bets = len([r for r in recommendations if r.get("bet_type") == "win"])
                    place_bets = len([r for r in recommendations if r.get("bet_type") == "place"])
                    deuzio_bets = len([r for r in recommendations if r.get("bet_type") == "deuzio"])
                    
                    # Update summary with required fields
                    summary.update({
                        "total_bet_amount": summary.get("total_amount", 0),  # Map total_amount to total_bet_amount
                        "win_bets": win_bets,
                        "place_bets": place_bets, 
                        "deuzio_bets": deuzio_bets,
                        "timestamp": timestamp
                    })

                    forwarded_payload = {
                        "race_url": url,
                        "recommendations": recommendations,
                        "summary": summary,
                    }

                    # Save betting request to CSV
                    save_to_csv(scraped_race_id, "betting_request", forwarded_payload, timestamp)

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
                        bookmarklet_json=race_data,
                        prediction_request=prediction_request,
                        prediction_response=prediction_response,
                        betting_request=forwarded_payload,
                        race_url=url,
                    )
                    sess.add(race_detail)
                    sess.commit()
                log.info("✅ Saved RaceDetail for race %d", race_id)
            else:
                log.error("❌ No runner data extracted for race %d", race_id)

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

def schedule_race_scrape(race: Race):
    """Schedule a race scrape job - imported by daily.py"""
    from app.scrapers.daily import schedule_race
    schedule_race(race)

if __name__ == "__main__":
    import argparse

    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    parser = argparse.ArgumentParser(description="Scrape one race detail by DB ID")
    parser.add_argument("race_id", type=int, help="Race.id to scrape")
    args = parser.parse_args()

    _scrape_sync(args.race_id)