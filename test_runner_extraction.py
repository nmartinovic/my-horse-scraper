#!/usr/bin/env python3
"""
Test script to extract runner data from a specific Unibet race URL
and export to CSV for verification.
"""

import asyncio
import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

TEST_URL = "https://www.unibet.fr/turf/race/02-06-2025-R2-C5-cholet-prix-des-capucines.html"

async def extract_runners_data(url: str):
    """Extract runner data using the runners.js logic"""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(ignore_https_errors=True)
        
        try:
            logger.info(f"Navigating to: {url}")
            await page.goto(url, wait_until="networkidle", timeout=60000)
            
            # Try to click "Tableau des partants" if it exists
            try:
                tableau_button = page.locator("span.text:has-text('Tableau des partants')")
                if await tableau_button.count() > 0:
                    await tableau_button.click()
                    await page.wait_for_timeout(2000)
                    logger.info("Clicked 'Tableau des partants'")
            except Exception as e:
                logger.warning(f"Could not click 'Tableau des partants': {e}")
            
            logger.info("Running extraction JavaScript...")
            
            # The enhanced JavaScript extraction logic
            js_code = """
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
                
                console.log(`Race ID: ${race_id}`);
                console.log(`Title: ${title}`);
                console.log(`Track: ${track}`);
                
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
                        console.log(`Found list with ${columnCount} columns`);
                        if (columnCount > maxColumns) {
                            maxColumns = columnCount;
                            bestList = list;
                        }
                    }
                });
                
                if (bestList) {
                    runnerItems = bestList.querySelectorAll('li.runner-item');
                    console.log(`Using best list with ${maxColumns} columns, found ${runnerItems.length} runners`);
                } else {
                    console.log(`Using default selection, found ${runnerItems.length} runners`);
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
                
                console.log(`Found results data for ${Object.keys(resultsData).length} horses`);
                
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
                    
                    console.log(`Processed runner ${index + 1}: ${runner.horse_name} (#${runner.number})`);
                    
                    // Only add if we have a horse name
                    if (runner.horse_name && runner.horse_name.length > 1) {
                        runners.push(runner);
                    }
                });
                
                // Fallback text parsing if no structured data
                if (runners.length === 0) {
                    console.log("No structured runners found, trying fallback text parsing");
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

                console.log(`Final result: ${uniqueRunners.length} unique runners`);

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
            
            # Execute the extraction
            result = await page.evaluate(js_code)
            logger.info(f"Extraction completed. Found {len(result.get('runners', []))} runners")
            
            return result
            
        except PlaywrightTimeoutError:
            logger.error("Timeout occurred while loading the page")
            return None
        except Exception as e:
            logger.error(f"Error during extraction: {e}")
            return None
        finally:
            await browser.close()

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
            
        # Fixed logging - use %s for both string and int race_id
        log.info("Saved %s for race %s to CSV", data_type, race_id)
        
    except Exception as e:
        log.error("Failed to save to CSV: %s", e)

def save_to_json(data: dict, output_path: Path):
    """Save extracted data to JSON file for debugging"""
    
    with open(output_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, indent=2, ensure_ascii=False)
    
    logger.info(f"Raw data saved to {output_path}")

async def main():
    """Main function to run the test"""
    
    logger.info("Starting runner extraction test...")
    logger.info(f"Target URL: {TEST_URL}")
    
    # Create output directory
    output_dir = Path("test_output")
    output_dir.mkdir(exist_ok=True)
    
    # Generate timestamp for unique filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Extract data
    data = await extract_runners_data(TEST_URL)
    
    if data:
        # Save to CSV
        csv_path = output_dir / f"runners_test_{timestamp}.csv"
        save_to_csv(data, csv_path)
        
        # Save to JSON for debugging
        json_path = output_dir / f"runners_test_{timestamp}.json"
        save_to_json(data, json_path)
        
        # Print summary
        runners = data.get('runners', [])
        race_info = data.get('race_info', {})
        
        print(f"\n{'='*60}")
        print(f"EXTRACTION SUMMARY")
        print(f"{'='*60}")
        print(f"Race: {race_info.get('title', 'Unknown')}")
        print(f"Track: {race_info.get('track', 'Unknown')}")
        print(f"Race ID: {race_info.get('race_id', 'Unknown')}")
        print(f"Runners found: {len(runners)}")
        print(f"CSV saved to: {csv_path}")
        print(f"JSON saved to: {json_path}")
        
        if runners:
            print(f"\nFirst few runners:")
            for i, runner in enumerate(runners[:5]):
                print(f"  {i+1}. #{runner.get('number', '?')} {runner.get('horse_name', 'Unknown')} - {runner.get('jockey', 'Unknown jockey')}")
        
        print(f"\n{'='*60}")
        
    else:
        logger.error("Failed to extract data")

if __name__ == "__main__":
    asyncio.run(main())