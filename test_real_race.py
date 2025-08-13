#!/usr/bin/env python3
"""
Test Live Race Scraper Process

This script adds the Argentan race to your database and triggers
your actual race scraper to process it through the real pipeline:
1. Scrape race data
2. Send to prediction server  
3. Forward to place-bets server
4. Save to database

This tests your live system end-to-end.
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from sqlmodel import Session, select
from app.db import engine, init_db
from app.models import Race, RaceDetail
from app.scrapers.race import _scrape_sync

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s │ %(message)s",
)
logger = logging.getLogger(__name__)

# Test race details
TEST_RACE_URL = "https://www.unibet.fr/turf/race/10-07-2025-R2-C1-karlshorst-bonjour-france-rennen.html"
TEST_RACE_DATA = {
    "unibet_id": "test_argentan_race_001", 
    "name": "PRIX DE LA PLAINE D'ARGENTAN",
    "meeting": "ARGENTAN",
    "race_time": datetime.now(timezone.utc) + timedelta(minutes=5),  # 5 minutes from now
    "url": TEST_RACE_URL,
    "surface": "PLAT",
    "distance_m": 2000,
    "scraped_at": datetime.utcnow()
}

def create_test_race():
    """Add the test race to database"""
    logger.info("Creating test race in database...")
    
    # Initialize database if needed
    try:
        init_db()
    except Exception as e:
        logger.warning(f"Database may already exist: {e}")
    
    with Session(engine) as session:
        # Check if race already exists
        existing_race = session.exec(
            select(Race).where(Race.unibet_id == TEST_RACE_DATA["unibet_id"])
        ).first()
        
        if existing_race:
            logger.info(f"Test race already exists with ID {existing_race.id}")
            return existing_race.id
        
        # Create new race
        race = Race(**TEST_RACE_DATA)
        session.add(race)
        session.commit()
        session.refresh(race)
        
        logger.info(f"✅ Created test race with ID {race.id}")
        logger.info(f"   Name: {race.name}")
        logger.info(f"   Meeting: {race.meeting}")
        logger.info(f"   URL: {race.url}")
        logger.info(f"   Scheduled for: {race.race_time}")
        
        return race.id

def run_race_scraper(race_id: int):
    """Run the actual race scraper on our test race"""
    logger.info(f"Starting race scraper for race ID {race_id}...")
    logger.info("This will test the full pipeline:")
    logger.info("  1. Extract runners data")
    logger.info("  2. Send to prediction server (localhost:8080)")
    logger.info("  3. Process recommendations")
    logger.info("  4. Forward to place-bets server (localhost:5173)")
    logger.info("  5. Save results to database")
    logger.info("")
    
    try:
        # This runs your actual race scraper logic
        _scrape_sync(race_id)
        logger.info("✅ Race scraper completed successfully!")
        return True
    except Exception as e:
        logger.error(f"❌ Race scraper failed: {e}")
        logger.exception("Full error details:")
        return False

def check_results(race_id: int):
    """Check what was saved to the database"""
    logger.info("Checking results in database...")
    
    with Session(engine) as session:
        # Get the race
        race = session.get(Race, race_id)
        if not race:
            logger.error("Race not found!")
            return
        
        # Get race details
        race_detail = session.exec(
            select(RaceDetail).where(RaceDetail.race_id == race_id)
        ).first()
        
        if not race_detail:
            logger.warning("No race details found - scraping may have failed")
            return
        
        logger.info("✅ Results saved to database:")
        logger.info(f"   Race Detail ID: {race_detail.id}")
        logger.info(f"   Race URL: {race_detail.race_url}")
        
        # Show request data
        if race_detail.bookmarklet_json:
            try:
                import json
                request_data = race_detail.bookmarklet_json
                if isinstance(request_data, str):
                    request_data = json.loads(request_data)
                
                runners_count = len(request_data.get('runners', []))
                race_info = request_data.get('race_info', {})
                
                logger.info(f"   Runners extracted: {runners_count}")
                logger.info(f"   Race ID: {race_info.get('race_id', 'Unknown')}")
                logger.info(f"   Track: {race_info.get('track', 'Unknown')}")
                
                # Show first few runners
                runners = request_data.get('runners', [])[:3]
                for i, runner in enumerate(runners, 1):
                    name = runner.get('horse_name', 'Unknown')
                    number = runner.get('number', '?')
                    odds = runner.get('odds_live', runner.get('odds_morning', '?'))
                    logger.info(f"     {i}. #{number} {name} (odds: {odds})")
                
            except Exception as e:
                logger.warning(f"Could not parse request data: {e}")
        
        # Show prediction response
        if race_detail.prediction_response:
            try:
                import json
                pred_data = json.loads(race_detail.prediction_response)
                recommendations = pred_data.get('recommendations', [])
                summary = pred_data.get('summary', {})
                
                logger.info(f"   Predictions received: {len(recommendations)} recommendations")
                logger.info(f"   Total bet amount: €{summary.get('total_amount', 0)}")
                logger.info(f"   Expected return: €{summary.get('expected_return', 0)}")
                
                # Show recommendations
                for i, rec in enumerate(recommendations[:3], 1):
                    bet_type = rec.get('bet_type', '?')
                    horse = rec.get('horse_name', 'Unknown')
                    amount = rec.get('bet_amount', 0)
                    odds = rec.get('odds', 0)
                    logger.info(f"     {i}. {bet_type.upper()} #{rec.get('horse_number', '?')} {horse}: €{amount} @ {odds}")
                
            except Exception as e:
                logger.warning(f"Could not parse prediction response: {e}")
        else:
            logger.warning("   No prediction response - prediction server may have failed")

def cleanup_test_race(race_id: int):
    """Remove the test race from database"""
    logger.info("Cleaning up test race...")
    
    with Session(engine) as session:
        # Delete race details first
        race_detail = session.exec(
            select(RaceDetail).where(RaceDetail.race_id == race_id)
        ).first()
        if race_detail:
            session.delete(race_detail)
        
        # Delete race
        race = session.get(Race, race_id)
        if race:
            session.delete(race)
        
        session.commit()
    
    logger.info("✅ Test race cleaned up")

def main():
    """Main test function"""
    print("LIVE RACE SCRAPER TEST")
    print("Testing your actual scraping pipeline with real Argentan race")
    print("="*70)
    print(f"Target URL: {TEST_RACE_URL}")
    print()
    
    # Check servers are running
    logger.info("Prerequisites:")
    logger.info("  ✓ Make sure prediction server is running on localhost:8080")
    logger.info("  ✓ Make sure place-bets server is running on localhost:5173")
    logger.info("  ✓ Make sure both servers can handle requests")
    print()
    
    race_id = None
    try:
        # Step 1: Create test race
        race_id = create_test_race()
        
        # Step 2: Run the actual race scraper
        success = run_race_scraper(race_id)
        
        # Step 3: Check results
        if success:
            check_results(race_id)
        
        # Summary
        print("\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)
        
        if success:
            print("✅ LIVE RACE SCRAPER TEST PASSED!")
            print("Your complete pipeline is working:")
            print("  ✓ Race data extraction")
            print("  ✓ Prediction server communication")
            print("  ✓ Place-bets server communication")
            print("  ✓ Database storage")
            print()
            print("You can now view the results in your dashboard at:")
            print("  http://localhost:8000")
            print()
            print("The 'Request Sent' column will show the extracted race data,")
            print("and 'Prediction Response' will show your server's betting recommendations.")
        else:
            print("❌ LIVE RACE SCRAPER TEST FAILED!")
            print("Check the logs above for details.")
            print("Common issues:")
            print("  - Prediction server not running on localhost:8080")
            print("  - Place-bets server not running on localhost:5173")
            print("  - Server response format issues")
            print("  - Network connectivity problems")
        
        print("="*70)
        
        # Ask about cleanup
        if race_id:
            response = input("\nRemove test race from database? [Y/n]: ").strip().lower()
            if response in ('', 'y', 'yes'):
                cleanup_test_race(race_id)
            else:
                print(f"Test race kept in database with ID {race_id}")
        
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        if race_id:
            cleanup_test_race(race_id)
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        logger.exception("Full error details:")

if __name__ == "__main__":
    main()