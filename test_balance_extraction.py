#!/usr/bin/env python3
"""
Test script to verify balance extraction and bankroll calculation
Usage: python test_balance_extraction.py
"""

import sys
import asyncio
import logging
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Configure logging
logging.basicConfig(
    format="%(asctime)s %(levelname)7s %(name)s │ %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

def extract_balance_from_page(page) -> float:
    """Extract current account balance from the page."""
    try:
        # Try to find the balance element
        balance_element = page.locator('span.balance-real-value').first
        if balance_element.count() > 0:
            balance_text = balance_element.text_content()
            log.info(f"Found balance text: '{balance_text}'")
            
            # Parse the balance - handle formats like "145,70 €" or "145.70 €"
            # Remove currency symbols and spaces
            cleaned = balance_text.replace('€', '').replace(' ', '').strip()
            
            # Handle both comma and dot as decimal separators
            if ',' in cleaned:
                cleaned = cleaned.replace(',', '.')
            
            balance = float(cleaned)
            log.info(f"Parsed balance: {balance}")
            return balance
        else:
            log.warning("Balance element not found on page")
            return 50.0  # Default fallback
            
    except Exception as e:
        log.error(f"Error extracting balance: {e}")
        return 50.0  # Default fallback

def get_bankroll_from_balance(balance: float) -> int:
    """Calculate bankroll as half the balance, rounded."""
    bankroll = round(balance / 2)
    # Ensure minimum bankroll of 1
    return max(bankroll, 1)

def test_connectivity():
    """Test basic connectivity to Unibet."""
    import requests
    
    log.info("Testing basic connectivity to Unibet...")
    
    test_urls = [
        "https://www.unibet.fr",
        "https://www.google.com",  # Basic connectivity test
    ]
    
    for url in test_urls:
        try:
            response = requests.get(url, timeout=10)
            log.info(f"✓ {url} - Status: {response.status_code}")
        except Exception as e:
            log.error(f"✗ {url} - Error: {e}")

def test_balance_extraction():
    """Test the balance extraction on the live race URL."""
    
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    # First test basic connectivity
    test_connectivity()
    
    url = "https://www.unibet.fr/turf/race/08-06-2025-R2-C1-sha-tin-prix-chak-on-handicap.html"
    log.info(f"Testing balance extraction on: {url}")
    
    try:
        with sync_playwright() as p:
            log.info("Launching browser (headless=False so you can see what's happening)")
            
            # Launch with additional args that might help with connectivity
            browser = p.chromium.launch(
                headless=False,
                args=[
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--no-sandbox',
                    '--disable-dev-shm-usage'
                ]
            )
            
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            page = context.new_page()
            
            # Try to go to the main Unibet page first
            log.info("First trying to navigate to main Unibet page...")
            try:
                page.goto("https://www.unibet.fr", wait_until="networkidle", timeout=30_000)
                log.info("✓ Successfully loaded main Unibet page")
                
                # Wait a moment and then try the race page
                page.wait_for_timeout(2000)
                log.info("Now navigating to the race page...")
                page.goto(url, wait_until="networkidle", timeout=60_000)
                
            except Exception as e:
                log.error(f"Failed to load main page, trying race page directly: {e}")
                page.goto(url, wait_until="networkidle", timeout=60_000)
            
            # Wait a moment for the page to fully load
            page.wait_for_timeout(3000)
            
            log.info("Looking for balance element...")
            
            # First, let's see what balance-related elements exist on the page
            balance_elements = page.locator('*[class*="balance"]').all()
            log.info(f"Found {len(balance_elements)} elements with 'balance' in class name")
            
            for i, elem in enumerate(balance_elements):
                try:
                    class_name = elem.get_attribute('class')
                    text_content = elem.text_content()
                    log.info(f"Balance element {i}: class='{class_name}', text='{text_content}'")
                except:
                    log.info(f"Balance element {i}: Could not read content")
            
            # Also look for any text that might contain currency symbols
            log.info("Looking for any elements containing '€' symbol...")
            euro_elements = page.locator('text=€').all()
            log.info(f"Found {len(euro_elements)} elements containing '€'")
            
            for i, elem in enumerate(euro_elements[:10]):  # Limit to first 10
                try:
                    text_content = elem.text_content()
                    log.info(f"Euro element {i}: text='{text_content}'")
                except:
                    log.info(f"Euro element {i}: Could not read content")
            
            # Now try our specific selector
            current_balance = extract_balance_from_page(page)
            bankroll = get_bankroll_from_balance(current_balance)
            
            log.info("=" * 50)
            log.info(f"RESULTS:")
            log.info(f"Current balance: {current_balance}")
            log.info(f"Calculated bankroll: {bankroll}")
            log.info(f"Predict URL would be: http://localhost:8080/predict?bankroll={bankroll}")
            log.info("=" * 50)
            
            # Keep browser open for a moment so you can inspect
            log.info("Keeping browser open for 15 seconds so you can inspect...")
            log.info("Check if you need to log in or if there are any popups to dismiss")
            page.wait_for_timeout(15000)
            
            browser.close()
            
    except Exception as e:
        log.exception(f"Error during test: {e}")
        log.info("Possible solutions:")
        log.info("1. Check if you're behind a corporate firewall")
        log.info("2. Try connecting via VPN if Unibet is geo-blocked")
        log.info("3. Check if you need to be logged into Unibet first")
        log.info("4. The race URL might be incorrect or expired")

def test_balance_parsing():
    """Test the balance parsing logic with different formats."""
    log.info("Testing balance parsing with different formats:")
    
    test_cases = [
        "145,70 €",
        "145.70 €", 
        "1 234,56 €",
        "1,234.56 €",
        "50€",
        "0,00 €",
        "999,99 €"
    ]
    
    for test_case in test_cases:
        try:
            # Simulate the parsing logic
            cleaned = test_case.replace('€', '').replace(' ', '').strip()
            if ',' in cleaned and '.' in cleaned:
                # Handle case like "1,234.56" (US format)
                if cleaned.index(',') < cleaned.index('.'):
                    cleaned = cleaned.replace(',', '')
                else:
                    # Handle case like "1.234,56" (EU format) 
                    cleaned = cleaned.replace('.', '').replace(',', '.')
            elif ',' in cleaned:
                cleaned = cleaned.replace(',', '.')
            
            balance = float(cleaned)
            bankroll = get_bankroll_from_balance(balance)
            log.info(f"'{test_case}' -> balance: {balance}, bankroll: {bankroll}")
        except Exception as e:
            log.error(f"Failed to parse '{test_case}': {e}")

if __name__ == "__main__":
    print("Testing balance parsing logic:")
    test_balance_parsing()
    
    print("\nTesting live balance extraction (this will open a browser):")
    input("Press Enter to continue with live test, or Ctrl+C to cancel...")
    test_balance_extraction()