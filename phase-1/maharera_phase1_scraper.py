#!/usr/bin/env python3
"""
Phase 1: MahaRERA Project List Scraper (Parallel with retries + failed pages handling)
"""

import asyncio
import logging
import pandas as pd
import os
import re
from playwright.async_api import async_playwright

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('phase1_scraper.log', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Config ---
BASE_URL = "https://maharera.maharashtra.gov.in/projects-search-result"
OUTPUT_CSV = "phase1_preview_links.csv"
RESUME_FILE = "last_page.txt"
FAILED_FILE = "failed_pages.txt"
MAX_PAGES = None         # 🔥 For test, run first 12 pages
PARALLEL_WINDOWS = 6   # 6 browsers at once
PAGES_PER_WINDOW = 2   # Each window scrapes 2 pages sequentially
PAGE_LOAD_TIMEOUT = 30000     # 30s
SELECTOR_TIMEOUT = 10000      # 10s
MAX_RETRIES = 2

# ---------------------- SCRAPER FUNCTIONS ---------------------- #

async def scrape_page(page, page_num):
    """Scrape a single page and return all 10 records."""
    logger.info(f"Scraping page {page_num}...")
    await page.goto(f"{BASE_URL}?page={page_num}", wait_until='domcontentloaded', timeout=PAGE_LOAD_TIMEOUT)
    await page.wait_for_selector("p.p-0", timeout=SELECTOR_TIMEOUT)

    # Extract registration numbers
    reg_numbers = await page.locator("p.p-0").all_inner_texts()
    reg_numbers = [r.replace("#", "").strip() for r in reg_numbers]

    # Extract View Details links
    view_links_all = await page.locator("a.viewLink").evaluate_all("els => els.map(e => e.href)")
    valid_links = [link for link in view_links_all if re.match(r'^https://maharerait\.maharashtra\.gov\.in/public/project/view/\d+$', link)]
    view_links = valid_links[:len(reg_numbers)]

    if len(reg_numbers) != len(view_links):
        logger.warning(f"⚠ Mismatch: {len(reg_numbers)} reg numbers vs {len(view_links)} links on page {page_num}")

    return [{"reg_no": reg, "view_link": link} for reg, link in zip(reg_numbers, view_links)]

async def scrape_window(playwright, start_page, end_page):
    """Scrape 2 pages sequentially in one browser window with retries."""
    browser = await playwright.firefox.launch(headless=True)
    context = await browser.new_context(viewport={'width': 1366, 'height': 768})
    page = await context.new_page()

    window_data, failed_pages = [], []

    for page_num in range(start_page, end_page + 1):
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                data = await scrape_page(page, page_num)
                window_data.extend(data)
                success = True
                break
            except Exception as e:
                logger.error(f"❌ Error on page {page_num} (Attempt {attempt}): {e}")
                if attempt == MAX_RETRIES:
                    failed_pages.append(page_num)

    await browser.close()
    return window_data, failed_pages

async def retry_failed_pages(failed_pages):
    """Retry all failed pages in sequence (one browser)."""
    if not failed_pages:
        return []

    logger.info(f"🔄 Retrying {len(failed_pages)} failed pages...")
    data = []
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        for page_num in failed_pages:
            try:
                page_data = await scrape_page(page, page_num)
                data.extend(page_data)
            except Exception as e:
                logger.error(f"🚫 Still failed on retry page {page_num}: {e}")

        await browser.close()
    return data

# ---------------------- MAIN FUNCTION ---------------------- #

async def main():
    # Resume state
    start_page = 1
    if os.path.exists(RESUME_FILE):
        with open(RESUME_FILE, "r") as f:
            start_page = int(f.read().strip()) + 1
        logger.info(f"Resuming from page {start_page}")

    all_data = []
    if os.path.exists(OUTPUT_CSV):
        all_data = pd.read_csv(OUTPUT_CSV).to_dict('records')

    async with async_playwright() as p:
        total_pages = MAX_PAGES if MAX_PAGES else 4305

        for batch_start in range(start_page, total_pages + 1, PARALLEL_WINDOWS * PAGES_PER_WINDOW):
            tasks, batch_failed = [], []

            for i in range(PARALLEL_WINDOWS):
                win_start = batch_start + i * PAGES_PER_WINDOW
                win_end = min(win_start + PAGES_PER_WINDOW - 1, total_pages)
                if win_start <= total_pages:
                    tasks.append(scrape_window(p, win_start, win_end))

            logger.info(f"🚀 Running batch: Pages {batch_start} to {min(batch_start + PARALLEL_WINDOWS * PAGES_PER_WINDOW - 1, total_pages)}")

            results = await asyncio.gather(*tasks)
            for window_data, failed in results:
                all_data.extend(window_data)
                batch_failed.extend(failed)

            # Save CSV + failed pages + resume state
            pd.DataFrame(all_data).to_csv(OUTPUT_CSV, index=False)
            logger.info(f"💾 Progress saved to {OUTPUT_CSV}")

            with open(FAILED_FILE, "a") as f:
                for fp in batch_failed:
                    f.write(str(fp) + "\n")

            last_done = min(batch_start + PARALLEL_WINDOWS * PAGES_PER_WINDOW - 1, total_pages)
            with open(RESUME_FILE, "w") as f:
                f.write(str(last_done))

        # Retry failed pages at the end
        if os.path.exists(FAILED_FILE):
            with open(FAILED_FILE, "r") as f:
                failed_pages = sorted(set(int(x.strip()) for x in f if x.strip()))
            retry_data = await retry_failed_pages(failed_pages)
            all_data.extend(retry_data)
            pd.DataFrame(all_data).to_csv(OUTPUT_CSV, index=False)
            logger.info("✅ Final retry complete. Data saved.")

    logger.info("🎉 Scraping (with retries) complete!")

if __name__ == "__main__":
    asyncio.run(main())
