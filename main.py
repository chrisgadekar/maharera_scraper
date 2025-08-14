import asyncio
import logging
import os
import pandas as pd
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page
from modules.captcha_solver import CaptchaSolver
from modules.data_extracter import DataExtracter
from typing import Set

# --- Configuration ---
# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MahaReraScraper")

# --- File Configuration ---
OUTPUT_FILENAME = "maharera_complete_data.csv"
FAILED_PROJECTS_FILENAME = "maharera_failed_projects.csv"

# --- Scraping Configuration ---
START_ID = 189
END_ID = 190 # You can set this to your desired end range
BASE_URL = "https://maharerait.maharashtra.gov.in/public/project/view/"
MAX_PARALLEL_BROWSERS = 8 # Number of browsers to run in parallel

# --- Column order for the final CSV ---
DESIRED_ORDER = [
    "project_id", "registration_number", "date_of_registration", "project_name",
    "project_type", "project_location", "proposed_completion_date",
    "extension_date", "project_status", "planning_authority",
    "full_name_of_planning_authority", "final_plot_bearing",
    "total_land_area", "land_area_applied", "permissible_builtup",
    "sanctioned_builtup", "aggregate_open_space", "CC/NA Order Issued to",
    "CC/NA Order in the name of", "project_address_state_ut",
    "project_address_district", "project_address_taluka",
    "project_address_village", "project_address_pin_code", "promoter_details",
    "promoter_official_communication_address_state_ut",
    "promoter_official_communication_address_district",
    "promoter_official_communication_address_taluka",
    "promoter_official_communication_address_village",
    "promoter_official_communication_address_pin_code", "partner_name",
    "partner_designation", "promoter_past_project_names",
    "promoter_past_project_statuses", "promoter_past_litigation_statuses",
    "authorised_signatory_names", "authorised_signatory_designations","spa_name","spa_designation",
    "architect_names", "engineer_names", "chartered_accountant_names","other_professional_names",
    "sro_name", "sro_document_name", "latest_form1_date", "latest_form2_date","latest_form5_date","has_occupancy_certificate",
    "promoter_is_landowner", "has_other_landowners", "landowner_names",
    "landowner_types", "landowner_share_types", "building_identification_plan",
    "wing_identification_plan", "sanctioned_floors",
    "sanctioned_habitable_floors", "sanctioned_apartments",
    "cc_issued_floors", "view_document_available",
    "summary_identification_building_wing", "summary_identification_wing_plan",
    "summary_floor_type", "summary_total_no_of_residential_apartments",
    "summary_total_no_of_non_residential_apartments",
    "summary_total_no_of_apartments_nr_r", "summary_total_no_of_sold_units",
    "summary_total_no_of_unsold_units", "summary_total_no_of_booked",
    "summary_total_no_of_rehab_units", "summary_total_no_of_mortgage",
    "summary_total_no_of_reservation",
    "summary_total_no_of_land_owner_investor_share_sale",
    "summary_total_no_of_land_owner_investor_share_not_for_sale",
    "total_no_of_apartments", "are_there_investors_other_than_promoter",
    "litigation_against_project_count", "open_space_parking_total",
    "closed_space_parking_total", "bank_name", "ifsc_code", "bank_address",
    "complaint_count", "complaint_numbers", "real_estate_agent_names",
    "maharera_certificate_nos"
]

# --- Thread-safe file writing locks ---
csv_lock = asyncio.Lock()
failed_csv_lock = asyncio.Lock()

async def save_record(data: dict):
    """Appends a single successful record to the main CSV file in a thread-safe manner."""
    async with csv_lock:
        try:
            df = pd.json_normalize([data])
            df = df.reindex(columns=DESIRED_ORDER)
            file_exists = os.path.exists(OUTPUT_FILENAME)
            df.to_csv(OUTPUT_FILENAME, mode='a', index=False, header=not file_exists)
        except Exception as e:
            logger.error(f"Failed to save record for {data.get('project_id')}: {e}")

async def log_failed_project(project_id: int, url: str):
    """Appends a single failed project to the failure CSV file in a thread-safe manner."""
    async with failed_csv_lock:
        try:
            file_exists = os.path.exists(FAILED_PROJECTS_FILENAME)
            with open(FAILED_PROJECTS_FILENAME, 'a', newline='', encoding='utf-8') as f:
                if not file_exists:
                    f.write("project_id,url\n")
                f.write(f"{project_id},{url}\n")
        except Exception as e:
            logger.error(f"Failed to log failed project {project_id}: {e}")

def get_processed_ids() -> Set[int]:
    """Reads both success and failure CSVs to get a set of all IDs that have been attempted."""
    processed_ids = set()
    for filename in [OUTPUT_FILENAME, FAILED_PROJECTS_FILENAME]:
        if os.path.exists(filename):
            try:
                df = pd.read_csv(filename, usecols=['project_id'], on_bad_lines='skip')
                processed_ids.update(df['project_id'].dropna().astype(int).tolist())
            except (ValueError, KeyError, FileNotFoundError) as e:
                logger.warning(f"Could not read project_id column from {filename}. It might be empty or malformed. Error: {e}")
    return processed_ids

async def process_single_project(page: Page, captcha_solver: CaptchaSolver, data_extracter: DataExtracter, project_id: int, url: str):
    """
    Handles the logic for a single project: navigation, captcha solving (1 try), and data extraction.
    """
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=60000)

        success = await captcha_solver.solve_and_fill(
            page=page,
            captcha_selector="canvas#captcahCanvas",
            input_selector="input[name='captcha']",
            submit_selector="button.btn.btn-primary.next",
            reg_no=str(project_id)
        )

        if not success:
            logger.warning(f"CAPTCHA FAILED for project {project_id}. Logging and moving on.")
            await log_failed_project(project_id, url)
            return

        await page.wait_for_load_state('networkidle', timeout=30000)
        await page.wait_for_timeout(1500)

        data = await data_extracter.extract_project_details(page, str(project_id))

        if data:
            data["project_id"] = project_id
            await save_record(data)
            logger.info(f"✅ SUCCESS: Saved data for project {project_id}")
        else:
            logger.warning(f"Data extraction returned None for project {project_id}. Logging as failed.")
            await log_failed_project(project_id, url)

    except Exception as e:
        logger.error(f"FATAL ERROR processing project {project_id}: {e}", exc_info=False)
        await log_failed_project(project_id, url)


async def worker(playwright: Playwright, project_queue: asyncio.Queue, captcha_solver: CaptchaSolver, data_extracter: DataExtracter):
    """
    A worker that processes projects from a queue. Each worker has its own browser instance.
    This function is now more robust against crashes and cancellation.
    """
    browser: Optional[Browser] = None
    try:
        browser = await playwright.firefox.launch(headless=True)
        context: BrowserContext = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
        )
        page: Page = await context.new_page()

        await page.route("**/*", lambda route: route.abort()
            if route.request.resource_type in ["image", "stylesheet", "font", "media"]
            else route.continue_())

        while True: # A robust loop that is explicitly broken by cancellation.
            project_id = await project_queue.get()
            url = f"{BASE_URL}{project_id}"
            logger.info(f"Worker processing project ID: {project_id}")
            await process_single_project(page, captcha_solver, data_extracter, project_id, url)
            project_queue.task_done()

    except asyncio.CancelledError:
        logger.info("Worker received cancellation request.")
    except Exception as e:
        logger.error(f"UNHANDLED EXCEPTION IN WORKER: {e}", exc_info=True)
    finally:
        if browser:
            logger.info("Worker closing browser.")
            await browser.close()


async def main():
    """Main function to set up the scraping environment and run workers in parallel."""
    logger.info("--- Starting MahaRERA Scraper ---")
    processed_ids = get_processed_ids()
    logger.info(f"Found {len(processed_ids)} previously attempted projects. They will be skipped.")

    project_queue = asyncio.Queue()
    for i in range(START_ID, END_ID + 1):
        if i not in processed_ids:
            await project_queue.put(i)

    total_to_process = project_queue.qsize()
    if total_to_process == 0:
        logger.info("No new projects to process. Exiting.")
        return

    logger.info(f"Queued {total_to_process} new projects for scraping.")
    captcha_solver = CaptchaSolver()
    data_extracter = DataExtracter()

    async with async_playwright() as p:
        tasks = [
            asyncio.create_task(worker(p, project_queue, captcha_solver, data_extracter))
            for _ in range(MAX_PARALLEL_BROWSERS)
        ]
        # Wait for all projects in the queue to be processed.
        await project_queue.join()

        # Gracefully cancel and await worker tasks to ensure clean shutdown.
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("--- SCRAPING RUN COMPLETE ---")
    logger.info(f"Successful data saved to: {OUTPUT_FILENAME}")
    logger.info(f"Failed/Skipped projects logged in: {FAILED_PROJECTS_FILENAME}")


if __name__ == "__main__":
    asyncio.run(main())
