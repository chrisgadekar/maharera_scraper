import asyncio
import logging
import os
import csv
import pandas as pd
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page
from modules.captcha_solver import CaptchaSolver
from modules.data_extracter import DataExtracter
from typing import Set, Optional

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MahaReraScraper")

# --- File Configuration ---
OUTPUT_FILENAME = "maharera_complete_data.csv"
FAILED_PROJECTS_FILENAME = "maharera_failed_projects.csv"

# --- Scraping Configuration ---
BASE_URL = "https://maharerait.maharashtra.gov.in/public/project/view/"
TOTAL_WORKERS = 1

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

csv_lock = asyncio.Lock()

async def save_record(data: dict):
    async with csv_lock:
        try:
            df = pd.json_normalize([data])
            df = df.reindex(columns=DESIRED_ORDER)
            file_exists = os.path.exists(OUTPUT_FILENAME)
            df.to_csv(OUTPUT_FILENAME, mode='a', index=False, header=not file_exists)
        except Exception as e:
            logger.error(f"Failed to save record for {data.get('project_id')}: {e}")

async def process_single_project(page: Page, captcha_solver: CaptchaSolver, data_extracter: DataExtracter, project_id: int, url: str) -> bool:
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
            logger.warning(f"CAPTCHA FAILED for project {project_id}.")
            return False

        try:
            await page.wait_for_selector('p#letter:has-text("Project details not found")', timeout=7000)
            logger.info(f"✅ HANDLED: Project ID {project_id} does not exist.")
            null_data = {col: "Project Not Found" for col in DESIRED_ORDER}
            null_data['project_id'] = project_id
            null_data['registration_number'] = str(project_id)
            await save_record(null_data)
            return True
        except Exception:
            logger.info(f"Project {project_id} page is valid, proceeding...")
            pass
        
        data = await data_extracter.extract_project_details(page, str(project_id))
        if data:
            data["project_id"] = project_id
            await save_record(data)
            logger.info(f"✅ SUCCESS: Saved data for project {project_id}")
            return True
        else:
            logger.warning(f"Data extraction returned None for project {project_id}.")
            return False
    except Exception as e:
        logger.error(f"FATAL ERROR processing project {project_id}: {e}", exc_info=False)
        return False

async def worker(browser: Browser, queue: asyncio.Queue, failures_queue: asyncio.Queue, captcha_solver: CaptchaSolver, data_extracter: DataExtracter):
    context: Optional[BrowserContext] = None
    try:
        context = await browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36')
        page = await context.new_page()
        await page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "stylesheet", "font", "media"] else route.continue_())

        while True:
            project_id = await queue.get()
            url = f"{BASE_URL}{project_id}"
            logger.info(f"Processing project ID: {project_id}")
            is_successful = await process_single_project(page, captcha_solver, data_extracter, project_id, url)
            if not is_successful:
                logger.warning(f"FAILED: Project {project_id}. This will remain in the failed list for the next run.")
                await failures_queue.put((project_id, url))
            queue.task_done()
    except asyncio.CancelledError:
        logger.info("Worker cancelled.")
    finally:
        if context: await context.close()

# <<< FIX 1: New function to get successfully completed IDs ---
def get_successfully_processed_ids() -> Set[int]:
    """Reads ONLY the success CSV to get a set of all IDs that are already completed."""
    processed_ids = set()
    if os.path.exists(OUTPUT_FILENAME):
        try:
            # Read only the 'project_id' column to save memory
            df = pd.read_csv(OUTPUT_FILENAME, usecols=['project_id'], on_bad_lines='skip')
            # Add valid integer IDs to our set
            processed_ids.update(df['project_id'].dropna().astype(int).tolist())
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not read project_id column from {OUTPUT_FILENAME}. It might be empty or malformed. Error: {e}")
    return processed_ids

async def main():
    logger.info("--- Starting MahaRERA Scraper (File Update Mode) ---")

    # <<< FIX 2: Get a list of already completed projects first ---
    successfully_processed_ids = get_successfully_processed_ids()
    if successfully_processed_ids:
        logger.info(f"Found {len(successfully_processed_ids)} already completed projects in {OUTPUT_FILENAME}. They will be skipped.")

    projects_to_process = []
    source_list = []
    
    if os.path.exists(FAILED_PROJECTS_FILENAME) and os.path.getsize(FAILED_PROJECTS_FILENAME) > 20:
        logger.info(f"Found {FAILED_PROJECTS_FILENAME}. Running in RETRY mode.")
        try:
            df_failed = pd.read_csv(FAILED_PROJECTS_FILENAME)
            source_list = df_failed['project_id'].dropna().astype(int).tolist()
        except Exception as e:
            logger.error(f"Could not read {FAILED_PROJECTS_FILENAME}: {e}")
            return
    else:
        logger.info("No failed projects file found. Running in INITIAL mode with hardcoded list.")
        source_list = sorted(list(set([
            29893, 40000, 56647, 58095, 58260, 58336, 58345, 58402, 58429, 58467, 58475, 58490, 58496, 58498, 58499, 58506, 58509, 58518, 58519, 58520, 58521, 58525, 58527, 58528, 58529, 58530, 58531, 58533, 58536, 58537, 58541, 58547, 58549, 58553, 58555, 58556, 58557, 58558, 58561, 58562, 58564, 58566, 58568, 58572, 58573, 58575, 58576, 58578, 58579, 58580, 58581, 58582, 58587, 58588, 58591, 58593, 58594, 58595, 58596, 58597, 58599, 58605, 58606, 58608, 58610, 58612, 58623, 58625, 58627, 58630, 58632, 58633, 58634, 58636, 58639, 58641, 58642, 58643, 58644, 58645, 58646, 58649, 58650, 58651, 58652, 58653, 58659, 58660, 58661, 58662, 58664, 58668, 58669, 58670, 58671, 58673, 58677, 58678, 58680, 58681, 58683, 58684, 58685, 58686, 58687, 58689, 58690, 58691, 58692, 58693, 58694, 58696, 58697, 58698, 58699, 58700, 58702, 58704, 58705, 58708, 58711, 58713, 58714, 58715, 58717, 58718, 58719, 58721, 58722, 58723, 58724, 58725, 58727, 58728, 58729, 58731, 58732, 58734, 58735, 58736, 58737, 58738, 58743, 58745, 58746, 58747, 58749, 58755, 58757, 58758, 58759, 58761, 58762, 58763, 58765, 58768, 58771, 58772, 58774, 58775, 58786, 58787, 58788, 58789, 58790, 58791, 58792, 58793, 58794, 58795, 58796, 58798, 58799, 58800, 58801, 58802, 58803, 58806, 58807, 58808, 58810, 58811, 58812, 58813, 58815, 58816, 58817, 58818, 58819, 58820, 58821, 58822, 58823, 58825, 58826, 58827, 58828, 58829, 58831, 58832, 58833, 58834, 58837, 58839, 58840, 58841, 58842, 58843, 58844, 58846, 58847, 58848, 58849, 58851, 58853, 58854, 58855, 58856, 58857, 58858, 58859, 58861, 58862, 58863, 58864, 58865, 58866, 58867, 58868, 58869, 58870, 58871, 58872, 58873, 58874, 58876, 58877, 58878, 58879, 58880, 58881, 58882, 58883, 58884, 58885, 58886, 58887, 58888, 58889, 58890, 58891, 58892, 58893, 58894, 58895, 58896, 58898, 58900, 58901, 58902, 58903, 58904, 58905, 58906, 58908, 58909, 58910, 58911, 58912, 58913, 58914, 58915, 58916, 58917, 58918, 58919, 58921, 58922, 58923, 58925, 58926, 58953, 58982
        ])))

    # <<< FIX 3: Filter the source list to skip already completed projects ---
    for pid in source_list:
        if pid not in successfully_processed_ids:
            projects_to_process.append(pid)

    if not projects_to_process:
        logger.info("No new projects to process. All seem to be completed. Exiting.")
        return
        
    process_queue = asyncio.Queue()
    failures_queue = asyncio.Queue()

    for pid in projects_to_process:
        await process_queue.put(pid)
    
    logger.info(f"Queued {process_queue.qsize()} projects for this run (skipped {len(source_list) - len(projects_to_process)} already completed ones).")

    captcha_solver = CaptchaSolver()
    data_extracter = DataExtracter()

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        tasks = []
        for _ in range(TOTAL_WORKERS):
            tasks.append(asyncio.create_task(
                worker(browser, process_queue, failures_queue, captcha_solver, data_extracter)
            ))

        await process_queue.join()

        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()

    remaining_failures = []
    while not failures_queue.empty():
        remaining_failures.append(await failures_queue.get())

    if remaining_failures:
        logger.info(f"Overwriting {FAILED_PROJECTS_FILENAME} with {len(remaining_failures)} remaining failures.")
        with open(FAILED_PROJECTS_FILENAME, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["project_id", "url"])
            writer.writerows(remaining_failures)
    else:
        logger.info(f"All projects in this run succeeded. Deleting {FAILED_PROJECTS_FILENAME}.")
        if os.path.exists(FAILED_PROJECTS_FILENAME):
            os.remove(FAILED_PROJECTS_FILENAME)

    logger.info("--- SCRAPING RUN COMPLETE ---")
    logger.info(f"Successful data saved to: {OUTPUT_FILENAME}")
    if remaining_failures:
        logger.info(f"{len(remaining_failures)} projects failed and are saved in {FAILED_PROJECTS_FILENAME} for the next run.")
    else:
        logger.info("No projects failed in this run.")

if __name__ == "__main__":
    asyncio.run(main())