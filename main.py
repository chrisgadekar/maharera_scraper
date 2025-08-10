import asyncio
import logging
import os
import pandas as pd
from playwright.async_api import async_playwright
from modules.captcha_solver import CaptchaSolver
from modules.data_extracter import DataExtracter

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OUTPUT_FILENAME = "maharera_complete_data.csv"

def save_record(data: dict, filename: str):
    """Save extracted data in the desired column order."""
    try:
        DESIRED_ORDER = [
            "project_id",
            "registration_number",
            "date_of_registration",
            "project_name",
            "project_type",
            "project_location",
            "proposed_completion_date",
            "extension_date",
            "project_status",
            "planning_authority",
            "full_name_of_planning_authority",
            "final_plot_bearing",
            "total_land_area",
            "land_area_applied",
            "permissible_builtup",
            "sanctioned_builtup",
            "aggregate_open_space",
            "CC/NA Order Issued to",
            "CC/NA Order in the name of",
            "project_address_state_ut",
            "project_address_district",
            "project_address_taluka",
            "project_address_village",
            "project_address_pin_code",
            "promoter_details",
            "promoter_official_communication_address_state_ut",
            "promoter_official_communication_address_district",
            "promoter_official_communication_address_taluka",
            "promoter_official_communication_address_village",
            "promoter_official_communication_address_pin_code",
            # "partner_name",
            # "partner_designation",
            # "promoter_past_project_names",
            # "promoter_past_project_statuses",
            # "promoter_past_litigation_statuses",
            # "authorised_signatory_names",
            # "authorised_signatory_designations"
             "building_identification_plan",
            "wing_identification_plan",
            "sanctioned_floors",
            "sanctioned_habitable_floors",
            "sanctioned_apartments",
            "cc_issued_floors",
            "view_document_available",
            "summary_identification_building_wing",
            "summary_identification_wing_plan",
            "summary_floor_type",
            "summary_total_no_of_residential_apartments",
            "summary_total_no_of_non_residential_apartments",
            "summary_total_no_of_apartments_nr_r",
            "summary_total_no_of_sold_units",
            "summary_total_no_of_unsold_units",
            "summary_total_no_of_booked",
            "summary_total_no_of_rehab_units",
            "summary_total_no_of_mortgage",
            "summary_total_no_of_reservation",
            "summary_total_no_of_land_owner_investor_share_sale",
            "summary_total_no_of_land_owner_investor_share_not_for_sale",
            
            "total_no_of_apartments"
        ]


        df = pd.json_normalize([data])
        df = df.reindex(columns=DESIRED_ORDER)

        file_exists = os.path.exists(filename)
        df.to_csv(filename, mode='a', index=False, header=not file_exists)

    except Exception as e:
        logging.error(f"Failed to save record for {data.get('project_id')}: {e}")

async def process_url(page, captcha_solver, data_extracter, project_id, view_link):
    """Navigate to the page, solve captcha, extract project details."""
    logging.info(f"🔗 Processing ID {project_id}: {view_link}")
    try:
        await page.goto(view_link, wait_until='domcontentloaded', timeout=60000)

        success = await captcha_solver.solve_and_fill(
            page,
            captcha_selector="canvas#captcahCanvas",
            input_selector="input[name='captcha']",
            submit_selector="button.btn.btn-primary.next",
            refresh_selector="a.cpt-btn",
            reg_no=str(project_id)  # For captcha/logging purposes
        )

        if not success:
            logging.warning(f"❌ Skipping {project_id} due to captcha failure")
            return None

        try:
            logging.info("⏳ Waiting for the main content card to appear...")
            await page.wait_for_selector('//h4[normalize-space()="Project Details"]', timeout=30000)
            logging.info("✅ Main content card appeared.")
            await page.wait_for_timeout(3000)
        except Exception:
            logging.error(f"❌ Timed out waiting for page content to load for {project_id}.")
            return None

        # Extract full data
        data = await data_extracter.extract_project_details(page, str(project_id))

        # Add our primary key
        data["project_id"] = project_id

        logging.info(f"✅ SUCCESS: {project_id} -> {data.get('project_name')}")
        return data

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {project_id}: {e}")
        return None

async def main():
    # --- Dynamic range config ---
    START_ID = 32268
    END_ID = 32268
    BASE_URL = "https://maharerait.maharashtra.gov.in/public/project/view/"

    # Resume capability
    processed_ids = set()
    if os.path.exists(OUTPUT_FILENAME):
        try:
            df_processed = pd.read_csv(OUTPUT_FILENAME)
            if 'project_id' in df_processed.columns:
                processed_ids = set(df_processed['project_id'])
            logging.info(f"Resuming scrape. Found {len(processed_ids)} previously processed records.")
        except Exception as e:
            logging.error(f"Could not read existing output file. Starting fresh. Error: {e}")

    captcha_solver = CaptchaSolver()
    data_extracter = DataExtracter()

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=False)
        context = await browser.new_context(
            device_scale_factor=2,
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()

        for i in range(START_ID, END_ID + 1):
            if i in processed_ids:
                logging.info(f"⏭ Skipping already processed ID {i}")
                continue

            url = f"{BASE_URL}{i}"
            data = await process_url(page, captcha_solver, data_extracter, i, url)

            if data:
                save_record(data, OUTPUT_FILENAME)

        await browser.close()

    logging.info("\n✅ SCRAPING RUN COMPLETE.")

if __name__ == "__main__":
    asyncio.run(main())
