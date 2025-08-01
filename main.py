import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from modules.captcha_solver import CaptchaSolver
from modules.data_extracter import DataExtracter
import logging
import os

# --- Setup basic logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CSV_PATH = "./phase-1/phase1_preview_links.csv"
OUTPUT_FILENAME = "maharera_complete_data.csv"

# def save_record(data: dict, filename: str):
#     try:
#         df = pd.json_normalize([data])
#         file_exists = os.path.exists(filename)
#         df.to_csv(filename, mode='a', index=False, header=not file_exists)
#     except Exception as e:
#         logging.error(f"Failed to save record for {data.get('reg_no')}: {e}")

def save_record(data: dict, filename: str):
    try:
        DESIRED_ORDER = [
            "reg_no",
            "registration_number",
            "date_of_registration",
            "project_name",
            "project_status",
            "project_type",
            "project_location",
            "proposed_completion_date",
            "planning_authority",
            "full_name_planning_authority",
            "final_plot_bearing",
            "total_land_area",
            "land_area_applied",
            "permissible_builtup",
            "sanctioned_builtup",
            "aggregate_open_space",
            "project_address_full",
            "promoter_type",
            "name_of_partnership",
            "promoter_official_address",
            "partner_details.names",
            "partner_details.designations",
            "promoter_past_project_names",
            "promoter_past_project_statuses",
            "promoter_past_litigation_statuses",
            "authorised_signatory_names",
            "authorised_signatory_designations",
            "Architect",
            "Engineer",
            "Other",
            "Promoter Project Member Number",
            "SRO Membership Type Name",
             "Landowner Type",
             "Are there investors other than promoter",
             "Litigation against this project",
             "Identification of Wing as per Sanctioned Plan",
             "Number of Sanctioned Floors (Incl. Basement+Stilt+Podium+Service+Habitable)"
        ]

        df = pd.json_normalize([data])
        df = df.reindex(columns=DESIRED_ORDER)  # Enforce column order

        file_exists = os.path.exists(filename)
        df.to_csv(filename, mode='a', index=False, header=not file_exists)
    except Exception as e:
        logging.error(f"Failed to save record for {data.get('reg_no')}: {e}")




async def process_url(page, captcha_solver, data_extracter, reg_no, view_link):
    logging.info(f"🔗 Processing: {view_link}")
    try:
        await page.goto(view_link, wait_until='domcontentloaded', timeout=60000)

        # --- Solve captcha ---
        success = await captcha_solver.solve_and_fill(
            page,
            captcha_selector="canvas#captcahCanvas",
            input_selector="input[name='captcha']",
            submit_selector="button.btn.btn-primary.next",
            refresh_selector="a.cpt-btn",
            reg_no=reg_no
        )

        if not success:
            logging.warning(f"❌ Skipping {reg_no} due to captcha failure")
            return None

        # --- Wait for content ---
        try:
            logging.info("⏳ Waiting for the main content card to appear...")
            await page.wait_for_selector('//h4[normalize-space()="Project Details"]', timeout=30000)
            logging.info("✅ Main content card appeared.")
            await page.wait_for_timeout(3000)  # Allow extra time to settle
        except Exception:
            logging.error(f"❌ Timed out waiting for page content to load for {reg_no}.")
            return None

        # --- Proceed with extraction ---
        data = await data_extracter.extract_project_details(page, reg_no)
        if data and data.get('project_name'):
            logging.info(f"✅ SUCCESS: {reg_no} -> {data.get('project_name')}")
            return data
        else:
            logging.warning(f"❌ Failed to extract data for {reg_no}, even after waiting.")
            return None

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {reg_no}: {e}")
        return None

async def main():
    processed_reg_nos = set()
    try:
        if os.path.exists(OUTPUT_FILENAME):
            df_processed = pd.read_csv(OUTPUT_FILENAME)
            if 'reg_no' in df_processed.columns:
                processed_reg_nos = set(df_processed['reg_no'])
            logging.info(f"Resuming scrape. Found {len(processed_reg_nos)} previously processed records.")
    except Exception as e:
        logging.error(f"Could not read existing output file to resume. Starting from scratch. Error: {e}")

    try:
        df_input = pd.read_csv(CSV_PATH)
    except FileNotFoundError:
        logging.error(f"Error: The input file was not found at {CSV_PATH}")
        return

    df_todo = df_input[~df_input['reg_no'].isin(processed_reg_nos)]
    if df_todo.empty:
        logging.info("✅ All records have already been processed. Nothing to do.")
        return

    logging.info(f"Total records to process: {len(df_todo)} out of {len(df_input)}.")
    df_todo = df_todo.iloc[:2]
    logging.info("--- 🧪 TEST MODE: Sirf 10th entry  process kiye ja rahe hain. ---")

    captcha_solver = CaptchaSolver()
    data_extracter = DataExtracter()

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=False)
        context = await browser.new_context(
            device_scale_factor=2,
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()

        for _, row in df_todo.iterrows():
            reg_no, view_link = row["reg_no"], row["view_link"]
            if pd.isna(view_link):
                logging.warning(f"Skipping row with empty view_link for reg_no: {reg_no}")
                continue
            data = await process_url(page, captcha_solver, data_extracter, reg_no, view_link)
            if data:
                save_record(data, OUTPUT_FILENAME)

        await browser.close()

    logging.info(f"\n✅ SCRAPING RUN COMPLETE.")

if __name__ == "__main__":
    asyncio.run(main())
