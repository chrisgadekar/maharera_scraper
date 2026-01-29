import asyncio
import csv
import os
import pandas as pd
from playwright.async_api import async_playwright
from typing import Callable, Optional, Set

from modules.data_extracter import DataExtracter
from modules.captcha_solver import CaptchaSolver

# --------------------------------------------------
# DEFAULT CONFIGURATION
# --------------------------------------------------
DEFAULT_SEARCH_URL = "https://maharera.maharashtra.gov.in/projects-search-result"
DEFAULT_RERA_COLUMN = "RERA No."

# CAPTCHA SELECTORS
CAPTCHA_CANVAS = "#captcahCanvas"
CAPTCHA_INPUT = "input[name='captcha']"
CAPTCHA_SUBMIT = "button.next"
CAPTCHA_REFRESH = "#captchaRefresh"

# INVALID CAPTCHA MODAL
INVALID_CAPTCHA_TEXT = "text=Invalid Captcha"
INVALID_CAPTCHA_OK_BTN = "button.btn-primary-messagebox.next"

# Column order for full page extraction
CSV_COLUMNS = [
    "rera_no", "registration_number", "date_of_registration", "project_name",
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
    "authorised_signatory_names", "authorised_signatory_designations", "spa_name", "spa_designation",
    "architect_names", "engineer_names", "chartered_accountant_names", "other_professional_names",
    "sro_name", "sro_document_name", "latest_form1_date", "latest_form2_date", "latest_form5_date", "has_occupancy_certificate",
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


# --------------------------------------------------
# FILE APPEND HELPER (CSV or XLSX)
# --------------------------------------------------
def append_to_file(path: str, row: dict):
    """Append a single record to CSV or XLSX with fixed column order."""
    dir_path = os.path.dirname(path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    ordered_row = {col: row.get(col, "") for col in CSV_COLUMNS}

    if path.endswith('.xlsx'):
        # XLSX handling
        if os.path.isfile(path):
            # Read existing data and append
            df_existing = pd.read_excel(path)
            df_new = pd.DataFrame([ordered_row])
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.reindex(columns=CSV_COLUMNS)
            df_combined.to_excel(path, index=False)
        else:
            # Create new file
            df = pd.DataFrame([ordered_row])
            df = df.reindex(columns=CSV_COLUMNS)
            df.to_excel(path, index=False)
    else:
        # CSV handling (default)
        file_exists = os.path.isfile(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(ordered_row)


# --------------------------------------------------
# INVALID CAPTCHA MODAL HANDLER
# --------------------------------------------------
async def handle_invalid_captcha_modal(page) -> bool:
    """Detects 'Invalid Captcha' modal and clicks OK."""
    try:
        await page.wait_for_selector(INVALID_CAPTCHA_TEXT, timeout=2000)
        await page.click(INVALID_CAPTCHA_OK_BTN)
        await page.wait_for_timeout(500)
        return True
    except:
        return False


# --------------------------------------------------
# GET ALREADY PROCESSED RERA NUMBERS
# --------------------------------------------------
def get_processed_rera_numbers(output_path: str) -> Set[str]:
    """Read already processed RERA numbers from the output CSV or XLSX."""
    processed = set()
    if os.path.exists(output_path):
        try:
            if output_path.endswith('.xlsx'):
                df = pd.read_excel(output_path, usecols=['rera_no'])
            else:
                df = pd.read_csv(output_path, usecols=['rera_no'], on_bad_lines='skip')
            processed = set(df['rera_no'].dropna().astype(str).str.strip().tolist())
        except (ValueError, KeyError, FileNotFoundError):
            pass
    return processed


# --------------------------------------------------
# MAIN SCRAPER FUNCTION (callable from Streamlit)
# --------------------------------------------------
async def run_scraper(
    input_path: str,
    output_path: str,
    start_row: int = 2,
    headless: bool = False,
    max_captcha_attempts: int = 6,
    rera_column: str = DEFAULT_RERA_COLUMN,
    log_callback: Optional[Callable[[str], None]] = None,
    stop_flag: Optional[Callable[[], bool]] = None
) -> dict:
    """
    Main scraper function that can be called from Streamlit or CLI.

    Args:
        input_path: Path to Excel/CSV file with RERA numbers
        output_path: Path where output CSV will be saved
        start_row: Starting row in Excel (1-based, default 2 to skip header)
        headless: Run browser in headless mode
        max_captcha_attempts: Max attempts to solve CAPTCHA
        rera_column: Column name containing RERA numbers
        log_callback: Optional function to send log messages to UI
        stop_flag: Optional function that returns True if scraping should stop

    Returns:
        dict with 'success_count', 'error_count', 'total'
    """

    def log(msg: str):
        if log_callback:
            log_callback(msg)
        print(msg)

    def should_stop() -> bool:
        return stop_flag() if stop_flag else False

    stats = {"success_count": 0, "error_count": 0, "total": 0}

    # --------------------------------------------------
    # 1. LOAD INPUT FILE
    # --------------------------------------------------
    try:
        if input_path.endswith('.csv'):
            df = pd.read_csv(input_path)
        else:
            df = pd.read_excel(input_path)
    except Exception as e:
        log(f"ERROR: Could not read input file: {e}")
        return stats

    if rera_column not in df.columns:
        log(f"ERROR: Missing column '{rera_column}' in input file")
        log(f"Available columns: {list(df.columns)}")
        return stats

    start_index = max(start_row - 2, 0)

    rera_numbers = (
        df.loc[start_index:, rera_column]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )

    # Filter out already processed
    processed = get_processed_rera_numbers(output_path)
    rera_numbers = [r for r in rera_numbers if r not in processed]

    stats["total"] = len(rera_numbers)

    log(f"Loaded {len(rera_numbers)} RERA numbers to process (skipping {len(processed)} already done)")

    if not rera_numbers:
        log("No new RERA numbers to process.")
        return stats

    # --------------------------------------------------
    # 2. PLAYWRIGHT SETUP
    # --------------------------------------------------
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            slow_mo=100
        )

        context = await browser.new_context()
        captcha_solver = CaptchaSolver()
        data_extracter = DataExtracter()

        # --------------------------------------------------
        # 3. PROCESS EACH RERA NUMBER
        # --------------------------------------------------
        for idx, rera_no in enumerate(rera_numbers, start=1):
            if should_stop():
                log("Scraping stopped by user.")
                break

            log(f"\n[{idx}/{stats['total']}] Processing RERA No: {rera_no}")
            page = await context.new_page()

            try:
                # SEARCH PAGE
                await page.goto(DEFAULT_SEARCH_URL, wait_until="domcontentloaded")

                await page.fill(
                    "input[placeholder='Project Name/ MahaRERA Registration Number']",
                    rera_no
                )

                await page.get_by_role("button", name="Search").first.click()
                await page.wait_for_selector("text=View Details", timeout=30000)

                # OPEN VIEW DETAILS (NEW TAB)
                async with context.expect_page() as new_tab:
                    await page.get_by_role("link", name="View Details").first.click()

                    # YES CONFIRMATION POPUP (IF PRESENT) - must click before new tab opens
                    try:
                        await page.wait_for_selector(
                            "div.dialog",
                            timeout=3000
                        )
                        # Click the Yes button (ID starts with "confirm-ok-")
                        await page.locator("button[id^='confirm-ok-']").click()
                        log("  Confirmation popup accepted")
                    except:
                        pass  # No confirmation popup

                project_page = await new_tab.value
                await project_page.bring_to_front()

                # CAPTCHA HANDLING
                try:
                    await project_page.wait_for_selector(CAPTCHA_CANVAS, timeout=5000)
                    log("CAPTCHA detected - starting auto solve")

                    captcha_solved = False

                    for attempt in range(1, max_captcha_attempts + 1):
                        log(f"  CAPTCHA attempt {attempt}/{max_captcha_attempts}")

                        await captcha_solver.solve_and_fill(
                            page=project_page,
                            captcha_selector=CAPTCHA_CANVAS,
                            input_selector=CAPTCHA_INPUT,
                            submit_selector=CAPTCHA_SUBMIT,
                            reg_no=rera_no
                        )

                        # SUCCESS CHECK
                        try:
                            await project_page.wait_for_selector(
                                "div.white-box h5.card-title:has-text('Promoter Details')",
                                timeout=2500
                            )
                            captcha_solved = True
                            log("  CAPTCHA solved successfully")
                            break
                        except:
                            pass

                        await handle_invalid_captcha_modal(project_page)

                        await project_page.evaluate(
                            "document.querySelector('#captchaRefresh')?.click()"
                        )
                        await project_page.wait_for_timeout(200)

                    if not captcha_solved:
                        log("  CAPTCHA failed after all attempts - skipping")
                        stats["error_count"] += 1
                        await project_page.close()
                        continue

                except:
                    log("  No CAPTCHA on this record")

                # WAIT FOR PROJECT PAGE
                await project_page.wait_for_url("**/public/project/view/**", timeout=30000)
                await project_page.wait_for_selector(
                    "div.white-box h5.card-title:has-text('Promoter Details')",
                    timeout=60000
                )
                await project_page.wait_for_load_state('networkidle', timeout=30000)
                await project_page.wait_for_timeout(2500)

                # EXTRACT FULL PROJECT DETAILS
                data = await data_extracter.extract_project_details(project_page, rera_no)

                if data:
                    data["rera_no"] = rera_no
                    append_to_file(output_path, data)
                    stats["success_count"] += 1
                    log(f"  SUCCESS: Data extracted for {rera_no}")
                else:
                    stats["error_count"] += 1
                    log(f"  WARNING: No data extracted for {rera_no}")

                await project_page.close()

            except Exception as e:
                stats["error_count"] += 1
                log(f"  ERROR for {rera_no}: {e}")

            finally:
                await page.close()

        await browser.close()

    log(f"\n{'='*50}")
    log(f"SCRAPING COMPLETE")
    log(f"  Success: {stats['success_count']}")
    log(f"  Errors:  {stats['error_count']}")
    log(f"  Output:  {output_path}")
    log(f"{'='*50}")

    return stats


# --------------------------------------------------
# CLI ENTRY POINT
# --------------------------------------------------
async def main():
    """CLI entry point with hardcoded defaults."""
    await run_scraper(
        input_path="data/input/Promoter office address.xlsx",
        output_path="data/output/maharera_full_data.csv",
        start_row=2,
        headless=False,
        max_captcha_attempts=6
    )


if __name__ == "__main__":
    asyncio.run(main())
