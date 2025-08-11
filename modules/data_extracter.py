import asyncio
import re
from typing import Dict, List, Optional, Any
import logging
from playwright.async_api import Page, expect


logger = logging.getLogger(__name__)

class DataExtracter:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def extract_project_details(self, page: Page, reg_no: str) -> Optional[Dict[str, Any]]:
        """Extract comprehensive project details from the MahaRERA project page."""
        try:
            await page.wait_for_selector("div.form-card", timeout=10000)
            data = {'reg_no': reg_no}

            tasks = [
                self._extract_registration_block(page),
                self._extract_project_details_block(page),
                self._extract_planning_authority_block(page),
                self._extract_planning_land_block(page),
                self._extract_commencement_certificate(page),
                self._extract_project_address(page),
                self._extract_promoter_details(page),
                self._extract_promoter_address(page),


                self._extract_all_tab_data(page),

                self._extract_latest_form_dates(page),
                 self._extract_investor_flag(page),
                self._extract_litigation_details(page),
                self._extract_building_details(page),
                self._extract_apartment_summary(page),
                self._extract_parking_details(page),
                self._extract_bank_details(page),
                self._extract_complaint_details(page),
                self._extract_real_estate_agents(page)



                
                
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.warning(f"A data block extraction failed for {reg_no}: {result}")
                elif result:
                    data.update(result)

            return data

        except Exception as e:
            self.logger.error(f"Fatal error extracting data for {reg_no}: {e}")
            return None







    async def _extract_registration_block(self, page: Page) -> Dict[str, str]:
        try:
            reg_number = await page.locator("label[for='yourUsername']:has-text('Registration Number')").locator("xpath=following-sibling::label[1]").inner_text(timeout=5000)
            reg_date = await page.locator("label[for='yourUsername']:has-text('Date of Registration')").locator("xpath=following-sibling::label[1]").inner_text(timeout=5000)

            result = {
                'registration_number': reg_number.strip(),
                'date_of_registration': reg_date.strip()
            }

            self.logger.info(f"Extracted Registration Block: {result}")  # ✅ Logging the result

            return result
        except Exception as e:
            self.logger.warning(f"Could not extract Registration Block: {e}")
            return {}


    async def _extract_project_details_block(self, page: Page) -> Dict[str, str]:
        data = {}

        fields = {
            'project_name': "Project Name",
            'project_type': "Project Type",
            'project_location': "Project Location",
            'proposed_completion_date': "Proposed Completion Date (Original)"
        }

        try:
            for key, label in fields.items():
                locator = page.locator(f"div:text-is('{label}')").nth(0)
                value_locator = locator.locator("xpath=following-sibling::div[1]")
                value = await value_locator.inner_text(timeout=5000)
                data[key] = value.strip()

            # Handle optional Revised Date (Extension Date)
            try:
                ext_label = "Proposed Completion Date (Revised)"
                ext_locator = page.locator(f"div:text-is('{ext_label}')").nth(0)
                ext_value_locator = ext_locator.locator("xpath=following-sibling::div[1]")
                ext_value = await ext_value_locator.inner_text(timeout=3000)
                data['extension_date'] = ext_value.strip()
            except Exception:
                data['extension_date'] = None  # Not present for all

             # ✅ Extract Project Status (separate DOM structure)
            try:
                status_label = page.locator("span:text-is('Project Status')").first
                status_value = await status_label.locator("xpath=../../following-sibling::div[1]//span").inner_text(timeout=3000)
                data['project_status'] = status_value.strip()
            except Exception:
                data['project_status'] = None  # Missing or unexpected structure

            self.logger.info(f"Extracted Project Details: {data}")
            return data

        except Exception as e:
            self.logger.warning(f"❌ Could not extract Project Details Block: {e}")
            return {}


    async def _extract_planning_authority_block(self, page: Page) -> Dict[str, Optional[str]]:
        """
        Extracts the Planning Authority details from the page.
        
        Based on the HTML structure:
        - Headers are in span elements with text "Planning Authority" and "Full Name of the Planning Authority"
        - Values are in sibling div elements next to the parent div of the span
        """
        data = {
            "planning_authority": None,
            "full_name_of_planning_authority": None
        }

        try:
            # 1. Locate the main container for the entire section
            container = page.locator('div.row:has-text("Planning Authority")').first
            await container.wait_for(timeout=5000)

            # 2. Extract "Planning Authority" value
            try:
                # Find span with "Planning Authority" text
                label_pa = container.locator('span:has-text("Planning Authority")')
                # Get the parent div of the span, then its next sibling div, then the first p within it
                value_pa_locator = label_pa.locator("xpath=./ancestor::div[contains(@class, 'col-12 text-font')]/following-sibling::div[1]/p").first
                value_pa = await value_pa_locator.inner_text()
                data["planning_authority"] = value_pa.strip() if value_pa else None
            except Exception as e:
                self.logger.warning(f"Could not extract 'Planning Authority' value: {e}")

            # 3. Extract "Full Name of the Planning Authority" value
            try:
                # Find span with "Full Name of the Planning Authority" text
                label_fn = container.locator('span:has-text("Full Name of the Planning Authority")')
                # Get the parent div of the span, then its next sibling div, then the first p within it
                value_fn_locator = label_fn.locator("xpath=./ancestor::div[contains(@class, 'col-12 text-font')]/following-sibling::div[1]/p").first
                value_fn = await value_fn_locator.inner_text()
                data["full_name_of_planning_authority"] = value_fn.strip() if value_fn else None
            except Exception as e:
                self.logger.warning(f"Could not extract 'Full Name of the Planning Authority' value: {e}")

            return data

        except Exception as e:
            self.logger.error(f"❌ Could not find or process the Planning Authority block: {e}")
            return data


    async def _extract_planning_land_block(self, page: Page) -> Dict[str, Optional[str]]:
        data = {}
        try:
            field_map = {
                'final_plot_bearing': "Final Plot bearing No/CTS Number/Survey Number",
                'total_land_area': "Total Land Area of Approved Layout (Sq. Mts.)",
                'land_area_applied': "Land Area for Project Applied for this Registration (Sq. Mts)",
                'permissible_builtup': "Permissible Built-up Area",
                'sanctioned_builtup': "Sanctioned Built-up Area of the Project applied for Registration",
                'aggregate_open_space': "Aggregate area(in sq. mts) of recreational open space as per Layout / DP Remarks"
            }

            # Step 1: Find the correct `.form-card` that contains the target heading
            section_card = page.locator("div.card-header:has-text('Land Area & Address Details')").first
            form_card = section_card.locator("xpath=ancestor::div[contains(@class, 'form-card')]").first

            await form_card.wait_for(timeout=5000)

            # Step 2: Iterate through the white-boxes inside the section
            white_boxes = form_card.locator("div.white-box")
            count = await white_boxes.count()

            for key, expected_label in field_map.items():
                found = False
                for i in range(count):
                    box = white_boxes.nth(i)
                    try:
                        label = await box.locator("label").inner_text()
                        if expected_label.strip() in label.strip():
                            value_div = box.locator("div.text-font.f-w-700")
                            await value_div.wait_for(timeout=2000)
                            value = await value_div.inner_text()
                            data[key] = value.strip()
                            found = True
                            break
                    except Exception:
                        continue
                if not found:
                    data[key] = None
                    self.logger.warning(f"⚠️ Label '{expected_label}' not found in Planning/Land block.")

            return data

        except Exception as e:
            self.logger.warning(f"❌ Could not extract Planning/Land Block at all: {e}")
            return {}

    async def _extract_commencement_certificate(self, page: Page) -> Dict[str, str]:
        data = {
            "CC/NA Order Issued to": "",
            "CC/NA Order in the name of": ""
        }
        try:
            # Locate the section by its heading text
            section = page.locator(
                "div:has(h5.card-title.mb-0:has-text('Commencement Certificate / NA Order Documents Details'))"
            )
            
            divOfTable=section.locator("xpath=following-sibling::div[1]");
            # From that section, find the table
            table = divOfTable.locator("table:has-text('CC/NA Order Issued to')")
            await table.wait_for(timeout=5000)

            # Get table rows
            rows = table.locator("tbody tr")
            count = await rows.count()

            if count == 0 or "No-Data-Found" in (await rows.first.inner_text()):
                self.logger.info("No Commencement Certificate data found in the table.")
                return data

            col2_values, col3_values = [], []
            for i in range(count):
                row = rows.nth(i)
                try:
                    col2 = await row.locator("td:nth-child(2)").inner_text()
                    col3 = await row.locator("td:nth-child(3)").inner_text()
                    col2_values.append(col2.strip())
                    col3_values.append(col3.strip())
                except Exception as e:
                    self.logger.warning(f"Could not process a row in Commencement Certificate table: {e}")
                    continue

            data["CC/NA Order Issued to"] = ", ".join(col2_values)
            data["CC/NA Order in the name of"] = ", ".join(col3_values)
            return data

        except Exception as e:
            self.logger.warning(f"Could not extract Commencement Certificate details: {e}")
            return data

    async def _extract_project_address(self, page: Page) -> Dict[str, str]:
        """
        Extracts State/UT, District, Taluka, Village, and Pin Code from the 
        'Project Address Details' section, with keys prefixed by project_address_.
        """
        try:
            target_labels = ["State/UT", "District", "Taluka", "Village", "Pin Code"]

            # Anchor to the Project Address Details section
            header = page.locator("h5.card-title:has-text('Project Address Details')")
            await header.wait_for(timeout=10000)
            section = header.locator("xpath=ancestor::div[contains(@class, 'white-box')]")

            results = {}
            for label in target_labels:
                label_locator = section.locator(f"label.form-label:has-text('{label}')")
                value_locator = label_locator.locator("xpath=following-sibling::*[1]")  # First sibling div
                child_div_locator = value_locator.locator("div")

                await child_div_locator.wait_for(timeout=5000)
                value_text = (await child_div_locator.text_content() or "").strip()

                key_name = f"project_address_{label.lower().replace('/', '_').replace(' ', '_')}"
                results[key_name] = value_text

            return results

        except Exception as e:
            self.logger.warning(f"❌ Could not extract location fields: {e}")
            return {
                f"project_address_{lbl.lower().replace('/', '_').replace(' ', '_')}": None
                for lbl in target_labels
            }

    async def _extract_promoter_details(self, page: Page) -> Dict[str, str]:
        """
        Extracts all promoter details from the 'Promoter Details' section and
        returns them as a single comma-joined string under 'promoter_details'.
        """
        try:
            header = page.locator("h5.card-title:has-text('Promoter Details')").first
            await header.wait_for(timeout=10000)

            # Prefer the nearest fieldset (most specific container for this card/section)
            section = header.locator("xpath=ancestor::fieldset[1]")
            await section.wait_for(timeout=5000)

            # Only rows inside the scoped section
            # Get the first big row containing promoter info
            outer_row = section.locator("xpath=.//div[contains(@class,'row')][.//label]").first

            # Get each col inside the outer row that has a label
            cols = outer_row.locator("xpath=.//div[contains(@class,'col')][.//label]")

            total_cols = await cols.count()
            details = []

            for i in range(total_cols):
                col = cols.nth(i)
                label_loc = col.locator("label")
                if await label_loc.count() == 0:
                    continue
                label_text = (await label_loc.first.text_content() or "").strip().rstrip(":")
                
                # Find value div/span inside same col
               # Find value div/span inside same col
                value_loc = col.locator("xpath=.//*[self::div or self::span][normalize-space(string(.))!=''][1]")

                # Get the raw text, which includes the duplicated label
                raw_value_text = (await value_loc.first.text_content() or "").strip() if await value_loc.count() > 0 else ""

                # THE FIX: Remove the label text from the raw text to get the clean value
                value_text = raw_value_text.replace(label_text, "").strip()
                
                if label_text and value_text:
                    details.append(f"{label_text} - {value_text}")

            promoter_details_str = ", ".join(details) if details else None

            return {"promoter_details": promoter_details_str}

        except Exception as e:
            self.logger.warning(f"❌ Could not extract Promoter Details: {e}")
            return {"promoter_details": None}

    async def _extract_promoter_address(self, page: Page) -> Dict[str, str]:
        """
        Extracts specific address fields from the 'Promoter Official Communication Address' section.
        """
        address_details = {}
        try:
            # Use the h5 title as a reliable anchor to find the correct section
            header = page.locator("h5:has-text('Promoter Official Communication Address')")
            
            # Navigate from the header to the ancestor <fieldset> which contains the form
            section = header.locator("xpath=ancestor::fieldset[1]")
            await section.wait_for(timeout=5000)

            # Define the exact labels you want to scrape
            fields_to_extract = ['State/UT', 'District', 'Taluka', 'Village', 'Pin Code']

            for field in fields_to_extract:
                # Find the label element that contains the text for the current field
                label_locator = section.locator(f"label:has-text('{field}')")
                value_text = None

                if await label_locator.count() > 0:
                    # Based on your screenshot, the value is in a div nested inside the label's sibling div.
                    # Structure: <label>...</label> <div> <div>VALUE</div> </div>
                    value_locator = label_locator.locator("xpath=./following-sibling::div/div")
                    
                    if await value_locator.count() > 0:
                        value_text = (await value_locator.first.text_content() or "").strip()

                # Create a clean key for the dictionary (e.g., "State/UT" -> "state_ut")
                key_suffix = re.sub(r'[^a-z0-9_]', '', field.lower().replace(' ', '_').replace('/', '_'))
                dict_key = f"promoter_official_communication_address_{key_suffix}"
                address_details[dict_key] = value_text

            return address_details

        except Exception as e:
            self.logger.warning(f"❌ Could not extract Promoter Address details: {e}")
            # On error, return a dictionary with None values to maintain a consistent data structure
            fields_to_extract = ['State/UT', 'District', 'Taluka', 'Village', 'Pin Code']
            return {
                f"promoter_official_communication_address_{re.sub(r'[^a-z0-9_]', '', field.lower().replace(' ', '_').replace('/', '_'))}": None
                for field in fields_to_extract
            }


    # works fine
    async def _extract_all_tab_data(self, page: Page) -> Dict[str, Any]:
        """
        Robust tab extraction:
        - Clicks each tab button
        - Finds the .tabs ancestor of that button
        - Searches the following sibling div[1] and div[2] for nested <table>
        - Waits for table visibility before scraping rows
        Returns a flat dictionary of extracted values.
        """
        self.logger.info("--- Starting Robust Sequential Tab Extraction ---")
        all_tab_data: Dict[str, Any] = {}

        TAB_SELECTOR_MAP = {
            "Partner Details": "partner_details",
            "Promoter Past Experience": "promoter_past_experience",
            "Authorised Signatory": "authorised_signatory",
            "Project Professionals": "project_professionals",
            "SRO Details": "sro_details",
        }

        SKIP_TABS = ["Single Point of Contact", "Allottee Grievance"]

        try:
            tab_buttons = await page.locator(".tabs button").all()
            self.logger.info(f"Found {len(tab_buttons)} tab buttons.")

            for idx, btn in enumerate(tab_buttons, start=1):
                raw_name = (await btn.text_content()) or ""
                tab_name = raw_name.strip()
                self.logger.info(f"Found Tab {idx}: '{tab_name}'")

                if any(skip in tab_name for skip in SKIP_TABS):
                    self.logger.info(f"Skipping Tab {idx} ('{tab_name}') — not required.")
                    continue

                matched_key = None
                for k in TAB_SELECTOR_MAP.keys():
                    if k.lower() in tab_name.lower():
                        matched_key = k
                        break

                if not matched_key:
                    self.logger.info(f"Skipping Tab {idx} ('{tab_name}') — not in target map.")
                    continue

                self.logger.info(f"Processing Tab {idx}: '{tab_name}' (matched {matched_key})")

                try:
                    await btn.scroll_into_view_if_needed()
                    await btn.click(force=True)
                except Exception as e:
                    self.logger.warning(f"Could not click tab button for '{tab_name}': {e}")
                    continue

                try:
                    tabs_container = btn.locator("xpath=ancestor::div[contains(@class,'tabs')]")
                    sibling_candidates = [
                        tabs_container.locator("xpath=following-sibling::div[1]"),
                        tabs_container.locator("xpath=following-sibling::div[2]")
                    ]

                    table_locator = None
                    found_in = None

                    for idx_sib, sib in enumerate(sibling_candidates, start=1):
                        candidate_table = sib.locator("xpath=.//table").first
                        try:
                            await candidate_table.wait_for(state="visible", timeout=5000)
                            table_locator = candidate_table
                            found_in = f"sibling_{idx_sib}"
                            break
                        except:
                            continue

                    if table_locator is None:
                        self.logger.warning(f"No visible table found for tab '{tab_name}' in sibling divs.")
                        continue

                    self.logger.info(f"Table for '{tab_name}' found in: {found_in}.")
                    # Prefer tbody rows if present
                    if await table_locator.locator("tbody tr").count() > 0:
                        rows = await table_locator.locator("tbody tr").all()
                    else:
                        rows = await table_locator.locator("tr").all()

                    self.logger.info(f"Found {len(rows)} rows in table for '{tab_name}'.")

                    # --- per-tab extraction logic ---
                    if matched_key == "Partner Details":
                        names, desigs = [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 2:
                                names.append((await cells[1].text_content() or "").strip())
                                desigs.append((await cells[2].text_content() or "").strip())
                        all_tab_data["partner_name"] = ", ".join(filter(None, names))
                        all_tab_data["partner_designation"] = ", ".join(filter(None, desigs))

                    elif matched_key == "Promoter Past Experience":
                        names, statuses, litigations = [], [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 5:
                                names.append((await cells[1].text_content() or "").strip())
                                statuses.append((await cells[4].text_content() or "").strip())
                                litigations.append((await cells[5].text_content() or "").strip())
                        all_tab_data["promoter_past_project_names"] = ", ".join(filter(None, names))
                        all_tab_data["promoter_past_project_statuses"] = ", ".join(filter(None, statuses))
                        all_tab_data["promoter_past_litigation_statuses"] = ", ".join(filter(None, litigations))

                    elif matched_key == "Authorised Signatory":
                        names, desigs = [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 2:
                                names.append((await cells[1].text_content() or "").strip())
                                desigs.append((await cells[2].text_content() or "").strip())
                        all_tab_data["authorised_signatory_names"] = ", ".join(filter(None, names))
                        all_tab_data["authorised_signatory_designations"] = ", ".join(filter(None, desigs))

                    elif matched_key == "Project Professionals":
                        architects, engineers, others = [], [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 2:
                                prof_type = (await cells[1].text_content() or "").strip().lower()
                                prof_name = (await cells[2].text_content() or "").strip()
                                if "architect" in prof_type:
                                    architects.append(prof_name)
                                elif "engineer" in prof_type:
                                    engineers.append(prof_name)
                                else:
                                    others.append(prof_name)
                        all_tab_data["architect_names"] = ", ".join(filter(None, architects))
                        all_tab_data["engineer_names"] = ", ".join(filter(None, engineers))
                        all_tab_data["other_professional_names"] = ", ".join(filter(None, others))

                    elif matched_key == "SRO Details":
                        sro_names, doc_names = [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 2:
                                sro_names.append((await cells[1].text_content() or "").strip())
                                doc_names.append((await cells[2].text_content() or "").strip())
                        all_tab_data["sro_name"] = ", ".join(filter(None, sro_names))
                        all_tab_data["sro_document_name"] = ", ".join(filter(None, doc_names))

                except Exception as e:
                    self.logger.warning(f"Could not process data in tab '{tab_name}': {e}")

            self.logger.info("--- Finished Robust Sequential Tab Extraction ---")
            return all_tab_data

        except Exception as e:
            self.logger.error(f"❌ Fatal error during tab extraction: {e}")
            return {}

  


    # works fine to extract latest dates
    async def _extract_latest_form_dates(self, page: Page) -> Dict[str, Optional[str]]:
        """
        (REVISED) Extracts the latest dates for 'Form 1' and 'Form 2' from the Document Library.
        """
        self.logger.info("Document Library se dates extract karne ki koshish kar rahe hain...")
        latest_dates = {
            "latest_form1_date": None,
            "latest_form2_date": None
        }

        try:
            from datetime import datetime # <-- THE FIX: Ensure datetime is in scope.
            
            # 1. Use a more specific selector targeting the button inside the h2 tag
            button_selector = 'h2#headingOne >> button[aria-controls="documentLibrary"]'
            self.logger.info(f"Button selector: '{button_selector}'")
            
            button = page.locator(button_selector)
            await button.wait_for(state="visible", timeout=7000)
            self.logger.info("Accordion button dikh raha hai.")

            # 2. Check if the accordion is already open by seeing if the table is visible
            table_selector = 'div#documentLibrary table'
            table = page.locator(table_selector)

            if not await table.is_visible():
                self.logger.info("Accordion band hai. Click karne ki koshish kar rahe hain...")
                await button.scroll_into_view_if_needed() # Ensure button is in viewport
                await button.click()
                
                # 3. IMPORTANT: Wait for the table to become visible as confirmation of the click
                self.logger.info("Click ke baad table ke visible hone ka wait kar rahe hain...")
                await table.wait_for(state="visible", timeout=5000)
                self.logger.info("Table ab visible hai. Click successful.")
            else:
                self.logger.info("Accordion pehle se khula hua hai.")

            rows = await table.locator('tbody tr').all()
            
            parsed_dates = {
                "Form 1": None,
                "Form 2": None
            }

            for row in rows:
                cells = await row.locator('td').all()
                if len(cells) >= 4:
                    document_type = (await cells[1].text_content() or "").strip()
                    created_date_str = (await cells[3].text_content() or "").strip()

                    try:
                        current_date = datetime.strptime(created_date_str, '%d/%m/%Y, %I:%M %p')
                        
                        for form_name in parsed_dates.keys():
                            if form_name in document_type:
                                if parsed_dates[form_name] is None or current_date > parsed_dates[form_name]:
                                    parsed_dates[form_name] = current_date
                                    
                    except ValueError:
                        self.logger.warning(f"Date format galat hai, isko ignore kar rahe hain: '{created_date_str}'")
                        continue
            
            if parsed_dates["Form 1"]:
                latest_dates["latest_form1_date"] = parsed_dates["Form 1"].strftime('%d/%m/%Y, %I:%M %p')
            if parsed_dates["Form 2"]:
                latest_dates["latest_form2_date"] = parsed_dates["Form 2"].strftime('%d/%m/%Y, %I:%M %p')

            self.logger.info(f"Successfully extracted form dates: {latest_dates}")
            return latest_dates

        except Exception as e:
            self.logger.error(f"Document Library se dates extract nahi kar paaye: {e}")
            return latest_dates


    # works fine extract building details
    async def _extract_building_details(self, page: Page) -> Dict[str, Any]:
        """
        Extracts data from the 'Building Details' table with normalized header matching.
        Handles a dynamic number of columns and checks for a 'View' icon.
        """
        self.logger.info("Building Details table se data extract kar rahe hain...")

        # --- Utility function to normalize header text ---
        def normalize(text: str) -> str:
            return " ".join(text.split()).strip().lower()

        # Original map but we'll normalize keys before lookup
        header_key_map = {
            "Identification of Building/ Wing as per Sanctioned Plan": "building_identification_plan",
            "Identification of Wing as per Sanctioned Plan": "wing_identification_plan",
            "Number of Sanctioned Floors (Including Basement+ Stilt+ Podium+ Service+ Habitable excluding terrace)": "sanctioned_floors",
            "Total No. of Building Sanctioned Habitable Floor": "sanctioned_habitable_floors",
            "Sanctioned Apartments / Unit (NR+R)": "sanctioned_apartments",
            "CC Issued up-to (No. of Floors)": "cc_issued_floors",
            "View": "view_document_available"
        }

        # Normalized lookup map
        normalized_header_map = {normalize(k): v for k, v in header_key_map.items()}

        # Initialize data storage
        building_data = {key: [] for key in header_key_map.values()}

        try:
            # Find the container with 'Building Details' text
            container = page.locator("div.white-box:has(b:has-text('Building Details'))")
            await container.wait_for(timeout=7000)

            table = container.locator("table")
            await table.wait_for(timeout=5000)

            # Extract and normalize actual headers from the DOM
            header_elements = await table.locator("thead th").all()
            actual_headers = [(await h.text_content() or "").strip() for h in header_elements]
            actual_headers = [h for h in actual_headers if h != '#']  # drop serial number column

           
            rows = await table.locator("tbody tr").all()
            

            for row in rows:
                # Skip any summary rows
                if "Total" in (await row.text_content() or ""):
                    continue

                cells = await row.locator("td").all()
                row_cells = cells[1:]  # skip serial number column

                for i, header_text in enumerate(actual_headers):
                    if i < len(row_cells):
                        cell = row_cells[i]
                        dict_key = normalized_header_map.get(normalize(header_text))

                        if not dict_key:
                            continue

                        if normalize(header_text) == normalize("View"):
                            eye_icon = cell.locator("i.bi-eye-fill")
                            is_visible = await eye_icon.count() > 0
                            building_data[dict_key].append(str(is_visible))
                        else:
                            cell_text = (await cell.text_content() or "").strip()
                            building_data[dict_key].append(cell_text)

            # Combine lists into comma-separated strings
            final_data = {key: ", ".join(value) for key, value in building_data.items() if value}

            self.logger.info(f"Successfully extracted Building Details: {final_data}")
            return final_data

        except Exception as e:
            self.logger.error(f"Building Details extract nahi kar paaye: {e}")
            return {key: None for key in header_key_map.values()}

    # works fine 
    async def _extract_apartment_summary(self, page: Page) -> Dict[str, Any]:
        """
        Extracts data from the 'Summary of Apartments/Units' table.
        This function can handle two different versions of the table.
        """
        self.logger.info("Summary of Apartments/Units table se data extract kar rahe hain...")

        # Define all possible keys. They will be populated based on the table found.
        all_keys = {
            "summary_identification_building_wing": None,
            "summary_identification_wing_plan": None,
            "summary_floor_type": None,
            "summary_total_no_of_residential_apartments": None,
            "summary_total_no_of_non_residential_apartments": None,
            "summary_total_no_of_apartments_nr_r": None,
            "summary_total_no_of_sold_units": None,
            "summary_total_no_of_unsold_units": None,
            "summary_total_no_of_booked": None,
            "summary_total_no_of_rehab_units": None,
            "summary_total_no_of_mortgage": None,
            "summary_total_no_of_reservation": None,
            "summary_total_no_of_land_owner_investor_share_sale": None,
            "summary_total_no_of_land_owner_investor_share_not_for_sale": None,
            "total_no_of_apartments": None, # For the simple table
        }

        try:
            # Locate the container for the summary section
           
            container = page.locator("div.white-box:has(b:has-text('Summary of Apartments/Units'))")
            await container.wait_for(timeout=7000)
         
            table = container.locator("table")
            await table.wait_for(timeout=5000)
            

            header_elements = await table.locator("thead th").all()
            header_count = len(header_elements)
          

            # --- Logic to handle the DETAILED table (15 columns) ---
            if header_count > 10:
                
                
                header_map = {
                    "Identification of Building/ Wing as per Sanctioned Plan": "summary_identification_building_wing",
                    "Identification of Wing as per Sanctioned Plan": "summary_identification_wing_plan",
                    "Floor Type": "summary_floor_type",
                    "Total No. Of Residential Apartments/ Units": "summary_total_no_of_residential_apartments",
                    "Total No. Of Non-Residential Apartments/ Units": "summary_total_no_of_non_residential_apartments",
                    "Total Apartments / Unit (NR+R)": "summary_total_no_of_apartments_nr_r",
                    "Total No. of Sold Units": "summary_total_no_of_sold_units",
                    "Total No. of Unsold Units": "summary_total_no_of_unsold_units",
                    "Total No. of Booked": "summary_total_no_of_booked",
                    "Total No. of Rehab Units": "summary_total_no_of_rehab_units",
                    "Total No. of Mortgage": "summary_total_no_of_mortgage",
                    "Total No. of Reservation": "summary_total_no_of_reservation",
                    "Total No. of Land Owner/ Investor Share (For Sale)": "summary_total_no_of_land_owner_investor_share_sale",
                    "Total No. of Land Owner/ Investor Share (Not For Sale)": "summary_total_no_of_land_owner_investor_share_not_for_sale",
                }

                temp_data = {key: [] for key in header_map.values()}
                actual_headers = [(await h.text_content() or "").strip() for h in header_elements if (await h.text_content() or "").strip() != '#']
                
                
                rows = await table.locator("tbody tr").all()
                
                for i, row in enumerate(rows):
                    cells = await row.locator("td").all()
                    if not cells:
                        continue
                    
                    first_cell_text = (await cells[0].text_content() or "").strip()
                    if first_cell_text == "Total":
                        
                        continue
                    
                   
                    row_cells = cells[1:]

                    for j, header_text in enumerate(actual_headers):
                        if j < len(row_cells):
                            cell = row_cells[j]
                            dict_key = header_map.get(header_text)
                            if dict_key:
                                cell_content = (await cell.text_content() or "").strip()
                                
                                temp_data[dict_key].append(cell_content)
                
                for key, values in temp_data.items():
                    all_keys[key] = ", ".join(values)

            # --- Logic to handle the SIMPLE table (5 columns) ---
            elif  header_count ==5:
                
                total_apartments = 0
                rows = await table.locator("tbody tr").all()
               
                for i, row in enumerate(rows):
                    cells = await row.locator("td").all()
                    if len(cells) == 5:
                        try:
                            # 5th column (index 4) has the number
                            num_text = (await cells[4].text_content() or "0").strip()
                            if num_text:
                            # --- THE FIX ---
                            # Correctly indented the following two lines
                                
                                total_apartments += int(num_text)
                            # --- END OF FIX ---
                        except (ValueError, IndexError) as e:
                            
                            continue # Ignore rows that don't have a valid number
                
                
                all_keys["total_no_of_apartments"] = str(total_apartments)

            else:
                self.logger.warning("Apartment summary table ka format anjaan hai.")

            self.logger.info(f"Successfully extracted Apartment Summary: {all_keys}")
            return all_keys

        except Exception as e:
            self.logger.error(f"Apartment Summary extract nahi kar paaye: {e}")
            return all_keys # Return the dictionary with None values on error

    # works fine too
    async def _extract_investor_flag(self, page: Page) -> Dict[str, Any]:
        """
        (REVISED) Check if there are investors other than the promoter in the project.
        """
        self.logger.info("Investor flag ko extract kar rahe hain...")
        
        # Define the key for the output dictionary
        result_key = "are_there_investors_other_than_promoter"
        
        try:
            # --- THE FIX ---
            # 1. Locate the container that holds both the question and the answer.
            # This is more stable than looking for siblings.
            container = page.locator("div.col-sm-12:has(label:has-text('Are there any Investor other than the Promoter'))")
            await container.wait_for(timeout=7000)

            # 2. Within that container, find the label that contains the answer (the one with the <b> tag).
            # This selector looks for a label with the specific class that contains a <b> tag.
            answer_label = container.locator("label.form-label-preview-text > b")
            
            # 3. Extract the text from the <b> tag.
            answer = (await answer_label.inner_text()).strip()
            # --- END OF FIX ---

            self.logger.info(f"Investor flag found: {answer}")
            return {result_key: answer}

        except Exception as e:
            self.logger.warning(f"❌ Could not extract investor info: {e}")
            return {result_key: None}

    #works fine
    async def _extract_litigation_details(self, page: Page) -> Dict[str, Any]:
        """(REVISED) Extract litigation status and count of litigations against the project."""
        self.logger.info("Litigation details ko extract kar rahe hain...")
        result_key = "litigation_against_project_count"
        
        try:
            # --- THE FIX ---
            # A more robust selector: First find the parent container for the whole section.
            litigation_container = page.locator("div.white-box:has(b:has-text('Litigation Details'))")
            await litigation_container.wait_for(timeout=7000)

            # Now, find the specific div that contains the question text inside that container.
            question_container = litigation_container.locator("div:has-text('Is there any litigation against this proposed project :  ')")

            # Finally, get the answer from the bold tag within that question container.
            answer_label = question_container.locator("label.form-label-preview-text  ")
            answer_text = (await answer_label.inner_text()).strip().lower()
            # --- END OF FIX ---

            if answer_text == "no":
                self.logger.info("Litigation answer is 'No'. Count is 0.")
                return {result_key: 0}

            # If the answer is 'Yes', the table should be present within the same main container.
            self.logger.info("Litigation answer is 'Yes'. Table dhoond rahe hain...")
            
            table = litigation_container.locator("div.table-responsive > table")
            await table.wait_for(timeout=5000)

            # Count the rows in the table body
            rows = table.locator("tbody > tr")
            row_count = await rows.count()
            
            # Handle the case where the table might be visible but empty (e.g., only a "No Data Found" row)
            if row_count == 1:
                first_row_text = (await rows.first.text_content() or "").lower()
                if "no data" in first_row_text or "no record" in first_row_text:
                    self.logger.info("Litigation table mein 'No Data' row mili. Count is 0.")
                    return {result_key: 0}

            self.logger.info(f"Litigation table mein {row_count} rows mili.")
            return {result_key: row_count}

        except Exception as e:
            self.logger.warning(f"❌ Could not extract litigation info: {e}")
            return {result_key: None}

    #works fine
    async def _extract_parking_details(self, page: Page) -> Dict[str, Any]:
        """(REVISED) Extracts Open and Closed space parking details from all tables under Parking Details."""
        self.logger.info("Parking details ko extract kar rahe hain...")
        
        # Define the keys for our final output
        results = {
            "open_space_parking_total": None,
            "closed_space_parking_total": None
        }

        try:
            # 1. Locate the accordion button for Parking Details
            button = page.locator("button:has-text('Parking Details')")
            await button.wait_for(timeout=7000)
            
            # 2. Click the button to expand the section
            parking_section_selector = "div#parkingDetails"
            parking_section = page.locator(parking_section_selector)
            
            if not "show" in (await parking_section.get_attribute("class") or ""):
                self.logger.info("Parking Details accordion band hai, click kar rahe hain...")
                await button.click()
                await expect(parking_section).to_have_class(re.compile(r".*\bshow\b.*"), timeout=5000)
                self.logger.info("Parking details section ab visible hai.")
            else:
                self.logger.info("Parking details section pehle se khula hai.")

            # 3. Find all tables within the visible parking section
            tables = parking_section.locator("div.table-responsive > table")
            table_count = await tables.count()
            self.logger.info(f"Parking section mein {table_count} table(s) mili.")

            if table_count == 0:
                return results

            open_counts = []
            closed_counts = []

            # 4. Loop through each table found
            for i in range(table_count):
                table = tables.nth(i)
                rows = await table.locator("tbody tr").all()
                
                open_sum_for_table = 0
                closed_sum_for_table = 0

                # 5. Loop through each row in the current table
                for row in rows:
                    cells = await row.locator("td").all()
                    # --- THE FIX ---
                    # The table now has 8 columns of data, so we check for at least that many
                    if len(cells) < 8: continue 

                    try:
                        # Column 2 (index 1) has the Parking Type
                        parking_type = (await cells[1].inner_text()).strip().lower()
                        # Column 6 (index 5) has the "Total No Of Parking"
                        count_text = (await cells[6].inner_text()).strip()
                        # --- END OF FIX ---
                        
                        count = int(count_text) if count_text.isdigit() else 0

                        if "open" in parking_type:
                            open_sum_for_table += count
                        elif "closed" in parking_type or "covered" in parking_type:
                            closed_sum_for_table += count
                    except (ValueError, IndexError):
                        continue
                
                open_counts.append(str(open_sum_for_table))
                closed_counts.append(str(closed_sum_for_table))

            # 6. Join the counts from all tables into a comma-separated string
            results["open_space_parking_total"] = ", ".join(open_counts)
            results["closed_space_parking_total"] = ", ".join(closed_counts)
            
            self.logger.info(f"Successfully extracted parking details: {results}")
            return results

        except Exception as e:
            self.logger.warning(f"❌ Could not extract parking details: {e}")
            return results

    #works fine
    async def _extract_bank_details(self, page: Page) -> Dict[str, Optional[str]]:
        """
        (REVISED) Extracts Bank Name, IFSC Code, and Bank Address from the new UI.
        """
        self.logger.info("🔍 Extracting Bank Details...")

        # Define the keys for our output dictionary
        result = {
            "bank_name": None,
            "ifsc_code": None,
            "bank_address": None
        }
        
        # This map links the text on the page to the keys in our dictionary
        fields_to_extract = {
            "Bank Name": "bank_name",
            "IFSC Code": "ifsc_code",
            "Bank Address": "bank_address"
        }

        try:
            # 1. Find the main container for the bank details section.
            container = page.locator("project-bank-details-preview fieldset").nth(0)
            await container.wait_for(timeout=7000)

            # 2. Loop through each field we want to extract
            for label_text, dict_key in fields_to_extract.items():
                try:
                    # 3. Find the label element by its text
                    label_locator = container.locator(f"label.form-label:has-text('{label_text}')")
                    
                   
                    value_locator = label_locator.locator("xpath=following-sibling::div[1]")

                    # --- END OF FIX ---
                    
                    value = (await value_locator.inner_text()).strip()
                    result[dict_key] = value
                    self.logger.debug(f"✅ {label_text}: {value}")
                    
                except Exception as inner_e:
                    self.logger.warning(f"⚠️ Could not find bank field '{label_text}': {inner_e}")
                    continue

            self.logger.info(f"✅ Extracted bank details: {result}")
            return result

        except Exception as e:
            self.logger.error(f"❌ Failed to extract bank details section: {e}")
            return result # Return the dictionary with None values

    #works fine
    async def _extract_complaint_details(self, page: Page) -> Dict[str, Any]:
        """
        (REVISED) Extracts the number of complaints and all complaint numbers 
        from the 'Complaint Details' section.
        """
        self.logger.info("🔍 Extracting Complaint Details...")
        
        # Define the keys for our output dictionary
        result = {
            "complaint_count": 0,
            "complaint_numbers": None
        }

        try:
            # 1. Locate the parent container for the 'Complaint Details' section.
            container = page.locator("div.white-box:has(b:has-text('Complaint Details'))")
            await container.wait_for(timeout=7000)
            
            # 2. Find the table within that container.
            table = container.locator("div.table-responsive > table")
            await table.wait_for(timeout=5000)

            # 3. Get all rows from the table body.
            rows = table.locator("tbody tr")
            row_count = await rows.count()

            # 4. Handle cases where there are no complaints.
            if row_count == 0:
                self.logger.info("Complaint table is empty. Count is 0.")
                return result
            
            # Check for a "No Data Found" message in the first row.
            if row_count == 1:
                first_row_text = (await rows.first.text_content() or "").lower()
                if "no data" in first_row_text or "no record" in first_row_text:
                    self.logger.info("Complaint table has a 'No Data' row. Count is 0.")
                    return result

            # 5. Loop through the rows and extract the complaint numbers.
            complaint_numbers = []
            for i in range(row_count):
                row = rows.nth(i)
                cells = await row.locator("td").all()
                
                # The complaint number is in the second column (index 1).
                if len(cells) > 1:
                    complaint_no = (await cells[1].text_content() or "").strip()
                    if complaint_no: # Ensure we don't add empty strings
                        complaint_numbers.append(complaint_no)

            # 6. Finalize the results.
            if complaint_numbers:
                result["complaint_count"] = len(complaint_numbers)
                result["complaint_numbers"] = ", ".join(complaint_numbers)
                
            self.logger.info(f"Successfully extracted {result['complaint_count']} complaint(s).")
            return result

        except Exception as e:
            # If the section doesn't exist or another error occurs, return the default empty result.
            self.logger.warning(f"❌ Could not extract complaint details: {e}")
            return result

    #works fine
    async def _extract_real_estate_agents(self, page: Page) -> Dict[str, Any]:
        """
        (REVISED) Extracts Real Estate Agent details from the new UI with more robust logic.
        """
        self.logger.info("🔍 Extracting Registered Real Estate Agent(s)...")
        
        result = {
            "real_estate_agent_names": None,
            "maharera_certificate_nos": None
        }

        try:
            # 1. Locate the accordion button.
            button = page.locator("button:has-text('Registered Agent(s)')")
            await button.wait_for(timeout=7000)

            # 2. Locate the table using the button's target ID.
            target_id = await button.get_attribute("data-bs-target")
            if not target_id:
                raise Exception("Could not find 'data-bs-target' on the agent accordion button.")
            
            table = page.locator(f"{target_id} div.table-responsive > table")

            # 3. THE FIX: Check if the TABLE is visible. If not, click the button and wait for it.
            if not await table.is_visible():
                self.logger.info("Real Estate Agent table not visible, clicking accordion...")
                await button.click()
                await table.wait_for(state="visible", timeout=5000)
                self.logger.info("Agent table is now visible.")
            else:
                self.logger.info("Agent table was already visible.")
            # --- END OF FIX ---

            # 4. Get all data rows from the table body.
            rows = table.locator("tbody tr")
            row_count = await rows.count()

            # 5. Handle cases with no agents listed.
            if row_count == 0:
                self.logger.info("Agent table is empty. Skipping.")
                return result
            
            if row_count == 1:
                first_row_text = (await rows.first.text_content() or "").lower()
                if "no data" in first_row_text or "no record" in first_row_text:
                    self.logger.info("Agent table has a 'No Data' row. Skipping.")
                    return result

            # 6. Loop through rows and extract the required data.
            agent_names = []
            cert_numbers = []
            for i in range(row_count):
                row = rows.nth(i)
                cells = await row.locator("td").all()
                
                if len(cells) > 2:
                    name = (await cells[1].text_content() or "").strip()
                    cert_no = (await cells[2].text_content() or "").strip()
                    if name:
                        agent_names.append(name)
                    if cert_no:
                        cert_numbers.append(cert_no)

            # 7. Finalize and return the comma-separated results.
            if agent_names:
                result["real_estate_agent_names"] = ", ".join(agent_names)
            if cert_numbers:
                result["maharera_certificate_nos"] = ", ".join(cert_numbers)
                
            self.logger.info(f"Successfully extracted {len(agent_names)} real estate agent(s).")
            return result

        except Exception as e:
            self.logger.warning(f"❌ Could not extract real estate agent details: {e}")
            return result
