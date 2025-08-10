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
                # self._extract_partner_details(page),
                # self._extract_promoter_past_experience(page),
                # self._extract_authorised_signatory(page),

                self._extract_latest_form_dates(page),
                self._extract_building_details(page),
                self._extract_apartment_summary(page),



                # self._extract_project_professionals(page),
                # self._extract_sro_details(page),
                # self._extract_landowner_type(page),
                # self._extract_investor_flag(page),
                # self._extract_litigation_details(page),
                # self._extract_building_details(page),
                # self._extract_parking_details(page),
                # self._extract_bank_details(page),
                # self._extract_complaint_details(page),
                # self._extract_real_estate_agents(page)
                
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

    # async def _switch_to_tab(self, page: Page, tab_name: str):
    #     """
    #     Clicks a specific tab button by its visible text.
    #     Does NOT wait for any table — that should be handled by the caller.
    #     """
    #     try:
    #         button = page.locator(f"div.tabs button:has-text('{tab_name}')")
    #         if await button.count() == 0:
    #             raise ValueError(f"No tab button found with name '{tab_name}'")

    #         await button.first.click()

    #         await expect(button.first).to_have_class(re.compile(r".*\bactive\b.*"), timeout=5000)

    #         # Optional: Wait for the tab to become 'active' (CSS class check)
    #         # try:
    #         #     await expect(button.first).to_have_class(re.compile(r".*\bactive\b.*"), timeout=3000)
    #         # except:
    #         #     self.logger.debug(f"Tab '{tab_name}' did not get 'active' class quickly.")

    #     except Exception as e:
    #         self.logger.error(f"Failed to switch to tab '{tab_name}': {e}")
    #         raise

    # async def _extract_partner_details(self, page: Page) -> Dict[str, Any]:
    #     default_return = {'partner_name': None, 'partner_designation': None}

    #     try:
    #         first_button = page.locator("div.tabs button").first
    #         if await first_button.count() == 0:
    #             self.logger.warning("No partner/director tab button found.")
    #             return default_return

    #         tab_name = (await first_button.text_content() or "").strip()
    #         if not tab_name:
    #             self.logger.warning("First tab button has no text.")
    #             return default_return

    #         # Switch to that tab (no table wait here)
    #         await self._switch_to_tab(page, tab_name)

    #         # Now check for table presence
    #         table = page.locator("project-personnel-modal-preview table")
    #         if await table.count() == 0:
    #             self.logger.info(f"No table found under tab '{tab_name}'.")
    #             return default_return

    #         # Wait for at least one cell, but handle no-data case gracefully
    #         try:
    #             await table.locator("tbody tr td").first.wait_for(timeout=5000)
    #         except:
    #             self.logger.info(f"Table under '{tab_name}' has no rows.")
    #             return default_return

    #         rows = table.locator("tbody tr")
    #         row_count = await rows.count()
    #         if row_count == 0:
    #             return default_return

    #         name_list, designation_list = [], []
    #         for i in range(row_count):
    #             row = rows.nth(i)
    #             name_text = (await row.locator("td").nth(1).text_content() or "").strip()
    #             designation_text = (await row.locator("td").nth(2).text_content() or "").strip()

    #             if name_text:
    #                 name_list.append(name_text)
    #             if designation_text:
    #                 designation_list.append(designation_text)

    #         return {
    #             'partner_name': ', '.join(name_list) if name_list else None,
    #             'partner_designation': ', '.join(designation_list) if designation_list else None
    #         }

    #     except Exception as e:
    #         self.logger.warning(f"❌ Could not extract Partner/Director Details: {e}")
    #         return default_return


    # async def _extract_promoter_past_experience(self, page: Page) -> dict:
    #     """
    #     Clicks the 'Past Experience' tab and extracts:
    #     - promoter_past_project_names (col 2)
    #     - promoter_past_project_statuses (col 5)
    #     - promoter_past_litigation_statuses (col 6)
    #     Returns None for each if table is missing or empty.
    #     """
    #     default_return = {
    #         "promoter_past_project_names": None,
    #         "promoter_past_project_statuses": None,
    #         "promoter_past_litigation_statuses": None
    #     }

    #     try:
    #         # Click the "Past Experience" tab
    #         past_exp_btn = page.locator("div.tabs button:has-text('Promoter Past Experience Details')")
    #         if await past_exp_btn.count() == 0:
    #             self.logger.warning("Past Experience tab not found.")
    #             return default_return

    #         await past_exp_btn.first.click()

    #         # Wait for the tab to become active
    #         await expect(past_exp_btn.first).to_have_class(re.compile(r".*\bactive\b.*"))

    #         # Wait for table rows
    #         table_rows = page.locator("project-legal-past-experience-preview table tbody tr")
    #         try:
    #             await table_rows.first.wait_for(timeout=5000)
    #         except:
    #             self.logger.info("Past Experience table not found or empty.")
    #             return default_return

    #         # Collect data
    #         names, statuses, litigations = [], [], []
    #         total_rows = await table_rows.count()

    #         for i in range(total_rows):
    #             row = table_rows.nth(i)
    #             col2 = (await row.locator("td").nth(1).text_content() or "").strip()
    #             col5 = (await row.locator("td").nth(4).text_content() or "").strip()
    #             col6 = (await row.locator("td").nth(5).text_content() or "").strip()

    #             if col2:
    #                 names.append(col2)
    #             if col5:
    #                 statuses.append(col5)
    #             if col6:
    #                 litigations.append(col6)

    #         return {
    #             "promoter_past_project_names": ", ".join(names) if names else None,
    #             "promoter_past_project_statuses": ", ".join(statuses) if statuses else None,
    #             "promoter_past_litigation_statuses": ", ".join(litigations) if litigations else None
    #         }

    #     except Exception as e:
    #         self.logger.warning(f"❌ Could not extract Promoter Past Experience: {e}")
    #         return default_return


    # async def _extract_authorised_signatory(self, page: Page) -> Dict[str, Any]:
    #     """Extract authorised signatory details: Professional Name and Designation."""
    #     default_return = {
    #         "authorised_signatory_names": None,
    #         "authorised_signatory_designations": None
    #     }
    #     try:
    #         # Switch using the reusable helper
    #         await self._switch_to_tab(page, " Authorised Signatory ")

    #         # Locate the table
    #         table = page.locator("div.table-responsive").nth(2).locator("table")
    #         rows = table.locator("tbody tr")
    #         row_count = await rows.count()

    #         # Handle "No Record Found"
    #         if row_count == 1:
    #             only_row_text = (await rows.nth(0).inner_text()).strip().lower()
    #             if "no record found" in only_row_text:
    #                 return default_return

    #         professional_names = []
    #         designations = []

    #         for i in range(row_count):
    #             cells = rows.nth(i).locator("td")
    #             if await cells.count() < 3:
    #                 continue
    #             name = (await cells.nth(1).inner_text()).strip()
    #             designation = (await cells.nth(2).inner_text()).strip()
    #             professional_names.append(name)
    #             designations.append(designation)

    #         return {
    #             "authorised_signatory_names": ", ".join(professional_names) if professional_names else None,
    #             "authorised_signatory_designations": ", ".join(designations) if designations else None
    #         }

    #     except Exception as e:
    #         self.logger.warning(f"❌ Could not extract Authorised Signatory data: {e}")
    #         return default_return


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


    # async def _extract_apartment_summary(self, page: Page) -> Dict[str, Any]:
    #     """
    #     Extracts data from the 'Summary of Apartments/Units' table.
    #     This function can handle two different versions of the table.
    #     """
    #     self.logger.info("Summary of Apartments/Units table se data extract kar rahe hain...")

    #     # Define all 15 possible keys. They will be populated based on the table found.
    #     all_keys = {
    #         "summary_identification_building_wing": None,
    #         "summary_identification_wing_plan": None,
    #         "summary_floor_type": None,
    #         "summary_total_no_of_residential_apartments": None,
    #         "summary_total_no_of_non_residential_apartments": None,
    #         "summary_total_no_of_apartments_nr_r": None,
    #         "summary_total_no_of_sold_units": None,
    #         "summary_total_no_of_booked_units": None,
    #         "summary_total_no_of_rehab_units": None,
    #         "summary_total_no_of_mortgage": None,
    #         "summary_total_no_of_reservation": None,
    #         "summary_total_no_of_land_owner_investor_share_sale": None,
    #         "summary_total_no_of_land_owner_investor_share": None,
    #         "summary_view": None,
    #         "total_no_of_apartments": None, # For the simple table
    #     }

    #     try:
    #         # Locate the container for the summary section
    #         self.logger.info("Apartment summary section ke container ko dhoond rahe hain...")
    #         container = page.locator("div.white-box:has(b:has-text('Summary of Apartments/Units'))")
    #         await container.wait_for(timeout=7000)
    #         self.logger.info("Container mil gaya. Table dhoond rahe hain...")
    #         table = container.locator("table")
    #         await table.wait_for(timeout=5000)
    #         self.logger.info("Table mil gayi.")

    #         header_elements = await table.locator("thead th").all()
    #         header_count = len(header_elements)
    #         self.logger.info(f"Table mein {header_count} columns mile.")

    #         # --- Logic to handle the DETAILED table (15 columns) ---
    #         if header_count > 10:
    #             self.logger.info("Detailed apartment summary table (15 columns) mili.")
                
    #             header_map = {
    #                 "Identification of Building/ Wing as per Sanctioned Plan": "summary_identification_building_wing",
    #                 "Identification of Wing as per Sanctioned Plan": "summary_identification_wing_plan",
    #                 "Floor Type": "summary_floor_type",
    #                 "Total No. Of Residential Apartments/ Units": "summary_total_no_of_residential_apartments",
    #                 "Total No. Of Non-Residential Apartments/ Units": "summary_total_no_of_non_residential_apartments",
    #                 "Total No. Of Apartments / Unit (NR+R)": "summary_total_no_of_apartments_nr_r",
    #                 "Total No. of Sold Units": "summary_total_no_of_sold_units",
    #                 "Total No. of Booked Units": "summary_total_no_of_booked_units",
    #                 "Total No. of Rehab Units": "summary_total_no_of_rehab_units",
    #                 "Total No. of Mortgage": "summary_total_no_of_mortgage",
    #                 "Total No. of Reservation": "summary_total_no_of_reservation",
    #                 "Total No. of Land Owner/ Investor Share (For Sale)": "summary_total_no_of_land_owner_investor_share_sale",
    #                 "Total No. of Land Owner/ Investor Share": "summary_total_no_of_land_owner_investor_share",
    #                 "View": "summary_view",
    #             }

    #             temp_data = {key: [] for key in header_map.values()}
    #             actual_headers = [(await h.text_content() or "").strip() for h in header_elements if (await h.text_content() or "").strip() != '#']
    #             self.logger.info(f"Actual Headers found: {actual_headers}")
                
    #             rows = await table.locator("tbody tr").all()
    #             self.logger.info(f"Table mein {len(rows)} rows mili.")
    #             for i, row in enumerate(rows):
    #                 row_text = (await row.text_content() or "").strip()
    #                 if "Total" in row_text:
    #                     self.logger.info(f"Row {i+1} ko ignore kar rahe hain (Total row).")
    #                     continue
                    
    #                 self.logger.info(f"Row {i+1} ko process kar rahe hain: {row_text[:100]}")
    #                 cells = await row.locator("td").all()
    #                 row_cells = cells[1:]

    #                 for j, header_text in enumerate(actual_headers):
    #                     if j < len(row_cells):
    #                         cell = row_cells[j]
    #                         dict_key = header_map.get(header_text)
    #                         if dict_key:
    #                             cell_content = (await cell.text_content() or "").strip()
    #                             self.logger.debug(f"  - Header '{header_text}' -> Key '{dict_key}': Value '{cell_content}'")
    #                             temp_data[dict_key].append(cell_content)
                
    #             for key, values in temp_data.items():
    #                 all_keys[key] = ", ".join(values)

    #         # --- Logic to handle the SIMPLE table (5 columns) ---
    #         elif header_count > 3:
    #             self.logger.info("Simple apartment summary table (5 columns) mili.")
    #             total_apartments = 0
    #             rows = await table.locator("tbody tr").all()
    #             self.logger.info(f"Table mein {len(rows)} rows mili.")
    #             for i, row in enumerate(rows):
    #                 cells = await row.locator("td").all()
    #                 if len(cells) == 5:
    #                     try:
    #                         # 5th column (index 4) has the number
    #                         num_text = (await cells[4].text_content() or "0").strip()
    #                         self.logger.debug(f"Row {i+1}, 5th column se value mili: '{num_text}'")
    #                         total_apartments += int(num_text)
    #                     except (ValueError, IndexError) as e:
    #                         self.logger.warning(f"Row {i+1} mein number parse nahi kar paaye: {e}")
    #                         continue # Ignore rows that don't have a valid number
                
    #             self.logger.info(f"Total apartments calculate kiye gaye: {total_apartments}")
    #             all_keys["total_no_of_apartments"] = str(total_apartments)

    #         else:
    #             self.logger.warning("Apartment summary table ka format anjaan hai.")

    #         self.logger.info(f"Successfully extracted Apartment Summary: {all_keys}")
    #         return all_keys

    #     except Exception as e:
    #         self.logger.error(f"Apartment Summary extract nahi kar paaye: {e}")
    #         return all_keys # Return the dictionary with None values on error


    # async def _extract_apartment_summary(self, page: Page) -> Dict[str, Any]:
    #     """
    #     Extracts data from the 'Summary of Apartments/Units' table.
    #     This function can handle two different versions of the table.
    #     """
    #     self.logger.info("Summary of Apartments/Units table se data extract kar rahe hain...")

    #     # Define all possible keys. They will be populated based on the table found.
    #     all_keys = {
    #         "summary_identification_building_wing": None,
    #         "summary_identification_wing_plan": None,
    #         "summary_floor_type": None,
    #         "summary_total_no_of_residential_apartments": None,
    #         "summary_total_no_of_non_residential_apartments": None,
    #         "summary_total_no_of_apartments_nr_r": None,
    #         "summary_total_no_of_sold_units": None,
    #         "summary_total_no_of_unsold_units": None, # Added
    #         "summary_total_no_of_booked": None, # Changed
    #         "summary_total_no_of_rehab_units": None,
    #         "summary_total_no_of_mortgage": None,
    #         "summary_total_no_of_reservation": None,
    #         "summary_total_no_of_land_owner_investor_share_sale": None,
    #         "summary_total_no_of_land_owner_investor_share_not_for_sale": None, # Added
    #         "summary_view": None,
    #         "total_no_of_apartments": None, # For the simple table
    #     }

    #     try:
    #         # Locate the container for the summary section
    #         self.logger.info("Apartment summary section ke container ko dhoond rahe hain...")
    #         container = page.locator("div.white-box:has(b:has-text('Summary of Apartments/Units'))")
    #         await container.wait_for(timeout=7000)
    #         self.logger.info("Container mil gaya. Table dhoond rahe hain...")
    #         table = container.locator("table")
    #         await table.wait_for(timeout=5000)
    #         self.logger.info("Table mil gayi.")

    #         header_elements = await table.locator("thead th").all()
    #         header_count = len(header_elements)
    #         self.logger.info(f"Table mein {header_count} columns mile.")

    #         # --- Logic to handle the DETAILED table (15 columns) ---
    #         if header_count > 10:
    #             self.logger.info("Detailed apartment summary table (15 columns) mili.")
                
    #             # --- THE FIX ---
    #             # Updated the header map to exactly match the headers from the logs
    #             header_map = {
    #                 "Identification of Building/ Wing as per Sanctioned Plan": "summary_identification_building_wing",
    #                 "Identification of Wing as per Sanctioned Plan": "summary_identification_wing_plan",
    #                 "Floor Type": "summary_floor_type",
    #                 "Total No. Of Residential Apartments/ Units": "summary_total_no_of_residential_apartments",
    #                 "Total No. Of Non-Residential Apartments/ Units": "summary_total_no_of_non_residential_apartments",
    #                 "Total Apartments / Unit (NR+R)": "summary_total_no_of_apartments_nr_r",
    #                 "Total No. of Sold Units": "summary_total_no_of_sold_units",
    #                 "Total No. of Unsold Units": "summary_total_no_of_unsold_units",
    #                 "Total No. of Booked": "summary_total_no_of_booked",
    #                 "Total No. of Rehab Units": "summary_total_no_of_rehab_units",
    #                 "Total No. of Mortgage": "summary_total_no_of_mortgage",
    #                 "Total No. of Reservation": "summary_total_no_of_reservation",
    #                 "Total No. of Land Owner/ Investor Share (For Sale)": "summary_total_no_of_land_owner_investor_share_sale",
    #                 "Total No. of Land Owner/ Investor Share (Not For Sale)": "summary_total_no_of_land_owner_investor_share_not_for_sale",
    #                 "View": "summary_view",
    #             }
    #             # --- END OF FIX ---

    #             temp_data = {key: [] for key in header_map.values()}
    #             actual_headers = [(await h.text_content() or "").strip() for h in header_elements if (await h.text_content() or "").strip() != '#']
    #             self.logger.info(f"Actual Headers found: {actual_headers}")
                
    #             rows = await table.locator("tbody tr").all()
    #             self.logger.info(f"Table mein {len(rows)} rows mili.")
    #             for i, row in enumerate(rows):
    #                 cells = await row.locator("td").all()
    #                 if not cells:
    #                     continue
                    
    #                 first_cell_text = (await cells[0].text_content() or "").strip()
    #                 if first_cell_text == "Total":
    #                     self.logger.info(f"Row {i+1} ko ignore kar rahe hain (Total row).")
    #                     continue
                    
    #                 self.logger.info(f"Row {i+1} ko process kar rahe hain: {(await row.text_content() or '' )[:100]}")
    #                 row_cells = cells[1:]

    #                 for j, header_text in enumerate(actual_headers):
    #                     if j < len(row_cells):
    #                         cell = row_cells[j]
    #                         dict_key = header_map.get(header_text)
    #                         if dict_key:
    #                             cell_content = (await cell.text_content() or "").strip()
    #                             self.logger.debug(f"  - Header '{header_text}' -> Key '{dict_key}': Value '{cell_content}'")
    #                             temp_data[dict_key].append(cell_content)
                
    #             for key, values in temp_data.items():
    #                 all_keys[key] = ", ".join(values)

    #         # --- Logic to handle the SIMPLE table (5 columns) ---
    #         elif header_count > 3:
    #             self.logger.info("Simple apartment summary table (5 columns) mili.")
    #             total_apartments = 0
    #             rows = await table.locator("tbody tr").all()
    #             self.logger.info(f"Table mein {len(rows)} rows mili.")
    #             for i, row in enumerate(rows):
    #                 cells = await row.locator("td").all()
    #                 if len(cells) == 5:
    #                     try:
    #                         # 5th column (index 4) has the number
    #                         num_text = (await cells[4].text_content() or "0").strip()
    #                         self.logger.debug(f"Row {i+1}, 5th column se value mili: '{num_text}'")
    #                         total_apartments += int(num_text)
    #                     except (ValueError, IndexError) as e:
    #                         self.logger.warning(f"Row {i+1} mein number parse nahi kar paaye: {e}")
    #                         continue # Ignore rows that don't have a valid number
                
    #             self.logger.info(f"Total apartments calculate kiye gaye: {total_apartments}")
    #             all_keys["total_no_of_apartments"] = str(total_apartments)

    #         else:
    #             self.logger.warning("Apartment summary table ka format anjaan hai.")

    #         self.logger.info(f"Successfully extracted Apartment Summary: {all_keys}")
    #         return all_keys

    #     except Exception as e:
    #         self.logger.error(f"Apartment Summary extract nahi kar paaye: {e}")
    #         return all_keys # Return the dictionary with None values on error


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
            self.logger.info("Apartment summary section ke container ko dhoond rahe hain...")
            container = page.locator("div.white-box:has(b:has-text('Summary of Apartments/Units'))")
            await container.wait_for(timeout=7000)
            self.logger.info("Container mil gaya. Table dhoond rahe hain...")
            table = container.locator("table")
            await table.wait_for(timeout=5000)
            self.logger.info("Table mil gayi.")

            header_elements = await table.locator("thead th").all()
            header_count = len(header_elements)
            self.logger.info(f"Table mein {header_count} columns mile.")

            # --- Logic to handle the DETAILED table (15 columns) ---
            if header_count > 10:
                self.logger.info("Detailed apartment summary table (15 columns) mili.")
                
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
                self.logger.info(f"Actual Headers found: {actual_headers}")
                
                rows = await table.locator("tbody tr").all()
                self.logger.info(f"Table mein {len(rows)} rows mili.")
                for i, row in enumerate(rows):
                    cells = await row.locator("td").all()
                    if not cells:
                        continue
                    
                    first_cell_text = (await cells[0].text_content() or "").strip()
                    if first_cell_text == "Total":
                        self.logger.info(f"Row {i+1} ko ignore kar rahe hain (Total row).")
                        continue
                    
                    self.logger.info(f"Row {i+1} ko process kar rahe hain: {(await row.text_content() or '' )[:100]}")
                    row_cells = cells[1:]

                    for j, header_text in enumerate(actual_headers):
                        if j < len(row_cells):
                            cell = row_cells[j]
                            dict_key = header_map.get(header_text)
                            if dict_key:
                                cell_content = (await cell.text_content() or "").strip()
                                self.logger.debug(f"  - Header '{header_text}' -> Key '{dict_key}': Value '{cell_content}'")
                                temp_data[dict_key].append(cell_content)
                
                for key, values in temp_data.items():
                    all_keys[key] = ", ".join(values)

            # --- Logic to handle the SIMPLE table (5 columns) ---
            elif  header_count ==5:
                self.logger.info("Simple apartment summary table (5 columns) mili.")
                total_apartments = 0
                rows = await table.locator("tbody tr").all()
                self.logger.info(f"Table mein {len(rows)} rows mili.")
                for i, row in enumerate(rows):
                    cells = await row.locator("td").all()
                    if len(cells) == 5:
                        try:
                            # 5th column (index 4) has the number
                            num_text = (await cells[4].text_content() or "0").strip()
                            if num_text:
                            # --- THE FIX ---
                            # Correctly indented the following two lines
                                self.logger.debug(f"Row {i+1}, 5th column se value mili: '{num_text}'")
                                total_apartments += int(num_text)
                            # --- END OF FIX ---
                        except (ValueError, IndexError) as e:
                            self.logger.warning(f"Row {i+1} mein number parse nahi kar paaye: {e}")
                            continue # Ignore rows that don't have a valid number
                
                self.logger.info(f"Total apartments calculate kiye gaye: {total_apartments}")
                all_keys["total_no_of_apartments"] = str(total_apartments)

            else:
                self.logger.warning("Apartment summary table ka format anjaan hai.")

            self.logger.info(f"Successfully extracted Apartment Summary: {all_keys}")
            return all_keys

        except Exception as e:
            self.logger.error(f"Apartment Summary extract nahi kar paaye: {e}")
            return all_keys # Return the dictionary with None values on error
