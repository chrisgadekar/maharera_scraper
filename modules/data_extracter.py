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

    async def _switch_to_tab(self, page: Page, tab_name: str):
        """
        Clicks a specific tab button by its visible text and waits for the
        first table row under that tab to appear.
        """
        try:
            # Locate the tab button by visible text
            button = page.locator(f"div.tabs button:has-text('{tab_name}')")
            if await button.count() == 0:
                raise ValueError(f"No tab button found with name '{tab_name}'")

            await button.first.click()

            # Wait for a table row to appear — more reliable than networkidle
            await page.locator("table tbody tr td").first.wait_for(timeout=7000)

        except Exception as e:
            self.logger.error(f"Failed to switch to or load tab '{tab_name}': {e}")
            raise


    async def _extract_partner_details(self, page: Page) -> Dict[str, Any]:
        """
        Clicks the first visible tab in the tabs container and scrapes the
        'Name' and 'Designation' columns from the resulting table.
        """
        default_return = {'partner_name': None, 'partner_designation': None}

        try:
            # Get the first tab button inside the container
            first_button = page.locator("div.tabs button").first
            if await first_button.count() == 0:
                self.logger.warning("No partner/director tab button found.")
                return default_return

            tab_name = (await first_button.text_content() or "").strip()
            if not tab_name:
                self.logger.warning("First tab button has no text.")
                return default_return

            # Switch to that tab and wait for table
            await self._switch_to_tab(page, tab_name)

            # Locate the table inside the content panel
            table = page.locator("project-personnel-modal-preview table")
            if await table.count() == 0:
                self.logger.info(f"No table found under tab '{tab_name}'.")
                return default_return

            rows = table.locator("tbody tr")
            row_count = await rows.count()
            if row_count == 0:
                return default_return

            # Extract names and designations
            name_list, designation_list = [], []
            for i in range(row_count):
                row = rows.nth(i)
                name_text = (await row.locator("td").nth(1).text_content() or "").strip()
                designation_text = (await row.locator("td").nth(2).text_content() or "").strip()

                if name_text:
                    name_list.append(name_text)
                if designation_text:
                    designation_list.append(designation_text)

            return {
                'partner_name': ', '.join(name_list) if name_list else None,
                'partner_designation': ', '.join(designation_list) if designation_list else None
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract Partner/Director Details: {e}")
            return default_return
    

    async def _extract_promoter_past_experience(self, page: Page) -> Dict[str, Any]:
        """Extract promoter past experience: Name, Project Status, Litigation Status"""
        try:
            # Locate the table under the Past Experience section
            section = page.locator("project-legal-past-experience-preview")
            table = section.locator("table.table-bordered.table-striped")

            # Get all rows from tbody
            rows = table.locator("tbody tr")
            count = await rows.count()

            names = []
            statuses = []
            litigations = []

            for i in range(count):
                row = rows.nth(i)
                cols = row.locator("td")

                name = await cols.nth(1).inner_text()        # Project Name
                status = await cols.nth(4).inner_text()      # Project Status
                litigation = await cols.nth(5).inner_text()  # Litigation Status

                names.append(name.strip())
                statuses.append(status.strip())
                litigations.append(litigation.strip())

            return {
                "promoter_past_project_names": ", ".join(names),
                "promoter_past_project_statuses": ", ".join(statuses),
                "promoter_past_litigation_statuses": ", ".join(litigations),
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract Promoter Past Experience: {e}")
            return {
                "promoter_past_project_names": None,
                "promoter_past_project_statuses": None,
                "promoter_past_litigation_statuses": None
            }


    async def _extract_authorised_signatory(self, page: Page) -> Dict[str, Any]:
        """Extract authorised signatory details: Professional Name and Designation."""
        try:
            # Locate the bold heading
            heading = page.locator("b:has-text('Authorised Signatory')")
            await heading.wait_for(timeout=5000)

            # Navigate to the sibling div that contains the table
            container_div = heading.locator("xpath=parent::div/following-sibling::div[1]")
            table = container_div.locator("table.table-bordered")

            # Wait for the first table row to appear or check for "No Record"
            await table.wait_for(timeout=5000)

            rows = table.locator("tbody tr")
            row_count = await rows.count()

            if row_count == 1:
                # Check if this row is "No Record Found"
                only_row_text = await rows.nth(0).inner_text()
                if "no record found" in only_row_text.lower():
                    return {
                        "authorised_signatory_names": None,
                        "authorised_signatory_designations": None
                    }

            professional_names = []
            designations = []

            for i in range(row_count):
                row = rows.nth(i)
                cells = row.locator("td")

                if await cells.count() < 3:
                    continue  # Skip incomplete rows

                name = await cells.nth(1).inner_text()
                designation = await cells.nth(2).inner_text()

                professional_names.append(name.strip())
                designations.append(designation.strip())

            return {
                "authorised_signatory_names": ", ".join(professional_names),
                "authorised_signatory_designations": ", ".join(designations)
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract Authorised Signatory data: {e}")
            return {
                "authorised_signatory_names": None,
                "authorised_signatory_designations": None
            }

    async def _extract_project_professionals(self, page: Page) -> dict:
        data = {
            "Architect": None,
            "Engineer": None,
            "Other": None
        }

        try:
            await page.click("text=Click to see list")

            # Wait for the accordion section to expand
            await page.wait_for_selector("#collapseProjectProfessionals.show", timeout=5000)

            # Retry loop to wait for rows if needed
            for _ in range(5):
                rows = await page.query_selector_all("#collapseProjectProfessionals table tbody tr")
                if rows:
                    break
                await page.wait_for_timeout(500)  # wait 0.5s and retry

            if not rows:
                self.logger.warning("❌ No rows found in Project Professionals section.")
                return data

            architect_names = []
            engineer_names = []
            other_names = []

            for row in rows:
                cols = await row.query_selector_all("td")
                if len(cols) < 3:
                    continue

                prof_type = (await cols[1].inner_text()).strip()
                name = (await cols[2].inner_text()).strip()

                if prof_type.lower() == "real estate agent":
                    continue
                elif prof_type.lower() == "architect":
                    architect_names.append(name)
                elif prof_type.lower() == "engineer":
                    engineer_names.append(name)
                else:
                    other_names.append(name)

            data["Architect"] = ", ".join(architect_names) if architect_names else None
            data["Engineer"] = ", ".join(engineer_names) if engineer_names else None
            data["Other"] = ", ".join(other_names) if other_names else None

        except Exception as e:
            self.logger.warning(f"❌ Could not extract project professionals: {e}")

        return data
    

    async def _extract_sro_details(self, page: Page) -> Dict[str, Any]:
        """Extract SRO Details: Promoter Project Member Number and SRO Membership Type Name."""
        try:
            # Locate <b>SRO Details</b> inside a div
            sro_header = page.locator("div.text-align-left:has(b:text('SRO Details'))")
            await sro_header.wait_for(timeout=5000)

            # Get the <hr> tag that comes after the header
            hr = sro_header.locator("xpath=following-sibling::hr[1]")
            await hr.wait_for(timeout=5000)

            # Then the div that comes right after <hr> and contains the table
            container_div = hr.locator("xpath=following-sibling::div[1]")
            await container_div.wait_for(timeout=5000)

            # Inside that div, find the table
            sro_table = container_div.locator("table.table-bordered")
            await sro_table.wait_for(timeout=5000)

            # Check tbody rows
            rows = sro_table.locator("tbody > tr")
            row_count = await rows.count()

            promoter_numbers = []
            membership_types = []

            for i in range(row_count):
                row = rows.nth(i)
                cells = row.locator("td")
                cell_count = await cells.count()

                # Skip if "No Record Found"
                if cell_count == 1:
                    cell_text = await cells.nth(0).inner_text()
                    if "No Record Found" in cell_text:
                        return {
                            "Promoter Project Member Number": None,
                            "SRO Membership Type Name": None
                        }

                if cell_count >= 3:
                    promoter = await cells.nth(1).inner_text()
                    membership = await cells.nth(2).inner_text()
                    promoter_numbers.append(promoter.strip())
                    membership_types.append(membership.strip())

            return {
                "Promoter Project Member Number": ", ".join(promoter_numbers) if promoter_numbers else None,
                "SRO Membership Type Name": ", ".join(membership_types) if membership_types else None
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract SRO details: {e}")
            return {
                "Promoter Project Member Number": None,
                "SRO Membership Type Name": None
            }
        
    async def _extract_landowner_type(self, page: Page) -> Dict[str, Any]:
        """Extract selected Landowner Type(s) from checkbox group."""
        try:
            # Locate the section containing the label 'Landowner types in the project'
            label_section = page.locator("label:has-text('Landowner types in the project')")
            await label_section.wait_for(timeout=5000)

            # Go up to the container div which wraps all checkboxes
            container_div = label_section.locator("xpath=ancestor::div[contains(@class, 'col-sm-12')]")
            checkboxes = container_div.locator("input.declerationCheckBox")
            labels = container_div.locator("label.form-check-label")

            count = await checkboxes.count()
            selected_types = []

            for i in range(count):
                checkbox = checkboxes.nth(i)
                is_checked = await checkbox.is_checked()

                if is_checked:
                    label = labels.nth(i)
                    text = await label.inner_text()
                    selected_types.append(text.strip())

            return {
                "Landowner Type": ", ".join(selected_types) if selected_types else None
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract landowner type: {e}")
            return {
                "Landowner Type": None
            }


    async def _extract_investor_flag(self, page: Page) -> Dict[str, Any]:
        """Check if there are investors other than promoter in the project."""
        try:
            # Locate the label with the specific question
            question_label = page.locator("label.form-label-preview-label:has-text('Investor other than the Promoter')")
            await question_label.wait_for(timeout=5000)

            # Go to the next label that contains the answer
            answer_label = question_label.locator("xpath=following-sibling::label[@class='form-label-preview-text']")
            bold_text = answer_label.locator("b")
            answer = (await bold_text.inner_text()).strip()

            return {
                "Are there investors other than promoter": answer
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract investor info: {e}")
            return {
                "Are there investors other than promoter": None
            }
        
    async def _extract_litigation_details(self, page: Page) -> Dict[str, Any]:
        """Extract litigation status and number of litigations against the project."""
        try:
            # Step 1: Locate the label with the question
            label_question = page.locator("label.form-label-preview-label:has-text('litigation against this proposed project')")
            await label_question.wait_for(timeout=5000)

            # Step 2: Get the answer label (Yes/No)
            label_answer = label_question.locator("xpath=following-sibling::label[@class='form-label-preview-text']")
            answer_text = (await label_answer.inner_text()).strip().lower()

            if answer_text == "no":
                return {"Litigation against this project": 0}

            # Step 3: If Yes, find the table nearby and count rows
            # Go up to the common container and look for table
            container_div = label_question.locator("xpath=ancestor::div[contains(@class, 'col-sm-12')]")
            table = container_div.locator("xpath=following::table[1]")
            await table.wait_for(timeout=5000)

            rows = table.locator("tbody > tr")
            row_count = await rows.count()

            return {"Litigation against this project": row_count}

        except Exception as e:
            self.logger.warning(f"❌ Could not extract litigation info: {e}")
            return {"Litigation against this project": None}

    async def _extract_building_details(self, page: Page) -> Dict[str, Any]:
        """Extract Identification of Wing and Number of Sanctioned Floors from Building Details."""
        try:
            # Step 1: Locate <b>Building Details</b>
            b_element = page.locator("b:text-is('Building Details')")
            await b_element.wait_for(timeout=5000)

            # Step 2: Go up to its containing div with col-sm-12 class
            container_div = b_element.locator("xpath=ancestor::div[contains(@class, 'col-sm-12')]")
            await container_div.wait_for(timeout=5000)

            # Step 3: Find <hr> immediately after the container
            hr = container_div.locator("xpath=following-sibling::hr[1]")
            await hr.wait_for(timeout=5000)

            # Step 4: Find the table immediately after <hr>
            table = hr.locator("xpath=following-sibling::table[contains(@class, 'table-bordered')]")
            await table.wait_for(timeout=5000)

            # Step 5: Extract rows from tbody
            rows = table.locator("tbody > tr")
            row_count = await rows.count()

            wing_ids = []
            sanctioned_floors = []

            for i in range(row_count):
                row = rows.nth(i)

                # Skip 'Total' row or blank rows
                inner_text = (await row.inner_text()).strip().lower()
                if not inner_text or "total" in inner_text:
                    continue

                cells = row.locator("td")
                if await cells.count() >= 4:
                    wing = await cells.nth(1).inner_text()
                    floor = await cells.nth(3).inner_text()
                    wing_ids.append(wing.strip())
                    sanctioned_floors.append(floor.strip())

            return {
                "Identification of Wing as per Sanctioned Plan": ", ".join(wing_ids) if wing_ids else None,
                "Number of Sanctioned Floors (Incl. Basement+Stilt+Podium+Service+Habitable)": ", ".join(sanctioned_floors) if sanctioned_floors else None
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract building details: {e}")
            return {
                "Identification of Wing as per Sanctioned Plan": None,
                "Number of Sanctioned Floors (Incl. Basement+Stilt+Podium+Service+Habitable)": None
            }
        
    async def _extract_parking_details(self, page: Page) -> Dict[str, Any]:
        """Extracts Open and Closed space parking details from all tables under Parking Details."""
        try:
            button = page.locator("button:has-text('Parking Details')")
            await button.wait_for(timeout=10000)
            await button.click()
            self.logger.info("✅ Clicked on Parking Details accordion.")

            parking_section = page.locator("div#parkingDetails.show")
            await parking_section.wait_for(timeout=7000)
            self.logger.info("✅ Parking details section is visible.")

            tables = parking_section.locator("table.table-bordered")
            table_count = await tables.count()
            self.logger.info(f"🧾 Found {table_count} parking table(s).")

            open_counts = []
            closed_counts = []

            for i in range(table_count):
                table = tables.nth(i)
                rows = table.locator("tbody tr")
                row_count = await rows.count()
                open_sum, closed_sum = 0, 0

                for j in range(row_count):
                    cells = rows.nth(j).locator("td")
                    if await cells.count() < 7: # Make sure the row has enough columns
                        continue

                    # --- THIS IS THE FIX ---
                    # Get the label from the 2nd column (index 1)
                    label = (await cells.nth(1).inner_text()).strip().lower()
                    # Get the value from the 7th column (index 6) - "Total No Of Parking"
                    value_text = (await cells.nth(6).inner_text()).strip()
                    # --- END OF FIX ---

                    try:
                        count_match = re.search(r'\d+', value_text)
                        count = int(count_match.group()) if count_match else 0
                    except (ValueError, AttributeError):
                        count = 0

                    if "open" in label:
                        open_sum += count
                    elif "closed" in label or "covered" in label:
                        closed_sum += count

                open_counts.append(str(open_sum))
                closed_counts.append(str(closed_sum))

            return {
                "Open Space Parking": ", ".join(open_counts) if open_counts else None,
                "Closed Space Parking": ", ".join(closed_counts) if closed_counts else None
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract parking details: {e}")
            return {"Open Space Parking": None, "Closed Space Parking": None}
        

    async def _extract_bank_details(self, page: Page) -> Dict[str, Optional[str]]:
        """
        Extracts Bank Name, IFSC Code, and Bank Address reliably by mapping labels to input/textarea fields.
        """
        try:
            self.logger.info("🔍 Extracting Bank Details...")

            container = page.locator("project-bank-details-preview")
            await container.wait_for(timeout=10000)

            fields_to_extract = {
                "Bank Name": "bank_name",
                "IFSC Code": "ifsc_code",
                "Bank Address": "bank_address"
            }

            result = {
                "bank_name": None,
                "ifsc_code": None,
                "bank_address": None
            }

            # Get all .row > .col-sm... elements where label & field are siblings
            rows = container.locator(".row > .col-sm-12")
            count = await rows.count()

            for i in range(count):
                col = rows.nth(i)
                label = col.locator("label")
                input_or_textarea = col.locator("input, textarea")

                try:
                    label_text = (await label.inner_text()).strip()
                    value = (await input_or_textarea.input_value()).strip()

                    if label_text in fields_to_extract:
                        result[fields_to_extract[label_text]] = value
                        self.logger.debug(f"✅ {label_text}: {value}")
                except Exception as inner_e:
                    self.logger.warning(f"⚠️ Skipped a bank field due to: {inner_e}")
                    continue

            self.logger.info(f"✅ Extracted bank details: {result}")
            return result

        except Exception as e:
            self.logger.error(f"❌ Failed to extract bank details: {e}")
            return {
                "bank_name": None,
                "ifsc_code": None,
                "bank_address": None
            }
        
    async def _extract_complaint_details(self, page: Page) -> Dict[str, Any]:
        """
        Extracts number of complaints and complaint numbers from the 'Complaint Details' section.
        """
        try:
            self.logger.info("🔍 Extracting Complaint Details...")

            # 1. Locate the div with <b>Complaint Details</b>
            complaint_header = page.locator("text='Complaint Details'").first
            table = complaint_header.locator("xpath=following::table[contains(@class, 'table-bordered')][1]")
            await table.wait_for(timeout=5000)

            # 2. Locate tbody rows
            rows = table.locator("tbody tr")
            row_count = await rows.count()

            # Check for "No Records Found"
            if row_count == 1:
                cell_text = (await rows.nth(0).inner_text()).strip()
                if "No Records Found" in cell_text:
                    return {
                        "complaint_count": 0,
                        "complaint_numbers": ""
                    }

            # 3. Extract complaint numbers (2nd column of each row)
            complaint_numbers = []
            for i in range(row_count):
                cells = rows.nth(i).locator("td")
                num_cols = await cells.count()
                if num_cols >= 2:
                    complaint_no = (await cells.nth(1).inner_text()).strip()
                    complaint_numbers.append(complaint_no)

            return {
                "complaint_count": len(complaint_numbers),
                "complaint_numbers": ", ".join(complaint_numbers)
            }

        except Exception as e:
            self.logger.error(f"❌ Failed to extract complaint details: {e}")
            return {
                "complaint_count": 0,
                "complaint_numbers": ""
            }



    async def _extract_real_estate_agents(self, page: Page) -> Dict[str, Any]:
        """
        Extracts Real Estate Agent details defensively, handling cases where no table exists.
        """
        try:
            self.logger.info("🧾 Extracting Registered Real Estate Agent(s)...")

            # STEP 1: First, check if the section even exists on the page.
            accordion_button = page.locator("button.accordion-button:has-text('Registered Real Estate Agent(s)')")
            try:
                # Use a short timeout to quickly check for the button's existence.
                await accordion_button.wait_for(state="attached", timeout=5000)
            except PlaywrightError:
                self.logger.info("Section 'Registered Real Estate Agent(s)' not found. Skipping.")
                return {"real_estate_agents": None, "maharera_cert_numbers": None}

            # STEP 2: Dynamically get the ID of the content pane and expand if needed.
            target_id_selector = await accordion_button.get_attribute("data-bs-target")
            if not target_id_selector:
                raise Exception("Could not find 'data-bs-target' on accordion button.")

            if await accordion_button.get_attribute("aria-expanded") == "false":
                await accordion_button.click()
                # Wait for the animation to finish by waiting for the .show class.
                await page.locator(f"{target_id_selector}.show").wait_for(timeout=5000)

            # STEP 3: THIS IS THE KEY FIX. Check if a table exists before trying to access it.
            table_container = page.locator(target_id_selector)
            table = table_container.locator("table.table-bordered")

            # Give the UI a brief moment to render the table after the accordion expands.
            await asyncio.sleep(0.5) 

            if await table.count() == 0:
                # If no table is found, log it as a success and move on.
                self.logger.info("✅ Section expanded, but no agent data table was found.")
                return {"real_estate_agents": None, "maharera_cert_numbers": None}
            
            # If we get here, the table exists. Now we can extract from it.
            self.logger.info("✅ Agent table is present. Proceeding with extraction.")

            # STEP 4: Extract data from the table.
            rows = table.locator("tbody tr")
            row_count = await rows.count()
            agents, cert_nos = [], []

            for i in range(row_count):
                cells = rows.nth(i).locator("td")
                if await cells.count() >= 3:
                    name = (await cells.nth(1).inner_text()).strip()
                    cert = (await cells.nth(2).inner_text()).strip()
                    if name: agents.append(name)
                    if cert: cert_nos.append(cert)

            return {
                "real_estate_agents": ", ".join(agents) if agents else None,
                "maharera_cert_numbers": ", ".join(cert_nos) if cert_nos else None
            }

        except Exception as e:
            self.logger.error(f"❌ An unexpected error occurred while extracting real estate agent details: {e}")
            return {"real_estate_agents": None, "maharera_cert_numbers": None}