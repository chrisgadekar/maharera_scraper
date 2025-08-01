import asyncio
import re
from typing import Dict, List, Optional, Any
import logging
from playwright.async_api import Page, expect

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
                self._extract_planning_land_block(page),
                # self._extract_commencement_certificate(page),
                self._extract_project_address(page),
                self._extract_promoter_details(page),
                self._extract_promoter_address(page),
                self._extract_partner_details(page),
                self._extract_promoter_past_experience(page),
                self._extract_authorised_signatory(page),
                self._extract_project_professionals(page),
                self._extract_sro_details(page),
                self._extract_landowner_type(page),
                self._extract_investor_flag(page),
                self._extract_litigation_details(page),
                self._extract_building_details(page),
                self._extract_parking_details(page)
                
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
            reg_number = await page.locator("label:has-text('Registration Number') + label").inner_text(timeout=5000)
            reg_date = await page.locator("label:has-text('Date of Registration') + label").inner_text(timeout=5000)
            return {
                'registration_number': reg_number.strip(),
                'date_of_registration': reg_date.strip()
            }
        except Exception as e:
            self.logger.warning(f"Could not extract Registration Block: {e}")
            return {}

    async def _extract_project_details_block(self, page: Page) -> Dict[str, str]:
        data = {}
        fields = {
            'project_name': "Project Name",
            'project_status': "Project Status",
            'project_type': "Project Type",
            'project_location': "Project Location",
            'proposed_completion_date': "Proposed Completion Date (Original)"
        }
        try:
            for key, label in fields.items():
                selector = f"label.form-label-preview-label:has-text('{label}') + label.form-control"
                value = await page.locator(selector).inner_text(timeout=5000)
                data[key] = value.strip() if value else None
            return data
        except Exception as e:
            self.logger.warning(f"Could not extract Project Details Block: {e}")
            return {}

    async def _extract_planning_land_block(self, page: Page) -> Dict[str, Optional[str]]:
        data = {}
        try:
            field_map = {
                'planning_authority': "Planning Authority",
                'full_name_planning_authority': "Full Name of the Planning Authority",
                'final_plot_bearing': "Final Plot bearing No/CTS Number/Survey Number",
                'total_land_area': "Total Land Area of Approved Layout (Sq. Mts.)",
                'land_area_applied': "Land Area for Project Applied for this Registration (Sq. Mts)",
                'permissible_builtup': "Permissible Built-up Area",
                'sanctioned_builtup': "Sanctioned Built-up Area of the Project applied for Registration",
                'aggregate_open_space': "Aggregate area(in sq. mts) of recreational open space as per Layout / DP Remarks"
            }

            for key, label in field_map.items():
                try:
                    selector = f"label.form-label:has-text('{label}') + input.form-control, label.form-label:has-text('{label}') + textarea.form-control"
                    locator = page.locator(selector).first
                    await locator.wait_for(timeout=5000)
                    value = await locator.evaluate("el => el.value")
                    data[key] = value.strip() if value else None
                except Exception as e:
                    self.logger.warning(f"Failed to extract '{label}' in Planning/Land Block: {e}")
                    data[key] = None

            return data

        except Exception as e:
            self.logger.warning(f"Could not extract Planning/Land Block at all: {e}")
            return {}

    # async def _extract_commencement_certificate(self, page: Page) -> Dict[str, str]:
    #     try:
    #         table_locator = page.locator("//table[contains(@class,'table-bordered') and contains(@class,'table-striped')]")
    #         headers = await table_locator.locator("thead tr th").all_inner_texts()
    #         rows = await table_locator.locator("tbody tr").all()

    #         col1_values, col3_values = [], []
    #         for row in rows:
    #             col1 = await row.locator("td:nth-child(1)").inner_text()
    #             col3 = await row.locator("td:nth-child(3)").inner_text()
    #             col1_values.append(col1.strip())
    #             col3_values.append(col3.strip())

    #         return {
    #             headers[0].strip(): ', '.join(filter(None, col1_values)),
    #             headers[2].strip(): ', '.join(filter(None, col3_values))
    #         }

    #     except Exception as e:
    #         self.logger.warning(f"Could not extract Commencement Certificate details: {e}")
    #         return {}

    async def _extract_project_address(self, page: Page) -> Dict[str, str]:
        """
        Extracts exactly 12 address fields under 'Project Address Details' using robust label-based scoping.
        Returns them as one comma-separated value string.
        """
        try:
            labels_in_order = [
                "Address", "State/UT", "District", "Taluka", "Village", "Pin Code",
                "Boundaries East", "Boundaries West", "Boundaries South",
                "Boundaries North", "Longitude", "Latitude"
            ]

            # Anchor to the Project Address Details section
            header = page.locator("h5.card-title:has-text('Project Address Details')")
            await header.wait_for(timeout=10000)
            section = header.locator("xpath=ancestor::fieldset")

            address_parts = []
            for label in labels_in_order:
                label_locator = section.locator(f"label.form-label:has-text('{label}')")
                input_locator = label_locator.locator("xpath=following-sibling::input[1]").first  # <== Fix here

                await input_locator.wait_for(timeout=5000)
                value = await input_locator.input_value()
                address_parts.append(value.strip() if value else "")

            return {
                "project_address_full": ", ".join(filter(None, address_parts))
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract Project Address: {e}")
            return {"project_address_full": None}

     


    async def _extract_promoter_details(self, page: Page) -> Dict[str, str]:
        """
        Extracts 'Promoter Type' and 'Name of Partnership' as two separate fields.
        """
        try:
            # Wait for 'Promoter Type' label to ensure form is loaded
            await page.locator("label:has-text('Promoter Type')").wait_for(timeout=10000)

            # PROMOTER TYPE: Inside fieldset
            promoter_type_label = page.locator("label:has-text('Promoter Type')")
            promoter_type_input = promoter_type_label.locator("xpath=following-sibling::input[1]").first
            promoter_type_value = await promoter_type_input.input_value()

            # NAME OF PARTNERSHIP: Outside fieldset, regular layout
            name_partnership_label = page.locator("label:has-text('Name of Partnership')")
            name_partnership_input = name_partnership_label.locator("xpath=following-sibling::input[1]").first
            name_partnership_value = await name_partnership_input.input_value()

            return {
                "promoter_type": promoter_type_value.strip(),
                "name_of_partnership": name_partnership_value.strip()
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract promoter details: {e}")
            return {
                "promoter_type": None,
                "name_of_partnership": None
            }

    async def _extract_promoter_address(self, page: Page) -> Dict[str, str]:
        try:
            # Selector to the block containing all address inputs
            container = page.locator("project-communication-address-preview fieldset form div.row")

            # Select all input elements inside the nested divs
            input_elements = container.locator("div > label + input")

            count = await input_elements.count()
            values = []

            for i in range(count):
                value = await input_elements.nth(i).evaluate("el => el.value")
                values.append(value.strip() if value else '')

            return {
                'promoter_official_address': ', '.join(values)
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract Promoter Address details: {e}")
            return {'promoter_official_address': None}


    async def _extract_partner_details(self, page: Page) -> Dict[str, Any]:
        """Extract partner details as comma-separated name and designation lists."""
        try:
            # Locate the personnel modal preview section directly
            personnel_section = page.locator("project-personnel-modal-preview fieldset form")

            # Make sure the table is there
            table = personnel_section.locator("table.table.table-bordered.table-striped")
            await table.wait_for(timeout=5000)

            rows = table.locator("tbody tr")

            name_list = []
            designation_list = []

            count = await rows.count()
            for i in range(count):
                row = rows.nth(i)
                cols = row.locator("td")

                name = await cols.nth(1).inner_text()
                designation = await cols.nth(2).inner_text()

                name_list.append(name.strip())
                designation_list.append(designation.strip())

            return {
                'partner_details': {
                    'names': ', '.join(name_list),
                    'designations': ', '.join(designation_list)
                }
            }

        except Exception as e:
            self.logger.warning(f"❌ Could not extract Partner Details: {e}")
            return {'partner_details': None}
        

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