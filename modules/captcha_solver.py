import os
import time
from PIL import Image
import io
import pytesseract
import numpy as np
import cv2

class CaptchaSolver:
    def __init__(self, captcha_dir="./captchas"):
        self.captcha_dir = captcha_dir
        os.makedirs(self.captcha_dir, exist_ok=True)

    async def preprocess_image(self, image_bytes):
        """Convert captcha image to binary thresholded form for OCR."""
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        img_np = np.array(img)

        # ✅ Step 1: Grayscale
        gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY)

        # ✅ Step 2: Blur to reduce noise (better for OCR in headless)
        blur = cv2.GaussianBlur(gray, (3, 3), 0)

        # ✅ Step 3: Adaptive threshold (binary image)
        _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

        return Image.fromarray(thresh)


    async def extract_text(self, image_bytes):
        """Run OCR on captcha image with multiple configs."""
        processed_img = await self.preprocess_image(image_bytes)
        
        configs = [
            '--psm 8 --oem 3 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',
            '--psm 7 --oem 3 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
        ]

        results = []
        for img in [Image.open(io.BytesIO(image_bytes)), processed_img]:
            for config in configs:
                text = pytesseract.image_to_string(img, config=config).strip()
                if text and len(text) == 6 and text.isalnum():
                    results.append(text.upper())
        if results:
            return max(set(results), key=results.count)
        return None

    
    async def solve_and_fill(
        self,
        page,
        captcha_selector,
        input_selector,
        submit_selector,
        refresh_selector,
        reg_no,
        max_attempts=2
    ):
        """Solve captcha, retry on failure, return success or failure."""
        for attempt in range(1, max_attempts + 1):
            try:
                # ✅ Refresh captcha from 2nd attempt onwards
                if attempt > 1 and refresh_selector:
                    print(f"🔄 Refreshing captcha (attempt {attempt})")
                    refresh_btn = await page.wait_for_selector(refresh_selector, timeout=5000)
                    await refresh_btn.click()
                    await page.wait_for_timeout(1500)  # 🕒 Wait after refresh for redraw

                # ✅ Wait for captcha element and take screenshot
                captcha_el = await page.wait_for_selector(captcha_selector, timeout=10000)
                captcha_bytes = await captcha_el.screenshot(type="png", scale="device")

                # Debug screenshot (for analysis if OCR fails)
                if page.context.browser.is_connected():
                    with open(f"debug_captcha_{reg_no}_attempt{attempt}.png", "wb") as f:
                        f.write(captcha_bytes)

                # ✅ Extract OCR text
                captcha_text = await self.extract_text(captcha_bytes)
                print(f"[DEBUG] OCR extracted (Attempt {attempt}): {captcha_text}")

                if captcha_text:
                    # Fill input and submit
                    await page.fill(input_selector, captcha_text)
                    await page.click(submit_selector)

                    # ✅ Wait for captcha to disappear (indicates success)
                    try:
                        await page.wait_for_selector(captcha_selector, state="detached", timeout=5000)
                        print(f"✅ Captcha solved successfully for {reg_no}")
                        return True
                    except:
                        print(f"⚠ Attempt {attempt}: Captcha incorrect, retrying...")
                else:
                    print(f"⚠ OCR failed to read text on attempt {attempt}")

            except Exception as e:
                print(f"⚠ Error in captcha solve attempt {attempt}: {e}")

        print(f"❌ Failed to solve captcha for {reg_no} after {max_attempts} attempts.")
        return False
