"""
Configuration file for MahaRERA Scraper
"""

# Scraping Settings
SCRAPING_CONFIG = {
    'headless': False,  # Set to False for debugging (True for production)
    'delay_between_pages': (3, 6),  # Random delay range in seconds
    'delay_between_requests': (1, 3),  # Random delay between requests
    'timeout': 60000,  # Timeout for page loads in milliseconds (increased)
    'max_retries': 3,  # Maximum retries for failed requests
    'save_progress_interval': 10,  # Save progress every N pages
    'max_pages': 2,  # Set to limit number of pages to scrape (None for all)
}

# Browser Settings
BROWSER_CONFIG = {
    'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'viewport': {'width': 1920, 'height': 1080},
    'args': [
        '--no-sandbox',
        '--disable-blink-features=AutomationControlled',
        '--disable-dev-shm-usage',
        '--disable-web-security',
        '--disable-features=VizDisplayCompositor',
        '--disable-extensions',
        '--disable-plugins',
        '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows',
        '--disable-renderer-backgrounding',
    ]
}

# Output Settings
OUTPUT_CONFIG = {
    'excel_filename': None,  # Auto-generated if None
    'csv_backup': True,  # Create CSV backup
    'include_timestamp': True,  # Include timestamp in filename
    'output_directory': 'output',  # Output directory
}

# Logging Settings
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'file': 'maharera_scraper.log',
    'console': True,
}

# CAPTCHA Settings
CAPTCHA_CONFIG = {
    'auto_solve': True,  # Automatically solve CAPTCHAs
    'max_attempts': 5,  # Maximum CAPTCHA solving attempts
    'delay_between_attempts': 2,  # Delay between CAPTCHA attempts
    'save_debug_images': True,  # Save CAPTCHA images for debugging
    'use_human_typing': True,  # Use human-like typing for CAPTCHA input
}

# Data Fields to Extract
DATA_FIELDS = [
    'project_id',
    'project_name', 
    'promoter',
    'location',
    'state',
    'pincode',
    'district',
    'last_modified',
    'certificate',
    'extension_certificate',
    'details_url',
    'page_number',
    'scraped_at'
]

# CSS Selectors for Data Extraction
SELECTORS = {
    'project_container': '.project-item, .result-item, [class*="project"], [class*="result"], div[class*="card"]',
    'project_id': 'h4, .project-id, [class*="id"]',
    'project_name': 'h4, .project-name, [class*="name"]',
    'promoter': '.promoter, [class*="promoter"]',
    'location': '.location, [class*="location"]',
    'state': '.state, [class*="state"]',
    'pincode': '.pincode, [class*="pincode"]',
    'district': '.district, [class*="district"]',
    'last_modified': '.last-modified, [class*="modified"]',
    'certificate': '.certificate, [class*="certificate"]',
    'extension_certificate': '.extension, [class*="extension"]',
    'details_link': 'a[href*="view"], a:contains("View Details")',
    'pagination': '.pagination-info, .page-info, [class*="pagination"]',
    'next_button': 'a[aria-label="Next"], .next, .pagination-next, a:contains("Next")',
    'search_button': 'button[type="submit"], input[type="submit"], .btn-search, button:contains("Search")',
    'captcha_image': 'img[src*="captcha"], img[alt*="captcha"]',
    'captcha_input': 'input[name*="captcha"], input[id*="captcha"]',
}

# Error Handling
ERROR_CONFIG = {
    'continue_on_error': True,  # Continue scraping even if some projects fail
    'log_errors': True,  # Log all errors
    'retry_failed_pages': True,  # Retry failed pages
    'max_consecutive_failures': 5,  # Stop after N consecutive failures
} 