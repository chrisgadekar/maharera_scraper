# MahaRERA Project Scraper

A comprehensive web scraper for extracting real estate project data from the Maharashtra Real Estate Regulatory Authority (MahaRERA) website. This scraper can extract all 43,000+ project records and export them to Excel format.

## Features

- **Full Data Extraction**: Extracts all project details including project ID, name, promoter, location, state, pincode, district, certificates, etc.
- **JavaScript Support**: Uses Playwright to handle Single-SPA JavaScript framework
- **CAPTCHA Handling**: Integrated CAPTCHA solving using OCR
- **Pagination Support**: Automatically navigates through all pages (4300+ pages)
- **Progress Tracking**: Saves progress periodically and can resume from where it left off
- **Excel Export**: Exports data to Excel with multiple sheets (Projects, Summary, Statistics)
- **Anti-Detection**: Implements stealth techniques to avoid bot detection
- **Error Handling**: Robust error handling with retry mechanisms
- **Logging**: Comprehensive logging for monitoring and debugging

## Prerequisites

- Python 3.8 or higher
- Tesseract OCR (for CAPTCHA solving)
- Chrome/Chromium browser

## Installation

1. **Clone or download the project files**

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install Playwright browsers:**
   ```bash
   playwright install chromium
   ```

4. **Install Tesseract OCR:**

   **On macOS:**
   ```bash
   brew install tesseract
   ```

   **On Ubuntu/Debian:**
   ```bash
   sudo apt-get install tesseract-ocr
   ```

   **On Windows:**
   Download from: https://github.com/UB-Mannheim/tesseract/wiki

## Configuration

The scraper uses a configuration file (`config.py`) where you can customize:

- **Scraping Settings**: Delays, timeouts, page limits
- **Browser Settings**: User agent, viewport, arguments
- **Output Settings**: File formats, directories
- **CAPTCHA Settings**: Auto-solving, retry attempts
- **Error Handling**: Retry policies, failure limits

### Key Configuration Options

```python
# Limit scraping to first 100 pages (for testing)
SCRAPING_CONFIG['max_pages'] = 100

# Run in headless mode (no browser window)
SCRAPING_CONFIG['headless'] = True

# Adjust delays between requests
SCRAPING_CONFIG['delay_between_pages'] = (5, 10)
```

## Usage

### Basic Usage

Run the scraper with default settings:

```bash
python main.py
```

### Advanced Usage

Use the improved scraper with better configuration:

```bash
python maharera_scraper.py
```

### Custom Configuration

Modify `config.py` to customize the scraping behavior:

```python
# Example: Scrape only first 50 pages for testing
SCRAPING_CONFIG['max_pages'] = 50

# Example: Increase delays to be more respectful
SCRAPING_CONFIG['delay_between_pages'] = (8, 15)
```

## Output

The scraper generates:

1. **Excel File**: `maharera_projects_YYYYMMDD_HHMMSS.xlsx`
   - **Projects Sheet**: All project data
   - **Summary Sheet**: Scraping statistics
   - **State_Statistics Sheet**: Projects by state
   - **District_Statistics Sheet**: Projects by district

2. **CSV Backup**: `maharera_projects_YYYYMMDD_HHMMSS.csv`

3. **Progress Files**: Temporary CSV files saved every 10 pages

4. **Log File**: `maharera_scraper.log` with detailed execution logs

## Data Fields Extracted

- Project ID
- Project Name
- Promoter Name
- Location
- State
- Pincode
- District
- Last Modified Date
- Certificate Status
- Extension Certificate
- Details URL
- Page Number
- Scraped Timestamp

## Troubleshooting

### Common Issues

1. **CAPTCHA Detection**: The scraper includes automatic CAPTCHA solving, but manual intervention may be needed occasionally.

2. **Rate Limiting**: If you encounter rate limiting, increase the delays in `config.py`:
   ```python
   SCRAPING_CONFIG['delay_between_pages'] = (10, 20)
   ```

3. **Browser Issues**: Ensure Chrome/Chromium is installed and Playwright browsers are set up:
   ```bash
   playwright install chromium
   ```

4. **Tesseract Issues**: Verify Tesseract installation:
   ```bash
   tesseract --version
   ```

### Debug Mode

For debugging, set headless mode to False in `config.py`:
```python
SCRAPING_CONFIG['headless'] = False
```

This will open a browser window so you can see what the scraper is doing.

## Performance Tips

1. **For Testing**: Set `max_pages` to a small number (e.g., 5-10)
2. **For Production**: Use headless mode and appropriate delays
3. **Resume Capability**: The scraper saves progress every 10 pages
4. **Memory Management**: Large datasets are processed in chunks

## Legal and Ethical Considerations

- **Respect robots.txt**: The scraper respects website terms
- **Rate Limiting**: Built-in delays to avoid overwhelming the server
- **Data Usage**: Ensure compliance with data protection laws
- **Terms of Service**: Review the website's terms of service

## Project Structure

```
scraper/
├── main.py                 # Basic scraper implementation
├── maharera_scraper.py     # Advanced scraper with configuration
├── config.py              # Configuration settings
├── requirements.txt       # Python dependencies
├── modules/
│   └── captcha_solver.py  # CAPTCHA solving module
├── output/                # Generated output files
└── README.md             # This file
```

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve the scraper.

## License

This project is for educational and research purposes. Please ensure compliance with applicable laws and website terms of service.

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review the log files for error details
3. Ensure all dependencies are properly installed
4. Verify the website is accessible and functioning 