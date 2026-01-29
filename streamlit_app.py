# streamlit_app.py

import streamlit as st
import tempfile
import asyncio
import os
from pathlib import Path
from datetime import datetime
import subprocess
import sys
import pandas as pd
from streamlit_autorefresh import st_autorefresh

# Fixed output directory
OUTPUT_DIR = "data/output"
LOG_FILE = "data/output/scraper_log.txt"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Page config
st.set_page_config(
    page_title="MahaRERA Scraper",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    /* Main container */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    /* Headers */
    h1 {
        color: #1E88E5;
        font-weight: 700 !important;
        margin-bottom: 0.5rem !important;
    }

    /* Cards */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }

    .metric-card-success {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }

    .metric-card-error {
        background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);
    }

    .metric-card-total {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }

    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0;
    }

    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
        margin-top: 0.5rem;
    }

    /* Status badges */
    .status-running {
        background-color: #4CAF50;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 2rem;
        font-weight: 600;
        display: inline-block;
        animation: pulse 2s infinite;
    }

    .status-idle {
        background-color: #9E9E9E;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 2rem;
        font-weight: 600;
        display: inline-block;
    }

    .status-complete {
        background-color: #2196F3;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 2rem;
        font-weight: 600;
        display: inline-block;
    }

    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(76, 175, 80, 0.7); }
        70% { box-shadow: 0 0 0 10px rgba(76, 175, 80, 0); }
        100% { box-shadow: 0 0 0 0 rgba(76, 175, 80, 0); }
    }

    /* Log container */
    .log-container {
        background-color: #1e1e1e;
        border-radius: 0.5rem;
        padding: 1rem;
        font-family: 'Monaco', 'Menlo', monospace;
        font-size: 0.85rem;
        color: #d4d4d4;
        max-height: 400px;
        overflow-y: auto;
    }

    /* Sidebar */
    .css-1d391kg {
        background-color: #f8f9fa;
    }

    /* File uploader */
    .uploadedFile {
        border: 2px dashed #1E88E5 !important;
        border-radius: 1rem !important;
    }

    /* Buttons */
    .stButton > button {
        border-radius: 0.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }

    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }

    /* Download button */
    .stDownloadButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 0.5rem;
        padding: 0.75rem 2rem;
        font-weight: 600;
    }

    /* Divider */
    hr {
        margin: 2rem 0;
        border: none;
        height: 1px;
        background: linear-gradient(to right, transparent, #ddd, transparent);
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------
# Session State Initialization
# -----------------------------
if "is_running" not in st.session_state:
    st.session_state.is_running = False
if "output_file_path" not in st.session_state:
    st.session_state.output_file_path = None
if "process" not in st.session_state:
    st.session_state.process = None

# -----------------------------
# Auto-refresh when scraping is running
# -----------------------------
if st.session_state.is_running:
    st_autorefresh(interval=2000, limit=None, key="log_refresh")

# Check if process finished
if st.session_state.is_running and st.session_state.process:
    poll = st.session_state.process.poll()
    if poll is not None:
        st.session_state.is_running = False
        st.session_state.process = None

# -----------------------------
# Header
# -----------------------------
col_title, col_status = st.columns([3, 1])

with col_title:
    st.markdown("# 🏗️ MahaRERA Scraper")
    st.markdown("*Extract project data from Maharashtra RERA portal*")

with col_status:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.session_state.is_running:
        st.markdown('<div class="status-running">● RUNNING</div>', unsafe_allow_html=True)
    elif st.session_state.output_file_path and os.path.exists(st.session_state.output_file_path):
        st.markdown('<div class="status-complete">✓ COMPLETE</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="status-idle">○ IDLE</div>', unsafe_allow_html=True)

st.markdown("---")

# -----------------------------
# Sidebar - Settings
# -----------------------------
with st.sidebar:
    st.markdown("## ⚙️ Settings")
    st.markdown("")

    start_row = st.number_input(
        "📍 Start Row",
        min_value=1,
        value=2,
        help="Row 1 is usually headers. Start from row 2 for data."
    )

    max_captcha_attempts = st.number_input(
        "🔄 Max CAPTCHA Attempts",
        min_value=1,
        max_value=50,
        value=20,
        help="Number of times to retry solving CAPTCHA"
    )

    rera_column = st.text_input(
        "📋 RERA Column Name",
        value="RERA No.",
        help="Column name in your Excel/CSV containing RERA numbers"
    )

    st.markdown("---")

    st.markdown("### 📖 Quick Guide")
    st.markdown("""
    1. **Upload** your Excel/CSV file
    2. **Configure** settings if needed
    3. **Click** Start Scraping
    4. **Monitor** progress in logs
    5. **Download** results anytime
    """)

    st.markdown("---")

    st.markdown("### 📁 Output Location")
    st.code(OUTPUT_DIR, language=None)

# -----------------------------
# Main Content
# -----------------------------
# File Upload Section
st.markdown("### 📂 Upload Input File")

uploaded_file = st.file_uploader(
    "Drag and drop your Excel (.xlsx) or CSV file",
    type=["xlsx", "xls", "csv"],
    help="File must contain a column with RERA registration numbers"
)

st.markdown("### 📄 Output Settings")

# Get existing output files
def get_existing_output_files():
    """Get list of existing CSV/XLSX files in output directory."""
    files = []
    if os.path.exists(OUTPUT_DIR):
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(('.csv', '.xlsx')) and not f.startswith('.'):
                filepath = os.path.join(OUTPUT_DIR, f)
                size = os.path.getsize(filepath)
                try:
                    if f.endswith('.csv'):
                        df = pd.read_csv(filepath, nrows=0)
                    else:
                        df = pd.read_excel(filepath, nrows=0)
                    records = len(pd.read_csv(filepath)) if f.endswith('.csv') else len(pd.read_excel(filepath))
                    files.append(f"{f} ({records} records, {size:,} bytes)")
                except:
                    files.append(f"{f} ({size:,} bytes)")
    return files

existing_files = get_existing_output_files()

col_mode, col_format = st.columns(2)

with col_mode:
    output_mode = st.radio(
        "📁 Output Mode",
        ["Create New File", "Continue Existing File"],
        horizontal=True,
        help="Continue from where you left off or start fresh"
    )

with col_format:
    output_format = st.radio(
        "📋 Output Format",
        ["CSV", "XLSX"],
        horizontal=True,
        help="Choose output file format"
    )

if output_mode == "Create New File":
    default_ext = ".csv" if output_format == "CSV" else ".xlsx"
    output_name = st.text_input(
        "📄 Output Filename",
        value=f"maharera_{datetime.now().strftime('%Y%m%d_%H%M%S')}{default_ext}",
        help="Name for the output file"
    )
else:
    if existing_files:
        selected_file = st.selectbox(
            "📂 Select Existing File to Continue",
            options=existing_files,
            help="Select an existing file to append new records to"
        )
        # Extract just the filename from the selection
        output_name = selected_file.split(" (")[0]

        # Show info about existing file
        existing_path = os.path.join(OUTPUT_DIR, output_name)
        if os.path.exists(existing_path):
            try:
                if output_name.endswith('.csv'):
                    df_existing = pd.read_csv(existing_path)
                else:
                    df_existing = pd.read_excel(existing_path)
                st.info(f"✅ Will continue from existing file with **{len(df_existing)}** records. Already processed RERA numbers will be skipped.")
            except Exception as e:
                st.warning(f"Could not read existing file: {e}")
    else:
        st.warning("No existing output files found. Please create a new file.")
        output_mode = "Create New File"
        default_ext = ".csv" if output_format == "CSV" else ".xlsx"
        output_name = st.text_input(
            "📄 Output Filename",
            value=f"maharera_{datetime.now().strftime('%Y%m%d_%H%M%S')}{default_ext}",
            help="Name for the output file"
        )

# Show file preview if uploaded
if uploaded_file:
    with st.expander("👀 Preview Uploaded File", expanded=False):
        try:
            if uploaded_file.name.endswith('.csv'):
                df_preview = pd.read_csv(uploaded_file, nrows=5)
            else:
                df_preview = pd.read_excel(uploaded_file, nrows=5)
            st.dataframe(df_preview, width="stretch")
            uploaded_file.seek(0)  # Reset file pointer
            st.caption(f"Showing first 5 rows. Columns: {', '.join(df_preview.columns.tolist())}")
        except Exception as e:
            st.error(f"Could not preview file: {e}")

st.markdown("---")

# -----------------------------
# Action Buttons
# -----------------------------
col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])

with col_btn1:
    start_clicked = st.button(
        "🚀 Start Scraping",
        disabled=st.session_state.is_running or not uploaded_file,
        type="primary",
        width="stretch"
    )

with col_btn2:
    stop_clicked = st.button(
        "⏹️ Stop",
        disabled=not st.session_state.is_running,
        type="secondary",
        width="stretch"
    )

if stop_clicked and st.session_state.process:
    st.session_state.process.terminate()
    st.session_state.is_running = False
    st.toast("Scraping stopped!", icon="⏹️")
    st.rerun()

# -----------------------------
# Stats Cards (Only show when scraping is active or has completed)
# -----------------------------
def parse_stats_from_log():
    """Parse success/error counts from log file."""
    stats = {"success": 0, "error": 0, "total": 0, "current": 0, "has_data": False}
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r") as f:
                content = f.read()

                # Check if there's actual scraping data
                if "Loaded" in content and "RERA numbers" in content:
                    stats["has_data"] = True

                # Count SUCCESS occurrences
                stats["success"] = content.count("SUCCESS:")
                stats["error"] = content.count("ERROR for") + content.count("CAPTCHA failed")

                # Try to find total from "Loaded X RERA numbers"
                import re
                match = re.search(r"Loaded (\d+) RERA numbers", content)
                if match:
                    stats["total"] = int(match.group(1))

                # Current progress
                matches = re.findall(r"\[(\d+)/\d+\]", content)
                if matches:
                    stats["current"] = int(matches[-1])
        except:
            pass
    return stats

stats = parse_stats_from_log()

# Only show statistics if scraping is running or has data
if st.session_state.is_running or stats["has_data"]:
    st.markdown("### 📊 Statistics")

    col_s1, col_s2, col_s3, col_s4 = st.columns(4)

    with col_s1:
        st.markdown(f"""
        <div class="metric-card metric-card-success">
            <p class="metric-value">{stats['success']}</p>
            <p class="metric-label">✓ Successful</p>
        </div>
        """, unsafe_allow_html=True)

    with col_s2:
        st.markdown(f"""
        <div class="metric-card metric-card-error">
            <p class="metric-value">{stats['error']}</p>
            <p class="metric-label">✗ Failed</p>
        </div>
        """, unsafe_allow_html=True)

    with col_s3:
        st.markdown(f"""
        <div class="metric-card metric-card-total">
            <p class="metric-value">{stats['total']}</p>
            <p class="metric-label">📋 Total</p>
        </div>
        """, unsafe_allow_html=True)

    with col_s4:
        progress_pct = (stats['current'] / stats['total'] * 100) if stats['total'] > 0 else 0
        st.markdown(f"""
        <div class="metric-card">
            <p class="metric-value">{progress_pct:.0f}%</p>
            <p class="metric-label">📈 Progress</p>
        </div>
        """, unsafe_allow_html=True)

    # Progress bar
    if stats['total'] > 0:
        st.progress(stats['current'] / stats['total'], text=f"Processing {stats['current']} of {stats['total']}")

    st.markdown("---")

# -----------------------------
# Logs Section (Only show when scraping is active or has data)
# -----------------------------
if st.session_state.is_running or stats["has_data"]:
    col_log_title, col_log_clear = st.columns([3, 1])

    with col_log_title:
        st.markdown("### 📝 Live Logs")

    with col_log_clear:
        if st.button("🗑️ Clear Logs", disabled=st.session_state.is_running):
            if os.path.exists(LOG_FILE):
                os.remove(LOG_FILE)
            st.toast("Logs cleared!", icon="🗑️")
            st.rerun()

    log_placeholder = st.empty()

    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            logs = f.read()
        if logs:
            # Show last 100 lines
            log_lines = logs.strip().split('\n')[-100:]
            log_placeholder.code('\n'.join(log_lines), language="bash")
        else:
            log_placeholder.info("Waiting for logs...")
    else:
        log_placeholder.info("📋 Logs will appear here when scraping starts...")

    st.markdown("---")

# -----------------------------
# Download Section
# -----------------------------
if st.session_state.output_file_path and os.path.exists(st.session_state.output_file_path):
    file_size = os.path.getsize(st.session_state.output_file_path)
    if file_size > 0:
        st.markdown("### 📥 Download Results")

        col_dl1, col_dl2 = st.columns([2, 1])

        is_xlsx = st.session_state.output_file_path.endswith('.xlsx')

        with col_dl1:
            # Show preview of output
            try:
                if is_xlsx:
                    df_output = pd.read_excel(st.session_state.output_file_path, nrows=5)
                else:
                    df_output = pd.read_csv(st.session_state.output_file_path, nrows=5)
                st.dataframe(df_output, width="stretch")
                st.caption(f"Preview of output file ({file_size:,} bytes)")
            except:
                st.info(f"Output file ready ({file_size:,} bytes)")

        with col_dl2:
            with open(st.session_state.output_file_path, "rb") as f:
                mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if is_xlsx else "text/csv"
                label = "⬇️ Download XLSX" if is_xlsx else "⬇️ Download CSV"
                st.download_button(
                    label=label,
                    data=f,
                    file_name=output_name,
                    mime=mime_type,
                    width="stretch"
                )

            st.markdown(f"**File location:**")
            st.code(st.session_state.output_file_path, language=None)

# -----------------------------
# Start Scraping Logic
# -----------------------------
if start_clicked:
    if not uploaded_file:
        st.error("Please upload an Excel or CSV file first.")
        st.stop()

    # Ensure filename has correct extension
    final_output_name = output_name
    expected_ext = ".xlsx" if output_format == "XLSX" else ".csv"
    if not final_output_name.endswith(expected_ext):
        # Remove any existing extension and add correct one
        final_output_name = os.path.splitext(final_output_name)[0] + expected_ext

    # Save uploaded file
    temp_dir = Path(tempfile.mkdtemp())
    input_path = temp_dir / uploaded_file.name
    output_path = Path(OUTPUT_DIR) / final_output_name

    with open(input_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    st.session_state.output_file_path = str(output_path)
    st.session_state.is_running = True

    # Check if continuing from existing file
    is_continuing = output_mode == "Continue Existing File" and os.path.exists(output_path)

    # Clear old log file
    with open(LOG_FILE, "w") as f:
        f.write(f"{'='*50}\n")
        f.write(f"  MahaRERA Scraper Started\n")
        f.write(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*50}\n\n")
        f.write(f"Input: {uploaded_file.name}\n")
        f.write(f"Output: {output_path}\n")
        f.write(f"Format: {output_format}\n")
        if is_continuing:
            f.write(f"Mode: CONTINUING from existing file\n")
        else:
            f.write(f"Mode: Creating NEW file\n")
        f.write(f"Settings: start_row={start_row}, max_captcha={max_captcha_attempts}\n")
        f.write("-" * 50 + "\n\n")

    # Run scraper as subprocess
    cmd = [
        sys.executable, "-c", f"""
import asyncio
import sys
sys.path.insert(0, '.')
from scraper import run_scraper

def log_to_file(msg):
    with open('{LOG_FILE}', 'a') as f:
        f.write(msg + '\\n')
        f.flush()
    print(msg)

asyncio.run(run_scraper(
    input_path='{input_path}',
    output_path='{output_path}',
    start_row={start_row},
    headless=False,
    max_captcha_attempts={max_captcha_attempts},
    rera_column='{rera_column}',
    log_callback=log_to_file
))
"""
    ]

    # Start subprocess
    process = subprocess.Popen(
        cmd,
        cwd=os.getcwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    st.session_state.process = process

    st.toast("Scraper started!", icon="🚀")
    st.rerun()

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #888; padding: 1rem;'>"
    "MahaRERA Scraper v1.0 | Built with Streamlit & Playwright"
    "</div>",
    unsafe_allow_html=True
)
