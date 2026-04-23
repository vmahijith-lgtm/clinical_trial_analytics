"""
Configuration settings for Clinical Trial Analytics Platform
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
BASE_DIR = Path(__file__).resolve().parent.parent

# Runtime data directory can be overridden for deployments.
# Defaults to a local ./data folder to avoid hard-coded subpaths.
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data")))
CACHE_DIR = BASE_DIR / "cache"

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# API Configuration
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Data quality thresholds
QUALITY_THRESHOLDS = {
    "completeness_threshold": 0.95,  # 95% data completeness
    "consistency_threshold": 0.90,   # 90% consistency
    "timeliness_days": 7,             # Data should be within 7 days
    "outlier_std_dev": 3              # Standard deviations for outliers
}

# File patterns to look for
FILE_PATTERNS = [
    "*.xlsx",
    "*.xls"
]

# Column mapping patterns (common variations)
COLUMN_MAPPINGS = {
    "subject_id": ["subject_id", "subject", "subjectid", "patient_id", "patientid"],
    "site_id": ["site_id", "site", "siteid", "center_id"],
    "visit_date": ["visit_date", "visitdate", "date", "visit_dt"],
    "status": ["status", "subject_status", "enrollment_status"],
    "ae_term": ["ae_term", "adverse_event", "ae", "event"],
    "severity": ["severity", "grade", "severity_grade"]
}

# Streamlit page configuration
PAGE_CONFIG = {
    "page_title": "Clinical Trial Analytics Platform",
    "page_icon": "🔬",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Color schemes for visualizations
COLOR_SCHEME = {
    "primary": "#1f77b4",
    "success": "#2ca02c",
    "warning": "#ff7f0e",
    "danger": "#d62728",
    "info": "#17becf"
}