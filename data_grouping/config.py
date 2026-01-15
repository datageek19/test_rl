import os

class Config:
    JOB_INTERVAL_MINUTES = 5
    CURL_ALERT_EXPORT_URL = "http://localhost:3001/api/alerts/alert-data-export?timeRange=5m"
    CURL_REQUEST_TIMEOUT = 120
    
    # PCA configuration - use fixed components for model stability across runs
    # Set to None to use variance-based (95%) - may vary between runs
    FIXED_PCA_COMPONENTS = 8  # Fixed at 8 components for stability

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    PARQUET_IN_DIR = os.path.join(BASE_DIR, "data_extraction_api", "output")
    ARCHIVE_DIR = os.path.join(BASE_DIR, "data_extraction_api", "archive")
