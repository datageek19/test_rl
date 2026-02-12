import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
# Look for .env in the project root (parent of src directory)
config_dir = Path(__file__).parent.parent
env_path = config_dir / '.env'
load_dotenv(env_path)

class Config:
    WORKFLOW_INTERVAL_MINUTES = int(os.getenv("WORKFLOW_INTERVAL_MINUTES") or "2")

    # Job Scheduler (APScheduler)
    RETRAIN_CRON_DAY_OF_WEEK = os.getenv("RETRAIN_CRON_DAY_OF_WEEK") or "mon"
    RETRAIN_CRON_HOUR = int(os.getenv("RETRAIN_CRON_HOUR") or "7")
    RETRAIN_CRON_MINUTE = int(os.getenv("RETRAIN_CRON_MINUTE") or "0")
    RETRAIN_MIN_ALERTS = int(os.getenv("RETRAIN_MIN_ALERTS") or "100")
    RETRAIN_PROMOTE_SILHOUETTE_IMPROVEMENT_THRESHOLD = float(
        os.getenv("RETRAIN_PROMOTE_SILHOUETTE_IMPROVEMENT_THRESHOLD") or "0.05"
    )

    ALERT_ALLOWED_PLATFORMS = (
        [
            p.strip().lower()
            for p in (os.getenv("ALERT_ALLOWED_PLATFORMS") or "aks,gke").split(",")
            if p.strip()
        ]
        or ["aks", "gke"]
    )
    ALERT_FIRING_STATUS = (
        (os.getenv("ALERT_FIRING_STATUS") or "firing").strip().lower() or "firing"
    )
    #Alert Processor
    TIME_WINDOW_MINUTES = int(os.getenv("TIME_WINDOW_MINUTES") or "15")
    MIN_MATCH_SCORE = int(os.getenv("MIN_MATCH_SCORE") or "2")
    MIN_CLUSTERING_SAMPLES = int(os.getenv("MIN_CLUSTERING_SAMPLES") or "10")
    PCA_VARIANCE_THRESHOLD = float(os.getenv("PCA_VARIANCE_THRESHOLD") or "0.95")
    OUTLIER_CONTAMINATION = float(os.getenv("OUTLIER_CONTAMINATION") or "0.05")
    OUTLIER_MIN_SAMPLES = int(os.getenv("OUTLIER_MIN_SAMPLES") or "20")
    SERVICE_GRAPH_MAX_DEPTH = int(os.getenv("SERVICE_GRAPH_MAX_DEPTH") or "2")
    PATH_SIMILARITY_THRESHOLD = float(os.getenv("PATH_SIMILARITY_THRESHOLD") or "0.6")
    RELATED_PATH_SIMILARITY_CUTOFF = float(os.getenv("RELATED_PATH_SIMILARITY_CUTOFF") or "0.3")
    SEVERITY_DOMINANCE_RATIO = float(os.getenv("SEVERITY_DOMINANCE_RATIO") or "0.5")

    # Catalog Manager
    CATALOG_ML_SIMILARITY_THRESHOLD = float(os.getenv("CATALOG_ML_SIMILARITY_THRESHOLD") or "0.7")

    # Paths
    SERVICE_GRAPH_DATA_PATH = Path(os.getenv("SERVICE_GRAPH_DATA_PATH", "/mnt/root/servicegraphdata"))
    SERVICE_GRAPH_DATA_ARCHIVE_DIR = Path(os.getenv("SERVICE_GRAPH_DATA_ARCHIVE_DIR", "/mnt/root/servicegraphdata/archive"))
    
    ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", "/mnt/root/results"))
    MODEL_DIR = Path(os.getenv("MODEL_DIR", "/mnt/root/data/models"))
    CATALOG_DIR = Path(os.getenv("CATALOG_DIR", "/mnt/root/data/clustercatalog"))
    FUDGED_CLUSTER_DIR = Path(os.getenv("FUDGED_CLUSTER_DIR", "/mnt/root/data/fudged_clusters"))
    CATALOG_PATH = CATALOG_DIR / "clustercatalog.json"
    ALERTS_DATA_PATH = Path(os.getenv("ALERTS_DATA_PATH", "/mnt/root/alertsdata"))
    ALERT_DATA_ARCHIVE_DIR = Path(os.getenv("ALERT_DATA_ARCHIVE_DIR", "/mnt/root/alertsdata/archive"))
    ALERT_DATA_NOT_PROCESSED_DIR = Path(
        os.getenv("ALERT_DATA_NOT_PROCESSED_DIR", "/mnt/root/alertsdata/not_processed")
    )

    # PCA configuration - use fixed components for model stability across runs
    # Set to None to use variance-based (95%) - may vary between runs
    _fixed_pca_raw = (os.getenv("FIXED_PCA_COMPONENTS") or "8").strip()
    FIXED_PCA_COMPONENTS = (
        None
        if _fixed_pca_raw.lower() in ("none", "null")
        else int(_fixed_pca_raw)
    )
    DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
    MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
    MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")
    CLUSTER_ACTIVE_HOURS_THRESHOLD = int(os.getenv("CLUSTER_ACTIVE_HOURS_THRESHOLD") or "24")
    
    # API configuration for triggering data export
    CURL_ALERT_EXPORT_URL = os.getenv("CURL_ALERT_EXPORT_URL")
    CURL_REQUEST_TIMEOUT = int(os.getenv("CURL_REQUEST_TIMEOUT") or "30")
