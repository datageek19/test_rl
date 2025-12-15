# Alert Data Classification Module

Modular, production-ready solution for alert classification, clustering, and analysis with model versioning and scheduling.

## Architecture

```
data_classifier_module/
├── __init__.py              # Package initialization
├── data_ingestion.py        # Data loading and preprocessing
├── alert_processor.py       # Core processing pipeline
├── model_manager.py         # Model training, versioning, serialization
├── cluster_predictor.py     # Cluster prediction using trained models
├── result_storage.py        # Result persistence
├── scheduler.py             # Workflow orchestration and scheduling
├── main.py                  # Entry point
├── requirements.txt         # Python dependencies
└── README.md               
```

## Features

### Modular Design
- **Data Ingestion**: Loads alerts and service graph, pre-computes graph features
- **Alert Processor**: Enriches alerts, groups by relationships, engineers features
- **Model Manager**: Trains, versions, and persists clustering models
- **Cluster Predictor**: Assigns alerts to clusters using trained models
- **Result Storage**: Persists results with timestamped runs
- **Scheduler**: Orchestrates workflow every 15 minutes + weekly retraining

### Model Versioning & Tracking
- Serialize clustering models (`.pkl` files)
- Version tracking (v1, v2, v3, ...)
- Metadata storage (silhouette score, training date, parameters)
- Cluster profiles (characteristics of each cluster)
- Model comparison (automatic promotion if >5% improvement)

### Production-Ready
- Scheduled execution (APScheduler)
- Error handling and logging
- Incremental processing (15-minute batches)
- Model persistence across runs
- Configurable parameters

## Installation

```bash
cd data_classifier_module
pip install -r requirements.txt
```

## Usage

### 1. Run Once (manual trigger)

```bash
python main.py --alerts ../alert_data.csv --graph ../graph_data.json --run-once
```

### 2. Run with Scheduler (Production)

```bash
# Run every 15 minutes
python main.py --alerts ../alert_data.csv --graph ../graph_data.json --interval 15

# Custom configuration
python main.py \
  --alerts ../alert_data.csv \
  --graph ../graph_data.json \
  --interval 15 \
  --output ../results \
  --models ../data/models \
  --no-retraining
```

### 3. scheduler usage

```python
from data_classifier_module import AlertWorkflowScheduler

# Initialize scheduler
scheduler = AlertWorkflowScheduler(
    alerts_csv_path='alert_data.csv',
    graph_json_path='graph_data.json',
    output_dir='results',
    model_dir='data/models'
)

# Run once (manual trigger)
scheduler.run_once()

# start scheduled execution
scheduler.start(workflow_interval_minutes=15, enable_retraining=True)

# Keep running
import time
while True:
    time.sleep(60)
```

## Workflow

### Every 15 Minutes: Classification Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│                  15-MINUTE WORKFLOW                          │
└──────────────────────────────────────────────────────────────┘

[1] Data Ingestion
    ├─ Load alerts from CSV
    ├─ Load service graph from JSON
    └─ Pre-compute graph features (cached)

[2] Alert Enrichment
    ├─ Map alerts to service graph
    ├─ Extract dependencies
    └─ Calculate mapping confidence

[3] Initial Consolidation
    ├─ Group by service relationships
    ├─ Find transitive dependencies
    └─ Create initial groups

[4] Feature Engineering
    ├─ Extract graph topology features (20)
    ├─ Extract alert metadata features (19)
    ├─ Scale features (StandardScaler)
    ├─ Remove outliers (Isolation Forest)
    └─ Apply PCA (95% variance)

[5] Cluster Prediction
    ├─ Load trained model (v{X})
    ├─ Predict cluster assignments
    └─ Calculate confidence scores

[6] Deduplication
    ├─ Find duplicates within clusters
    ├─ Graph-based detection
    └─ Mark duplicates

[7] Ranking & Naming (TODO: rethink cluster ranking and naming business logic)
    ├─ Generate cluster names
    ├─ Calculate importance scores
    └─ Rank clusters

[8] Save Results
    ├─ Save to results/{run_id}/
    ├─ Export CSV files
    └─ Store metadata
```

### Weekly: Model Retraining (Mondays 7 AM)

```
┌──────────────────────────────────────────────────────────────┐
│               WEEKLY MODEL RETRAINING                        │
└──────────────────────────────────────────────────────────────┘

[1] Load historical data (load all historical alerts and service map graph data from azure blob)
[2] Process alerts (same pipeline as classification)
[3] Train new clustering model (K-Means, DBSCAN, or Hierarchical)
[4] Calculate silhouette score (indicate cluster output quality)
[5] Compare with current model
[6] Promote if >5% improvement, otherwise keep current
[7] Save new model as v{X+1}
```

## Model Versioning

### Directory Structure

```
data/models/
├── alert_clustering_v1.pkl        # K-Means model
├── scaler_v1.pkl                  # Feature scaler
├── pca_v1.pkl                     # PCA transformer
├── alert_clustering_v2.pkl        # Retrained model
├── scaler_v2.pkl
├── pca_v2.pkl
└── metadata/
    ├── v1_metadata.json           # Model metadata
    ├── v1_profiles.json           # Cluster profiles
    ├── v2_metadata.json
    └── v2_profiles.json
```

### Metadata mockup example

```json
{
  "version": 2,
  "algorithm": "kmeans",
  "n_clusters": 12,
  "silhouette_score": 0.58,
  "n_samples": 15000,
  "timestamp": "20241210_020000",
  "feature_count": 39,
  "pca_components": 18
}
```

### Cluster Profiles mockup Example

```json
{
  "3": {
    "cluster_id": 3,
    "size": 450,
    "top_alert_types": {
      "HighCPUUsage": 200,
      "CPUSaturation": 150,
      "HighPodCPU": 100
    },
    "top_services": {
      "api-service": 180,
      "worker-service": 150
    },
    "severity_distribution": {
      "critical": 300,
      "high": 150
    },
    "most_common_alert": "HighCPUUsage",
    "most_common_service": "api-service"
  }
}
```

## Output Structure

```
results/
├── 20241210_100000/                    # Run ID (timestamp)
│   ├── alert_consolidation_final.csv   # Main output
│   ├── cluster_summary.csv             # Cluster statistics
│   ├── ranked_clusters.csv             # Ranked clusters
│   ├── deduplicated_alerts.csv         # Unique alerts
│   ├── alerts_by_cluster_detailed.csv  # Detailed view
│   ├── clustering_statistics.json      # Clustering metrics
│   ├── mapping_statistics.csv          # Mapping stats
│   └── run_metadata.json               # Run metadata
├── 20241210_101500/                    # Next run
│   └── ...
└── 20241210_103000/
    └── ...
```

## Configuration

### Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--alerts` | Path to alerts CSV | Required |
| `--graph` | Path to service graph JSON | Required |
| `--output` | Output directory | `results` |
| `--models` | Model directory | `data/models` |
| `--interval` | Workflow interval (minutes) | `15` |
| `--no-retraining` | Disable weekly retraining | Disabled |
| `--run-once` | Run once and exit  (manual trigger)| Disabled |

### Module Configuration

Edit constants in `alert_processor.py`:

```python
TIME_WINDOW_MINUTES = 15          # Duplicate detection window
MIN_MATCH_SCORE = 2               # Fallback mapping threshold
MIN_CLUSTERING_SAMPLES = 10       # Minimum alerts for clustering
PCA_VARIANCE_THRESHOLD = 0.95     # PCA variance retention
OUTLIER_CONTAMINATION = 0.05      # Outlier detection threshold
```


## Complete Workflow Run

```bash
# 1. Initial setup
cd data_classifier_module
pip install -r requirements.txt

# 2. Train initial model (manual trigger)
python main.py --alerts ../alert_data.csv --graph ../graph_data.json --run-once

# 3. Start scheduler (production)
python main.py --alerts ../alert_data.csv --graph ../graph_data.json --interval 15

# Output:
# ==================================================================
# ALERT CLASSIFICATION WORKFLOW SCHEDULER
# ==================================================================
# Workflow interval: Every 15 minutes
# Model retraining: Enabled (Sundays 7 AM)
# ==================================================================
#
# ✓ Scheduled workflow: Every 15 minutes
# ✓ Scheduled retraining: Sundays at 7:00 AM
#
# ✓ Scheduler started successfully!
# ==================================================================
```

## Monitoring

```bash
# Check latest results
ls -lt results/

# View run metadata
cat results/20241210_100000/run_metadata.json

# Check model versions
ls -lt data/models/

# View model metadata
cat data/models/metadata/v2_metadata.json
```

## License

Internal use only.

## Contact

For questions or issues, contact Jurat from AIOps team.
