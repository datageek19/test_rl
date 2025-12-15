# Quick Start Guide

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

### 2. Run Every 15 Minutes (Production)
```bash
python main.py --alerts ../alert_data.csv --graph ../graph_data.json --interval 15
```

### 3. Custom Configuration
```bash
python main.py \
  --alerts ../alert_data.csv \
  --graph ../graph_data.json \
  --interval 15 \
  --output ../results \
  --models ../data/models
```

## Module Imports

```python
# Import individual modules
from data_classifier_module import (
    DataIngestionService,     # Load alerts and graph
    AlertProcessor,           # Process and enrich alerts
    ModelManager,             # Train and version models
    ClusterPredictor,         # Predict clusters
    ResultStorage,            # Save results
    AlertWorkflowScheduler    # Orchestrate workflow
)

# import the scheduler directly
from data_classifier_module import AlertWorkflowScheduler

scheduler = AlertWorkflowScheduler(
    alerts_csv_path='alert_data.csv',
    graph_json_path='graph_data.json'
)
scheduler.run_once()  # scheduler.start()
```

## Directory Structure

```
alert_data_classification_beta/
├── alert_data.csv                  # alert data
├── graph_data.json                 # service graph
├── data_classifier_module/     
│   ├── __init__.py
│   ├── data_ingestion.py
│   ├── alert_processor.py
│   ├── model_manager.py
│   ├── cluster_predictor.py
│   ├── result_storage.py
│   ├── scheduler.py
│   ├── main.py
│   ├── requirements.txt
│   └── README.md
├── data/
│   └── models/                     # ← Models stored here
│       ├── alert_clustering_v1.pkl
│       ├── scaler_v1.pkl
│       ├── pca_v1.pkl
│       └── metadata/
│           ├── v1_metadata.json
│           └── v1_profiles.json
└── results/                        # ← Results stored here
    ├── 20241210_100000/
    │   ├── alert_consolidation_final.csv
    │   ├── cluster_summary.csv
    │   ├── ranked_clusters.csv
    │   └── ...
    └── 20241210_101500/
        └── ...
```

## Common Tasks

### Check Model Versions
```bash
ls -lt data/models/
cat data/models/metadata/v1_metadata.json
```

### View Latest Results
```bash
ls -lt results/
cat results/20241210_100000/run_metadata.json
```

### Load Results
```python
from data_classifier_module import ResultStorage

storage = ResultStorage(output_dir='results')
latest = storage.get_latest_run_id()
results = storage.load_run_results(latest)

print(f"Total alerts: {len(results['alerts'])}")
print(f"Total clusters: {len(results['clusters'])}")
```

### Train New Model
```python
from data_classifier_module import ModelManager

model_mgr = ModelManager(model_dir='data/models')

# Training happens automatically on first run
# Or manually trigger retraining:
scheduler.retrain_model()
```

## Workflow

```
Every 15 Minutes:
[Load Data] → [Enrich] → [Consolidate] → [Extract Features]
    ↓
[Predict Clusters] → [Deduplicate] → [Rank] → [Save Results]

Every Sunday 2 AM:
[Load Historical Data] → [Train New Model] → [Compare] → [Promote if Better]
```

