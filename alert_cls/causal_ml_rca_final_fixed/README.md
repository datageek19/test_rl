# Causal ML RCA

Root cause analysis for service latency issues using causal inference (DoWhy GCM).

Inpspired by these tutorials:
 - Based on [DoWhy Microservice RCA](https://www.pywhy.org/dowhy/main/example_notebooks/gcm_rca_microservice_architecture.html) and [DoWhy Online Shop](https://www.pywhy.org/dowhy/main/example_notebooks/gcm_online_shop.html)


## What it does

1. Filters alerts (firing + Latency subcategory) and maps them to a service graph
2. Extracts a focused subgraph of affected services
3. Pulls latency data for those services
4. Builds a causal graph (reversed edges — downstream latency causes upstream latency)
5. Detects anomalous periods in latency data
6. Runs two RCA scenarios using DoWhy's StructuralCausalModel:
   - **Anomaly Attribution** — why was a specific service slow for specific samples?
   - **Distribution Change** — what caused the overall latency shift?

## Pipeline flow

```
Alerts (JSON)          Service Graph (JSON)
      \                     /
       → data_preprocessing →  focused subgraph
                                      |
                            latency_data_loader  (pull data for subgraph services)
                                      |
                              graph_builder      (reverse edges → DAG)
                                      |
                            anomaly_detector     (split normal vs anomalous)
                                      |
                              causal_model       (build + fit SCM)
                                      |
                              rca_analyzer       (attribution + distribution change)
```

## Modules

| File | Purpose |
|------|---------|
| `data_preprocessing.py` | Load alerts, filter firing/Latency, map to service graph, build focused subgraph |
| `latency_data_loader.py` | Extract services from subgraph, load latency data from files or DB |
| `graph_builder.py` | Reverse edges (call→latency causation), convert to DAG, align with data |
| `anomaly_detector.py` | Split latency data into normal/anomalous periods, rank affected services |
| `causal_model.py` | Build, fit, evaluate DoWhy StructuralCausalModel |
| `rca_analyzer.py` | Anomaly attribution (Scenario 1) and distribution change (Scenario 2) |
| `main.py` | Pipeline orchestrator and entry point |

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
python main.py
```

Configure paths in `main.py`:

```python
ALERTS_FOLDER = r"path/to/alertsdata/incoming"
GRAPH_PATH    = r"path/to/service_graph_data.json"
LATENCY_FOLDER = r"path/to/latency_data"
```

### Data sources

**Latency data from local files** (CSV folder, single CSV, or JSON):
```python
results = run_full_pipeline(
    focused_subgraph=focused_subgraph,
    data_source="path/to/latency_data",
)
```

**Latency data from DB** (requires implementing `load_latency_from_db` in `latency_data_loader.py`):
```python
results = run_full_pipeline(
    focused_subgraph=focused_subgraph,
    data_source="db",
    db_config={"url": "...", "start": "...", "end": "...", "step": "30m"},
)
```

## Input data

- **Alerts**: JSON files with alert objects containing `status`, `sub_category`, `service_name`
- **Service graph**: JSON with `nodes` and `edges` arrays
- **Latency data**: CSV files with a time column and service latency columns (e.g. `"2.26 ms"`)
