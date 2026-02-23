# Causal ML RCA

Root cause analysis for service latency issues using causal inference with DoWhy GCM (Graphical Causal Model).

This implementation follows the approach described in the DoWhy documentation for root cause analysis in microservice architectures. The two RCA scenarios — anomaly attribution and distribution change — are taken directly from the DoWhy GCM tutorials.

---

## Overview

The pipeline takes two inputs — a service dependency graph and latency time-series data — and produces root cause attributions for latency anomalies. It does this by:

1. Loading a service graph (nodes and CALLS edges) and latency measurements
2. Building a focused subgraph around services that have latency data
3. Reversing edges to represent latency causation direction
4. Splitting latency data into normal and anomalous periods
5. Fitting a DoWhy `StructuralCausalModel` on the normal period
6. Running two DoWhy GCM analysis functions against the fitted model

---

## Two entry points

### `main.py` — Full pipeline (with alert preprocessing)

Reads alert JSON files, filters to firing alerts with `alert_subcategory == "Latency"`, maps alert service names to the service graph, and builds a focused subgraph from matched services plus their neighbors. Then pulls latency data for those services and runs causal RCA.

This is the production path where alerts determine which services to analyze.

### `run_simple.py` — Simplified pipeline (no alert preprocessing)

Reads latency CSV files and the service graph directly. Builds the focused subgraph by matching latency column names to service graph node names. Skips alert loading/filtering entirely.

This is useful for testing the causal RCA pipeline when alert data is not available. The pipeline steps after subgraph construction are identical to `main.py`.

---

## Pipeline steps

```
                                  ┌──────────────────────┐
                                  │   Service Graph JSON  │
                                  │  (nodes + CALLS edges)│
                                  └──────────┬───────────┘
                                             │
           ┌─────────────────────────────────┤
           │                                 │
   ┌───────┴────────┐              ┌─────────┴──────────┐
   │  main.py path  │              │  run_simple.py path │
   │                │              │                     │
   │ Alert JSONs    │              │ Latency CSVs        │
   │   ↓ filter     │              │   ↓ load all        │
   │   ↓ map to     │              │   ↓ match names     │
   │     graph      │              │     to graph        │
   │   ↓            │              │   ↓                 │
   │ focused        │              │ focused             │
   │ subgraph       │              │ subgraph            │
   └───────┬────────┘              └─────────┬──────────┘
           │                                 │
           └────────────┬────────────────────┘
                        │
                        ▼
              graph_builder.py
              (reverse edges → DAG)
                        │
                        ▼
              anomaly_detector.py
              (split normal vs anomalous)
                        │
                        ▼
              causal_model.py
              (build + fit StructuralCausalModel)
                        │
                        ▼
              rca_analyzer.py
              ├─ Scenario 1: gcm.attribute_anomalies()
              └─ Scenario 2: gcm.distribution_change()
```

---

## Module details

### `data_preprocessing.py`

Class `AlertDataPreprocessor` handles the following steps in order:

1. **Load alerts** — Reads all JSON files from an incoming folder. Each file can contain a single alert dict or a list of alert dicts.
2. **Filter firing** — Keeps alerts where `status == "firing"` (case-insensitive).
3. **Filter Latency** — Keeps firing alerts where `alert_subcategory == "Latency"` (case-insensitive).
4. **Load service graph** — Parses the graph JSON. Supports two formats:
   - `{"data": {"nodes": [...], "edges": [...]}}` — current format. Node names come from `properties.name`. Edges use node IDs resolved via a lookup dict.
   - `[{"source_properties": {...}, "target_properties": {...}, "relationship_type": "..."}]` — legacy format.
5. **Map alerts to graph** — Intersects alert `service_name` values with graph node names. Produces `matched_services` and `unmatched_services` sets. Builds a DataFrame of latency alerts enriched with graph match status and payload fields.
6. **Build focused subgraph** — Starting from matched services, expands outward by `hop_depth` hops (both predecessors and successors) and extracts a subgraph copy.

The `run()` method executes all steps and returns `(alerts_df, focused_subgraph)`.

### `latency_data_loader.py`

Loads latency time-series data for a given list of service names.

- `get_services_from_subgraph()` — Extracts sorted node names from a NetworkX DiGraph.
- `load_latency_data()` — Main entry point. Routes to file-based or DB-based loading.
- **File loading** supports three formats:
  - **CSV folder** — Each CSV has a `Time` column and one or more service columns. Column names are normalized by stripping `" - inbound"` suffix and whitespace. Values like `"2.26 ms"` are cleaned to numeric. Individual service DataFrames are joined with outer join.
  - **Single CSV** — Same normalization as folder mode.
  - **JSON** — Either `{service: [{time, value}, ...]}` or `[{time, service_a, service_b, ...}]`.
- After loading, gaps are filled with forward-fill, back-fill, and time-based interpolation.
- `load_latency_from_db()` — Placeholder. Raises `NotImplementedError`. Intended for Prometheus/Mimir PromQL or similar DB queries.

### `graph_builder.py`

Converts the focused subgraph (service call direction) into a causal graph (latency causation direction).

1. **Reverse edges** — If A calls B (`A→B` in service graph), then B's latency causes A's latency (`B→A` in causal graph). Uses `nx.DiGraph.reverse()`.
2. **Convert to DAG** — DoWhy `StructuralCausalModel` requires a directed acyclic graph. If cycles exist, edges are removed iteratively using `nx.find_cycle()`. The last edge in each detected cycle is removed until the graph is acyclic.
3. **Align nodes with data** — Computes the intersection of graph node names and latency DataFrame column names. Removes graph nodes that have no corresponding latency data column. Filters the DataFrame to matched columns only.

Returns `(causal_graph, filtered_latency_data)`.

### `anomaly_detector.py`

Splits latency data into normal and anomalous periods.

`detect_anomalies()` supports three methods:

- **zscore** — For each service column, computes `|value - mean| / std`. A row is anomalous if any service has a z-score exceeding the threshold (default 2.5).
- **iqr** — Flags values outside `[Q1 - threshold*IQR, Q3 + threshold*IQR]`. A row is anomalous if any service is flagged.
- **percentile** — Flags values above the `(100 - threshold)` percentile. A row is anomalous if any service is flagged.

Returns three values:
- `normal_data` — DataFrame of rows not flagged as anomalous.
- `anomalous_data` — DataFrame of rows flagged as anomalous.
- `anomaly_info` — Dict containing method, threshold, sample counts, and per-service statistics (normal mean, anomalous mean, percentage increase).

`identify_most_affected_services()` ranks services by `increase_pct` (percentage increase from normal mean to anomalous mean) and returns the top N as `(service_name, increase_pct)` tuples.

### `causal_model.py`

Wraps DoWhy GCM model construction and fitting.

`build_causal_model()`:
- Creates a `gcm.StructuralCausalModel` from the causal graph (a NetworkX DAG).
- **Auto mode** (`use_auto=True`, default): Calls `gcm.auto.assign_causal_mechanisms(model, normal_data, quality=AssignmentQuality.GOOD)`. DoWhy selects appropriate causal mechanisms for each node based on the data.
- **Manual mode** (`use_auto=False`): Root nodes (in-degree 0) get `gcm.ScipyDistribution(halfnorm)`. Non-root nodes get `gcm.AdditiveNoiseModel(gcm.ml.create_linear_regressor())`.

`fit_model()`:
- Calls `gcm.fit(causal_model, normal_data)`. Fits the assigned causal mechanisms using the normal (non-anomalous) data only.

`evaluate_model()`:
- Calls `gcm.evaluate_causal_model()` with `compare_mechanism_baselines=False` and `evaluate_invertibility_assumptions=False`. Prints a summary of model quality.

### `rca_analyzer.py`

Implements two RCA scenarios from the DoWhy GCM documentation.

**Scenario 1 — Anomaly Attribution** (`perform_anomaly_attribution`):

Calls `gcm.attribute_anomalies(causal_model, target_node, anomaly_samples=anomalous_data)`.

This answers: "For these specific anomalous samples, how much did each upstream service contribute to the anomaly at the target service?"

The function returns a dict of `{node: array_of_scores}` (one score per anomalous sample). The implementation computes the median score per node, sorts by absolute value, and reports percentage contribution.

**Scenario 2 — Distribution Change** (`perform_distribution_change`):

Calls `gcm.distribution_change(causal_model, normal_data, anomalous_data, target_node, difference_estimation_func)`.

The `difference_estimation_func` is `lambda x, y: np.mean(y) - np.mean(x)` — the difference in means between the two periods.

This answers: "What caused the overall shift in average latency at the target service between the normal period and the anomalous period?"

The function returns a dict of `{node: delta_mean_attribution}`. Results are sorted by absolute value and reported as percentages.

### `run_simple.py`

Simplified entry point that bypasses alert preprocessing.

1. Loads the full service graph from JSON.
2. Loads all latency CSVs from a folder (no service filter — all columns are loaded).
3. Matches latency column names to service graph node names. Services not found in the graph are skipped. Builds a focused subgraph by expanding matched services by `hop_depth` hops.
4. Delegates to the same `graph_builder`, `anomaly_detector`, `causal_model`, and `rca_analyzer` modules used by `main.py`.

### `main.py`

Full pipeline entry point. Runs `AlertDataPreprocessor` first to produce a focused subgraph from alert data, then calls `run_full_pipeline()` which executes steps 2–7 (load latency data, build causal graph, detect anomalies, build/fit model, run both RCA scenarios).

---

## Input data formats

### Service graph JSON

Current format used by the pipeline:

```json
{
  "success": true,
  "data": {
    "nodes": [
      {
        "id": "0",
        "label": "Service",
        "properties": {
          "name": "service-name",
          "cluster": "...",
          "environment": "...",
          "namespace": "..."
        }
      }
    ],
    "edges": [
      {
        "id": "...",
        "source": "0",
        "target": "2348",
        "label": "CALLS",
        "properties": {}
      }
    ]
  }
}
```

Node `id` values are strings. Edges reference nodes by `source`/`target` IDs. The `label` field on edges (e.g., `"CALLS"`) is used as the relationship type.

### Latency CSV files

Each CSV has a `Time` column and one or more service latency columns:

```csv
"Time","service-name - inbound"
2025-10-18 18:30:00,2.26 ms
2025-10-18 19:00:00,1.28 ms
```

Column names are normalized: `" - inbound"` suffix is stripped, `" ms"` is stripped from values, and values are converted to float.

Multiple CSV files in a folder are loaded and joined on the Time index using outer join.

### Alert JSON files (for `main.py` path only)

Each JSON file contains one alert dict or a list of alert dicts. Required fields for filtering:

- `status` — must be `"firing"`
- `alert_subcategory` — must be `"Latency"`
- `service_name` — used to match against service graph node names

---

## Setup

```bash
pip install -r requirements.txt
```

Dependencies: `pandas`, `numpy`, `networkx`, `scipy`, `dowhy` (>= 0.11.0).

## Usage

### With alert preprocessing

```bash
python main.py
```

Configure paths in `main.py`:

```python
ALERTS_FOLDER  = r"path/to/alertsdata/incoming"
GRAPH_PATH     = r"path/to/service_graph_data.json"
LATENCY_FOLDER = r"path/to/latency_data"
```

### Without alert preprocessing (direct latency + graph)

```bash
python run_simple.py
```

Configure paths in `run_simple.py`:

```python
GRAPH_PATH     = r"path/to/service_graph_data.json"
LATENCY_FOLDER = r"path/to/latency_data"
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `anomaly_method` | `"zscore"` | Anomaly detection method: `"zscore"`, `"iqr"`, or `"percentile"` |
| `anomaly_threshold` | `2.5` | Threshold for the chosen method |
| `use_auto_mechanisms` | `True` | If True, DoWhy auto-assigns causal mechanisms. If False, uses halfnorm (roots) + linear regression (non-roots). |
| `analyze_top_n` | `1` | Number of top affected services to use as RCA target |
| `hop_depth` | `1` | How many hops from seed services to include in the focused subgraph |

---

## Pipeline output

Both entry points return a dict:

```python
{
    "anomaly_attribution": {
        "target": "service-name",
        "attributions": {"service-a": 0.42, "service-b": 0.15, ...},
        "sorted_causes": [("service-a", 0.42), ("service-b", 0.15), ...],
        "raw_attributions": {"service-a": array([...]), ...}
    },
    "distribution_change": {
        "target": "service-name",
        "attributions": {"service-a": 1.23, "service-b": 0.45, ...},
        "sorted_causes": [("service-a", 1.23), ("service-b", 0.45), ...]
    },
    "target_node": "service-name"
}
```

Returns `None` if no anomalies are detected in the latency data.

---

## Reference

- DoWhy GCM documentation: https://www.pywhy.org/dowhy/main/user_guide/gcm_based_inference/
- DoWhy microservice RCA tutorial (anomaly attribution and distribution change scenarios)
