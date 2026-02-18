# AlertProcessor Module — Detailed Technical Design & Knowledge Transfer Document


## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture & Context](#2-system-architecture--context)
3. [Module Overview](#3-module-overview)
4. [Class Design & Initialization](#4-class-design--initialization)
5. [Processing Pipeline — End-to-End Flow](#5-processing-pipeline--end-to-end-flow)
6. [Phase 1: Alert Enrichment](#6-phase-1-alert-enrichment)
7. [Phase 2: Service-Based Grouping](#7-phase-2-service-based-grouping)
8. [Phase 3: Feature Engineering](#8-phase-3-feature-engineering)
9. [Phase 4: Outlier Detection & Removal](#9-phase-4-outlier-detection--removal)
10. [Phase 5: PCA Dimensionality Reduction](#10-phase-5-pca-dimensionality-reduction)
11. [Phase 6: Cluster Label Assignment](#11-phase-6-cluster-label-assignment)
12. [Phase 7: Alert Deduplication](#12-phase-7-alert-deduplication)
13. [Phase 8: Cluster Naming & Ranking](#13-phase-8-cluster-naming--ranking)
14. [Index Mapping System — The Critical Detail](#14-index-mapping-system--the-critical-detail)
15. [Feature Encoding Reference](#15-feature-encoding-reference)
16. [Configuration Parameters](#16-configuration-parameters)
17. [Integration with Other Modules](#17-integration-with-other-modules)
18. [Data Flow Diagrams](#18-data-flow-diagrams)
19. [Alert Data Model — Field Reference](#19-alert-data-model--field-reference)
20. [Edge Cases & Error Handling](#20-edge-cases--error-handling)
21. [Known Issues & TODOs](#21-known-issues--todos)
22. [Appendix: Method Reference](#22-appendix-method-reference)

---

## 1. overview

The `AlertProcessor` module is the **core data processing engine** of the ESOBI Alert Deduplication system. It is responsible for transforming raw infrastructure alerts (originating from Kubernetes platforms such as AKS and GKE) into enriched, grouped, deduplicated, and named clusters.

This modules Takes a list of raw alert dictionaries, enriches them with service graph topology, engineers numeric features for ML clustering, removes outliers, assigns cluster labels, deduplicates within clusters, and produces grouped alerts with specific cluster names and IDs.

**Key functionality:**
- Map alerts to a service dependency graph (NetworkX DiGraph)
- Group related alerts by service topology (BFS traversal)
- Build a numeric feature matrix from alert metadata and graph properties
- Detect and remove statistical outliers using Isolation Forest
- Reduce feature dimensionality using PCA (worth to try feature selection in the future)
- Write cluster labels back into alert records (with index remapping)
- Deduplicate alerts within each cluster using rule-based matching
- Generate stable, readable cluster names and IDs

---

## 2. System Architecture & Context

### 2.1 System Overview

The alert deduplication runs as a scheduled service that processes infrastructure alerts at regular intervals (default: every 5 minutes). The system consists of the following major components:

| Component | File | Responsibility |
|-----------|------|----------------|
| **Scheduler** | `job_scheduler.py` | Orchestrates the entire workflow on a schedule |
| **Data Ingestion** | `data_ingestion.py` | Loads alert data (parquet) and service graph (JSON) |
| **Alert Processor** | `alert_processor.py` | Enrichment, grouping, feature engineering, clustering,deduplication |
| **Model Manager** | `model_manager.py` | Trains/loads/versions KMeans clustering models |
| **Cluster Predictor** | `cluster_predictor.py` | Uses trained model to predict cluster labels |
| **Catalog Manager** | `catalog_manager.py` | Persistent cluster catalog with matching logic |
| **Result Storage** | `result_storage.py` | Exports results as CSV/JSON artifacts |

### 2.2 Pipeline Position

```
┌─────────────────┐    ┌──────────────────┐    ┌───────────────────┐    ┌─────────────────┐
│ DataIngestion    │ ──>│ AlertProcessor   │ ──>│ ClusterPredictor  │ ──>│ CatalogManager  │
│ (load alerts &   │    │ (enrich, group,  │    │ (predict cluster  │    │ (match/update   │
│  service graph)  │    │  features, dedup)│    │  labels via model)│    │  cluster catalog)│
└─────────────────┘    └──────────────────┘    └───────────────────┘    └─────────────────┘
                                                                                │
                                                                                v
                                                                        ┌─────────────────┐
                                                                        │ ResultStorage    │
                                                                        │ (save CSV/JSON)  │
                                                                        └─────────────────┘
```

### 2.3 How AlertProcessor Is Instantiated

The `AlertWorkflowScheduler` (in `job_scheduler.py`) creates `AlertProcessor` instances. Multiple instances may be created within a single workflow run for different alert populations:

```python
alert_processor = AlertProcessor(
    service_graph=service_graph,              # NetworkX DiGraph from DataIngestionService
    service_to_graph=service_to_graph,        # Dict mapping service_name -> graph node info
    service_features_cache=service_features_cache,  # Pre-computed graph features per service
    pagerank_cache=pagerank_cache             # Pre-computed PageRank scores per service
)
```

**Important:** The scheduler creates **separate** AlertProcessor instances for:
1. **Mapped alerts** — alerts successfully linked to service graph nodes
2. **Unmatched alerts** — alerts that didn't match any existing catalog cluster
3. **Retraining** — a fresh processor for model retraining scenarios

---

## 3. Module Overview

### 3.1 Class Attributes (from Config)

These are loaded from environment variables via the `Config` class at class-definition time:

| Attribute | Default | Source |
|-----------|---------|--------|
| `PCA_VARIANCE_THRESHOLD` | `0.95` | `Config.PCA_VARIANCE_THRESHOLD` |
| `OUTLIER_CONTAMINATION` | `0.05` | `Config.OUTLIER_CONTAMINATION` |
| `SERVICE_GRAPH_MAX_DEPTH` | `2` | `Config.SERVICE_GRAPH_MAX_DEPTH` |
| `SEVERITY_DOMINANCE_RATIO` | `0.5` | `Config.SEVERITY_DOMINANCE_RATIO` |

---

## 4. Class Design & Initialization

### 4.1 Constructor Parameters

```python
def __init__(self, service_graph: nx.DiGraph, service_to_graph: Dict,
             service_features_cache: Dict, pagerank_cache: Dict):
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `service_graph` | `networkx.DiGraph` | Directed graph representing service-to-service dependencies. Nodes are service names, edges represent relationships (e.g., "calls", "depends_on"). |
| `service_to_graph` | `Dict` | Lookup dictionary mapping `service_name` (string) to its graph node information including `properties` (cloud, platform, env, etc.). |
| `service_features_cache` | `Dict` | Pre-computed graph-level features per service. Keys are service names; values contain `pagerank`, `degree_total`, `in_degree`, `out_degree`, `betweenness`, `num_upstream`, `num_downstream`. |
| `pagerank_cache` | `Dict` | Pre-computed PageRank scores. Keys are service names; values are float scores. Computed by `DataIngestionService` using `nx.pagerank(alpha=0.85)`. |

### 4.2 Instance State Variables

| Variable | Type | Initial Value | Description |
|----------|------|---------------|-------------|
| `enriched_alerts` | `List[Dict]` | `[]` | Alerts after enrichment (mutated in-place) |
| `consolidated_groups` | `List[Dict]` | `[]` | Service-based groups created by `group_alerts_by_relationships()` |
| `alerts_df` | `pd.DataFrame` | `None` | DataFrame created during `engineer_features()`, may be modified by outlier removal |
| `feature_matrix` | `np.ndarray` | `None` | Raw (unscaled) feature matrix |
| `feature_matrix_scaled` | `np.ndarray` | `None` | Scaled feature matrix (overwritten by PCA output) |
| `feature_names` | `List[str]` | `[]` | Column names of the feature matrix |
| `scaler` | `StandardScaler` | new instance | Fitted scaler (zero mean, unit variance) |
| `pca` | `PCA` | `None` | Fitted PCA transformer |
| `outlier_indices` | `set` | `set()` | Set of original indices identified as outliers |
| `outlier_removal_enabled` | `bool` | `True` | Flag to enable/disable outlier detection |
| `_idx_mapping` | `Dict` | `None` | Maps post-outlier-removal indices to original indices. **Critical for correctness.** |
| `deduplicated_alerts` | `List[Dict]` | `[]` | Alerts after deduplication |
| `duplicate_groups` | `List[Dict]` | `[]` | Groups of duplicate alerts with representative info |

### 4.3 Statefulness Warning

**`AlertProcessor` is a stateful, mutable class.** Methods must be called in a specific order because later methods depend on state set by earlier methods. The alert dictionaries passed in are **mutated in-place** — fields are added directly to the original dictionary objects.

---

## 5. Processing Pipeline — End-to-End Flow

### 5.1 Method Invocation Order

The methods **must** be called in this sequence:

```
Step 1:  enrich_alerts(alerts)
            │
            ├── _enrich_alert_with_graph_info(alert)  (per alert)
            └── _get_service_dependencies(service)    (per mapped alert)
            │
Step 2:  group_alerts_by_relationships()              (MAPPED alerts only)
            │
            ├── _find_impacted_services(service)      (BFS downstream traversal)
            └── _calculate_all_group_metrics()
            │
Step 3:  engineer_features()
            │
            ├── _encode_severity(), _encode_alert_category(), _encode_alert_subcategory()
            ├── StandardScaler.fit_transform()
            ├── _remove_outliers()                    (IsolationForest)
            └── _apply_pca()                          (PCA reduction)
            │
Step 4:  assign_cluster_labels(labels, method)        (labels from ClusterPredictor)
            │
Step 5:  deduplicate_alerts()
            │
            └── _are_duplicates_basic(alert1, alert2) (pairwise rule check)
            │
Step 6:  rank_and_name_clusters()
            │
            ├── _generate_cluster_name(alerts)
            └── _generate_cluster_id(name)            (MD5 hash)
```

### 5.2 Branching Logic in the Scheduler

The scheduler (`job_scheduler.py`) splits alerts into two populations and uses AlertProcessor differently for each:

```
All Incoming Alerts
        │
        ├── enrich_alerts() ── split by mapping result
        │
        ├── MAPPED (graph_service != None)
        │       │
        │       ├── group_alerts_by_relationships()
        │       │
        │       └── CatalogManager.match_alerts_to_catalog()
        │               │
        │               ├── MATCHED ──> Assigned existing catalog cluster_id
        │               │                  (deduplicated separately)
        │               │
        │               └── UNMATCHED ──> Falls through to clustering
        │
        └── UNMAPPED (graph_service == None)
                │
                └── Combined with UNMATCHED ──> Goes to clustering
                                                    │
                                        ┌───────────┴───────────┐
                                        │                       │
                                   (No model exists)      (Model exists)
                                        │                       │
                                   Train new model        predict_clusters()
                                        │                       │
                                        └───────────┬───────────┘
                                                    │
                                         assign_cluster_labels()
                                         deduplicate_alerts()
                                         rank_and_name_clusters()
```

---

## 6. Phase 1: Alert Enrichment

### 6.1 Method: `enrich_alerts(alerts: List[Dict]) -> List[Dict]`

**Purpose:** Augment each raw alert dictionary with service graph information and dependency data.

**Input:** List of alert dictionaries (from `DataIngestionService`). Each alert has fields like `service_name`, `cloud`, `platform`, `environment`, `severity`, `alert_name`, `alert_category`, `alert_subcategory`, `namespace`, `pod`, `cluster`, `node`, `start_timestamp`, etc.

**Process:**
1. Iterate through all alerts
2. For each alert, call `_enrich_alert_with_graph_info(alert)` to attempt mapping
3. If mapping succeeds, call `_get_service_dependencies(graph_service)` to add dependency info
4. Store all alerts in `self.enriched_alerts`

**Output:** The same list of alert dicts, now with additional fields added in-place.

### 6.2 Method: `_enrich_alert_with_graph_info(alert: Dict) -> bool`

**Purpose:** Attempt to match a single alert to a node in the service dependency graph.

**Matching Logic (all conditions must be true):**

1. The alert's `service_name` must exist in `self.service_to_graph` (direct lookup)
2. If found, additional validation checks are performed:
   - `alert.cloud` must match `graph_node.cloud` (case-insensitive comparison)
   - `alert.platform` must match `graph_node.platform` (case-insensitive comparison)
   - `alert.environment` must match `graph_node.env` (case-insensitive comparison)
   - `alert.service_name` must match `graph_node.service_name` (case-insensitive comparison)

**Important behavior of `_matches()` inner function:**
- If the graph value is empty/null, the match is treated as `True` (permissive — missing graph data does not prevent matching)
- Only when the graph value is populated does an actual comparison occur

**Fields added to alert on SUCCESSFUL match:**

| Field | Value | Description |
|-------|-------|-------------|
| `graph_service` | `service_name` | The matched service graph node name |
| `graph_info` | `dict` | Full graph node info from `service_to_graph` |
| `mapping_method` | `'service_name'` | How the mapping was achieved |
| `mapping_confidence` | `1.0` | Confidence score (always 1.0 for direct match) |

**Fields added to alert on FAILED match:**

| Field | Value |
|-------|-------|
| `graph_service` | `None` |
| `graph_info` | `None` |
| `mapping_method` | `'unmapped'` |
| `mapping_confidence` | `0.0` |

### 6.3 Method: `_get_service_dependencies(service_name: str) -> Dict`

**Purpose:** For a successfully mapped service, retrieve its upstream and downstream dependencies from the NetworkX DiGraph.

**Returns:**
```python
{
    'upstream': [{'service': 'svc-a', 'relationship': 'calls'}, ...],    # predecessors
    'downstream': [{'service': 'svc-b', 'relationship': 'depends_on'}, ...],  # successors
    'peers': []  # currently unused, always empty
}
```

**Graph traversal:**
- **Upstream:** `self.service_graph.predecessors(service_name)` — services that call/depend on this service
- **Downstream:** `self.service_graph.successors(service_name)` — services this service calls/depends on
- Edge data key used: `relationship_type`

---

## 7. Phase 2: Service-Based Grouping

### 7.1 Method: `group_alerts_by_relationships() -> List[Dict]`

**Purpose:** Group **mapped** alerts (those with `graph_service` set) by their service graph relationships. This creates "consolidated groups" where each group represents a primary service and its downstream blast radius.

**Precondition:** Must be called ONLY with mapped alerts. The scheduler sets `alert_processor.enriched_alerts = mapped_alerts` before calling this method.

**method:**

1. **Partition by service:** Group alerts by their `graph_service` value into `service_groups` dict
2. **For each service group:**
   - Call `_find_impacted_services(service_name, max_depth=2)` to find downstream services via BFS
   - Create a consolidated group containing the service's alerts plus blast radius metadata
3. **Compute group-level statistics:** severity distribution, category distribution, time span, most common alert type
4. **Assign `initial_group_id`** to each alert — this ID is used later by `CatalogManager` for catalog matching
5. **Propagate `impacted_services` and `primary_service`** from the group down to each individual alert
6. **Calculate graph metrics** via `_calculate_all_group_metrics()`

### 7.2 Group Data Structure

Each consolidated group is a dictionary with these fields:

```python
{
    'group_id': int,                     # Sequential index
    'primary_service': str,              # Main service name or symptom service
    'impacted_services': List[str],      # Downstream services within blast radius
    'alerts': List[Dict],                # Alerts in this group
    'alert_count': int,                  # Number of alerts
    'impact_radius': int,                # Number of impacted downstream services
    'alert_types': List[str],            # Unique alert names
    'namespaces': List[str],             # Unique namespaces
    'clusters': List[str],              # Unique k8s clusters
    'severity_distribution': Dict,       # e.g., {'critical': 5, 'warning': 3}
    'category_distribution': Dict,       # Alert category counts
    'subcategory_distribution': Dict,    # Alert subcategory counts
    'earliest_alert': float,             # Min start_timestamp
    'latest_alert': float,               # Max start_timestamp
    'time_span_minutes': float,          # Time window of alerts in group
    'most_common_alert': str,            # Most frequent alert name
    'most_common_category': str,         # Most frequent category
    'most_common_subcategory': str,      # Most frequent subcategory
    # Graph metrics (added by _calculate_all_group_metrics):
    'impacted_services_count': int,
    'blast_radius': int,
    'upstream_services': List[str],
    'upstream_count': int,
    'downstream_services': List[str],
    'downstream_count': int,
    'avg_pagerank': float
}
```

### 7.3 Method: `_find_impacted_services(service_name, max_depth=2) -> Set`

**Purpose:** Perform a Breadth-First Search (BFS) on the service graph to find all downstream services within `max_depth` hops.

**method:**
```
Initialize: queue = [(service_name, depth=0)], visited = {}, impacted = {}
While queue is not empty:
    Pop (current_node, depth) from queue
    Add current_node to visited
    If depth >= max_depth: skip
    For each successor of current_node in DiGraph:
        If successor not visited AND successor != original service_name:
            Add successor to impacted set
            Enqueue (successor, depth + 1)
Return impacted set
```

**Default max_depth:** 2 (configurable via `SERVICE_GRAPH_MAX_DEPTH` env var)

**Note:** The BFS follows edges in the **forward direction** (successors only), representing downstream impact propagation. It does NOT traverse upstream (predecessors).

### 7.4 Method: `_group_unmapped_alerts(unmapped_alerts) -> List[Dict]`

**Purpose:** Fallback grouping for alerts that could not be mapped to the service graph. Groups by best available context using a priority hierarchy.

**Grouping key priority (highest to lowest):**
1. `cluster + namespace` — most specific
2. `namespace + node`
3. `cluster` alone
4. `namespace` alone
5. `node` alone
6. `unknown:unknown` — no identifiable context

> **Note:** This method is defined in the class but is **not called** in the current scheduler workflow. Unmapped alerts currently go directly to ML-based clustering via `engineer_features(alert_features_only=True)`.

### 7.5 Graph Metrics Calculation

Two methods compute graph-topology metrics for each group:

**`_calculate_service_graph_metrics(service_name)`** — Per-service metrics:
- `pagerank`: From pre-computed cache, indicates relative importance in the graph
- `degree`, `in_degree`, `out_degree`: Connection counts
- `upstream_services`, `downstream_services`: Direct neighbor lists

**`_calculate_group_metrics(group)`** — Per-group aggregated metrics:
- Pulls metrics for the `primary_service`
- Computes average PageRank across all services in the group (including `correlated_services` if any)
- Copies upstream/downstream counts

---

## 8. Phase 3: Feature Engineering

### 8.1 Method: `engineer_features(target_pca_components=None, alert_features_only=False)`

**Purpose:** Transform enriched alert dictionaries into a numeric feature matrix suitable for KMeans clustering.

**Parameters:**
- `target_pca_components` (int, optional): If set, use this many PCA components. If `None`, use variance-based threshold (95%). In production, typically set to `8` for model stability.
- `alert_features_only` (bool): When `True`, skip graph features. **This is the production default** — used for both unmapped alerts and the current clustering workflow.

**Returns:** `Tuple[np.ndarray, StandardScaler, PCA, List[str]]`
- `feature_matrix_scaled`: The final feature matrix (after scaling, outlier removal, and PCA)
- `scaler`: The fitted StandardScaler instance
- `pca`: The fitted PCA instance
- `feature_names`: List of original feature column names (pre-PCA)

### 8.2 Feature Vector Composition

**When `alert_features_only=False` (13 features total):**

| # | Feature | Type | Source | Description |
|---|---------|------|--------|-------------|
| 1 | `pagerank` | float | Graph cache | PageRank centrality score |
| 2 | `degree` | int | Graph cache | Total connections (in + out) |
| 3 | `in_degree` | int | Graph cache | Incoming edge count |
| 4 | `out_degree` | int | Graph cache | Outgoing edge count |
| 5 | `betweenness` | float | Graph cache | Betweenness centrality |
| 6 | `num_upstream` | int | Graph cache | Number of upstream services |
| 7 | `num_downstream` | int | Graph cache | Number of downstream services |
| 8 | `severity` | int (0-4) | Alert metadata | Encoded severity level |
| 9 | `category` | int (0-6) | Alert metadata | Encoded alert category |
| 10 | `subcategory` | int (0-9) | Alert metadata | Encoded alert subcategory |
| 11 | `is_error` | binary (0/1) | Alert name | Contains "error" keyword |
| 12 | `is_resource` | binary (0/1) | Alert name | Contains "cpu"/"memory"/"resource" |
| 13 | `is_network` | binary (0/1) | Alert name | Contains "network" keyword |

**When `alert_features_only=True` (6 features total):**

Only features 8-13 are included. Graph features (1-7) are completely omitted.

### 8.3 Feature Engineering Pipeline Steps

```
Raw alerts (List[Dict])
    │
    ▼
Build feature_dict per alert (iterate DataFrame rows)
    │
    ▼
Create features DataFrame → feature_matrix (numpy array)
    │
    ▼
StandardScaler.fit_transform() → feature_matrix_scaled
    │  (zero mean, unit variance normalization)
    │
    ▼
IsolationForest outlier removal (if enabled & n >= 20)
    │  (removes outlier rows, updates _idx_mapping)
    │
    ▼
PCA dimensionality reduction
    │  (reduces to target components or 95% variance)
    │
    ▼
Final feature_matrix_scaled (overwritten with PCA output)
```

**Critical side effect:** `self.feature_matrix_scaled` is **overwritten** by the PCA output. The variable name is misleading — after `_apply_pca()`, it holds PCA-transformed data, not just scaled data.

---

## 9. Phase 4: Outlier Detection & Removal

### 9.1 Method: `_remove_outliers()`

**Purpose:** Identify and remove statistical outliers from the feature matrix to improve clustering quality.

**Algorithm:** Isolation Forest (scikit-learn)
- An ensemble method that isolates observations by randomly selecting a feature and then randomly selecting a split value between the max and min of that feature
- Outliers are easier to isolate and thus have shorter path lengths in the trees

**Configuration:**
| Parameter | Value | Description |
|-----------|-------|-------------|
| `contamination` | 0.05 (5%) | Expected proportion of outliers in the dataset |
| `random_state` | 42 | Reproducibility seed |
| `n_estimators` | 100 | Number of isolation trees |

**Activation conditions:**
- `self.outlier_removal_enabled` is `True` (always true by default)
- Number of samples > `Config.OUTLIER_MIN_SAMPLES` (default: 20)

### 9.2 What Happens During Outlier Removal

1. **Fit & Predict:** IsolationForest is fitted on `feature_matrix_scaled`, predicting -1 for outliers and 1 for inliers
2. **Identify outlier indices:** `np.where(outlier_labels == -1)` gives the set of row indices flagged as outliers
3. **removal from feature matrix:** `np.delete(feature_matrix_scaled, outlier_indices, axis=0)`
4. **removal from DataFrame:** `alerts_df.iloc[valid_indices].reset_index(drop=True)`
5. **Build index mapping:** Creates `_idx_mapping = {new_index: original_index}` so downstream methods can trace back

### 9.3 Why This Matters

After outlier removal:
- `feature_matrix_scaled` has **fewer rows** than the original `enriched_alerts` list
- `alerts_df` has been sliced and reset — its indices no longer match `enriched_alerts` positions
- Every downstream method that touches alert indices **must** use `_idx_mapping` to translate

### 9.4 Error Handling

If `IsolationForest` fails for any reason:
- `outlier_indices` is set to empty set
- `_idx_mapping` is set to identity mapping `{i: i}`
- Processing continues without outlier removal

---

## 10. Phase 5: PCA Dimensionality Reduction

### 10.1 Method: `_apply_pca(target_components=None)`

**Purpose:** Reduce the feature matrix dimensionality using Principal Component Analysis.

**Two modes of operation:**

| Mode | Condition | Behavior |
|------|-----------|----------|
| **Fixed components** | `target_components` is set (e.g., 8) | Uses exactly `min(target, n_features, n_samples)` components |
| **Variance-based** | `target_components` is `None` | Retains enough components to explain 95% variance (`PCA_VARIANCE_THRESHOLD`) |

**Production default:** `FIXED_PCA_COMPONENTS = 8` (from config). This ensures model stability — variance-based PCA may produce different numbers of components across runs, causing dimension mismatches with the trained model.

**Side effects:**
- `self.pca` is set to the fitted PCA instance
- `self.feature_matrix_scaled` is **overwritten** with the PCA-transformed matrix
- `self.feature_matrix_pca` also holds the PCA-transformed matrix (redundant)

**Error handling:** If PCA fails, `self.pca` is set to `None`, and the original (non-PCA) matrix remains in `feature_matrix_scaled`.

---

## 11. Phase 6: Cluster Label Assignment

### 11.1 Method: `assign_cluster_labels(cluster_labels: np.ndarray, clustering_method: str)`

**Purpose:** Take the cluster label array from `ClusterPredictor.predict_clusters()` and write the labels back into each alert dictionary in `enriched_alerts`.

**The index remapping challenge:**

The `cluster_labels` array has indices corresponding to the **cleaned** feature matrix (after outlier removal). But we need to assign labels back to the **original** `enriched_alerts` list. This requires reversing the `_idx_mapping`.

**method:**
1. **Build reverse mapping:** `{original_index: new_index}` from `_idx_mapping` (which is `{new: orig}`)
2. **For each alert in `enriched_alerts`:**
   - If the alert's index is in `outlier_indices` → `cluster_id = -1`, method = `'outlier_removed'`
   - If the alert's index exists in `reverse_mapping` → look up `new_idx`, get `cluster_labels[new_idx]`
   - If no `_idx_mapping` exists → use direct indexing `cluster_labels[i]`
   - Otherwise → `cluster_id = -1`, method = `'unmapped'`

**Fields added to each alert:**

| Field | Type | Description |
|-------|------|-------------|
| `cluster_id` | int | The numeric cluster label (or -1 for outliers/unmapped) |
| `clustering_method` | str | `'kmeans'`, `'outlier_removed'`, or `'unmapped'` |

---

## 12. Phase 7: Alert Deduplication

### 12.1 Method: `deduplicate_alerts() -> List[Dict]`

**Purpose:** Within each cluster, identify duplicate alerts and select representative alerts.

**method:**
1. Group alerts by `cluster_id` using DataFrame groupby
2. For each cluster:
   - Build list of `(original_index, alert_dict)` tuples using `_idx_mapping`
   - Pairwise comparison: compare alert `i` with all subsequent alerts `j`
   - If `_are_duplicates_basic(alert_i, alert_j)` returns True:
     - Mark `alert_j` as duplicate (`is_duplicate = True`, `duplicate_of = idx_i`)
     - Add `j` to processed set
   - The first (unmatched) alert becomes the **representative** → added to `deduplicated_alerts`
3. Track duplicate groups for reporting

### 12.2 Deduplication Rules: `_are_duplicates_basic(alert1, alert2) -> bool`

Two alerts are considered duplicates if **either** of these conditions is true:

**Rule 1 — Same alert targeting same resource:**
```
alert1.alert_name == alert2.alert_name  (must be non-empty)
AND (
    alert1.pod == alert2.pod  (must be non-empty)
    OR
    alert1.service_name == alert2.service_name  (must be non-empty)
)
```

**Rule 2 — Same service with same classification:**
```
alert1.service_name == alert2.service_name  (must be non-empty)
AND alert1.alert_category == alert2.alert_category  (must be non-empty)
AND alert1.alert_subcategory == alert2.alert_subcategory  (must be non-empty)
```

**Important details:**
- All comparisons require the values to be **non-empty**
- The deduplication is **order-dependent** — the first alert in each group becomes the representative
- Deduplication happens **within clusters only**, never across clusters

### 12.3 Output

**`self.deduplicated_alerts`:** List of representative alerts (duplicates removed)

**`self.duplicate_groups`:** List of dicts with:
```python
{
    'representative_idx': int,     # Original index of the representative alert
    'duplicate_indices': List[int], # Original indices of the duplicates
    'count': int                   # Total alerts in group (representative + duplicates)
}
```

**Fields added to each alert:**

| Field | Value (representative) | Value (duplicate) |
|-------|----------------------|-------------------|
| `is_duplicate` | `False` | `True` |
| `duplicate_of` | `None` | Original index of representative |

---

## 13. Phase 8: Cluster Naming & Ranking

### 13.1 Method: `rank_and_name_clusters() -> List[Dict]`

**Purpose:** Replace numeric cluster IDs with readable names and hash-based identifiers.

**Process:**
1. Group all alerts by their numeric `cluster_id`
2. For each cluster (excluding -1):
   - Generate a descriptive name via `_generate_cluster_name()`
   - Generate a stable ID via `_generate_cluster_id()` (MD5 hash, first 16 hex chars)
3. Sort clusters by `ranking_score` (currently always 0.0 — ranking not yet implemented)
4. Assign rank numbers starting from 1
5. Update every alert with the new `cluster_id`, `cluster_rank`, `cluster_name`, `cluster_score`
6. Outlier alerts (cluster_id = -1) get `cluster_name = 'outlier_or_unmapped'`

### 13.2 Cluster Name Generation: `_generate_cluster_name(cluster_alerts)`

**Name format:** `{most_common_alert}-{top_service}-{severity_key}`

**Examples:**
- `HighMemoryUsage-payment-service-critical`
- `PodCrashLoopBackOff-frontend-api-mixed`
- `CPUThrottling-unknown-high`

**How each component is determined:**

| Component | Logic |
|-----------|-------|
| `most_common_alert` | Most frequent `alert_name` among alerts for the top service |
| `top_service` | Most frequent service in cluster (`graph_service` preferred, fallback to `service_name`). `'unmapped'` if none. |
| `severity_key` | `'critical'` if >50% alerts are critical, `'high'` if >50% high, otherwise `'mixed'`. The 50% threshold is effectively hardcoded (uses `len(cluster_alerts) * 0.5`). |

### 13.3 Cluster ID Generation: `_generate_cluster_id(cluster_name)`

**method:** `MD5(cluster_name)[:16]` — takes first 16 hexadecimal characters of the MD5 hash.

**Properties:**
- **Deterministic:** Same name always produces the same ID
- **Stable across runs:** If the same alert pattern recurs, it gets the same cluster ID
- **Collision-resistant:** 16 hex chars = 64 bits, providing ~2^32 unique IDs before birthday paradox collision

### 13.4 Cluster Metadata Output

```python
{
    'cluster_id': str,              # 16-char hex hash
    'numeric_cluster_id': int,      # Original numeric ID from KMeans
    'cluster_name': str,            # Human-readable name
    'ranking_score': float,         # Currently always 0.0
    'alert_count': int,
    'unique_alert_types': int,
    'unique_services': int,
    'primary_service': str,
    'most_common_category': str,
    'rank': int                     # 1-based rank
}
```

---

## 14. Index Mapping System — The Critical Detail

This is the most important implementation detail for maintaining or debugging this module.

### 14.1 The Problem

When outliers are removed during `_remove_outliers()`, rows are deleted from:
- `self.feature_matrix_scaled` (numpy array)
- `self.alerts_df` (pandas DataFrame, re-indexed with `reset_index(drop=True)`)

But `self.enriched_alerts` (the original list of alert dicts) remains unchanged. The indices in these two representations no longer correspond.

### 14.2 The Solution: `_idx_mapping`

`_idx_mapping` is a dictionary of `{new_index: original_index}` pairs.


### 14.3 Where `_idx_mapping` Is Used

| Method | Usage |
|--------|-------|
| `assign_cluster_labels()` | Reverse-maps `_idx_mapping` to translate original alert indices to cleaned indices, then reads `cluster_labels[cleaned_idx]` |
| `deduplicate_alerts()` | For each row in `alerts_df.groupby('cluster_id')`, translates the DataFrame index to original `enriched_alerts` index via `_idx_mapping[idx]` |

### 14.4 When `_idx_mapping` Is Identity

If no outliers are detected, or if outlier removal is disabled, or if it fails:
```python
self._idx_mapping = {i: i for i in range(len(self.alerts_df))}
```
This identity mapping means "new index == original index" and all downstream operations work as if no remapping occurred.

---

## 15. Feature Encoding Reference

### 15.1 Severity Encoding

| Severity Level | Encoded Value |
|---------------|---------------|
| `critical` | 4 |
| `high` | 3 |
| `warning` | 2 |
| `info` | 1 |
| `unknown` / unrecognized | 0 |

### 15.2 Alert Category Encoding

| Category | Encoded Value |
|----------|---------------|
| `saturation` | 1 |
| `anomaly` | 2 |
| `error` | 3 |
| `critical` | 4 |
| `failure` | 5 |
| `slo` | 6 |
| `unknown` / unrecognized | 0 |

### 15.3 Alert Subcategory Encoding

| Subcategory | Encoded Value |
|-------------|---------------|
| `hpa` | 1 |
| `resource` | 2 |
| `error` | 3 |
| `node` | 4 |
| `memory` | 5 |
| `latency` | 6 |
| `other` | 7 |
| `volume` | 8 |
| `cpu` | 9 |
| `unknown` / unrecognized | 0 |

### 15.4 Workload Type Encoding (defined but unused)

| Workload Type | Encoded Value |
|---------------|---------------|
| `deployment` | 1 |
| `daemonset` | 2 |
| `statefulset` | 3 |
| `job` | 4 |
| `cronjob` | 5 |
| `pod` | 6 |
| `unknown` | 0 |

> **Note:** `_encode_workload_type()` is defined in the class but is **never called** in the current implementation. It is reserved for potential future feature engineering.

### 15.5 Binary Feature Flags

| Feature | Detection Logic |
|---------|----------------|
| `is_error` | `'error' in alert_name.lower()` |
| `is_resource` | `any word in ['cpu', 'memory', 'resource'] found in alert_name.lower()` |
| `is_network` | `'network' in alert_name.lower()` |

---

## 16. Configuration Parameters

All configuration is driven by environment variables, loaded via the `Config` class. Defaults are used if environment variables are not set.

### 16.1 AlertProcessor-Specific Configuration

| Environment Variable | Config Attribute | Default | Type | Description |
|---------------------|-----------------|---------|------|-------------|
| `PCA_VARIANCE_THRESHOLD` | `Config.PCA_VARIANCE_THRESHOLD` | `0.95` | float | Minimum variance to retain in PCA (when using variance-based mode) |
| `FIXED_PCA_COMPONENTS` | `Config.FIXED_PCA_COMPONENTS` | `8` | int or None | Fixed number of PCA components (set to "none" for variance-based) |
| `OUTLIER_CONTAMINATION` | `Config.OUTLIER_CONTAMINATION` | `0.05` | float | Expected proportion of outliers (0.0 to 0.5) |
| `OUTLIER_MIN_SAMPLES` | `Config.OUTLIER_MIN_SAMPLES` | `20` | int | Minimum samples required to run outlier detection |
| `SERVICE_GRAPH_MAX_DEPTH` | `Config.SERVICE_GRAPH_MAX_DEPTH` | `2` | int | BFS depth for downstream impact traversal |
| `PATH_SIMILARITY_THRESHOLD` | `Config.PATH_SIMILARITY_THRESHOLD` | `0.6` | float | (Declared, not currently used) |
| `RELATED_PATH_SIMILARITY_CUTOFF` | `Config.RELATED_PATH_SIMILARITY_CUTOFF` | `0.3` | float | (Declared, not currently used) |
| `SEVERITY_DOMINANCE_RATIO` | `Config.SEVERITY_DOMINANCE_RATIO` | `0.5` | float | (Declared as class attr, naming logic uses hardcoded `0.5`) |

### 16.2 Related Configuration (Used by Scheduler)

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `WORKFLOW_INTERVAL_MINUTES` | `5` | How frequently the pipeline runs |
| `MIN_CLUSTERING_SAMPLES` | `10` | Minimum alerts needed for clustering |
| `ALERT_ALLOWED_PLATFORMS` | `aks,gke` | Platform filter for incoming alerts |
| `ALERT_FIRING_STATUS` | `firing` | Only process alerts with this status |
| `CATALOG_ML_SIMILARITY_THRESHOLD` | `0.7` | Cosine similarity threshold for catalog fuzzy matching |

---

## 17. Integration with Other Modules

### 17.1 DataIngestionService → AlertProcessor

**What DataIngestionService provides:**
- `service_graph` (nx.DiGraph): Built from service graph JSON. Nodes have properties from the graph data. Edges have `relationship_type`.
- `service_to_graph` (Dict): Maps each `service_name` to its full graph node info for quick lookup.
- `service_features_cache` (Dict): Pre-computed per-service features (pagerank, degree, betweenness, etc.).
- `pagerank_cache` (Dict): `nx.pagerank(service_graph, alpha=0.85)` result.

### 17.2 AlertProcessor → ClusterPredictor

**What AlertProcessor provides:**
- `feature_matrix_scaled` (np.ndarray): Final feature matrix (scaled + outlier-removed + PCA-transformed)

**What ClusterPredictor returns:**
- `cluster_labels` (np.ndarray): Array of integer cluster IDs, same length as `feature_matrix_scaled`
- `clustering_method` (str): Algorithm name (e.g., `'kmeans'`)
- `avg_confidence` (float): Average cluster assignment confidence

AlertProcessor then calls `assign_cluster_labels(cluster_labels, method)` to write labels back.

### 17.3 AlertProcessor → CatalogManager

**Interaction 1: Before AlertProcessor clustering**
The scheduler calls `CatalogManager.match_alerts_to_catalog(grouped_alerts)` on the mapped+grouped alerts. This returns matched alerts (assigned existing catalog cluster IDs) and unmatched alerts (need new clustering).

**Interaction 2: After AlertProcessor processing**
The scheduler calls `CatalogManager.update_catalog_with_clusters(all_deduplicated)` with the final deduplicated alerts to update the persistent catalog.

The catalog matching strategies (in priority order):
1. **Priority 1:** Exact match on `category + subcategory + service`
2. **Priority 2:** Exact match on `category + service`
3. **ML-based:** TF-IDF cosine similarity of alert signature vs catalog entry signatures

### 17.4 AlertProcessor → ResultStorage

The scheduler passes AlertProcessor outputs to ResultStorage:
- `enriched_alerts` (all alerts, including duplicates)
- `deduplicated_alerts` (unique representative alerts only)
- `cluster_metadata` (from `rank_and_name_clusters()`)
- `duplicate_groups` and `duplicate_alerts` for duplicate tracking

ResultStorage generates:
- **Level 1 CSV:** Cluster-level view (one row per cluster)
- **Level 2 CSV:** Alert-level view (one row per deduplicated alert)
- **JSON files:** Clustering statistics, run metadata, mapping statistics

### 17.5 AlertProcessor → ModelManager

During initial training or retraining:
- AlertProcessor provides `feature_matrix_scaled`, `scaler`, `pca`, `feature_names`
- ModelManager trains a KMeans model, saves `model.pkl`, `scaler.pkl`, `pca.pkl`, and metadata JSON
- The trained model is later loaded by ClusterPredictor for inference

---

## 18. Data Flow Diagrams

### 18.1 Alert Lifecycle Through AlertProcessor

```
Raw Alert Dict (from DataIngestionService)
│
│ Fields: service_name, severity, alert_name, alert_category, 
│         alert_subcategory, namespace, pod, cluster, node, 
│         platform, cloud, environment, start_timestamp, ...
│
├── enrich_alerts() ── adds: graph_service, graph_info, 
│                            mapping_method, mapping_confidence,
│                            dependencies
│
├── group_alerts_by_relationships() ── adds: initial_group_id,
│                                            impacted_services,
│                                            primary_service
│
│   [Alert goes through CatalogManager matching]
│   [If unmatched, continues to clustering]
│
├── engineer_features() ── creates numeric feature vector
│       (internal: alerts_df, feature_matrix_scaled, _idx_mapping)
│
├── [ClusterPredictor.predict_clusters(feature_matrix)] ── returns labels
│
├── assign_cluster_labels() ── adds: cluster_id, clustering_method
│
├── deduplicate_alerts() ── adds: is_duplicate, duplicate_of
│
└── rank_and_name_clusters() ── updates: cluster_id (to hash),
                                         cluster_rank, cluster_name,
                                         cluster_score
```

### 18.2 Feature Matrix Transformations

```
enriched_alerts (List[Dict], N items)
        │
        ▼
alerts_df (DataFrame, N rows) ← engineer_features() creates this
        │
        ▼
feature_matrix (ndarray, N × F)  ← F = 6 (alert-only) or 13 (with graph)
        │
        ▼  StandardScaler.fit_transform()
feature_matrix_scaled (ndarray, N × F)
        │
        ▼  _remove_outliers()  [if n >= 20]
feature_matrix_scaled (ndarray, M × F)  ← M = N - outliers
alerts_df (DataFrame, M rows, re-indexed)
_idx_mapping = {0..M-1 → original indices}
        │
        ▼  _apply_pca()
feature_matrix_scaled (ndarray, M × P)  ← P = PCA components (e.g., 8)
        │
        ▼  [returned to caller for clustering]
```

---

## 19. Alert Data Model — Field Reference

### 19.1 Fields Present on Raw Incoming Alert

| Field | Type | Source |
|-------|------|--------|
| `alert_name` | str | Alert rule name |
| `severity` | str | `critical`, `high`, `warning`, `info` |
| `service_name` | str | Originating service (from alert labels) |
| `namespace` | str | Kubernetes namespace |
| `pod` | str | Pod name |
| `node` | str | Node name |
| `cluster` | str | Kubernetes cluster name |
| `platform` | str | `aks` or `gke` |
| `cloud` | str | Cloud provider |
| `environment` | str | Environment name |
| `alert_category` | str | High-level category |
| `alert_subcategory` | str | Sub-category |
| `workload_type` | str | K8s workload type |
| `start_timestamp` | float | Unix timestamp |
| `description` | str | Alert description text |
| `_id` | str | Unique alert identifier |

### 19.2 Fields Added by AlertProcessor

| Field | Added By | Type | Description |
|-------|----------|------|-------------|
| `graph_service` | `enrich_alerts()` | str or None | Matched service graph node name |
| `graph_info` | `enrich_alerts()` | dict or None | Full graph node info |
| `mapping_method` | `enrich_alerts()` | str | `'service_name'` or `'unmapped'` |
| `mapping_confidence` | `enrich_alerts()` | float | `1.0` (mapped) or `0.0` (unmapped) |
| `dependencies` | `enrich_alerts()` | dict | Upstream/downstream services |
| `initial_group_id` | `group_alerts_by_relationships()` | int | Service-based group index |
| `impacted_services` | `group_alerts_by_relationships()` | list | Downstream services in blast radius |
| `primary_service` | `group_alerts_by_relationships()` | str | Primary service of the group |
| `cluster_id` | `assign_cluster_labels()` | int → str | Numeric label, later replaced by hash |
| `clustering_method` | `assign_cluster_labels()` | str | `'kmeans'`, `'outlier_removed'`, etc. |
| `is_duplicate` | `deduplicate_alerts()` | bool | Whether this alert is a duplicate |
| `duplicate_of` | `deduplicate_alerts()` | int or None | Index of representative alert |
| `cluster_rank` | `rank_and_name_clusters()` | int | 1-based importance rank |
| `cluster_name` | `rank_and_name_clusters()` | str | Human-readable cluster name |
| `cluster_score` | `rank_and_name_clusters()` | float | Ranking score (currently 0.0) |

---

## 20. Edge Cases & Error Handling

### 20.1 Empty Inputs
- **No alerts:** `enrich_alerts([])` returns `[]`. Downstream methods operate on empty lists without error.
- **No service graph nodes:** All alerts become unmapped (`graph_service = None`).

### 20.2 Insufficient Data for Outlier Removal
- If `len(feature_matrix_scaled) <= OUTLIER_MIN_SAMPLES` (20): outlier removal is skipped entirely; `_idx_mapping` is set to identity.

### 20.3 PCA Component Count Exceeds Dimensions
- `_apply_pca()` caps components at `min(target_components, n_features, n_samples)`.

### 20.4 All Alerts Are Outliers
- It might be possible with high contamination values. All alerts would get `cluster_id = -1`. The deduplication and naming phases would produce empty outputs.

### 20.5 Single Alert in Cluster
- Deduplication produces the alert as-is (no duplicates possible).
- Naming produces a valid name from that single alert.

### 20.6 PCA Failure
- If PCA throws an exception, `self.pca = None` and the unmodified `feature_matrix_scaled` continues being used.

### 20.7 Cluster Naming with No Services
- If no alerts in a cluster have `graph_service` or `service_name`, `top_service = 'unmapped'`.
- If no `alert_name` values exist, `most_common_alert = 'unknown'`.
- Result: `"unknown-unmapped-mixed"`

---
