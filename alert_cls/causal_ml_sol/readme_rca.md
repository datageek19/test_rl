# Causal Inference Root Cause Analysis for Microservices

This module implements **causal inference-based root cause analysis (RCA)** for identifying the root causes of elevated latencies in microservices architectures.

## Overview

The solution uses **DoWhy's Graphical Causal Models (GCM)** to:
1. Model causal relationships between service latencies based on dependency graphs
2. Attribute elevated latencies at target services to upstream services
3. Identify root causes by analyzing attribution scores and service topology

**Reference**: [DoWhy GCM RCA Tutorial](https://www.pywhy.org/dowhy/main/example_notebooks/gcm_rca_microservice_architecture.html)

## Architecture

```
┌─────────────────────┐
│  Alert Consolidation │  ← Groups alerts by service relationships
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Latency Data Prep  │  ← Normal baseline + Anomalous measurements
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Causal Model Build │  ← GCM from service dependency graph
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Anomaly Attribution│  ← Attribute latency to upstream services
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Root Cause Ranking │  ← Identify and rank root causes
└─────────────────────┘
```

## Installation

### Prerequisites

```bash
pip install dowhy[gcm]
pip install networkx pandas numpy scipy
```

### Required Packages

- `dowhy`: Causal inference library
- `networkx`: Graph processing
- `pandas`: Data manipulation
- `numpy`: Numerical computations
- `scipy`: Statistical functions

## Usage

### Quick Start (Alerts Only - No Latency Data Needed!)

```python
from rca_pipeline import RCAPipeline

# Initialize pipeline - latency will be inferred from alerts
pipeline = RCAPipeline(
    alerts_csv_path='alert_data.csv',
    graph_json_path='graph_data.json',
    output_dir='./rca_results'
    # No latency_data_path needed!
)

# Run complete pipeline
results = pipeline.run_complete_pipeline()
```

### Command Line

```bash
# Basic usage (latency inferred from alerts)
python rca_pipeline.py \
    --alerts alert_data.csv \
    --graph graph_data.json \
    --output ./results

# With real latency data (optional, for better accuracy)
python rca_pipeline.py \
    --alerts alert_data.csv \
    --graph graph_data.json \
    --latency-data latency_measurements.csv \
    --normal-latency normal_baseline.csv \
    --output ./results

# Analyze specific service
python rca_pipeline.py \
    --alerts alert_data.csv \
    --graph graph_data.json \
    --target-service "api-gateway" \
    --output ./results
```

## Data Requirements

### 1. Alert Data (`alert_data.csv`) - **REQUIRED**

Format: Same as alert consolidation input
- `service_name`: Service identifier
- `alert_name`: Alert type
- `starts_at` or `start_timestamp`: Alert timestamp
- `status`: Alert status (firing/resolved)
- `severity`: Alert severity (critical/high/warning/info) - **recommended**
- `graph_service`: Service mapped to service graph (from consolidation)
- Other metadata (namespace, cluster, etc.)

**Note**: Latency is **inferred from alerts** - no latency measurements needed!

### 2. Service Graph (`graph_data.json`) - **REQUIRED**

Format: Array of relationship objects
```json
[
  {
    "source_name": "service-a",
    "target_name": "service-b",
    "relationship_type": "CALLS",
    "source_properties": {"name": "service-a", ...},
    "target_properties": {"name": "service-b", ...}
  }
]
```

### 3. Latency Data (Optional - Only if Available)

**If latency data is NOT provided**, the system will:
1. Infer latency from alert patterns
2. Estimate baseline from service graph structure
3. Elevate latency for services with alerts
4. Propagate latency through dependencies

See [ALERT_BASED_RCA_GUIDE.md](ALERT_BASED_RCA_GUIDE.md) for details.

## How It Works

### 1. Latency Inference from Alerts (No Measurements Needed!)

**Key Innovation**: We infer latency from alert patterns:

- **Services with alerts** → Elevated latency (inferred)
- **Alert frequency** → Latency severity (more alerts = worse)
- **Alert severity** → Latency impact (critical > high > warning)
- **Alert-free periods** → Normal baseline latency

See [ALERT_BASED_RCA_GUIDE.md](ALERT_BASED_RCA_GUIDE.md) for detailed explanation.

### 2. Causal Graph Construction

The service dependency graph is converted to a causal graph where:
- **Nodes**: Services
- **Edges**: Causal dependencies (if Service A calls Service B, B's latency affects A)
- **Direction**: Downstream services depend on upstream services

### 3. Latency Modeling

For each service, latency is modeled as:
```
Observed_Latency(Service) = Intrinsic_Latency(Service) + 
                            Sum(Latency of all downstream services)
```

**Inferred latency** respects this model:
- Baseline estimated from graph structure
- Elevated for services with alerts
- Propagates downstream through dependencies

### 4. Anomaly Attribution

Using GCM's Shapley-based attribution:
- For each target service with elevated latency
- Calculate contribution of each upstream service
- Score indicates how much each upstream service contributes to the anomaly

### 5. Root Cause Identification

Root causes are identified by:
1. **High attribution scores**: Service significantly contributes to anomaly
2. **Latency increase**: Service shows elevated latency itself
3. **Topology**: Root services (no upstream) or services with minimal dependencies

## Output Files

### 1. `latency_attributions.csv`

Attribution scores for each upstream service:
- `target_service`: Service with elevated latency
- `upstream_service`: Potential cause
- `attribution_score`: Contribution score (higher = more likely cause)
- `latency_increase`: Percentage increase in latency
- `baseline_mean`, `anomalous_mean`: Latency statistics

### 2. `root_causes.csv`

Ranked list of root causes:
- `root_cause_service`: Identified root cause
- `root_cause_score`: Overall root cause score
- `affected_target`: Service affected by this root cause
- `latency_increase_pct`: Latency increase percentage
- `is_root_service`: Whether service has no upstream dependencies
- `rank`: Ranking (1 = most likely root cause)

### 3. `latency_baseline_stats.csv`

Baseline latency statistics for each service:
- `mean`: Average latency
- `std`: Standard deviation
- `p95`: 95th percentile
- `p99`: 99th percentile

### 4. `rca_summary_report.txt`

Human-readable summary of analysis results.

## Example Scenario

### Input
- **Alert**: `HighLatency` on `api-gateway` service
- **Service Graph**: `api-gateway → auth-service → user-db`

### Analysis
1. Identify `api-gateway` has elevated latency
2. Attribute to upstream services: `auth-service`, `user-db`
3. Calculate attribution scores:
   - `user-db`: 0.85 (high score)
   - `auth-service`: 0.45 (medium score)

### Output
**Root Cause**: `user-db` (Score: 92.5)
- High attribution score (0.85)
- Significant latency increase (+150%)
- Root service (no upstream dependencies)

## Integration with Alert Consolidation

The pipeline automatically integrates with the alert consolidation solution:

1. **Alert Consolidation** groups alerts by service relationships
2. **RCA Analysis** uses consolidated alerts to identify latency issues
3. **Root Causes** are linked back to alert clusters

## Advanced Usage

### Custom Target Service

```python
from causal_rca_analyzer import CausalRCAAnalyzer

analyzer = CausalRCAAnalyzer(service_graph=graph)
analyzer.build_causal_graph_from_service_graph()
normal_data, anomalous_data = analyzer.prepare_latency_data(alerts)

# Analyze specific service
attributions = analyzer.attribute_latency_anomalies(
    anomalous_data, 
    target_service="api-gateway"
)
```

### Custom Attribution Scoring

Modify `attribute_latency_anomalies()` to use different anomaly scorers:

```python
from dowhy.gcm import ShapleyConfig

attributions = attribute_anomalies(
    gcm_model,
    anomalous_data,
    target_anomalies=target_service,
    anomaly_scorer=lambda x, y: np.abs(x - y) / y,  # Relative difference
    shapley_config=ShapleyConfig(n_jobs=4)  # Parallel processing
)
```
## References

- [DoWhy Documentation](https://www.pywhy.org/dowhy/)
- [GCM RCA Tutorial](https://www.pywhy.org/dowhy/main/example_notebooks/gcm_rca_microservice_architecture.html)
- [Shapley Values for Attribution](https://www.pywhy.org/dowhy/gcm/main/user_guide/attribution.html)

## Next Steps
answer those questions:

Scope
Feature Defination
Risks & Watchouts
Dependencies:
   Platform
   DevOps
Business benefits

