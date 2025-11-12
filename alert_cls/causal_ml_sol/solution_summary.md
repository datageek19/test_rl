# Causal Inference Root Cause Analysis - Solution Summary

## Solution Overview

This implementation provides an **end-to-end causal inference-based root cause analysis (RCA)** system for identifying root causes of elevated latencies in microservices architectures.

### Key Features

**Causal Model Construction**: Builds causal graphs from service dependency relationships  
**Latency Attribution**: Attributes elevated latencies to upstream services using Shapley values  
**Root Cause Identification**: Ranks services by root cause likelihood  
**Alert Integration**: Seamlessly integrates with existing alert consolidation pipeline  
**Synthetic Data Support**: Can generate synthetic latency data for testing  

## Files Created

### Core Implementation

1. **`causal_rca_analyzer.py`** (Main RCA Engine)
   - `CausalRCAAnalyzer` class
   - Causal graph construction from service dependencies
   - GCM model fitting and anomaly attribution
   - Root cause identification and ranking

2. **`rca_pipeline.py`** (End-to-End Pipeline)
   - `RCAPipeline` class
   - Integrates alert consolidation with causal RCA
   - Complete pipeline orchestration
   - Command-line interface

3. **`example_usage.py`** (Usage Examples)
   - Basic usage examples
   - Step-by-step pipeline examples
   - Integration examples

### Documentation

4. **`README_RCA.md`** - Complete user guide
5. **`IMPLEMENTATION_GUIDE.md`** - Step-by-step implementation guide
6. **`requirements.txt`** - Python dependencies

## Quick Start

### 1. Install Dependencies

```bash
cd causal_ml
pip install -r requirements.txt
```

### 2. Run Basic Analysis

```python
from rca_pipeline import RCAPipeline

pipeline = RCAPipeline(
    alerts_csv_path='alert_data.csv',
    graph_json_path='graph_data.json',
    output_dir='./rca_results',
    use_synthetic_latency=True  # For testing
)

results = pipeline.run_complete_pipeline()
```

### 3. Command Line

```bash
python rca_pipeline.py \
    --alerts alert_data.csv \
    --graph graph_data.json \
    --output ./rca_results \
    --synthetic
```

## Pipeline Flow

```
1. Alert Consolidation
   ↓
2. Latency Data Preparation
   ↓
3. Causal Model Construction
   ↓
4. Anomaly Attribution
   ↓
5. Root Cause Identification
   ↓
6. Results Export
```

## Output Files

### 1. `root_causes.csv`
Ranked list of root causes with:
- Root cause service name
- Root cause score (higher = more likely)
- Latency increase percentage
- Topology information

### 2. `latency_attributions.csv`
Detailed attribution scores:
- Target service (with elevated latency)
- Upstream service (potential cause)
- Attribution score (contribution level)
- Latency statistics

### 3. `latency_baseline_stats.csv`
Baseline latency statistics:
- Mean, std, p95, p99 for each service

### 4. `rca_summary_report.txt`
Human-readable summary report

## How It Works

### Causal Model

The system models latency relationships as:
```
Observed_Latency(Service) = Intrinsic_Latency(Service) + 
                            Sum(Latency of downstream services)
```

### Attribution Method

Uses **Shapley-based attribution** from DoWhy GCM:
- For each target service with elevated latency
- Calculates contribution of each upstream service
- Scores indicate how much each service contributes

### Root Cause Scoring

Root causes are scored by:
1. **Attribution Score** (50%): How much service contributes
2. **Latency Increase** (30%): How much latency increased
3. **Topology** (20%): Bonus for root services (no upstream)

## Integration Points

### With Alert Consolidation

The pipeline automatically:
1. Uses consolidated alerts from `ComprehensiveAlertConsolidator`
2. Maps alerts to service graph
3. Identifies latency-related alerts
4. Links root causes back to alert clusters

### With Monitoring Systems

Can integrate with:
- **Prometheus**: Query latency metrics
- **Datadog**: Collect trace data
- **Custom APIs**: Any latency measurement source

## Example Scenario

### Input
- **Alert**: `HighLatency` on `api-gateway`
- **Service Graph**: `api-gateway → auth-service → user-db`

### Analysis
1. Identifies `api-gateway` has elevated latency
2. Attributes to upstream: `auth-service`, `user-db`
3. Calculates scores:
   - `user-db`: 0.85 (high)
   - `auth-service`: 0.45 (medium)

### Output
**Root Cause**: `user-db` (Score: 92.5)
- High attribution (0.85)
- Significant latency increase (+150%)
- Root service (no upstream)


### Target Service Analysis

```python
# Analyze specific service
pipeline.run_complete_pipeline(target_service='api-gateway')
```

### Custom Attribution

Modify `attribute_latency_anomalies()` to use different:
- Anomaly scorers
- Shapley configurations
- Attribution methods

## Key Methods

### `CausalRCAAnalyzer`

- `build_causal_graph_from_service_graph()`: Build causal model
- `prepare_latency_data()`: Load and prepare latency data
- `fit_causal_model()`: Fit GCM to normal data
- `attribute_latency_anomalies()`: Attribute anomalies
- `identify_root_causes()`: Identify and rank root causes

### `RCAPipeline`

- `run_alert_consolidation()`: Step 1: Consolidate alerts
- `prepare_for_rca()`: Step 2: Prepare data
- `run_causal_rca()`: Step 3: Run RCA analysis
- `run_complete_pipeline()`: Run all steps

## Data Requirements

### Alert Data
- Service names
- Timestamps
- Alert types
- Status (firing/resolved)

### Service Graph
- Service relationships (CALLS, OWNS, BELONGS_TO)
- Service properties

### Latency Data (Optional)
- Timestamp
- Service name
- Latency in seconds
- Anomaly flag (optional)


## Next Steps

1. **Collect Real Latency Data**
   - Integrate with Prometheus/Datadog
   - Set up regular data collection

2. **Tune Parameters**
   - Adjust anomaly thresholds
   - Customize root cause scoring weights

3. **Add Visualizations**
   - Causal graph visualization
   - Attribution score charts
   - Root cause timeline

4. **Automate**
   - Schedule daily RCA runs
   - Trigger on critical alerts
   - Send notifications

5. **Validate Results**
   - Compare with manual investigations
   - Track accuracy over time
   - Refine models

## References

- [DoWhy Documentation](https://www.pywhy.org/dowhy/)
- [GCM RCA Tutorial](https://www.pywhy.org/dowhy/main/example_notebooks/gcm_rca_microservice_architecture.html)
- [Shapley Attribution](https://www.pywhy.org/dowhy/gcm/main/user_guide/attribution.html)
