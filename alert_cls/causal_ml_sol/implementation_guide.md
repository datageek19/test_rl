# End-to-End Implementation Guide: Causal RCA for Microservices

## Overview

This guide provides step-by-step instructions for implementing causal inference-based root cause analysis (RCA) for elevated latencies in your microservices architecture.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Alert Consolidation                       │
│  - Groups alerts by service relationships                   │
│  - Maps alerts to service graph                             │
│  - Clusters related alerts                                  │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Latency Data Collection & Prep                  │
│  - Collect normal baseline latencies                        │
│  - Collect anomalous latencies (during alerts)             │
│  - Align data with service graph                            │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Causal Model Construction                       │
│  - Build causal graph from service dependencies             │
│  - Model latency relationships                              │
│  - Fit GCM to normal operation data                         │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Anomaly Attribution                            │
│  - Attribute elevated latency to upstream services         │
│  - Calculate Shapley-based attribution scores             │
│  - Identify contributing services                           │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Root Cause Identification                       │
│  - Rank services by root cause score                        │
│  - Consider topology (root vs intermediate services)      │
│  - Generate actionable root cause list                      │
└─────────────────────────────────────────────────────────────┘
```

## Step-by-Step Implementation

### Step 1: Install Dependencies

```bash
cd causal_ml
pip install -r requirements.txt
```

Or install manually:
```bash
pip install dowhy[gcm] networkx pandas numpy scipy scikit-learn
```

### Step 2: Prepare Your Data

#### 2.1 Alert Data
Ensure you have `alert_data.csv` with:
- Service names
- Alert timestamps
- Alert types (especially latency-related)
- Status (firing/resolved)

#### 2.2 Service Graph
Ensure you have `graph_data.json` with service relationships:
```json
[
  {
    "source_name": "service-a",
    "target_name": "service-b",
    "relationship_type": "CALLS",
    "source_properties": {...},
    "target_properties": {...}
  }
]
```

### Step 3: Run Basic Analysis

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

### Step 4: Integrate with Real Latency Data

#### 4.1 Format Latency Data

```python
import pandas as pd

# Convert to required format
latency_df = pd.DataFrame({
    'timestamp': timestamps,
    'service_name': service_names,
    'latency_seconds': latency_values,
    'is_anomalous': is_anomalous_flags
})

latency_df.to_csv('latency_measurements.csv', index=False)
```

#### 4.2 Run with alert Data

```python
pipeline = RCAPipeline(
    alerts_csv_path='alert_data.csv',
    graph_json_path='graph_data.json',
    latency_data_path='latency_measurements.csv',
    output_dir='./rca_results',
    use_synthetic_latency=False
)

results = pipeline.run_complete_pipeline()
```

### Step 5: Interpret Results

#### 5.1 Root Causes (`root_causes.csv`)

```python
import pandas as pd

root_causes = pd.read_csv('./rca_results/root_causes.csv')
print(root_causes.head(10))
```

**Key columns:**
- `root_cause_service`: Service identified as root cause
- `root_cause_score`: Higher = more likely root cause
- `latency_increase_pct`: How much latency increased
- `is_root_service`: True if service has no upstream dependencies

#### 5.2 Attributions (`latency_attributions.csv`)

```python
attributions = pd.read_csv('./rca_results/latency_attributions.csv')
print(attributions.sort_values('attribution_score', ascending=False).head(10))
```

**Key columns:**
- `target_service`: Service with elevated latency
- `upstream_service`: Potential contributing service
- `attribution_score`: Contribution score (0-1, higher = more contribution)

### Step 6: Action on Results

1. **Investigate Top Root Causes**
   - Check top 3-5 services from `root_causes.csv`
   - Review logs, metrics, and recent deployments

2. **Verify Attribution**
   - Check if upstream services show latency increases
   - Validate causal relationships in service graph

3. **Prioritize Fixes**
   - Root services (no upstream) are highest priority
   - Services with high attribution scores need attention

## Integration with Monitoring Systems

### Prometheus Integration

```python
from prometheus_client import PrometheusConnect
import pandas as pd
from datetime import datetime, timedelta

def collect_latency_from_prometheus(service_names, start_time, end_time):
    """Collect latency metrics from Prometheus"""
    prom = PrometheusConnect(url="http://prometheus:9090")
    
    latency_data = []
    
    for service in service_names:
        # Query p95 latency
        query = f'''
        histogram_quantile(0.95, 
            rate(http_request_duration_seconds_bucket{{service="{service}"}}[5m])
        )
        '''
        
        results = prom.custom_query(query=query)
        
        for result in results:
            latency_data.append({
                'timestamp': datetime.fromtimestamp(result['value'][0]),
                'service_name': service,
                'latency_seconds': result['value'][1],
                'is_anomalous': False  # Determine based on alerts
            })
    
    return pd.DataFrame(latency_data)
```

### Datadog Integration

```python
from datadog import initialize, api

def collect_latency_from_datadog(service_names, start_time, end_time):
    """Collect latency metrics from Datadog"""
    options = {
        'api_key': 'YOUR_API_KEY',
        'app_key': 'YOUR_APP_KEY'
    }
    initialize(**options)
    
    latency_data = []
    
    for service in service_names:
        query = f'avg:trace.http.request.duration{{service:{service}}}'
        
        results = api.Metric.query(
            start=int(start_time.timestamp()),
            end=int(end_time.timestamp()),
            query=query
        )
        
        # Process results...
    
    return pd.DataFrame(latency_data)
```

## Advanced Configuration

### Custom Attribution Method

```python
from dowhy.gcm import attribute_anomalies, ShapleyConfig
import numpy as np

# Custom anomaly scorer (relative difference)
def relative_anomaly_scorer(actual, expected):
    return np.abs(actual - expected) / (expected + 1e-6)

# Use in attribution
attributions = attribute_anomalies(
    gcm_model,
    anomalous_data,
    target_anomalies=target_service,
    anomaly_scorer=relative_anomaly_scorer,
    shapley_config=ShapleyConfig(n_jobs=4)  # Parallel processing
)
```

### Custom Root Cause Scoring

Modify `identify_root_causes()` in `causal_rca_analyzer.py`:

```python
# Adjust weights for root cause score
root_cause_score = (
    attribution_score * 0.6 +  # Increase attribution weight
    min(latency_increase / 100, 1.0) * 100 * 0.2 +  # Decrease latency weight
    (100 if is_root_service else 0) * 0.2  # Keep topology bonus
)
```

## Automation

### Scheduled RCA Runs

```python
import schedule
import time

def run_daily_rca():
    """Run RCA analysis daily"""
    pipeline = RCAPipeline(
        alerts_csv_path='alert_data.csv',
        graph_json_path='graph_data.json'
        output_dir=f'./rca_results/{datetime.now().strftime("%Y%m%d")}'
    )
    pipeline.run_complete_pipeline()

# Schedule daily at 2 AM
schedule.every().day.at("02:00").do(run_daily_rca)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### Alert-Triggered RCA

```python
def on_alert_fired(alert):
    """Trigger RCA when high-severity latency alert fires"""
    if alert['severity'] == 'critical' and 'latency' in alert['alert_name'].lower():
        pipeline = RCAPipeline(
            alerts_csv_path='alert_data.csv',
            graph_json_path='graph_data.json',
            output_dir=f'./rca_results/alert_{alert["id"]}'
        )
        
        # Analyze specific service
        results = pipeline.run_complete_pipeline(
            target_service=alert['service_name']
        )
        
        # Send notification with root causes
        send_notification(results['root_causes'].head(3))
```

### Performance Optimization

1. **Reduce service graph size**: Filter to relevant services
2. **Sample data**: Use representative samples for large datasets
3. **Parallel processing**: Use `ShapleyConfig(n_jobs=4)`

## Next Steps

1. **Collect Real Latency Data**: Integrate with your monitoring system
2. **Tune Parameters**: Adjust thresholds and scoring weights
3. **Add Visualizations**: Create graphs showing causal relationships
4. **Automate**: Schedule regular RCA runs
5. **Integrate with Alerting**: Trigger RCA on critical alerts

## References

- [DoWhy GCM Documentation](https://www.pywhy.org/dowhy/gcm/)
- [RCA Tutorial](https://www.pywhy.org/dowhy/main/example_notebooks/gcm_rca_microservice_architecture.html)
- [Shapley Values](https://www.pywhy.org/dowhy/gcm/main/user_guide/attribution.html)

