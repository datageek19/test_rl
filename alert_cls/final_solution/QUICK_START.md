# Quick Start Guide - Alert Consolidation Pipeline

## Get Started in 5 Minutes

### Step 1: Install Dependencies

```bash
pip install pandas numpy networkx scikit-learn matplotlib seaborn scipy
```

### Step 2: Prepare input Data

**Alerts CSV** (`alert_data.csv`):
```csv
alert_id, status, labels, annotations, startsAt
1, firing, {'service_name': 'frontend', 'namespace': 'prod', 'alert_category': 'saturation'}, {'description': 'High CPU usage'}, 2024-01-01T10:00:00Z
2, firing, {'service_name': 'api', 'namespace': 'prod', 'alert_category': 'error'}, {'description': 'Connection timeout'}, 2024-01-01T10:05:00Z
```

**Service Graph JSON** (`graph_data.json`):
```json
[
  {
    "source_name": "frontend",
    "target_name": "api",
    "relationship_type": "CALLS",
    "source_properties": {"name": "frontend", "namespace": "prod"},
    "target_properties": {"name": "api", "namespace": "prod"}
  }
]
```

### Step 3: Run the Pipeline

```python
from alert_consolidation_complete import ComprehensiveAlertConsolidator

# Initialize
consolidator = ComprehensiveAlertConsolidator(
    alerts_csv_path='alert_data.csv',
    graph_json_path='graph_data.json',
    output_dir='results'
)

# Run
consolidator.run_consolidation()
```

### Step 4: View Results

```bash
# Check the main output
cat results/alert_consolidation_final.csv

# View ranked clusters
cat results/ranked_clusters.csv

# Open the visualization
open results/cluster_visualization.png
```

---

## Understanding the Output

### Main Files

1. **`alert_consolidation_final.csv`**: All consolidated alerts
   - Each alert has a `cluster_id`, `cluster_name`, `cluster_rank`, and `cluster_score`
   - Use `cluster_score` to prioritize (higher = more important)

2. **`ranked_clusters.csv`**: Top clusters by importance
   - Look at `rank` = 1 for the most critical issue
   - Check `alert_count` for volume

3. **`cluster_visualization.png`**: Dashboard
   - Top 10 critical clusters
   - Severity distribution
   - Key metrics

### Key Columns

```
cluster_id:       Unique cluster identifier
cluster_name:     Descriptive name (e.g., "cpu_usage_frontend_saturation_high")
cluster_rank:     Priority rank (1 = most critical)
cluster_score:    Composite importance score
is_duplicate:     Whether this alert is a duplicate
```

---

## Common Use Cases

### Use Case 1: Find Most Critical Issues

```python
import pandas as pd

df = pd.read_csv('results/ranked_clusters.csv')
top_5 = df.head(5)

print("Top 5 Critical Issues:")
for idx, row in top_5.iterrows():
    print(f"{row['rank']}. {row['cluster_name']}")
    print(f"   {row['alert_count']} alerts, Score: {row['ranking_score']:.1f}")
```

**Output**:
```
1. cpu_usage_frontend_saturation_high
   87 alerts, Score: 85.3
2. memory_leak_api_error_high
   45 alerts, Score: 78.2
...
```

### Use Case 2: Get All Alerts in a Cluster

```python
import pandas as pd

df = pd.read_csv('results/alert_consolidation_final.csv')

# Find cluster ID for a specific cluster
critical_cluster = df[df['cluster_rank'] == 1]

# Get all alerts in this cluster
alerts_in_cluster = df[df['cluster_id'] == critical_cluster['cluster_id'].iloc[0]]

print(f"Cluster: {critical_cluster['cluster_name'].iloc[0]}")
print(f"Alerts: {len(alerts_in_cluster)}")
```

### Use Case 3: Check Deduplication Impact

```python
import pandas as pd

df = pd.read_csv('results/alert_consolidation_final.csv')

total = len(df)
duplicates = df['is_duplicate'].sum()
unique = total - duplicates

print(f"Total alerts: {total}")
print(f"Unique alerts: {unique}")
print(f"Duplicates removed: {duplicates} ({duplicates/total*100:.1f}%)")
```

### Use Case 4: Service Impact Analysis

```python
import pandas as pd

df = pd.read_csv('results/alert_consolidation_final.csv')

# Count alerts by service
service_impact = df.groupby('graph_service').agg({
    'cluster_id': 'count',
    'severity': lambda x: (x == 'critical').sum()
}).sort_values('cluster_id', ascending=False)

print("Top 10 Affected Services:")
print(service_impact.head(10))
```

---

## Configuration Quick Reference

### Adjust Time Window for Duplicates

```python
# In alert_consolidation_complete.py, line 40
TIME_WINDOW_MINUTES = 10  # Default: 5
```

### Control Outlier Removal

```python
# Disable outlier removal
consolidator.outlier_removal_enabled = False

# Or adjust contamination rate (default: 0.05)
consolidator.OUTLIER_CONTAMINATION = 0.01
```

### Modify Clustering

```python
# Reduce minimum samples for clustering
consolidator.MIN_CLUSTERING_SAMPLES = 5  # Default: 10

# Adjust PCA variance threshold
consolidator.PCA_VARIANCE_THRESHOLD = 0.90  # Default: 0.95
```

---

## 📝 Example Script

Create `run_consolidation.py`:

```python
#!/usr/bin/env python
"""
Example: Run alert consolidation pipeline
"""

from alert_consolidation_complete import ComprehensiveAlertConsolidator
import pandas as pd
import os

def main():
    # Configuration
    alerts_csv = 'alert_data.csv'
    graph_json = 'graph_data.json'
    output_dir = 'results'
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize
    print("Initializing Alert Consolidation Pipeline...")
    consolidator = ComprehensiveAlertConsolidator(
        alerts_csv_path=alerts_csv,
        graph_json_path=graph_json,
        output_dir=output_dir
    )
    
    # Run pipeline
    print("\nRunning consolidation...")
    results = consolidator.run_consolidation()
    
    # Display summary
    df = pd.read_csv(f'{output_dir}/ranked_clusters.csv')
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total clusters: {len(df)}")
    print(f"\nTop 5 Clusters:")
    for idx, row in df.head(5).iterrows():
        print(f"\n{row['rank']}. {row['cluster_name']}")
        print(f"   Alerts: {row['alert_count']}, Score: {row['ranking_score']:.1f}")
    
    print("\n✓ Pipeline complete!")
    print(f"✓ Results in: {output_dir}/")

if __name__ == '__main__':
    main()
```

**Run it**:
```bash
python run_consolidation.py
```

---

## Next Steps

- **Read**: `README.md` for detailed documentation
- **Explore**: `ARCHITECTURE.md` for system design
- **Customize**: Modify configuration in the pipeline
- **Visualize**: Run `python visualize_clusters.py`


