# Alert Consolidation Pipeline

## Overview

An intelligent alert consolidation and clustering pipeline that transforms raw alerts into actionable insights by leveraging service dependency graphs and machine learning clustering algorithms.

**Key Features:**
- **Service Graph Integration**: Maps alerts to service dependencies
- **Machine Learning Clustering**: Automatic grouping using K-Means, DBSCAN, and Hierarchical clustering
- **Intelligent Deduplication**: Removes duplicate alerts based on temporal and dependency patterns
- **Ranking & Scoring**: Prioritizes clusters by importance and repetitiveness
- **Rich Visualizations**: Intuitive dashboards for end-users
- **Incremental Processing**: Supports batch updates with historical data

---

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Pipeline Overview](#pipeline-overview)
- [Installation](#installation)
- [Usage Guide](#usage-guide)
- [Configuration](#configuration)
- [Output Files](#output-files)
- [Visualization Guide](#visualization-guide)
- [Key Concepts](#key-concepts)
- [Troubleshooting](#troubleshooting)
- [API Reference](#api-reference)

---

## Quick Start

### Basic Usage

```python
from alert_consolidation_complete import ComprehensiveAlertConsolidator

# Initialize the consolidator
consolidator = ComprehensiveAlertConsolidator(
    alerts_csv_path='alert_data.csv',
    graph_json_path='graph_data.json',
    output_dir='./output'
)

# Run the complete pipeline
consolidator.run_consolidation()
```

### Command Line

```bash
# Run the main pipeline
python alert_consolidation_complete.py

# Generate visualizations
python visualize_clusters.py
```

---

## Architecture

### Pipeline Phases

```
┌─────────────────────────────────────────────────────────────────┐
│                  ALERT CONSOLIDATION PIPELINE                     │
└─────────────────────────────────────────────────────────────────┘

1. DATA LOADING & PREPROCESSING
   ├─ Load firing alerts from CSV
   ├─ Parse alert metadata (labels, annotations)
   ├─ Extract temporal information
   └─ Load service graph relationships (JSON)

2. ALERT-TO-GRAPH MAPPING
   ├─ Direct mapping: service_name match
   ├─ Fallback mapping: namespace + cluster + node
   └─ Track mapping confidence and method

3. INITIAL GROUPING BY RELATIONSHIPS
   ├─ Group alerts by service dependencies
   ├─ Find related services (parent sinks, traversal paths)
   └─ Create consolidated groups

4. FEATURE ENGINEERING
   ├─ Graph topology features (20 features)
   ├─ Alert metadata features (19 features)
   ├─ Outlier detection (Isolation Forest)
   └─ PCA for dimensionality reduction

5. CLUSTERING
   ├─ K-Means (optimal k selection)
   ├─ DBSCAN (density-based)
   ├─ Hierarchical Clustering
   └─ Select best method by silhouette score

6. DEDUPLICATION
   ├─ Temporal proximity check
   ├─ Dependency-based duplicate detection
   └─ Mark representative alerts

7. RANKING & NAMING
   ├─ Calculate cluster scores
   ├─ Generate distinctive names
   └─ Assign ranks

8. EXPORT & VISUALIZATION
   ├─ Export consolidated results
   ├─ Generate cluster summaries
   └─ Create visualizations
```

### Key Components

- **ComprehensiveAlertConsolidator**: Main pipeline orchestrator
- **Service Graph**: NetworkX DiGraph for dependency modeling
- **Feature Engineering**: 39-dimensional feature space
- **Clustering Algorithms**: Multiple algorithms with automatic selection
- **Visualization**: Simple, intuitive dashboards

---

## Installation

### Requirements

```bash
pip install pandas numpy networkx scikit-learn matplotlib seaborn scipy
```

### Required Packages

```
pandas >= 1.3.0
numpy >= 1.21.0
networkx >= 2.6
scikit-learn >= 1.0.0
matplotlib >= 3.3.0
seaborn >= 0.11.0
scipy >= 1.7.0
```

---

## Usage Guide

### 1. Prepare Input Data

Input alert data (saafe alert, user alert) and service graph data are presumebly being read from data staging area. 

### 2. Run the Pipeline

```python
# Initialize
consolidator = ComprehensiveAlertConsolidator(
    alerts_csv_path='alert_data.csv',
    graph_json_path='graph_data.json',
    output_dir='./results'
)

# Execute
results = consolidator.run_consolidation()
```

### 3. View Results

```python
# Load consolidated alerts
df = pd.read_csv('results/alert_consolidation_final.csv')

# Load ranked clusters
ranked = pd.read_csv('results/ranked_clusters.csv')

# View top critical clusters
print(ranked.head(10))
```

---

## ⚙️ Configuration

### Configuration Constants

```python
TIME_WINDOW_MINUTES = 5           # Time window for duplicate detection
MIN_MATCH_SCORE = 2               # Minimum score for fallback mapping
DUPLICATE_THRESHOLD = 5           # Minimum score to consider duplicates
MIN_CLUSTERING_SAMPLES = 10       # Minimum alerts needed for clustering
PCA_VARIANCE_THRESHOLD = 0.95    # Retain 95% variance
OUTLIER_CONTAMINATION = 0.05     # Expect 5% outliers
```

### Customization

Modify these values in `alert_consolidation_complete.py`:

```python
class ComprehensiveAlertConsolidator:
    TIME_WINDOW_MINUTES = 10      # Increase for broader time window
    MIN_MATCH_SCORE = 3           # Stricter fallback mapping
    # ... other constants
```

---

## Output Files

### Main Outputs

1. **`alert_consolidation_final.csv`**
   - All consolidated alerts with cluster assignments
   - Includes cluster name, rank, and score
   - Key columns: `cluster_id`, `cluster_name`, `cluster_rank`, `cluster_score`

2. **`ranked_clusters.csv`**
   - Summary of all clusters ranked by importance
   - Key columns: `rank`, `cluster_name`, `ranking_score`, `alert_count`

3. **`alerts_by_cluster_detailed.csv`**
   - Detailed view of alerts by cluster
   - Includes service graph information
   - Sortable by cluster rank

### Supporting Outputs

- `cluster_summary.csv`: Statistics for each cluster
- `deduplicated_alerts.csv`: Unique alerts only
- `mapping_statistics.csv`: Mapping method distribution
- `clustering_statistics.csv`: Clustering algorithm details
- `cluster_visualization.png`: Dashboard visualization
- `cluster_detailed_summary.csv`: Summary table

---

## Visualization Guide

### Generate Visualizations

```bash
python visualize_clusters.py
```

### Dashboard Components

The visualization dashboard (`cluster_visualization.png`) contains 4 key panels:

1. **Top 10 Critical Clusters**: Bar chart showing most important clusters
2. **Severity Distribution**: Pie chart of alert severities
3. **Top Services Affected**: Services with most alerts
4. **Key Metrics**: Summary statistics and consolidation impact

### Understanding Clusters

**Cluster Names**: Auto-generated descriptive names
- Format: `{alert_type}_{service}_{category}_{severity}`
- Example: `cpu_usage_frontend_saturation_high`

**Cluster Scores**: Composite ranking based on:
- Repetitiveness (40%): How often alerts repeat
- Severity impact (25%): Average severity weight
- Cluster size (20%): Number of alerts
- Service importance (15%): PageRank centrality
- Time concentration (10%): How clustered in time

---

## Key Concepts

### Alert Mapping

**Direct Mapping** (Confidence: 1.0)
- Exact match on `service_name`
- Uses graph service directly

**Fallback Mapping** (Confidence: 0.2-1.0)
- Matches by namespace + cluster
- Score based on attribute overlap
- Lower confidence but still usable

### Deduplication Strategy

Alerts are considered duplicates if they:
1. Share the same service (100% match)
2. Share common upstream dependencies (parent services)
3. Share common downstream dependencies (child services)
4. Eventually sink to the same services (transitive dependencies)
5. Occur within the time window (default: 5 minutes)

### Clustering

**Multiple Algorithms**:
- **K-Means**: Centroid-based, good for spherical clusters
- **DBSCAN**: Density-based, handles noise
- **Hierarchical**: Agglomerative, preserves hierarchy

**Best Algorithm Selection**:
- Selected based on silhouette score
- Ensures optimal separation and cohesion

### Outlier Detection

- Uses **Isolation Forest** to identify anomalous alerts
- Removes 5% most extreme outliers
- Preserves normal alert patterns
- Improves clustering quality

---

## Example Workflow for Alert Data Consolidation

### Complete Example

```python
from alert_consolidation_complete import ComprehensiveAlertConsolidator
import pandas as pd

# Initialize
consolidator = ComprehensiveAlertConsolidator(
    alerts_csv_path='data/alerts.csv',
    graph_json_path='data/graph.json',
    output_dir='results/'
)

#  Run pipeline
print("Running consolidation pipeline...")
results = consolidator.run_consolidation()

# Analyze results
df = pd.read_csv('results/alert_consolidation_final.csv')
ranked = pd.read_csv('results/ranked_clusters.csv')

# Review top clusters
print("\nTop 5 Clusters:")
for idx, row in ranked.head(5).iterrows():
    print(f"{row['rank']}. {row['cluster_name']}")
    print(f"   Alerts: {row['alert_count']}, Score: {row['ranking_score']:.1f}")

# Generate visualizations
import visualize_clusters
visualize_clusters.load_and_visualize_clusters()
```

### Output Analysis

```python
# Load results
df = pd.read_csv('results/alert_consolidation_final.csv')

# Find critical clusters
critical_clusters = df[df['severity'] == 'critical'].groupby('cluster_name').size()
print("Critical alert clusters:", critical_clusters)

# Service impact analysis
service_impact = df.groupby('graph_service')['severity'].value_counts()
print("\nService Impact:")
print(service_impact.head(20))

# Deduplication effectiveness
total_alerts = len(df)
unique_alerts = len(df[~df['is_duplicate']])
duplicates_removed = total_alerts - unique_alerts
print(f"\nDeduplication: Removed {duplicates_removed} duplicates ({duplicates_removed/total_alerts*100:.1f}%)")
```

### Performance Optimization

**Large Datasets** (10K+ alerts):
```python
# Disable outlier removal for faster processing
consolidator.outlier_removal_enabled = False

# Reduce clustering iterations
consolidator._find_optimal_k(max_k=10)  # Limit k search
```

**Memory Issues**:
```python
# Process in batches
consolidator.PCA_VARIANCE_THRESHOLD = 0.90  # Fewer components
```

---

## API Reference

### Main Class

#### `ComprehensiveAlertConsolidator`

**Methods**:

- `run_consolidation()`: Execute complete pipeline
- `_load_firing_alerts()`: Load and parse alerts
- `_load_graph_data()`: Load service graph
- `_enrich_alert_with_graph_info()`: Map alerts to graph
- `_engineer_features()`: Extract features
- `_apply_clustering()`: Run clustering algorithms
- `_deduplicate_alerts()`: Remove duplicates
- `_rank_and_name_clusters()`: Rank and name clusters
- `_export_results()`: Export all results

**Attributes**:

- `enriched_alerts`: List of enriched alerts
- `consolidated_groups`: Initial consolidation groups
- `service_graph`: NetworkX DiGraph
- `clustering_results`: Clustering method results
- `deduplicated_alerts`: Unique alerts after dedup

### Configuration Options

```python
class ComprehensiveAlertConsolidator:
    # Time-based settings
    TIME_WINDOW_MINUTES = 5
    
    # Mapping settings
    MIN_MATCH_SCORE = 2
    
    # Deduplication settings
    DUPLICATE_THRESHOLD = 5
    DESCRIPTION_SIMILARITY_THRESHOLD = 0.7
    
    # Clustering settings
    MIN_CLUSTERING_SAMPLES = 10
    PCA_VARIANCE_THRESHOLD = 0.95
    OUTLIER_CONTAMINATION = 0.05
```

---

## Best Practices

### 1. Data Quality

- Ensure service names are consistent across alerts and graph
- Use meaningful namespaces and clusters
- Include accurate timestamps
- Provide complete metadata in labels

### 2. Service Graph

- Keep service graph up-to-date
- Include all relationships (CALLS, OWNS, BELONGS_TO)
- Ensure service names match alert service names

### 3. Monitoring

- Track mapping confidence scores
- Monitor deduplication rates
- Review outlier removal impact
- Check cluster quality metrics

### 4. Incremental Processing

- Save consolidated state after each run
- Use timestamp-based filtering for new data
- Merge with historical data carefully
- Validate cluster continuity

### 5. Visualization

- Review visualizations after each run
- Focus on top-ranked clusters
- Investigate high-scoring clusters
- Monitor severity distributions

---


