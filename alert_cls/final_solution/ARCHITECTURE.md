# Alert Consolidation Pipeline - Architecture Documentation

## System Architecture

### Overview

The Alert Consolidation Pipeline is a multi-stage data processing system that transforms raw alert streams into actionable, prioritized clusters using service dependency graphs and machine learning algorithms.

**Core Idea**: 
- Graph-based consolidation leveraging service dependencies
- Multi-algorithm clustering with automatic selection
- Intelligent deduplication based on temporal and spatial patterns
- Ranking and scoring for actionable insights

---

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         INPUT LAYER                                   │
│  ┌──────────────┐              ┌──────────────┐                     │
│  │  Alert CSV   │              │  Graph JSON  │                     │
│  │   (CSV)      │              │   (JSON)     │                     │
│  └──────┬───────┘              └──────┬───────┘                     │
└─────────┼──────────────────────────────┼────────────────────────────┘
          │                              │
          ▼                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PROCESSING PIPELINE                               │
│                                                                       │
│  Stage 1: Loading & Preprocessing                                    │
│  ├─ Parse alert CSV (pivot structure)                                │
│  ├─ Extract metadata from labels/annotations                         │
│  ├─ Parse temporal information                                       │
│  └─ Build NetworkX service graph                                     │
│                                                                       │
│  Stage 2: Alert-to-Graph Mapping                                     │
│  ├─ Direct service_name mapping                                      │
│  ├─ Fallback: namespace + cluster matching                           │
│  ├─ Compute mapping confidence                                       │
│  └─ Annotate mapping method                                          │
│                                                                       │
│  Stage 3: Initial Grouping by Relationships                          │
│  ├─ Group by graph service                                           │
│  ├─ Find related services (neighbors, transitive)                   │
│  ├─ Create consolidated groups                                       │
│  └─ Handle unmapped alerts                                           │
│                                                                       │
│  Stage 4: Feature Engineering                                        │
│  ├─ Graph topology features (20 dims)                               │
│  ├─ Alert metadata features (19 dims)                               │
│  ├─ Outlier detection (Isolation Forest)                            │
│  └─ PCA for dimensionality reduction                                 │
│                                                                       │
│  Stage 5: Clustering                                                 │
│  ├─ K-Means (optimal k via silhouette)                              │
│  ├─ DBSCAN (density-based, noise handling)                           │
│  ├─ Hierarchical (ward linkage)                                      │
│  └─ Best algorithm selection                                         │
│                                                                       │
│  Stage 6: Deduplication                                              │
│  ├─ Temporal proximity check (time window)                          │
│  ├─ Dependency-based similarity                                      │
│  ├─ Mark duplicates vs representatives                               │
│  └─ Create deduplicated alert set                                    │
│                                                                       │
│  Stage 7: Ranking & Naming                                           │
│  ├─ Calculate composite cluster scores                              │
│  ├─ Generate distinctive cluster names                                │
│  ├─ Sort by score and assign ranks                                   │
│  └─ Export ranked cluster metadata                                   │
│                                                                       │
│  Stage 8: Export & Visualization                                     │
│  ├─ Export consolidated results                                      │
│  ├─ Generate cluster summaries                                       │
│  ├─ Create detailed cluster views                                    │
│  └─ Generate visualizations                                          │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
          │                              │
          ▼                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         OUTPUT LAYER                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐         │
│  │   Final CSV  │    │  Rankings    │    │  Visuals     │         │
│  │   Files      │    │  & Summaries │    │  Dashboard   │         │
│  └──────────────┘    └──────────────┘    └──────────────┘         │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Breakdown

#### 1. Data Loading Component

**Responsibilities**:
- Parse pivoted CSV structure
- Extract metadata from labels and annotations
- Build temporal information
- Construct NetworkX service graph

**Key Methods**:
```python
_load_firing_alerts()     # Parse alert CSV
_parse_alert_metadata()   # Extract labels/annotations
_parse_temporal_info()    # Extract timestamps
_load_graph_data()        # Build service graph
```

#### 2. Mapping Component

**Responsibilities**:
- Map alerts to graph services
- Fallback matching strategies
- Compute mapping confidence
- Track mapping methods

**Mapping Strategies**:
1. **Direct**: service_name exact match (confidence: 1.0)
2. **Fallback**: namespace + cluster + node (confidence: 0.2-1.0)

**Key Methods**:
```python
_enrich_alert_with_graph_info()  # Main mapping logic
_get_service_dependencies()       # Get upstream/downstream
```

#### 3. Grouping Component

**Responsibilities**:
- Initial alert grouping by service
- Find related services via graph traversal
- Create consolidated groups
- Handle unmapped alerts

**Grouping Logic**:
- Group by graph service
- Find neighbors (1-hop)
- Find transitive parents (3-hop)
- Check path similarity (Jaccard > 0.3)

**Key Methods**:
```python
_group_alerts_by_relationships()      # Main grouping
_find_related_alert_services()       # Find related services
_get_transitive_parent_services()    # BFS for parents
_compute_path_similarity()          # Jaccard similarity
```

#### 4. Feature Engineering Component

**Responsibilities**:
- Extract 39-dimensional features
- Detect outliers
- Apply PCA for dimensionality reduction
- Scale features

**Feature Categories**:
- **Graph Topology (20)**:
  - Degree metrics (in/out/total)
  - Centrality measures (PageRank, Betweenness)
  - Clustering coefficient
  - Relationship type counts
  - Neighborhood features

- **Alert Metadata (19)**:
  - Severity encoding
  - Category/subcategory encoding
  - Workload type encoding
  - Temporal features (hour, day, business hours)
  - Pattern indicators (error, resource, network, anomaly)

**Key Methods**:
```python
_engineer_features()    # Extract all features
_encode_severity()       # Severity encoding
_encode_workload_type() # Workload encoding
_remove_outliers()      # Isolation Forest
_apply_pca()            # Dimensionality reduction
```

#### 5. Clustering Component

**Responsibilities**:
- Run multiple clustering algorithms
- Select best algorithm via silhouette score
- Handle edge cases (too few samples, all outliers)

**Algorithms Used**:
1. **K-Means**: Optimal k via silhouette score
2. **DBSCAN**: Adaptive eps estimation
3. **Hierarchical**: Ward linkage, optimal n

**Key Methods**:
```python
_apply_clustering()         # Run all algorithms
_find_optimal_k()           # Silhouette-based k selection
_estimate_dbscan_eps()       # K-nearest neighbors estimation
_select_best_clustering()    # Silhouette-based selection
```

#### 6. Deduplication Component

**Responsibilities**:
- Detect duplicate alerts within clusters
- Temporal proximity checking
- Dependency-based similarity
- Mark representative alerts

**Duplicate Detection Logic**:
1. Same service → duplicates
2. Share upstream dependencies → duplicates
3. Share downstream dependencies → duplicates
4. Transitive downstream overlap → duplicates
5. Within time window → check above

**Key Methods**:
```python
_deduplicate_alerts()              # Main deduplication
_are_duplicates()                  # Duplicate check logic
_get_transitive_downstream()       # BFS for downstream
```

#### 7. Ranking & Naming Component

**Responsibilities**:
- Calculate composite cluster scores
- Generate descriptive cluster names
- Assign ranks
- Export ranked metadata

**Scoring Formula**:
```
Total Score = 
  40% × Repetitiveness Score        # (1 - unique_ratio) × 100
  25% × Severity Impact              # weighted average
  20% × Cluster Size                 # min(size/20, 1.0) × 100
  15% × Service Importance           # average PageRank × 1000
  10% × Time Concentration           # 100 / (1 + hours)
```

**Naming Convention**:
```
Format: {alert_type}_{service}_{category}_{severity}
Example: cpu_usage_frontend_saturation_high
```

**Key Methods**:
```python
_rank_and_name_clusters()    # Main ranking logic
_generate_cluster_name()      # Name generation
_calculate_cluster_score()   # Score calculation
```

#### 8. Export & Visualization Component

**Responsibilities**:
- Export consolidated results
- Generate summaries
- Create detailed views
- Generate visualizations

**Key Methods**:
```python
_export_results()              # Main export
_export_group_summary()        # Cluster summaries
_export_deduplicated()         # Unique alerts
_export_cluster_detail_view() # Detailed views
_export_cluster_stats()        # Clustering stats
```

---

## 🔑 Key Design Decisions

### 1. Why NetworkX for Graph?

**Decision**: Use NetworkX DiGraph for service dependencies

**Rationale**:
- Native support for directed graphs
- Built-in algorithms (PageRank, betweenness)
- Efficient neighbor iteration
- Proven in production

**Trade-offs**:
- Memory usage for large graphs
- Some algorithms scale O(n²)

### 2. Why Multiple Clustering Algorithms?

**Decision**: Run K-Means, DBSCAN, and Hierarchical

**Rationale**:
- Different algorithms capture different patterns
- Automatic selection via silhouette score
- Handles various cluster shapes
- Provides fallback options

**Trade-offs**:
- Higher computational cost
- Requires selection criteria

### 3. Why Feature-Based Clustering?

**Decision**: Use 39-dimensional feature space

**Rationale**:
- Captures both topology and metadata
- Enables outlier detection
- Allows PCA for dimensionality reduction
- Separates concerns (graph vs. alert)

**Trade-offs**:
- Feature engineering complexity
- Curse of dimensionality

### 4. Why Time-Window Deduplication?

**Decision**: 5-minute time window for duplicates

**Rationale**:
- Alerts from same incident occur close in time
- Reduces false positives
- Configurable threshold
- Time-aware processing

**Trade-offs**:
- Misses delayed duplicates
- Requires accurate timestamps

### 5. Why Composite Scoring?

**Decision**: Multi-factor composite score for ranking

**Rationale**:
- Captures multiple dimensions of importance
- Actionable for operations teams
- Configurable weights
- Intuitive interpretation

**Trade-offs**:
- Arbitrary weight selection
- Requires tuning

---

## Data Flow

### Input Data Flow

```
Alerts CSV → Parse → Extract Metadata → Enrich with Graph → Group → Feature Engineer
     ↓
Graph JSON → Parse → Build NetworkX Graph → Compute Metrics → Cache Results
```

### Processing Data Flow

```
Enriched Alerts → Initial Grouping → Feature Extraction → Outlier Removal → PCA
     ↓
Clustering (K-Means | DBSCAN | Hierarchical) → Best Algorithm Selection
     ↓
Deduplication → Ranking & Naming → Export
```

### Output Data Flow

```
Consolidated Alerts → Export CSVs → Generate Summaries → Visualize
```

---

## Extensibility Points

### 1. Add New Clustering Algorithm

```python
def _apply_clustering(self):
    # Add new algorithm
    spectral = SpectralClustering(n_clusters=n)
    spectral_labels = spectral.fit_predict(self.feature_matrix_scaled)
    
    self.clustering_results['spectral'] = {
        'labels': spectral_labels,
        'n_clusters': n,
        'algorithm': 'spectral'
    }
    self._select_best_clustering()
```

### 2. Add New Feature

```python
def _engineer_features(self):
    # In feature extraction loop
    feature_dict['my_new_feature'] = self._compute_my_feature(alert)
    
    # Update feature count in print statement
    print(f"      Custom features: 20 features")
```

### 3. Add New Deduplication Rule

```python
def _are_duplicates(self, alert1, alert2):
    # Add new rule
    if alert1.get('new_field') == alert2.get('new_field'):
        return True
    
    # Existing logic...
    return self._are_duplicates_basic(alert1, alert2)
```

### 4. Customize Scoring

```python
def _calculate_cluster_score(self, cluster_alerts, cluster_id):
    # Modify weights or add new factor
    score += my_new_factor * 0.05  # 5% weight
    
    # Existing logic...
    return score
```

---

## Testing Strategy

### Unit Testing

```python
def test_alert_mapping():
    consolidator = ComprehensiveAlertConsolidator(...)
    consolidator._load_graph_data()
    
    alert = {'service_name': 'frontend', ...}
    result = consolidator._enrich_alert_with_graph_info(alert)
    
    assert result == True
    assert alert['graph_service'] == 'frontend'
    assert alert['mapping_confidence'] == 1.0

def test_deduplication():
    alert1 = {...}  # Same service, same time
    alert2 = {...}  # Different alert instance
    
    result = consolidator._are_duplicates(alert1, alert2)
    assert result == True
```

### Integration Testing

```python
def test_full_pipeline():
    consolidator = ComprehensiveAlertConsolidator(...)
    results = consolidator.run_consolidation()
    
    # Verify outputs exist
    assert os.path.exists('alert_consolidation_final.csv')
    assert os.path.exists('ranked_clusters.csv')
    
    # Verify data quality
    df = pd.read_csv('alert_consolidation_final.csv')
    assert len(df) > 0
    assert 'cluster_id' in df.columns
```

### Performance Testing

```python
import time

def test_performance():
    start = time.time()
    results = consolidator.run_consolidation()
    duration = time.time() - start
    
    assert duration < 300  # Complete in < 5 minutes
    print(f"Pipeline completed in {duration:.2f}s")
```

---

## Performance Characteristics

### Optimization Opportunities

1. **Parallel Processing**: Feature engineering
2. **Caching**: Graph metrics computation
3. **Sampling**: Large cluster handling
4. **Approximation**: Faster clustering algorithms

---

## References

### Key Algorithms

1. **PageRank**: Service importance ranking
2. **Betweenness Centrality**: Service criticality
3. **Isolation Forest**: Outlier detection
4. **PCA**: Feature dimensionality reduction
5. **K-Means**: Centroid-based clustering
6. **DBSCAN**: Density-based clustering
7. **Silhouette Score**: Clustering quality metric

### Libraries Used

- **pandas**: Data manipulation
- **networkx**: Graph operations
- **scikit-learn**: Machine learning
- **numpy**: Numerical computing
- **matplotlib/seaborn**: Visualization

---

## Future Enhancements

### Future Enhancement

1. **Incremental Processing**: Support batch updates
2. **Real-time Processing**: Stream processing capability
3. **Custom Clustering**: User-defined cluster functions
4. **Alert Correlation**: Cross-service correlation rules
5. **Anomaly Detection**: Pattern-based anomaly detection
6. **Dashboard Web UI**: Interactive visualization
7. **API Endpoints**: REST API for programmatic access
8. **Multi-tenancy**: Support multiple environments

