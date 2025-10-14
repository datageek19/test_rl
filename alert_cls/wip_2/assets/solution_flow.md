# Alert Consolidation System - Solution Architecture

## 📐 High-Level Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                    COMPREHENSIVE ALERT CONSOLIDATION SYSTEM                │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐          ┌──────────────────────────────┐               │
│  │ Input Layer  │          │    Processing Engine         │               │
│  └──────────────┘          └──────────────────────────────┘               │
│                                                                             │
│  ┌─────────────────┐       ┌───────────────────────────────┐              │
│  │ Alerts CSV      │──────▶│ ComprehensiveAlert           │              │
│  │ (Pivoted)       │       │ Consolidator                 │              │
│  │                 │       │                               │              │
│  │ • status        │       │ ┌───────────────────────┐    │              │
│  │ • labels        │       │ │ Phase 1: Load & Parse │    │              │
│  │ • annotations   │       │ └───────────────────────┘    │              │
│  │ • startsAt      │       │           ↓                   │              │
│  │ • fingerprint   │       │ ┌───────────────────────┐    │              │
│  └─────────────────┘       │ │ Phase 2: Map to Graph │    │              │
│                             │ └───────────────────────┘    │              │
│  ┌─────────────────┐       │           ↓                   │              │
│  │ Graph JSON      │──────▶│ ┌───────────────────────┐    │              │
│  │ (Relationships) │       │ │ Phase 3: Enrich       │    │              │
│  │                 │       │ └───────────────────────┘    │              │
│  │ • source        │       │           ↓                   │              │
│  │ • target        │       │ ┌───────────────────────┐    │              │
│  │ • relationship  │       │ │ Phase 4: Consolidate  │    │              │
│  │ • properties    │       │ └───────────────────────┘    │              │
│  └─────────────────┘       │           ↓                   │              │
│                             │ ┌───────────────────────┐    │              │
│                             │ │ Phase 5: Engineer     │    │              │
│                             │ │   39 Features         │    │              │
│                             │ └───────────────────────┘    │              │
│                             │           ↓                   │              │
│                             │ ┌───────────────────────┐    │              │
│                             │ │ Phase 6: Cluster      │    │              │
│                             │ │   3 Algorithms        │    │              │
│                             │ └───────────────────────┘    │              │
│                             │           ↓                   │              │
│                             │ ┌───────────────────────┐    │              │
│                             │ │ Phase 7: Deduplicate  │    │              │
│                             │ └───────────────────────┘    │              │
│                             │           ↓                   │              │
│                             │ ┌───────────────────────┐    │              │
│                             │ │ Phase 8: Export       │    │              │
│                             │ └───────────────────────┘    │              │
│                             └───────────────────────────────┘              │
│                                         ↓                                   │
│  ┌──────────────────────────────────────────────────────────────┐         │
│  │                     Output Layer                              │         │
│  ├──────────────────────────────────────────────────────────────┤         │
│  │ • alert_consolidation_final.csv (main results)               │         │
│  │ • cluster_summary.csv (group statistics)                     │         │
│  │ • deduplicated_alerts.csv (unique alerts)                    │         │
│  │ • mapping_statistics.csv (quality metrics)                   │         │
│  │ • clustering_statistics.csv (algorithm performance)          │         │
│  │ • high_priority_alerts.csv (critical alerts)                 │         │
│  │ • root_cause_candidates.csv (RCA insights)                   │         │
│  │ • alerts_namespace_*.csv (team views)                        │         │
│  └──────────────────────────────────────────────────────────────┘         │
│                                                                             │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏗️ Component Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                  COMPREHENSIVE ALERT CONSOLIDATOR                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  DATA LAYER                                                 │    │
│  ├────────────────────────────────────────────────────────────┤    │
│  │  • firing_alerts: List[Dict]                               │    │
│  │  • graph_relationships: List[Dict]                          │    │
│  │  • service_graph: nx.DiGraph                                │    │
│  │  • service_to_graph: Dict[str, Dict]                        │    │
│  │  • enriched_alerts: List[Dict]                              │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  PROCESSING LAYER                                           │    │
│  ├────────────────────────────────────────────────────────────┤    │
│  │  ┌──────────────────────────────────────────────────┐      │    │
│  │  │  Data Loader Module                               │      │    │
│  │  │  • _load_firing_alerts()                          │      │    │
│  │  │  • _load_graph_data()                             │      │    │
│  │  │  • _parse_alert_metadata()                        │      │    │
│  │  │  • _parse_temporal_info()                         │      │    │
│  │  └──────────────────────────────────────────────────┘      │    │
│  │                                                              │    │
│  │  ┌──────────────────────────────────────────────────┐      │    │
│  │  │  Mapping Module                                   │      │    │
│  │  │  • _enrich_alert_with_graph_info()                │      │    │
│  │  │  • _get_service_dependencies()                    │      │    │
│  │  └──────────────────────────────────────────────────┘      │    │
│  │                                                              │    │
│  │  ┌──────────────────────────────────────────────────┐      │    │
│  │  │  Consolidation Module                             │      │    │
│  │  │  • _group_alerts_by_relationships()               │      │    │
│  │  │  • _find_related_alert_services()                 │      │    │
│  │  │  • _group_unmapped_alerts()                       │      │    │
│  │  │  • _create_consolidated_output()                  │      │    │
│  │  └──────────────────────────────────────────────────┘      │    │
│  │                                                              │    │
│  │  ┌──────────────────────────────────────────────────┐      │    │
│  │  │  Feature Engineering Module                       │      │    │
│  │  │  • _engineer_features()                           │      │    │
│  │  │  • _encode_severity/category/subcategory()        │      │    │
│  │  │  • _encode_workload_type()                        │      │    │
│  │  └──────────────────────────────────────────────────┘      │    │
│  │                                                              │    │
│  │  ┌──────────────────────────────────────────────────┐      │    │
│  │  │  Clustering Module                                │      │    │
│  │  │  • _apply_clustering()                            │      │    │
│  │  │  • _find_optimal_k()                              │      │    │
│  │  │  • _estimate_dbscan_eps()                         │      │    │
│  │  │  • _select_best_clustering()                      │      │    │
│  │  └──────────────────────────────────────────────────┘      │    │
│  │                                                              │    │
│  │  ┌──────────────────────────────────────────────────┐      │    │
│  │  │  Deduplication Module                             │      │    │
│  │  │  • _deduplicate_alerts()                          │      │    │
│  │  │  • _are_duplicates()                              │      │    │
│  │  └──────────────────────────────────────────────────┘      │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  EXPORT LAYER                                               │    │
│  ├────────────────────────────────────────────────────────────┤    │
│  │  • _export_results()                                        │    │
│  │  • _export_group_summary()                                  │    │
│  │  • _export_deduplicated()                                   │    │
│  │  • _export_mapping_details()                                │    │
│  │  • _export_cluster_stats()                                  │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  CACHE LAYER                                                │    │
│  ├────────────────────────────────────────────────────────────┤    │
│  │  • _pagerank_cache: Dict                                    │    │
│  │  • _betweenness_cache: Dict                                 │    │
│  │  • scaler: StandardScaler (fitted)                          │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 System Data Flow

```
                    ┌─────────────────────────────────────┐
                    │      INPUT DATA SOURCES              │
                    └─────────────────────────────────────┘
                                    │
                    ┌───────────────┴──────────────┐
                    │                              │
           ┌────────▼────────┐          ┌─────────▼────────┐
           │  Alerts CSV     │          │  Graph JSON      │
           │  (10K rows)     │          │  (148K rels)     │
           └────────┬────────┘          └─────────┬────────┘
                    │                              │
                    │                              │
          ┌─────────▼─────────┐          ┌─────────▼──────────┐
          │ Parse & Pivot     │          │ Build NetworkX     │
          │ Extract Labels    │          │ Directed Graph     │
          │ Filter 'firing'   │          │ Create Service Map │
          └─────────┬─────────┘          └─────────┬──────────┘
                    │                              │
                    │         ┌────────────────────┘
                    │         │
          ┌─────────▼─────────▼────────┐
          │   ENRICHMENT LAYER          │
          │                             │
          │ 1. Map alerts to services   │
          │    • Direct match (85%)     │
          │    • Fallback match (12%)   │
          │    • Unmapped (3%)          │
          │                             │
          │ 2. Add dependencies         │
          │    • Upstream services      │
          │    • Downstream services    │
          │                             │
          │ 3. Graph metrics            │
          │    • PageRank               │
          │    • Betweenness            │
          │    • Clustering coef        │
          └─────────┬──────────────────┘
                    │
          ┌─────────▼──────────────────┐
          │  CONSOLIDATION LAYER        │
          │                             │
          │ 1. Group by service         │
          │    • Primary service        │
          │    • Related services       │
          │                             │
          │ 2. Merge related groups     │
          │    • 1-hop neighbors        │
          │    • Share dependencies     │
          │                             │
          │ 3. Handle unmapped          │
          │    • By cluster+namespace   │
          │    • By namespace+node      │
          └─────────┬──────────────────┘
                    │
          ┌─────────▼──────────────────┐
          │  FEATURE LAYER              │
          │                             │
          │ Extract 39 Features:        │
          │                             │
          │ Graph (20):                 │
          │ ├─ Topology metrics         │
          │ ├─ Centrality scores        │
          │ ├─ Relationship analysis    │
          │ └─ Neighborhood features    │
          │                             │
          │ Alert (19):                 │
          │ ├─ Severity, category       │
          │ ├─ Workload type            │
          │ ├─ Temporal patterns        │
          │ └─ Combination flags        │
          │                             │
          │ Normalize & Scale           │
          └─────────┬──────────────────┘
                    │
          ┌─────────▼──────────────────┐
          │  CLUSTERING LAYER           │
          │                             │
          │ Run 3 Algorithms:           │
          │                             │
          │ ┌─────────────────────┐    │
          │ │ K-Means             │    │
          │ │ • Optimal k (2-20)  │    │
          │ │ • Silhouette score  │    │
          │ └─────────────────────┘    │
          │                             │
          │ ┌─────────────────────┐    │
          │ │ DBSCAN              │    │
          │ │ • Auto eps          │    │
          │ │ • Density-based     │    │
          │ └─────────────────────┘    │
          │                             │
          │ ┌─────────────────────┐    │
          │ │ Hierarchical        │    │
          │ │ • Ward linkage      │    │
          │ │ • Agglomerative     │    │
          │ └─────────────────────┘    │
          │                             │
          │ Auto-Select Best            │
          │ (Highest Silhouette)        │
          └─────────┬──────────────────┘
                    │
          ┌─────────▼──────────────────┐
          │  DEDUPLICATION LAYER        │
          │                             │
          │ For each cluster:           │
          │                             │
          │ 1. Compare alert pairs      │
          │    • Alert name (3 pts)     │
          │    • Service (3 pts)        │
          │    • Severity (1 pt)        │
          │    • Category+sub (2 pts)   │
          │    • Description (2 pts)    │
          │    • Same pod (4 pts)       │
          │    • Group (1 pt)           │
          │    • Namespace+cluster(1pt) │
          │                             │
          │ 2. Score >= 5? Duplicate    │
          │                             │
          │ 3. Keep representative      │
          └─────────┬──────────────────┘
                    │
          ┌─────────▼──────────────────┐
          │  OUTPUT LAYER               │
          │                             │
          │ Generate 8+ Files:          │
          │ • Final results             │
          │ • Cluster summaries         │
          │ • Deduplicated alerts       │
          │ • Mapping stats             │
          │ • Clustering stats          │
          │ • High priority alerts      │
          │ • Root cause candidates     │
          │ • Namespace-specific views  │
          └────────────────────────────┘
```

---

## 🔀 Detailed Phase Flow Diagrams

See [FLOW_DIAGRAMS.md](FLOW_DIAGRAMS.md) for detailed phase-by-phase flow diagrams.

---

## 🗃️ Data Models

### Alert Data Model

```
Alert {
    // From CSV columns
    _id: string
    batch_id: string
    starts_at: datetime
    ends_at: datetime
    status: string
    service_name: string
    alert_category: string
    alert_subcategory: string
    severity: string
    alert_name: string
    
    // From labels dict
    namespace: string
    cluster: string
    pod: string
    node: string
    workload_type: string
    anomaly_resource_type: string
    platform: string
    
    // From annotations dict
    description: string
    
    // Enriched during processing
    graph_service: string
    graph_info: Dict
    mapping_method: string
    mapping_confidence: float
    dependencies: {
        upstream: List[{service, relationship}]
        downstream: List[{service, relationship}]
    }
    initial_group_id: int
    cluster_id: int
    clustering_method: string
    is_duplicate: boolean
    duplicate_of: int
}
```

### Service Graph Model

```
ServiceGraph: NetworkX DiGraph {
    Nodes: {
        service_name: string (key)
        properties: {
            name: string
            namespace: string
            cluster: string
            environment: string
            type: string
            tenantId: string
        }
    }
    
    Edges: {
        source: service_name
        target: service_name
        relationship_type: CALLS | OWNS | BELONGS_TO
    }
}
```

### Consolidated Group Model

```
ConsolidatedGroup {
    group_id: int
    primary_service: string
    related_services: List[string]
    alerts: List[Alert]
    alert_count: int
    service_count: int
    grouping_method: string
    
    // Statistics
    alert_types: List[string]
    namespaces: List[string]
    clusters: List[string]
    severity_distribution: Dict[string, int]
    category_distribution: Dict[string, int]
    subcategory_distribution: Dict[string, int]
    
    // Temporal
    earliest_alert: timestamp
    latest_alert: timestamp
    time_span_minutes: float
    
    // Insights
    most_common_alert: string
    most_common_category: string
    most_common_subcategory: string
}
```

---

## 🎯 Technology Stack

```
┌──────────────────────────────────────────────┐
│          TECHNOLOGY STACK                     │
├──────────────────────────────────────────────┤
│                                               │
│  Data Processing                              │
│  ├─ pandas (DataFrame operations)            │
│  ├─ numpy (Numerical arrays)                 │
│  └─ ast (Label parsing)                      │
│                                               │
│  Graph Processing                             │
│  └─ networkx (Graph algorithms)              │
│                                               │
│  Machine Learning                             │
│  ├─ scikit-learn                             │
│  │   ├─ KMeans                                │
│  │   ├─ DBSCAN                                │
│  │   ├─ AgglomerativeClustering              │
│  │   ├─ StandardScaler                       │
│  │   └─ silhouette_score                     │
│  └─ scipy (Distance metrics)                 │
│                                               │
│  Data Structures                              │
│  ├─ collections.Counter                      │
│  ├─ typing (Type hints)                      │
│  └─ datetime (Temporal processing)           │
│                                               │
└──────────────────────────────────────────────┘
```

---

## 📊 Performance Characteristics

### Time Complexity by Phase

| Phase | Complexity | Dominant Operation |
|-------|------------|-------------------|
| Load Alerts | O(n) | CSV parsing |
| Load Graph | O(m) | JSON parsing |
| Build Graph | O(m) | Edge insertion |
| Graph Metrics | O(n+m) | PageRank, Betweenness |
| Mapping | O(n×s) | Service lookup (⚠️ optimizable) |
| Consolidation | O(n + s×d) | Neighbor finding |
| Feature Engineering | O(n×d) | Dependency traversal |
| K-Means | O(n×k×i×f) | Iterations |
| DBSCAN | O(n log n) | Neighbor search |
| Hierarchical | O(n²) | Linkage computation |
| Deduplication | O(c×m²) | Pairwise comparison |
| Export | O(n) | File writing |

**Legend:**
- n = alerts count
- s = services count
- m = relationships count
- d = avg degree
- k = clusters
- i = iterations
- f = features
- c = cluster count
- m = avg cluster size

### Space Complexity

| Component | Space | Notes |
|-----------|-------|-------|
| Alerts | O(n) | List of dicts |
| Graph | O(n + m) | Sparse representation |
| Features | O(n × f) | 39 features per alert |
| Cached Metrics | O(n) | PageRank, Betweenness dicts |
| Total | O(n × f + m) | Dominated by feature matrix |

---

## 🔐 Configuration & Tuning

### Tunable Parameters

```python
class ComprehensiveAlertConsolidator:
    # Deduplication
    TIME_WINDOW_MINUTES = 5              # Time proximity for duplicates
    DUPLICATE_THRESHOLD = 5              # Score needed to mark duplicate
    DESCRIPTION_SIMILARITY_THRESHOLD = 0.7  # Text similarity threshold
    
    # Mapping
    MIN_MATCH_SCORE = 2                  # Fallback mapping minimum
    
    # Clustering
    MIN_CLUSTERING_SAMPLES = 10          # Minimum alerts for ML clustering
```

### Parameter Impact Matrix

| Parameter | Increase → | Decrease → |
|-----------|------------|------------|
| `TIME_WINDOW_MINUTES` | More duplicates found | Fewer duplicates |
| `DUPLICATE_THRESHOLD` | Fewer duplicates (stricter) | More duplicates (looser) |
| `DESCRIPTION_SIMILARITY` | Fewer text-based duplicates | More text-based duplicates |
| `MIN_MATCH_SCORE` | Fewer fallback mappings | More fallback mappings |
| `MIN_CLUSTERING_SAMPLES` | Clustering skipped more often | Clustering on smaller datasets |

---

## 🎯 Integration Points

```
┌──────────────────────────────────────────────────────────────────┐
│                    SYSTEM INTEGRATION                             │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  UPSTREAM (Data Sources)                                          │
│  ├─ Prometheus/AlertManager → alerts CSV export                  │
│  ├─ Service Mesh / APM → graph relationship export               │
│  └─ Monitoring Platform → real-time alert stream                 │
│                                                                   │
│  CORE PROCESSING                                                  │
│  └─ ComprehensiveAlertConsolidator                               │
│      └─ run_consolidation() → consolidated groups                │
│                                                                   │
│  DOWNSTREAM (Consumers)                                           │
│  ├─ Incident Management                                           │
│  │   └─ Import cluster_summary.csv                               │
│  │       └─ Create incident per cluster                          │
│  │                                                                │
│  ├─ Dashboards                                                    │
│  │   └─ Import alert_consolidation_final.csv                     │
│  │       └─ Visualize alert groups                               │
│  │                                                                │
│  ├─ On-Call Notifications                                         │
│  │   └─ Import high_priority_alerts.csv                          │
│  │       └─ Page SRE team                                        │
│  │                                                                │
│  ├─ Root Cause Analysis                                           │
│  │   └─ Import root_cause_candidates.csv                         │
│  │       └─ Guide investigation                                  │
│  │                                                                │
│  └─ Team-Specific Views                                           │
│      └─ Import alerts_namespace_*.csv                            │
│          └─ Distribute to teams                                  │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 📈 Scalability Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              SCALABILITY CONSIDERATIONS                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Current Capacity (Single Instance)                          │
│  ├─ Alerts: 50K                                             │
│  ├─ Services: 20K                                            │
│  ├─ Relationships: 500K                                      │
│  ├─ Processing Time: ~3 minutes                             │
│  └─ Memory: ~3 GB                                            │
│                                                              │
│  Optimization Strategies                                     │
│  ├─ Caching (Implemented ✅)                                 │
│  │   └─ PageRank, Betweenness pre-computed                  │
│  │                                                            │
│  ├─ Sampling (Partial ✅)                                    │
│  │   └─ Betweenness uses k=100 sampling                     │
│  │                                                            │
│  ├─ Indexing (Recommended ⚠️)                                │
│  │   └─ Build namespace/cluster indices for O(1) lookup     │
│  │                                                            │
│  └─ Parallel Processing (Future 🔮)                          │
│      └─ Parallelize feature extraction                       │
│                                                              │
│  For Extreme Scale (>100K alerts)                            │
│  ├─ Distributed Processing (Spark/Dask)                     │
│  ├─ Graph Database (Neo4j) for relationships                │
│  ├─ Approximate Algorithms (MinHash LSH for dedup)          │
│  └─ Incremental Clustering (online learning)                │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔒 Quality Assurance Architecture

```
┌─────────────────────────────────────────────────────┐
│            QUALITY ASSURANCE LAYERS                  │
├─────────────────────────────────────────────────────┤
│                                                      │
│  Input Validation Layer                              │
│  ├─ File existence checks                           │
│  ├─ Data format validation                          │
│  └─ Minimum dataset requirements                    │
│                                                      │
│  Processing Validation Layer                         │
│  ├─ Mapping coverage tracking                       │
│  ├─ Feature matrix NaN detection                    │
│  ├─ Clustering quality metrics (silhouette)         │
│  └─ Deduplication rate monitoring                   │
│                                                      │
│  Output Validation Layer                             │
│  ├─ File generation verification                    │
│  ├─ Group ID continuity checks                      │
│  └─ Metadata completeness validation                │
│                                                      │
│  Error Handling Strategy                             │
│  ├─ Specific exceptions (ValueError, SyntaxError)   │
│  ├─ Graceful degradation (skip vs fail)            │
│  ├─ Informative error messages                      │
│  └─ Validation script (validate_implementation.py)  │
│                                                      │
└─────────────────────────────────────────────────────┘
```

---

*See [FLOW_DIAGRAMS.md](FLOW_DIAGRAMS.md) for detailed phase-by-phase flow diagrams*

