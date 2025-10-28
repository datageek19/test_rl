# Alert Consolidation Pipeline - User Guide

## Table of Contents

- [Understanding the Pipeline](#understanding-the-pipeline)
- [Interpreting Results](#interpreting-results)
- [Understanding Clusters](#understanding-clusters)
- [Using the Dashboard](#using-the-dashboard)
- [Best Practices](#best-practices)
- [Common Scenarios](#common-scenarios)

---

## Understanding the Pipeline

### What Does It Do?

The Alert Consolidation Pipeline:

1. **Loads** raw alerts and service dependency graphs
2. **Maps** alerts to graph services (with fallback strategies)
3. **Groups** alerts by service relationships
4. **Clusters** alerts using machine learning
5. **Deduplicates** similar alerts
6. **Ranks** clusters by importance
7. **Exports** consolidated results with visualizations

### Why Use It?

- **Reduces Alert Fatigue**: Consolidates duplicates into single issues
- **Prioritizes Critical Issues**: Scores clusters by importance
- **Provides Context**: Maps alerts to service dependencies
- **Actionable Insights**: Clear, ranked clusters with names

---

## Interpreting Results

### Output Files Overview

#### 1. `alert_consolidation_final.csv`

**Primary output** - Every alert with cluster assignments.

**Key Columns**:
- `cluster_id`: Cluster assignment
- `cluster_name`: Descriptive cluster name
- `cluster_rank`: Priority rank (1 = most critical)
- `cluster_score`: Importance score (higher = more critical)
- `is_duplicate`: Whether alert is a duplicate
- `severity`, `alert_category`, `service_name`, etc.

**Example**:
```csv
alert_id, cluster_id, cluster_name, cluster_rank, cluster_score, is_duplicate
1001, 5, cpu_usage_frontend_saturation_high, 2, 85.3, false
1002, 5, cpu_usage_frontend_saturation_high, 2, 85.3, true
```

#### 2. `ranked_clusters.csv`

**Summary** - Top clusters by importance.

**Key Columns**:
- `rank`: Priority rank (1 = most critical)
- `cluster_name`: Descriptive name
- `ranking_score`: Composite score
- `alert_count`: Number of alerts in cluster
- `unique_alert_types`: Variety of alerts
- `primary_service`: Most affected service

**Example**:
```csv
rank, cluster_name, ranking_score, alert_count, primary_service
1, memory_leak_api_error_critical, 92.5, 143, api-service
2, cpu_usage_frontend_saturation_high, 85.3, 87, frontend-service
```

#### 3. `cluster_visualization.png`

**Dashboard** - 4-panel visualization:
1. Top 10 most critical clusters
2. Severity distribution
3. Top 10 services affected
4. Key metrics summary

---

## Understanding Clusters

### What is a Cluster?

A cluster is a group of related alerts that:
- Share similar characteristics (service, category, pattern)
- Are linked via service dependencies
- Occur within similar timeframes
- Represent the same underlying issue

### How Clusters Are Named

**Naming Convention**: `{alert_type}_{service}_{category}_{severity}`

**Examples**:
- `cpu_usage_frontend_saturation_high`
- `memory_leak_api_error_critical`
- `latency_spike_db_anomaly_warning`

**Breaking Down Names**:
- **alert_type**: Primary alert pattern (cpu_usage, memory_leak, etc.)
- **service**: Most affected service (frontend, api, db)
- **category**: Alert category (saturation, error, anomaly)
- **severity**: Predominant severity level (high, critical, warning)

### How Clusters Are Scored

**Composite Score** (0-100, higher = more important):
- **40%**: Repetitiveness - how often alerts repeat
- **25%**: Severity impact - average severity weight
- **20%**: Cluster size - number of alerts
- **15%**: Service importance - service centrality in graph
- **10%**: Time concentration - how clustered in time

**Interpretation**:
- **Score > 80**: Very critical, immediate attention
- **Score 60-80**: High priority, investigate soon
- **Score 40-60**: Medium priority, monitor
- **Score < 40**: Low priority, routine check

---

## Using the Dashboard

### Understanding the Panels

#### Panel 1: Top 10 Critical Clusters

**What it shows**: Bar chart of most important clusters

**What to look for**:
- Clusters with highest alert counts
- Consistent patterns across time
- High severity concentrations

**Action items**:
- Investigate top 3 clusters first
- Check alert counts for volume
- Review cluster names for issue type

#### Panel 2: Severity Distribution

**What it shows**: Pie chart of overall alert severity

**What to look for**:
- Proportion of critical/high alerts
- Balance across severity levels
- Alert storm indicators (high % critical)

**Action items**:
- If >50% critical, investigate immediately
- Normal distribution: ~20% critical, 40% high, 40% warning
- Alert storm: >70% critical

#### Panel 3: Top Services Affected

**What it shows**: Services with most alerts

**What to look for**:
- Most vulnerable services
- Service-specific patterns
- Cascading failures

**Action items**:
- Focus on top 3 services
- Check for service dependencies
- Investigate cascading failures

#### Panel 4: Key Metrics

**What it shows**: Summary statistics

**Key metrics**:
- Total alerts processed
- Consolidation impact (clusters created)
- Deduplication effect (duplicates removed)
- Critical alert count

**Action items**:
- High dedup % (>50%) indicates alert storms
- Many clusters (>50) suggests widespread issues
- High critical % (>30%) indicates escalation needed

---

## Best Practices

### 1. Regular Execution

**Recommended**: Run pipeline daily or per shift

**Benefits**:
- Detect patterns early
- Track issue resolution
- Monitor service health

**Setup**:
```bash
# Daily cron job
0 9 * * * /path/to/python /path/to/alert_consolidation_complete.py
```

### 2. Review Top Clusters First

**Focus on**: Rank 1-5 clusters

**Why**: These represent most critical, repetitive issues

**Process**:
1. Check cluster name for issue type
2. Review alert count for volume
3. Examine service impact
4. Investigate root cause

### 3. Use Deduplication Metrics

**Track**: Duplicate removal percentage

**Interpretation**:
- **High (>40%)**: Alert storms, focus on automation
- **Medium (20-40%)**: Normal operation
- **Low (<20%)**: High diversity, investigate variety

**Action**:
- If >40%, implement alert correlation rules
- If <20%, review alert definitions

### 4. Monitor Service Impact

**Track**: Top affected services over time

**Pattern detection**:
- Consistent top services → infrastructure issues
- Varying top services → application issues
- Cascading patterns → dependency problems

**Actions**:
- Infrastructure issues → check infrastructure team
- Application issues → check application team
- Cascading failures → check service dependencies

### 5. Adjust Configuration as Needed

**When to adjust**:
- Too many outliers → reduce `OUTLIER_CONTAMINATION`
- Too few clusters → increase `MIN_CLUSTERING_SAMPLES`
- Missed duplicates → increase `TIME_WINDOW_MINUTES`
- Poor mapping → review service graph

**Configuration changes**:
```python
# In alert_consolidation_complete.py
class ComprehensiveAlertConsolidator:
    TIME_WINDOW_MINUTES = 10      # Increase for broader dedup
    OUTLIER_CONTAMINATION = 0.01  # Reduce for fewer outliers
    MIN_CLUSTERING_SAMPLES = 5    # Lower for more clusters
```

---

## 🔍 Scenarios

### Scenario 1: Alert Storm Detection

**Symptoms**:
- Many alerts in short time
- High deduplication rate (>50%)
- Top cluster with high alert count

**Pipeline Output**:
- Single dominant cluster
- High cluster score (>80)
- Many duplicates

**Actions**:
1. Review top cluster name
2. Check service affected
3. Investigate root cause
4. Implement mitigation

### Scenario 2: Cascading Failure

**Symptoms**:
- Multiple services affected
- Temporal clustering
- Dependency chain visible

**Pipeline Output**:
- Multiple related clusters
- Shared parent services
- High service importance scores

**Actions**:
1. Identify root service
2. Check dependency chain
3. Fix root cause
4. Validate recovery

### Scenario 3: False Positive Flood

**Symptoms**:
- Many low-severity alerts
- Different alert types
- Low deduplication rate

**Pipeline Output**:
- Many small clusters
- Low cluster scores
- High variety of alert types

**Actions**:
1. Review alert definitions
2. Adjust alert thresholds
3. Tune alert rules
4. Remove false positives

### Scenario 4: Critical Issue Buried

**Symptoms**:
- Few critical alerts
- Mixed with many warnings
- Low ranking

**Pipeline Output**:
- Critical cluster with low rank
- But high severity weight

**Actions**:
1. Check severity distribution
2. Prioritize by severity not just rank
3. Adjust scoring weights
4. Create separate critical view

---

## Recommendation

### Recommended actions

✅ Run pipeline regularly  
✅ Focus on top 5 clusters  
✅ Review deduplication effectiveness  
✅ Monitor service impact over time  
✅ Customize configuration for your needs  
✅ Use cluster names to understand issues  
✅ Track trends in rankings  
✅ Validate mapping quality  

### Suggested DON'Ts:

❌ Ignore clusters with low scores  
❌ Skip reviewing duplicate statistics  
❌ Overlook service dependencies  
❌ Skip visualizations  
❌ Run on too-small datasets (<10 alerts)  
❌ Ignore mapping confidence scores  
❌ Skip cluster detail views  
❌ Forget to check severity distribution  

---

## Metrics to Track

### Key Performance Indicators

1. **Deduplication Rate**
   - Target: 20-40%
   - Too high: Alert storm
   - Too low: High variety

2. **Cluster Count**
   - Target: 10-30 clusters
   - Too many: Fragmented issues
   - Too few: Over-consolidation

3. **Top Cluster Score**
   - Target: 50-70
   - >80: Critical issue
   - <30: Low priority

4. **Critical Alert Percentage**
   - Target: <20%
   - >30%: Escalation needed
   - >50%: Critical situation

---

## Continuous Improvement

### Weekly Review

- Check top 10 clusters
- Review deduplication trends
- Analyze service impact
- Adjust configuration

### Monthly Review

- Track cluster patterns
- Review scoring weights
- Validate mapping quality
- Update service graph

### Quarterly Review

- Evaluate pipeline effectiveness
- Refine clustering parameters
- Update documentation
- Train new users

