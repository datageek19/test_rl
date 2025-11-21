# Causal Root Cause Analysis for Microservices: Technical Documentation

**Version:** 1.0  
**Framework:** DoWhy (PyWhy)  
**Language:** Python 3.12

---

## Overview

### Objective

Automatically identifies which microservice is causing latency anomalies using causal inference instead of correlation analysis.

**Problem:** When latency increases, traditional monitoring shows all correlated services. But correlation doesn't tell you which service is the actual root cause.

**Solution:** In this solution we uses causal graphs and probabilistic models to distinguish causation from correlation, accurately identifying root causes.

### Quick Example

```
Scenario: Website latency increased from 50ms to 150ms

Traditional Approach:
- 10 services show high latency (all correlated)
- Manual investigation of all 10 services
- Takes hours to identify root cause

Causal RCA:
- Identifies service-x as 85% responsible
- Provides quantified contribution for each service
```

### Key Capability

Given service latency time series data and service dependencies graph, this solutions aims to output:
- Which service(s) caused the anomaly
- Percentage contribution of each service
- Whether issue is intrinsic or propagated

---

## Conceptual Overview

### 1. Graph Direction (Critical Understanding)

Latency causation is the REVERSE of service call direction.

```
Service Calls (who calls whom):
    Frontend → API → Database
    
Latency Causation (who causes whose latency):
    Database → API → Frontend
```

**Why?** 
- Frontend calls API and waits for response
- If API is slow, Frontend becomes slow
- Therefore: API's latency CAUSES Frontend's latency


### 2. Causal Inference vs Correlation

| Approach | What It Finds | Example |
|----------|---------------|---------|
| **Correlation** | Services that change together | A and B both slow → correlation |
| **Causation** | Services that cause changes | B slow → A becomes slow → B causes A |

**Actual Business Impact:** In production, correlation-based systems often identify wrong services because everything is correlated during incidents.

### 3. Two Analysis Methods

In this solution we implements two complementary DoWhy methods:

**Method 1: Anomaly Attribution**
- Analyzes specific anomalous samples
- Question: "Why is THIS particular spike happening?"
- Use for: Incident investigation, real-time RCA

**Method 2: Distribution Change**
- Compares normal vs anomalous periods
- Question: "What changed between time periods?"
- Use for: Trend analysis, performance degradation

---

## System Architecture

### Pipeline Flow diagram

```
1. Load latency data
   ↓
2. Load service dependency graph
   ↓
3. Build latency causal graph (REVERSE direction)
   ↓
4. Detect anomalies (statistical methods to detect outlier)
   ↓
5. Build probabilistic causal model
   ↓
6. Fit model on NORMAL data only
   ↓
7. Run RCA on anomalous data
   ↓
8. Output attributions + visualizations
```

### Components

**CausalRCA Class** - Main orchestrator
- Loads and processes data
- Builds causal graph
- Runs both analysis methods
- Generates visualizations

**Causal Model Construction** - Causal inference engine
- Probabilistic causal models
- Structural equation modeling
- Attribution algorithms


---

## Implementation details

### Installation

```bash
pip install dowhy pandas numpy networkx matplotlib scikit-learn
```

### Quick usecases

```python
from causal_rca_complete import CausalRCA

rca = CausalRCA(
    latency_folder="./service_latency_data",
    graph_json_path="./service_graph.json"
)

# Run analysis
results = rca.run_complete_analysis(
    anomaly_method='percentile',
    anomaly_threshold=5,
    use_auto_mechanisms=True,
    visualize=True
)

print(results['anomaly_attribution'])
print(results['distribution_change'])
```

### Step-by-Step Execution

For more control over each step:

```python
# 1. Load data
rca.load_latency_data()           # Returns DataFrame
rca.load_service_graph()          # Returns nx.DiGraph

# 2. Build causal graph
rca.build_latency_causal_graph()  # Reverses graph direction

# 3. Detect anomalies
rca.detect_anomalies(method='percentile', threshold=5)
# Returns: normal_data, anomalous_data

# 4. Build and fit model
rca.build_and_fit_model(use_auto=True)

# 5. Evaluate quality
model_metrics = rca.evaluate_model()
# Check R² values 

# 6. Run RCA
result1 = rca.attribute_anomalies(target_node='service-name')
result2 = rca.distribution_change_analysis(target_node='service-name')

# 7. Visualize
rca.visualize_attributions(result1, save_path='output.png')
```

---

## Analysis Methods

### Method 1: Anomaly Attribution

**Implementation:**
```python
attributions = gcm.attribute_anomalies(
    causal_model,
    target_node='frontend',
    anomaly_samples=anomalous_data  # Specific samples
)
```

**Sample Output:**
```
Service A:  12.5 ms (78%)  ← Primary cause
Service B:   3.2 ms (20%)  ← Secondary cause
Service C:   0.3 ms (2%)   ← Minor contribution
```

**Interpretation:**
- Service A directly contributed 12.5ms to the anomaly
- 78% of the anomaly is attributable to Service A
- **Action:** Investigate Service A first

**Technical Details:**
- Uses Shapley values to quantify contributions
- Considers causal paths and interventions
- Accounts for both direct and propagated effects

### Method 2: Distribution Change

**Implementation:**
```python
attributions = gcm.distribution_change(
    causal_model,
    normal_data,      # Baseline period (last week)
    anomalous_data,   # Comparison period (this week)
    target_node='frontend',
    difference_estimation_func=lambda x, y: np.mean(y) - np.mean(x)
)
```

**Sample Output:**
```
Service A:  +8.2 ms (73%)  ← Mean increased by 8.2ms
Service B:  +2.1 ms (19%)  ← Mean increased by 2.1ms
Service C:  +0.9 ms (8%)   ← Mean increased by 0.9ms
```

**Interpretation:**
- Service A's average latency increased by 8.2ms
- This accounts for 73% of the total increase
- **Action:** Investigate what changed in Service A between periods

### Sampel Scenario whihc method is best fit for the ask

| Scenario | Method 1 | Method 2 |
|----------|----------|----------|
| Incident response | Yes | No |
| Post-mortem | Yes | No |
| Weekly review | No | Yes |
| Comparing releases | No | Yes |
| Real-time alerting | Yes | No |
| Capacity planning | No | Yes |

To get better insight, we should run both for comprehensive analysis. They answer different questions.

---

## Running RCA

### Anomaly Detection Configuration

**1. Percentile**
```python
anomaly_method='percentile'
anomaly_threshold=5  # Top 5% are anomalies
```
- Most robust
- Works with any distribution
- Intuitive (top X% are anomalies)

**2. Z-Score**
```python
anomaly_method='zscore'
anomaly_threshold=2.5  # 2.5 standard deviations
```
- Good for normal distributions
- More sensitive to outliers

**3. IQR (Interquartile Range)**
```python
anomaly_method='iqr'
anomaly_threshold=1.5  # 1.5 × IQR
```
- Robust to outliers
- Works well for skewed distributions

**hyperparameter tuning:**

```python
# Too few anomalies detected (<10 samples)
anomaly_threshold=10  # More sensitive

# Too many anomalies (>30% of data)
anomaly_threshold=2   # Less sensitive
```

### Model Configuration

**Automatic Mechanism Assignment :**
```python
use_auto_mechanisms=True
```
DoWhy automatically selects best causal mechanisms per service.

**Manual Assignment:**
```python
use_auto_mechanisms=False
```
Uses:
- Root nodes: EmpiricalDistribution
- Other nodes: AdditiveNoiseModel with linear regression

### Target Selection

**Automatic:**
```python
target_node=None  # Auto-selects most affected service
```

**Manual:**
```python
target_node='specific-service-name'
```

---

## Interpreting Results

### Understanding Contributions of affected services

**Positive Values = Increased Latency**
```
Service A:  +5.8 ms  → Caused 5.8ms latency increase
Service B:  +2.1 ms  → Caused 2.1ms latency increase
```

**Negative Values = Decreased Latency**
```
Service C:  -0.5 ms  → Reduced latency by 0.5ms (improved)
```

**Percentage Calculation:**
```
Total = |5.8| + |2.1| + |-0.5| = 8.4
Service A: |5.8| / 8.4 × 100% = 69%
Service B: |2.1| / 8.4 × 100% = 25%
Service C: |-0.5| / 8.4 × 100% = 6%
```

### Model Quality Assessment

**R² (Coefficient of Determination):**

| R² Range | Quality | Action |
|----------|---------|--------|
| 0.9 - 1.0 | Excellent | Trust results fully |
| 0.7 - 0.9 | Good | Results reliable |
| 0.5 - 0.7 | Fair | Cross-validate with logs |
| < 0.5 | Poor | Check data/graph quality |

**R² metric intuition:**
- R² = 0.95: Model explains 95% of latency variance
- R² = 0.45: Model explains 45% of latency variance (investigate why)


### mockup Example Interpretation

**Scenario:**
```
Target: frontend (baseline 50ms, now 150ms)

Method 1 Results:
  api-gateway:  +62 ms (68%)
  auth-service: +18 ms (20%)
  database:     +11 ms (12%)

Method 2 Results:
  api-gateway:  +22 ms (71%)
  auth-service:  +6 ms (19%)
  database:      +3 ms (10%)
```

**Analysis:**

**Both methods agree:** api-gateway is primary cause (68-71%)

**Method 1 (Anomaly Attribution):**
- Specific incidents: api-gateway contributes 62ms directly
- When frontend has anomaly, 68% is from api-gateway

**Method 2 (Distribution Change):**
- Average behavior: api-gateway's mean increased by 22ms
- 71% of mean increase is from api-gateway

**Action Plan:**
1. Investigate api-gateway logs (68-71% responsible)
2. Check recent api-gateway deployments
3. Review api-gateway resource metrics
4. Secondary: check auth-service (19-20%)

### Common Patterns

**Pattern 1: Single Dominant Service**
```
Service A: 85%
Service B: 10%
Service C: 5%
```
**Interpretation:** Clear root cause (Service A)
**Action:** Focus investigation on Service A

**Pattern 2: Split Attribution**
```
Service A: 45%
Service B: 40%
Service C: 15%
```
**Interpretation:** Multiple contributing factors
**Action:** Investigate both A and B

**Pattern 3: Propagated Issue**
```
Frontend: 10%
API: 5%
Database: 85%
```
**Interpretation:** Database issue propagating upstream
**Action:** Fix Database, others will improve automatically

---

## Environment Setup


**1. Install Dependencies**
```bash
# requirements.txt
dowhy==0.11
pandas>=1.5.0
numpy>=1.20.0
networkx>=2.8
matplotlib>=3.5.0
scikit-learn>=1.0.0
```

**3. Configuration File**
```python
# config.py
LATENCY_FOLDER = "/data/latency"
GRAPH_PATH = "/data/service_graph.json"

ANOMALY_METHOD = "percentile"
ANOMALY_THRESHOLD = 5
USE_AUTO_MECHANISMS = True
VISUALIZE = True
OUTPUT_DIR = "/output/rca_results"
```

### Pipeline Run


**run_rca.py:**
```python
import sys
from datetime import datetime
from pathlib import Path
from causal_rca_complete import CausalRCA
import json

def main():
    # Initialize
    rca = CausalRCA(
        latency_folder="/data/latency",
        graph_json_path="/data/service_graph.json"
    )
    
    # Run analysis
    try:
        results = rca.run_complete_analysis(
            anomaly_method='percentile',
            anomaly_threshold=5,
            use_auto_mechanisms=True,
            visualize=True
        )
        
        if results is None:
            print("No anomalies detected")
            return 0
        
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"/output/rca_results_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'anomaly_attribution': {
                    'target': results['anomaly_attribution']['target'],
                    'attributions': results['anomaly_attribution']['attributions']
                },
                'distribution_change': {
                    'target': results['distribution_change']['target'],
                    'attributions': results['distribution_change']['attributions']
                }
            }, f, indent=2)
        
        print(f"Results saved to {output_file}")
        return 0
        
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

### Monitoring Integration

**Prometheus Metrics Export:**
```python
from prometheus_client import Gauge, CollectorRegistry, write_to_textfile

registry = CollectorRegistry()

rca_contribution_gauge = Gauge(
    'rca_service_contribution',
    'Root cause contribution per service',
    ['service', 'method', 'target'],
    registry=registry
)

# Export metrics
results = rca.run_complete_analysis()

for service, value in results['anomaly_attribution']['attributions'].items():
    rca_contribution_gauge.labels(
        service=service,
        method='anomaly_attribution',
        target=results['anomaly_attribution']['target']
    ).set(value)

write_to_textfile('/var/lib/node_exporter/textfile_collector/rca.prom', registry)
```

**Grafana Dashboard Query:**
```promql
# Top root cause services
topk(5, rca_service_contribution{method="anomaly_attribution"})

# Root cause contribution over time
rate(rca_service_contribution[5m])
```

## Causal RCA extension

### Model Validation

Validate causal model assumptions:

```python
# 1. Check causal sufficiency (no hidden confounders)
def check_residual_correlations(rca):
    """Check if residuals are correlated (indicates confounders)"""
    residuals = {}
    
    for node in rca.causal_graph.nodes():
        if rca.causal_graph.in_degree(node) > 0:
            parents = list(rca.causal_graph.predecessors(node))
            X = rca.normal_data[parents].values
            y = rca.normal_data[node].values
            
            mechanism = rca.causal_model.causal_mechanism(node)
            y_pred = mechanism.prediction_model.predict(X)
            residuals[node] = y - y_pred
    
    # Check pairwise residual correlations
    residual_df = pd.DataFrame(residuals)
    corr_matrix = residual_df.corr()
    
    # High residual correlations indicate confounders
    high_corr = (corr_matrix.abs() > 0.3) & (corr_matrix.abs() < 1.0)
    if high_corr.any().any():
        print("Warning: High residual correlations detected (possible confounders)")
        print(corr_matrix[high_corr])
    else:
        print("Residual correlations OK")

# 2. Cross-validation
def cross_validate_model(rca, n_folds=5):
    """Cross-validate model on normal data"""
    from sklearn.model_selection import KFold
    
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=42)
    r2_scores = []
    
    for train_idx, test_idx in kf.split(rca.normal_data):
        train_data = rca.normal_data.iloc[train_idx]
        test_data = rca.normal_data.iloc[test_idx]
        
        # Fit model on train
        temp_model = gcm.ProbabilisticCausalModel(rca.causal_graph)
        gcm.auto.assign_causal_mechanisms(temp_model, train_data)
        gcm.fit(temp_model, train_data)
        
        # Evaluate on test
        # ... (evaluation code)
        
    print(f"Cross-validation R²: {np.mean(r2_scores):.3f} ± {np.std(r2_scores):.3f}")
```

---

## References

### Primary Documentation

- **DoWhy Microservice RCA**: https://www.pywhy.org/dowhy/main/example_notebooks/gcm_rca_microservice_architecture.html
- **DoWhy Online Shop RCA**: https://www.pywhy.org/dowhy/main/example_notebooks/gcm_online_shop.html
- **DoWhy Documentation**: https://www.pywhy.org/dowhy/

---
