"""
Main Pipeline

Orchestrates the full causal inference RCA pipeline:
  1. Preprocessing  (alerts → focused subgraph)
  2. Load latency data
  3. Build causal graph (reverse + DAG + align)
  4. Detect anomalies
  5. Build & fit causal model
  6. Scenario 1 – Anomaly Attribution
  7. Scenario 2 – Distribution Change
"""

from typing import Dict, Optional
import networkx as nx

from data_preprocessing import AlertDataPreprocessor
from latency_data_loader import get_services_from_subgraph, load_latency_data
from graph_builder import build_latency_causal_graph
from anomaly_detector import detect_anomalies, identify_most_affected_services
from causal_model import build_causal_model, fit_model, evaluate_model
from rca_analyzer import perform_anomaly_attribution, perform_distribution_change


def run_full_pipeline(
    focused_subgraph: nx.DiGraph,
    data_source: str,
    db_config: Optional[Dict] = None,
    anomaly_method: str = 'zscore',
    anomaly_threshold: float = 2.5,
    use_auto_mechanisms: bool = True,
    analyze_top_n: int = 1,
    alert_context: Optional[Dict] = None,
) -> Optional[Dict]:
    """
    Run complete RCA pipeline.

    Args:
        focused_subgraph: Subgraph from preprocessing (firing Latency alerts mapped to service graph)
        data_source: Path to latency data (CSV folder, single CSV, JSON) or 'db'
        db_config: Required if data_source='db'
        anomaly_method: 'zscore', 'iqr', or 'percentile'
        anomaly_threshold: Threshold for anomaly detection
        use_auto_mechanisms: Use automatic causal mechanism assignment
        analyze_top_n: Number of top affected services to analyze
        alert_context: Dict with 'alerts_df', 'matched_services', etc.

    Returns:
        Dict with anomaly_attribution, distribution_change, target_node — or None
    """
    print("=" * 70)
    print("CAUSAL INFERENCE ROOT CAUSE ANALYSIS")
    print("=" * 70)
    print()

    if alert_context is not None:
        print(f" Alert context: {alert_context.get('num_latency_alerts', '?')} latency alerts, "
              f"{alert_context.get('num_matched_services', '?')} matched services")

    print(f" Focused subgraph: {focused_subgraph.number_of_nodes()} nodes, "
          f"{focused_subgraph.number_of_edges()} edges")
    print()

    # 1. Extract services from subgraph, then pull latency data
    services = get_services_from_subgraph(focused_subgraph)
    latency_data = load_latency_data(services, data_source, db_config)

    # 2. Build causal graph from focused subgraph (reversed + DAG)
    causal_graph, latency_data = build_latency_causal_graph(focused_subgraph, latency_data)

    # 3. Detect anomalies
    normal_data, anomalous_data, anomaly_info = detect_anomalies(
        latency_data, method=anomaly_method, threshold=anomaly_threshold
    )

    if len(anomalous_data) == 0:
        print("\n No anomalies detected. Cannot perform RCA.")
        return None

    # 4. Build, fit causal model
    model = build_causal_model(causal_graph, normal_data, use_auto=use_auto_mechanisms)
    fit_model(model, normal_data)

    # 5. Select target service for RCA
    top_affected = identify_most_affected_services(anomaly_info, top_n=analyze_top_n)
    target_node = top_affected[0][0] if top_affected else list(causal_graph.nodes())[0]

    # 6. Scenario 1: Anomaly Attribution
    attrib_result = perform_anomaly_attribution(model, anomalous_data, target_node)

    # 7. Scenario 2: Distribution Change
    dist_change_result = perform_distribution_change(model, normal_data, anomalous_data, target_node)

    print(f"\n{'=' * 70}")
    print("RCA PIPELINE COMPLETE")
    print(f"{'=' * 70}")

    return {
        'anomaly_attribution': attrib_result,
        'distribution_change': dist_change_result,
        'target_node': target_node,
    }


if __name__ == "__main__":
    # --- Paths ---
    ALERTS_FOLDER = r"C:\Users\jurat.shayidin\aiops\causal_ml_rca_final\alertsdata\incoming"
    GRAPH_PATH = r"C:\Users\jurat.shayidin\aiops\causal_ml_rca_final\service_graph_data\inprocess\service_graph_data.json"
    LATENCY_FOLDER = r"C:\Users\jurat.shayidin\aiops\causal_ml_rca_final\latency_data"

    # --- Step 1: Preprocessing ---
    # alerts → firing → Latency subcategory → map to service graph → focused subgraph
    preprocessor = AlertDataPreprocessor(ALERTS_FOLDER, GRAPH_PATH)
    alerts_df, focused_subgraph = preprocessor.run(hop_depth=1)

    alert_context = {
        'alerts_df': alerts_df,
        'matched_services': preprocessor.matched_services,
        'unmatched_services': preprocessor.unmatched_services,
        'num_latency_alerts': len(preprocessor.latency_alerts),
        'num_matched_services': len(preprocessor.matched_services),
    }

    # --- Step 2: Causal RCA on the focused subgraph ---
    # Services come from subgraph; latency data is pulled for those services
    results = run_full_pipeline(
        focused_subgraph=focused_subgraph,
        data_source=LATENCY_FOLDER,  # or 'db' with db_config for live DB pull
        anomaly_method='zscore',
        anomaly_threshold=2.5,
        use_auto_mechanisms=True,
        analyze_top_n=1,
        alert_context=alert_context,
    )
