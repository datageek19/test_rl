"""
Simplified Pipeline Runner

Skips alert preprocessing: reads latency data + service graph directly,
builds focused subgraph from overlapping services, and runs causal RCA.

Usage:
    python run_simple.py
"""

import json
import pandas as pd
import networkx as nx
from pathlib import Path
from typing import List, Tuple

from graph_builder import build_latency_causal_graph
from anomaly_detector import detect_anomalies, identify_most_affected_services
from causal_model import build_causal_model, fit_model
from rca_analyzer import perform_anomaly_attribution, perform_distribution_change


# ── Config ────────────────────────────────────────────────────────────
GRAPH_PATH = r"C:\Users\jshay21\causal_ml_rca_final\service_graph_data\inprocess\service_graph_data.json"
LATENCY_FOLDER = r"C:\Users\jshay21\causal_ml_rca_final\latency_data"

ANOMALY_METHOD = "zscore"
ANOMALY_THRESHOLD = 2.5
USE_AUTO_MECHANISMS = True
ANALYZE_TOP_N = 1


# ── Helpers ───────────────────────────────────────────────────────────
def load_service_graph(graph_json_path: str) -> nx.DiGraph:
    """Load service graph from JSON (nodes/edges format)."""
    with open(graph_json_path, "r") as f:
        graph_data = json.load(f)

    graph = nx.DiGraph()
    node_lookup = {}

    if isinstance(graph_data, dict) and "data" in graph_data:
        nodes = graph_data["data"].get("nodes", [])
        edges = graph_data["data"].get("edges", [])

        for node in nodes:
            node_id = str(node.get("id", ""))
            props = node.get("properties", {})
            name = props.get("name", "")
            if name:
                node_lookup[node_id] = name
                graph.add_node(name, **props, label=node.get("label", ""))

        for edge in edges:
            src_name = node_lookup.get(str(edge.get("source", "")), "")
            tgt_name = node_lookup.get(str(edge.get("target", "")), "")
            rel_type = edge.get("label", "")
            if src_name and tgt_name and rel_type:
                graph.add_edge(src_name, tgt_name, relationship_type=rel_type)

    elif isinstance(graph_data, list):
        for rel in graph_data:
            src_name = (rel.get("source_properties") or {}).get("name", "")
            tgt_name = (rel.get("target_properties") or {}).get("name", "")
            rel_type = rel.get("relationship_type", "")
            if src_name:
                graph.add_node(src_name, **(rel.get("source_properties") or {}))
            if tgt_name:
                graph.add_node(tgt_name, **(rel.get("target_properties") or {}))
            if src_name and tgt_name and rel_type:
                graph.add_edge(src_name, tgt_name, relationship_type=rel_type)

    print(f"  Service graph loaded: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")
    return graph


def load_all_latency_data(latency_folder: str) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load ALL latency CSVs from folder (no service filter).
    Returns (DataFrame, list_of_service_names).
    """
    folder = Path(latency_folder)
    csv_files = sorted(folder.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files in {folder}")

    dataframes = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        time_col = next((c for c in df.columns if "time" in c.lower()), None)
        if time_col is None:
            continue

        df[time_col] = pd.to_datetime(df[time_col])

        for col in df.columns:
            if col == time_col:
                continue
            clean_name = col.replace(" - inbound", "").strip()
            values = df[col].astype(str).str.replace(" ms", "", regex=False)
            values = pd.to_numeric(values, errors="coerce")
            sdf = pd.DataFrame({"Time": df[time_col], clean_name: values}).set_index("Time")
            dataframes.append(sdf)

    if not dataframes:
        raise ValueError(f"No latency data loaded from {folder}")

    result = dataframes[0]
    for sdf in dataframes[1:]:
        result = result.join(sdf, how="outer")

    result = result.sort_index().ffill().bfill().interpolate(method="time")
    services = list(result.columns)
    print(f"  Latency data loaded: {result.shape[0]} samples, {len(services)} services: {services}")
    return result, services


def build_focused_subgraph(
    full_graph: nx.DiGraph,
    services: List[str],
    hop_depth: int = 1,
) -> nx.DiGraph:
    """
    Build a focused subgraph around the services found in latency data.
    Only includes services that exist in the service graph + their neighbors.
    """
    graph_nodes = set(full_graph.nodes())
    matched = [s for s in services if s in graph_nodes]
    unmatched = [s for s in services if s not in graph_nodes]

    print(f"  Latency services matched to graph: {matched}")
    if unmatched:
        print(f"  Latency services NOT in graph (will be skipped): {unmatched}")

    if not matched:
        raise ValueError(
            f"No overlap between latency services {services} and graph nodes.\n"
            "Check that service names in latency CSVs match the graph."
        )

    # Expand by hop_depth
    affected_nodes = set(matched)
    frontier = set(matched)
    for _ in range(hop_depth):
        next_frontier = set()
        for node in frontier:
            next_frontier.update(full_graph.predecessors(node))
            next_frontier.update(full_graph.successors(node))
        affected_nodes.update(next_frontier)
        frontier = next_frontier - affected_nodes

    subgraph = full_graph.subgraph(affected_nodes).copy()
    print(f"  Focused subgraph: {subgraph.number_of_nodes()} nodes, {subgraph.number_of_edges()} edges")
    return subgraph


# ── Main ──────────────────────────────────────────────────────────────
def run_simple_pipeline():
    print("=" * 70)
    print("CAUSAL INFERENCE RCA — SIMPLE MODE (no alert preprocessing)")
    print("=" * 70)

    # 1. Load service graph
    print("\n[1/6] Loading service graph...")
    full_graph = load_service_graph(GRAPH_PATH)

    # 2. Load latency data (all CSVs, no filter)
    print("\n[2/6] Loading latency data...")
    latency_data, services = load_all_latency_data(LATENCY_FOLDER)

    # 3. Build focused subgraph from latency services
    print("\n[3/6] Building focused subgraph...")
    focused_subgraph = build_focused_subgraph(full_graph, services, hop_depth=1)

    # 4. Build causal graph (reverse edges + DAG + align with latency data)
    print("\n[4/6] Building causal graph...")
    causal_graph, latency_data = build_latency_causal_graph(focused_subgraph, latency_data)

    # 5. Detect anomalies
    print("\n[5/6] Detecting anomalies...")
    normal_data, anomalous_data, anomaly_info = detect_anomalies(
        latency_data, method=ANOMALY_METHOD, threshold=ANOMALY_THRESHOLD
    )

    if len(anomalous_data) == 0:
        print("\n  No anomalies detected — cannot perform RCA.")
        print("  Try lowering ANOMALY_THRESHOLD or using a different method.")
        return None

    # 6. Build + fit causal model, run RCA
    print("\n[6/6] Building causal model and running RCA...")
    model = build_causal_model(causal_graph, normal_data, use_auto=USE_AUTO_MECHANISMS)
    fit_model(model, normal_data)

    # Target: most affected service
    top_affected = identify_most_affected_services(anomaly_info, top_n=ANALYZE_TOP_N)
    target_node = top_affected[0][0] if top_affected else list(causal_graph.nodes())[0]

    # Scenario 1: Anomaly Attribution
    attrib_result = perform_anomaly_attribution(model, anomalous_data, target_node)

    # Scenario 2: Distribution Change
    dist_change_result = perform_distribution_change(model, normal_data, anomalous_data, target_node)

    print(f"\n{'=' * 70}")
    print("RCA PIPELINE COMPLETE")
    print(f"{'=' * 70}")

    return {
        "anomaly_attribution": attrib_result,
        "distribution_change": dist_change_result,
        "target_node": target_node,
    }


if __name__ == "__main__":
    results = run_simple_pipeline()
