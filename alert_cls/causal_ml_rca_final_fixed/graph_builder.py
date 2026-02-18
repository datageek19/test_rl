"""
Graph Builder

Builds the latency causal graph from the focused subgraph:
  1. Reverse edges (call direction -> latency causation direction)
  2. Convert to DAG (required by DoWhy)
  3. Align graph nodes with available latency data columns
"""

import pandas as pd
import networkx as nx
from typing import Tuple


def build_latency_causal_graph(
    focused_subgraph: nx.DiGraph,
    latency_data: pd.DataFrame,
) -> Tuple[nx.DiGraph, pd.DataFrame]:
    """
    Build causal graph from the focused subgraph produced by data preprocessing.

    The focused subgraph comes from:
      alerts (firing + Latency) -> map to service graph -> extract subgraph

    Latency causation is REVERSED from service calls:
    If A calls B (A->B), then B's latency causes A's latency (B->A in causal graph).

    Args:
        focused_subgraph: Service call graph (from preprocessing)
        latency_data: DataFrame with service latency columns

    Returns:
        (causal_graph, filtered_latency_data) with aligned nodes/columns
    """
    # Reverse edges: service call direction -> latency causation direction
    # If A calls B (A->B), B's latency causes A's latency (B->A in causal graph)
    latency_graph = focused_subgraph.reverse()

    # Convert to DAG (required by DoWhy)
    latency_graph = _convert_to_dag(latency_graph)

    # Align graph nodes <-> latency data columns (both must match exactly)
    graph_services = set(latency_graph.nodes())
    data_services = set(latency_data.columns)
    common = sorted(graph_services & data_services)

    if not common:
        raise ValueError(
            f"No overlap between causal graph nodes ({len(graph_services)}) "
            f"and latency data columns ({len(data_services)})"
        )

    # Filter latency data to matched services
    latency_data = latency_data[common]

    # Filter graph to matched services (remove nodes with no latency data)
    nodes_to_remove = graph_services - set(common)
    if nodes_to_remove:
        latency_graph.remove_nodes_from(nodes_to_remove)
        print(f"  Removed {len(nodes_to_remove)} graph nodes with no latency data")

    print(f"  Aligned {len(common)} services: {common}")
    print(f"  Causal graph: {latency_graph.number_of_nodes()} nodes, {latency_graph.number_of_edges()} edges")

    return latency_graph, latency_data


def _convert_to_dag(graph: nx.DiGraph) -> nx.DiGraph:
    """Convert cyclic graph to DAG by removing cycle-creating edges."""
    dag = graph.copy()

    if nx.is_directed_acyclic_graph(dag):
        return dag

    edges_removed = []
    while not nx.is_directed_acyclic_graph(dag):
        try:
            cycle = nx.find_cycle(dag, orientation='original')
            edge_to_remove = cycle[-1][:2]
            dag.remove_edge(*edge_to_remove)
            edges_removed.append(edge_to_remove)
        except nx.NetworkXNoCycle:
            break

    if edges_removed:
        print(f"  Removed {len(edges_removed)} edges to break cycles: {edges_removed[:3]}{'...' if len(edges_removed) > 3 else ''}")

    return dag
