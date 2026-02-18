"""
Data Preprocessing Module for Causal ML RCA

Handles:
1. Loading alerts from JSON files (incoming folder)
2. Filtering firing alerts by subcategory (Latency)
3. Loading service graph (nodes/edges JSON format)
4. Mapping alerts to service graph by service_name
5. Building a focused subgraph of affected services for causal analysis
"""

import pandas as pd
import numpy as np
import json
import os
import networkx as nx
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict


class AlertDataPreprocessor:
    """Loads, filters, and maps alert data to the service graph for causal RCA."""

    def __init__(self, alerts_folder: str, graph_json_path: str):
        """
        Args:
            alerts_folder: Path to folder containing alert JSON files
            graph_json_path: Path to service_graph_data.json
        """
        self.alerts_folder = Path(alerts_folder)
        self.graph_json_path = Path(graph_json_path)

        # Raw data
        self.raw_alerts: List[Dict] = []
        self.firing_alerts: List[Dict] = []
        self.latency_alerts: List[Dict] = []

        # Service graph
        self.service_graph: Optional[nx.DiGraph] = None
        self.node_lookup: Dict[str, Dict] = {}  # node_id -> node data
        self.service_names_in_graph: set = set()

        # Mapped results
        self.matched_services: set = set()
        self.unmatched_services: set = set()
        self.alerts_df: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------
    # 1. Load alerts
    # ------------------------------------------------------------------
    def load_alerts(self) -> List[Dict]:
        """Load all alert JSON files from the incoming folder."""

        json_files = sorted(self.alerts_folder.glob("*.json"))
        if not json_files:
            raise FileNotFoundError(f"No JSON files found in {self.alerts_folder}")

        for json_file in json_files:
            with open(json_file, "r") as f:
                data = json.load(f)
            if isinstance(data, list):
                self.raw_alerts.extend(data)
            elif isinstance(data, dict):
                self.raw_alerts.append(data)

        print(f"  Loaded {len(self.raw_alerts)} alerts from {len(json_files)} files")
        return self.raw_alerts

    # ------------------------------------------------------------------
    # 2. Filter firing + Latency
    # ------------------------------------------------------------------
    def filter_firing_alerts(self) -> List[Dict]:
        """Filter alerts with status == 'firing'."""
        self.firing_alerts = [
            a for a in self.raw_alerts
            if str(a.get("status", "")).strip().lower() == "firing"
        ]
        print(f"  Firing alerts: {len(self.firing_alerts)} / {len(self.raw_alerts)}")
        return self.firing_alerts

    def filter_latency_alerts(self) -> List[Dict]:
        """Filter firing alerts with alert_subcategory == 'Latency'."""
        self.latency_alerts = [
            a for a in self.firing_alerts
            if str(a.get("alert_subcategory", "")).strip().lower() == "latency"
        ]
        print(f"  Latency alerts: {len(self.latency_alerts)} / {len(self.firing_alerts)} firing")
        return self.latency_alerts

    # ------------------------------------------------------------------
    # 3. Load service graph (nodes + edges JSON format)
    # ------------------------------------------------------------------
    def load_service_graph(self) -> nx.DiGraph:
        """
        Load service graph from JSON.

        Supports two formats:
          - dict with data.nodes / data.edges (current format)
          - list of relationship objects (legacy format)
        """

        with open(self.graph_json_path, "r") as f:
            graph_data = json.load(f)

        graph = nx.DiGraph()

        if isinstance(graph_data, dict) and "data" in graph_data:
            nodes = graph_data["data"].get("nodes", [])
            edges = graph_data["data"].get("edges", [])

            # Build node lookup: id -> full node dict
            for node in nodes:
                node_id = node.get("id")
                props = node.get("properties", {})
                name = props.get("name", "")
                if node_id is not None:
                    self.node_lookup[str(node_id)] = node
                if name:
                    self.service_names_in_graph.add(name)
                    graph.add_node(name, **props, label=node.get("label", ""))

            for edge in edges:
                src_id = str(edge.get("source", ""))
                tgt_id = str(edge.get("target", ""))
                rel_type = edge.get("label", "")
                edge_props = edge.get("properties", {})

                src_node = self.node_lookup.get(src_id, {})
                tgt_node = self.node_lookup.get(tgt_id, {})

                src_name = src_node.get("properties", {}).get("name", "")
                tgt_name = tgt_node.get("properties", {}).get("name", "")

                if src_name and tgt_name and rel_type:
                    graph.add_edge(src_name, tgt_name, relationship_type=rel_type, **edge_props)

        elif isinstance(graph_data, list):
            for rel in graph_data:
                src_props = rel.get("source_properties") or {}
                tgt_props = rel.get("target_properties") or {}
                src_name = src_props.get("name", "")
                tgt_name = tgt_props.get("name", "")
                rel_type = rel.get("relationship_type", "")

                if src_name:
                    self.service_names_in_graph.add(src_name)
                    graph.add_node(src_name, **src_props)
                if tgt_name:
                    self.service_names_in_graph.add(tgt_name)
                    graph.add_node(tgt_name, **tgt_props)
                if src_name and tgt_name and rel_type:
                    graph.add_edge(src_name, tgt_name, relationship_type=rel_type)
        else:
            raise ValueError(f"Unexpected graph data format: {type(graph_data)}")

        self.service_graph = graph
        print(f"  Service graph: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")
        return graph

    # ------------------------------------------------------------------
    # 4. Map alerts to service graph
    # ------------------------------------------------------------------
    def map_alerts_to_graph(self) -> pd.DataFrame:
        """
        Map latency alerts to service graph nodes by service_name.

        Returns a DataFrame of latency alerts enriched with graph metadata
        and a flag indicating whether the service exists in the graph.
        """
        print("Mapping alerts to service graph by service_name...")

        alert_service_names = set(
            a.get("service_name", "") for a in self.latency_alerts if a.get("service_name")
        )

        self.matched_services = alert_service_names.intersection(self.service_names_in_graph)
        self.unmatched_services = alert_service_names - self.service_names_in_graph

        print(f"  Alert services:   {len(alert_service_names)}")
        print(f"  Matched to graph: {len(self.matched_services)}")
        print(f"  Unmatched:        {len(self.unmatched_services)}")

        if self.matched_services:
            print(f"  Matched list:     {sorted(self.matched_services)}")
        if self.unmatched_services and len(self.unmatched_services) <= 15:
            print(f"  Unmatched list:   {sorted(self.unmatched_services)}")

        # Build enriched DataFrame
        records = []
        for alert in self.latency_alerts:
            svc = alert.get("service_name", "")
            in_graph = svc in self.service_names_in_graph

            record = {
                "alert_id": alert.get("_id", ""),
                "batch_id": alert.get("batch_id", ""),
                "service_name": svc,
                "alert_name": alert.get("alert_name", ""),
                "alert_category": alert.get("alert_category", ""),
                "alert_subcategory": alert.get("alert_subcategory", ""),
                "severity": alert.get("severity", ""),
                "status": alert.get("status", ""),
                "starts_at": alert.get("starts_at", ""),
                "ends_at": alert.get("ends_at", ""),
                "recorded_at": alert.get("recorded_at", ""),
                "cluster": alert.get("cluster", ""),
                "namespace": alert.get("namespace", ""),
                "environment": alert.get("environment", ""),
                "cloud": alert.get("cloud", ""),
                "platform": alert.get("platform", ""),
                "entity_type": alert.get("entity_type", ""),
                "in_service_graph": in_graph,
            }

            # Extract payload labels
            payload = alert.get("payload", {})
            labels = payload.get("labels", {})
            record["pod"] = labels.get("pod", "")
            record["node"] = labels.get("node", "")
            record["workload_type"] = labels.get("workload_type", "")
            record["anomaly_resource_type"] = labels.get("anomaly_resource_type", "")

            # Extract description
            annotations = payload.get("annotations", {})
            record["description"] = annotations.get("description", "")

            records.append(record)

        self.alerts_df = pd.DataFrame(records)

        # Parse timestamps
        for col in ("starts_at", "ends_at", "recorded_at"):
            if col in self.alerts_df.columns:
                self.alerts_df[col] = pd.to_datetime(self.alerts_df[col], errors="coerce")

        print(f"  Built alerts DataFrame: {self.alerts_df.shape}")
        return self.alerts_df

    # ------------------------------------------------------------------
    # 5. Build focused subgraph for affected services
    # ------------------------------------------------------------------
    def build_affected_subgraph(self, hop_depth: int = 1) -> nx.DiGraph:
        """
        Build a subgraph containing matched services and their neighbors
        up to `hop_depth` hops away.  This focused graph feeds directly
        into the CausalRCA causal graph builder.

        Args:
            hop_depth: Number of hops from matched services to include

        Returns:
            Subgraph as nx.DiGraph
        """
        print(f"Building affected subgraph (depth={hop_depth})...")

        if not self.matched_services or self.service_graph is None:
            raise ValueError("Run map_alerts_to_graph first")

        # Collect nodes within hop_depth of matched services
        affected_nodes = set(self.matched_services)
        frontier = set(self.matched_services)

        for _ in range(hop_depth):
            next_frontier = set()
            for node in frontier:
                if node in self.service_graph:
                    next_frontier.update(self.service_graph.predecessors(node))
                    next_frontier.update(self.service_graph.successors(node))
            affected_nodes.update(next_frontier)
            frontier = next_frontier - affected_nodes

        subgraph = self.service_graph.subgraph(affected_nodes).copy()
        print(f"  Affected subgraph: {subgraph.number_of_nodes()} nodes, {subgraph.number_of_edges()} edges")
        return subgraph

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    def dataproc_summary(self):
        """Print a summary of the preprocessing results."""
        print("\n" + "=" * 70)
        print("PREPROCESSING SUMMARY")
        print("=" * 70)
        print(f"  Raw alerts loaded:        {len(self.raw_alerts)}")
        print(f"  Firing alerts:            {len(self.firing_alerts)}")
        print(f"  Latency alerts:           {len(self.latency_alerts)}")
        print(f"  Services in alerts:       {len(set(a.get('service_name','') for a in self.latency_alerts))}")
        print(f"  Services in graph:        {len(self.service_names_in_graph)}")
        print(f"  Matched to graph:         {len(self.matched_services)}")
        print(f"  Unmatched:                {len(self.unmatched_services)}")

        if self.alerts_df is not None:
            matched_df = self.alerts_df[self.alerts_df["in_service_graph"]]
            print(f"  Alerts with graph match:  {len(matched_df)}")

            # Alert count per matched service
            if not matched_df.empty:
                print("\n  Alerts per matched service:")
                counts = matched_df["service_name"].value_counts()
                for svc, cnt in counts.items():
                    print(f"    {svc:45s} {cnt:4d} alerts")

        print("=" * 70)

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------
    def run(self, hop_depth: int = 2) -> Tuple[pd.DataFrame, nx.DiGraph]:
        """
        Run the full preprocessing pipeline.

        Returns:
            (alerts_df, affected_subgraph)
        """
        self.load_alerts()
        self.filter_firing_alerts()
        self.filter_latency_alerts()
        self.load_service_graph()
        alerts_df = self.map_alerts_to_graph()
        subgraph = self.build_affected_subgraph(hop_depth=hop_depth)
        self.dataproc_summary()
        return alerts_df, subgraph


# ======================================================================
# main run
# ======================================================================
if __name__ == "__main__":
    ALERTS_FOLDER = r"C:\Users\jshay21\causal_ml_rca_final\alertsdata\incoming"
    GRAPH_PATH = r"C:\Users\jshay21\causal_ml_rca_final\service_graph_data\inprocess\service_graph_data.json"

    preprocessor = AlertDataPreprocessor(ALERTS_FOLDER, GRAPH_PATH)
    alerts_df, affected_subgraph = preprocessor.run(hop_depth=1)

    # Show matched alerts
    matched = alerts_df[alerts_df["in_service_graph"]]
    print(f"\nMatched latency alerts ready for RCA: {len(matched)}")
    print(matched[["service_name", "alert_name", "severity", "starts_at"]].to_string(index=False))
