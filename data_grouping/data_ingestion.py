"""
Data Ingestion for alert Classification Module

Handles loading and preprocessing of alert data and service graph data.
"""

import pandas as pd
import json
import ast
import os
import networkx as nx
import numpy as np
from typing import Dict, List, Tuple


class DataIngestionService:
    """Service for loading and preprocessing alert and graph data"""
    
    def __init__(self):
        self.firing_alerts = []
        self.graph_relationships = []
        self.service_graph = nx.DiGraph()
        self.service_to_graph = {}
        
        self._pagerank_cache = None
        self._betweenness_cache = None
        self._undirected_graph = None
        self._clustering_coef_cache = {}
        self._service_features_cache = {}
    
    def load_firing_alerts(self, alerts_csv_path: str) -> List[Dict]:
        """Load and parse firing alerts with metadata extraction from payload"""
        
        if not os.path.exists(alerts_csv_path):
            raise FileNotFoundError(f"Alerts CSV not found: {alerts_csv_path}")
        
        df_raw = pd.read_csv(alerts_csv_path, dtype=str, on_bad_lines='skip')
        
        # Pivot the data to get structured alerts
        df_raw = df_raw.rename(columns={
            df_raw.columns[0]: "attribute", 
            df_raw.columns[-1]: "value"
        })
        
        id_cols = [c for c in df_raw.columns if c not in ("attribute", "value")]
        df_pivoted = df_raw.pivot_table(
            index=id_cols, 
            columns='attribute', 
            values='value', 
            aggfunc='first'
        )
        
        index_names = list(df_pivoted.index.names)
        conflicting_names = [name for name in index_names if name in df_pivoted.columns]
        
        if conflicting_names:
            df_pivoted = df_pivoted.drop(columns=conflicting_names)
        
        df_pivoted = df_pivoted.reset_index()
        if 'status' in df_pivoted.columns:
            firing_df = df_pivoted[df_pivoted['status'].str.strip().str.lower() == 'firing']
        else:
            firing_df = df_pivoted
            
        self.firing_alerts = firing_df.to_dict('records')
        for alert in self.firing_alerts:
            self._parse_alert_metadata(alert)
            self._parse_temporal_info(alert)
        return self.firing_alerts
    
    def _parse_alert_metadata(self, alert: Dict):
        """Extract service name and metadata from alert payload"""
        labels_str = alert.get('labels', '')
        if labels_str:
            try:
                labels = ast.literal_eval(labels_str)
                alert['service_name'] = labels.get('service_name', '')
                alert['namespace'] = labels.get('namespace', '')
                alert['pod'] = labels.get('pod', '')
                alert['node'] = labels.get('node', '')
                alert['cluster'] = labels.get('cluster', '')
                alert['workload_type'] = labels.get('workload_type', '')
                alert['anomaly_resource_type'] = labels.get('anomaly_resource_type', '')
                alert['alert_category'] = labels.get('alert_category', '')
                alert['alert_subcategory'] = labels.get('alert_subcategory', '')
                alert['platform'] = labels.get('platform', '')
            except (ValueError, SyntaxError, TypeError):
                raise ValueError(f"Could not parse labels for alert: {alert}")
        
        annotations_str = alert.get('annotations', '')
        if annotations_str:
            try:
                annotations = ast.literal_eval(annotations_str)
                alert['description'] = annotations.get('description', '')
            except (ValueError, SyntaxError, TypeError):
                alert['description'] = alert.get('description', '')
    
    def _parse_temporal_info(self, alert: Dict):
        """Parse temporal information for grouping"""
        try:
            starts_at = alert.get('startsAt') or alert.get('starts_at', '')
            if starts_at:
                alert['start_datetime'] = pd.to_datetime(starts_at)
                alert['start_timestamp'] = alert['start_datetime'].timestamp()
            else:
                alert['start_datetime'] = None
                alert['start_timestamp'] = 0
        except:
            alert['start_datetime'] = None
            alert['start_timestamp'] = 0
    
    def load_graph_data(self, graph_json_path: str) -> Tuple[nx.DiGraph, Dict]:
        """Load graph data and build service mappings"""
        
        if not os.path.exists(graph_json_path):
            raise FileNotFoundError(f"Graph JSON not found: {graph_json_path}")
        
        with open(graph_json_path, 'r') as f:
            graph_data = json.load(f)
        if isinstance(graph_data, list):
            self.graph_relationships = graph_data
        elif isinstance(graph_data, dict) and 'data' in graph_data:
            nodes = graph_data['data'].get('nodes', [])
            edges = graph_data['data'].get('edges', [])
            node_lookup = {}
            for node in nodes:
                node_id = node.get('id', '')
                node_lookup[node_id] = node
            self.graph_relationships = []
            for edge in edges:
                source_id = edge.get('source', '')
                target_id = edge.get('target', '')
                
                source_node = node_lookup.get(source_id, {})
                target_node = node_lookup.get(target_id, {})
                
                rel = {
                    'source_name': source_id,
                    'target_name': target_id,
                    'source_label': source_node.get('label', ''),
                    'target_label': target_node.get('label', ''),
                    'source_properties': source_node.get('properties', {}),
                    'target_properties': target_node.get('properties', {}),
                    'relationship_type': edge.get('label', '') or edge.get('relationship_type', 'RELATES_TO')
                }
                self.graph_relationships.append(rel)
        else:
            raise ValueError(f"Unexpected graph data format: {type(graph_data)}")
        for i, rel in enumerate(self.graph_relationships):
            source_props = rel.get('source_properties') or {}
            target_props = rel.get('target_properties') or {}
            
            source_service_name = source_props.get('name', '')
            target_service_name = target_props.get('name', '')
            if source_service_name:
                self.service_to_graph[source_service_name] = {
                    'graph_name': rel.get('source_name', ''),
                    'properties': source_props,
                    'type': rel.get('source_label', ''),
                    'environment': source_props.get('environment', ''),
                    'namespace': source_props.get('namespace', ''),
                    'cluster': source_props.get('cluster', '')
                }
                self.service_graph.add_node(source_service_name, **source_props)
            
            if target_service_name:
                self.service_to_graph[target_service_name] = {
                    'graph_name': rel.get('target_name', ''),
                    'properties': target_props,
                    'type': rel.get('target_label', ''),
                    'environment': target_props.get('environment', ''),
                    'namespace': target_props.get('namespace', ''),
                    'cluster': target_props.get('cluster', '')
                }
                self.service_graph.add_node(target_service_name, **target_props)
            rel_type = rel.get('relationship_type', '')
            if source_service_name and target_service_name and rel_type:
                self.service_graph.add_edge(
                    source_service_name,
                    target_service_name,
                    relationship_type=rel_type
                )
        try:
            self._pagerank_cache = nx.pagerank(self.service_graph)
            self._betweenness_cache = nx.betweenness_centrality(
                self.service_graph, 
                k=min(100, len(self.service_graph))
            )
            self._undirected_graph = self.service_graph.to_undirected()
            clustering_dict = nx.clustering(self._undirected_graph)
            self._clustering_coef_cache = clustering_dict
            self._precompute_service_features()
        except Exception as e:
            print(f" Warning: Could not compute some graph metrics: {e}")
        
        return self.service_graph, self.service_to_graph
    
    def _precompute_service_features(self):
        """Pre-compute all graph features per service"""
        for service_name in self.service_graph.nodes():
            if service_name not in self._service_features_cache:
                features = {}

                features['degree_total'] = self.service_graph.degree(service_name)
                features['in_degree'] = self.service_graph.in_degree(service_name)
                features['out_degree'] = self.service_graph.out_degree(service_name)

                features['pagerank'] = self._pagerank_cache.get(service_name, 0) if self._pagerank_cache else 0
                features['betweenness'] = self._betweenness_cache.get(service_name, 0) if self._betweenness_cache else 0
                features['clustering_coef'] = self._clustering_coef_cache.get(service_name, 0)

                upstream_rels = []
                downstream_rels = []
                
                for predecessor in self.service_graph.predecessors(service_name):
                    edge_data = self.service_graph.get_edge_data(predecessor, service_name)
                    rel_type = edge_data.get('relationship_type', '') if edge_data else ''
                    upstream_rels.append(rel_type)
                
                for successor in self.service_graph.successors(service_name):
                    edge_data = self.service_graph.get_edge_data(service_name, successor)
                    rel_type = edge_data.get('relationship_type', '') if edge_data else ''
                    downstream_rels.append(rel_type)
                
                features['num_upstream'] = len(upstream_rels)
                features['num_downstream'] = len(downstream_rels)
                features['upstream_calls'] = upstream_rels.count('CALLS')
                features['upstream_owns'] = upstream_rels.count('OWNS')
                features['upstream_belongs_to'] = upstream_rels.count('BELONGS_TO')
                features['downstream_calls'] = downstream_rels.count('CALLS')
                features['downstream_owns'] = downstream_rels.count('OWNS')
                features['downstream_belongs_to'] = downstream_rels.count('BELONGS_TO')

                total_rels = len(upstream_rels) + len(downstream_rels)
                if total_rels > 0:
                    features['ratio_calls'] = (features['upstream_calls'] + features['downstream_calls']) / total_rels
                    features['ratio_owns'] = (features['upstream_owns'] + features['downstream_owns']) / total_rels
                    features['ratio_belongs_to'] = (features['upstream_belongs_to'] + features['downstream_belongs_to']) / total_rels
                else:
                    features['ratio_calls'] = 0
                    features['ratio_owns'] = 0
                    features['ratio_belongs_to'] = 0

                features['dependency_direction'] = features['out_degree'] - features['in_degree']
                neighbors = list(self.service_graph.predecessors(service_name)) + list(self.service_graph.successors(service_name))
                if neighbors:
                    neighbor_degrees = [self.service_graph.degree(n) for n in neighbors]
                    features['avg_neighbor_degree'] = np.mean(neighbor_degrees)
                    features['max_neighbor_degree'] = np.max(neighbor_degrees)
                else:
                    features['avg_neighbor_degree'] = 0
                    features['max_neighbor_degree'] = 0
                
                self._service_features_cache[service_name] = features
    
    def get_service_features_cache(self) -> Dict:
        """Return the pre-computed service features cache"""
        return self._service_features_cache
    
    def get_pagerank_cache(self) -> Dict:
        """Return the pre-computed PageRank cache"""
        return self._pagerank_cache
    
    def get_betweenness_cache(self) -> Dict:
        """Return the pre-computed Betweenness cache"""
        return self._betweenness_cache
