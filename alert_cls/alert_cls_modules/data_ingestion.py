
import pandas as pd
import json
import ast
import os
import networkx as nx
import numpy as np
from typing import Dict, List, Tuple
from config import Config


class DataIngestionService:
    
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
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++
    def load_firing_alerts(self, alerts_csv_path: str) -> List[Dict]:
        
        if not os.path.exists(alerts_csv_path):
            raise FileNotFoundError(f"Alerts CSV not found: {alerts_csv_path}")
        
        df_raw = pd.read_csv(alerts_csv_path, dtype=str, on_bad_lines='skip')
        
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
            firing_df = df_pivoted[df_pivoted['status'].str.strip().str.lower() == Config.ALERT_FIRING_STATUS]
        else:
            firing_df = df_pivoted
            
        self.firing_alerts = firing_df.to_dict('records')
        for alert in self.firing_alerts:
            self._parse_alert_metadata(alert)
            self._parse_temporal_info(alert)
        return self.firing_alerts
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++
    def _parse_alert_metadata(self, alert: Dict):
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
                alert['description'] = (
                    annotations.get('description', '') or 
                    annotations.get('summary', '') or 
                    annotations.get('message', '') or
                    ''
                )
            except (ValueError, SyntaxError, TypeError):
                alert['description'] = alert.get('description', '') or alert.get('summary', '') or alert.get('message', '') or ''   
        else:
            alert['description'] = alert.get('description', '') or alert.get('summary', '') or alert.get('message', '') or ''
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++
    def _parse_temporal_info(self, alert: Dict):
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
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++
    def _resolve_graph_file_path(self, graph_json_path: str) -> str:
        """Resolve the actual graph file path from a directory or file path.
        
        If graph_json_path is a directory, manages incoming/inprocess/archive folders:
        - Checks for .json file in incoming folder
        - If found, archives existing inprocess file and moves JSON to inprocess
        - If not found, uses existing JSON file from inprocess
        
        If graph_json_path is a file, returns it directly.
        
        Returns:
            str: The actual path to the graph JSON file to load
        """
        # Check if graph_json_path is a directory or file
        if os.path.isdir(graph_json_path):
            # Directory-based approach with incoming/inprocess folders
            import shutil
            base_dir = Config.SERVICE_GRAPH_DATA_PATH
            incoming_dir = os.path.join(base_dir, 'incoming')
            inprocess_dir = os.path.join(base_dir, 'inprocess')
            archive_dir = Config.SERVICE_GRAPH_DATA_ARCHIVE_DIR
            
            # Create directories if they don't exist
            os.makedirs(incoming_dir, exist_ok=True)
            os.makedirs(inprocess_dir, exist_ok=True)
            os.makedirs(archive_dir, exist_ok=True)
            
            # Check for JSON files in incoming folder
            incoming_json_files = [f for f in os.listdir(incoming_dir) if f.endswith('.json')]
            
            if incoming_json_files:
                # Check if file exists in inprocess and archive it
                inprocess_files = [f for f in os.listdir(inprocess_dir) if f.endswith('.json')]
                
                if inprocess_files:
                    # Move existing inprocess file to archive
                    existing_file = os.path.join(inprocess_dir, inprocess_files[0])
                    archive_file = os.path.join(archive_dir, inprocess_files[0])
                    
                    # If file already exists in archive, remove it first
                    if os.path.exists(archive_file):
                        os.remove(archive_file)
                    
                    shutil.move(existing_file, archive_file)
                    print(f"  Archived existing graph data: {inprocess_files[0]} to archive")
                
                # Move JSON to inprocess
                source_file = os.path.join(incoming_dir, incoming_json_files[0])
                dest_file = os.path.join(inprocess_dir, incoming_json_files[0])
                
                # If file already exists in inprocess, remove it first
                if os.path.exists(dest_file):
                    os.remove(dest_file)
                
                shutil.move(source_file, dest_file)
                
                print(f"  Moved graph data: {incoming_json_files[0]} (incoming) -> {incoming_json_files[0]} (inprocess)")
                return dest_file
            else:
                # No file in incoming, pick from inprocess
                inprocess_files = [f for f in os.listdir(inprocess_dir) if f.endswith('.json')]
                
                if not inprocess_files:
                    raise FileNotFoundError(f"No JSON files found in {incoming_dir} and no JSON files in {inprocess_dir}")
                
                # Use the file in inprocess
                actual_graph_path = os.path.join(inprocess_dir, inprocess_files[0])
                print(f"  Using existing graph data from inprocess: {inprocess_files[0]}")
                return actual_graph_path
        else:
            # Original file-based approach
            if not os.path.exists(graph_json_path):
                raise FileNotFoundError(f"Graph JSON not found: {graph_json_path}")
            return graph_json_path
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++
    def load_graph_data(self, graph_json_path: str) -> Tuple[nx.DiGraph, Dict]:
        
        actual_graph_path = self._resolve_graph_file_path(graph_json_path)
        
        with open(actual_graph_path, 'r') as f:
            graph_data = json.load(f)
        if isinstance(graph_data, list):
            self.graph_relationships = graph_data
        elif isinstance(graph_data, dict):
            nodes = graph_data.get('nodes', [])
            edges = graph_data.get('edges', [])
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
                    'cloud': source_props.get('cloud', ''),
                    'platform': source_props.get('platform', ''),
                    'environment': source_props.get('env', ''),
                    'namespace': source_props.get('namespace', ''),
                    'cluster': source_props.get('cluster', '')
                }
                self.service_graph.add_node(source_service_name, **source_props)
            
            if target_service_name:
                self.service_to_graph[target_service_name] = {
                    'graph_name': rel.get('target_name', ''),
                    'properties': target_props,
                    'type': rel.get('target_label', ''),
                    'cloud': target_props.get('cloud', ''),
                    'platform': target_props.get('platform', ''),
                    'environment': target_props.get('env', ''),
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
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++
    def _precompute_service_features(self):
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
                # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                # todo start: bug fix related to graph features
                # fix: since we remove finding upstream/downstream services based on relationship type, we need to count relationship types separately, we may comment out related derived features
                features['num_upstream'] = len(upstream_rels)
                features['num_downstream'] = len(downstream_rels)
                features['upstream_calls'] = upstream_rels.count('CALLS')
                features['upstream_owns'] = upstream_rels.count('OWNS')
                features['upstream_belongs_to'] = upstream_rels.count('BELONGS_TO')
                features['downstream_calls'] = downstream_rels.count('CALLS')
                features['downstream_owns'] = downstream_rels.count('OWNS')
                features['downstream_belongs_to'] = downstream_rels.count('BELONGS_TO')
                # todo end
                # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++
    def get_service_features_cache(self) -> Dict:
        return self._service_features_cache
    
    def get_pagerank_cache(self) -> Dict:
        return self._pagerank_cache
    
    def get_betweenness_cache(self) -> Dict:
        return self._betweenness_cache
