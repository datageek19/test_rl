import networkx as nx
import pandas as pd
import json
import ast
import os
import re
from collections import Counter, defaultdict
from typing import Dict, List, Any, Tuple
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN, KMeans, AgglomerativeClustering
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.ensemble import RandomForestClassifier
import warnings
warnings.filterwarnings('ignore')

class ServiceGraphAlertConsolidator:
    """
    Alert Consolidation using Service Graph Relationships:
        1. Maps alert service names to graph service names
        2. Identifies hierarchical relationships (calls, belongs_to, owns)
        3. Groups alerts by service dependencies and infrastructure layers
        4. Categorizes alerts based on service graph topology
    """
    
    def __init__(self, alerts_csv_path: str, graph_json_path: str):
        self.alerts_csv_path = alerts_csv_path
        self.graph_json_path = graph_json_path
        
        # Core data structures
        self.firing_alerts = []
        self.graph_relationships = []
        self.service_graph = nx.DiGraph()
        self.service_to_graph = {}
        
        # Consolidation results
        self.consolidated_groups = []
        self.service_hierarchy = {}
        self.alert_categories = {}
        self.enriched_alerts = []  # Add missing attribute
        
        # Clustering attributes
        self.feature_matrix = None
        self.feature_matrix_scaled = None
        self.feature_names = []
        self.scaler = StandardScaler()
        self.clustering_results = {}
        self.best_clustering = None
        self.cluster_patterns = {}
        self.pattern_classifier = None
        
    def _load_firing_alerts(self):
        """Load and parse firing alerts with enhanced metadata extraction"""
        df_raw = pd.read_csv(self.alerts_csv_path, dtype=str, on_bad_lines='skip')
        
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
        
        # Handle conflicting column names before reset_index
        index_names = list(df_pivoted.index.names)
        conflicting_names = [name for name in index_names if name in df_pivoted.columns]
        
        if conflicting_names:
            df_pivoted = df_pivoted.drop(columns=conflicting_names)
        
        df_pivoted = df_pivoted.reset_index()
        
        # Filter firing alerts
        if 'status' in df_pivoted.columns:
            firing_df = df_pivoted[df_pivoted['status'].str.strip().str.lower() == 'firing']
        else:
            firing_df = df_pivoted
            
        self.firing_alerts = firing_df.to_dict('records')
        
        # Parse alert metadata
        for alert in self.firing_alerts:
            self._parse_alert_metadata(alert)
            self._parse_temporal_info(alert)
    
    def _parse_alert_metadata(self, alert):
        """Extract service name and metadata from alert payload"""
        # Parse labels for service information
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
            except:
                pass
        
        annotations_str = alert.get('annotations', '')
        if annotations_str:
            try:
                annotations = ast.literal_eval(annotations_str)
                alert['description'] = annotations.get('description', '')
            except:
                pass
    
    def _parse_temporal_info(self, alert):
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
    
    def _load_graph_data(self):
        """Load graph data and build service mappings"""
        print("    Loading graph data...")
        
        with open(self.graph_json_path, 'r') as f:
            self.graph_relationships = json.load(f)
        
        print(f"  Loaded {len(self.graph_relationships)} relationships")
        print("   Building service graph...")
        
        # Build service graph with progress indicators
        total_rels = len(self.graph_relationships)
        for i, rel in enumerate(self.graph_relationships):
            if i % 10000 == 0:
                progress = (i / total_rels) * 100
                print(f"      Processed {i}/{total_rels} relationships ({progress:.1f}%)")
            
            # Handle None values for properties
            source_props = rel.get('source_properties') or {}
            target_props = rel.get('target_properties') or {}
            
            source_service_name = source_props.get('name', '')
            target_service_name = target_props.get('name', '')
            
            # Map services
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
            
            # Add relationship edge
            rel_type = rel.get('relationship_type', '')
            if source_service_name and target_service_name and rel_type:
                self.service_graph.add_edge(
                    source_service_name,
                    target_service_name,
                    relationship_type=rel_type
                )
        
        print(f"    Built graph with {len(self.service_to_graph)} services and {self.service_graph.number_of_edges()} edges")
    
    def _enrich_alert_with_graph_info(self, alert):
        """Map alert to graph service using service_name, with fallback to node/namespace/cluster"""
        service_name = alert.get('service_name', '')
        
        # Primary mapping: direct service_name match
        if service_name and service_name in self.service_to_graph:
            alert['graph_service'] = service_name
            alert['graph_info'] = self.service_to_graph[service_name]
            alert['mapping_method'] = 'service_name'
            return True
        
        # Fallback mapping: match by namespace + cluster combination
        alert_namespace = alert.get('namespace', '')
        alert_cluster = alert.get('cluster', '')
        alert_node = alert.get('node', '')
        
        # Try to find matching services by namespace and cluster
        matched_services = []
        for svc_name, svc_info in self.service_to_graph.items():
            match_score = 0
            
            # Match namespace
            if alert_namespace and svc_info.get('namespace') == alert_namespace:
                match_score += 2
            
            # Match cluster
            if alert_cluster and svc_info.get('cluster') == alert_cluster:
                match_score += 2
            
            # Match node (if present in service properties)
            if alert_node and svc_info.get('properties', {}).get('node') == alert_node:
                match_score += 1
            
            if match_score >= 2:  # Require at least namespace or cluster match
                matched_services.append((svc_name, svc_info, match_score))
        
        # Use best match if found
        if matched_services:
            # Sort by match score and take best
            matched_services.sort(key=lambda x: x[2], reverse=True)
            best_match = matched_services[0]
            
            alert['graph_service'] = best_match[0]
            alert['graph_info'] = best_match[1]
            alert['mapping_method'] = 'namespace_cluster_fallback'
            alert['match_score'] = best_match[2]
            return True
        
        # No mapping found
        alert['graph_service'] = None
        alert['graph_info'] = None
        alert['mapping_method'] = 'unmapped'
        return False
    
    def _get_service_dependencies(self, service_name):
        """Get upstream and downstream dependencies for a service"""
        dependencies = {
            'upstream': [],   # Services this one depends on
            'downstream': [], # Services that depend on this
            'peers': []       # Services in same namespace/cluster
        }
        
        if not self.service_graph.has_node(service_name):
            return dependencies
        
        # Get upstream dependencies (services this service calls)
        for predecessor in self.service_graph.predecessors(service_name):
            edge_data = self.service_graph.get_edge_data(predecessor, service_name)
            dependencies['upstream'].append({
                'service': predecessor,
                'relationship': edge_data.get('relationship_type', '')
            })
        
        # Get downstream dependencies (services that call this service)
        for successor in self.service_graph.successors(service_name):
            edge_data = self.service_graph.get_edge_data(service_name, successor)
            dependencies['downstream'].append({
                'service': successor,
                'relationship': edge_data.get('relationship_type', '')
            })
        
        return dependencies
    
    def consolidate_alerts(self):
        """Main consolidation logic: enrich alerts and group by relationships"""
        print("\n=== Starting Alert Consolidation ===")
        
        # Load data
        print("\n Loading alerts...")
        self._load_firing_alerts()
        print(f"    Loaded {len(self.firing_alerts)} firing alerts")
        
        print("\n Loading service graph...")
        self._load_graph_data()
        
        # Enrich alerts with graph information
        print("\n[3/5] Enriching alerts with graph relationships...")
        mapped_count = 0
        fallback_count = 0
        unmapped_count = 0
        
        for alert in self.firing_alerts:
            is_mapped = self._enrich_alert_with_graph_info(alert)
            
            if is_mapped:
                mapping_method = alert.get('mapping_method', '')
                if mapping_method == 'service_name':
                    mapped_count += 1
                else:
                    fallback_count += 1
                    
                # Add dependency info
                graph_service = alert.get('graph_service')
                if graph_service:
                    alert['dependencies'] = self._get_service_dependencies(graph_service)
            else:
                unmapped_count += 1
        
        print(f"    Direct mapping (service_name): {mapped_count}")
        print(f"    Fallback mapping (namespace/cluster): {fallback_count}")
        print(f"    Unmapped alerts: {unmapped_count}")
        
        self.enriched_alerts = self.firing_alerts
        
        # Step 3: Group alerts by service and relationships
        print("\n[4/5] Grouping alerts by service relationships...")
        self._group_alerts_by_relationships()
        
        # Step 4: Create consolidated output
        print("\n[5/5] Creating consolidated output...")
        self._create_consolidated_output()
        
        print(f"\n=== Consolidation Complete ===")
        print(f"Total alert groups: {len(self.consolidated_groups)}")
        
        return self.consolidated_groups
    
    def _group_alerts_by_relationships(self):
        """Group alerts based on service relationships and dependencies"""
        # Group alerts by graph service
        service_groups = {}
        unmapped_alerts = []
        
        for alert in self.enriched_alerts:
            graph_service = alert.get('graph_service')
            
            if graph_service:
                if graph_service not in service_groups:
                    service_groups[graph_service] = []
                service_groups[graph_service].append(alert)
            else:
                unmapped_alerts.append(alert)
        
        print(f"    Found {len(service_groups)} service groups")
        print(f"    {len(unmapped_alerts)} unmapped alerts")
        
        # Analyze relationships between alert groups
        self.consolidated_groups = []
        processed_services = set()
        
        for service_name, alerts in service_groups.items():
            if service_name in processed_services:
                continue
            
            # Find related services with alerts
            related_services = self._find_related_alert_services(service_name, service_groups)
            
            # Create consolidated group
            group = {
                'primary_service': service_name,
                'related_services': list(related_services),
                'alerts': alerts.copy(),
                'alert_count': len(alerts),
                'service_count': 1 + len(related_services)
            }
            
            # Add alerts from related services
            for related_svc in related_services:
                if related_svc in service_groups:
                    group['alerts'].extend(service_groups[related_svc])
                    group['alert_count'] += len(service_groups[related_svc])
                    processed_services.add(related_svc)
            
            # Add group metadata
            group['alert_types'] = list(set(a.get('alert_name', '') for a in group['alerts']))
            group['namespaces'] = list(set(a.get('namespace', '') for a in group['alerts'] if a.get('namespace')))
            group['clusters'] = list(set(a.get('cluster', '') for a in group['alerts'] if a.get('cluster')))
            
            self.consolidated_groups.append(group)
            processed_services.add(service_name)
        
        # Handle unmapped alerts - group by namespace/cluster
        if unmapped_alerts:
            unmapped_groups = self._group_unmapped_alerts(unmapped_alerts)
            self.consolidated_groups.extend(unmapped_groups)
    
    def _find_related_alert_services(self, service_name, service_groups, max_depth=1):
        """Find services with alerts that are related to given service"""
        related = set()
        
        if not self.service_graph.has_node(service_name):
            return related
        
        # Get immediate neighbors (depth 1)
        for neighbor in self.service_graph.neighbors(service_name):
            if neighbor in service_groups and neighbor != service_name:
                related.add(neighbor)
        
        # Get reverse neighbors (services calling this service)
        for predecessor in self.service_graph.predecessors(service_name):
            if predecessor in service_groups and predecessor != service_name:
                related.add(predecessor)
        
        return related
    
    def _group_unmapped_alerts(self, unmapped_alerts):
        """Group unmapped alerts by namespace and cluster"""
        groups_dict = {}
        
        for alert in unmapped_alerts:
            namespace = alert.get('namespace', 'unknown')
            cluster = alert.get('cluster', 'unknown')
            key = f"{cluster}:{namespace}"
            
            if key not in groups_dict:
                groups_dict[key] = []
            groups_dict[key].append(alert)
        
        unmapped_groups = []
        for key, alerts in groups_dict.items():
            cluster, namespace = key.split(':', 1)
            group = {
                'primary_service': f'unmapped_{namespace}_{cluster}',
                'related_services': [],
                'alerts': alerts,
                'alert_count': len(alerts),
                'service_count': 0,
                'alert_types': list(set(a.get('alert_name', '') for a in alerts)),
                'namespaces': [namespace],
                'clusters': [cluster],
                'is_unmapped': True
            }
            unmapped_groups.append(group)
        
        pd.DataFrame(unmapped_groups).to_csv('unmapped_alerts.csv', index=False)
        return unmapped_groups
    
    def _create_consolidated_output(self):
        """Create consolidated output dataframe"""
        # Create detailed results
        detailed_results = []
        
        for group_idx, group in enumerate(self.consolidated_groups):
            for alert in group['alerts']:
                result_row = {
                    'group_id': group_idx,
                    'primary_service': group['primary_service'],
                    'related_services_count': len(group['related_services']),
                    'group_alert_count': group['alert_count'],
                    'alert_name': alert.get('alert_name', ''),
                    'severity': alert.get('severity', ''),
                    'status': alert.get('status', ''),
                    'graph_service': alert.get('graph_service', ''),
                    'service_name': alert.get('service_name', ''),
                    'namespace': alert.get('namespace', ''),
                    'cluster': alert.get('cluster', ''),
                    'mapping_method': alert.get('mapping_method', ''),
                    'start_time': alert.get('startsAt', ''),
                    'description': alert.get('description', ''),
                }
                detailed_results.append(result_row)
        
        self.detailed_results_df = pd.DataFrame(detailed_results)
        return self.detailed_results_df
    
    def export_results(self, output_path):
        """Export consolidation results"""
        self.detailed_results_df.to_csv(output_path, index=False)
        print(f"\n✓ Exported consolidated results to: {output_path}")
        
        # Export summary
        summary_path = output_path.replace('.csv', '_summary.csv')
        summary_data = []
        for idx, group in enumerate(self.consolidated_groups):
            summary_data.append({
                'group_id': idx,
                'primary_service': group['primary_service'],
                'related_services_count': len(group['related_services']),
                'total_alerts': group['alert_count'],
                'alert_types': ', '.join(group['alert_types']),
                'namespaces': ', '.join(group['namespaces']),
                'clusters': ', '.join(group['clusters'])
            })
        pd.DataFrame(summary_data).to_csv(summary_path, index=False)
        print(f"✓ Exported summary to: {summary_path}")
    
    def export_mapping_details(self, output_dir='.'):
        """Export detailed mapping information"""
        direct_mapping = []
        fallback_mapping = []
        unmapped = []
        
        for alert in self.enriched_alerts:
            mapping_method = alert.get('mapping_method', '')
            
            row = {
                'alert_name': alert.get('alert_name', ''),
                'service_name': alert.get('service_name', ''),
                'namespace': alert.get('namespace', ''),
                'cluster': alert.get('cluster', ''),
                'graph_service': alert.get('graph_service', ''),
                'mapping_method': mapping_method
            }
            
            if mapping_method == 'service_name':
                direct_mapping.append(row)
            elif mapping_method == 'namespace_cluster_fallback':
                fallback_mapping.append(row)
            else:
                unmapped.append(row)
        
        pd.DataFrame(direct_mapping).to_csv(f'{output_dir}/direct_mapping_details.csv', index=False)
        pd.DataFrame(fallback_mapping).to_csv(f'{output_dir}/fallback_mapping_details.csv', index=False)
        pd.DataFrame(unmapped).to_csv(f'{output_dir}/unmapped_alerts_details.csv', index=False)
        
        print(f"\n✓ Exported mapping details:")
        print(f"  - direct_mapping_details.csv ({len(direct_mapping)} alerts)")
        print(f"  - fallback_mapping_details.csv ({len(fallback_mapping)} alerts)")
        print(f"  - unmapped_alerts_details.csv ({len(unmapped)} alerts)")
    
    def create_hierarchical_super_groups(self):
        """Phase 2: Create hierarchical super-groups"""
        print("\n=== Phase 2: Creating Hierarchical Super-Groups ===")
        
        # Group by alert type patterns
        alert_type_groups = defaultdict(list)
        
        for group in self.consolidated_groups:
            for alert_type in group['alert_types']:
                alert_type_groups[alert_type].append(group)
        
        super_groups = []
        for alert_type, groups in alert_type_groups.items():
            super_group = {
                'alert_type': alert_type,
                'group_count': len(groups),
                'total_alerts': sum(g['alert_count'] for g in groups),
                'affected_services': list(set(g['primary_service'] for g in groups)),
                'groups': groups
            }
            super_groups.append(super_group)
        
        print(f"✓ Created {len(super_groups)} super-groups")
        return super_groups
    
    def export_hierarchical_results(self, super_groups, output_dir='.'):
        """Export hierarchical super-group results"""
        summary_data = []
        drill_down_data = []
        
        for sg in super_groups:
            summary_data.append({
                'alert_type': sg['alert_type'],
                'group_count': sg['group_count'],
                'total_alerts': sg['total_alerts'],
                'affected_services': ', '.join(sg['affected_services'][:5])
            })
            
            for group in sg['groups']:
                drill_down_data.append({
                    'alert_type': sg['alert_type'],
                    'primary_service': group['primary_service'],
                    'related_services_count': len(group['related_services']),
                    'alert_count': group['alert_count']
                })
        
        pd.DataFrame(summary_data).to_csv(f'{output_dir}/hierarchical_super_groups_summary.csv', index=False)
        pd.DataFrame(drill_down_data).to_csv(f'{output_dir}/hierarchical_drill_down.csv', index=False)
        
        print(f"\n✓ Exported hierarchical results:")
        print(f"  - hierarchical_super_groups_summary.csv")
        print(f"  - hierarchical_drill_down.csv")
    
    def print_hierarchical_summary(self, super_groups):
        """Print hierarchical summary"""
        print("\n" + "=" * 70)
        print("PHASE 2: HIERARCHICAL SUPER-GROUPS (Top 10)")
        print("=" * 70)
        
        sorted_super_groups = sorted(super_groups, key=lambda x: x['total_alerts'], reverse=True)
        
        for i, sg in enumerate(sorted_super_groups[:10]):
            print(f"\nSuper-Group {i}: {sg['alert_type']}")
            print(f"  Service Groups: {sg['group_count']}")
            print(f"  Total Alerts: {sg['total_alerts']}")
            print(f"  Affected Services: {', '.join(sg['affected_services'][:3])}")
    
    # ============================================================================
    # PHASE 3: ML-BASED CLUSTERING FOR BEHAVIORAL PATTERNS
    # ============================================================================
    
    def engineer_features_for_clustering(self):
        """Extract comprehensive features from consolidated alerts for clustering"""
        print("\n=== Phase 3: Feature Engineering for Clustering ===")
        
        if self.detailed_results_df is None:
            raise ValueError("No consolidated results available. Run consolidate_alerts() first.")
        
        features_list = []
        
        for idx, alert in self.detailed_results_df.iterrows():
            feature_dict = {}
            
            # 1. Graph Topology Features
            graph_service = alert.get('graph_service', '')
            if pd.notna(graph_service) and graph_service and graph_service in self.service_graph:
                feature_dict['degree_centrality'] = self.service_graph.degree(graph_service)
                feature_dict['in_degree'] = self.service_graph.in_degree(graph_service)
                feature_dict['out_degree'] = self.service_graph.out_degree(graph_service)
                
                # PageRank
                try:
                    if not hasattr(self, '_pagerank_cache'):
                        self._pagerank_cache = nx.pagerank(self.service_graph)
                    feature_dict['pagerank'] = self._pagerank_cache.get(graph_service, 0)
                except:
                    feature_dict['pagerank'] = 0
            else:
                feature_dict['degree_centrality'] = 0
                feature_dict['in_degree'] = 0
                feature_dict['out_degree'] = 0
                feature_dict['pagerank'] = 0
            
            # 2. Alert Metadata Features
            alert_name = str(alert.get('alert_name', '')).lower()
            feature_dict['severity_encoded'] = self._encode_severity(alert.get('severity', ''))
            feature_dict['is_error_alert'] = 1 if 'error' in alert_name else 0
            feature_dict['is_resource_alert'] = 1 if any(x in alert_name for x in ['cpu', 'memory', 'resource', 'hpa']) else 0
            feature_dict['is_latency_alert'] = 1 if 'latency' in alert_name else 0
            feature_dict['is_availability_alert'] = 1 if any(x in alert_name for x in ['down', 'unavailable', 'oom']) else 0
            
            # 3. Service Context Features
            feature_dict['has_namespace'] = 1 if pd.notna(alert.get('namespace')) and alert.get('namespace') != '' else 0
            feature_dict['has_cluster'] = 1 if pd.notna(alert.get('cluster')) and alert.get('cluster') != '' else 0
            feature_dict['mapping_quality'] = 2 if alert.get('mapping_method') == 'service_name' else 1 if alert.get('mapping_method') == 'namespace_cluster_fallback' else 0
            
            # 4. Group Context Features (from consolidation)
            feature_dict['group_size'] = alert.get('group_alert_count', 1)
            feature_dict['related_services_count'] = alert.get('related_services_count', 0)
            feature_dict['group_id'] = alert.get('group_id', -1)
            
            # 5. Temporal features (if available)
            start_time = alert.get('start_time', '')
            if start_time and pd.notna(start_time):
                try:
                    dt = pd.to_datetime(start_time)
                    feature_dict['hour_of_day'] = dt.hour
                    feature_dict['day_of_week'] = dt.dayofweek
                    feature_dict['is_weekend'] = 1 if dt.dayofweek >= 5 else 0
                except:
                    feature_dict['hour_of_day'] = 0
                    feature_dict['day_of_week'] = 0
                    feature_dict['is_weekend'] = 0
            else:
                feature_dict['hour_of_day'] = 0
                feature_dict['day_of_week'] = 0
                feature_dict['is_weekend'] = 0
            
            features_list.append(feature_dict)
        
        # Convert to DataFrame
        features_df = pd.DataFrame(features_list)
        self.feature_names = features_df.columns.tolist()
        
        # Handle any NaN values
        features_df = features_df.fillna(0)
        
        # Store and scale features
        self.feature_matrix = features_df.values
        self.feature_matrix_scaled = self.scaler.fit_transform(self.feature_matrix)
        
        print(f"✓ Created {self.feature_matrix.shape[1]} features for {self.feature_matrix.shape[0]} alerts")
        print(f"  Features: {', '.join(self.feature_names[:10])}...")
        
        return features_df
    
    def _encode_severity(self, severity):
        """Encode severity to numeric value"""
        severity_map = {
            'critical': 4,
            'high': 3,
            'warning': 2,
            'info': 1,
            'unknown': 0
        }
        return severity_map.get(str(severity).lower().strip(), 0)
    
    def apply_clustering_algorithms(self):
        """Apply multiple clustering algorithms and compare"""
        print("\n=== Applying Clustering Algorithms ===")
        
        if self.feature_matrix_scaled is None:
            raise ValueError("Features not engineered. Run engineer_features_for_clustering() first.")
        
        # 1. DBSCAN - Density-based clustering (good for noise)
        print("\n[1/3] DBSCAN Clustering...")
        dbscan = DBSCAN(eps=0.5, min_samples=5, metric='euclidean')
        dbscan_labels = dbscan.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['dbscan'] = {
            'labels': dbscan_labels,
            'n_clusters': len(set(dbscan_labels)) - (1 if -1 in dbscan_labels else 0),
            'n_noise': list(dbscan_labels).count(-1)
        }
        print(f"  ✓ Found {self.clustering_results['dbscan']['n_clusters']} clusters, {self.clustering_results['dbscan']['n_noise']} noise points")
        
        # 2. K-Means - Prototype-based clustering
        print("\n[2/3] K-Means Clustering (auto-selecting k)...")
        best_k = self._find_optimal_k(max_k=20)
        kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
        kmeans_labels = kmeans.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['kmeans'] = {
            'labels': kmeans_labels,
            'n_clusters': best_k,
            'centroids': kmeans.cluster_centers_
        }
        print(f"  ✓ Optimal k={best_k}, created {best_k} clusters")
        
        # 3. Hierarchical Clustering
        print("\n[3/3] Hierarchical Clustering...")
        n_clusters_hier = min(15, max(2, len(self.detailed_results_df) // 10))
        hierarchical = AgglomerativeClustering(n_clusters=n_clusters_hier, linkage='ward')
        hier_labels = hierarchical.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['hierarchical'] = {
            'labels': hier_labels,
            'n_clusters': n_clusters_hier
        }
        print(f"  ✓ Created {n_clusters_hier} hierarchical clusters")
    
    def _find_optimal_k(self, max_k=20):
        """Find optimal number of clusters using elbow method and silhouette score"""
        n_samples = len(self.feature_matrix_scaled)
        max_k = min(max_k, n_samples // 5)
        
        if max_k < 3:
            return 2
        
        silhouette_scores = []
        K_range = range(2, max_k + 1)
        
        for k in K_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(self.feature_matrix_scaled)
            try:
                score = silhouette_score(self.feature_matrix_scaled, labels)
                silhouette_scores.append(score)
            except:
                silhouette_scores.append(0)
        
        # Select k with highest silhouette score
        if silhouette_scores:
            best_k = K_range[np.argmax(silhouette_scores)]
            return best_k
        return 2
    
    def evaluate_clustering_quality(self):
        """Evaluate and compare clustering quality"""
        print("\n=== Clustering Quality Evaluation ===")
        
        evaluation_results = {}
        
        for method, result in self.clustering_results.items():
            labels = result['labels']
            
            # Skip if only one cluster or all noise
            unique_labels = set(labels)
            if len(unique_labels) <= 1 or (len(unique_labels) == 2 and -1 in labels):
                print(f"\n{method.upper()}: Insufficient clusters for evaluation")
                continue
            
            # Filter out noise points for evaluation
            mask = labels != -1
            if mask.sum() < 2:
                continue
            
            filtered_features = self.feature_matrix_scaled[mask]
            filtered_labels = labels[mask]
            
            # Check if we have at least 2 clusters
            if len(set(filtered_labels)) < 2:
                continue
            
            # Calculate metrics
            try:
                silhouette = silhouette_score(filtered_features, filtered_labels)
                davies_bouldin = davies_bouldin_score(filtered_features, filtered_labels)
                calinski_harabasz = calinski_harabasz_score(filtered_features, filtered_labels)
                
                evaluation_results[method] = {
                    'silhouette_score': silhouette,
                    'davies_bouldin_score': davies_bouldin,
                    'calinski_harabasz_score': calinski_harabasz,
                    'n_clusters': result['n_clusters']
                }
                
                print(f"\n{method.upper()}:")
                print(f"  Silhouette Score: {silhouette:.3f} (higher is better)")
                print(f"  Davies-Bouldin Score: {davies_bouldin:.3f} (lower is better)")
                print(f"  Calinski-Harabasz Score: {calinski_harabasz:.1f} (higher is better)")
            except Exception as e:
                print(f"\n{method.upper()}: Error in evaluation - {e}")
                continue
        
        # Select best clustering based on silhouette score
        if evaluation_results:
            best_method = max(evaluation_results.items(), key=lambda x: x[1]['silhouette_score'])[0]
            self.best_clustering = best_method
            print(f"\n✓ Best clustering method: {best_method.upper()}")
        
        return evaluation_results
    
    def extract_cluster_patterns(self, clustering_method=None):
        """Extract interpretable patterns from clusters"""
        if clustering_method is None:
            clustering_method = self.best_clustering or 'kmeans'
        
        print(f"\n=== Extracting Cluster Patterns ({clustering_method}) ===")
        
        labels = self.clustering_results[clustering_method]['labels']
        self.detailed_results_df['cluster_id'] = labels
        
        # Analyze each cluster
        cluster_patterns = {}
        
        for cluster_id in sorted(set(labels)):
            if cluster_id == -1:  # Skip noise
                continue
            
            cluster_alerts = self.detailed_results_df[self.detailed_results_df['cluster_id'] == cluster_id]
            
            if len(cluster_alerts) == 0:
                continue
            
            # Extract patterns
            pattern = {
                'cluster_id': int(cluster_id),
                'size': len(cluster_alerts),
                
                # Most common attributes
                'common_alert_types': cluster_alerts['alert_name'].value_counts().head(3).to_dict() if 'alert_name' in cluster_alerts.columns else {},
                'common_services': cluster_alerts['graph_service'].value_counts().head(5).to_dict() if 'graph_service' in cluster_alerts.columns else {},
                'common_namespaces': cluster_alerts['namespace'].value_counts().head(3).to_dict() if 'namespace' in cluster_alerts.columns else {},
                'common_clusters': cluster_alerts['cluster'].value_counts().head(3).to_dict() if 'cluster' in cluster_alerts.columns else {},
                
                # Severity distribution
                'severity_distribution': cluster_alerts['severity'].value_counts().to_dict() if 'severity' in cluster_alerts.columns else {},
                
                # Mapping quality
                'mapping_methods': cluster_alerts['mapping_method'].value_counts().to_dict() if 'mapping_method' in cluster_alerts.columns else {},
                
                # Dominant characteristics
                'dominant_alert_type': cluster_alerts['alert_name'].mode()[0] if 'alert_name' in cluster_alerts.columns and len(cluster_alerts['alert_name'].mode()) > 0 else 'unknown',
                'dominant_severity': cluster_alerts['severity'].mode()[0] if 'severity' in cluster_alerts.columns and len(cluster_alerts['severity'].mode()) > 0 else 'unknown',
                
                # Feature statistics
                'avg_group_size': float(cluster_alerts['group_alert_count'].mean()) if 'group_alert_count' in cluster_alerts.columns else 0,
                'avg_related_services': float(cluster_alerts['related_services_count'].mean()) if 'related_services_count' in cluster_alerts.columns else 0,
                
                # Service group distribution
                'affected_service_groups': cluster_alerts['group_id'].nunique() if 'group_id' in cluster_alerts.columns else 0,
            }
            
            cluster_patterns[cluster_id] = pattern
            
            print(f"\nCluster {cluster_id} ({len(cluster_alerts)} alerts):")
            print(f"  Dominant Alert: {pattern['dominant_alert_type']}")
            print(f"  Severity: {pattern['dominant_severity']}")
            print(f"  Avg Group Size: {pattern['avg_group_size']:.1f}")
            print(f"  Spans {pattern['affected_service_groups']} service groups")
        
        self.cluster_patterns = cluster_patterns
        return cluster_patterns
    
    def train_pattern_classifier(self, clustering_method=None):
        """Train classifier to assign future alerts to clusters"""
        if clustering_method is None:
            clustering_method = self.best_clustering or 'kmeans'
        
        print(f"\n=== Training Pattern Classifier ===")
        
        labels = self.clustering_results[clustering_method]['labels']
        
        # Filter out noise points
        mask = labels != -1
        X_train = self.feature_matrix_scaled[mask]
        y_train = labels[mask]
        
        if len(set(y_train)) < 2:
            print("  ⚠ Insufficient unique clusters for training classifier")
            return None
        
        # Train Random Forest classifier
        self.pattern_classifier = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        self.pattern_classifier.fit(X_train, y_train)
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.pattern_classifier.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(f"✓ Trained classifier on {len(X_train)} alerts")
        print(f"\nTop 5 Important Features:")
        for idx, row in feature_importance.head(5).iterrows():
            print(f"  {row['feature']}: {row['importance']:.3f}")
        
        return feature_importance
    
    def export_clustering_results(self, output_dir='.'):
        """Export clustering results with both group_id and cluster_id"""
        print("\n=== Exporting Clustering Results ===")
        
        # 1. Export alert cluster assignments (with BOTH group_id and cluster_id)
        cluster_assignments_path = f'{output_dir}/alert_cluster_assignments.csv'
        output_df = self.detailed_results_df.copy()
        
        # Add all clustering results
        for method, result in self.clustering_results.items():
            output_df[f'cluster_{method}'] = result['labels']
        
        output_df.to_csv(cluster_assignments_path, index=False)
        print(f"✓ Alert cluster assignments: {cluster_assignments_path}")
        
        # 2. Export cluster patterns
        if self.cluster_patterns:
            patterns_path = f'{output_dir}/cluster_patterns.json'
            with open(patterns_path, 'w') as f:
                # Convert numpy types to native Python types for JSON serialization
                patterns_serializable = {}
                for cluster_id, pattern in self.cluster_patterns.items():
                    patterns_serializable[str(cluster_id)] = {
                        k: (v.item() if isinstance(v, (np.integer, np.floating)) else 
                            int(v) if isinstance(v, np.integer) else
                            float(v) if isinstance(v, np.floating) else v)
                        for k, v in pattern.items()
                    }
                json.dump(patterns_serializable, f, indent=2)
            print(f"✓ Cluster patterns: {patterns_path}")
        
        # 3. Export cluster summary
        if self.cluster_patterns:
            summary_data = []
            for cluster_id, pattern in self.cluster_patterns.items():
                summary_data.append({
                    'cluster_id': cluster_id,
                    'size': pattern['size'],
                    'dominant_alert_type': pattern['dominant_alert_type'],
                    'dominant_severity': pattern['dominant_severity'],
                    'avg_group_size': pattern['avg_group_size'],
                    'avg_related_services': pattern['avg_related_services'],
                    'affected_service_groups': pattern['affected_service_groups'],
                    'top_services': ', '.join([f"{k}({v})" for k, v in list(pattern['common_services'].items())[:3]])
                })
            
            summary_df = pd.DataFrame(summary_data).sort_values('size', ascending=False)
            summary_path = f'{output_dir}/cluster_summary.csv'
            summary_df.to_csv(summary_path, index=False)
            print(f"✓ Cluster summary: {summary_path}")
        
        print(f"\n✓ All clustering results exported to {output_dir}/")
    
    def visualize_clusters_2d(self, output_dir='.', clustering_method=None):
        """Create 2D visualization of clusters using PCA"""
        if clustering_method is None:
            clustering_method = self.best_clustering or 'kmeans'
        
        print(f"\n=== Creating 2D Cluster Visualization ===")
        
        # Reduce to 2D using PCA
        pca = PCA(n_components=2)
        features_2d = pca.fit_transform(self.feature_matrix_scaled)
        
        labels = self.clustering_results[clustering_method]['labels']
        
        # Create visualization data
        viz_df = pd.DataFrame({
            'x': features_2d[:, 0],
            'y': features_2d[:, 1],
            'cluster': labels,
            'group_id': self.detailed_results_df['group_id'],
            'alert_name': self.detailed_results_df['alert_name'],
            'service': self.detailed_results_df['graph_service'],
            'severity': self.detailed_results_df['severity']
        })
        
        viz_path = f'{output_dir}/cluster_visualization_2d.csv'
        viz_df.to_csv(viz_path, index=False)
        
        print(f"✓ 2D visualization data: {viz_path}")
        print(f"  Variance explained by PC1: {pca.explained_variance_ratio_[0]:.2%}")
        print(f"  Variance explained by PC2: {pca.explained_variance_ratio_[1]:.2%}")
        
        return viz_df
    
    def run_complete_clustering_pipeline(self, output_dir='.'):
        """Run Phase 3: ML-based clustering on consolidated alerts"""
        print("\n" + "="*70)
        print("PHASE 3: ML-BASED BEHAVIORAL CLUSTERING")
        print("="*70)
        
        # Step 1: Engineer features
        self.engineer_features_for_clustering()
        
        # Step 2: Apply clustering algorithms
        self.apply_clustering_algorithms()
        
        # Step 3: Evaluate clustering quality
        evaluation_results = self.evaluate_clustering_quality()
        
        # Step 4: Extract cluster patterns
        self.extract_cluster_patterns()
        
        # Step 5: Train pattern classifier
        feature_importance = self.train_pattern_classifier()
        
        # Step 6: Export results
        self.export_clustering_results(output_dir)
        
        # Step 7: Create visualization
        self.visualize_clusters_2d(output_dir)
        
        print("\n" + "="*70)
        print("CLUSTERING PIPELINE COMPLETE")
        print("="*70)
        
        return {
            'evaluation': evaluation_results,
            'patterns': self.cluster_patterns,
            'feature_importance': feature_importance
        }
    
    
if __name__ == "__main__":
    print("=" * 70)
    print("INTEGRATED ALERT CONSOLIDATION & CLUSTERING PIPELINE")
    print("=" * 70)
    
    # Initialize consolidator
    consolidator = ServiceGraphAlertConsolidator(
        alerts_csv_path='C:/Users/jurat.shayidin/aiops/wip_2/alert_data.csv',  
        graph_json_path='C:/Users/jurat.shayidin/aiops/wip_2/graph_data.json'
    )

    # ========================================================================
    # PHASE 1: SERVICE GRAPH CONSOLIDATION
    # ========================================================================
    print("\n" + "=" * 70)
    print("PHASE 1: SERVICE GRAPH CONSOLIDATION")
    print("=" * 70)
    
    # Run consolidation
    consolidated_groups = consolidator.consolidate_alerts()
    
    # Export consolidated results
    consolidator.export_results('alert_consolidation_results.csv')
    
    # Export detailed mapping information
    consolidator.export_mapping_details('.')
    
    # Phase 2: Create hierarchical super-groups
    super_groups = consolidator.create_hierarchical_super_groups()
    
    # Export hierarchical results
    if super_groups:
        consolidator.export_hierarchical_results(super_groups, output_dir='.')
        consolidator.print_hierarchical_summary(super_groups)
    
    # Print Phase 1 summary
    print("\n" + "=" * 70)
    print("PHASE 1 SUMMARY (Top 20 Groups)")
    print("=" * 70)
    for i, group in enumerate(consolidated_groups[:20]):
        print(f"\nGroup {i}:")
        print(f"  Primary Service: {group['primary_service']}")
        print(f"  Related Services: {len(group['related_services'])}")
        print(f"  Total Alerts: {group['alert_count']}")
        print(f"  Alert Types: {', '.join(group['alert_types'][:3])}")
    
    # ========================================================================
    # PHASE 3: ML-BASED BEHAVIORAL CLUSTERING
    # ========================================================================
    print("\n" + "=" * 70)
    print("RUNNING PHASE 3: ML-BASED CLUSTERING...")
    print("=" * 70)
    
    try:
        clustering_results = consolidator.run_complete_clustering_pipeline(output_dir='.')
        
        # Print cluster insights
        print("\n" + "=" * 70)
        print("CLUSTERING INSIGHTS")
        print("=" * 70)
        
        if consolidator.cluster_patterns:
            print(f"\n✓ Discovered {len(consolidator.cluster_patterns)} behavioral patterns")
            print("\nTop Patterns:")
            sorted_patterns = sorted(consolidator.cluster_patterns.items(), 
                                    key=lambda x: x[1]['size'], reverse=True)
            
            for cluster_id, pattern in sorted_patterns[:5]:
                print(f"\n  Cluster {cluster_id}: {pattern['dominant_alert_type']}")
                print(f"    Size: {pattern['size']} alerts")
                print(f"    Severity: {pattern['dominant_severity']}")
                print(f"    Spans: {pattern['affected_service_groups']} service groups")
                print(f"    Top Services: {list(pattern['common_services'].keys())[:3]}")
        
    except Exception as e:
        print(f"\n⚠ Clustering failed: {e}")
        print("  Continuing with Phase 1 results only...")
    
    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    print("\n" + "=" * 70)
    print("ALL FILES GENERATED:")
    print("=" * 70)
    
    print("\n📊 Phase 1 - Service Topology Grouping:")
    print("  ✓ alert_consolidation_results.csv (detailed with group_id)")
    print("  ✓ alert_consolidation_results_summary.csv (summary)")
    
    print("\n🔍 Mapping Details:")
    print("  ✓ direct_mapping_details.csv (direct matches)")
    print("  ✓ fallback_mapping_details.csv (fallback matches)")
    print("  ✓ unmapped_alerts_details.csv (unmapped alerts)")
    
    print("\n📈 Phase 2 - Hierarchical Grouping:")
    print("  ✓ hierarchical_super_groups_summary.csv (super-groups by alert type)")
    print("  ✓ hierarchical_drill_down.csv (super-group drill-down)")
    
    print("\n🤖 Phase 3 - ML Clustering (Behavioral Patterns):")
    print("  ✓ alert_cluster_assignments.csv (with BOTH group_id AND cluster_id)")
    print("  ✓ cluster_patterns.json (detailed pattern definitions)")
    print("  ✓ cluster_summary.csv (cluster statistics)")
    print("  ✓ cluster_visualization_2d.csv (2D PCA visualization data)")
    
    print("\n" + "=" * 70)
    print("🎯 KEY OUTPUT: alert_cluster_assignments.csv")
    print("=" * 70)
    print("Each alert now has:")
    print("  • group_id   → Service topology grouping (Phase 1)")
    print("  • cluster_id → Behavioral pattern clustering (Phase 3)")
    print("\nUse BOTH for smart alert routing and root cause analysis!")
    print("=" * 70)import networkx as nx
import pandas as pd
import json
import ast
import os
import re
from collections import Counter, defaultdict
from typing import Dict, List, Any, Tuple
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN, KMeans, AgglomerativeClustering
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.ensemble import RandomForestClassifier
import warnings
warnings.filterwarnings('ignore')

class ServiceGraphAlertConsolidator:
    """
    Alert Consolidation using Service Graph Relationships:
        1. Maps alert service names to graph service names
        2. Identifies hierarchical relationships (calls, belongs_to, owns)
        3. Groups alerts by service dependencies and infrastructure layers
        4. Categorizes alerts based on service graph topology
    """
    
    def __init__(self, alerts_csv_path: str, graph_json_path: str):
        self.alerts_csv_path = alerts_csv_path
        self.graph_json_path = graph_json_path
        
        # Core data structures
        self.firing_alerts = []
        self.graph_relationships = []
        self.service_graph = nx.DiGraph()
        self.service_to_graph = {}
        
        # Consolidation results
        self.consolidated_groups = []
        self.service_hierarchy = {}
        self.alert_categories = {}
        self.enriched_alerts = []  # Add missing attribute
        
        # Clustering attributes
        self.feature_matrix = None
        self.feature_matrix_scaled = None
        self.feature_names = []
        self.scaler = StandardScaler()
        self.clustering_results = {}
        self.best_clustering = None
        self.cluster_patterns = {}
        self.pattern_classifier = None
        
    def _load_firing_alerts(self):
        """Load and parse firing alerts with enhanced metadata extraction"""
        df_raw = pd.read_csv(self.alerts_csv_path, dtype=str, on_bad_lines='skip')
        
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
        
        # Handle conflicting column names before reset_index
        index_names = list(df_pivoted.index.names)
        conflicting_names = [name for name in index_names if name in df_pivoted.columns]
        
        if conflicting_names:
            df_pivoted = df_pivoted.drop(columns=conflicting_names)
        
        df_pivoted = df_pivoted.reset_index()
        
        # Filter firing alerts
        if 'status' in df_pivoted.columns:
            firing_df = df_pivoted[df_pivoted['status'].str.strip().str.lower() == 'firing']
        else:
            firing_df = df_pivoted
            
        self.firing_alerts = firing_df.to_dict('records')
        
        # Parse alert metadata
        for alert in self.firing_alerts:
            self._parse_alert_metadata(alert)
            self._parse_temporal_info(alert)
    
    def _parse_alert_metadata(self, alert):
        """Extract service name and metadata from alert payload"""
        # Parse labels for service information
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
            except:
                pass
        
        annotations_str = alert.get('annotations', '')
        if annotations_str:
            try:
                annotations = ast.literal_eval(annotations_str)
                alert['description'] = annotations.get('description', '')
            except:
                pass
    
    def _parse_temporal_info(self, alert):
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
    
    def _load_graph_data(self):
        """Load graph data and build service mappings"""
        print("    Loading graph data...")
        
        with open(self.graph_json_path, 'r') as f:
            self.graph_relationships = json.load(f)
        
        print(f"  Loaded {len(self.graph_relationships)} relationships")
        print("   Building service graph...")
        
        # Build service graph with progress indicators
        total_rels = len(self.graph_relationships)
        for i, rel in enumerate(self.graph_relationships):
            if i % 10000 == 0:
                progress = (i / total_rels) * 100
                print(f"      Processed {i}/{total_rels} relationships ({progress:.1f}%)")
            
            # Handle None values for properties
            source_props = rel.get('source_properties') or {}
            target_props = rel.get('target_properties') or {}
            
            source_service_name = source_props.get('name', '')
            target_service_name = target_props.get('name', '')
            
            # Map services
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
            
            # Add relationship edge
            rel_type = rel.get('relationship_type', '')
            if source_service_name and target_service_name and rel_type:
                self.service_graph.add_edge(
                    source_service_name,
                    target_service_name,
                    relationship_type=rel_type
                )
        
        print(f"    Built graph with {len(self.service_to_graph)} services and {self.service_graph.number_of_edges()} edges")
    
    def _enrich_alert_with_graph_info(self, alert):
        """Map alert to graph service using service_name, with fallback to node/namespace/cluster"""
        service_name = alert.get('service_name', '')
        
        # Primary mapping: direct service_name match
        if service_name and service_name in self.service_to_graph:
            alert['graph_service'] = service_name
            alert['graph_info'] = self.service_to_graph[service_name]
            alert['mapping_method'] = 'service_name'
            return True
        
        # Fallback mapping: match by namespace + cluster combination
        alert_namespace = alert.get('namespace', '')
        alert_cluster = alert.get('cluster', '')
        alert_node = alert.get('node', '')
        
        # Try to find matching services by namespace and cluster
        matched_services = []
        for svc_name, svc_info in self.service_to_graph.items():
            match_score = 0
            
            # Match namespace
            if alert_namespace and svc_info.get('namespace') == alert_namespace:
                match_score += 2
            
            # Match cluster
            if alert_cluster and svc_info.get('cluster') == alert_cluster:
                match_score += 2
            
            # Match node (if present in service properties)
            if alert_node and svc_info.get('properties', {}).get('node') == alert_node:
                match_score += 1
            
            if match_score >= 2:  # Require at least namespace or cluster match
                matched_services.append((svc_name, svc_info, match_score))
        
        # Use best match if found
        if matched_services:
            # Sort by match score and take best
            matched_services.sort(key=lambda x: x[2], reverse=True)
            best_match = matched_services[0]
            
            alert['graph_service'] = best_match[0]
            alert['graph_info'] = best_match[1]
            alert['mapping_method'] = 'namespace_cluster_fallback'
            alert['match_score'] = best_match[2]
            return True
        
        # No mapping found
        alert['graph_service'] = None
        alert['graph_info'] = None
        alert['mapping_method'] = 'unmapped'
        return False
    
    def _get_service_dependencies(self, service_name):
        """Get upstream and downstream dependencies for a service"""
        dependencies = {
            'upstream': [],   # Services this one depends on
            'downstream': [], # Services that depend on this
            'peers': []       # Services in same namespace/cluster
        }
        
        if not self.service_graph.has_node(service_name):
            return dependencies
        
        # Get upstream dependencies (services this service calls)
        for predecessor in self.service_graph.predecessors(service_name):
            edge_data = self.service_graph.get_edge_data(predecessor, service_name)
            dependencies['upstream'].append({
                'service': predecessor,
                'relationship': edge_data.get('relationship_type', '')
            })
        
        # Get downstream dependencies (services that call this service)
        for successor in self.service_graph.successors(service_name):
            edge_data = self.service_graph.get_edge_data(service_name, successor)
            dependencies['downstream'].append({
                'service': successor,
                'relationship': edge_data.get('relationship_type', '')
            })
        
        return dependencies
    
    def consolidate_alerts(self):
        """Main consolidation logic: enrich alerts and group by relationships"""
        print("\n=== Starting Alert Consolidation ===")
        
        # Load data
        print("\n Loading alerts...")
        self._load_firing_alerts()
        print(f"    Loaded {len(self.firing_alerts)} firing alerts")
        
        print("\n Loading service graph...")
        self._load_graph_data()
        
        # Enrich alerts with graph information
        print("\n[3/5] Enriching alerts with graph relationships...")
        mapped_count = 0
        fallback_count = 0
        unmapped_count = 0
        
        for alert in self.firing_alerts:
            is_mapped = self._enrich_alert_with_graph_info(alert)
            
            if is_mapped:
                mapping_method = alert.get('mapping_method', '')
                if mapping_method == 'service_name':
                    mapped_count += 1
                else:
                    fallback_count += 1
                    
                # Add dependency info
                graph_service = alert.get('graph_service')
                if graph_service:
                    alert['dependencies'] = self._get_service_dependencies(graph_service)
            else:
                unmapped_count += 1
        
        print(f"    Direct mapping (service_name): {mapped_count}")
        print(f"    Fallback mapping (namespace/cluster): {fallback_count}")
        print(f"    Unmapped alerts: {unmapped_count}")
        
        self.enriched_alerts = self.firing_alerts
        
        # Step 3: Group alerts by service and relationships
        print("\n[4/5] Grouping alerts by service relationships...")
        self._group_alerts_by_relationships()
        
        # Step 4: Create consolidated output
        print("\n[5/5] Creating consolidated output...")
        self._create_consolidated_output()
        
        print(f"\n=== Consolidation Complete ===")
        print(f"Total alert groups: {len(self.consolidated_groups)}")
        
        return self.consolidated_groups
    
    def _group_alerts_by_relationships(self):
        """Group alerts based on service relationships and dependencies"""
        # Group alerts by graph service
        service_groups = {}
        unmapped_alerts = []
        
        for alert in self.enriched_alerts:
            graph_service = alert.get('graph_service')
            
            if graph_service:
                if graph_service not in service_groups:
                    service_groups[graph_service] = []
                service_groups[graph_service].append(alert)
            else:
                unmapped_alerts.append(alert)
        
        print(f"    Found {len(service_groups)} service groups")
        print(f"    {len(unmapped_alerts)} unmapped alerts")
        
        # Analyze relationships between alert groups
        self.consolidated_groups = []
        processed_services = set()
        
        for service_name, alerts in service_groups.items():
            if service_name in processed_services:
                continue
            
            # Find related services with alerts
            related_services = self._find_related_alert_services(service_name, service_groups)
            
            # Create consolidated group
            group = {
                'primary_service': service_name,
                'related_services': list(related_services),
                'alerts': alerts.copy(),
                'alert_count': len(alerts),
                'service_count': 1 + len(related_services)
            }
            
            # Add alerts from related services
            for related_svc in related_services:
                if related_svc in service_groups:
                    group['alerts'].extend(service_groups[related_svc])
                    group['alert_count'] += len(service_groups[related_svc])
                    processed_services.add(related_svc)
            
            # Add group metadata
            group['alert_types'] = list(set(a.get('alert_name', '') for a in group['alerts']))
            group['namespaces'] = list(set(a.get('namespace', '') for a in group['alerts'] if a.get('namespace')))
            group['clusters'] = list(set(a.get('cluster', '') for a in group['alerts'] if a.get('cluster')))
            
            self.consolidated_groups.append(group)
            processed_services.add(service_name)
        
        # Handle unmapped alerts - group by namespace/cluster
        if unmapped_alerts:
            unmapped_groups = self._group_unmapped_alerts(unmapped_alerts)
            self.consolidated_groups.extend(unmapped_groups)
    
    def _find_related_alert_services(self, service_name, service_groups, max_depth=1):
        """Find services with alerts that are related to given service"""
        related = set()
        
        if not self.service_graph.has_node(service_name):
            return related
        
        # Get immediate neighbors (depth 1)
        for neighbor in self.service_graph.neighbors(service_name):
            if neighbor in service_groups and neighbor != service_name:
                related.add(neighbor)
        
        # Get reverse neighbors (services calling this service)
        for predecessor in self.service_graph.predecessors(service_name):
            if predecessor in service_groups and predecessor != service_name:
                related.add(predecessor)
        
        return related
    
    def _group_unmapped_alerts(self, unmapped_alerts):
        """Group unmapped alerts by namespace and cluster"""
        groups_dict = {}
        
        for alert in unmapped_alerts:
            namespace = alert.get('namespace', 'unknown')
            cluster = alert.get('cluster', 'unknown')
            key = f"{cluster}:{namespace}"
            
            if key not in groups_dict:
                groups_dict[key] = []
            groups_dict[key].append(alert)
        
        unmapped_groups = []
        for key, alerts in groups_dict.items():
            cluster, namespace = key.split(':', 1)
            group = {
                'primary_service': f'unmapped_{namespace}_{cluster}',
                'related_services': [],
                'alerts': alerts,
                'alert_count': len(alerts),
                'service_count': 0,
                'alert_types': list(set(a.get('alert_name', '') for a in alerts)),
                'namespaces': [namespace],
                'clusters': [cluster],
                'is_unmapped': True
            }
            unmapped_groups.append(group)
        
        pd.DataFrame(unmapped_groups).to_csv('unmapped_alerts.csv', index=False)
        return unmapped_groups
    
    def _create_consolidated_output(self):
        """Create consolidated output dataframe"""
        # Create detailed results
        detailed_results = []
        
        for group_idx, group in enumerate(self.consolidated_groups):
            for alert in group['alerts']:
                result_row = {
                    'group_id': group_idx,
                    'primary_service': group['primary_service'],
                    'related_services_count': len(group['related_services']),
                    'group_alert_count': group['alert_count'],
                    'alert_name': alert.get('alert_name', ''),
                    'severity': alert.get('severity', ''),
                    'status': alert.get('status', ''),
                    'graph_service': alert.get('graph_service', ''),
                    'service_name': alert.get('service_name', ''),
                    'namespace': alert.get('namespace', ''),
                    'cluster': alert.get('cluster', ''),
                    'mapping_method': alert.get('mapping_method', ''),
                    'start_time': alert.get('startsAt', ''),
                    'description': alert.get('description', ''),
                }
                detailed_results.append(result_row)
        
        self.detailed_results_df = pd.DataFrame(detailed_results)
        return self.detailed_results_df
    
    def export_results(self, output_path):
        """Export consolidation results"""
        self.detailed_results_df.to_csv(output_path, index=False)
        print(f"\n✓ Exported consolidated results to: {output_path}")
        
        # Export summary
        summary_path = output_path.replace('.csv', '_summary.csv')
        summary_data = []
        for idx, group in enumerate(self.consolidated_groups):
            summary_data.append({
                'group_id': idx,
                'primary_service': group['primary_service'],
                'related_services_count': len(group['related_services']),
                'total_alerts': group['alert_count'],
                'alert_types': ', '.join(group['alert_types']),
                'namespaces': ', '.join(group['namespaces']),
                'clusters': ', '.join(group['clusters'])
            })
        pd.DataFrame(summary_data).to_csv(summary_path, index=False)
        print(f"✓ Exported summary to: {summary_path}")
    
    def export_mapping_details(self, output_dir='.'):
        """Export detailed mapping information"""
        direct_mapping = []
        fallback_mapping = []
        unmapped = []
        
        for alert in self.enriched_alerts:
            mapping_method = alert.get('mapping_method', '')
            
            row = {
                'alert_name': alert.get('alert_name', ''),
                'service_name': alert.get('service_name', ''),
                'namespace': alert.get('namespace', ''),
                'cluster': alert.get('cluster', ''),
                'graph_service': alert.get('graph_service', ''),
                'mapping_method': mapping_method
            }
            
            if mapping_method == 'service_name':
                direct_mapping.append(row)
            elif mapping_method == 'namespace_cluster_fallback':
                fallback_mapping.append(row)
            else:
                unmapped.append(row)
        
        pd.DataFrame(direct_mapping).to_csv(f'{output_dir}/direct_mapping_details.csv', index=False)
        pd.DataFrame(fallback_mapping).to_csv(f'{output_dir}/fallback_mapping_details.csv', index=False)
        pd.DataFrame(unmapped).to_csv(f'{output_dir}/unmapped_alerts_details.csv', index=False)
        
        print(f"\n✓ Exported mapping details:")
        print(f"  - direct_mapping_details.csv ({len(direct_mapping)} alerts)")
        print(f"  - fallback_mapping_details.csv ({len(fallback_mapping)} alerts)")
        print(f"  - unmapped_alerts_details.csv ({len(unmapped)} alerts)")
    
    def create_hierarchical_super_groups(self):
        """Phase 2: Create hierarchical super-groups"""
        print("\n=== Phase 2: Creating Hierarchical Super-Groups ===")
        
        # Group by alert type patterns
        alert_type_groups = defaultdict(list)
        
        for group in self.consolidated_groups:
            for alert_type in group['alert_types']:
                alert_type_groups[alert_type].append(group)
        
        super_groups = []
        for alert_type, groups in alert_type_groups.items():
            super_group = {
                'alert_type': alert_type,
                'group_count': len(groups),
                'total_alerts': sum(g['alert_count'] for g in groups),
                'affected_services': list(set(g['primary_service'] for g in groups)),
                'groups': groups
            }
            super_groups.append(super_group)
        
        print(f"✓ Created {len(super_groups)} super-groups")
        return super_groups
    
    def export_hierarchical_results(self, super_groups, output_dir='.'):
        """Export hierarchical super-group results"""
        summary_data = []
        drill_down_data = []
        
        for sg in super_groups:
            summary_data.append({
                'alert_type': sg['alert_type'],
                'group_count': sg['group_count'],
                'total_alerts': sg['total_alerts'],
                'affected_services': ', '.join(sg['affected_services'][:5])
            })
            
            for group in sg['groups']:
                drill_down_data.append({
                    'alert_type': sg['alert_type'],
                    'primary_service': group['primary_service'],
                    'related_services_count': len(group['related_services']),
                    'alert_count': group['alert_count']
                })
        
        pd.DataFrame(summary_data).to_csv(f'{output_dir}/hierarchical_super_groups_summary.csv', index=False)
        pd.DataFrame(drill_down_data).to_csv(f'{output_dir}/hierarchical_drill_down.csv', index=False)
        
        print(f"\n✓ Exported hierarchical results:")
        print(f"  - hierarchical_super_groups_summary.csv")
        print(f"  - hierarchical_drill_down.csv")
    
    def print_hierarchical_summary(self, super_groups):
        """Print hierarchical summary"""
        print("\n" + "=" * 70)
        print("PHASE 2: HIERARCHICAL SUPER-GROUPS (Top 10)")
        print("=" * 70)
        
        sorted_super_groups = sorted(super_groups, key=lambda x: x['total_alerts'], reverse=True)
        
        for i, sg in enumerate(sorted_super_groups[:10]):
            print(f"\nSuper-Group {i}: {sg['alert_type']}")
            print(f"  Service Groups: {sg['group_count']}")
            print(f"  Total Alerts: {sg['total_alerts']}")
            print(f"  Affected Services: {', '.join(sg['affected_services'][:3])}")
    
    # ============================================================================
    # PHASE 3: ML-BASED CLUSTERING FOR BEHAVIORAL PATTERNS
    # ============================================================================
    
    def engineer_features_for_clustering(self):
        """Extract comprehensive features from consolidated alerts for clustering"""
        print("\n=== Phase 3: Feature Engineering for Clustering ===")
        
        if self.detailed_results_df is None:
            raise ValueError("No consolidated results available. Run consolidate_alerts() first.")
        
        features_list = []
        
        for idx, alert in self.detailed_results_df.iterrows():
            feature_dict = {}
            
            # 1. Graph Topology Features
            graph_service = alert.get('graph_service', '')
            if pd.notna(graph_service) and graph_service and graph_service in self.service_graph:
                feature_dict['degree_centrality'] = self.service_graph.degree(graph_service)
                feature_dict['in_degree'] = self.service_graph.in_degree(graph_service)
                feature_dict['out_degree'] = self.service_graph.out_degree(graph_service)
                
                # PageRank
                try:
                    if not hasattr(self, '_pagerank_cache'):
                        self._pagerank_cache = nx.pagerank(self.service_graph)
                    feature_dict['pagerank'] = self._pagerank_cache.get(graph_service, 0)
                except:
                    feature_dict['pagerank'] = 0
            else:
                feature_dict['degree_centrality'] = 0
                feature_dict['in_degree'] = 0
                feature_dict['out_degree'] = 0
                feature_dict['pagerank'] = 0
            
            # 2. Alert Metadata Features
            alert_name = str(alert.get('alert_name', '')).lower()
            feature_dict['severity_encoded'] = self._encode_severity(alert.get('severity', ''))
            feature_dict['is_error_alert'] = 1 if 'error' in alert_name else 0
            feature_dict['is_resource_alert'] = 1 if any(x in alert_name for x in ['cpu', 'memory', 'resource', 'hpa']) else 0
            feature_dict['is_latency_alert'] = 1 if 'latency' in alert_name else 0
            feature_dict['is_availability_alert'] = 1 if any(x in alert_name for x in ['down', 'unavailable', 'oom']) else 0
            
            # 3. Service Context Features
            feature_dict['has_namespace'] = 1 if pd.notna(alert.get('namespace')) and alert.get('namespace') != '' else 0
            feature_dict['has_cluster'] = 1 if pd.notna(alert.get('cluster')) and alert.get('cluster') != '' else 0
            feature_dict['mapping_quality'] = 2 if alert.get('mapping_method') == 'service_name' else 1 if alert.get('mapping_method') == 'namespace_cluster_fallback' else 0
            
            # 4. Group Context Features (from consolidation)
            feature_dict['group_size'] = alert.get('group_alert_count', 1)
            feature_dict['related_services_count'] = alert.get('related_services_count', 0)
            feature_dict['group_id'] = alert.get('group_id', -1)
            
            # 5. Temporal features (if available)
            start_time = alert.get('start_time', '')
            if start_time and pd.notna(start_time):
                try:
                    dt = pd.to_datetime(start_time)
                    feature_dict['hour_of_day'] = dt.hour
                    feature_dict['day_of_week'] = dt.dayofweek
                    feature_dict['is_weekend'] = 1 if dt.dayofweek >= 5 else 0
                except:
                    feature_dict['hour_of_day'] = 0
                    feature_dict['day_of_week'] = 0
                    feature_dict['is_weekend'] = 0
            else:
                feature_dict['hour_of_day'] = 0
                feature_dict['day_of_week'] = 0
                feature_dict['is_weekend'] = 0
            
            features_list.append(feature_dict)
        
        # Convert to DataFrame
        features_df = pd.DataFrame(features_list)
        self.feature_names = features_df.columns.tolist()
        
        # Handle any NaN values
        features_df = features_df.fillna(0)
        
        # Store and scale features
        self.feature_matrix = features_df.values
        self.feature_matrix_scaled = self.scaler.fit_transform(self.feature_matrix)
        
        print(f"✓ Created {self.feature_matrix.shape[1]} features for {self.feature_matrix.shape[0]} alerts")
        print(f"  Features: {', '.join(self.feature_names[:10])}...")
        
        return features_df
    
    def _encode_severity(self, severity):
        """Encode severity to numeric value"""
        severity_map = {
            'critical': 4,
            'high': 3,
            'warning': 2,
            'info': 1,
            'unknown': 0
        }
        return severity_map.get(str(severity).lower().strip(), 0)
    
    def apply_clustering_algorithms(self):
        """Apply multiple clustering algorithms and compare"""
        print("\n=== Applying Clustering Algorithms ===")
        
        if self.feature_matrix_scaled is None:
            raise ValueError("Features not engineered. Run engineer_features_for_clustering() first.")
        
        # 1. DBSCAN - Density-based clustering (good for noise)
        print("\n[1/3] DBSCAN Clustering...")
        dbscan = DBSCAN(eps=0.5, min_samples=5, metric='euclidean')
        dbscan_labels = dbscan.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['dbscan'] = {
            'labels': dbscan_labels,
            'n_clusters': len(set(dbscan_labels)) - (1 if -1 in dbscan_labels else 0),
            'n_noise': list(dbscan_labels).count(-1)
        }
        print(f"  ✓ Found {self.clustering_results['dbscan']['n_clusters']} clusters, {self.clustering_results['dbscan']['n_noise']} noise points")
        
        # 2. K-Means - Prototype-based clustering
        print("\n[2/3] K-Means Clustering (auto-selecting k)...")
        best_k = self._find_optimal_k(max_k=20)
        kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
        kmeans_labels = kmeans.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['kmeans'] = {
            'labels': kmeans_labels,
            'n_clusters': best_k,
            'centroids': kmeans.cluster_centers_
        }
        print(f"  ✓ Optimal k={best_k}, created {best_k} clusters")
        
        # 3. Hierarchical Clustering
        print("\n[3/3] Hierarchical Clustering...")
        n_clusters_hier = min(15, max(2, len(self.detailed_results_df) // 10))
        hierarchical = AgglomerativeClustering(n_clusters=n_clusters_hier, linkage='ward')
        hier_labels = hierarchical.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['hierarchical'] = {
            'labels': hier_labels,
            'n_clusters': n_clusters_hier
        }
        print(f"  ✓ Created {n_clusters_hier} hierarchical clusters")
    
    def _find_optimal_k(self, max_k=20):
        """Find optimal number of clusters using elbow method and silhouette score"""
        n_samples = len(self.feature_matrix_scaled)
        max_k = min(max_k, n_samples // 5)
        
        if max_k < 3:
            return 2
        
        silhouette_scores = []
        K_range = range(2, max_k + 1)
        
        for k in K_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(self.feature_matrix_scaled)
            try:
                score = silhouette_score(self.feature_matrix_scaled, labels)
                silhouette_scores.append(score)
            except:
                silhouette_scores.append(0)
        
        # Select k with highest silhouette score
        if silhouette_scores:
            best_k = K_range[np.argmax(silhouette_scores)]
            return best_k
        return 2
    
    def evaluate_clustering_quality(self):
        """Evaluate and compare clustering quality"""
        print("\n=== Clustering Quality Evaluation ===")
        
        evaluation_results = {}
        
        for method, result in self.clustering_results.items():
            labels = result['labels']
            
            # Skip if only one cluster or all noise
            unique_labels = set(labels)
            if len(unique_labels) <= 1 or (len(unique_labels) == 2 and -1 in labels):
                print(f"\n{method.upper()}: Insufficient clusters for evaluation")
                continue
            
            # Filter out noise points for evaluation
            mask = labels != -1
            if mask.sum() < 2:
                continue
            
            filtered_features = self.feature_matrix_scaled[mask]
            filtered_labels = labels[mask]
            
            # Check if we have at least 2 clusters
            if len(set(filtered_labels)) < 2:
                continue
            
            # Calculate metrics
            try:
                silhouette = silhouette_score(filtered_features, filtered_labels)
                davies_bouldin = davies_bouldin_score(filtered_features, filtered_labels)
                calinski_harabasz = calinski_harabasz_score(filtered_features, filtered_labels)
                
                evaluation_results[method] = {
                    'silhouette_score': silhouette,
                    'davies_bouldin_score': davies_bouldin,
                    'calinski_harabasz_score': calinski_harabasz,
                    'n_clusters': result['n_clusters']
                }
                
                print(f"\n{method.upper()}:")
                print(f"  Silhouette Score: {silhouette:.3f} (higher is better)")
                print(f"  Davies-Bouldin Score: {davies_bouldin:.3f} (lower is better)")
                print(f"  Calinski-Harabasz Score: {calinski_harabasz:.1f} (higher is better)")
            except Exception as e:
                print(f"\n{method.upper()}: Error in evaluation - {e}")
                continue
        
        # Select best clustering based on silhouette score
        if evaluation_results:
            best_method = max(evaluation_results.items(), key=lambda x: x[1]['silhouette_score'])[0]
            self.best_clustering = best_method
            print(f"\n✓ Best clustering method: {best_method.upper()}")
        
        return evaluation_results
    
    def extract_cluster_patterns(self, clustering_method=None):
        """Extract interpretable patterns from clusters"""
        if clustering_method is None:
            clustering_method = self.best_clustering or 'kmeans'
        
        print(f"\n=== Extracting Cluster Patterns ({clustering_method}) ===")
        
        labels = self.clustering_results[clustering_method]['labels']
        self.detailed_results_df['cluster_id'] = labels
        
        # Analyze each cluster
        cluster_patterns = {}
        
        for cluster_id in sorted(set(labels)):
            if cluster_id == -1:  # Skip noise
                continue
            
            cluster_alerts = self.detailed_results_df[self.detailed_results_df['cluster_id'] == cluster_id]
            
            if len(cluster_alerts) == 0:
                continue
            
            # Extract patterns
            pattern = {
                'cluster_id': int(cluster_id),
                'size': len(cluster_alerts),
                
                # Most common attributes
                'common_alert_types': cluster_alerts['alert_name'].value_counts().head(3).to_dict() if 'alert_name' in cluster_alerts.columns else {},
                'common_services': cluster_alerts['graph_service'].value_counts().head(5).to_dict() if 'graph_service' in cluster_alerts.columns else {},
                'common_namespaces': cluster_alerts['namespace'].value_counts().head(3).to_dict() if 'namespace' in cluster_alerts.columns else {},
                'common_clusters': cluster_alerts['cluster'].value_counts().head(3).to_dict() if 'cluster' in cluster_alerts.columns else {},
                
                # Severity distribution
                'severity_distribution': cluster_alerts['severity'].value_counts().to_dict() if 'severity' in cluster_alerts.columns else {},
                
                # Mapping quality
                'mapping_methods': cluster_alerts['mapping_method'].value_counts().to_dict() if 'mapping_method' in cluster_alerts.columns else {},
                
                # Dominant characteristics
                'dominant_alert_type': cluster_alerts['alert_name'].mode()[0] if 'alert_name' in cluster_alerts.columns and len(cluster_alerts['alert_name'].mode()) > 0 else 'unknown',
                'dominant_severity': cluster_alerts['severity'].mode()[0] if 'severity' in cluster_alerts.columns and len(cluster_alerts['severity'].mode()) > 0 else 'unknown',
                
                # Feature statistics
                'avg_group_size': float(cluster_alerts['group_alert_count'].mean()) if 'group_alert_count' in cluster_alerts.columns else 0,
                'avg_related_services': float(cluster_alerts['related_services_count'].mean()) if 'related_services_count' in cluster_alerts.columns else 0,
                
                # Service group distribution
                'affected_service_groups': cluster_alerts['group_id'].nunique() if 'group_id' in cluster_alerts.columns else 0,
            }
            
            cluster_patterns[cluster_id] = pattern
            
            print(f"\nCluster {cluster_id} ({len(cluster_alerts)} alerts):")
            print(f"  Dominant Alert: {pattern['dominant_alert_type']}")
            print(f"  Severity: {pattern['dominant_severity']}")
            print(f"  Avg Group Size: {pattern['avg_group_size']:.1f}")
            print(f"  Spans {pattern['affected_service_groups']} service groups")
        
        self.cluster_patterns = cluster_patterns
        return cluster_patterns
    
    def train_pattern_classifier(self, clustering_method=None):
        """Train classifier to assign future alerts to clusters"""
        if clustering_method is None:
            clustering_method = self.best_clustering or 'kmeans'
        
        print(f"\n=== Training Pattern Classifier ===")
        
        labels = self.clustering_results[clustering_method]['labels']
        
        # Filter out noise points
        mask = labels != -1
        X_train = self.feature_matrix_scaled[mask]
        y_train = labels[mask]
        
        if len(set(y_train)) < 2:
            print("  ⚠ Insufficient unique clusters for training classifier")
            return None
        
        # Train Random Forest classifier
        self.pattern_classifier = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        self.pattern_classifier.fit(X_train, y_train)
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.pattern_classifier.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(f"✓ Trained classifier on {len(X_train)} alerts")
        print(f"\nTop 5 Important Features:")
        for idx, row in feature_importance.head(5).iterrows():
            print(f"  {row['feature']}: {row['importance']:.3f}")
        
        return feature_importance
    
    def export_clustering_results(self, output_dir='.'):
        """Export clustering results with both group_id and cluster_id"""
        print("\n=== Exporting Clustering Results ===")
        
        # 1. Export alert cluster assignments (with BOTH group_id and cluster_id)
        cluster_assignments_path = f'{output_dir}/alert_cluster_assignments.csv'
        output_df = self.detailed_results_df.copy()
        
        # Add all clustering results
        for method, result in self.clustering_results.items():
            output_df[f'cluster_{method}'] = result['labels']
        
        output_df.to_csv(cluster_assignments_path, index=False)
        print(f"✓ Alert cluster assignments: {cluster_assignments_path}")
        
        # 2. Export cluster patterns
        if self.cluster_patterns:
            patterns_path = f'{output_dir}/cluster_patterns.json'
            with open(patterns_path, 'w') as f:
                # Convert numpy types to native Python types for JSON serialization
                patterns_serializable = {}
                for cluster_id, pattern in self.cluster_patterns.items():
                    patterns_serializable[str(cluster_id)] = {
                        k: (v.item() if isinstance(v, (np.integer, np.floating)) else 
                            int(v) if isinstance(v, np.integer) else
                            float(v) if isinstance(v, np.floating) else v)
                        for k, v in pattern.items()
                    }
                json.dump(patterns_serializable, f, indent=2)
            print(f"✓ Cluster patterns: {patterns_path}")
        
        # 3. Export cluster summary
        if self.cluster_patterns:
            summary_data = []
            for cluster_id, pattern in self.cluster_patterns.items():
                summary_data.append({
                    'cluster_id': cluster_id,
                    'size': pattern['size'],
                    'dominant_alert_type': pattern['dominant_alert_type'],
                    'dominant_severity': pattern['dominant_severity'],
                    'avg_group_size': pattern['avg_group_size'],
                    'avg_related_services': pattern['avg_related_services'],
                    'affected_service_groups': pattern['affected_service_groups'],
                    'top_services': ', '.join([f"{k}({v})" for k, v in list(pattern['common_services'].items())[:3]])
                })
            
            summary_df = pd.DataFrame(summary_data).sort_values('size', ascending=False)
            summary_path = f'{output_dir}/cluster_summary.csv'
            summary_df.to_csv(summary_path, index=False)
            print(f"✓ Cluster summary: {summary_path}")
        
        print(f"\n✓ All clustering results exported to {output_dir}/")
    
    def visualize_clusters_2d(self, output_dir='.', clustering_method=None):
        """Create 2D visualization of clusters using PCA"""
        if clustering_method is None:
            clustering_method = self.best_clustering or 'kmeans'
        
        print(f"\n=== Creating 2D Cluster Visualization ===")
        
        # Reduce to 2D using PCA
        pca = PCA(n_components=2)
        features_2d = pca.fit_transform(self.feature_matrix_scaled)
        
        labels = self.clustering_results[clustering_method]['labels']
        
        # Create visualization data
        viz_df = pd.DataFrame({
            'x': features_2d[:, 0],
            'y': features_2d[:, 1],
            'cluster': labels,
            'group_id': self.detailed_results_df['group_id'],
            'alert_name': self.detailed_results_df['alert_name'],
            'service': self.detailed_results_df['graph_service'],
            'severity': self.detailed_results_df['severity']
        })
        
        viz_path = f'{output_dir}/cluster_visualization_2d.csv'
        viz_df.to_csv(viz_path, index=False)
        
        print(f"✓ 2D visualization data: {viz_path}")
        print(f"  Variance explained by PC1: {pca.explained_variance_ratio_[0]:.2%}")
        print(f"  Variance explained by PC2: {pca.explained_variance_ratio_[1]:.2%}")
        
        return viz_df
    
    def run_complete_clustering_pipeline(self, output_dir='.'):
        """Run Phase 3: ML-based clustering on consolidated alerts"""
        print("\n" + "="*70)
        print("PHASE 3: ML-BASED BEHAVIORAL CLUSTERING")
        print("="*70)
        
        # Step 1: Engineer features
        self.engineer_features_for_clustering()
        
        # Step 2: Apply clustering algorithms
        self.apply_clustering_algorithms()
        
        # Step 3: Evaluate clustering quality
        evaluation_results = self.evaluate_clustering_quality()
        
        # Step 4: Extract cluster patterns
        self.extract_cluster_patterns()
        
        # Step 5: Train pattern classifier
        feature_importance = self.train_pattern_classifier()
        
        # Step 6: Export results
        self.export_clustering_results(output_dir)
        
        # Step 7: Create visualization
        self.visualize_clusters_2d(output_dir)
        
        print("\n" + "="*70)
        print("CLUSTERING PIPELINE COMPLETE")
        print("="*70)
        
        return {
            'evaluation': evaluation_results,
            'patterns': self.cluster_patterns,
            'feature_importance': feature_importance
        }
    
    
if __name__ == "__main__":
    print("=" * 70)
    print("INTEGRATED ALERT CONSOLIDATION & CLUSTERING PIPELINE")
    print("=" * 70)
    
    # Initialize consolidator
    consolidator = ServiceGraphAlertConsolidator(
        alerts_csv_path='C:/Users/jurat.shayidin/aiops/wip_2/alert_data.csv',  
        graph_json_path='C:/Users/jurat.shayidin/aiops/wip_2/graph_data.json'
    )

    # ========================================================================
    # PHASE 1: SERVICE GRAPH CONSOLIDATION
    # ========================================================================
    print("\n" + "=" * 70)
    print("PHASE 1: SERVICE GRAPH CONSOLIDATION")
    print("=" * 70)
    
    # Run consolidation
    consolidated_groups = consolidator.consolidate_alerts()
    
    # Export consolidated results
    consolidator.export_results('alert_consolidation_results.csv')
    
    # Export detailed mapping information
    consolidator.export_mapping_details('.')
    
    # Phase 2: Create hierarchical super-groups
    super_groups = consolidator.create_hierarchical_super_groups()
    
    # Export hierarchical results
    if super_groups:
        consolidator.export_hierarchical_results(super_groups, output_dir='.')
        consolidator.print_hierarchical_summary(super_groups)
    
    # Print Phase 1 summary
    print("\n" + "=" * 70)
    print("PHASE 1 SUMMARY (Top 20 Groups)")
    print("=" * 70)
    for i, group in enumerate(consolidated_groups[:20]):
        print(f"\nGroup {i}:")
        print(f"  Primary Service: {group['primary_service']}")
        print(f"  Related Services: {len(group['related_services'])}")
        print(f"  Total Alerts: {group['alert_count']}")
        print(f"  Alert Types: {', '.join(group['alert_types'][:3])}")
    
    # ========================================================================
    # PHASE 3: ML-BASED BEHAVIORAL CLUSTERING
    # ========================================================================
    print("\n" + "=" * 70)
    print("RUNNING PHASE 3: ML-BASED CLUSTERING...")
    print("=" * 70)
    
    try:
        clustering_results = consolidator.run_complete_clustering_pipeline(output_dir='.')
        
        # Print cluster insights
        print("\n" + "=" * 70)
        print("CLUSTERING INSIGHTS")
        print("=" * 70)
        
        if consolidator.cluster_patterns:
            print(f"\n✓ Discovered {len(consolidator.cluster_patterns)} behavioral patterns")
            print("\nTop Patterns:")
            sorted_patterns = sorted(consolidator.cluster_patterns.items(), 
                                    key=lambda x: x[1]['size'], reverse=True)
            
            for cluster_id, pattern in sorted_patterns[:5]:
                print(f"\n  Cluster {cluster_id}: {pattern['dominant_alert_type']}")
                print(f"    Size: {pattern['size']} alerts")
                print(f"    Severity: {pattern['dominant_severity']}")
                print(f"    Spans: {pattern['affected_service_groups']} service groups")
                print(f"    Top Services: {list(pattern['common_services'].keys())[:3]}")
        
    except Exception as e:
        print(f"\n⚠ Clustering failed: {e}")
        print("  Continuing with Phase 1 results only...")
    
    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    print("\n" + "=" * 70)
    print("ALL FILES GENERATED:")
    print("=" * 70)
    
    print("\n📊 Phase 1 - Service Topology Grouping:")
    print("  ✓ alert_consolidation_results.csv (detailed with group_id)")
    print("  ✓ alert_consolidation_results_summary.csv (summary)")
    
    print("\n🔍 Mapping Details:")
    print("  ✓ direct_mapping_details.csv (direct matches)")
    print("  ✓ fallback_mapping_details.csv (fallback matches)")
    print("  ✓ unmapped_alerts_details.csv (unmapped alerts)")
    
    print("\n📈 Phase 2 - Hierarchical Grouping:")
    print("  ✓ hierarchical_super_groups_summary.csv (super-groups by alert type)")
    print("  ✓ hierarchical_drill_down.csv (super-group drill-down)")
    
    print("\n🤖 Phase 3 - ML Clustering (Behavioral Patterns):")
    print("  ✓ alert_cluster_assignments.csv (with BOTH group_id AND cluster_id)")
    print("  ✓ cluster_patterns.json (detailed pattern definitions)")
    print("  ✓ cluster_summary.csv (cluster statistics)")
    print("  ✓ cluster_visualization_2d.csv (2D PCA visualization data)")
    
    print("\n" + "=" * 70)
    print("🎯 KEY OUTPUT: alert_cluster_assignments.csv")
    print("=" * 70)
    print("Each alert now has:")
    print("  • group_id   → Service topology grouping (Phase 1)")
    print("  • cluster_id → Behavioral pattern clustering (Phase 3)")
    print("\nUse BOTH for smart alert routing and root cause analysis!")
    print("=" * 70)
