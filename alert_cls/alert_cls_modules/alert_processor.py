"""
Alert Processor:
    data processing logic for alert enrichment, consolidation, feature engineering,
    deduplication, and ranking.
"""

import pandas as pd
import numpy as np
import networkx as nx
import re
from collections import Counter
from typing import Dict, List, Set, Tuple
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest


class AlertProcessor:
    """
    Core alert processing pipeline:
    1. Enrich alerts with graph topology features
    2. Initial grouping by service relationships
    3. Feature engineering (graph + alert metadata)
    4. Deduplication
    5. Cluster ranking and naming
    """
    
    # Configuration 
    TIME_WINDOW_MINUTES = 15  # Time window for duplicate detection (15 min workflow)
    MIN_MATCH_SCORE = 2  # Minimum score for fallback mapping
    MIN_CLUSTERING_SAMPLES = 10  # Minimum alerts needed for clustering
    PCA_VARIANCE_THRESHOLD = 0.95  # Retain 95% variance
    OUTLIER_CONTAMINATION = 0.05  # Expect 5% outliers
    
    def __init__(self, service_graph: nx.DiGraph, service_to_graph: Dict,
                 service_features_cache: Dict, pagerank_cache: Dict):
        self.service_graph = service_graph
        self.service_to_graph = service_to_graph
        self._service_features_cache = service_features_cache
        self._pagerank_cache = pagerank_cache
        
        # Processing state
        self.enriched_alerts = []
        self.consolidated_groups = []
        self.alerts_df = None
        self.feature_matrix = None
        self.feature_matrix_scaled = None
        self.feature_names = []
        self.scaler = StandardScaler()
        self.pca = None
        
        # Outlier detection
        self.outlier_indices = set()
        self.outlier_removal_enabled = True
        self._idx_mapping = None 
        
        # Deduplication
        self.deduplicated_alerts = []
        self.duplicate_groups = []
    
    # ========================================================================
    # PHASE 1: ALERT ENRICHMENT
    # ========================================================================
    
    def enrich_alerts(self, alerts: List[Dict]) -> List[Dict]:
        """Enrich alerts with graph information and dependencies"""
        
        mapped_count = 0
        fallback_count = 0
        unmapped_count = 0
        
        for alert in alerts:
            is_mapped = self._enrich_alert_with_graph_info(alert)
            
            if is_mapped:
                mapping_method = alert.get('mapping_method', '')
                if mapping_method == 'service_name':
                    mapped_count += 1
                else:
                    fallback_count += 1
                    
                graph_service = alert.get('graph_service')
                if graph_service:
                    alert['dependencies'] = self._get_service_dependencies(graph_service)
            else:
                unmapped_count += 1
        
        self.enriched_alerts = alerts
        return self.enriched_alerts
    
    def _enrich_alert_with_graph_info(self, alert: Dict) -> bool:
        """Map alert to graph service using service_name, with fallback to node/namespace/cluster"""
        service_name = alert.get('service_name', '')
        
        # Primary mapping: direct service_name match
        if service_name and service_name in self.service_to_graph:
            alert['graph_service'] = service_name
            alert['graph_info'] = self.service_to_graph[service_name]
            alert['mapping_method'] = 'service_name'
            alert['mapping_confidence'] = 1.0
            return True
        
        # Fallback mapping: match by namespace + cluster combination
        alert_namespace = alert.get('namespace', '')
        alert_cluster = alert.get('cluster', '')
        alert_node = alert.get('node', '')
        
        matched_services = []
        for svc_name, svc_info in self.service_to_graph.items():
            match_score = 0
            
            if alert_namespace and svc_info.get('namespace') == alert_namespace:
                match_score += 2
            if alert_cluster and svc_info.get('cluster') == alert_cluster:
                match_score += 2
            if alert_node and svc_info.get('properties', {}).get('node') == alert_node:
                match_score += 1
            
            if match_score >= self.MIN_MATCH_SCORE:
                matched_services.append((svc_name, svc_info, match_score))
        
        if matched_services:
            matched_services.sort(key=lambda x: x[2], reverse=True)
            best_match = matched_services[0]
            
            alert['graph_service'] = best_match[0]
            alert['graph_info'] = best_match[1]
            alert['mapping_method'] = 'namespace_cluster_fallback'
            alert['match_score'] = best_match[2]
            alert['mapping_confidence'] = best_match[2] / 5.0
            return True
        
        # No mapping found
        alert['graph_service'] = None
        alert['graph_info'] = None
        alert['mapping_method'] = 'unmapped'
        alert['mapping_confidence'] = 0.0
        return False
    
    def _get_service_dependencies(self, service_name: str) -> Dict:
        """Get upstream and downstream dependencies for a service"""
        dependencies = {
            'upstream': [],
            'downstream': [],
            'peers': []
        }
        
        if not self.service_graph.has_node(service_name):
            return dependencies
        
        # find upstream dependencies
        for predecessor in self.service_graph.predecessors(service_name):
            edge_data = self.service_graph.get_edge_data(predecessor, service_name)
            dependencies['upstream'].append({
                'service': predecessor,
                'relationship': edge_data.get('relationship_type', '') if edge_data else ''
            })
        
        # find downstream dependencies
        for successor in self.service_graph.successors(service_name):
            edge_data = self.service_graph.get_edge_data(service_name, successor)
            dependencies['downstream'].append({
                'service': successor,
                'relationship': edge_data.get('relationship_type', '') if edge_data else ''
            })
        
        return dependencies
    
    # ========================================================================
    # PHASE 2: INITIAL CONSOLIDATION
    # ========================================================================
    
    def group_alerts_by_relationships(self) -> List[Dict]:
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
        
        # Create consolidated groups
        self.consolidated_groups = []
        processed_services = set()
        
        for service_name, alerts in service_groups.items():
            if service_name in processed_services:
                continue
            
            # Find related services with alerts
            related_services = self._find_related_alert_services(service_name, service_groups)
            
            # Create consolidated group
            group = {
                'group_id': len(self.consolidated_groups),
                'primary_service': service_name,
                'related_services': list(related_services),
                'alerts': alerts.copy(),
                'alert_count': len(alerts),
                'service_count': 1 + len(related_services),
                'grouping_method': 'graph_relationships_correlated'
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
            group['correlation_type'] = 'immediate_neighbors' if len(related_services) <= 2 else 'transitive_sinks'
            
            self.consolidated_groups.append(group)
            processed_services.add(service_name)
        
        # Handle unmapped alerts
        if unmapped_alerts:
            unmapped_groups = self._group_unmapped_alerts(unmapped_alerts)
            self.consolidated_groups.extend(unmapped_groups)
        
        # Assign initial group IDs
        for i, group in enumerate(self.consolidated_groups):
            group['group_id'] = i
            severities = [a.get('severity', 'unknown') for a in group['alerts']]
            alert_categories = [a.get('alert_category', 'unknown') for a in group['alerts']]
            alert_subcategories = [a.get('alert_subcategory', 'unknown') for a in group['alerts']]
            
            group['severity_distribution'] = dict(Counter(severities))
            group['category_distribution'] = dict(Counter(alert_categories))
            group['subcategory_distribution'] = dict(Counter(alert_subcategories))
            
            # Assign initial group ID to alerts
            for alert in group['alerts']:
                alert['initial_group_id'] = i
        
        print(f"  Created {len(self.consolidated_groups)} initial consolidated groups")
        return self.consolidated_groups
    
    def _find_related_alert_services(self, service_name: str, service_groups: Dict, max_depth: int = 1) -> Set:
        """Find services with alerts that are related to given service"""
        related = set()
        
        if not self.service_graph.has_node(service_name):
            return related
        
        # find immediate neighbors
        for neighbor in self.service_graph.neighbors(service_name):
            if neighbor in service_groups and neighbor != service_name:
                related.add(neighbor)
        
        # find reverse neighbors
        for predecessor in self.service_graph.predecessors(service_name):
            if predecessor in service_groups and predecessor != service_name:
                related.add(predecessor)
        
        # Find services that share transitive parent sinks
        parent_sinks1 = self._get_transitive_parent_services(service_name, max_depth=3)
        
        for other_service in service_groups.keys():
            if other_service == service_name or other_service in related:
                continue
            
            parent_sinks2 = self._get_transitive_parent_services(other_service, max_depth=3)
            
            if parent_sinks1 and parent_sinks2 and (parent_sinks1 & parent_sinks2):
                related.add(other_service)
            
            # Check path similarity
            similarity = self._compute_path_similarity(service_name, other_service)
            if similarity > 0.3:
                related.add(other_service)
        
        return related
    
    def _get_transitive_parent_services(self, service_name: str, max_depth: int = 3) -> Set:
        """Get transitive upstream services (all parent services up to max_depth)"""
        parent_set = set()
        
        if not self.service_graph.has_node(service_name):
            return parent_set
        
        # BFS to collect upstream services
        visited = set()
        queue = [(service_name, 0)]
        
        while queue:
            current, depth = queue.pop(0)
            visited.add(current)
            
            if depth >= max_depth:
                continue
            
            for predecessor in self.service_graph.predecessors(current):
                if predecessor not in visited and predecessor != service_name:
                    parent_set.add(predecessor)
                    queue.append((predecessor, depth + 1))
        
        return parent_set
    
    def _compute_path_similarity(self, service1: str, service2: str) -> float:
        """Compute similarity between two services based on their graph paths"""
        if service1 == service2:
            return 1.0
        
        parents1 = self._get_transitive_parent_services(service1)
        parents2 = self._get_transitive_parent_services(service2)
        
        if not parents1 or not parents2:
            return 0.0
        
        # compute Jaccard similarity
        intersection = len(parents1 & parents2)
        union = len(parents1 | parents2)
        
        return intersection / union if union > 0 else 0.0
    
    def _group_unmapped_alerts(self, unmapped_alerts: List[Dict]) -> List[Dict]:
        """Group unmapped alerts by cluster, namespace, node, or combination"""
        groups_dict = {}
        
        for alert in unmapped_alerts:
            namespace = alert.get('namespace', 'unknown')
            cluster = alert.get('cluster', 'unknown')
            node = alert.get('node', 'unknown')
            
            if cluster != 'unknown' and namespace != 'unknown':
                key = f"cluster_ns:{cluster}:{namespace}"
            elif namespace != 'unknown' and node != 'unknown':
                key = f"ns_node:{namespace}:{node}"
            elif cluster != 'unknown':
                key = f"cluster:{cluster}"
            elif namespace != 'unknown':
                key = f"namespace:{namespace}"
            elif node != 'unknown':
                key = f"node:{node}"
            else:
                key = "unknown:unknown"
            
            if key not in groups_dict:
                groups_dict[key] = []
            groups_dict[key].append(alert)
        
        unmapped_groups = []
        for key, alerts in groups_dict.items():
            parts = key.split(':', 1)
            group_type = parts[0] if len(parts) > 0 else 'unknown'
            group_value = parts[1] if len(parts) > 1 else 'unknown'
            
            group = {
                'group_id': -1,
                'primary_service': f'unmapped_{group_type}_{group_value}',
                'related_services': [],
                'alerts': alerts,
                'alert_count': len(alerts),
                'service_count': 0,
                'alert_types': list(set(a.get('alert_name', '') for a in alerts)),
                'namespaces': list(set(a.get('namespace', '') for a in alerts if a.get('namespace'))),
                'clusters': list(set(a.get('cluster', '') for a in alerts if a.get('cluster'))),
                'nodes': list(set(a.get('node', '') for a in alerts if a.get('node'))),
                'is_unmapped': True,
                'grouping_method': f'unmapped_{group_type}',
                'grouping_key': key
            }
            unmapped_groups.append(group)
        
        return unmapped_groups
    
    # ========================================================================
    # PHASE 3: FEATURE ENGINEERING
    # ========================================================================
    
    def engineer_features(self) -> Tuple[np.ndarray, StandardScaler, PCA, List[str]]:
        """Extract comprehensive features for clustering"""
        self.alerts_df = pd.DataFrame(self.enriched_alerts)
        
        # Pre-define all graph feature keys
        graph_feature_keys = ['degree_total', 'in_degree', 'out_degree', 'pagerank', 'betweenness', 
                             'clustering_coef', 'num_upstream', 'num_downstream',
                             'upstream_calls', 'upstream_owns', 'upstream_belongs_to',
                             'downstream_calls', 'downstream_owns', 'downstream_belongs_to',
                             'ratio_calls', 'ratio_owns', 'ratio_belongs_to',
                             'dependency_direction', 'avg_neighbor_degree', 'max_neighbor_degree']
        
        features_list = []
        
        for row in self.alerts_df.itertuples():
            feature_dict = {}
            
            # Get graph service from row
            graph_service = getattr(row, 'graph_service', '')
            
            # Use pre-computed graph features if available
            if pd.notna(graph_service) and graph_service and graph_service in self._service_features_cache:
                cached_features = self._service_features_cache[graph_service]
                for key in graph_feature_keys:
                    feature_dict[key] = cached_features.get(key, 0)
            else:
                for key in graph_feature_keys:
                    feature_dict[key] = 0
            
            # ALERT METADATA FEATURES
            alert_name = str(getattr(row, 'alert_name', '')).lower()
            feature_dict['severity_encoded'] = self._encode_severity(getattr(row, 'severity', ''))
            feature_dict['is_error_alert'] = 1 if 'error' in alert_name else 0
            feature_dict['is_resource_alert'] = 1 if any(x in alert_name for x in ['cpu', 'memory', 'hpa', 'resource']) else 0
            feature_dict['is_network_alert'] = 1 if any(x in alert_name for x in ['network', 'rx_bytes', 'tx_bytes']) else 0
            feature_dict['is_anomaly_alert'] = 1 if 'anomaly' in str(getattr(row, 'alert_category', '')).lower() else 0
            
            feature_dict['alert_category_encoded'] = self._encode_alert_category(getattr(row, 'alert_category', ''))
            feature_dict['alert_subcategory_encoded'] = self._encode_alert_subcategory(getattr(row, 'alert_subcategory', ''))
            feature_dict['workload_type_encoded'] = self._encode_workload_type(getattr(row, 'workload_type', ''))
            
            # Temporal features
            start_dt = getattr(row, 'start_datetime', None)
            if start_dt and pd.notna(start_dt):
                feature_dict['hour_of_day'] = start_dt.hour
                feature_dict['day_of_week'] = start_dt.dayofweek
                feature_dict['is_business_hours'] = 1 if 9 <= start_dt.hour <= 17 else 0
                feature_dict['is_weekend'] = 1 if start_dt.dayofweek >= 5 else 0
            else:
                feature_dict['hour_of_day'] = 0
                feature_dict['day_of_week'] = 0
                feature_dict['is_business_hours'] = 0
                feature_dict['is_weekend'] = 0
            
            # Category-Subcategory combination features
            category = str(getattr(row, 'alert_category', '')).lower().strip()
            subcategory = str(getattr(row, 'alert_subcategory', '')).lower().strip()
            
            feature_dict['is_critical_resource'] = 1 if (category in ['critical', 'failure'] and subcategory == 'resource') else 0
            feature_dict['is_saturation_memory'] = 1 if (category == 'saturation' and subcategory == 'memory') else 0
            feature_dict['is_saturation_cpu'] = 1 if (category == 'saturation' and subcategory == 'cpu') else 0
            feature_dict['is_error_node'] = 1 if (category == 'error' and subcategory == 'node') else 0
            feature_dict['is_anomaly_latency'] = 1 if (category == 'anomaly' and subcategory == 'latency') else 0
            feature_dict['is_slo_violation'] = 1 if category == 'slo' else 0
            
            feature_dict['mapping_confidence'] = getattr(row, 'mapping_confidence', 0)
            
            features_list.append(feature_dict)
        
        features_df = pd.DataFrame(features_list)
        self.feature_names = features_df.columns.tolist()
        features_df = features_df.fillna(0)
        
        # Store and scale features
        self.feature_matrix = features_df.values
        self.feature_matrix_scaled = self.scaler.fit_transform(self.feature_matrix)

        # Remove outliers 
        if self.outlier_removal_enabled and len(self.feature_matrix_scaled) > 20:
            self._remove_outliers()
        else:
            self._idx_mapping = {i: i for i in range(len(self.alerts_df))}
        
        # Apply PCA for dimensionality reduction
        self._apply_pca()
        
        return self.feature_matrix_scaled, self.scaler, self.pca, self.feature_names
    
    def _encode_severity(self, severity: str) -> int:
        """Encode severity to numeric value"""
        severity_map = {
            'critical': 4,
            'high': 3,
            'warning': 2,
            'info': 1,
            'unknown': 0
        }
        return severity_map.get(str(severity).lower().strip(), 0)
    
    def _encode_workload_type(self, workload_type: str) -> int:
        """Encode workload type to numeric value"""
        workload_map = {
            'deployment': 1,
            'daemonset': 2,
            'statefulset': 3,
            'job': 4,
            'cronjob': 5,
            'pod': 6,
            'unknown': 0
        }
        return workload_map.get(str(workload_type).lower().strip(), 0)
    
    def _encode_alert_category(self, category: str) -> int:
        """Encode alert category to numeric value"""
        category_map = {
            'saturation': 1,
            'anomaly': 2,
            'error': 3,
            'critical': 4,
            'failure': 5,
            'slo': 6,
            'unknown': 0
        }
        return category_map.get(str(category).lower().strip(), 0)
    
    def _encode_alert_subcategory(self, subcategory: str) -> int:
        """Encode alert subcategory to numeric value"""
        subcategory_map = {
            'hpa': 1,
            'resource': 2,
            'error': 3,
            'node': 4,
            'memory': 5,
            'latency': 6,
            'other': 7,
            'volume': 8,
            'cpu': 9,
            'unknown': 0
        }
        return subcategory_map.get(str(subcategory).lower().strip(), 0)
    
    def _remove_outliers(self):
        """Remove outliers using Isolation Forest"""
        try:
            isolation_forest = IsolationForest(
                contamination=self.OUTLIER_CONTAMINATION,
                random_state=42,
                n_estimators=100
            )
            
            outlier_labels = isolation_forest.fit_predict(self.feature_matrix_scaled)
            self.outlier_indices = set(np.where(outlier_labels == -1)[0])
            
            n_outliers = len(self.outlier_indices)
            n_total = len(self.feature_matrix_scaled)
            outlier_pct = (n_outliers / n_total) * 100
            
            print(f" Detected {n_outliers} outliers ({outlier_pct:.1f}% of data)")
            
            if n_outliers > 0:
                self.feature_matrix_scaled = np.delete(self.feature_matrix_scaled, 
                                                       list(self.outlier_indices), axis=0)
                
                valid_indices = [i for i in range(len(self.alerts_df)) if i not in self.outlier_indices]
                self.alerts_df = self.alerts_df.iloc[valid_indices].reset_index(drop=True)
                
                print(f" Removed {n_outliers} outliers, remaining: {len(self.feature_matrix_scaled)}")
            else:
                valid_indices = list(range(len(self.alerts_df)))
            
            self._idx_mapping = {new_idx: orig_idx 
                                 for new_idx, orig_idx in enumerate(valid_indices)}
            
        except Exception as e:
            self.outlier_indices = set()
            self._idx_mapping = {i: i for i in range(len(self.alerts_df))}
    
    def _apply_pca(self):
        """Apply PCA for dimensionality reduction"""
        try:
            pca = PCA(n_components=self.PCA_VARIANCE_THRESHOLD, random_state=42)
            self.feature_matrix_pca = pca.fit_transform(self.feature_matrix_scaled)
            
            n_components = pca.n_components_
            variance_explained = pca.explained_variance_ratio_.sum()
            
            self.pca = pca
            self.feature_matrix_scaled = self.feature_matrix_pca
            
        except Exception as e:
            self.pca = None
            self.feature_matrix_pca = None
    
    # ========================================================================
    # PHASE 4: ASSIGN CLUSTER LABELS (from trained model)
    # ========================================================================
    
    def assign_cluster_labels(self, cluster_labels: np.ndarray, clustering_method: str):
        """Assign cluster labels from trained model to alerts"""
        self.alerts_df['cluster_id'] = cluster_labels
        self.alerts_df['clustering_method'] = clustering_method
        
        # Update enriched alerts
        reverse_mapping = {}
        if self._idx_mapping:
            for new_idx, orig_idx in self._idx_mapping.items():
                reverse_mapping[orig_idx] = new_idx
        
        for i, alert in enumerate(self.enriched_alerts):
            if i in self.outlier_indices:
                alert['cluster_id'] = -1
                alert['clustering_method'] = 'outlier_removed'
            else:
                if self._idx_mapping and i in reverse_mapping:
                    new_idx = reverse_mapping[i]
                    alert['cluster_id'] = int(cluster_labels[new_idx])
                    alert['clustering_method'] = clustering_method
                elif not self._idx_mapping:
                    alert['cluster_id'] = int(cluster_labels[i])
                    alert['clustering_method'] = clustering_method
                else:
                    alert['cluster_id'] = -1
                    alert['clustering_method'] = 'unmapped'
    
    # ========================================================================
    # PHASE 5: DEDUPLICATION
    # ========================================================================
    
    def deduplicate_alerts(self) -> List[Dict]:
        """Deduplicate alerts within clusters based on similarity"""

        self.deduplicated_alerts = []
        self.duplicate_groups = []
        
        # Group by cluster
        clusters = self.alerts_df.groupby('cluster_id')
        
        total_duplicates = 0
        
        for cluster_id, cluster_df in clusters:
            if cluster_id == -1:
                # Add all noise points without deduplication
                for idx in cluster_df.index:
                    orig_idx = self._idx_mapping[idx] if self._idx_mapping else idx
                    alert = self.enriched_alerts[orig_idx]
                    alert['is_duplicate'] = False
                    alert['duplicate_of'] = None
                    self.deduplicated_alerts.append(alert)
                continue
            
            # Find duplicates within cluster
            processed_indices = set()
            
            for idx in cluster_df.index:
                if idx in processed_indices:
                    continue
                
                orig_idx = self._idx_mapping[idx] if self._idx_mapping else idx
                alert = self.enriched_alerts[orig_idx]
                alert['is_duplicate'] = False
                alert['duplicate_of'] = None
                
                # Find similar alerts
                duplicates = []
                
                for other_idx in cluster_df.index:
                    if other_idx <= idx or other_idx in processed_indices:
                        continue
                    
                    orig_other_idx = self._idx_mapping[other_idx] if self._idx_mapping else other_idx
                    other_alert = self.enriched_alerts[orig_other_idx]
                    
                    if self._are_duplicates(alert, other_alert):
                        duplicates.append(orig_other_idx)
                        processed_indices.add(other_idx)
                        
                        other_alert['is_duplicate'] = True
                        other_alert['duplicate_of'] = orig_idx
                        total_duplicates += 1
                
                self.deduplicated_alerts.append(alert)
                
                if duplicates:
                    self.duplicate_groups.append({
                        'representative_idx': orig_idx,
                        'duplicate_indices': duplicates,
                        'count': len(duplicates) + 1
                    })
                
                processed_indices.add(idx)
        
        return self.deduplicated_alerts
    
    def _are_duplicates(self, alert1: Dict, alert2: Dict) -> bool:
        """Check if two alerts are duplicates based on service dependencies and time window"""
        # method 1: Must be close in time
        time1 = alert1.get('start_timestamp', 0)
        time2 = alert2.get('start_timestamp', 0)
        
        if abs(time1 - time2) > self.TIME_WINDOW_MINUTES * 60:
            return False
        
        service1 = alert1.get('graph_service')
        service2 = alert2.get('graph_service')
        
        if not service1 or not service2:
            return self._are_duplicates_basic(alert1, alert2)
        
        # method 2: Same service → duplicates
        if service1 == service2:
            return True
        
        # method 3: Share common upstream/downstream dependencies
        deps1 = self._get_service_dependencies(service1)
        deps2 = self._get_service_dependencies(service2)
        
        upstream1 = {d['service'] for d in deps1['upstream']}
        upstream2 = {d['service'] for d in deps2['upstream']}
        downstream1 = {d['service'] for d in deps1['downstream']}
        downstream2 = {d['service'] for d in deps2['downstream']}
        
        if upstream1 and upstream2 and upstream1 & upstream2:
            return True
        
        if downstream1 and downstream2 and downstream1 & downstream2:
            return True
        
        # method 4: Eventually sink to same services
        transitive_down1 = self._get_transitive_downstream(service1)
        transitive_down2 = self._get_transitive_downstream(service2)
        
        if transitive_down1 and transitive_down2 and transitive_down1 & transitive_down2:
            return True
        
        return False
    
    def _are_duplicates_basic(self, alert1: Dict, alert2: Dict) -> bool:
        """Basic duplicate detection for unmapped alerts"""
        if (alert1.get('alert_name') == alert2.get('alert_name') and
            alert1.get('pod') == alert2.get('pod') and
            alert1.get('pod')):
            return True
        
        if (alert1.get('service_name') == alert2.get('service_name') and
            alert1.get('pod') == alert2.get('pod') and
            alert1.get('service_name')):
            return True
        
        return False
    
    def _get_transitive_downstream(self, service_name: str, max_depth: int = 2) -> Set:
        """Get transitive downstream services"""
        downstream_set = set()
        
        if not self.service_graph.has_node(service_name):
            return downstream_set
        
        visited = set()
        queue = [(service_name, 0)]
        
        while queue:
            current, depth = queue.pop(0)
            visited.add(current)
            
            if depth >= max_depth:
                continue
            
            for successor in self.service_graph.successors(current):
                if successor not in visited:
                    downstream_set.add(successor)
                    queue.append((successor, depth + 1))
        
        return downstream_set
    
    # ========================================================================
    # PHASE 6: RANKING AND NAMING
    # ========================================================================
    
    def rank_and_name_clusters(self) -> List[Dict]:
        """Rank clusters and assign distinctive names"""
        print("\n[Alert Processor] Naming and ranking clusters...")
        
        # Group by cluster_id
        cluster_dict = {}
        for alert in self.enriched_alerts:
            cluster_id = alert.get('cluster_id', -1)
            if cluster_id not in cluster_dict:
                cluster_dict[cluster_id] = []
            cluster_dict[cluster_id].append(alert)
        
        # Calculate scores and generate names
        cluster_metadata = []
        
        for cluster_id, cluster_alerts in cluster_dict.items():
            if cluster_id == -1:
                continue
            
            cluster_name = self._generate_cluster_name(cluster_alerts)
            score = self._calculate_cluster_score(cluster_alerts, cluster_id)
            
            alert_types = [a.get('alert_name', '') for a in cluster_alerts]
            services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
            categories = [a.get('alert_category', '') for a in cluster_alerts if a.get('alert_category')]
            
            cluster_metadata.append({
                'cluster_id': cluster_id,
                'cluster_name': cluster_name,
                'ranking_score': round(score, 2),
                'alert_count': len(cluster_alerts),
                'unique_alert_types': len(set(alert_types)),
                'unique_services': len(set(services)),
                'primary_service': Counter(services).most_common(1)[0][0] if services else '',
                'most_common_category': Counter(categories).most_common(1)[0][0] if categories else '',
            })
        
        # Sort by ranking score
        cluster_metadata.sort(key=lambda x: x['ranking_score'], reverse=True)
        
        # Assign ranks
        for rank, metadata in enumerate(cluster_metadata, start=1):
            metadata['rank'] = rank
            
            cluster_id = metadata['cluster_id']
            for alert in cluster_dict[cluster_id]:
                alert['cluster_rank'] = rank
                alert['cluster_name'] = metadata['cluster_name']
                alert['cluster_score'] = metadata['ranking_score']

        for i, meta in enumerate(cluster_metadata[:5], 1):
            print(f"      {i}. {meta['cluster_name']} (Score: {meta['ranking_score']:.1f}, {meta['alert_count']} alerts)")
        
        return cluster_metadata
    
    # todo: rethink cluster naming business logic
    def _generate_cluster_name(self, cluster_alerts: List[Dict]) -> str:
        """Generate a distinctive name for the cluster"""
        if not cluster_alerts:
            return "Empty_Cluster"
        
        alert_types = [a.get('alert_name', '') for a in cluster_alerts]
        services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
        categories = [a.get('alert_category', '') for a in cluster_alerts if a.get('alert_category')]
        severities = [a.get('severity', '').lower() for a in cluster_alerts]
        
        if alert_types:
            most_common_alert = Counter(alert_types).most_common(1)[0][0]
            alert_keywords = re.sub(r'[^a-zA-Z0-9\s]', '', most_common_alert).split()[:3]
            alert_prefix = '_'.join(alert_keywords[:2]).lower()
        else:
            alert_prefix = "unknown_alert"
        
        if services:
            most_common_service = Counter(services).most_common(1)[0][0]
            service_parts = most_common_service.replace(':', '_').split('_')
            service_key = service_parts[-1][:15].lower()
        else:
            service_key = "unmapped"
        
        if categories:
            most_common_cat = Counter(categories).most_common(1)[0][0]
            cat_key = most_common_cat.lower()[:12]
        else:
            cat_key = "general"
        
        severity_counts = Counter(severities)
        if 'critical' in severity_counts and severity_counts['critical'] > len(cluster_alerts) * 0.5:
            severity_key = "critical"
        elif 'high' in severity_counts and severity_counts['high'] > len(cluster_alerts) * 0.5:
            severity_key = "high"
        else:
            severity_key = "mixed"
        
        cluster_name = f"{alert_prefix}_{service_key}_{cat_key}_{severity_key}"
        cluster_name = re.sub(r'_+', '_', cluster_name)
        cluster_name = cluster_name.strip('_')
        
        return cluster_name
    
    # todo: rethink cluster ranking business logic
    def _calculate_cluster_score(self, cluster_alerts: List[Dict], cluster_id: int) -> float:
        """Calculate ranking score for a cluster"""
        score = 0
        
        if not cluster_alerts:
            return 0
        
        # Repetition score (60%)
        alert_types = [a.get('alert_name', '') for a in cluster_alerts]
        if alert_types:
            unique_types = len(set(alert_types))
            total_alerts = len(alert_types)
            repetition_score = (1 - unique_types / total_alerts) * 100
            score += repetition_score * 0.6 # 60% weight
        
        # Severity impact (25%)
        severities = [a.get('severity', '').lower() for a in cluster_alerts]
        severity_weights = {'critical': 5, 'high': 4, 'warning': 2, 'info': 1}
        avg_severity = sum(severity_weights.get(s, 0) for s in severities) / len(severities)
        score += avg_severity * 5
        
        # Cluster size (20%)    # todo: try different size thresholds
        size_score = min(len(cluster_alerts) / 20, 1.0) * 100
        score += size_score * 0.2
        
        # Service importance (15%)    # todo: try different importance thresholds
        services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
        if services and self._pagerank_cache:
            pageranks = [self._pagerank_cache.get(s, 0) for s in set(services)]
            if pageranks:
                avg_pagerank = np.mean(pageranks) * 1000
                score += min(avg_pagerank, 100) * 0.15
        
        # Time concentration (10%)    # todo: try different time thresholds
        timestamps = [a.get('start_timestamp', 0) for a in cluster_alerts if a.get('start_timestamp')]
        if timestamps and len(timestamps) > 1:
            time_span = max(timestamps) - min(timestamps)
            if time_span > 0:
                concentration_score = 100 / (1 + time_span / 3600)
                score += concentration_score * 0.1
        
        return score

