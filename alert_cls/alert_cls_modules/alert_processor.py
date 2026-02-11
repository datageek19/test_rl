
import pandas as pd
import numpy as np
import networkx as nx
from collections import Counter
from typing import Dict, List, Set, Tuple
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from config import Config

class AlertProcessor:
    
    PCA_VARIANCE_THRESHOLD = Config.PCA_VARIANCE_THRESHOLD
    OUTLIER_CONTAMINATION = Config.OUTLIER_CONTAMINATION
    SERVICE_GRAPH_MAX_DEPTH = Config.SERVICE_GRAPH_MAX_DEPTH
    PATH_SIMILARITY_THRESHOLD = Config.PATH_SIMILARITY_THRESHOLD
    RELATED_PATH_SIMILARITY_CUTOFF = Config.RELATED_PATH_SIMILARITY_CUTOFF
    SEVERITY_DOMINANCE_RATIO = Config.SEVERITY_DOMINANCE_RATIO
    
    def __init__(self, service_graph: nx.DiGraph, service_to_graph: Dict,
                 service_features_cache: Dict, pagerank_cache: Dict):
        self.service_graph = service_graph
        self.service_to_graph = service_to_graph
        self._service_features_cache = service_features_cache
        self._pagerank_cache = pagerank_cache
        self.enriched_alerts = []
        self.consolidated_groups = []
        self.alerts_df = None
        self.feature_matrix = None
        self.feature_matrix_scaled = None
        self.feature_names = []
        self.scaler = StandardScaler()
        self.pca = None
        self.outlier_indices = set()
        self.outlier_removal_enabled = True
        self._idx_mapping = None 
        self.deduplicated_alerts = []
        self.duplicate_groups = []
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++
    def enrich_alerts(self, alerts: List[Dict]) -> List[Dict]:
        
        for alert in alerts:
            is_mapped = self._enrich_alert_with_graph_info(alert)
            
            if is_mapped:
                graph_service = alert.get('graph_service')
                if graph_service:
                    alert['dependencies'] = self._get_service_dependencies(graph_service)
        
        self.enriched_alerts = alerts
        return self.enriched_alerts
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++
    # note: basic mapping setup suggested and reviewed 
    def _enrich_alert_with_graph_info(self, alert: Dict) -> bool:
        service_name = alert.get('service_name', '')
       
        if service_name and service_name in self.service_to_graph:
            graph_info = self.service_to_graph[service_name]
            graph_props = graph_info.get('properties', {}) if graph_info else {}

            def _matches(alert_value: str, graph_value: str) -> bool:
                if graph_value:
                    return str(alert_value).strip().lower() == str(graph_value).strip().lower()
                return True

            alert_cloud = alert.get('cloud', '')
            alert_platform = alert.get('platform', '')
            alert_environment = alert.get('environment', '')
            alert_servicename = alert.get('service_name', '')

            graph_cloud = graph_info.get('cloud') or graph_props.get('cloud', '')
            graph_platform = graph_info.get('platform') or graph_props.get('platform', '')
            graph_environment = graph_info.get('env') or graph_props.get('env', '')
            graph_servicename = graph_props.get('service_name') or graph_props.get('service_name', '')
            if (
                _matches(alert_cloud, graph_cloud)
                and _matches(alert_platform, graph_platform)
                and _matches(alert_environment, graph_environment)
                and _matches(alert_servicename, graph_servicename)
            ):
                alert['graph_service'] = service_name
                alert['graph_info'] = graph_info
                alert['mapping_method'] = 'service_name'
                alert['mapping_confidence'] = 1.0
                return True
        alert['graph_service'] = None
        alert['graph_info'] = None
        alert['mapping_method'] = 'unmapped'
        alert['mapping_confidence'] = 0.0
        return False
    # +++++++++++++++++++++++++++++++++++++++++++++++++++
    def _get_service_dependencies(self, service_name: str) -> Dict:
        dependencies = {
            'upstream': [],
            'downstream': [],
            'peers': []
        }
        
        if not self.service_graph.has_node(service_name):
            return dependencies
        
        for predecessor in self.service_graph.predecessors(service_name):
            edge_data = self.service_graph.get_edge_data(predecessor, service_name)
            dependencies['upstream'].append({
                'service': predecessor,
                'relationship': edge_data.get('relationship_type', '') if edge_data else ''
            })
        
        for successor in self.service_graph.successors(service_name):
            edge_data = self.service_graph.get_edge_data(service_name, successor)
            dependencies['downstream'].append({
                'service': successor,
                'relationship': edge_data.get('relationship_type', '') if edge_data else ''
            })
        
        return dependencies
    # +++++++++++++++++++++++++++++++++++++++++++++++++++
    # note: make sure this grouping must happen first before clustering
    def group_alerts_by_relationships(self) -> List[Dict]:
        print("\n  Grouping alerts by service relationships...")
        
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
        
        print("    Analyzing service relationships and impact propagation...")
        
        self.consolidated_groups = []
        processed_services = set()
        
        for service_name, alerts in service_groups.items():
            if service_name in processed_services:
                continue
            
            impacted_services = self._find_impacted_services(service_name, max_depth=self.SERVICE_GRAPH_MAX_DEPTH)
            
            correlated_services = self._find_correlated_alert_services(
                service_name, service_groups, impacted_services
            )
            
            group = {
                'group_id': len(self.consolidated_groups),
                'primary_service': service_name,
                'impacted_services': list(impacted_services),
                'correlated_services': list(correlated_services),
                'alerts': alerts.copy(),
                'alert_count': len(alerts),
                'service_count': 1 + len(correlated_services),
                'grouping_method': 'impact_and_correlation',
                'impact_radius': len(impacted_services)
            }
            
            for correlated_svc in correlated_services:
                if correlated_svc in service_groups:
                    correlated_alert_count = len(service_groups[correlated_svc])
                    group['alerts'].extend(service_groups[correlated_svc])
                    group['alert_count'] += correlated_alert_count
                    processed_services.add(correlated_svc)

            group['alert_types'] = list(set(a.get('alert_name', '') for a in group['alerts']))
            group['namespaces'] = list(set(a.get('namespace', '') for a in group['alerts'] if a.get('namespace')))
            group['clusters'] = list(set(a.get('cluster', '') for a in group['alerts'] if a.get('cluster')))
            group['correlation_type'] = 'immediate_neighbors' if len(correlated_services) <= 2 else 'transitive_sinks'
            
            self.consolidated_groups.append(group)
            processed_services.add(service_name)
            
        if unmapped_alerts:
            unmapped_groups = self._group_unmapped_alerts(unmapped_alerts)
            self.consolidated_groups.extend(unmapped_groups)

        for i, group in enumerate(self.consolidated_groups):
            group['group_id'] = i
            severities = [a.get('severity', 'unknown') for a in group['alerts']]
            alert_categories = [a.get('alert_category', 'unknown') for a in group['alerts']]
            alert_subcategories = [a.get('alert_subcategory', 'unknown') for a in group['alerts']]
            
            group['severity_distribution'] = dict(Counter(severities))
            group['category_distribution'] = dict(Counter(alert_categories))
            group['subcategory_distribution'] = dict(Counter(alert_subcategories))
            
            timestamps = [a.get('start_timestamp', 0) for a in group['alerts'] if a.get('start_timestamp')]
            if timestamps:
                group['earliest_alert'] = min(timestamps)
                group['latest_alert'] = max(timestamps)
                group['time_span_minutes'] = (max(timestamps) - min(timestamps)) / 60
            else:
                group['earliest_alert'] = 0
                group['latest_alert'] = 0
                group['time_span_minutes'] = 0
            
            alert_types = [a.get('alert_name', '') for a in group['alerts'] if a.get('alert_name', '')]
            group['most_common_alert'] = Counter(alert_types).most_common(1)[0][0] if alert_types else ''
            group['most_common_category'] = Counter(alert_categories).most_common(1)[0][0] if alert_categories else ''
            group['most_common_subcategory'] = Counter(alert_subcategories).most_common(1)[0][0] if alert_subcategories else ''
            
            for alert in group['alerts']:
                alert['initial_group_id'] = i
        
        self._calculate_all_group_metrics()
        
        print(f"  Created {len(self.consolidated_groups)} initial consolidated groups")
        return self.consolidated_groups
    # +++++++++++++++++++++++++++++++++++++++++++++++++++
    def _find_impacted_services(self, service_name: str, max_depth: int = None) -> Set:
        impacted = set()

        if max_depth is None:
            max_depth = self.SERVICE_GRAPH_MAX_DEPTH
        
        if not self.service_graph.has_node(service_name):
            return impacted
        
        visited = set()
        queue = [(service_name, 0)]
        
        while queue:
            current, depth = queue.pop(0)
            visited.add(current)
            
            if depth >= max_depth:
                continue
            for successor in self.service_graph.successors(current):
                if successor not in visited and successor != service_name:
                    impacted.add(successor)
                    queue.append((successor, depth + 1))
        
        return impacted
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _find_correlated_alert_services(self, service_name: str, service_groups: Dict, already_impacted: Set) -> Set:
        correlated = set()
        
        if not self.service_graph.has_node(service_name):
            return correlated
        
        for neighbor in self.service_graph.neighbors(service_name):
            if neighbor in service_groups and neighbor != service_name:
                correlated.add(neighbor)
        
        for predecessor in self.service_graph.predecessors(service_name):
            if predecessor in service_groups and predecessor != service_name:
                correlated.add(predecessor)
        
        for other_service in service_groups.keys():
            if other_service == service_name or other_service in correlated:
                continue
            
            similarity = self._compute_path_similarity(service_name, other_service)
            
            if similarity >= self.PATH_SIMILARITY_THRESHOLD:
                correlated.add(other_service)
        
        for impacted_svc in already_impacted:
            if impacted_svc in service_groups:
                correlated.add(impacted_svc)
        
        return correlated
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _find_related_alert_services(self, service_name: str, service_groups: Dict) -> Set:
        related = set()
        
        if not self.service_graph.has_node(service_name):
            return related
        for neighbor in self.service_graph.neighbors(service_name):
            if neighbor in service_groups and neighbor != service_name:
                related.add(neighbor)
        for predecessor in self.service_graph.predecessors(service_name):
            if predecessor in service_groups and predecessor != service_name:
                related.add(predecessor)
        parent_sinks1 = self._get_transitive_parent_services(service_name, max_depth=3)
        
        for other_service in service_groups.keys():
            if other_service == service_name or other_service in related:
                continue
            
            parent_sinks2 = self._get_transitive_parent_services(other_service, max_depth=3)
            
            if parent_sinks1 and parent_sinks2 and (parent_sinks1 & parent_sinks2):
                related.add(other_service)
            similarity = self._compute_path_similarity(service_name, other_service)
            if similarity > self.RELATED_PATH_SIMILARITY_CUTOFF:
                related.add(other_service)
        
        return related
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _get_transitive_parent_services(self, service_name: str, max_depth: int = 3) -> Set:
        parent_set = set()
        
        if not self.service_graph.has_node(service_name):
            return parent_set
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
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # note: we may not need this method as this time, double check
    def _compute_path_similarity(self, service1: str, service2: str) -> float:
        if service1 == service2:
            return 1.0
        
        parents1 = self._get_transitive_parent_services(service1)
        parents2 = self._get_transitive_parent_services(service2)
        
        if not parents1 or not parents2:
            return 0.0
        intersection = len(parents1 & parents2)
        union = len(parents1 | parents2)
        
        return intersection / union if union > 0 else 0.0
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _group_unmapped_alerts(self, unmapped_alerts: List[Dict]) -> List[Dict]:
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
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _calculate_all_group_metrics(self):
        for group in self.consolidated_groups:
            group_metrics = self._calculate_group_metrics(group)
            group.update(group_metrics)
    
    def _calculate_service_graph_metrics(self, service_name: str) -> Dict:
        metrics = {
            'pagerank': 0.0,
            'betweenness': 0.0,
            'degree': 0,
            'in_degree': 0,
            'out_degree': 0,
            'blast_radius': 0,
            'impacted_services': [],
            'upstream_services': [],
            'downstream_services': []
        }
        
        if not service_name or not self.service_graph.has_node(service_name):
            return metrics
        
        if self._pagerank_cache and service_name in self._pagerank_cache:
            metrics['pagerank'] = round(self._pagerank_cache[service_name], 6)
        
        metrics['degree'] = self.service_graph.degree(service_name)
        metrics['in_degree'] = self.service_graph.in_degree(service_name)
        metrics['out_degree'] = self.service_graph.out_degree(service_name)
        
        metrics['upstream_services'] = list(self.service_graph.predecessors(service_name))
        
        metrics['downstream_services'] = list(self.service_graph.successors(service_name))
        
        impacted = self._get_transitive_downstream(service_name, max_depth=self.SERVICE_GRAPH_MAX_DEPTH)
        metrics['impacted_services'] = list(impacted)
        metrics['blast_radius'] = len(impacted)
        
        return metrics
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _calculate_group_metrics(self, group: Dict) -> Dict:
        primary_service = group.get('primary_service', '')
        alerts = group.get('alerts', [])
        
        metrics = {
            'primary_service': primary_service,
            'impacted_services': group.get('impacted_services', []),
            'impacted_services_count': len(group.get('impacted_services', [])),
            'blast_radius': group.get('impact_radius', 0),
            'upstream_services': [],
            'upstream_count': 0,
            'downstream_services': [],
            'downstream_count': 0,
            'avg_pagerank': 0.0,
            'impact_propagation_score': 0.0,
            'criticality_score': 0.0,
            'root_cause_services': [],
            'root_cause_description': ''
        }
        
        if not primary_service or not self.service_graph.has_node(primary_service):
            return metrics
        
        service_metrics = self._calculate_service_graph_metrics(primary_service)
        
        metrics['upstream_services'] = service_metrics['upstream_services']
        metrics['upstream_count'] = len(service_metrics['upstream_services'])
        metrics['downstream_services'] = service_metrics['downstream_services']
        metrics['downstream_count'] = len(service_metrics['downstream_services'])
        
        group_services = set()
        group_services.add(primary_service)
        for svc in group.get('correlated_services', []):
            if svc:
                group_services.add(svc)
        
        pagerank_sum = 0.0
        service_count = 0
        
        for svc in group_services:
            if self.service_graph.has_node(svc):
                if self._pagerank_cache and svc in self._pagerank_cache:
                    pagerank_sum += self._pagerank_cache[svc]
                service_count += 1
        
        if service_count > 0:
            metrics['avg_pagerank'] = round(pagerank_sum / service_count, 6)
        
        blast_score = min(metrics['blast_radius'] / 20.0, 1.0) * 40
        
        severities = [a.get('severity', '').lower() for a in alerts]
        severity_weights = {'critical': 1.0, 'high': 0.75, 'warning': 0.5, 'info': 0.25}
        avg_severity = sum(severity_weights.get(s, 0) for s in severities) / len(severities) if severities else 0
        severity_score = avg_severity * 40
        
        service_score = min(len(group_services) / 5.0, 1.0) * 20
        
        metrics['impact_propagation_score'] = round(blast_score + severity_score + service_score, 2)
        
        pagerank_score = min(metrics['avg_pagerank'] * 5000, 35)
        degree_score = min(service_metrics['degree'] / 30.0, 1.0) * 25
        alert_score = min(len(alerts) / 30.0, 1.0) * 40
        
        metrics['criticality_score'] = round(pagerank_score + degree_score + alert_score, 2)
        
        root_causes = []
        for svc in group_services:
            if self.service_graph.has_node(svc):
                svc_blast = len(self._get_transitive_downstream(svc, max_depth=self.SERVICE_GRAPH_MAX_DEPTH))
                if svc_blast >= 3:
                    root_causes.append({'service': svc, 'blast_radius': svc_blast})
        
        root_causes.sort(key=lambda x: x['blast_radius'], reverse=True)
        metrics['root_cause_services'] = [rc['service'] for rc in root_causes[:3]]
        
        if root_causes:
            top_rc = root_causes[0]
            metrics['root_cause_description'] = f"{top_rc['service']} (impacts {top_rc['blast_radius']} downstream services)"
        else:
            metrics['root_cause_description'] = 'No high-impact root cause identified'
        
        return metrics
    # +++++++++++++++++++++++++++++++++++++++++++++++++
    # to do: revisit service graph features logic
    def engineer_features(self, target_pca_components: int = None) -> Tuple[np.ndarray, StandardScaler, PCA, List[str]]:
        self.alerts_df = pd.DataFrame(self.enriched_alerts)
        
        graph_feature_keys = ['degree_total', 'in_degree', 'out_degree', 'pagerank', 'betweenness', 
                             'clustering_coef', 'num_upstream', 'num_downstream',
                             'upstream_calls', 'upstream_owns', 'upstream_belongs_to',
                             'downstream_calls', 'downstream_owns', 'downstream_belongs_to',
                             'ratio_calls', 'ratio_owns', 'ratio_belongs_to',
                             'dependency_direction', 'avg_neighbor_degree', 'max_neighbor_degree']
        
        features_list = []
        
        for row in self.alerts_df.itertuples():
            feature_dict = {}
            graph_service = getattr(row, 'graph_service', '')
            if pd.notna(graph_service) and graph_service and graph_service in self._service_features_cache:
                cached_features = self._service_features_cache[graph_service]
                for key in graph_feature_keys:
                    feature_dict[key] = cached_features.get(key, 0)
            else:
                for key in graph_feature_keys:
                    feature_dict[key] = 0
            alert_name = str(getattr(row, 'alert_name', '')).lower()
            feature_dict['severity_encoded'] = self._encode_severity(getattr(row, 'severity', ''))
            feature_dict['is_error_alert'] = 1 if 'error' in alert_name else 0
            feature_dict['is_resource_alert'] = 1 if any(x in alert_name for x in ['cpu', 'memory', 'hpa', 'resource']) else 0
            feature_dict['is_network_alert'] = 1 if any(x in alert_name for x in ['network', 'rx_bytes', 'tx_bytes']) else 0
            feature_dict['is_anomaly_alert'] = 1 if 'anomaly' in str(getattr(row, 'alert_category', '')).lower() else 0
            
            feature_dict['alert_category_encoded'] = self._encode_alert_category(getattr(row, 'alert_category', ''))
            feature_dict['alert_subcategory_encoded'] = self._encode_alert_subcategory(getattr(row, 'alert_subcategory', ''))
            feature_dict['workload_type_encoded'] = self._encode_workload_type(getattr(row, 'workload_type', ''))

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
        self.feature_matrix = features_df.values
        self.feature_matrix_scaled = self.scaler.fit_transform(self.feature_matrix)
        if self.outlier_removal_enabled and len(self.feature_matrix_scaled) > Config.OUTLIER_MIN_SAMPLES:
            self._remove_outliers()
        else:
            self._idx_mapping = {i: i for i in range(len(self.alerts_df))}
        self._apply_pca(target_components=target_pca_components)
        
        return self.feature_matrix_scaled, self.scaler, self.pca, self.feature_names
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _encode_severity(self, severity: str) -> int:
        severity_map = {
            'critical': 4,
            'high': 3,
            'warning': 2,
            'info': 1,
            'unknown': 0
        }
        return severity_map.get(str(severity).lower().strip(), 0)
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _encode_workload_type(self, workload_type: str) -> int:
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
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _encode_alert_category(self, category: str) -> int:
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
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _remove_outliers(self):
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
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def _apply_pca(self, target_components: int = None):
        try:
            if target_components is not None:
                max_components = min(target_components, self.feature_matrix_scaled.shape[1], self.feature_matrix_scaled.shape[0])
                pca = PCA(n_components=max_components, random_state=42)
            else:
                pca = PCA(n_components=self.PCA_VARIANCE_THRESHOLD, random_state=42)
            
            self.feature_matrix_pca = pca.fit_transform(self.feature_matrix_scaled)
            
            n_components = pca.n_components_
            variance_explained = pca.explained_variance_ratio_.sum()
            
            print(f"    Applied PCA: {n_components} components, {variance_explained:.2%} variance explained")
            
            self.pca = pca
            self.feature_matrix_scaled = self.feature_matrix_pca
            
        except Exception as e:
            print(f"    PCA failed: {e}")
            self.pca = None
            self.feature_matrix_pca = None
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    def assign_cluster_labels(self, cluster_labels: np.ndarray, clustering_method: str):
        self.alerts_df['cluster_id'] = cluster_labels
        self.alerts_df['clustering_method'] = clustering_method
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
    
    # +++++++++++++++++++++++++++++++++++++++++++++++++
    def deduplicate_alerts(self) -> List[Dict]:
        """
        Deduplicate alerts within each cluster using basic deduplication rules.
        Processes each cluster separately to find duplicates.
        """
        self.deduplicated_alerts = []
        self.duplicate_groups = []
        
        # Group alerts by cluster_id
        clusters = self.alerts_df.groupby('cluster_id')
        
        for cluster_id, cluster_df in clusters:
            # Get alerts for this cluster
            cluster_alerts = []
            for idx in cluster_df.index:
                orig_idx = self._idx_mapping[idx] if self._idx_mapping else idx
                cluster_alerts.append((orig_idx, self.enriched_alerts[orig_idx]))
            
            # Deduplicate within this cluster
            processed = set()
            
            for i, (idx_i, alert) in enumerate(cluster_alerts):
                if i in processed:
                    continue
                
                alert['is_duplicate'] = False
                alert['duplicate_of'] = None
                duplicates = []
                
                # Compare with remaining alerts in cluster
                for j in range(i + 1, len(cluster_alerts)):
                    if j in processed:
                        continue
                    
                    idx_j, other_alert = cluster_alerts[j]
                    
                    if self._are_duplicates_basic(alert, other_alert):
                        duplicates.append(idx_j)
                        processed.add(j)
                        
                        other_alert['is_duplicate'] = True
                        other_alert['duplicate_of'] = idx_i
                
                # Add representative to deduplicated list
                self.deduplicated_alerts.append(alert)
                
                # Track duplicate group if any
                if duplicates:
                    self.duplicate_groups.append({
                        'representative_idx': idx_i,
                        'duplicate_indices': duplicates,
                        'count': len(duplicates) + 1
                    })
                
                processed.add(i)
        
        return self.deduplicated_alerts
    # +++++++++++++++++++++++++++++++++++++++++++++++++
    def _are_duplicates_basic(self, alert1: Dict, alert2: Dict) -> bool:
        # Same alert_name AND (same pod OR same service_name)
        if (alert1.get('alert_name') == alert2.get('alert_name') and
            alert1.get('alert_name') and
            (alert1.get('pod') == alert2.get('pod') and alert1.get('pod') or
             alert1.get('service_name') == alert2.get('service_name') and alert1.get('service_name'))):
            return True
        
        # Same service_name AND same category AND same subcategory
        if (alert1.get('service_name') == alert2.get('service_name') and
            alert1.get('service_name') and
            alert1.get('alert_category') == alert2.get('alert_category') and
            alert1.get('alert_category') and
            alert1.get('alert_subcategory') == alert2.get('alert_subcategory') and
            alert1.get('alert_subcategory')):
            return True
        
        return False
    # +++++++++++++++++++++++++++++++++++++++++++++++++
    def _get_transitive_downstream(self, service_name: str, max_depth: int = None) -> Set:
        downstream_set = set()

        if max_depth is None:
            max_depth = self.SERVICE_GRAPH_MAX_DEPTH
        
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

    # +++++++++++++++++++++++++++++++++++++++++++++++++
    def rank_and_name_clusters(self) -> List[Dict]:
        print("\n  Naming and ranking clusters...")
        
        cluster_dict = {}
        for alert in self.enriched_alerts:
            cluster_id = alert.get('cluster_id', -1)
            if cluster_id not in cluster_dict:
                cluster_dict[cluster_id] = []
            cluster_dict[cluster_id].append(alert)
        
        cluster_metadata = []
        numeric_to_solid_id_map = {}
        
        for numeric_cluster_id, cluster_alerts in cluster_dict.items():
            if numeric_cluster_id == -1:
                continue
            cluster_name = self._generate_cluster_name(cluster_alerts)
            solid_cluster_id = self._generate_cluster_id(cluster_name)
            numeric_to_solid_id_map[numeric_cluster_id] = solid_cluster_id
        
            alert_types = [a.get('alert_name', '') for a in cluster_alerts]
            services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
            categories = [a.get('alert_category', '') for a in cluster_alerts if a.get('alert_category')]
            
            cluster_metadata.append({
                'cluster_id': solid_cluster_id,
                'numeric_cluster_id': numeric_cluster_id,
                'cluster_name': cluster_name,
                'ranking_score': 0.0,
                'alert_count': len(cluster_alerts),
                'unique_alert_types': len(set(alert_types)),
                'unique_services': len(set(services)),
                'primary_service': Counter(services).most_common(1)[0][0] if services else '',
                'most_common_category': Counter(categories).most_common(1)[0][0] if categories else '',
            })
        
        cluster_metadata.sort(key=lambda x: x['ranking_score'], reverse=True)
        
        for rank, metadata in enumerate(cluster_metadata, start=1):
            metadata['rank'] = rank
            solid_cluster_id = metadata['cluster_id']
            numeric_cluster_id = metadata['numeric_cluster_id']
            
            if numeric_cluster_id in cluster_dict:
                for alert in cluster_dict[numeric_cluster_id]:
                    alert['cluster_id'] = solid_cluster_id
                    alert['cluster_rank'] = rank
                    alert['cluster_name'] = metadata['cluster_name']
                    alert['cluster_score'] = metadata['ranking_score']
        
        if -1 in cluster_dict:
            for alert in cluster_dict[-1]:
                alert['cluster_rank'] = -1
                alert['cluster_name'] = 'outlier_or_unmapped'
                alert['cluster_score'] = 0.0

        print(f"    Ranked {len(cluster_metadata)} clusters")
        
        if cluster_metadata:
            print("\n    Top 5 clusters by importance:")
            for i, meta in enumerate(cluster_metadata[:5], 1):
                print(f"      {i}. {meta['cluster_name']} (Score: {meta['ranking_score']:.1f}, {meta['alert_count']} alerts)")
        
        return cluster_metadata
    # +++++++++++++++++++++++++++++++++++++++++++++++++
    def _generate_cluster_id(self, cluster_name: str) -> str:
        import hashlib    
        pattern_hash = hashlib.md5(cluster_name.encode()).hexdigest()[:16]
        cluster_id = f"{pattern_hash}"
        return cluster_id
    # +++++++++++++++++++++++++++++++++++++++++++++++++
    def _generate_cluster_name(self, cluster_alerts: List[Dict]) -> str:
        if not cluster_alerts:
            return "Empty_Cluster"
        services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
        top_service = Counter(services).most_common(1)[0][0] if services else 'unmapped'
        
        # Get the most repeated alert_name for the top_service
        top_service_alerts = [a.get('alert_name', '') for a in cluster_alerts 
                             if a.get('graph_service') == top_service and a.get('alert_name')]
        
        # Get the most common alert name
        most_common_alert = Counter(top_service_alerts).most_common(1)[0][0] if top_service_alerts else 'unknown'
        
        severities = [a.get('severity', '').lower() for a in cluster_alerts]
        severity_counts = Counter(severities)
        if 'critical' in severity_counts and severity_counts['critical'] > len(cluster_alerts) * self.SEVERITY_DOMINANCE_RATIO:
            severity_key = "critical"
        elif 'high' in severity_counts and severity_counts['high'] > len(cluster_alerts) * self.SEVERITY_DOMINANCE_RATIO:
            severity_key = "high"
        else:
            severity_key = "mixed"
        cluster_name = f"{most_common_alert}-{top_service}-{severity_key}"
        cluster_name = cluster_name.strip('_')
        return cluster_name
