"""
Alert Processor: this module handles alert enrichment with service graph, consolidation, feature engineering, deduplication, and ranking.
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
    TIME_WINDOW_MINUTES = 15            # Time window for duplicate detection (15 min workflow)
    MIN_MATCH_SCORE = 2                 # Minimum score for fallback mapping
    MIN_CLUSTERING_SAMPLES = 10         # Minimum alerts needed for clustering
    PCA_VARIANCE_THRESHOLD = 0.95       # Retain 95% variance
    OUTLIER_CONTAMINATION = 0.05        # Expect 5% outliers
    
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
        
        # direct service_name match
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
            has_namespace_match = False
            has_cluster_match = False
            
            if alert_namespace and svc_info.get('namespace') == alert_namespace:
                match_score += 3  # Higher weight - namespace is important
                has_namespace_match = True
  
            if alert_cluster and svc_info.get('cluster') == alert_cluster:
                match_score += 3  # Higher weight - cluster is important
                has_cluster_match = True

            if alert_node and svc_info.get('properties', {}).get('node') == alert_node:
                match_score += 1 
     
            if has_namespace_match and has_cluster_match:
                matched_services.append((svc_name, svc_info, match_score))
        
        if matched_services:
            matched_services.sort(key=lambda x: x[2], reverse=True)
            best_match = matched_services[0]
            top_score = best_match[2]
            ambiguous_matches = [m for m in matched_services if m[2] == top_score]
            
            alert['graph_service'] = best_match[0]
            alert['graph_info'] = best_match[1]
            alert['match_score'] = best_match[2]
            
            if len(ambiguous_matches) > 1:
                alert['mapping_method'] = 'namespace_cluster_fallback_ambiguous'
                alert['mapping_confidence'] = 0.5  # Lower confidence due to ambiguity
                alert['ambiguous_match_count'] = len(ambiguous_matches)
            else:
                alert['mapping_method'] = 'namespace_cluster_fallback'
                alert['mapping_confidence'] = min(best_match[2] / 7.0, 0.95)
            
            return True
 
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
    
    def group_alerts_by_relationships(self) -> List[Dict]:
        """Group alerts based on service relationships and impact propagation"""
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
            
            # Find impacted downstream services (blast radius)
            impacted_services = self._find_impacted_services(service_name, max_depth=2)
            
            # Find correlated services with alerts
            correlated_services = self._find_correlated_alert_services(
                service_name, service_groups, impacted_services
            )
            
            group = {
                'group_id': len(self.consolidated_groups),
                'primary_service': service_name,  # Service with the alert
                'impacted_services': list(impacted_services),  # Services affected downstream
                'correlated_services': list(correlated_services),  # Services with alerts that might be related
                'alerts': alerts.copy(),
                'alert_count': len(alerts),
                'service_count': 1 + len(correlated_services),
                'grouping_method': 'impact_and_correlation',
                'impact_radius': len(impacted_services)  # Blast radius
            }
            
            # Merge alerts from correlated services
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
            
            # Time range
            timestamps = [a.get('start_timestamp', 0) for a in group['alerts'] if a.get('start_timestamp')]
            if timestamps:
                group['earliest_alert'] = min(timestamps)
                group['latest_alert'] = max(timestamps)
                group['time_span_minutes'] = (max(timestamps) - min(timestamps)) / 60
            else:
                group['earliest_alert'] = 0
                group['latest_alert'] = 0
                group['time_span_minutes'] = 0
            
            # Most common alert type, category, subcategory
            alert_types = [a.get('alert_name', '') for a in group['alerts'] if a.get('alert_name', '')]
            group['most_common_alert'] = Counter(alert_types).most_common(1)[0][0] if alert_types else ''
            group['most_common_category'] = Counter(alert_categories).most_common(1)[0][0] if alert_categories else ''
            group['most_common_subcategory'] = Counter(alert_subcategories).most_common(1)[0][0] if alert_subcategories else ''
            
            for alert in group['alerts']:
                alert['initial_group_id'] = i
        
        # Calculate group metrics (impact propagation and criticality scores)
        self._calculate_all_group_metrics()
        
        print(f"  Created {len(self.consolidated_groups)} initial consolidated groups")
        return self.consolidated_groups
    
    def _find_impacted_services(self, service_name: str, max_depth: int = 2) -> Set:
        """
        Find services that are potentially IMPACTED by issues in the given service.
        This traverses DOWNSTREAM (successors) to find dependent services.
        
        Example:
            Database → API → Frontend
            If Database has alert, impacted services = [API, Frontend]
        """
        impacted = set()
        
        if not self.service_graph.has_node(service_name):
            return impacted
        
        visited = set()
        queue = [(service_name, 0)]
        
        while queue:
            current, depth = queue.pop(0)
            visited.add(current)
            
            if depth >= max_depth:
                continue
            # Get services that depend on current service (DOWNSTREAM)
            for successor in self.service_graph.successors(current):
                if successor not in visited and successor != service_name:
                    impacted.add(successor)
                    queue.append((successor, depth + 1))
        
        return impacted
    
    def _find_correlated_alert_services(self, service_name: str, service_groups: Dict, already_impacted: Set) -> Set:
        """
        Find services WITH ALERTS that are correlated to the given service.
        This helps group alerts that are likely related.
        
        Correlation types:
        1. Direct neighbors (immediate upstream/downstream)
        2. Share common root cause (path similarity >= 60%)
        3. Part of cascading failure (impacted services with alerts)
        """
        correlated = set()
        
        if not self.service_graph.has_node(service_name):
            return correlated
        
        # Direct neighbors with alerts
        for neighbor in self.service_graph.neighbors(service_name):
            if neighbor in service_groups and neighbor != service_name:
                correlated.add(neighbor)
        
        for predecessor in self.service_graph.predecessors(service_name):
            if predecessor in service_groups and predecessor != service_name:
                correlated.add(predecessor)
        
        # Services with high path similarity (share common root causes)
        PATH_SIMILARITY_THRESHOLD = 0.6  # 60% shared dependencies
        
        for other_service in service_groups.keys():
            if other_service == service_name or other_service in correlated:
                continue
            
            similarity = self._compute_path_similarity(service_name, other_service)
            
            if similarity >= PATH_SIMILARITY_THRESHOLD:
                correlated.add(other_service)
        
        # Cascading failures - impacted services that also have alerts
        for impacted_svc in already_impacted:
            if impacted_svc in service_groups:
                correlated.add(impacted_svc)
        
        return correlated

    def _find_related_alert_services(self, service_name: str, service_groups: Dict, max_depth: int = 1) -> Set:
        """Find services with alerts that are related to given service (legacy method)"""
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
            if similarity > 0.3:
                related.add(other_service)
        
        return related
    
    def _get_transitive_parent_services(self, service_name: str, max_depth: int = 3) -> Set:
        """Get transitive upstream services (all parent services up to max_depth)"""
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
    
    def _compute_path_similarity(self, service1: str, service2: str) -> float:
        """Compute similarity between two services based on their graph paths"""
        if service1 == service2:
            return 1.0
        
        parents1 = self._get_transitive_parent_services(service1)
        parents2 = self._get_transitive_parent_services(service2)
        
        if not parents1 or not parents2:
            return 0.0
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
    
    def _calculate_all_group_metrics(self):
        """Calculate metrics for all consolidated groups"""
        for group in self.consolidated_groups:
            group_metrics = self._calculate_group_metrics(group)
            group.update(group_metrics)
    
    def _calculate_service_graph_metrics(self, service_name: str) -> Dict:
        """Calculate graph-level metrics for a single service"""
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
        
        # Upstream services (services this depends on)
        metrics['upstream_services'] = list(self.service_graph.predecessors(service_name))
        
        # Downstream services (direct dependents)
        metrics['downstream_services'] = list(self.service_graph.successors(service_name))
        
        # Impacted services (transitive downstream) and blast radius
        impacted = self._get_transitive_downstream(service_name, max_depth=2)
        metrics['impacted_services'] = list(impacted)
        metrics['blast_radius'] = len(impacted)
        
        return metrics
    
    def _calculate_group_metrics(self, group: Dict) -> Dict:
        """Calculate aggregated metrics for a consolidated group"""
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
        
        # Aggregate services in group
        group_services = set()
        group_services.add(primary_service)
        for svc in group.get('correlated_services', []):
            if svc:
                group_services.add(svc)
        
        # Calculate averages
        pagerank_sum = 0.0
        service_count = 0
        
        for svc in group_services:
            if self.service_graph.has_node(svc):
                if self._pagerank_cache and svc in self._pagerank_cache:
                    pagerank_sum += self._pagerank_cache[svc]
                service_count += 1
        
        if service_count > 0:
            metrics['avg_pagerank'] = round(pagerank_sum / service_count, 6)
        
        # Calculate Impact Propagation Score (0-100)
        # Components: blast_radius (40%), severity (40%), service_count (20%)
        blast_score = min(metrics['blast_radius'] / 20.0, 1.0) * 40
        
        severities = [a.get('severity', '').lower() for a in alerts]
        severity_weights = {'critical': 1.0, 'high': 0.75, 'warning': 0.5, 'info': 0.25}
        avg_severity = sum(severity_weights.get(s, 0) for s in severities) / len(severities) if severities else 0
        severity_score = avg_severity * 40
        
        service_score = min(len(group_services) / 5.0, 1.0) * 20
        
        metrics['impact_propagation_score'] = round(blast_score + severity_score + service_score, 2)
        
        # Calculate Criticality Score (0-100)
        # Components: pagerank (35%), degree (25%), alert_volume (40%)
        pagerank_score = min(metrics['avg_pagerank'] * 5000, 35)
        degree_score = min(service_metrics['degree'] / 30.0, 1.0) * 25
        alert_score = min(len(alerts) / 30.0, 1.0) * 40
        
        metrics['criticality_score'] = round(pagerank_score + degree_score + alert_score, 2)
        
        # Identify root cause services (services with high blast radius in this group)
        root_causes = []
        for svc in group_services:
            if self.service_graph.has_node(svc):
                svc_blast = len(self._get_transitive_downstream(svc, max_depth=2))
                if svc_blast >= 3:
                    root_causes.append({'service': svc, 'blast_radius': svc_blast})
        
        root_causes.sort(key=lambda x: x['blast_radius'], reverse=True)
        metrics['root_cause_services'] = [rc['service'] for rc in root_causes[:3]]
        
        # Generate root cause description
        if root_causes:
            top_rc = root_causes[0]
            metrics['root_cause_description'] = f"{top_rc['service']} (impacts {top_rc['blast_radius']} downstream services)"
        else:
            metrics['root_cause_description'] = 'No high-impact root cause identified'
        
        return metrics

    def deduplicate_within_groups(self):
        """
        Deduplicate alerts within graph-based groups 
        """
        self.deduplicated_alerts = []
        self.duplicate_groups = []
        total_duplicates = 0
        total_alerts_before = sum(len(g['alerts']) for g in self.consolidated_groups)
        
        print(f" Starting deduplication on {total_alerts_before} alerts across {len(self.consolidated_groups)} groups")
        
        for group in self.consolidated_groups:
            group_alerts = group['alerts']
            group['original_count'] = len(group_alerts)
            
            if len(group_alerts) == 0:
                continue
            processed_indices = set()
            group_deduplicated = []
            
            for i, alert in enumerate(group_alerts):
                if i in processed_indices:
                    continue
                alert['is_duplicate'] = False
                alert['duplicate_of'] = None
                group_deduplicated.append(alert)
                self.deduplicated_alerts.append(alert)
                duplicates = []
                for j, other_alert in enumerate(group_alerts):
                    if j <= i or j in processed_indices:
                        continue
                    
                    if self._are_duplicates_graph_based(alert, other_alert):
                        other_alert['is_duplicate'] = True
                        other_alert['duplicate_of'] = i
                        duplicates.append(j)
                        processed_indices.add(j)
                        total_duplicates += 1
                
                if duplicates:
                    self.duplicate_groups.append({
                        'representative': alert,
                        'duplicate_count': len(duplicates)
                    })
                
                processed_indices.add(i)
            group['alerts'] = group_deduplicated
            group['alert_count'] = len(group_deduplicated)
            group['duplicate_count'] = group['original_count'] - len(group_deduplicated)
        
        total_original = sum(g['original_count'] for g in self.consolidated_groups)
        dedup_rate = (total_duplicates / total_original * 100) if total_original > 0 else 0
        
        print(f"    Found {total_duplicates} duplicates across {len(self.consolidated_groups)} groups")
        print(f"    {len(self.deduplicated_alerts)} unique alerts remain (reduced from {total_original})")
        print(f"    Deduplication rate: {dedup_rate:.1f}%")
        
        return self.deduplicated_alerts
    
    def _are_duplicates_graph_based(self, alert1: Dict, alert2: Dict) -> bool:
        """
        Check if two alerts are duplicates using graph relationships from alert_data_cls_theta.py.
        Must meet ALL criteria:
        1. Same alert type OR same alert category
        2. Within time window
        3. Service relationship criteria (one of):
           a. Same service
           b. High path similarity (>60% shared dependencies)
           c. Share significant immediate dependencies (>50%)
        """
        time1 = alert1.get('start_timestamp', 0)
        time2 = alert2.get('start_timestamp', 0)
        
        if time1 and time2 and abs(time1 - time2) > self.TIME_WINDOW_MINUTES * 60:
            return False
        alert_name1 = alert1.get('alert_name', '')
        alert_name2 = alert2.get('alert_name', '')
        alert_cat1 = alert1.get('alert_category', '')
        alert_cat2 = alert2.get('alert_category', '')
        same_alert_type = (alert_name1 == alert_name2) and alert_name1
        same_category = (alert_cat1 == alert_cat2) and alert_cat1
        
        if not (same_alert_type or same_category):
            return False
        service1 = alert1.get('graph_service')
        service2 = alert2.get('graph_service')
        if not service1 or not service2:
            return self._are_duplicates_basic(alert1, alert2)

        if service1 == service2:
            return True
        PATH_SIMILARITY_DUPLICATE_THRESHOLD = 0.6  # 60% shared dependencies
        
        similarity = self._compute_path_similarity(service1, service2)
        if similarity >= PATH_SIMILARITY_DUPLICATE_THRESHOLD:
            return True
        
        # if Share significant immediate dependencies
        deps1 = self._get_service_dependencies(service1)
        deps2 = self._get_service_dependencies(service2)
        
        upstream1 = {d['service'] for d in deps1['upstream']}
        upstream2 = {d['service'] for d in deps2['upstream']}
        downstream1 = {d['service'] for d in deps1['downstream']}
        downstream2 = {d['service'] for d in deps2['downstream']}
        
        # if Share significant upstream overlap
        if upstream1 and upstream2:
            overlap = len(upstream1 & upstream2)
            min_size = min(len(upstream1), len(upstream2))
            if min_size > 0 and overlap / min_size > 0.6:
                return True
        
        # if Share significant downstream overlap
        if downstream1 and downstream2:
            overlap = len(downstream1 & downstream2)
            min_size = min(len(downstream1), len(downstream2))
            if min_size > 0 and overlap / min_size > 0.6:
                return True
        
        return False
    
    def _get_upstream_dependencies(self, service_name: str, max_depth: int = 3) -> Set:
        """
        Get upstream services (dependencies) up to max_depth.
        These are services that the given service DEPENDS ON.
        Used for finding services with common root causes.
        """
        dependencies = set()
        
        if not self.service_graph.has_node(service_name):
            return dependencies
        visited = set()
        queue = [(service_name, 0)]
        
        while queue:
            current, depth = queue.pop(0)
            visited.add(current)
            
            if depth >= max_depth:
                continue
            
            for predecessor in self.service_graph.predecessors(current):
                if predecessor not in visited:
                    dependencies.add(predecessor)
                    queue.append((predecessor, depth + 1))
        
        return dependencies
    
    def engineer_features(self, target_pca_components: int = None) -> Tuple[np.ndarray, StandardScaler, PCA, List[str]]:
        """Extract comprehensive features for clustering
        
        Args:
            target_pca_components: Fixed number of PCA components to use for stability.
                                  If None, uses variance threshold (may vary between runs).
        """
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
        if self.outlier_removal_enabled and len(self.feature_matrix_scaled) > 20:
            self._remove_outliers()
        else:
            self._idx_mapping = {i: i for i in range(len(self.alerts_df))}
        self._apply_pca(target_components=target_pca_components)
        
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
    
    def _apply_pca(self, target_components: int = None):
        """Apply PCA for dimensionality reduction
        
        Args:
            target_components: Fixed number of components to use. If None, uses variance threshold.
                             Using fixed components ensures model stability across runs.
        """
        try:
            # Use fixed component count for stability if provided
            if target_components is not None:
                # Ensure we don't exceed available features
                max_components = min(target_components, self.feature_matrix_scaled.shape[1], self.feature_matrix_scaled.shape[0])
                pca = PCA(n_components=max_components, random_state=42)
            else:
                # Use variance threshold as fallback
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
    
    def assign_cluster_labels(self, cluster_labels: np.ndarray, clustering_method: str):
        """Assign cluster labels from trained model to alerts"""
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
    
    def deduplicate_alerts(self) -> List[Dict]:
        """Deduplicate alerts within clusters based on similarity"""

        self.deduplicated_alerts = []
        self.duplicate_groups = []
        clusters = self.alerts_df.groupby('cluster_id')
        
        total_duplicates = 0
        
        for cluster_id, cluster_df in clusters:
            if cluster_id == -1:
                for idx in cluster_df.index:
                    orig_idx = self._idx_mapping[idx] if self._idx_mapping else idx
                    alert = self.enriched_alerts[orig_idx]
                    alert['is_duplicate'] = False
                    alert['duplicate_of'] = None
                    self.deduplicated_alerts.append(alert)
                continue
            processed_indices = set()
            
            for idx in cluster_df.index:
                if idx in processed_indices:
                    continue
                
                orig_idx = self._idx_mapping[idx] if self._idx_mapping else idx
                alert = self.enriched_alerts[orig_idx]
                alert['is_duplicate'] = False
                alert['duplicate_of'] = None
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
        time1 = alert1.get('start_timestamp', 0)
        time2 = alert2.get('start_timestamp', 0)
        
        if abs(time1 - time2) > self.TIME_WINDOW_MINUTES * 60:
            return False
        
        service1 = alert1.get('graph_service')
        service2 = alert2.get('graph_service')
        
        if not service1 or not service2:
            return self._are_duplicates_basic(alert1, alert2)
        if service1 == service2:
            return True
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
    
    def generate_cluster_descriptions(self, cluster_metadata: List[Dict]) -> List[Dict]:
        """
        Generate human-readable descriptions for each cluster.
        Uses graph topology, alert patterns, and service relationships.
        """
        for meta in cluster_metadata:
            cluster_id = meta['cluster_id']
            cluster_alerts = [a for a in self.enriched_alerts if a.get('cluster_id') == cluster_id]
            
            if not cluster_alerts:
                meta['description'] = "Empty cluster"
                continue
            services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
            categories = [a.get('alert_category', '') for a in cluster_alerts if a.get('alert_category')]
            subcategories = [a.get('alert_subcategory', '') for a in cluster_alerts if a.get('alert_subcategory')]
            severities = [a.get('severity', '') for a in cluster_alerts]
            namespaces = [a.get('namespace', '') for a in cluster_alerts if a.get('namespace')]
            
            most_common_category = Counter(categories).most_common(1)[0][0] if categories else 'unknown'
            most_common_subcategory = Counter(subcategories).most_common(1)[0][0] if subcategories else ''
            critical_count = severities.count('critical')
            high_count = severities.count('high')

            description_parts = []
            if most_common_subcategory:
                description_parts.append(f"{most_common_category.title()} {most_common_subcategory} issues")
            else:
                description_parts.append(f"{most_common_category.title()} issues")

            if critical_count > 0:
                description_parts.append(f"with {critical_count} critical alerts")
            elif high_count > 0:
                description_parts.append(f"with {high_count} high-severity alerts")

            if services:
                unique_services = len(set(services))
                top_service = Counter(services).most_common(1)[0][0]
                if unique_services == 1:
                    description_parts.append(f"affecting {top_service}")
                else:
                    description_parts.append(f"across {unique_services} services (primarily {top_service})")
            if namespaces:
                unique_namespaces = len(set(namespaces))
                if unique_namespaces == 1:
                    description_parts.append(f"in {list(set(namespaces))[0]} namespace")
                else:
                    description_parts.append(f"across {unique_namespaces} namespaces")
            
            meta['description'] = ' '.join(description_parts)
            meta['description_data'] = {
                'most_common_category': most_common_category,
                'most_common_subcategory': most_common_subcategory,
                'primary_services': ', '.join([svc for svc, _ in Counter(services).most_common(3)]) if services else '',
                'affected_service_count': len(set(services)),
                'root_cause_summary': self._identify_root_causes(cluster_alerts)
            }
        
        return cluster_metadata
    
    def _identify_root_causes(self, cluster_alerts: List[Dict]) -> str:
        """Identify potential root causes based on service graph analysis"""
        services_with_alerts = [a.get('graph_service') for a in cluster_alerts if a.get('graph_service')]
        
        if not services_with_alerts:
            return "Unable to determine root cause (unmapped services)"
        root_cause_candidates = []
        for service in set(services_with_alerts):
            if self.service_graph.has_node(service):
                downstream = self._get_transitive_downstream(service, max_depth=2)
                downstream_in_cluster = downstream & set(services_with_alerts)
                
                if len(downstream_in_cluster) > 0:
                    root_cause_candidates.append({
                        'service': service,
                        'impact': len(downstream_in_cluster),
                        'alert_count': services_with_alerts.count(service)
                    })
        
        if root_cause_candidates:
            root_cause_candidates.sort(key=lambda x: (x['impact'], x['alert_count']), reverse=True)
            top_causes = root_cause_candidates[:2]
            causes_str = ', '.join([f"{c['service']} (impacting {c['impact']} services)" for c in top_causes])
            return f"Potential root causes: {causes_str}"
        
        return f"Widespread issues across {len(set(services_with_alerts))} services"
    
    def rank_and_name_clusters(self) -> List[Dict]:
        """Rank clusters and assign distinctive names with solid cluster IDs"""
        print("\n  Naming and ranking clusters...")
        
        cluster_dict = {}
        for alert in self.enriched_alerts:
            cluster_id = alert.get('cluster_id', -1)
            if cluster_id not in cluster_dict:
                cluster_dict[cluster_id] = []
            cluster_dict[cluster_id].append(alert)
        
        # Generate solid cluster IDs and calculate scores
        cluster_metadata = []
        numeric_to_solid_id_map = {}
        
        for numeric_cluster_id, cluster_alerts in cluster_dict.items():
            if numeric_cluster_id == -1:
                continue  # Skip outliers
            
            # Generate solid cluster ID
            solid_cluster_id = self._generate_cluster_id(cluster_alerts)
            numeric_to_solid_id_map[numeric_cluster_id] = solid_cluster_id
            
            # Generate cluster name
            cluster_name = self._generate_cluster_name(cluster_alerts)
            
            # Calculate ranking score
            score = self._calculate_cluster_score(cluster_alerts, numeric_cluster_id)
            
            alert_types = [a.get('alert_name', '') for a in cluster_alerts]
            services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
            categories = [a.get('alert_category', '') for a in cluster_alerts if a.get('alert_category')]
            
            cluster_metadata.append({
                'cluster_id': solid_cluster_id,
                'numeric_cluster_id': numeric_cluster_id,
                'cluster_name': cluster_name,
                'ranking_score': round(score, 2),
                'alert_count': len(cluster_alerts),
                'unique_alert_types': len(set(alert_types)),
                'unique_services': len(set(services)),
                'primary_service': Counter(services).most_common(1)[0][0] if services else '',
                'most_common_category': Counter(categories).most_common(1)[0][0] if categories else '',
            })
        
        # Sort by ranking score (highest first)
        cluster_metadata.sort(key=lambda x: x['ranking_score'], reverse=True)
        
        for rank, metadata in enumerate(cluster_metadata, start=1):
            metadata['rank'] = rank
            solid_cluster_id = metadata['cluster_id']
            numeric_cluster_id = metadata['numeric_cluster_id']
            
            if numeric_cluster_id in cluster_dict:
                for alert in cluster_dict[numeric_cluster_id]:
                    alert['cluster_id'] = solid_cluster_id  # Replace numeric with solid ID
                    alert['cluster_rank'] = rank
                    alert['cluster_name'] = metadata['cluster_name']
                    alert['cluster_score'] = metadata['ranking_score']
        
        # Handle outliers
        if -1 in cluster_dict:
            for alert in cluster_dict[-1]:
                alert['cluster_rank'] = -1
                alert['cluster_name'] = 'outlier_or_unmapped'
                alert['cluster_score'] = 0.0

        print(f"    Ranked {len(cluster_metadata)} clusters")
        
        # Show top 5 clusters
        if cluster_metadata:
            print(f"\n    Top 5 clusters by importance:")
            for i, meta in enumerate(cluster_metadata[:5], 1):
                print(f"      {i}. {meta['cluster_name']} (Score: {meta['ranking_score']:.1f}, {meta['alert_count']} alerts)")
        
        return cluster_metadata
    
    def _generate_cluster_id(self, cluster_alerts: List[Dict]) -> str:
        """
        Generate stable cluster id for catalog management based on pattern (not timestamp).
        Format: CL-{category}-{subcategory}-{top_service}
        
        This ensures the same pattern of alerts gets the same cluster ID across runs,
        allowing proper catalog matching.
        
        Args:
            cluster_alerts: List of alerts in the cluster
            
        Returns:
            String cluster ID based on alert pattern
        """
        import hashlib
        
        # Get most common category and subcategory
        categories = [a.get('alert_category', 'unknown') for a in cluster_alerts]
        subcategories = [a.get('alert_subcategory', 'unknown') for a in cluster_alerts]
        services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
        
        top_category = Counter(categories).most_common(1)[0][0] if categories else 'unknown'
        top_subcategory = Counter(subcategories).most_common(1)[0][0] if subcategories else 'unknown'
        top_service = Counter(services).most_common(1)[0][0] if services else 'unmapped'
        
        # Clean up for ID format
        cat_clean = re.sub(r'[^a-zA-Z0-9]', '', top_category.lower())[:10]
        subcat_clean = re.sub(r'[^a-zA-Z0-9]', '', top_subcategory.lower())[:10]
        svc_clean = re.sub(r'[^a-zA-Z0-9]', '', top_service.lower())[:15]
        
        # Create stable ID based on pattern
        pattern_str = f"{cat_clean}-{subcat_clean}-{svc_clean}"
        
        # Add short hash for uniqueness if needed
        pattern_hash = hashlib.md5(pattern_str.encode()).hexdigest()[:6]
        
        cluster_id = f"CL-{pattern_str}-{pattern_hash}"
        return cluster_id
    
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
        """
        Calculate ranking score for a cluster (0-100 scale).
        
        Weights:
        - Alert type repetition: 30% (higher repetition = more related alerts)
        - Severity impact: 25% (critical alerts rank higher)
        - Cluster size: 20% (larger clusters are more significant)
        - Service importance: 15% (PageRank importance)
        - Time concentration: 10% (alerts close in time are more urgent)
        
        Total: 100%
        """
        if not cluster_alerts:
            return 0
        
        score = 0
        
        # 1. Alert type repetition (30%) - higher repetition = more cohesive cluster
        alert_types = [a.get('alert_name', '') for a in cluster_alerts]
        if alert_types:
            unique_types = len(set(alert_types))
            total_alerts = len(alert_types)
            # Repetition ratio: 1 unique type out of 100 alerts = high repetition (good)
            repetition_ratio = 1 - (unique_types / total_alerts) if total_alerts > 0 else 0
            score += repetition_ratio * 30  # max 30 points
        
        # 2. Severity impact (25%) - critical alerts rank higher
        severities = [a.get('severity', '').lower() for a in cluster_alerts]
        severity_weights = {'critical': 1.0, 'high': 0.75, 'warning': 0.4, 'info': 0.1}
        if severities:
            avg_severity = sum(severity_weights.get(s, 0) for s in severities) / len(severities)
            score += avg_severity * 25  # max 25 points
        
        # 3. Cluster size (20%) - larger clusters are more significant
        # Scale: 1 alert = 0%, 50+ alerts = 100%
        size_ratio = min(len(cluster_alerts) / 50, 1.0)
        score += size_ratio * 20  # max 20 points
        
        # 4. Service importance (15%) - PageRank based
        services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
        if services and self._pagerank_cache:
            pageranks = [self._pagerank_cache.get(s, 0) for s in set(services)]
            if pageranks:
                # Normalize PageRank (typically 0-0.1 range)
                max_pr = max(self._pagerank_cache.values()) if self._pagerank_cache else 0.01
                avg_pagerank = np.mean(pageranks)
                pagerank_ratio = min(avg_pagerank / max_pr, 1.0) if max_pr > 0 else 0
                score += pagerank_ratio * 15  # max 15 points
        
        # 5. Time concentration (10%) - alerts close in time are more urgent
        timestamps = [a.get('start_timestamp', 0) for a in cluster_alerts if a.get('start_timestamp')]
        if timestamps and len(timestamps) > 1:
            time_span_minutes = (max(timestamps) - min(timestamps)) / 60
            # Score inversely proportional to time span
            # All within 5 min = 100%, spread over 60 min = ~10%
            concentration_ratio = 1 / (1 + time_span_minutes / 5)
            score += concentration_ratio * 10  # max 10 points
        elif timestamps:
            score += 10  # Single alert = fully concentrated
        
        return score
