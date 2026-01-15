"""
Result Storage: Handles persistence of clustering results, metrics, and outputs.
"""

import pandas as pd
import os
import json
from datetime import datetime
from typing import Dict, List
from collections import Counter
import networkx as nx


class ResultStorage:
    """Store and manage clustering results"""
    
    def __init__(self, output_dir='results', service_graph=None, consolidated_groups=None):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.service_graph = service_graph
        self.consolidated_groups = consolidated_groups or []
        # Cache for graph metrics
        self._pagerank_cache = {}
        self._betweenness_cache = {}
        
        # Compute graph metrics if service graph is provided
        if self.service_graph and len(self.service_graph.nodes()) > 0:
            try:
                self._pagerank_cache = nx.pagerank(self.service_graph, alpha=0.85)
                self._betweenness_cache = nx.betweenness_centrality(self.service_graph)
            except:
                pass
    
    def save_results(self, run_id: str, enriched_alerts: List[Dict], 
                    cluster_metadata: List[Dict], deduplicated_alerts: List[Dict],
                    clustering_stats: Dict) -> Dict[str, str]:
        """Save all results for a workflow run with two-level view"""
        run_dir = f'{self.output_dir}/{run_id}'
        os.makedirs(run_dir, exist_ok=True)
        
        output_paths = {}
        # Two-level view exports
        output_paths['level1'] = self._export_level1_cluster_view(run_dir, deduplicated_alerts)
        output_paths['level2'] = self._export_level2_alert_view(run_dir, deduplicated_alerts)
        
        # Additional exports
        output_paths['stats'] = self._save_clustering_stats(run_dir, clustering_stats)
        output_paths['mapping'] = self._save_mapping_stats(run_dir, enriched_alerts)
        output_paths['metadata'] = self._save_run_metadata(run_dir, run_id, enriched_alerts, cluster_metadata)
        
        print(f"  Results saved to: {run_dir}")
        return output_paths
    
    def _export_level1_cluster_view(self, run_dir: str, deduplicated_alerts: List[Dict]) -> str:
        """
        Export Level 1: Cluster-Level View
        Shows: cluster_name, cluster_description, primary_services, impacted_services, 
               root_cause_services, root_cause_description, blast_radius, scores
        """
        from datetime import datetime, timezone
        
        print("    Generating Level 1: Cluster-Level View...")
        
        cluster_data = []
        
        # Group deduplicated alerts by cluster_id
        cluster_alerts_dict = {}
        for alert in deduplicated_alerts:
            cluster_id = alert.get('cluster_id', -1)
            if cluster_id == -1:
                continue
            if cluster_id not in cluster_alerts_dict:
                cluster_alerts_dict[cluster_id] = []
            cluster_alerts_dict[cluster_id].append(alert)
        
        for cluster_id in sorted(cluster_alerts_dict.keys(), key=lambda x: str(x)):
            alerts = cluster_alerts_dict[cluster_id]
            
            # Get cluster metadata
            cluster_name = alerts[0].get('cluster_name', f'cluster_{cluster_id}') if alerts else f'cluster_{cluster_id}'
            cluster_rank = alerts[0].get('cluster_rank', -1) if alerts else -1
            cluster_score = alerts[0].get('cluster_score', 0) if alerts else 0
            
            # Extract primary services (services with alerts)
            services = [a.get('graph_service', '') for a in alerts if a.get('graph_service')]
            service_counts = Counter(services)
            primary_services = [f"{svc} ({count})" for svc, count in service_counts.most_common(5)]
            
            # Get consolidated group for graph metrics
            group = None
            for g in self.consolidated_groups:
                if g.get('group_id') == alerts[0].get('initial_group_id', -1):
                    group = g
                    break
            
            # Extract metrics from group
            impacted_services = []
            blast_radius = 0
            root_cause_services = []
            root_cause_description = ''
            impact_propagation_score = 0.0
            criticality_score = 0.0
            
            if group:
                impacted_services = group.get('impacted_services', [])
                blast_radius = group.get('blast_radius', 0)
                root_cause_services = group.get('root_cause_services', [])
                root_cause_description = group.get('root_cause_description', '')
                impact_propagation_score = group.get('impact_propagation_score', 0.0)
                criticality_score = group.get('criticality_score', 0.0)
            
            # Calculate metadata for cluster
            severities = [a.get('severity', '').lower() for a in alerts]
            categories = [a.get('alert_category', '') for a in alerts if a.get('alert_category')]
            subcategories = [a.get('alert_subcategory', '') for a in alerts if a.get('alert_subcategory')]
            
            severity_dist = Counter(severities)
            category = Counter(categories).most_common(1)[0][0] if categories else 'unknown'
            subcategory = Counter(subcategories).most_common(1)[0][0] if subcategories else ''
       
            primary_count = len(set(services)) if services else 0
            primary_list = ', '.join([s.split('(')[0].strip() for s in primary_services[:5]]) if primary_services else 'none'
            
            # Impacted services
            impacted_count = len(impacted_services) if impacted_services else 0
            impacted_list = ', '.join(impacted_services[:5]) if impacted_services else 'none'
            
            # Root cause services
            root_cause_count = len(root_cause_services) if root_cause_services else 0
            root_cause_list = ', '.join(root_cause_services[:5]) if root_cause_services else 'none identified'
            
            # Generate detailed cluster description
            cluster_description_detailed = (
                f"Observing issue where {primary_count} primary services: {primary_list}; "
                f"with {impacted_count} impacted dependency services: {impacted_list}, "
                f"and with {root_cause_count} potential root causes services: {root_cause_list}"
            )
            
            # Timestamps
            created_ts = datetime.now(tz=timezone.utc)
            
            timestamps = [a.get('start_timestamp', 0) for a in alerts if a.get('start_timestamp')]
            if timestamps:
                start_ts = datetime.fromtimestamp(min(timestamps), tz=timezone.utc)
                end_ts = datetime.fromtimestamp(max(timestamps), tz=timezone.utc)
            else:
                start_ts = None
                end_ts = None
            
            # Collect all alert IDs in this cluster (deduplicated alerts)
            alert_ids = [a.get('alert_id', '') or a.get('_id', '') for a in alerts if a.get('alert_id') or a.get('_id')]
            alert_ids_str = str(alert_ids)  # Store as string representation of list
            
            cluster_data.append({
                'cluster_rank': cluster_rank,
                'cluster_id': cluster_id,
                'cluster_name': cluster_name,
                'cluster_score': cluster_score,
                'cluster_description_detailed': cluster_description_detailed,
                'created_ts': created_ts,
                'start_ts': start_ts,
                'end_ts': end_ts,
                'alert_count': len(alerts),
                'alert_ids': alert_ids_str,
                'unique_alert_types': len(set(a.get('alert_name', '') for a in alerts)),
                'primary_services': ', '.join(primary_services) if primary_services else 'unmapped',
                'primary_service_count': len(set(services)),
                'impacted_services': ', '.join(impacted_services[:10]) if impacted_services else 'none',
                'impacted_services_count': len(impacted_services),
                'blast_radius': blast_radius,
                'root_cause_services': ', '.join(root_cause_services) if root_cause_services else 'none identified',
                'root_cause_description': root_cause_description,
                'impact_propagation_score': impact_propagation_score,
                'criticality_score': criticality_score,
                'severity_distribution': str(dict(severity_dist)),
                'most_common_category': category,
                'most_common_subcategory': subcategory,
                'namespaces': ', '.join(sorted(set(a.get('namespace', '') for a in alerts if a.get('namespace')))[:5]),
                'clustering_method': alerts[0].get('clustering_method', '') if alerts else ''
            })
        
        df_level1 = pd.DataFrame(cluster_data)
        if not df_level1.empty:
            df_level1 = df_level1.sort_values('cluster_rank')
        
        level1_path = f'{run_dir}/cluster_level_view.csv'
        df_level1.to_csv(level1_path, index=False)
        print(f"    Level 1 - Cluster View: {level1_path}")
        
        return level1_path
    
    def _export_level2_alert_view(self, run_dir: str, deduplicated_alerts: List[Dict]) -> str:
        """
        Export Level 2: Alert Detail View
        Shows: deduplicated, enriched alerts with service graph metrics
        (pagerank, betweenness, blast_radius, impact_propagation_score, criticality_score)
        """
        print("    Generating Level 2: Alert Detail View...")
        
        alert_data = []
        
        for alert in deduplicated_alerts:
            graph_service = alert.get('graph_service', '')
            
            # Calculate service graph metrics for this alert's service
            service_metrics = self._calculate_service_graph_metrics(graph_service)
            
            # Find the group for impact/criticality scores
            initial_group_id = alert.get('initial_group_id', -1)
            impact_propagation_score = 0.0
            criticality_score = 0.0
            
            for group in self.consolidated_groups:
                if group.get('group_id') == initial_group_id:
                    impact_propagation_score = group.get('impact_propagation_score', 0.0)
                    criticality_score = group.get('criticality_score', 0.0)
                    break
            
            alert_data.append({
                # Cluster context
                'cluster_rank': alert.get('cluster_rank', -1),
                'cluster_id': alert.get('cluster_id', -1),
                'cluster_name': alert.get('cluster_name', ''),
                
                # Alert identity
                'alert_name': alert.get('alert_name', ''),
                'severity': alert.get('severity', ''),
                'alert_category': alert.get('alert_category', ''),
                'alert_subcategory': alert.get('alert_subcategory', ''),
                'description': alert.get('description', ''),
                
                # Service info
                'service_name': alert.get('service_name', ''),
                'graph_service': graph_service,
                'namespace': alert.get('namespace', ''),
                'cluster': alert.get('cluster', ''),
                'pod': alert.get('pod', ''),
                'node': alert.get('node', ''),
                'workload_type': alert.get('workload_type', ''),
                
                # Service graph metrics
                'pagerank': service_metrics['pagerank'],
                'betweenness': service_metrics['betweenness'],
                'degree': service_metrics['degree'],
                'in_degree': service_metrics['in_degree'],
                'out_degree': service_metrics['out_degree'],
                'blast_radius': service_metrics['blast_radius'],
                'impacted_services': ', '.join(service_metrics['impacted_services'][:10]),
                'impacted_count': len(service_metrics['impacted_services']),
                'upstream_services': ', '.join(service_metrics['upstream_services'][:5]),
                'upstream_count': len(service_metrics['upstream_services']),
                'downstream_services': ', '.join(service_metrics['downstream_services'][:5]),
                'downstream_count': len(service_metrics['downstream_services']),
                
                # Scores
                'impact_propagation_score': impact_propagation_score,
                'criticality_score': criticality_score,
                
                # Mapping info
                'mapping_method': alert.get('mapping_method', ''),
                'mapping_confidence': alert.get('mapping_confidence', 0),
                'clustering_method': alert.get('clustering_method', ''),
                
                # Temporal
                'starts_at': alert.get('startsAt', ''),
                'start_timestamp': alert.get('start_timestamp', 0),
            })
        
        df_level2 = pd.DataFrame(alert_data)
        if not df_level2.empty:
            df_level2 = df_level2.sort_values(['cluster_rank', 'cluster_id', 'start_timestamp'])
        
        level2_path = f'{run_dir}/alert_detail_view.csv'
        df_level2.to_csv(level2_path, index=False)
        print(f"    Level 2 - Alert Detail View: {level2_path}")
        
        return level2_path
    
    def _calculate_service_graph_metrics(self, service_name):
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
        
        if not service_name or not self.service_graph or not self.service_graph.has_node(service_name):
            return metrics
        
        # PageRank
        if self._pagerank_cache and service_name in self._pagerank_cache:
            metrics['pagerank'] = round(self._pagerank_cache[service_name], 6)
        
        # Betweenness centrality
        if self._betweenness_cache and service_name in self._betweenness_cache:
            metrics['betweenness'] = round(self._betweenness_cache[service_name], 6)
        
        # Degree metrics
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
    
    def _get_transitive_downstream(self, service_name, max_depth=2):
        """Get all services reachable downstream from the given service (BFS with depth limit)"""
        if not self.service_graph or not self.service_graph.has_node(service_name):
            return set()
        
        visited = set()
        queue = [(service_name, 0)]
        
        while queue:
            current, depth = queue.pop(0)
            
            if depth >= max_depth:
                continue
            
            for successor in self.service_graph.successors(current):
                if successor not in visited:
                    visited.add(successor)
                    queue.append((successor, depth + 1))
        
        return visited
    
    def _save_clustering_stats(self, run_dir: str, clustering_stats: Dict) -> str:
        """Save clustering statistics"""
        stats_converted = {}
        for key, value in clustering_stats.items():
            if hasattr(value, 'item'): 
                stats_converted[key] = value.item()
            elif isinstance(value, (int, float, str, bool, type(None))):
                stats_converted[key] = value
            else:
                stats_converted[key] = str(value)
        
        output_path = f'{run_dir}/clustering_statistics.json'
        with open(output_path, 'w') as f:
            json.dump(stats_converted, f, indent=2)
        
        return output_path
    
    def _save_mapping_stats(self, run_dir: str, enriched_alerts: List[Dict]) -> str:
        """Save mapping statistics"""
        mapping_stats = {
            'direct': 0,
            'fallback': 0,
            'unmapped': 0
        }
        
        for alert in enriched_alerts:
            method = alert.get('mapping_method', 'unmapped')
            if method == 'service_name':
                mapping_stats['direct'] += 1
            elif method == 'namespace_cluster_fallback':
                mapping_stats['fallback'] += 1
            else:
                mapping_stats['unmapped'] += 1
        
        mapping_df = pd.DataFrame([mapping_stats])
        output_path = f'{run_dir}/mapping_statistics.csv'
        mapping_df.to_csv(output_path, index=False)
        
        return output_path
    
    def _save_run_metadata(self, run_dir: str, run_id: str, enriched_alerts: List[Dict], 
                          cluster_metadata: List[Dict]) -> str:
        """Save run metadata"""
        metadata = {
            'run_id': str(run_id),
            'timestamp': datetime.now().isoformat(),
            'total_alerts': int(len(enriched_alerts)),
            'total_clusters': int(len(cluster_metadata)),
            'unique_services': int(len(set(a.get('service_name', '') for a in enriched_alerts if a.get('service_name')))),
            'duplicates_removed': int(sum(1 for a in enriched_alerts if a.get('is_duplicate'))),
            'unmapped_alerts': int(sum(1 for a in enriched_alerts if a.get('mapping_method') == 'unmapped')),
        }
        
        output_path = f'{run_dir}/run_metadata.json'
        with open(output_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return output_path
    
    def get_latest_run_id(self) -> str:
        """Get the most recent run ID"""
        if not os.path.exists(self.output_dir):
            return None
        
        run_dirs = [d for d in os.listdir(self.output_dir) 
                   if os.path.isdir(os.path.join(self.output_dir, d))]
        
        if not run_dirs:
            return None
        run_dirs.sort(reverse=True)
        return run_dirs[0]
    
    def load_run_results(self, run_id: str = None) -> Dict:
        """Load results from a specific run (or latest)"""
        if run_id is None:
            run_id = self.get_latest_run_id()
        
        if run_id is None:
            return None
        
        run_dir = f'{self.output_dir}/{run_id}'
        
        if not os.path.exists(run_dir):
            return None
        
        results = {
            'run_id': run_id,
            'cluster_view': None,
            'alert_detail': None,
            'metadata': None
        }
        
        # Load Level 1: Cluster View
        level1_path = f'{run_dir}/cluster_level_view.csv'
        if os.path.exists(level1_path):
            results['cluster_view'] = pd.read_csv(level1_path)
        
        # Load Level 2: Alert Detail View
        level2_path = f'{run_dir}/alert_detail_view.csv'
        if os.path.exists(level2_path):
            results['alert_detail'] = pd.read_csv(level2_path)
        
        # Load metadata
        metadata_path = f'{run_dir}/run_metadata.json'
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                results['metadata'] = json.load(f)
        
        return results
