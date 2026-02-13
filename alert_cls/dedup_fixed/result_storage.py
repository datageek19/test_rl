
import pandas as pd
import os
import json
from datetime import datetime
from typing import Dict, List, Optional
from collections import Counter
import networkx as nx
import numpy as np

from config import Config


def convert_to_json_serializable(obj):
    """Convert non-JSON-serializable objects to serializable formats."""
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_to_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]
    else:
        return obj


class ResultStorage:
    
    def __init__(self, output_dir='results', service_graph=None, consolidated_groups=None, catalog_manager=None):

        self.output_dir = Config.ARTIFACTS_DIR or output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.service_graph = service_graph
        self.consolidated_groups = consolidated_groups or []
        self.catalog_manager = catalog_manager
        self._pagerank_cache = {}
        self._betweenness_cache = {}
        
        if self.service_graph and len(self.service_graph.nodes()) > 0:
            self._compute_graph_metrics()
    
    def set_graph_context(self, service_graph, consolidated_groups=None, 
                          pagerank_cache=None, betweenness_cache=None, catalog_manager=None):
        self.service_graph = service_graph
        self.consolidated_groups = consolidated_groups or []
        if catalog_manager:
            self.catalog_manager = catalog_manager
        
        if pagerank_cache:
            self._pagerank_cache = pagerank_cache
        elif self.service_graph and len(self.service_graph.nodes()) > 0:
            try:
                self._pagerank_cache = nx.pagerank(self.service_graph, alpha=0.85)
            except Exception as e:
                print(f"  Warning: Could not compute pagerank: {e}")
                self._pagerank_cache = {}
        
        if betweenness_cache:
            self._betweenness_cache = betweenness_cache
        elif self.service_graph and len(self.service_graph.nodes()) > 0:
            try:
                self._betweenness_cache = nx.betweenness_centrality(self.service_graph)
            except Exception as e:
                print(f"  Warning: Could not compute betweenness: {e}")
                self._betweenness_cache = {}
    
    def _compute_graph_metrics(self):
        if not self.service_graph or len(self.service_graph.nodes()) == 0:
            return
        try:
            self._pagerank_cache = nx.pagerank(self.service_graph, alpha=0.85)
            self._betweenness_cache = nx.betweenness_centrality(self.service_graph)
        except Exception as e:
            print(f"  Warning: Could not compute graph metrics: {e}")
    
    def save_results(self, run_id: str, enriched_alerts: List[Dict], 
                    cluster_metadata: List[Dict], deduplicated_alerts: List[Dict],
                    clustering_stats: Dict, duplicate_groups: List[Dict] = None,
                    duplicate_alerts: List[Dict] = None) -> Dict[str, str]:
        run_dir = os.path.join(self.output_dir, run_id)
        os.makedirs(run_dir, exist_ok=True)
        
        output_paths = {}
        output_paths['level1'] = self._export_level1_cluster_view(run_dir, deduplicated_alerts)
        output_paths['level2'] = self._export_level2_alert_view(run_dir, deduplicated_alerts)
        
        output_paths['stats'] = self._save_clustering_stats(run_dir, clustering_stats)
        output_paths['mapping'] = self._save_mapping_stats(run_dir, enriched_alerts)
        output_paths['metadata'] = self._save_run_metadata(run_dir, run_id, enriched_alerts, cluster_metadata)
        
        if duplicate_groups:
            output_paths['duplicates'] = self._save_duplicate_groups(run_dir, duplicate_groups)
        
        if duplicate_alerts:
            output_paths['duplicate_alerts'] = self._save_duplicate_alerts_list(run_dir, duplicate_alerts)
        
        print(f"  Results saved to: {run_dir}")
        return output_paths
    
    def _get_transitive_downstream(self, service_name: str, max_depth: int = 2) -> set:
        impacted = set()
        if not self.service_graph or not self.service_graph.has_node(service_name):
            return impacted
        
        visited = set()
        queue = [(service_name, 0)]
        
        while queue:
            current, depth = queue.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            
            if depth > 0:
                impacted.add(current)
            
            for successor in self.service_graph.successors(current):
                if successor not in visited:
                    queue.append((successor, depth + 1))
        
        return impacted
    
    def _save_duplicate_groups(self, run_dir: str, duplicate_groups: List[Dict]) -> str:
        output_path = os.path.join(run_dir, 'duplicate_groups.json')
        
        duplicate_data = {
            'total_duplicate_groups': len(duplicate_groups),
            'total_duplicates': sum(g['count'] - 1 for g in duplicate_groups),
            'groups': convert_to_json_serializable(duplicate_groups)
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(duplicate_data, f, indent=2, ensure_ascii=False)
        
        return output_path
    
    def _save_duplicate_alerts_list(self, run_dir: str, duplicate_alerts: List[Dict]) -> str:
        output_path = os.path.join(run_dir, 'duplicate_alerts_list.json')
        
        duplicate_data = {
            'total_duplicate_alerts': len(duplicate_alerts),
            'alerts': convert_to_json_serializable(duplicate_alerts)
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(duplicate_data, f, indent=2, ensure_ascii=False)
        
        return output_path
    
    def _compute_cluster_graph_metrics(self, cluster_services: list, alerts: list) -> dict:
        metrics = {
            'impacted_services': [],
            'blast_radius': 0
        }
        
        if not self.service_graph or not cluster_services:
            return metrics
        
        all_impacted = set()
        service_blast_radii = []
        
        for svc in cluster_services:
            if self.service_graph.has_node(svc):
                impacted = self._get_transitive_downstream(svc, max_depth=2)
                all_impacted.update(impacted)
                service_blast_radii.append({'service': svc, 'blast_radius': len(impacted)})
        
        metrics['impacted_services'] = list(all_impacted)
        metrics['blast_radius'] = len(all_impacted)
        
        service_blast_radii.sort(key=lambda x: x['blast_radius'], reverse=True)
        return metrics
    
    def _export_level1_cluster_view(self, run_dir: str, deduplicated_alerts: List[Dict]) -> str:
        from datetime import datetime, timezone
        
        print("    Generating Level 1: Cluster-Level View...")
        
        cluster_data = []
        
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
            
            cluster_name = alerts[0].get('cluster_name', f'cluster_{cluster_id}') if alerts else f'cluster_{cluster_id}'
            cluster_rank = alerts[0].get('cluster_rank', -1) if alerts else -1
            cluster_score = alerts[0].get('cluster_score', 0) if alerts else 0
            
            services = [a.get('graph_service', '') for a in alerts if a.get('graph_service')]
            service_counts = Counter(services)
            primary_services = [f"{svc} ({count})" for svc, count in service_counts.most_common(5)]
            
            unique_services = list(set(services))
            
            cluster_metrics = self._compute_cluster_graph_metrics(unique_services, alerts)
            
            impacted_services = cluster_metrics['impacted_services']
            blast_radius = cluster_metrics['blast_radius']
            
            severities = [a.get('severity', '').lower() for a in alerts]
            categories = [a.get('alert_category', '') for a in alerts if a.get('alert_category')]
            subcategories = [a.get('alert_subcategory', '') for a in alerts if a.get('alert_subcategory')]
            
            severity_dist = Counter(severities)
            category = Counter(categories).most_common(1)[0][0] if categories else 'unknown'
            subcategory = Counter(subcategories).most_common(1)[0][0] if subcategories else ''
       
            top_service = service_counts.most_common(1)[0][0] if service_counts else None
            
            downstream_count = 0
            upstream_count = 0
            service_found_in_graph = False
            
            if top_service and self.service_graph and self.service_graph.has_node(top_service):
                service_found_in_graph = True
                downstream_count = len(list(self.service_graph.successors(top_service)))
                upstream_count = len(list(self.service_graph.predecessors(top_service)))
            
            if not top_service:
                cluster_description_detailed = "Observing issues with unknown service name (not found), no upstream or downstream dependencies found"
            elif not service_found_in_graph:
                cluster_description_detailed = f"Observing issues with {top_service} (not found in service graph), no upstream or downstream dependencies found"
            elif upstream_count == 0 and downstream_count == 0:
                cluster_description_detailed = f"Observing issues with {top_service}, no upstream or downstream dependencies found"
            else:
                dependency_parts = []
                if upstream_count > 0:
                    dependency_parts.append(f"{upstream_count} upstream")
                if downstream_count > 0:
                    dependency_parts.append(f"{downstream_count} downstream")
                dependency_str = " and ".join(dependency_parts) + " dependencies"
                cluster_description_detailed = f"Observing issues with {top_service}, where {dependency_str}"
            
            created_ts = datetime.now(tz=timezone.utc)
            
            all_timestamps = []
            for a in alerts:
                ts = a.get('start_timestamp')
                if ts and ts > 0:
                    all_timestamps.append(ts)
                    continue
                
                start_dt = a.get('start_datetime')
                if start_dt and pd.notna(start_dt):
                    try:
                        all_timestamps.append(pd.Timestamp(start_dt).timestamp())
                        continue
                    except:
                        pass
                
                starts_at = a.get('startsAt') or a.get('starts_at')
                if starts_at:
                    try:
                        all_timestamps.append(pd.to_datetime(starts_at).timestamp())
                        continue
                    except:
                        pass
                
                ends_at = a.get('endsAt') or a.get('ends_at')
                if ends_at:
                    try:
                        all_timestamps.append(pd.to_datetime(ends_at).timestamp())
                        continue
                    except:
                        pass
                
                updated_at = a.get('updatedAt') or a.get('updated_at')
                if updated_at:
                    try:
                        all_timestamps.append(pd.to_datetime(updated_at).timestamp())
                    except:
                        pass
            
            if all_timestamps:
                start_ts = datetime.fromtimestamp(min(all_timestamps), tz=timezone.utc)
                end_ts = datetime.fromtimestamp(max(all_timestamps), tz=timezone.utc)
            else:
                start_ts = None
                end_ts = None
    
            cluster_data.append({
                'cluster_rank': cluster_rank,
                'cluster_id': cluster_id,
                'cluster_name': cluster_name,
                'cluster_score': cluster_score,
                'cluster_description_detailed': cluster_description_detailed,
                'created_ts': created_ts,
                'start_ts': start_ts,
                'end_ts': end_ts,
                'current_alert_count': len(alerts),
                'unique_alert_types': len(set(a.get('alert_name', '') for a in alerts)),
                'primary_services': ', '.join(primary_services) if primary_services else 'unmapped',
                'primary_service_count': len(set(services)),
                'impacted_services': ', '.join(impacted_services[:10]) if impacted_services else 'none',
                'impacted_services_count': len(impacted_services),
                'blast_radius': blast_radius,
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
        print("    Generating Level 2: Alert Detail View...")
        
        alert_data = []
        
        for alert in deduplicated_alerts:
            graph_service = alert.get('graph_service', '')
            
            service_metrics = self._calculate_service_graph_metrics(graph_service)
          
            alert_data.append({
                'cluster_rank': alert.get('cluster_rank', -1),
                'cluster_id': alert.get('cluster_id', -1),
                'cluster_name': alert.get('cluster_name', ''),
                
                'alert_name': alert.get('alert_name', ''),
                'severity': alert.get('severity', ''),
                'alert_category': alert.get('alert_category', ''),
                'alert_subcategory': alert.get('alert_subcategory', ''),
                'description': alert.get('description', ''),
                
                'service_name': alert.get('service_name', ''),
                'graph_service': graph_service,
                'namespace': alert.get('namespace', ''),
                'cluster': alert.get('cluster', ''),
                
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
                'mapping_method': alert.get('mapping_method', ''),
                'mapping_confidence': alert.get('mapping_confidence', 0),
                'clustering_method': alert.get('clustering_method', ''),
                
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
        
        if self._pagerank_cache and service_name in self._pagerank_cache:
            metrics['pagerank'] = round(self._pagerank_cache[service_name], 6)
        
        if self._betweenness_cache and service_name in self._betweenness_cache:
            metrics['betweenness'] = round(self._betweenness_cache[service_name], 6)
        
        metrics['degree'] = self.service_graph.degree(service_name)
        metrics['in_degree'] = self.service_graph.in_degree(service_name)
        metrics['out_degree'] = self.service_graph.out_degree(service_name)
        
        metrics['upstream_services'] = list(self.service_graph.predecessors(service_name))
        
        metrics['downstream_services'] = list(self.service_graph.successors(service_name))
        
        impacted = self._get_transitive_downstream(service_name, max_depth=2)
        metrics['impacted_services'] = list(impacted)
        metrics['blast_radius'] = len(impacted)
        
        return metrics
    
    def _save_clustering_stats(self, run_dir: str, clustering_stats: Dict) -> str:
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
    
    def get_latest_run_id(self) -> Optional[str]:
        if not os.path.exists(self.output_dir):
            return None
        
        run_dirs = [d for d in os.listdir(self.output_dir) 
                   if os.path.isdir(os.path.join(self.output_dir, d))]
        
        if not run_dirs:
            return None
        run_dirs.sort(reverse=True)
        return run_dirs[0]
    
    def load_run_results(self, run_id: str = None) -> Optional[Dict]:
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
        
        level1_path = f'{run_dir}/cluster_level_view.csv'
        if os.path.exists(level1_path):
            results['cluster_view'] = pd.read_csv(level1_path)
        
        level2_path = f'{run_dir}/alert_detail_view.csv'
        if os.path.exists(level2_path):
            results['alert_detail'] = pd.read_csv(level2_path)
        
        metadata_path = f'{run_dir}/run_metadata.json'
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                results['metadata'] = json.load(f)
        
        return results
