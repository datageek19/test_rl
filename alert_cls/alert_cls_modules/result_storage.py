"""
Result Storage

    Handles persistence of clustering results, metrics, and outputs.
"""

import pandas as pd
import os
import json
from datetime import datetime
from typing import Dict, List
from collections import Counter


class ResultStorage:
    """Store and manage clustering results"""
    
    def __init__(self, output_dir='results'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def save_results(self, run_id: str, enriched_alerts: List[Dict], 
                    cluster_metadata: List[Dict], deduplicated_alerts: List[Dict],
                    clustering_stats: Dict) -> Dict[str, str]:
        """Save all results for a workflow run"""
        
        # Create run-specific directory
        run_dir = f'{self.output_dir}/{run_id}'
        os.makedirs(run_dir, exist_ok=True)
        
        output_paths = {}
        
        # Save main consolidated alerts
        output_paths['main'] = self._save_consolidated_alerts(run_dir, enriched_alerts)
        
        # Save cluster summary
        output_paths['cluster_summary'] = self._save_cluster_summary(run_dir, cluster_metadata)
        
        # Save ranked clusters
        output_paths['ranked_clusters'] = self._save_ranked_clusters(run_dir, cluster_metadata)
        
        # Save deduplicated alerts
        output_paths['deduplicated'] = self._save_deduplicated_alerts(run_dir, deduplicated_alerts)
        
        # Save detailed cluster view
        output_paths['cluster_detail'] = self._save_cluster_detail_view(run_dir, enriched_alerts, cluster_metadata)
        
        # Save clustering statistics
        output_paths['stats'] = self._save_clustering_stats(run_dir, clustering_stats)
        
        # Save mapping statistics
        output_paths['mapping'] = self._save_mapping_stats(run_dir, enriched_alerts)
        
        # Save run metadata
        output_paths['metadata'] = self._save_run_metadata(run_dir, run_id, enriched_alerts, cluster_metadata)
        
        print(f"  Results saved to: {run_dir}")
        return output_paths
    
    def _save_consolidated_alerts(self, run_dir: str, enriched_alerts: List[Dict]) -> str:
        """Save main consolidated alerts"""
        output_data = []
        
        for i, alert in enumerate(enriched_alerts):
            output_data.append({
                'alert_id': i,
                'final_group_id': alert.get('cluster_id', -1),
                'initial_group_id': alert.get('initial_group_id', -1),
                'clustering_method': alert.get('clustering_method', ''),
                'is_duplicate': alert.get('is_duplicate', False),
                'duplicate_of': alert.get('duplicate_of', ''),
                
                # Cluster info
                'cluster_name': alert.get('cluster_name', ''),
                'cluster_rank': alert.get('cluster_rank', -1),
                'cluster_score': alert.get('cluster_score', 0),
                
                # Alert info
                'alert_name': alert.get('alert_name', ''),
                'severity': alert.get('severity', ''),
                'service_name': alert.get('service_name', ''),
                'namespace': alert.get('namespace', ''),
                'cluster': alert.get('cluster', ''),
                'pod': alert.get('pod', ''),
                'node': alert.get('node', ''),
                
                # Graph mapping
                'graph_service': alert.get('graph_service', ''),
                'mapping_method': alert.get('mapping_method', ''),
                'mapping_confidence': alert.get('mapping_confidence', 0),
                
                # Temporal
                'starts_at': alert.get('startsAt', ''),
                'start_timestamp': alert.get('start_timestamp', 0),
                
                # Metadata
                'alert_category': alert.get('alert_category', ''),
                'alert_subcategory': alert.get('alert_subcategory', ''),
                'workload_type': alert.get('workload_type', ''),
                'description': alert.get('description', ''),
            })
        
        df_output = pd.DataFrame(output_data)
        output_path = f'{run_dir}/alert_consolidation_final.csv'
        df_output.to_csv(output_path, index=False)
        
        return output_path
    
    def _save_cluster_summary(self, run_dir: str, cluster_metadata: List[Dict]) -> str:
        """Save cluster summary"""
        df_summary = pd.DataFrame(cluster_metadata)
        if not df_summary.empty:
            df_summary = df_summary.sort_values('ranking_score', ascending=False)
        
        output_path = f'{run_dir}/cluster_summary.csv'
        df_summary.to_csv(output_path, index=False)
        
        return output_path
    
    def _save_ranked_clusters(self, run_dir: str, cluster_metadata: List[Dict]) -> str:
        """Save ranked clusters"""
        df_ranked = pd.DataFrame(cluster_metadata)
        output_path = f'{run_dir}/ranked_clusters.csv'
        df_ranked.to_csv(output_path, index=False)
        
        return output_path
    
    def _save_deduplicated_alerts(self, run_dir: str, deduplicated_alerts: List[Dict]) -> str:
        """Save deduplicated alerts"""
        dedup_data = []
        
        for alert in deduplicated_alerts:
            dedup_data.append({
                'alert_name': alert.get('alert_name', ''),
                'service_name': alert.get('service_name', ''),
                'graph_service': alert.get('graph_service', ''),
                'cluster_id': alert.get('cluster_id', -1),
                'cluster_name': alert.get('cluster_name', ''),
                'namespace': alert.get('namespace', ''),
                'cluster': alert.get('cluster', ''),
                'severity': alert.get('severity', ''),
                'alert_category': alert.get('alert_category', ''),
                'alert_subcategory': alert.get('alert_subcategory', ''),
                'starts_at': alert.get('startsAt', ''),
            })
        
        df_dedup = pd.DataFrame(dedup_data)
        output_path = f'{run_dir}/deduplicated_alerts.csv'
        df_dedup.to_csv(output_path, index=False)
        
        return output_path
    
    def _save_cluster_detail_view(self, run_dir: str, enriched_alerts: List[Dict], 
                                  cluster_metadata: List[Dict]) -> str:
        """Save detailed cluster-based view"""
        ranked_dict = {meta['cluster_id']: meta for meta in cluster_metadata}
        
        cluster_details = []
        
        for alert in enriched_alerts:
            cluster_id = alert.get('cluster_id', -1)
            
            if cluster_id == -1:
                continue
            
            cluster_meta = ranked_dict.get(cluster_id, {})
            graph_info = alert.get('graph_info', {})
            dependencies = alert.get('dependencies', {})
            
            alert_detail = {
                # Cluster metadata
                'cluster_id': cluster_id,
                'cluster_name': cluster_meta.get('cluster_name', f'cluster_{cluster_id}'),
                'cluster_rank': cluster_meta.get('rank', -1),
                'cluster_score': cluster_meta.get('ranking_score', 0),
                
                # Alert identity
                'alert_name': alert.get('alert_name', ''),
                'severity': alert.get('severity', ''),
                'alert_category': alert.get('alert_category', ''),
                'alert_subcategory': alert.get('alert_subcategory', ''),
                'description': alert.get('description', ''),
                
                # Alert location
                'service_name': alert.get('service_name', ''),
                'namespace': alert.get('namespace', ''),
                'pod': alert.get('pod', ''),
                'node': alert.get('node', ''),
                'cluster': alert.get('cluster', ''),
                
                # Service graph information
                'graph_service': alert.get('graph_service', ''),
                'graph_service_type': graph_info.get('type', '') if graph_info else '',
                'upstream_services': ', '.join([d['service'] for d in dependencies.get('upstream', [])]),
                'downstream_services': ', '.join([d['service'] for d in dependencies.get('downstream', [])]),
                'upstream_count': len(dependencies.get('upstream', [])),
                'downstream_count': len(dependencies.get('downstream', [])),
                
                # Mapping info
                'mapping_method': alert.get('mapping_method', ''),
                'mapping_confidence': alert.get('mapping_confidence', 0),
                
                # Temporal
                'starts_at': alert.get('startsAt', ''),
                'start_timestamp': alert.get('start_timestamp', 0),
                
                # Additional metadata
                'workload_type': alert.get('workload_type', ''),
                'is_duplicate': alert.get('is_duplicate', False),
            }
            
            cluster_details.append(alert_detail)
        
        df_details = pd.DataFrame(cluster_details)
        
        if not df_details.empty:
            df_details = df_details.sort_values(['cluster_rank', 'cluster_id', 'start_timestamp'], 
                                                 ascending=[True, True, True])
        
        output_path = f'{run_dir}/alerts_by_cluster_detailed.csv'
        df_details.to_csv(output_path, index=False)
        
        return output_path
    
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
        
        # Sort by timestamp (run_id format: YYYYMMDD_HHMMSS)
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
            'alerts': None,
            'clusters': None,
            'metadata': None
        }
        
        # Load main alerts
        alerts_path = f'{run_dir}/alert_consolidation_final.csv'
        if os.path.exists(alerts_path):
            results['alerts'] = pd.read_csv(alerts_path)
        
        # Load cluster summary
        clusters_path = f'{run_dir}/cluster_summary.csv'
        if os.path.exists(clusters_path):
            results['clusters'] = pd.read_csv(clusters_path)
        
        # Load metadata
        metadata_path = f'{run_dir}/run_metadata.json'
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                results['metadata'] = json.load(f)
        
        return results

