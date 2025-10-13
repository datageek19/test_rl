import networkx as nx
import pandas as pd
import json
import ast
import numpy as np
from collections import Counter, defaultdict
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.metrics import silhouette_score
import warnings
warnings.filterwarnings('ignore')


class AlertConsolidationClustering:
    def __init__(self, alerts_csv_path: str, graph_json_path: str):
        self.alerts_csv_path = alerts_csv_path
        self.graph_json_path = graph_json_path
        self.service_graph = nx.DiGraph()
        self.service_to_graph = {}
        self.alerts_df = None
        self.consolidated_groups = []
        self.clustering_results = {}
        self.cluster_patterns = {}
        
    def load_and_process_alerts(self):
        df_raw = pd.read_csv(self.alerts_csv_path, dtype=str, on_bad_lines='skip')
        df_raw = df_raw.rename(columns={df_raw.columns[0]: "attribute", df_raw.columns[-1]: "value"})
        id_cols = [c for c in df_raw.columns if c not in ("attribute", "value")]
        df_pivoted = df_raw.pivot_table(index=id_cols, columns='attribute', values='value', aggfunc='first')
        
        conflicting = [name for name in df_pivoted.index.names if name in df_pivoted.columns]
        if conflicting:
            df_pivoted = df_pivoted.drop(columns=conflicting)
        df_pivoted = df_pivoted.reset_index()
        
        if 'status' in df_pivoted.columns:
            df_pivoted = df_pivoted[df_pivoted['status'].str.strip().str.lower() == 'firing']
        
        firing_alerts = df_pivoted.to_dict('records')
        
        for alert in firing_alerts:
            labels_str = alert.get('labels', '')
            if labels_str:
                try:
                    labels = ast.literal_eval(labels_str)
                    alert['service_name'] = labels.get('service_name', '')
                    alert['namespace'] = labels.get('namespace', '')
                    alert['cluster'] = labels.get('cluster', '')
                except:
                    pass
            
            try:
                starts_at = alert.get('startsAt') or alert.get('starts_at', '')
                if starts_at:
                    alert['start_datetime'] = pd.to_datetime(starts_at)
            except:
                alert['start_datetime'] = None
        
        return firing_alerts
    
    def load_service_graph(self):
        with open(self.graph_json_path, 'r') as f:
            graph_relationships = json.load(f)
        
        for rel in graph_relationships:
            source_props = rel.get('source_properties') or {}
            target_props = rel.get('target_properties') or {}
            source_service_name = source_props.get('name', '')
            target_service_name = target_props.get('name', '')
            
            if source_service_name:
                self.service_to_graph[source_service_name] = {
                    'properties': source_props,
                    'namespace': source_props.get('namespace', ''),
                    'cluster': source_props.get('cluster', '')
                }
                self.service_graph.add_node(source_service_name, **source_props)
            
            if target_service_name:
                self.service_to_graph[target_service_name] = {
                    'properties': target_props,
                    'namespace': target_props.get('namespace', ''),
                    'cluster': target_props.get('cluster', '')
                }
                self.service_graph.add_node(target_service_name, **target_props)
            
            rel_type = rel.get('relationship_type', '')
            if source_service_name and target_service_name and rel_type:
                self.service_graph.add_edge(source_service_name, target_service_name, relationship_type=rel_type)
    
    def map_alerts_to_graph(self, alerts):
        for alert in alerts:
            service_name = alert.get('service_name', '')
            
            if service_name and service_name in self.service_to_graph:
                alert['graph_service'] = service_name
                alert['mapping_method'] = 'direct'
            else:
                alert_namespace = alert.get('namespace', '')
                alert_cluster = alert.get('cluster', '')
                matched_services = []
                
                for svc_name, svc_info in self.service_to_graph.items():
                    score = 0
                    if alert_namespace and svc_info.get('namespace') == alert_namespace:
                        score += 2
                    if alert_cluster and svc_info.get('cluster') == alert_cluster:
                        score += 2
                    if score >= 2:
                        matched_services.append((svc_name, score))
                
                if matched_services:
                    matched_services.sort(key=lambda x: x[1], reverse=True)
                    alert['graph_service'] = matched_services[0][0]
                    alert['mapping_method'] = 'fallback'
                else:
                    alert['graph_service'] = None
                    alert['mapping_method'] = 'unmapped'
    
    def group_by_service_relationships(self, alerts):
        service_groups = {}
        
        for alert in alerts:
            graph_service = alert.get('graph_service')
            if graph_service:
                if graph_service not in service_groups:
                    service_groups[graph_service] = []
                service_groups[graph_service].append(alert)
        
        self.consolidated_groups = []
        processed = set()
        
        for service_name, alerts in service_groups.items():
            if service_name in processed:
                continue
            
            related = set()
            if self.service_graph.has_node(service_name):
                for neighbor in self.service_graph.neighbors(service_name):
                    if neighbor in service_groups:
                        related.add(neighbor)
                for predecessor in self.service_graph.predecessors(service_name):
                    if predecessor in service_groups:
                        related.add(predecessor)
            
            group = {
                'primary_service': service_name,
                'related_services': list(related),
                'alerts': alerts.copy()
            }
            
            for related_svc in related:
                if related_svc in service_groups:
                    group['alerts'].extend(service_groups[related_svc])
                    processed.add(related_svc)
            
            self.consolidated_groups.append(group)
            processed.add(service_name)
        
        detailed_results = []
        for group_idx, group in enumerate(self.consolidated_groups):
            for alert in group['alerts']:
                detailed_results.append({
                    'group_id': group_idx,
                    'primary_service': group['primary_service'],
                    'related_services_count': len(group['related_services']),
                    'group_alert_count': len(group['alerts']),
                    'alert_name': alert.get('alert_name', ''),
                    'severity': alert.get('severity', ''),
                    'graph_service': alert.get('graph_service', ''),
                    'service_name': alert.get('service_name', ''),
                    'namespace': alert.get('namespace', ''),
                    'cluster': alert.get('cluster', ''),
                    'mapping_method': alert.get('mapping_method', ''),
                    'start_time': alert.get('startsAt', '')
                })
        
        self.alerts_df = pd.DataFrame(detailed_results)
    
    def engineer_relationship_features(self):
        features_list = []
        
        if not hasattr(self, '_pagerank_cache'):
            try:
                self._pagerank_cache = nx.pagerank(self.service_graph)
            except:
                self._pagerank_cache = {}
        
        if not hasattr(self, '_betweenness_cache'):
            try:
                self._betweenness_cache = nx.betweenness_centrality(self.service_graph)
            except:
                self._betweenness_cache = {}
        
        for _, alert in self.alerts_df.iterrows():
            feature_dict = {}
            graph_service = alert.get('graph_service', '')
            
            if pd.notna(graph_service) and graph_service and graph_service in self.service_graph:
                feature_dict['in_degree'] = self.service_graph.in_degree(graph_service)
                feature_dict['out_degree'] = self.service_graph.out_degree(graph_service)
                feature_dict['pagerank'] = self._pagerank_cache.get(graph_service, 0)
                feature_dict['betweenness'] = self._betweenness_cache.get(graph_service, 0)
                
                try:
                    feature_dict['clustering_coef'] = nx.clustering(self.service_graph.to_undirected(), graph_service)
                except:
                    feature_dict['clustering_coef'] = 0
                
                upstream_rels = []
                downstream_rels = []
                
                for predecessor in self.service_graph.predecessors(graph_service):
                    edge_data = self.service_graph.get_edge_data(predecessor, graph_service)
                    upstream_rels.append(edge_data.get('relationship_type', ''))
                
                for successor in self.service_graph.successors(graph_service):
                    edge_data = self.service_graph.get_edge_data(graph_service, successor)
                    downstream_rels.append(edge_data.get('relationship_type', ''))
                
                feature_dict['num_upstream'] = len(upstream_rels)
                feature_dict['num_downstream'] = len(downstream_rels)
                feature_dict['upstream_calls'] = upstream_rels.count('CALLS')
                feature_dict['upstream_belongs_to'] = upstream_rels.count('BELONGS_TO')
                feature_dict['downstream_calls'] = downstream_rels.count('CALLS')
                feature_dict['downstream_belongs_to'] = downstream_rels.count('BELONGS_TO')
                
                total_rels = len(upstream_rels) + len(downstream_rels)
                if total_rels > 0:
                    feature_dict['ratio_calls'] = (feature_dict['upstream_calls'] + feature_dict['downstream_calls']) / total_rels
                    feature_dict['ratio_belongs_to'] = (feature_dict['upstream_belongs_to'] + feature_dict['downstream_belongs_to']) / total_rels
                else:
                    feature_dict['ratio_calls'] = 0
                    feature_dict['ratio_belongs_to'] = 0
                
                feature_dict['dependency_direction'] = feature_dict['out_degree'] - feature_dict['in_degree']
            else:
                for key in ['in_degree', 'out_degree', 'pagerank', 'betweenness', 'clustering_coef',
                           'num_upstream', 'num_downstream', 'upstream_calls', 'upstream_belongs_to',
                           'downstream_calls', 'downstream_belongs_to', 'ratio_calls', 'ratio_belongs_to',
                           'dependency_direction']:
                    feature_dict[key] = 0
            
            alert_name = str(alert.get('alert_name', '')).lower()
            feature_dict['severity_encoded'] = {'critical': 4, 'high': 3, 'warning': 2, 'info': 1}.get(
                str(alert.get('severity', '')).lower().strip(), 0)
            feature_dict['is_error_alert'] = 1 if 'error' in alert_name else 0
            feature_dict['is_resource_alert'] = 1 if any(x in alert_name for x in ['cpu', 'memory', 'hpa']) else 0
            
            features_list.append(feature_dict)
        
        features_df = pd.DataFrame(features_list).fillna(0)
        self.feature_names = features_df.columns.tolist()
        self.feature_matrix = features_df.values
        
        scaler = StandardScaler()
        self.feature_matrix_scaled = scaler.fit_transform(self.feature_matrix)
    
    def apply_clustering(self):
        best_k = self._find_optimal_k(max_k=20)
        kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
        kmeans_labels = kmeans.fit_predict(self.feature_matrix_scaled)
        
        hierarchical = AgglomerativeClustering(n_clusters=min(15, max(2, len(self.alerts_df) // 10)), linkage='ward')
        hier_labels = hierarchical.fit_predict(self.feature_matrix_scaled)
        
        self.clustering_results = {
            'kmeans': {'labels': kmeans_labels, 'n_clusters': best_k},
            'hierarchical': {'labels': hier_labels, 'n_clusters': min(15, max(2, len(self.alerts_df) // 10))}
        }
        
        silhouette_kmeans = silhouette_score(self.feature_matrix_scaled, kmeans_labels)
        silhouette_hier = silhouette_score(self.feature_matrix_scaled, hier_labels)
        
        self.best_clustering = 'kmeans' if silhouette_kmeans > silhouette_hier else 'hierarchical'
    
    def _find_optimal_k(self, max_k=20):
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
        
        return K_range[np.argmax(silhouette_scores)] if silhouette_scores else 2
    
    def analyze_patterns(self):
        labels = self.clustering_results[self.best_clustering]['labels']
        self.alerts_df['cluster_id'] = labels
        
        features_df = pd.DataFrame(self.feature_matrix, columns=self.feature_names)
        self.cluster_patterns = {}
        
        for cluster_id in sorted(set(labels)):
            cluster_mask = labels == cluster_id
            cluster_alerts = self.alerts_df[cluster_mask]
            cluster_features = features_df[cluster_mask]
            
            if len(cluster_alerts) == 0:
                continue
            
            pattern = {
                'cluster_id': int(cluster_id),
                'size': len(cluster_alerts),
                'dominant_alert': cluster_alerts['alert_name'].mode()[0] if len(cluster_alerts) > 0 else 'unknown',
                'dominant_severity': cluster_alerts['severity'].mode()[0] if len(cluster_alerts) > 0 else 'unknown',
                'top_services': cluster_alerts['graph_service'].value_counts().head(3).to_dict(),
                'avg_in_degree': float(cluster_features['in_degree'].mean()),
                'avg_out_degree': float(cluster_features['out_degree'].mean()),
                'avg_calls_ratio': float(cluster_features['ratio_calls'].mean()),
                'avg_dependency_direction': float(cluster_features['dependency_direction'].mean()),
                'groups_spanned': int(cluster_alerts['group_id'].nunique())
            }
            
            if pattern['avg_dependency_direction'] > 5:
                pattern['type'] = 'Caller'
            elif pattern['avg_dependency_direction'] < -5:
                pattern['type'] = 'Callee'
            elif pattern['avg_in_degree'] < 2 and pattern['avg_out_degree'] < 2:
                pattern['type'] = 'Isolated'
            else:
                pattern['type'] = 'Mixed'
            
            self.cluster_patterns[cluster_id] = pattern
    
    def export_results(self, output_dir='.'):
        self.alerts_df.to_csv(f'{output_dir}/alerts_consolidated_clustered.csv', index=False)
        
        patterns_serializable = {str(k): v for k, v in self.cluster_patterns.items()}
        with open(f'{output_dir}/cluster_patterns.json', 'w') as f:
            json.dump(patterns_serializable, f, indent=2)
        
        summary_data = []
        for cluster_id, pattern in self.cluster_patterns.items():
            summary_data.append({
                'cluster_id': cluster_id,
                'type': pattern['type'],
                'size': pattern['size'],
                'alert': pattern['dominant_alert'],
                'severity': pattern['dominant_severity'],
                'in_degree': pattern['avg_in_degree'],
                'out_degree': pattern['avg_out_degree'],
                'groups_spanned': pattern['groups_spanned'],
                'top_services': ', '.join([f"{k}({v})" for k, v in list(pattern['top_services'].items())[:3]])
            })
        
        pd.DataFrame(summary_data).sort_values('size', ascending=False).to_csv(
            f'{output_dir}/cluster_summary.csv', index=False)
    
    def run_pipeline(self, output_dir='.'):
        print("Loading alerts...")
        alerts = self.load_and_process_alerts()
        print(f"  {len(alerts)} alerts loaded")
        
        print("Loading service graph...")
        self.load_service_graph()
        print(f"  {self.service_graph.number_of_nodes()} nodes, {self.service_graph.number_of_edges()} edges")
        
        print("Mapping alerts to services...")
        self.map_alerts_to_graph(alerts)
        
        print("Grouping by service relationships...")
        self.group_by_service_relationships(alerts)
        print(f"  {len(self.consolidated_groups)} groups created")
        
        print("Engineering relationship features...")
        self.engineer_relationship_features()
        print(f"  {len(self.feature_names)} features")
        
        print("Clustering...")
        self.apply_clustering()
        print(f"  {self.clustering_results[self.best_clustering]['n_clusters']} clusters ({self.best_clustering})")
        
        print("Analyzing patterns...")
        self.analyze_patterns()
        print(f"  {len(self.cluster_patterns)} patterns")
        
        print("Exporting results...")
        self.export_results(output_dir)
        print("Done.")
        
        return self.alerts_df, self.cluster_patterns


if __name__ == "__main__":
    consolidator = AlertConsolidationClustering(
        alerts_csv_path='C:/Users/jurat.shayidin/aiops/wip_2/alert_data.csv',
        graph_json_path='C:/Users/jurat.shayidin/aiops/wip_2/graph_data.json'
    )
    
    alerts_df, patterns = consolidator.run_pipeline(output_dir='.')
    
    print(f"\nGenerated files:")
    print("  - alerts_consolidated_clustered.csv")
    print("  - cluster_patterns.json")
    print("  - cluster_summary.csv")

