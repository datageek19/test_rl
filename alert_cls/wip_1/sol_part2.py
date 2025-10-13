"""
Relationship-Focused Alert Clustering
Groups alerts based on service graph relationship patterns (calls, owns, belongs_to)

This version focuses on SERVICE RELATIONSHIPS rather than alert metadata.
"""

import networkx as nx
import pandas as pd
import numpy as np
import json
from collections import Counter, defaultdict
from typing import Dict, List, Any, Tuple
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN, KMeans, AgglomerativeClustering, SpectralClustering
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
import warnings
warnings.filterwarnings('ignore')


class RelationshipFocusedClustering:
    """
    Cluster alerts based on service graph relationship patterns
    """
    
    def __init__(self, consolidated_results_path: str, graph_json_path: str):
        self.consolidated_results_path = consolidated_results_path
        self.graph_json_path = graph_json_path
        
        # Data structures
        self.alerts_df = None
        self.service_graph = nx.DiGraph()
        
        # Feature matrices
        self.feature_matrix = None
        self.feature_matrix_scaled = None
        self.feature_names = []
        self.scaler = StandardScaler()
        
        # Clustering results
        self.clustering_results = {}
        self.best_clustering = None
        self.cluster_patterns = {}
    
    def load_data(self):
        """Load consolidated alerts and service graph"""
        print("\n=== Loading Data ===")
        
        # Load consolidated alerts
        self.alerts_df = pd.read_csv(self.consolidated_results_path)
        print(f"✓ Loaded {len(self.alerts_df)} consolidated alerts")
        
        # Load service graph
        with open(self.graph_json_path, 'r') as f:
            graph_data = json.load(f)
        
        # Build graph with relationship types
        for rel in graph_data:
            source_props = rel.get('source_properties') or {}
            target_props = rel.get('target_properties') or {}
            
            source = source_props.get('name', '')
            target = target_props.get('name', '')
            rel_type = rel.get('relationship_type', '')
            
            if source and target:
                self.service_graph.add_edge(
                    source, target, 
                    relationship_type=rel_type,
                    source_props=source_props,
                    target_props=target_props
                )
        
        print(f"✓ Loaded service graph: {self.service_graph.number_of_nodes()} nodes, {self.service_graph.number_of_edges()} edges")
        
        # Analyze relationship types
        rel_types = [data.get('relationship_type', '') for _, _, data in self.service_graph.edges(data=True)]
        rel_type_counts = Counter(rel_types)
        print(f"\n  Relationship types in graph:")
        for rel_type, count in rel_type_counts.most_common():
            print(f"    {rel_type}: {count}")
    
    def engineer_relationship_features(self):
        """Extract features focused on service relationships"""
        print("\n=== Engineering Relationship-Focused Features ===")
        
        features_list = []
        
        for idx, alert in self.alerts_df.iterrows():
            feature_dict = {}
            
            graph_service = alert.get('graph_service', '')
            
            if pd.notna(graph_service) and graph_service and graph_service in self.service_graph:
                # === TOPOLOGY FEATURES ===
                feature_dict['degree_total'] = self.service_graph.degree(graph_service)
                feature_dict['in_degree'] = self.service_graph.in_degree(graph_service)
                feature_dict['out_degree'] = self.service_graph.out_degree(graph_service)
                
                # Centrality measures
                try:
                    if not hasattr(self, '_pagerank_cache'):
                        self._pagerank_cache = nx.pagerank(self.service_graph)
                    feature_dict['pagerank'] = self._pagerank_cache.get(graph_service, 0)
                except:
                    feature_dict['pagerank'] = 0
                
                try:
                    if not hasattr(self, '_betweenness_cache'):
                        self._betweenness_cache = nx.betweenness_centrality(self.service_graph)
                    feature_dict['betweenness'] = self._betweenness_cache.get(graph_service, 0)
                except:
                    feature_dict['betweenness'] = 0
                
                # Clustering coefficient (how connected are neighbors)
                try:
                    feature_dict['clustering_coef'] = nx.clustering(self.service_graph.to_undirected(), graph_service)
                except:
                    feature_dict['clustering_coef'] = 0
                
                # === RELATIONSHIP TYPE FEATURES ===
                # Analyze relationship types for this service
                upstream_rels = []  # Services this service depends on
                downstream_rels = []  # Services that depend on this
                
                # Upstream relationships (incoming edges)
                for predecessor in self.service_graph.predecessors(graph_service):
                    edge_data = self.service_graph.get_edge_data(predecessor, graph_service)
                    rel_type = edge_data.get('relationship_type', '')
                    upstream_rels.append(rel_type)
                
                # Downstream relationships (outgoing edges)
                for successor in self.service_graph.successors(graph_service):
                    edge_data = self.service_graph.get_edge_data(graph_service, successor)
                    rel_type = edge_data.get('relationship_type', '')
                    downstream_rels.append(rel_type)
                
                # Count relationship types
                feature_dict['num_upstream'] = len(upstream_rels)
                feature_dict['num_downstream'] = len(downstream_rels)
                feature_dict['upstream_calls'] = upstream_rels.count('calls')
                feature_dict['upstream_owns'] = upstream_rels.count('owns')
                feature_dict['upstream_belongs_to'] = upstream_rels.count('belongs_to')
                feature_dict['downstream_calls'] = downstream_rels.count('calls')
                feature_dict['downstream_owns'] = downstream_rels.count('owns')
                feature_dict['downstream_belongs_to'] = downstream_rels.count('belongs_to')
                
                # Relationship ratios
                total_rels = len(upstream_rels) + len(downstream_rels)
                if total_rels > 0:
                    feature_dict['ratio_calls'] = (feature_dict['upstream_calls'] + feature_dict['downstream_calls']) / total_rels
                    feature_dict['ratio_owns'] = (feature_dict['upstream_owns'] + feature_dict['downstream_owns']) / total_rels
                    feature_dict['ratio_belongs_to'] = (feature_dict['upstream_belongs_to'] + feature_dict['downstream_belongs_to']) / total_rels
                else:
                    feature_dict['ratio_calls'] = 0
                    feature_dict['ratio_owns'] = 0
                    feature_dict['ratio_belongs_to'] = 0
                
                # Dependency direction (is this service more of a caller or callee?)
                feature_dict['dependency_direction'] = feature_dict['out_degree'] - feature_dict['in_degree']
                
                # === NEIGHBORHOOD FEATURES ===
                # Analyze neighbors' connectivity
                neighbors = list(self.service_graph.predecessors(graph_service)) + list(self.service_graph.successors(graph_service))
                if neighbors:
                    neighbor_degrees = [self.service_graph.degree(n) for n in neighbors]
                    feature_dict['avg_neighbor_degree'] = np.mean(neighbor_degrees)
                    feature_dict['max_neighbor_degree'] = np.max(neighbor_degrees)
                else:
                    feature_dict['avg_neighbor_degree'] = 0
                    feature_dict['max_neighbor_degree'] = 0
                
            else:
                # Service not in graph - set all to 0
                for key in ['degree_total', 'in_degree', 'out_degree', 'pagerank', 'betweenness', 
                           'clustering_coef', 'num_upstream', 'num_downstream',
                           'upstream_calls', 'upstream_owns', 'upstream_belongs_to',
                           'downstream_calls', 'downstream_owns', 'downstream_belongs_to',
                           'ratio_calls', 'ratio_owns', 'ratio_belongs_to',
                           'dependency_direction', 'avg_neighbor_degree', 'max_neighbor_degree']:
                    feature_dict[key] = 0
            
            # === MINIMAL ALERT METADATA ===
            # Only include critical alert info, not behavioral features
            alert_name = str(alert.get('alert_name', '')).lower()
            feature_dict['severity_encoded'] = self._encode_severity(alert.get('severity', ''))
            feature_dict['is_error_alert'] = 1 if 'error' in alert_name else 0
            feature_dict['is_resource_alert'] = 1 if any(x in alert_name for x in ['cpu', 'memory', 'hpa']) else 0
            
            # Keep group_id as context but don't use it as a feature (it's what we're trying to improve!)
            # feature_dict['group_id_original'] = alert.get('group_id', -1)
            
            features_list.append(feature_dict)
        
        # Convert to DataFrame
        features_df = pd.DataFrame(features_list)
        self.feature_names = features_df.columns.tolist()
        
        # Handle any NaN values
        features_df = features_df.fillna(0)
        
        # Store and scale features
        self.feature_matrix = features_df.values
        self.feature_matrix_scaled = self.scaler.fit_transform(self.feature_matrix)
        
        print(f"✓ Created {self.feature_matrix.shape[1]} relationship-focused features for {self.feature_matrix.shape[0]} alerts")
        print(f"\n  Relationship features:")
        for fname in self.feature_names:
            if any(x in fname for x in ['upstream', 'downstream', 'ratio', 'dependency']):
                print(f"    - {fname}")
        
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
        """Apply clustering algorithms"""
        print("\n=== Applying Clustering Algorithms ===")
        
        # 1. K-Means with optimal k
        print("\n[1/3] K-Means Clustering (auto-selecting k)...")
        best_k = self._find_optimal_k(max_k=20)
        kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
        kmeans_labels = kmeans.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['kmeans'] = {
            'labels': kmeans_labels,
            'n_clusters': best_k,
            'centroids': kmeans.cluster_centers_
        }
        print(f"  ✓ Optimal k={best_k}, created {best_k} clusters")
        
        # 2. Hierarchical Clustering
        print("\n[2/3] Hierarchical Clustering...")
        n_clusters_hier = min(15, max(2, len(self.alerts_df) // 10))
        hierarchical = AgglomerativeClustering(n_clusters=n_clusters_hier, linkage='ward')
        hier_labels = hierarchical.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['hierarchical'] = {
            'labels': hier_labels,
            'n_clusters': n_clusters_hier
        }
        print(f"  ✓ Created {n_clusters_hier} hierarchical clusters")
        
        # 3. Spectral Clustering (uses graph structure via affinity matrix)
        print("\n[3/3] Spectral Clustering...")
        n_clusters_spec = min(10, max(2, len(self.alerts_df) // 15))
        spectral = SpectralClustering(n_clusters=n_clusters_spec, random_state=42, affinity='rbf')
        spec_labels = spectral.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['spectral'] = {
            'labels': spec_labels,
            'n_clusters': n_clusters_spec
        }
        print(f"  ✓ Created {n_clusters_spec} spectral clusters")
    
    def _find_optimal_k(self, max_k=20):
        """Find optimal number of clusters using silhouette score"""
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
        
        if silhouette_scores:
            best_k = K_range[np.argmax(silhouette_scores)]
            return best_k
        return 2
    
    def evaluate_clustering_quality(self):
        """Evaluate clustering quality"""
        print("\n=== Clustering Quality Evaluation ===")
        
        evaluation_results = {}
        
        for method, result in self.clustering_results.items():
            labels = result['labels']
            
            if len(set(labels)) < 2:
                print(f"\n{method.upper()}: Insufficient clusters for evaluation")
                continue
            
            try:
                silhouette = silhouette_score(self.feature_matrix_scaled, labels)
                davies_bouldin = davies_bouldin_score(self.feature_matrix_scaled, labels)
                calinski_harabasz = calinski_harabasz_score(self.feature_matrix_scaled, labels)
                
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
        
        if evaluation_results:
            best_method = max(evaluation_results.items(), key=lambda x: x[1]['silhouette_score'])[0]
            self.best_clustering = best_method
            print(f"\n✓ Best clustering method: {best_method.upper()}")
        
        return evaluation_results
    
    def analyze_relationship_patterns(self, clustering_method=None):
        """Analyze relationship patterns in each cluster"""
        if clustering_method is None:
            clustering_method = self.best_clustering or 'kmeans'
        
        print(f"\n=== Analyzing Relationship Patterns ({clustering_method}) ===")
        
        labels = self.clustering_results[clustering_method]['labels']
        self.alerts_df['cluster_relationship'] = labels
        
        # Get original features for analysis
        features_df = pd.DataFrame(self.feature_matrix, columns=self.feature_names)
        
        cluster_patterns = {}
        
        for cluster_id in sorted(set(labels)):
            cluster_mask = labels == cluster_id
            cluster_alerts = self.alerts_df[cluster_mask]
            cluster_features = features_df[cluster_mask]
            
            if len(cluster_alerts) == 0:
                continue
            
            # Analyze relationship characteristics
            pattern = {
                'cluster_id': int(cluster_id),
                'size': len(cluster_alerts),
                
                # Alert info
                'dominant_alert_type': cluster_alerts['alert_name'].mode()[0] if len(cluster_alerts) > 0 else 'unknown',
                'dominant_severity': cluster_alerts['severity'].mode()[0] if len(cluster_alerts) > 0 else 'unknown',
                'top_services': cluster_alerts['graph_service'].value_counts().head(5).to_dict(),
                
                # Relationship characteristics
                'avg_degree': float(cluster_features['degree_total'].mean()),
                'avg_in_degree': float(cluster_features['in_degree'].mean()),
                'avg_out_degree': float(cluster_features['out_degree'].mean()),
                'avg_upstream_deps': float(cluster_features['num_upstream'].mean()),
                'avg_downstream_deps': float(cluster_features['num_downstream'].mean()),
                
                # Relationship type distribution
                'avg_calls_ratio': float(cluster_features['ratio_calls'].mean()),
                'avg_owns_ratio': float(cluster_features['ratio_owns'].mean()),
                'avg_belongs_to_ratio': float(cluster_features['ratio_belongs_to'].mean()),
                
                # Topology
                'avg_pagerank': float(cluster_features['pagerank'].mean()),
                'avg_betweenness': float(cluster_features['betweenness'].mean()),
                'avg_clustering_coef': float(cluster_features['clustering_coef'].mean()),
                
                # Dependency pattern
                'avg_dependency_direction': float(cluster_features['dependency_direction'].mean()),
                
                # Compare to original grouping
                'original_groups_spanned': cluster_alerts['group_id'].nunique() if 'group_id' in cluster_alerts.columns else 0,
            }
            
            # Classify pattern type
            if pattern['avg_dependency_direction'] > 5:
                pattern['pattern_type'] = 'Caller/Client Services'
            elif pattern['avg_dependency_direction'] < -5:
                pattern['pattern_type'] = 'Callee/Server Services'
            elif pattern['avg_betweenness'] > 0.01:
                pattern['pattern_type'] = 'Hub/Bottleneck Services'
            elif pattern['avg_clustering_coef'] > 0.3:
                pattern['pattern_type'] = 'Tightly Coupled Services'
            elif pattern['avg_degree'] < 2:
                pattern['pattern_type'] = 'Isolated/Leaf Services'
            else:
                pattern['pattern_type'] = 'Mixed Pattern'
            
            cluster_patterns[cluster_id] = pattern
            
            print(f"\nCluster {cluster_id} - {pattern['pattern_type']} ({len(cluster_alerts)} alerts):")
            print(f"  Alert Type: {pattern['dominant_alert_type']}")
            print(f"  Avg Degree: in={pattern['avg_in_degree']:.1f}, out={pattern['avg_out_degree']:.1f}")
            print(f"  Relationships: calls={pattern['avg_calls_ratio']:.2f}, owns={pattern['avg_owns_ratio']:.2f}, belongs_to={pattern['avg_belongs_to_ratio']:.2f}")
            print(f"  Top Services: {list(pattern['top_services'].keys())[:3]}")
            print(f"  Spans {pattern['original_groups_spanned']} original service groups")
        
        self.cluster_patterns = cluster_patterns
        return cluster_patterns
    
    def export_results(self, output_dir='.'):
        """Export clustering results"""
        print("\n=== Exporting Results ===")
        
        # Export with all clustering labels
        output_df = self.alerts_df.copy()
        for method, result in self.clustering_results.items():
            output_df[f'cluster_{method}'] = result['labels']
        
        output_path = f'{output_dir}/alert_cluster_relationship_focused.csv'
        output_df.to_csv(output_path, index=False)
        print(f"✓ Alert assignments: {output_path}")
        
        # Export patterns
        if self.cluster_patterns:
            patterns_path = f'{output_dir}/relationship_patterns.json'
            with open(patterns_path, 'w') as f:
                # Convert numpy int32 keys to strings for JSON serialization
                patterns_serializable = {str(k): v for k, v in self.cluster_patterns.items()}
                json.dump(patterns_serializable, f, indent=2)
            print(f"✓ Relationship patterns: {patterns_path}")
            
            # Export summary
            summary_data = []
            for cluster_id, pattern in self.cluster_patterns.items():
                summary_data.append({
                    'cluster_id': cluster_id,
                    'pattern_type': pattern['pattern_type'],
                    'size': pattern['size'],
                    'alert_type': pattern['dominant_alert_type'],
                    'avg_degree': pattern['avg_degree'],
                    'avg_upstream': pattern['avg_upstream_deps'],
                    'avg_downstream': pattern['avg_downstream_deps'],
                    'calls_ratio': pattern['avg_calls_ratio'],
                    'original_groups_spanned': pattern['original_groups_spanned'],
                    'top_services': ', '.join([f"{k}({v})" for k, v in list(pattern['top_services'].items())[:3]])
                })
            
            summary_df = pd.DataFrame(summary_data).sort_values('size', ascending=False)
            summary_path = f'{output_dir}/relationship_patterns_summary.csv'
            summary_df.to_csv(summary_path, index=False)
            print(f"✓ Pattern summary: {summary_path}")
    
    def run_pipeline(self, output_dir='.'):
        """Run complete relationship-focused clustering pipeline"""
        print("\n" + "="*70)
        print("RELATIONSHIP-FOCUSED CLUSTERING PIPELINE")
        print("="*70)
        
        self.load_data()
        self.engineer_relationship_features()
        self.apply_clustering_algorithms()
        evaluation = self.evaluate_clustering_quality()
        self.analyze_relationship_patterns()
        self.export_results(output_dir)
        
        print("\n" + "="*70)
        print("PIPELINE COMPLETE")
        print("="*70)
        
        return evaluation, self.cluster_patterns


if __name__ == "__main__":
    clustering = RelationshipFocusedClustering(
        consolidated_results_path='C:/Users/jurat.shayidin/aiops/wip_2/alert_consolidation_results.csv',
        graph_json_path='C:/Users/jurat.shayidin/aiops/wip_2/graph_data.json'
    )
    
    results = clustering.run_pipeline(output_dir='.')
    
    print("\n" + "="*70)
    print("GENERATED FILES:")
    print("="*70)
    print("✓ alert_cluster_relationship_focused.csv - Alerts with relationship-based clusters")
    print("✓ relationship_patterns.json - Detailed relationship patterns")
    print("✓ relationship_patterns_summary.csv - Pattern summary")
    print("="*70)

