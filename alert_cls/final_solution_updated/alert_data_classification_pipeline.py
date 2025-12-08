import networkx as nx
import pandas as pd
import json
import ast
import os
import re
from collections import Counter
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# Machine Learning imports
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering, SpectralClustering
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from scipy.spatial.distance import cosine

'''
mockup example:
    service-frontend → service-api → service-db (parent sink)
    service-backend → service-api → service-db (parent sink)
'''

class ComprehensiveAlertConsolidator:
    """
    Comprehensive Alert Consolidation Pipeline:
    1. Load and preprocess alerts + service graph
    2. Map alerts to graph services (direct + fallback)
    3. Enrich alerts with graph topology features
    4. Initial grouping by service relationships
    5. Feature engineering (graph + alert metadata)
    6. Apply clustering algorithms for refined grouping
    7. Deduplicate alerts within groups
    8. Export consolidated results
    """
    
    # Configuration constants
    TIME_WINDOW_MINUTES = 5  # Time window for duplicate detection
    MIN_MATCH_SCORE = 2  # Minimum score for fallback mapping
    DUPLICATE_THRESHOLD = 5  # Minimum score to consider duplicates
    DESCRIPTION_SIMILARITY_THRESHOLD = 0.7  # Jaccard similarity threshold
    MIN_CLUSTERING_SAMPLES = 10  # Minimum alerts needed for clustering
    PCA_VARIANCE_THRESHOLD = 0.95  # Retain 95% variance
    OUTLIER_CONTAMINATION = 0.05  # Expect 5% outliers
    
    def __init__(self, alerts_csv_path, graph_json_path, output_dir='.'):
        self.alerts_csv_path = alerts_csv_path
        self.graph_json_path = graph_json_path
        self.output_dir = output_dir
        
        # Validate inputs
        if not os.path.exists(alerts_csv_path):
            raise FileNotFoundError(f"Alerts CSV not found: {alerts_csv_path}")
        if not os.path.exists(graph_json_path):
            raise FileNotFoundError(f"Graph JSON not found: {graph_json_path}")
        
        # Core data structures
        self.firing_alerts = []
        self.graph_relationships = []
        self.service_graph = nx.DiGraph()
        self.service_to_graph = {}
        
        # Consolidation results
        self.enriched_alerts = []
        self.consolidated_groups = []
        self.service_hierarchy = {}
        
        # Clustering
        self.alerts_df = None
        self.feature_matrix = None
        self.feature_matrix_scaled = None
        self.feature_names = []
        self.scaler = StandardScaler()
        self.clustering_results = {}
        
        # PCA and outlier detection
        self.pca = None
        self.feature_matrix_pca = None
        self.outlier_indices = set()
        self.outlier_removal_enabled = True
        self._idx_mapping = None  # Mapping from alerts_df index to enriched_alerts index
        
        # Deduplication
        self.deduplicated_alerts = []
        self.duplicate_groups = []
        
        # Caches for expensive computations
        self._pagerank_cache = None
        self._betweenness_cache = None
        self._closeness_cache = None
        self._undirected_graph = None  # Cache undirected graph conversion
        self._clustering_coef_cache = {}  # Cache clustering coefficients per service
        self._service_features_cache = {}  # Pre-computed graph features per service
        
    # ========================================================================
    # PHASE 1: DATA LOADING AND PREPROCESSING
    # ========================================================================
    
    def _load_firing_alerts(self):
        """Load and parse firing alerts with enhanced metadata extraction"""
        print("\n[1/8] Loading firing alerts...")
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
            
        print(f" Loaded {len(self.firing_alerts)} firing alerts")
    
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
                alert['alert_subcategory'] = labels.get('alert_subcategory', '')  # CRITICAL FIX: Add subcategory parsing
                alert['platform'] = labels.get('platform', '')
            except (ValueError, SyntaxError, TypeError):
                # Failed to parse labels, set defaults
                alert['service_name'] = alert.get('service_name', '')
                alert['namespace'] = alert.get('namespace', '')
                alert['pod'] = alert.get('pod', '')
                alert['node'] = alert.get('node', '')
                alert['cluster'] = alert.get('cluster', '')
                alert['workload_type'] = alert.get('workload_type', '')
                alert['anomaly_resource_type'] = alert.get('anomaly_resource_type', '')
                alert['alert_category'] = alert.get('alert_category', '')
                alert['alert_subcategory'] = alert.get('alert_subcategory', '')
                alert['platform'] = alert.get('platform', '')
        
        annotations_str = alert.get('annotations', '')
        if annotations_str:
            try:
                annotations = ast.literal_eval(annotations_str)
                alert['description'] = annotations.get('description', '')
            except (ValueError, SyntaxError, TypeError):
                # Failed to parse annotations, set default
                alert['description'] = alert.get('description', '')
    
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
        print("\n[2/8] Loading service graph...")
        
        with open(self.graph_json_path, 'r') as f:
            self.graph_relationships = json.load(f)
        
        print(f"    Loaded {len(self.graph_relationships)} relationships")
        print("    Building service graph...")
        
        # Build service graph with progress indicators
        total_rels = len(self.graph_relationships)
        for i, rel in enumerate(self.graph_relationships):
            
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
        
        print(f" Built graph: {len(self.service_to_graph)} services, {self.service_graph.number_of_edges()} edges")
        
        # Pre-compute centrality metrics
        print("    Computing graph metrics...")
        try:
            self._pagerank_cache = nx.pagerank(self.service_graph)
            self._betweenness_cache = nx.betweenness_centrality(self.service_graph, k=min(100, len(self.service_graph)))
            
            # Cache undirected graph for clustering coefficient calculations
            self._undirected_graph = self.service_graph.to_undirected()
            
            # Pre-compute clustering coefficients for all services
            print("    Pre-computing graph features per service...")
            clustering_dict = nx.clustering(self._undirected_graph)
            self._clustering_coef_cache = clustering_dict
            
            # Pre-compute all graph features per service
            self._precompute_service_features()
        except Exception as e:
            print(f"    Warning: Could not compute some graph metrics: {e}")
    
    def _precompute_service_features(self):
        """Pre-compute all graph features per service"""
        print("    Pre-computing service graph features...")
        
        for service_name in self.service_graph.nodes():
            if service_name not in self._service_features_cache:
                features = {}
                
                # Basic degree metrics
                features['degree_total'] = self.service_graph.degree(service_name)
                features['in_degree'] = self.service_graph.in_degree(service_name)
                features['out_degree'] = self.service_graph.out_degree(service_name)
                
                # Centrality features (cached)
                features['pagerank'] = self._pagerank_cache.get(service_name, 0) if self._pagerank_cache else 0
                features['betweenness'] = self._betweenness_cache.get(service_name, 0) if self._betweenness_cache else 0
                features['clustering_coef'] = self._clustering_coef_cache.get(service_name, 0)
                
                # Relationship type counts
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
                
                features['num_upstream'] = len(upstream_rels)
                features['num_downstream'] = len(downstream_rels)
                features['upstream_calls'] = upstream_rels.count('CALLS')
                features['upstream_owns'] = upstream_rels.count('OWNS')
                features['upstream_belongs_to'] = upstream_rels.count('BELONGS_TO')
                features['downstream_calls'] = downstream_rels.count('CALLS')
                features['downstream_owns'] = downstream_rels.count('OWNS')
                features['downstream_belongs_to'] = downstream_rels.count('BELONGS_TO')
                
                # Relationship ratios
                total_rels = len(upstream_rels) + len(downstream_rels)
                if total_rels > 0:
                    features['ratio_calls'] = (features['upstream_calls'] + features['downstream_calls']) / total_rels
                    features['ratio_owns'] = (features['upstream_owns'] + features['downstream_owns']) / total_rels
                    features['ratio_belongs_to'] = (features['upstream_belongs_to'] + features['downstream_belongs_to']) / total_rels
                else:
                    features['ratio_calls'] = 0
                    features['ratio_owns'] = 0
                    features['ratio_belongs_to'] = 0
                
                # Dependency direction
                features['dependency_direction'] = features['out_degree'] - features['in_degree']
                
                # Neighborhood features
                neighbors = list(self.service_graph.predecessors(service_name)) + list(self.service_graph.successors(service_name))
                if neighbors:
                    neighbor_degrees = [self.service_graph.degree(n) for n in neighbors]
                    features['avg_neighbor_degree'] = np.mean(neighbor_degrees)
                    features['max_neighbor_degree'] = np.max(neighbor_degrees)
                else:
                    features['avg_neighbor_degree'] = 0
                    features['max_neighbor_degree'] = 0
                
                self._service_features_cache[service_name] = features
        
        print(f" Pre-computed features for {len(self._service_features_cache)} services")
    
    # ========================================================================
    # PHASE 2: ALERT-TO-GRAPH MAPPING
    # ========================================================================
    
    def _enrich_alert_with_graph_info(self, alert):
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
        
        # find matching services by namespace and cluster
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
            
            if match_score >= self.MIN_MATCH_SCORE:  # Require at least namespace or cluster match
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
            alert['mapping_confidence'] = best_match[2] / 5.0  # Max score is 5
            return True
        
        # No mapping found
        alert['graph_service'] = None
        alert['graph_info'] = None
        alert['mapping_method'] = 'unmapped'
        alert['mapping_confidence'] = 0.0
        return False
    
    def _get_service_dependencies(self, service_name):
        """Get upstream and downstream dependencies for a service"""
        dependencies = {
            'upstream': [],
            'downstream': [],
            'peers': []       # if any Services in same namespace/cluster
        }
        
        if not self.service_graph.has_node(service_name):
            return dependencies
        
        # Get upstream dependencies
        for predecessor in self.service_graph.predecessors(service_name):
            edge_data = self.service_graph.get_edge_data(predecessor, service_name)
            dependencies['upstream'].append({
                'service': predecessor,
                'relationship': edge_data.get('relationship_type', '') if edge_data else ''
            })
        
        # Get downstream dependencies
        for successor in self.service_graph.successors(service_name):
            edge_data = self.service_graph.get_edge_data(service_name, successor)
            dependencies['downstream'].append({
                'service': successor,
                'relationship': edge_data.get('relationship_type', '') if edge_data else ''
            })
        
        return dependencies
    
    # ========================================================================
    # PHASE 3: INITIAL CONSOLIDATION BY RELATIONSHIPS
    # ========================================================================
    
    def _group_alerts_by_relationships(self):
        """Group alerts based on service relationships and dependencies"""
        print("\n[4/8] Grouping alerts by service relationships...")
        
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
        
        # Analyze service relationships
        print("    Analyzing service correlations (parent sinks and traversal paths)...")
        
        # relationships between alert groups
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
                'grouping_method': 'graph_relationships_correlated'  # Updated label
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
            
            # Add correlation metadata
            group['correlation_type'] = 'immediate_neighbors' if len(related_services) <= 2 else 'transitive_sinks'
            
            self.consolidated_groups.append(group)
            processed_services.add(service_name)
        
        # Handle unmapped alerts - group by namespace/cluster/node
        if unmapped_alerts:
            unmapped_groups = self._group_unmapped_alerts(unmapped_alerts)
            self.consolidated_groups.extend(unmapped_groups)
        
        print(f" Created {len(self.consolidated_groups)} initial consolidated groups")
    
    def _find_related_alert_services(self, service_name, service_groups, max_depth=1):
        """Find services with alerts that are related to given service """
        related = set()
        
        if not self.service_graph.has_node(service_name):
            return related
        
        # Get immediate neighbors
        for neighbor in self.service_graph.neighbors(service_name):
            if neighbor in service_groups and neighbor != service_name:
                related.add(neighbor)
        
        # Get reverse neighbors
        for predecessor in self.service_graph.predecessors(service_name):
            if predecessor in service_groups and predecessor != service_name:
                related.add(predecessor)
        
        # Find services that share transitive parent sinks
        parent_sinks1 = self._get_transitive_parent_services(service_name, max_depth=3)
        
        for other_service in service_groups.keys():
            if other_service == service_name or other_service in related:
                continue
            
            # Check if they share common parent sinks
            parent_sinks2 = self._get_transitive_parent_services(other_service, max_depth=3)
            
            if parent_sinks1 and parent_sinks2 and (parent_sinks1 & parent_sinks2):
                related.add(other_service)
            
            # Check path similarity (threshold > 0.3)
            similarity = self._compute_path_similarity(service_name, other_service)
            if similarity > 0.3:  # 30% shared parent sinks or similar paths
                related.add(other_service)
        
        return related

    def _get_transitive_parent_services(self, service_name, max_depth=3):
        """
        Get transitive upstream services (all parent services up to max_depth).
        This finds services that this service eventually depends on.
        """
        parent_set = set()
        
        if not self.service_graph.has_node(service_name):
            return parent_set
        
        # BFS to collect upstream services
        visited = set()
        queue = [(service_name, 0)]  # (service, depth)
        
        while queue:
            current, depth = queue.pop(0)
            visited.add(current)
            
            if depth >= max_depth:
                continue
            
            # Get immediate predecessors (parents)
            for predecessor in self.service_graph.predecessors(current):
                if predecessor not in visited and predecessor != service_name:
                    parent_set.add(predecessor)
                    queue.append((predecessor, depth + 1))
        
        return parent_set

    def _compute_path_similarity(self, service1, service2):
        """
        Compute similarity between two services based on their graph paths.
        Returns a score from 0 to 1 indicating how similar their traversal paths are.
        """
        if service1 == service2:
            return 1.0
        
        # Get transitive upstream sinks for both services
        parents1 = self._get_transitive_parent_services(service1)
        parents2 = self._get_transitive_parent_services(service2)
        
        if not parents1 or not parents2:
            return 0.0
        
        # Jaccard similarity of parent sets
        intersection = len(parents1 & parents2)
        union = len(parents1 | parents2)
        
        if union == 0:
            return 0.0
        
        return intersection / union
    
    def _group_unmapped_alerts(self, unmapped_alerts):
        """Group unmapped alerts by cluster, namespace, node, or combination"""
        groups_dict = {}
        
        for alert in unmapped_alerts:
            namespace = alert.get('namespace', 'unknown')
            cluster = alert.get('cluster', 'unknown')
            node = alert.get('node', 'unknown')
            
            # create meaningful groups
            # Priority: cluster+namespace > namespace+node > cluster > namespace > node
            
            if cluster != 'unknown' and namespace != 'unknown':
                # Best case: cluster + namespace
                key = f"cluster_ns:{cluster}:{namespace}"
            elif namespace != 'unknown' and node != 'unknown':
                # Namespace + node
                key = f"ns_node:{namespace}:{node}"
            elif cluster != 'unknown':
                # Cluster only
                key = f"cluster:{cluster}"
            elif namespace != 'unknown':
                # Namespace only
                key = f"namespace:{namespace}"
            elif node != 'unknown':
                # Node only
                key = f"node:{node}"
            else:
                # totally unknown
                key = "unknown:unknown"
            
            if key not in groups_dict:
                groups_dict[key] = []
            groups_dict[key].append(alert)
        
        unmapped_groups = []
        for key, alerts in groups_dict.items():
            # Parse key to extract grouping info
            parts = key.split(':', 1)
            group_type = parts[0] if len(parts) > 0 else 'unknown'
            group_value = parts[1] if len(parts) > 1 else 'unknown'
            
            group = {
                'group_id': -1,  # Will be assigned later
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
    
    def _create_consolidated_output(self):
        """Create summary statistics for consolidated groups"""
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
            if alert_types:
                group['most_common_alert'] = Counter(alert_types).most_common(1)[0][0]
            else:
                group['most_common_alert'] = ''
            
            if alert_categories:
                group['most_common_category'] = Counter(alert_categories).most_common(1)[0][0]
            else:
                group['most_common_category'] = ''
            
            if alert_subcategories:
                group['most_common_subcategory'] = Counter(alert_subcategories).most_common(1)[0][0]
            else:
                group['most_common_subcategory'] = ''
    
    # ========================================================================
    # PHASE 4: FEATURE ENGINEERING FOR CLUSTERING
    # ========================================================================
    
    def _engineer_features(self):
        """
        Extract comprehensive features for clustering 
        
        Performance optimizations:
        1. Pre-computed graph features per service (done once in _load_graph_data)
        2. Cached undirected graph conversion and clustering coefficients
        """
        print("\n[5/8] Engineering features for clustering...")
        
        # assign initial group IDs to all alerts
        for alert in self.enriched_alerts:
            alert['initial_group_id'] = -1
        
        for group in self.consolidated_groups:
            for alert in group['alerts']:
                alert['initial_group_id'] = group['group_id']
        
        self.alerts_df = pd.DataFrame(self.enriched_alerts)
     
        # Pre-define all graph feature keys
        graph_feature_keys = ['degree_total', 'in_degree', 'out_degree', 'pagerank', 'betweenness', 
                             'clustering_coef', 'num_upstream', 'num_downstream',
                             'upstream_calls', 'upstream_owns', 'upstream_belongs_to',
                             'downstream_calls', 'downstream_owns', 'downstream_belongs_to',
                             'ratio_calls', 'ratio_owns', 'ratio_belongs_to',
                             'dependency_direction', 'avg_neighbor_degree', 'max_neighbor_degree']
        
        features_list = []
        
        # cached features
        for row in self.alerts_df.itertuples():
            feature_dict = {}
            
            # Get graph service from row
            graph_service = getattr(row, 'graph_service', '')
            
            # Use pre-computed graph features if available
            if pd.notna(graph_service) and graph_service and graph_service in self._service_features_cache:
                # Copy all pre-computed graph features
                cached_features = self._service_features_cache[graph_service]
                for key in graph_feature_keys:
                    feature_dict[key] = cached_features.get(key, 0)
            else:
                # Service not in graph - set all to 0
                for key in graph_feature_keys:
                    feature_dict[key] = 0
            
            # === ALERT METADATA FEATURES 
            alert_name = str(getattr(row, 'alert_name', '')).lower()
            feature_dict['severity_encoded'] = self._encode_severity(getattr(row, 'severity', ''))
            feature_dict['is_error_alert'] = 1 if 'error' in alert_name else 0
            feature_dict['is_resource_alert'] = 1 if any(x in alert_name for x in ['cpu', 'memory', 'hpa', 'resource']) else 0
            feature_dict['is_network_alert'] = 1 if any(x in alert_name for x in ['network', 'rx_bytes', 'tx_bytes']) else 0
            feature_dict['is_anomaly_alert'] = 1 if 'anomaly' in str(getattr(row, 'alert_category', '')).lower() else 0

            # Encode alert category and subcategory
            feature_dict['alert_category_encoded'] = self._encode_alert_category(getattr(row, 'alert_category', ''))
            feature_dict['alert_subcategory_encoded'] = self._encode_alert_subcategory(getattr(row, 'alert_subcategory', ''))

            # Encode workload type
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

            # Mapping confidence
            feature_dict['mapping_confidence'] = getattr(row, 'mapping_confidence', 0)
            
            features_list.append(feature_dict)

        features_df = pd.DataFrame(features_list)
        self.feature_names = features_df.columns.tolist()

        features_df = features_df.fillna(0)
        
        # Store and scale features
        self.feature_matrix = features_df.values
        self.feature_matrix_scaled = self.scaler.fit_transform(self.feature_matrix)
        
        print(f" Created {self.feature_matrix.shape[1]} features for {self.feature_matrix.shape[0]} alerts")
        print(f"      Graph topology: 20 features")
        print(f"      Alert metadata: 19 features (severity, category, subcategory, workload, temporal, combinations)")
        
        # Step 2: Remove outliers if enabled
        if self.outlier_removal_enabled and len(self.feature_matrix_scaled) > 20:
            print("    Detecting and removing outliers...")
            self._remove_outliers()
        else:
            print("    Skipping outlier removal (disabled or insufficient data)")
            # Create identity mapping when no outliers removed
            self._idx_mapping = {i: i for i in range(len(self.alerts_df))}
        
        # Step 3: Apply PCA for dimensionality reduction
        print("    Applying PCA for feature selection...")
        self._apply_pca()
    
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
    
    def _encode_workload_type(self, workload_type):
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

    def _encode_alert_category(self, category):
        """Encode alert category to numeric value - UPDATED to match actual data"""
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

    def _encode_alert_subcategory(self, subcategory):
        """Encode alert subcategory to numeric value - UPDATED to match actual data"""
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
            # Fit Isolation Forest
            isolation_forest = IsolationForest(
                contamination=self.OUTLIER_CONTAMINATION,
                random_state=42,
                n_estimators=100
            )
            
            # Detect outliers
            outlier_labels = isolation_forest.fit_predict(self.feature_matrix_scaled)
            
            # Extract outlier indices (labels == -1 are outliers)
            self.outlier_indices = set(np.where(outlier_labels == -1)[0])
            
            n_outliers = len(self.outlier_indices)
            n_total = len(self.feature_matrix_scaled)
            outlier_pct = (n_outliers / n_total) * 100
            
            print(f"   Detected {n_outliers} outliers ({outlier_pct:.1f}% of data)")
            
            if n_outliers > 0:
                # Remove outliers from feature matrix
                self.feature_matrix_scaled = np.delete(self.feature_matrix_scaled, 
                                                       list(self.outlier_indices), axis=0)
                
                # Update alerts_df to remove outlier rows
                valid_indices = [i for i in range(len(self.alerts_df)) if i not in self.outlier_indices]
                self.alerts_df = self.alerts_df.iloc[valid_indices].reset_index(drop=True)
                
                print(f"   Removed {n_outliers} outliers from feature matrix")
                print(f"   Remaining alerts: {len(self.feature_matrix_scaled)}")
            else:
                # No outliers removed, create identity mapping
                valid_indices = list(range(len(self.alerts_df)))
            
            # Create mapping: alerts_df index -> original enriched_alerts index
            self._idx_mapping = {new_idx: orig_idx 
                                 for new_idx, orig_idx in enumerate(valid_indices)}
            
        except Exception as e:
            print(f"       Warning: Outlier removal failed: {e}")
            print("      Continuing without outlier removal...")
            self.outlier_indices = set()
            # Create identity mapping when no outliers removed
            self._idx_mapping = {i: i for i in range(len(self.alerts_df))}
    
    def _apply_pca(self):
        """Apply PCA for dimensionality reduction"""
        try:
            # Determine number of components to retain 95% variance
            pca = PCA(n_components=self.PCA_VARIANCE_THRESHOLD, random_state=42)
            self.feature_matrix_pca = pca.fit_transform(self.feature_matrix_scaled)
            
            n_components = pca.n_components_
            variance_explained = pca.explained_variance_ratio_.sum()
            
            print(f"   PCA: Reduced {self.feature_matrix_scaled.shape[1]} features to {n_components} components")
            print(f"   Explained variance: {variance_explained:.1%}")
            
            # Store PCA object for later use
            self.pca = pca
            
            # Update feature matrix to use PCA-reduced features for clustering
            self.feature_matrix_scaled = self.feature_matrix_pca
            
        except Exception as e:
            print(f"       Warning: PCA failed: {e}")
            print("      Continuing with original features...")
            self.pca = None
            self.feature_matrix_pca = None
    
    # ========================================================================
    # PHASE 5: CLUSTERING FOR REFINED GROUPING
    # ========================================================================
    
    def _apply_clustering(self):
        """Apply clustering algorithms for refined grouping"""
        print("\n[6/8] Applying clustering algorithms...")
        
        if len(self.feature_matrix_scaled) < self.MIN_CLUSTERING_SAMPLES:
            print(f"     Not enough alerts for clustering (need >= {self.MIN_CLUSTERING_SAMPLES}, have {len(self.feature_matrix_scaled)})")
            print("     Skipping clustering - using initial groups only")
            # Assign cluster_id from initial_group_id
            self.alerts_df['cluster_id'] = self.alerts_df['initial_group_id']
            self.alerts_df['clustering_method'] = 'none_initial_groups_only'
            
            # Set silhouette score to None (clustering not performed)
            self.final_silhouette_score = None
            self.final_clustering_method = 'none_initial_groups_only'
            
            # Update enriched alerts - handle outlier removal
            for i, alert in enumerate(self.enriched_alerts):
                if i in self.outlier_indices:
                    # This alert was removed as outlier
                    alert['cluster_id'] = -1
                    alert['clustering_method'] = 'outlier_removed'
                else:
                    # Get initial group id from the alert directly
                    alert['cluster_id'] = alert.get('initial_group_id', -1)
                    alert['clustering_method'] = 'none_initial_groups_only'
            return
        
        # 1. K-Means with optimal k
        print("    Running K-Means clustering...")
        best_k = self._find_optimal_k(max_k=min(20, len(self.feature_matrix_scaled) // 2))
        kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
        kmeans_labels = kmeans.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['kmeans'] = {
            'labels': kmeans_labels,
            'n_clusters': best_k,
            'centroids': kmeans.cluster_centers_,
            'algorithm': 'kmeans'
        }
        print(f"   K-Means: k={best_k} clusters")
        
        # 2. DBSCAN (density-based)
        print("    Running DBSCAN clustering...")
        eps = self._estimate_dbscan_eps()
        dbscan = DBSCAN(eps=eps, min_samples=max(2, len(self.feature_matrix_scaled) // 100))
        dbscan_labels = dbscan.fit_predict(self.feature_matrix_scaled)
        n_clusters_dbscan = len(set(dbscan_labels)) - (1 if -1 in dbscan_labels else 0)
        self.clustering_results['dbscan'] = {
            'labels': dbscan_labels,
            'n_clusters': n_clusters_dbscan,
            'algorithm': 'dbscan'
        }
        print(f"   DBSCAN: {n_clusters_dbscan} clusters, {list(dbscan_labels).count(-1)} noise points")
        
        # 3. Hierarchical Clustering
        print("    Running Hierarchical clustering...")
        n_clusters_hier = min(15, max(2, len(self.alerts_df) // 20))
        hierarchical = AgglomerativeClustering(n_clusters=n_clusters_hier, linkage='ward')
        hier_labels = hierarchical.fit_predict(self.feature_matrix_scaled)
        self.clustering_results['hierarchical'] = {
            'labels': hier_labels,
            'n_clusters': n_clusters_hier,
            'algorithm': 'hierarchical'
        }
        print(f"   Hierarchical: {n_clusters_hier} clusters")
        
        # Select best clustering result
        self._select_best_clustering()
    
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
    
    def _estimate_dbscan_eps(self):
        """Estimate DBSCAN eps parameter"""
        from sklearn.neighbors import NearestNeighbors
        
        n_samples = len(self.feature_matrix_scaled)
        k = min(5, max(2, n_samples // 10))
        
        nbrs = NearestNeighbors(n_neighbors=k).fit(self.feature_matrix_scaled)
        distances, _ = nbrs.kneighbors(self.feature_matrix_scaled)
        
        # Use 75th percentile of k-nearest neighbor distances
        eps = np.percentile(distances[:, -1], 75)
        return max(0.3, min(eps, 2.0))
    
    def _select_best_clustering(self):
        """Select best clustering result based on silhouette score"""
        print("    Selecting best clustering...")
        
        best_score = -1
        best_method = 'kmeans'
        
        for method, result in self.clustering_results.items():
            labels = result['labels']
            
            # Skip if all same cluster
            unique_labels = set(labels)
            if len(unique_labels) <= 1:
                continue
            
            if -1 in unique_labels and list(labels).count(-1) > len(labels) * 0.3:
                continue  # Too much noise
            
            try:
                score = silhouette_score(self.feature_matrix_scaled, labels)
                if score > best_score:
                    best_score = score
                    best_method = method
            except:
                continue
        
        print(f"   Selected {best_method} (silhouette score: {best_score:.3f})")
        
        # Store silhouette score for reporting
        self.final_silhouette_score = best_score
        self.final_clustering_method = best_method
        
        # Assign final cluster labels
        final_labels = self.clustering_results[best_method]['labels']
        self.alerts_df['cluster_id'] = final_labels
        self.alerts_df['clustering_method'] = best_method
        
        # Update enriched alerts - map back using index mapping
        # Create reverse mapping: orig_idx -> new_idx for lookup
        reverse_mapping = {}
        if self._idx_mapping:
            for new_idx, orig_idx in self._idx_mapping.items():
                reverse_mapping[orig_idx] = new_idx
        
        for i, alert in enumerate(self.enriched_alerts):
            # Check if this alert was kept after outlier removal
            if i in self.outlier_indices:
                # This alert was removed as outlier, mark as -1
                alert['cluster_id'] = -1
                alert['clustering_method'] = 'outlier_removed'
            else:
                # Find the index in alerts_df that corresponds to this enriched_alert index
                if self._idx_mapping and i in reverse_mapping:
                    # Map to new index
                    new_idx = reverse_mapping[i]
                    alert['cluster_id'] = int(final_labels[new_idx])
                    alert['clustering_method'] = best_method
                elif not self._idx_mapping:
                    # No outlier removal, direct mapping
                    alert['cluster_id'] = int(final_labels[i])
                    alert['clustering_method'] = best_method
                else:
                    # Not found in mapping (shouldn't happen)
                    alert['cluster_id'] = -1
                    alert['clustering_method'] = 'unmapped'
    
    # ========================================================================
    # PHASE 6: DEDUPLICATION
    # ========================================================================
    
    def _deduplicate_alerts(self):
        """Deduplicate alerts within clusters based on similarity"""
        print("\n[7/8] Deduplicating alerts...")
        
        self.deduplicated_alerts = []
        self.duplicate_groups = []
        
        # Group by cluster
        clusters = self.alerts_df.groupby('cluster_id')
        
        total_duplicates = 0
        
        for cluster_id, cluster_df in clusters:
            if cluster_id == -1:  # Skip noise points
                # Add all noise points without deduplication
                for idx in cluster_df.index:
                    # Map to original enriched_alerts index
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
                
                # Map to original enriched_alerts index
                orig_idx = self._idx_mapping[idx] if self._idx_mapping else idx
                alert = self.enriched_alerts[orig_idx]
                alert['is_duplicate'] = False
                alert['duplicate_of'] = None
                
                # Find similar alerts
                duplicates = []
                
                for other_idx in cluster_df.index:
                    if other_idx <= idx or other_idx in processed_indices:
                        continue
                    
                    # Map to original enriched_alerts index
                    orig_other_idx = self._idx_mapping[other_idx] if self._idx_mapping else other_idx
                    other_alert = self.enriched_alerts[orig_other_idx]
                    
                    if self._are_duplicates(alert, other_alert):
                        duplicates.append(orig_other_idx)
                        processed_indices.add(other_idx)
                        
                        # Mark as duplicate
                        other_alert['is_duplicate'] = True
                        other_alert['duplicate_of'] = orig_idx
                        total_duplicates += 1
                
                # Add representative alert
                self.deduplicated_alerts.append(alert)
                
                # Store duplicate group if any
                if duplicates:
                    self.duplicate_groups.append({
                        'representative_idx': orig_idx,
                        'duplicate_indices': duplicates,
                        'count': len(duplicates) + 1
                    })
                
                processed_indices.add(idx)
        
        print(f" Found {total_duplicates} duplicates")
        print(f" {len(self.deduplicated_alerts)} unique alerts remain")
    
    def _are_duplicates(self, alert1, alert2, time_window_minutes=None):
        """
        Check if two alerts are duplicates based on:
        1. Same service
        2. Sharing common parent (upstream) dependencies
        3. Sharing common child (downstream) dependencies
        4. Eventually sink to same services (transitive dependencies
        5. Within time window
        """
        if time_window_minutes is None:
            time_window_minutes = self.TIME_WINDOW_MINUTES
        
        # Must be close in time
        time1 = alert1.get('start_timestamp', 0)
        time2 = alert2.get('start_timestamp', 0)
        
        if abs(time1 - time2) > time_window_minutes * 60:
            return False
        
        service1 = alert1.get('graph_service')
        service2 = alert2.get('graph_service')
        
        # If either alert is unmapped, fall back to basic matching
        if not service1 or not service2:
            return self._are_duplicates_basic(alert1, alert2)
        
        # Both mapped to graph - use dependency-based duplication
        
        # 1. Same service → duplicates
        if service1 == service2:
            return True
        
        # 2. Share common upstream dependencies (same parent services)
        deps1 = self._get_service_dependencies(service1)
        deps2 = self._get_service_dependencies(service2)
        
        # Extract service names from dependency lists
        upstream1 = {d['service'] for d in deps1['upstream']}
        upstream2 = {d['service'] for d in deps2['upstream']}
        downstream1 = {d['service'] for d in deps1['downstream']}
        downstream2 = {d['service'] for d in deps2['downstream']}
        
        # Share common parents
        if upstream1 and upstream2 and upstream1 & upstream2:
            return True
        
        # Share common children
        if downstream1 and downstream2 and downstream1 & downstream2:
            return True
        
        # 3. Eventually sink to same services (transitive downstream)
        # Check if services eventually reach same downstream services
        transitive_down1 = self._get_transitive_downstream(service1)
        transitive_down2 = self._get_transitive_downstream(service2)
        
        if transitive_down1 and transitive_down2 and transitive_down1 & transitive_down2:
            return True
        
        return False
    
    def _are_duplicates_basic(self, alert1, alert2):
        """Basic duplicate detection for unmapped alerts"""
        # Same alert type + same pod
        if (alert1.get('alert_name') == alert2.get('alert_name') and
            alert1.get('pod') == alert2.get('pod') and
            alert1.get('pod')):
            return True
        
        # Same service name + same pod
        if (alert1.get('service_name') == alert2.get('service_name') and
            alert1.get('pod') == alert2.get('pod') and
            alert1.get('service_name')):
            return True
        
        return False
    
    def _get_transitive_downstream(self, service_name, max_depth=2):
        """Get transitive downstream services (services that service eventually calls/connects to)"""
        downstream_set = set()
        
        if not self.service_graph.has_node(service_name):
            return downstream_set
        
        # BFS to collect downstream services
        visited = set()
        queue = [(service_name, 0)]  # (service, depth)
        
        while queue:
            current, depth = queue.pop(0)
            visited.add(current)
            
            if depth >= max_depth:
                continue
            
            # Get immediate downstream
            for successor in self.service_graph.successors(current):
                if successor not in visited:
                    downstream_set.add(successor)
                    queue.append((successor, depth + 1))
        
        return downstream_set
    
    # ========================================================================
    # CLUSTER NAMING AND RANKING
    # ========================================================================
    
    def _generate_cluster_name(self, cluster_alerts):
        """Generate a distinctive name for the cluster based on its characteristics"""
        if not cluster_alerts:
            return "Empty_Cluster"
        
        # Get most common patterns
        alert_types = [a.get('alert_name', '') for a in cluster_alerts]
        services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
        categories = [a.get('alert_category', '') for a in cluster_alerts if a.get('alert_category')]
        severities = [a.get('severity', '').lower() for a in cluster_alerts]
        
    
        if alert_types:
            most_common_alert = Counter(alert_types).most_common(1)[0][0]
            # Extract keywords from alert name (remove special chars)
            alert_keywords = re.sub(r'[^a-zA-Z0-9\s]', '', most_common_alert).split()[:3]
            alert_prefix = '_'.join(alert_keywords[:2]).lower()
        else:
            alert_prefix = "unknown_alert"
        
        if services:
            most_common_service = Counter(services).most_common(1)[0][0]
            # Extract service name (last part after dots or slashes)
            service_parts = most_common_service.replace(':', '_').split('_')
            service_key = service_parts[-1][:15].lower()
        else:
            service_key = "unmapped"
        
        # based on category dominance
        if categories:
            most_common_cat = Counter(categories).most_common(1)[0][0]
            cat_key = most_common_cat.lower()[:12]
        else:
            cat_key = "general"
        
        # based on severity pattern
        severity_counts = Counter(severities)
        if 'critical' in severity_counts and severity_counts['critical'] > len(cluster_alerts) * 0.5:
            severity_key = "critical"
        elif 'high' in severity_counts and severity_counts['high'] > len(cluster_alerts) * 0.5:
            severity_key = "high"
        else:
            severity_key = "mixed"
        
        # Build cluster name: alert_type_service_category_severity
        cluster_name = f"{alert_prefix}_{service_key}_{cat_key}_{severity_key}"
        
        # Clean up name
        cluster_name = re.sub(r'_+', '_', cluster_name)  # Remove duplicate underscores
        cluster_name = cluster_name.strip('_')
        
        return cluster_name
    
    def _calculate_cluster_score(self, cluster_alerts, cluster_id):
        """Calculate ranking score for a cluster (higher = more important/repetitive)"""
        score = 0
        
        if not cluster_alerts:
            return 0
        
        # based on Alert frequency (repetition score)
        alert_types = [a.get('alert_name', '') for a in cluster_alerts]
        if alert_types:
            unique_types = len(set(alert_types))
            total_alerts = len(alert_types)
            # Lower unique/total ratio = more repetitive
            repetition_score = (1 - unique_types / total_alerts) * 100
            score += repetition_score * 0.4  # 40% weight
        
        # based on Severity impact
        severities = [a.get('severity', '').lower() for a in cluster_alerts]
        severity_weights = {'critical': 5, 'high': 4, 'warning': 2, 'info': 1}
        avg_severity = sum(severity_weights.get(s, 0) for s in severities) / len(severities)
        score += avg_severity * 5  # 25% weight (5 * avg * 5)
        
        # based on Cluster size (more alerts = more important)
        size_score = min(len(cluster_alerts) / 20, 1.0) * 100  # Cap at 100 for clusters with 20+ alerts
        score += size_score * 0.2  # 20% weight
        
        # based on Service importance (graph centrality)
        services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
        if services and self._pagerank_cache:
            # Get average pagerank of services in cluster
            pageranks = [self._pagerank_cache.get(s, 0) for s in set(services)]
            if pageranks:
                avg_pagerank = np.mean(pageranks) * 1000  # Scale up
                score += min(avg_pagerank, 100) * 0.15  # 15% weight
        
        # based on Time concentration (alert storm indicator)
        timestamps = [a.get('start_timestamp', 0) for a in cluster_alerts if a.get('start_timestamp')]
        if timestamps and len(timestamps) > 1:
            time_span = max(timestamps) - min(timestamps)
            # Shorter time span = more concentrated = higher score
            if time_span > 0:
                concentration_score = 100 / (1 + time_span / 3600)  # Hours to score
                score += concentration_score * 0.1  # 10% weight
        
        return score
    
    def _rank_and_name_clusters(self):
        """Rank clusters and assign distinctive names"""
        print("\n[7.5/8] Naming and ranking clusters...")
        
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
                continue  # Skip noise/outliers
            
            # Generate distinctive name
            cluster_name = self._generate_cluster_name(cluster_alerts)
            
            # Calculate ranking score
            score = self._calculate_cluster_score(cluster_alerts, cluster_id)
            
            # Additional metadata
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
        
        # Sort by ranking score (highest first)
        cluster_metadata.sort(key=lambda x: x['ranking_score'], reverse=True)
        
        # Assign ranks
        for rank, metadata in enumerate(cluster_metadata, start=1):
            metadata['rank'] = rank
            
            # Add rank and name to alerts in this cluster
            cluster_id = metadata['cluster_id']
            for alert in cluster_dict[cluster_id]:
                alert['cluster_rank'] = rank
                alert['cluster_name'] = metadata['cluster_name']
                alert['cluster_score'] = metadata['ranking_score']
        
        # Export ranked clusters
        self._export_ranked_clusters(cluster_metadata)
        
        print(f" Ranked {len(cluster_metadata)} clusters")
        print(f" Top 5 clusters by score:")
        for i, meta in enumerate(cluster_metadata[:5], 1):
            print(f"      {i}. {meta['cluster_name']} (Score: {meta['ranking_score']:.1f}, {meta['alert_count']} alerts)")
    
    def _export_ranked_clusters(self, cluster_metadata):
        """Export ranked cluster summary"""
        df_ranked = pd.DataFrame(cluster_metadata)
        ranked_path = f'{self.output_dir}/ranked_clusters.csv'
        df_ranked.to_csv(ranked_path, index=False)
        print(f" Ranked clusters: {ranked_path}")
    
    # ========================================================================
    # PHASE 7: EXPORT RESULTS
    # ========================================================================
    
    def _export_results(self):
        """Export consolidated and clustered results"""
        print("\n[8/8] Exporting results...")
        
        # Prepare final output
        output_data = []
        
        for i, alert in enumerate(self.enriched_alerts):
            output_data.append({
                'alert_id': i,
                'final_group_id': alert.get('cluster_id', -1),
                'initial_group_id': alert.get('initial_group_id', -1),
                'clustering_method': alert.get('clustering_method', ''),
                'is_duplicate': alert.get('is_duplicate', False),
                'duplicate_of': alert.get('duplicate_of', ''),
                
                # NEW: Cluster ranking and naming
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
                'anomaly_resource_type': alert.get('anomaly_resource_type', ''),
                'description': alert.get('description', ''),
            })
        
        df_output = pd.DataFrame(output_data)
        
        # Export main results
        main_output_path = f'{self.output_dir}/alert_consolidation_final.csv'
        df_output.to_csv(main_output_path, index=False)
        print(f" Main results: {main_output_path}")
        
        # Export group summary
        self._export_group_summary()
        
        # Export deduplicated alerts
        self._export_deduplicated()
        
        # Export mapping details
        self._export_mapping_details()
        
        # Export cluster statistics
        self._export_cluster_stats()
        
        # After _export_cluster_stats() call, add:
        self._export_cluster_detail_view()

        print("\n" + "=" * 70)
        print("CONSOLIDATION COMPLETE!")
        print("=" * 70)
        print(f"Total alerts processed: {len(self.enriched_alerts)}")
        print(f"Unique alerts (after dedup): {len(self.deduplicated_alerts)}")
        print(f"Final groups/clusters: {len(set(df_output['final_group_id']))}")
        
        # Report clustering quality
        if hasattr(self, 'final_silhouette_score') and self.final_silhouette_score is not None:
            print(f"\nClustering Quality Metrics:")
            print(f"  Method: {self.final_clustering_method}")
            print(f"  Silhouette Score: {self.final_silhouette_score:.3f}")
            if self.final_silhouette_score > 0.5:
                print("  Excellent clustering quality")
            elif self.final_silhouette_score > 0.3:
                print("  Good clustering quality")
            elif self.final_silhouette_score > 0.1:
                print("   Moderate clustering quality")
            else:
                print("   Low clustering quality - consider feature refinement")
        elif hasattr(self, 'final_clustering_method') and self.final_clustering_method == 'none_initial_groups_only':
            print(f"\nClustering Quality Metrics:")
            print(f"  Method: {self.final_clustering_method}")
            print(f"   Clustering skipped - insufficient samples")
        
        print(f"\nOutput directory: {self.output_dir}")
    
    def _export_group_summary(self):
        """Export summary of each cluster/group"""
        cluster_summary = []
        
        clusters = self.alerts_df.groupby('cluster_id')
        
        for cluster_id, cluster_df in clusters:
            # Get alerts in this cluster
            cluster_alerts = [self.enriched_alerts[idx] for idx in cluster_df.index]
            
            # Compute statistics
            alert_names = [a.get('alert_name', '') for a in cluster_alerts]
            services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
            namespaces = [a.get('namespace', '') for a in cluster_alerts if a.get('namespace')]
            severities = [a.get('severity', '') for a in cluster_alerts]
            
            # Get category and subcategory distributions
            categories = [a.get('alert_category', '') for a in cluster_alerts if a.get('alert_category')]
            subcategories = [a.get('alert_subcategory', '') for a in cluster_alerts if a.get('alert_subcategory')]
            
            cluster_summary.append({
                'cluster_id': cluster_id,
                'alert_count': len(cluster_alerts),
                'unique_alert_types': len(set(alert_names)),
                'most_common_alert': Counter(alert_names).most_common(1)[0][0] if alert_names else '',
                'unique_services': len(set(services)),
                'primary_service': Counter(services).most_common(1)[0][0] if services else '',
                'namespaces': ','.join(sorted(set(namespaces))[:5]),
                'severity_distribution': str(dict(Counter(severities))),
                'category_distribution': str(dict(Counter(categories))) if categories else '',
                'subcategory_distribution': str(dict(Counter(subcategories))) if subcategories else '',
                'most_common_category': Counter(categories).most_common(1)[0][0] if categories else '',
                'most_common_subcategory': Counter(subcategories).most_common(1)[0][0] if subcategories else '',
                'clustering_method': cluster_df['clustering_method'].iloc[0] if len(cluster_df) > 0 else ''
            })
        
        df_summary = pd.DataFrame(cluster_summary)
        df_summary = df_summary.sort_values('alert_count', ascending=False)
        
        summary_path = f'{self.output_dir}/cluster_summary.csv'
        df_summary.to_csv(summary_path, index=False)
        print(f" Group summary: {summary_path}")
    
    def _export_deduplicated(self):
        """Export deduplicated alerts and duplicate info"""
        dedup_data = []
        
        for alert in self.deduplicated_alerts:
            dedup_data.append({
                'alert_name': alert.get('alert_name', ''),
                'service_name': alert.get('service_name', ''),
                'graph_service': alert.get('graph_service', ''),
                'cluster_id': alert.get('cluster_id', -1),
                'namespace': alert.get('namespace', ''),
                'cluster': alert.get('cluster', ''),
                'severity': alert.get('severity', ''),
                'alert_category': alert.get('alert_category', ''),
                'alert_subcategory': alert.get('alert_subcategory', ''),
                'starts_at': alert.get('startsAt', ''),
            })
        
        df_dedup = pd.DataFrame(dedup_data)
        dedup_path = f'{self.output_dir}/deduplicated_alerts.csv'
        df_dedup.to_csv(dedup_path, index=False)
        print(f" Deduplicated alerts: {dedup_path}")
    
    def _export_mapping_details(self):
        """Export mapping statistics"""
        mapping_stats = {
            'direct': 0,
            'fallback': 0,
            'unmapped': 0
        }
        
        for alert in self.enriched_alerts:
            method = alert.get('mapping_method', 'unmapped')
            if method == 'service_name':
                mapping_stats['direct'] += 1
            elif method == 'namespace_cluster_fallback':
                mapping_stats['fallback'] += 1
            else:
                mapping_stats['unmapped'] += 1
        
        mapping_df = pd.DataFrame([mapping_stats])
        mapping_path = f'{self.output_dir}/mapping_statistics.csv'
        mapping_df.to_csv(mapping_path, index=False)
        print(f" Mapping stats: {mapping_path}")
    
    def _export_cluster_stats(self):
        """Export detailed clustering statistics including silhouette scores"""
        stats = []
        
        for method, result in self.clustering_results.items():
            labels = result.get('labels', [])
            # Calculate silhouette score for this method
            silhouette = None
            if len(labels) > 1 and len(set(labels)) > 1:
                try:
                    # Skip noise points (-1) for silhouette calculation if present
                    if -1 in labels:
                        valid_mask = np.array(labels) != -1
                        if np.sum(valid_mask) > 1 and len(set(labels[valid_mask])) > 1:
                            silhouette = silhouette_score(
                                self.feature_matrix_scaled[valid_mask], 
                                np.array(labels)[valid_mask]
                            )
                    else:
                        silhouette = silhouette_score(self.feature_matrix_scaled, labels)
                except:
                    silhouette = None
            
            stats.append({
                'method': method,
                'n_clusters': result.get('n_clusters', 0),
                'algorithm': result.get('algorithm', method),
                'silhouette_score': silhouette if silhouette is not None else 'N/A'
            })
        
        df_stats = pd.DataFrame(stats)
        stats_path = f'{self.output_dir}/clustering_statistics.csv'
        df_stats.to_csv(stats_path, index=False)
        print(f" Clustering stats: {stats_path}")
    
    def _export_cluster_detail_view(self):
        """Export detailed cluster-based view with all alert information"""
        print("    Generating detailed cluster view...")
        
        # Load ranked clusters to get metadata
        ranked_clusters_df = pd.read_csv(f'{self.output_dir}/ranked_clusters.csv') if os.path.exists(f'{self.output_dir}/ranked_clusters.csv') else None
        ranked_dict = {}
        if ranked_clusters_df is not None:
            for _, row in ranked_clusters_df.iterrows():
                ranked_dict[row['cluster_id']] = {
                    'cluster_name': row.get('cluster_name', ''),
                    'ranking_score': row.get('ranking_score', 0),
                    'rank': row.get('rank', -1),
                    'alert_count': row.get('alert_count', 0)
                }
        
        # Group alerts by final_group_id
        cluster_details = []
        
        for alert in self.enriched_alerts:
            cluster_id = alert.get('cluster_id', -1)
            
            # Skip outliers
            if cluster_id == -1:
                continue
            
            # Get cluster metadata
            cluster_meta = ranked_dict.get(cluster_id, {})
            
            # Get service graph information
            graph_service = alert.get('graph_service', '')
            graph_info = alert.get('graph_info', {})
            dependencies = alert.get('dependencies', {})
            
            # Prepare detailed alert record
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
                'graph_service': graph_service,
                'graph_service_type': graph_info.get('type', '') if graph_info else '',
                'graph_environment': graph_info.get('environment', '') if graph_info else '',
                'graph_namespace': graph_info.get('namespace', '') if graph_info else '',
                'graph_cluster': graph_info.get('cluster', '') if graph_info else '',
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
                'platform': alert.get('platform', ''),
                'is_duplicate': alert.get('is_duplicate', False),
                
                # Original payload (if available)
                'labels': alert.get('labels', ''),
                'annotations': alert.get('annotations', ''),
            }
            
            cluster_details.append(alert_detail)
        
        # Create DataFrame
        df_details = pd.DataFrame(cluster_details)
        
        # Sort by cluster_rank, then by cluster_id, then by start_timestamp
        df_details = df_details.sort_values(['cluster_rank', 'cluster_id', 'start_timestamp'], 
                                             ascending=[True, True, True])
        
        # Export
        details_path = f'{self.output_dir}/alerts_by_cluster_detailed.csv'
        df_details.to_csv(details_path, index=False)
        print(f" Detailed cluster view: {details_path}")
        
        return df_details
    
    # ========================================================================
    # MAIN CONSOLIDATION PIPELINE
    # ========================================================================
    
    def run_consolidation(self):
        """Execute the complete consolidation pipeline"""
        print("\n" + "=" * 70)
        print("COMPREHENSIVE ALERT CONSOLIDATION PIPELINE")
        print("=" * 70)
        
        # Phase 1: Load data
        self._load_firing_alerts()
        self._load_graph_data()
        
        # Phase 2: Enrich alerts with graph info
        print("\n[3/8] Enriching alerts with graph relationships...")
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
                graph_service = alert.get('graph_service')
                if graph_service:
                    alert['dependencies'] = self._get_service_dependencies(graph_service)
            else:
                unmapped_count += 1
        
        print(f"    Direct mapping: {mapped_count}")
        print(f"    Fallback mapping: {fallback_count}")
        print(f"    Unmapped: {unmapped_count}")
        
        self.enriched_alerts = self.firing_alerts
        self._group_alerts_by_relationships()
        self._create_consolidated_output()
        self._engineer_features()
        self._apply_clustering()
        self._deduplicate_alerts()
        self._rank_and_name_clusters()
        self._export_results()
        
        return self.consolidated_groups


if __name__ == "__main__":
    # Initialize consolidator
    consolidator = ComprehensiveAlertConsolidator(
        alerts_csv_path='alert_data.csv',  
        graph_json_path='graph_data.json',
        output_dir='.'
    )

    # Run complete pipeline
    consolidated_groups = consolidator.run_consolidation()
    
    # Generate visualizations
    print("\n" + "=" * 70)
    print("GENERATING VISUALIZATIONS")
    print("=" * 70)
    try:
        import viz_utils
        viz_utils.load_and_visualize_clusters()
        viz_utils.generate_cluster_summary_table()
    except Exception as e:
        print(f" Visualization failed: {e}")
        print("   Run 'python viz_utils.py' manually to generate visualizations")

