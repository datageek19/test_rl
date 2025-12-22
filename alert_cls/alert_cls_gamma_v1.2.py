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
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from scipy.spatial.distance import cosine

'''
quick example:
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
    
    def __init__(self, alerts_csv_path, graph_json_path, output_dir='temp'):
        self.alerts_csv_path = alerts_csv_path
        self.graph_json_path = graph_json_path
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
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
        self._undirected_graph = None       # Cache undirected graph conversion
        self._clustering_coef_cache = {}    # Cache clustering coefficients per service
        self._service_features_cache = {}   # Pre-computed graph features per service
        
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
                # Failed to parse labels - skip this alert
                pass
        
        annotations_str = alert.get('annotations', '')
        if annotations_str:
            try:
                annotations = ast.literal_eval(annotations_str)
                alert['description'] = annotations.get('description', '')
            except (ValueError, SyntaxError, TypeError):
                # Failed to parse annotations - skip
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
        print("\n[2/8] Loading service graph...")
        
        with open(self.graph_json_path, 'r') as f:
            graph_data = json.load(f)
        
        # Handle different JSON structures
        if isinstance(graph_data, dict) and 'data' in graph_data:
            # New format: {success: ..., data: {nodes: [...], edges: [...]}}
            nodes = graph_data['data'].get('nodes', [])
            edges = graph_data['data'].get('edges', [])
            
            # Build node lookup table: node_id -> node_data
            node_lookup = {}
            for node in nodes:
                node_id = node.get('id')
                if node_id:
                    node_lookup[node_id] = node
            
            # Convert new format to old format for compatibility
            self.graph_relationships = []
            for edge in edges:
                source_id = edge.get('source')
                target_id = edge.get('target')
                
                source_node = node_lookup.get(source_id, {})
                target_node = node_lookup.get(target_id, {})
                
                source_props = source_node.get('properties', {})
                target_props = target_node.get('properties', {})
                
                # Convert to old format
                rel = {
                    'source_name': source_props.get('name', ''),
                    'target_name': target_props.get('name', ''),
                    'source_label': source_node.get('label', ''),
                    'target_label': target_node.get('label', ''),
                    'relationship_type': edge.get('label', ''),
                    'source_properties': source_props,
                    'target_properties': target_props
                }
                self.graph_relationships.append(rel)
                
        elif isinstance(graph_data, list):
            # Old format: direct list of relationships
            self.graph_relationships = graph_data
        else:
            raise ValueError(f"Unexpected graph data format: {type(graph_data)}")
        
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
        """Map alert to graph service using service_name, with fallback to namespace + cluster"""
        service_name = alert.get('service_name', '')
        
        # Primary mapping: direct service_name match
        if service_name and service_name in self.service_to_graph:
            alert['graph_service'] = service_name
            alert['graph_info'] = self.service_to_graph[service_name]
            alert['mapping_method'] = 'service_name'
            alert['mapping_confidence'] = 1.0
            return True
        
        # Fallback mapping: need namespace + cluster combination
        alert_namespace = alert.get('namespace', '')
        alert_cluster = alert.get('cluster', '')
        alert_node = alert.get('node', '')
        
        # sanity check: In Kubernetes, namespace + cluster should uniquely identify a service
        
        matched_services = []
        for svc_name, svc_info in self.service_to_graph.items():
            match_score = 0
            has_namespace_match = False
            has_cluster_match = False
            
            # Match namespace (needed for fallback)
            if alert_namespace and svc_info.get('namespace') == alert_namespace:
                match_score += 3  # Higher weight - namespace is important
                has_namespace_match = True
            
            # Match cluster (needed for fallback)
            if alert_cluster and svc_info.get('cluster') == alert_cluster:
                match_score += 3  # Higher weight - cluster is important
                has_cluster_match = True
            
            # Match node
            if alert_node and svc_info.get('properties', {}).get('node') == alert_node:
                match_score += 1 
            
            # Require both namespace AND cluster for fallback mapping
            if has_namespace_match and has_cluster_match:
                matched_services.append((svc_name, svc_info, match_score))
        
        # Use best match 
        if matched_services:
            matched_services.sort(key=lambda x: x[2], reverse=True)
            best_match = matched_services[0]
            top_score = best_match[2]
            
            # Check for ambiguous matches (multiple services with same top score)
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
            'downstream': []
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
        """Group alerts based on service relationships and impact propagation"""
        print("\n[4/8] Grouping alerts by service relationships and impact...")
        
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
        
        print(f"    Found {len(service_groups)} service groups with {len(unmapped_alerts)} unmapped alerts")
        
        # Analyze service relationships and impact
        print("    Analyzing service relationships and impact propagation...")
        
        self.consolidated_groups = []
        processed_services = set()
        
        group_counter = 0
        for service_name, alerts in service_groups.items():
            if service_name in processed_services:
                continue
            
            # Find impacted services (downstream) and correlated services
            impacted_services = self._find_impacted_services(service_name, max_depth=2)
            correlated_services = self._find_correlated_alert_services(
                service_name, service_groups, impacted_services
            )
            
            # Create consolidated group
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
            
            # Add alerts from correlated services (services with alerts that are related)
            for correlated_svc in correlated_services:
                if correlated_svc in service_groups:
                    correlated_alert_count = len(service_groups[correlated_svc])
                    group['alerts'].extend(service_groups[correlated_svc])
                    group['alert_count'] += correlated_alert_count
                    processed_services.add(correlated_svc)
            
            # Add group metadata
            group['alert_types'] = list(set(a.get('alert_name', '') for a in group['alerts']))
            group['namespaces'] = list(set(a.get('namespace', '') for a in group['alerts'] if a.get('namespace')))
            group['clusters'] = list(set(a.get('cluster', '') for a in group['alerts'] if a.get('cluster')))
            
            self.consolidated_groups.append(group)
            processed_services.add(service_name)
            group_counter += 1
        
        # Handle unmapped alerts - group by namespace/cluster/node
        if unmapped_alerts:
            print(f"\n    Processing {len(unmapped_alerts)} unmapped alerts...")
            unmapped_groups = self._group_unmapped_alerts(unmapped_alerts)
            self.consolidated_groups.extend(unmapped_groups)
            print(f"    Created {len(unmapped_groups)} unmapped groups")
        
        print(f"\n Created {len(self.consolidated_groups)} consolidated groups")
        
        # Apply deduplication immediately after grouping (before feature engineering)
        print("\n[4.5/8] Deduplicating alerts within graph-based groups...")
        self._deduplicate_within_groups()
    
    def _find_impacted_services(self, service_name, max_depth=2):
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
        
        # BFS to collect downstream services (successors)
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
    
    def _find_correlated_alert_services(self, service_name, service_groups, already_impacted):
        """
        Find services WITH ALERTS that are correlated to the given service.
        This helps group alerts that are likely related.
        
        Correlation types:
        1. Direct neighbors (immediate upstream/downstream)
        2. Share common root cause (path similarity >= 30%)
        3. Part of cascading failure (impacted services with alerts)
        """
        correlated = set()
        
        if not self.service_graph.has_node(service_name):
            return correlated
        
        # Type 1: Direct neighbors with alerts
        for neighbor in self.service_graph.neighbors(service_name):
            if neighbor in service_groups and neighbor != service_name:
                correlated.add(neighbor)
        
        for predecessor in self.service_graph.predecessors(service_name):
            if predecessor in service_groups and predecessor != service_name:
                correlated.add(predecessor)
        
        # Type 2: Services with high path similarity (share common root causes)
        # USING _compute_path_similarity() for smarter grouping
        PATH_SIMILARITY_THRESHOLD = 0.3  # 30% shared dependencies
        
        for other_service in service_groups.keys():
            if other_service == service_name or other_service in correlated:
                continue
            
            # Use path similarity to find services with similar dependencies
            similarity = self._compute_path_similarity(service_name, other_service)
            
            if similarity >= PATH_SIMILARITY_THRESHOLD:
                # They share significant dependencies - likely related alerts
                correlated.add(other_service)
        
        # Type 3: Cascading failures - impacted services that also have alerts
        for impacted_svc in already_impacted:
            if impacted_svc in service_groups:
                correlated.add(impacted_svc)
        
        return correlated

    def _get_upstream_dependencies(self, service_name, max_depth=3):
        """
        Get upstream services (dependencies) up to max_depth.
        These are services that the given service DEPENDS ON.
        Used for finding services with common root causes.
        
        Example:
            Database → API → Frontend
            Frontend's upstream dependencies = [API, Database]
        """
        dependencies = set()
        
        if not self.service_graph.has_node(service_name):
            return dependencies
        
        # BFS to collect upstream services
        visited = set()
        queue = [(service_name, 0)]
        
        while queue:
            current, depth = queue.pop(0)
            visited.add(current)
            
            if depth >= max_depth:
                continue
            
            # Get predecessors (services that current depends on)
            for predecessor in self.service_graph.predecessors(current):
                if predecessor not in visited and predecessor != service_name:
                    dependencies.add(predecessor)
                    queue.append((predecessor, depth + 1))
        
        return dependencies

    def _compute_path_similarity(self, service1, service2):
        """
        Compute similarity between two services based on shared dependencies.
        High similarity means they might be affected by same root causes.
        Returns a score from 0 to 1 (Jaccard similarity).
        """
        if service1 == service2:
            return 1.0
        
        # Get upstream dependencies for both
        deps1 = self._get_upstream_dependencies(service1, max_depth=3)
        deps2 = self._get_upstream_dependencies(service2, max_depth=3)
        
        if not deps1 or not deps2:
            return 0.0
        
        # Jaccard similarity
        intersection = len(deps1 & deps2)
        union = len(deps1 | deps2)
        
        return intersection / union if union > 0 else 0.0
    
    def _group_unmapped_alerts(self, unmapped_alerts):
        """Group unmapped alerts by cluster, namespace, node, or combination"""
        groups_dict = {}
        
        for alert in unmapped_alerts:
            namespace = alert.get('namespace', 'unknown')
            cluster = alert.get('cluster', 'unknown')
            node = alert.get('node', 'unknown')
            
            # create alert groups
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
    
    def _deduplicate_within_groups(self):
        """
        Deduplicate alerts within graph-based groups BEFORE feature engineering
        """
        self.deduplicated_alerts = []
        self.duplicate_groups = []
        total_duplicates = 0
        total_alerts_before = sum(len(g['alerts']) for g in self.consolidated_groups)
        
        print(f"    Starting deduplication on {total_alerts_before} alerts across {len(self.consolidated_groups)} groups")
        
        for group in self.consolidated_groups:
            group_alerts = group['alerts']
            
            if not group_alerts:
                continue
            
            # Find duplicates within this graph-based group
            processed_indices = set()
            group_unique_alerts = []
            
            for i, alert in enumerate(group_alerts):
                if i in processed_indices:
                    continue
                
                # Mark as not duplicate initially
                alert['is_duplicate'] = False
                alert['duplicate_of'] = None
                
                # Find duplicates of this alert
                duplicates = []
                
                for j in range(i + 1, len(group_alerts)):
                    if j in processed_indices:
                        continue
                    
                    other_alert = group_alerts[j]
                    
                    if self._are_duplicates_graph_based(alert, other_alert):
                        duplicates.append(j)
                        processed_indices.add(j)
                        
                        # Mark as duplicate
                        other_alert['is_duplicate'] = True
                        other_alert['duplicate_of'] = i  # Index within group
                        total_duplicates += 1
                
                # Add representative alert to unique list
                group_unique_alerts.append(alert)
                self.deduplicated_alerts.append(alert)
                
                # Store duplicate group if any
                if duplicates:
                    self.duplicate_groups.append({
                        'group_id': group['group_id'],
                        'representative_idx': i,
                        'duplicate_indices': duplicates,
                        'count': len(duplicates) + 1
                    })
                
                processed_indices.add(i)
            
            # Update group with deduplicated alerts
            group['unique_alerts'] = group_unique_alerts
            group['original_count'] = len(group_alerts)
            group['unique_count'] = len(group_unique_alerts)
            group['duplicate_count'] = len(group_alerts) - len(group_unique_alerts)
        
        total_original = sum(g['original_count'] for g in self.consolidated_groups)
        dedup_rate = (total_duplicates / total_original * 100) if total_original > 0 else 0
        
        print(f"    Found {total_duplicates} duplicates across {len(self.consolidated_groups)} groups")
        print(f"    {len(self.deduplicated_alerts)} unique alerts remain (reduced from {total_original})")
        print(f"    Deduplication rate: {dedup_rate:.1f}%")
        
        # Detailed breakdown per group (for large groups)
        large_groups = [g for g in self.consolidated_groups if g['original_count'] > 10]
        if large_groups:
            print(f"\n    Top 5 groups with most duplicates:")
            large_groups_sorted = sorted(large_groups, key=lambda x: x['duplicate_count'], reverse=True)[:5]
            for g in large_groups_sorted:
                print(f"      Group {g['group_id']}: {g['duplicate_count']} duplicates from {g['original_count']} alerts " +
                      f"({g['duplicate_count']/g['original_count']*100:.1f}% dedup rate)")
        
        # Mark all alerts in enriched_alerts with their deduplication status
        for group in self.consolidated_groups:
            for alert in group['alerts']:
                pass
    
    def _are_duplicates_graph_based(self, alert1, alert2):
        """
        Check if two alerts are duplicates using graph relationships.
        Must meet ALL criteria:
        1. Same alert type OR same alert category
        2. Within time window
        3. Service relationship criteria (one of):
           a. Same service
           b. High path similarity (>60% shared dependencies)
           c. Share significant immediate dependencies (>50%)
        """
        # Check time window FIRST (fast filter)
        time1 = alert1.get('start_timestamp', 0)
        time2 = alert2.get('start_timestamp', 0)
        
        if time1 and time2 and abs(time1 - time2) > self.TIME_WINDOW_MINUTES * 60:
            return False
        
        # Check alert similarity
        alert_name1 = alert1.get('alert_name', '')
        alert_name2 = alert2.get('alert_name', '')
        alert_cat1 = alert1.get('alert_category', '')
        alert_cat2 = alert2.get('alert_category', '')
        
        # Must have similar alert type or category
        same_alert_type = (alert_name1 == alert_name2) and alert_name1
        same_category = (alert_cat1 == alert_cat2) and alert_cat1
        
        if not (same_alert_type or same_category):
            return False
        
        # Check service relationships
        service1 = alert1.get('graph_service')
        service2 = alert2.get('graph_service')
        
        # If either unmapped, use basic matching
        if not service1 or not service2:
            return self._are_duplicates_basic(alert1, alert2)
        
        # 1. Same service → duplicates
        if service1 == service2:
            return True
        
        # 2. HIGH path similarity → likely duplicates (same root cause)
        # USING _compute_path_similarity() for smarter deduplication
        PATH_SIMILARITY_DUPLICATE_THRESHOLD = 0.6  # 60% shared dependencies
        
        similarity = self._compute_path_similarity(service1, service2)
        if similarity >= PATH_SIMILARITY_DUPLICATE_THRESHOLD:
            return True
        
        # 3. Share significant immediate dependencies (fallback for low path similarity)
        # This catches cases where services share critical immediate dependencies
        deps1 = self._get_service_dependencies(service1)
        deps2 = self._get_service_dependencies(service2)
        
        upstream1 = {d['service'] for d in deps1['upstream']}
        upstream2 = {d['service'] for d in deps2['upstream']}
        downstream1 = {d['service'] for d in deps1['downstream']}
        downstream2 = {d['service'] for d in deps2['downstream']}
        
        # Share significant upstream overlap (more than 50%)
        if upstream1 and upstream2:
            overlap = len(upstream1 & upstream2)
            min_size = min(len(upstream1), len(upstream2))
            if min_size > 0 and overlap / min_size > 0.5:
                return True
        
        # Share significant downstream overlap
        if downstream1 and downstream2:
            overlap = len(downstream1 & downstream2)
            min_size = min(len(downstream1), len(downstream2))
            if min_size > 0 and overlap / min_size > 0.5:
                return True
        
        return False
    
    # graph level metrics
    def _calculate_service_relationship_metrics(self, group):
        """Calculate service relationship statistics and scores for a group"""
        primary_service = group.get('primary_service', '')
        
        # Initialize metrics
        metrics = {
            'total_upstream_services': 0,
            'total_downstream_services': 0,
            'total_calls_relationships': 0,
            'total_owns_relationships': 0,
            'total_belongs_to_relationships': 0,
            'impact_propagation_score': 0.0,
            'criticality_score': 0.0
        }
        
        if not primary_service or not self.service_graph.has_node(primary_service):
            return metrics
        
        # Get upstream and downstream services
        upstream_services = set(self.service_graph.predecessors(primary_service))
        downstream_services = set(self.service_graph.successors(primary_service))
        
        metrics['total_upstream_services'] = len(upstream_services)
        metrics['total_downstream_services'] = len(downstream_services)
        
        # Count relationship types
        for upstream in upstream_services:
            edge_data = self.service_graph.get_edge_data(upstream, primary_service)
            if edge_data:
                rel_type = edge_data.get('relationship_type', '')
                if rel_type == 'CALLS':
                    metrics['total_calls_relationships'] += 1
                elif rel_type == 'OWNS':
                    metrics['total_owns_relationships'] += 1
                elif rel_type == 'BELONGS_TO':
                    metrics['total_belongs_to_relationships'] += 1
        
        for downstream in downstream_services:
            edge_data = self.service_graph.get_edge_data(primary_service, downstream)
            if edge_data:
                rel_type = edge_data.get('relationship_type', '')
                if rel_type == 'CALLS':
                    metrics['total_calls_relationships'] += 1
                elif rel_type == 'OWNS':
                    metrics['total_owns_relationships'] += 1
                elif rel_type == 'BELONGS_TO':
                    metrics['total_belongs_to_relationships'] += 1
        
        # Calculate Impact Propagation Score
        # Based on: downstream reach (50%), alert severity (30%), service count in group (20%)
        downstream_transitive = self._get_transitive_downstream(primary_service, max_depth=2)
        downstream_score = min(len(downstream_transitive) / 10.0, 1.0) * 50  # Normalized to 50 points
        
        # Severity score
        severities = [a.get('severity', '') for a in group['alerts']]
        severity_weights = {'critical': 1.0, 'high': 0.75, 'warning': 0.5, 'info': 0.25}
        avg_severity = sum(severity_weights.get(s.lower(), 0) for s in severities) / len(severities) if severities else 0
        severity_score = avg_severity * 30  # 30 points max
        
        # Service count score
        service_count_score = min(group.get('service_count', 1) / 5.0, 1.0) * 20  # 20 points max
        
        metrics['impact_propagation_score'] = round(downstream_score + severity_score + service_count_score, 2)
        
        # Calculate Criticality Score
        # Based on: pagerank (40%), betweenness (30%), degree centrality (20%), alert count (10%)
        pagerank_score = 0
        betweenness_score = 0
        
        if self._pagerank_cache and primary_service in self._pagerank_cache:
            pagerank_score = min(self._pagerank_cache[primary_service] * 1000, 40)  # 40 points max
        
        if self._betweenness_cache and primary_service in self._betweenness_cache:
            betweenness_score = min(self._betweenness_cache[primary_service] * 100, 30)  # 30 points max
        
        # Degree centrality
        degree_score = min(self.service_graph.degree(primary_service) / 20.0, 1.0) * 20  # 20 points max
        
        # Alert count score
        alert_count_score = min(group.get('alert_count', 0) / 50.0, 1.0) * 10  # 10 points max
        
        metrics['criticality_score'] = round(pagerank_score + betweenness_score + degree_score + alert_count_score, 2)
        
        return metrics
    
    def _create_consolidated_output(self):
        """Create summary statistics for consolidated groups"""
        for i, group in enumerate(self.consolidated_groups):
            group['group_id'] = i
            
            severities = [a.get('severity', 'unknown') for a in group['alerts']]
            alert_categories = [a.get('alert_category', 'unknown') for a in group['alerts']]
            alert_subcategories = [a.get('alert_subcategory', 'unknown') for a in group['alerts']]
            
            severity_counter = Counter(severities)
            category_counter = Counter(alert_categories)
            subcategory_counter = Counter(alert_subcategories)
            
            group['severity_distribution'] = dict(severity_counter)
            group['category_distribution'] = dict(category_counter)
            group['subcategory_distribution'] = dict(subcategory_counter)
            
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
            group['most_common_category'] = category_counter.most_common(1)[0][0] if alert_categories else ''
            group['most_common_subcategory'] = subcategory_counter.most_common(1)[0][0] if alert_subcategories else ''
            
            # Calculate service relationship metrics and scores
            relationship_metrics = self._calculate_service_relationship_metrics(group)
            group.update(relationship_metrics)
    
    # ========================================================================
    # PHASE 4: FEATURE ENGINEERING FOR CLUSTERING
    # ========================================================================
    
    def _engineer_features(self):
        """
        Extract comprehensive features for clustering 
        
        CRITICAL: Works on DEDUPLICATED alerts only (after graph-based dedup)
        
        Performance optimizations:
        1. Pre-computed graph features per service (done once in _load_graph_data)
        2. Cached undirected graph conversion and clustering coefficients
        3. Only processes unique alerts (duplicates already removed)
        """
        print("\n[5/8] Engineering features for clustering...")
        
        # Use DEDUPLICATED alerts for feature engineering
        print(f"    Working with {len(self.deduplicated_alerts)} deduplicated alerts")
        
        # assign initial group IDs to deduplicated alerts
        for alert in self.deduplicated_alerts:
            alert['initial_group_id'] = -1
        
        for group in self.consolidated_groups:
            # Use unique_alerts instead of all alerts
            unique_alerts = group.get('unique_alerts', group['alerts'])
            for alert in unique_alerts:
                alert['initial_group_id'] = group['group_id']
        
        # Create DataFrame from deduplicated alerts only
        self.alerts_df = pd.DataFrame(self.deduplicated_alerts)
     
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
        """
        Apply clustering if graph-based groups are too fragmented.
        
        Simple Strategy:
        1. Start with graph-based groups (from Phase 4)
        2. Check if groups are fragmented (many small groups)
        3. If fragmented, apply ML clustering to consolidate
        4. Otherwise, use graph-based groups as-is
        """
        print("\n[6/8] Applying clustering to consolidate groups...")
        
        if len(self.feature_matrix_scaled) < self.MIN_CLUSTERING_SAMPLES:
            print(f"     Not enough alerts for clustering (need >= {self.MIN_CLUSTERING_SAMPLES}, have {len(self.feature_matrix_scaled)})")
            print("     Using graph-based groups only")
            self.alerts_df['cluster_id'] = self.alerts_df['initial_group_id']
            self.alerts_df['clustering_method'] = 'graph_based_only'
            self.final_silhouette_score = None
            self.final_clustering_method = 'graph_based_only'
            self._update_enriched_alerts_with_clusters()
            return
        
        # Analyze graph-based grouping quality
        num_graph_groups = len(self.alerts_df['initial_group_id'].unique())
        num_alerts = len(self.alerts_df)
        avg_group_size = num_alerts / num_graph_groups if num_graph_groups > 0 else 0
        
        print(f"    Graph-based groups: {num_graph_groups}")
        print(f"    Average group size: {avg_group_size:.1f}")
        
        # Check if fragmentation is high (too many groups)
        # Apply clustering if:
        # 1. Too many groups (>50) - regardless of average size
        # 2. Many groups (>20) with small average size (<5)
        is_fragmented = (num_graph_groups > 50) or (num_graph_groups > 20 and avg_group_size < 5)
        
        if is_fragmented:
            print(f"    Groups are fragmented (too many groups: {num_graph_groups})")
            print(f"    Applying ML clustering to consolidate...")
            self._apply_ml_clustering_for_consolidation()
        else:
            print(f"    Groups are well-structured, using graph-based groups")
            self.alerts_df['cluster_id'] = self.alerts_df['initial_group_id']
            self.alerts_df['clustering_method'] = 'graph_based'
            self.final_silhouette_score = None
            self.final_clustering_method = 'graph_based'
        
        # Update enriched alerts with final cluster assignments
        self._update_enriched_alerts_with_clusters()
    
    def _apply_ml_clustering_for_consolidation(self):
        """Apply ML clustering to consolidate fragmented graph-based groups"""
        # Find optimal number of clusters
        best_k = self._find_optimal_k(max_k=min(20, len(self.feature_matrix_scaled) // 5))
        
        print(f"    Running K-Means with k={best_k}...")
        
        # Apply K-Means clustering
        kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(self.feature_matrix_scaled)
        
        # Assign cluster IDs
        self.alerts_df['cluster_id'] = cluster_labels
        self.alerts_df['clustering_method'] = 'ml_kmeans'
        
        # Calculate silhouette score
        try:
            score = silhouette_score(self.feature_matrix_scaled, cluster_labels)
            self.final_silhouette_score = score
            self.final_clustering_method = 'ml_kmeans'
            print(f"    Created {best_k} consolidated clusters (silhouette score: {score:.3f})")
        except:
            self.final_silhouette_score = None
            self.final_clustering_method = 'ml_kmeans'
            print(f"    Created {best_k} consolidated clusters")
        
        # Store results
        self.clustering_results['kmeans'] = {
            'labels': cluster_labels,
            'n_clusters': best_k,
            'algorithm': 'kmeans'
        }
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
    
    def _update_enriched_alerts_with_clusters(self):
        """Update enriched alerts with final cluster assignments"""
        # Build a mapping from deduplicated_alerts to their cluster info
        dedup_alert_to_cluster = {}
        for idx, alert in enumerate(self.deduplicated_alerts):
            # Use a unique identifier for the alert
            alert_key = (
                alert.get('alert_name', ''),
                alert.get('service_name', ''),
                alert.get('pod', ''),
                alert.get('start_timestamp', 0)
            )
            
            # Get cluster info from alerts_df if available
            if idx < len(self.alerts_df):
                cluster_id = self.alerts_df.at[idx, 'cluster_id'] if 'cluster_id' in self.alerts_df.columns else -1
                clustering_method = self.alerts_df.at[idx, 'clustering_method'] if 'clustering_method' in self.alerts_df.columns else 'unknown'
                dedup_alert_to_cluster[alert_key] = {
                    'cluster_id': int(cluster_id),
                    'clustering_method': clustering_method
                }
        
        # Update all enriched alerts (including duplicates)
        for alert in self.enriched_alerts:
            alert_key = (
                alert.get('alert_name', ''),
                alert.get('service_name', ''),
                alert.get('pod', ''),
                alert.get('start_timestamp', 0)
            )
            
            # Check if this is a duplicate
            if alert.get('is_duplicate', False):
                # For duplicates, find their representative alert's cluster
                # Duplicates should inherit cluster info from their representative
                if alert_key in dedup_alert_to_cluster:
                    cluster_info = dedup_alert_to_cluster[alert_key]
                    alert['cluster_id'] = cluster_info['cluster_id']
                    # Mark as duplicate with base method
                    base_method = cluster_info['clustering_method']
                    if base_method == 'graph_based':
                        alert['clustering_method'] = 'graph_based (duplicate)'
                    elif base_method == 'ml_kmeans':
                        alert['clustering_method'] = 'ml_clustering (duplicate)'
                    else:
                        alert['clustering_method'] = f'{base_method} (duplicate)'
                else:
                    # Fallback: use initial_group_id if available
                    alert['cluster_id'] = alert.get('initial_group_id', -1)
                    alert['clustering_method'] = 'graph_based (duplicate)'
            else:
                # For non-duplicates, get cluster info from mapping
                if alert_key in dedup_alert_to_cluster:
                    cluster_info = dedup_alert_to_cluster[alert_key]
                    alert['cluster_id'] = cluster_info['cluster_id']
                    # Standardize clustering method names
                    method = cluster_info['clustering_method']
                    if method == 'graph_based_only' or method == 'graph_based':
                        alert['clustering_method'] = 'graph_based'
                    elif method == 'ml_kmeans':
                        alert['clustering_method'] = 'ml_clustering'
                    else:
                        alert['clustering_method'] = method
                else:
                    # Not found - shouldn't happen for non-duplicates
                    alert['cluster_id'] = alert.get('initial_group_id', -1)
                    alert['clustering_method'] = 'graph_based'

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
        """
        Get transitive downstream services (services that depend on this service).
        
        This is useful for root cause analysis:
        - If this service has alerts AND many downstream dependencies
        - Then this service is likely a ROOT CAUSE affecting those downstream services
        - Downstream count indicates the blast radius / criticality
        """
        downstream_set = set()
        
        if not self.service_graph.has_node(service_name):
            return downstream_set
        
        # BFS to collect downstream services (successors)
        visited = set()
        queue = [(service_name, 0)]  # (service, depth)
        
        while queue:
            current, depth = queue.pop(0)
            visited.add(current)
            
            if depth >= max_depth:
                continue
            
            # Get immediate downstream (services that depend on current)
            for successor in self.service_graph.successors(current):
                if successor not in visited:
                    downstream_set.add(successor)
                    queue.append((successor, depth + 1))
        
        return downstream_set
    
    # ========================================================================
    # CLUSTER NAMING AND RANKING
    # ========================================================================
    
    def _generate_cluster_name(self, cluster_alerts, cluster_id, description_data=None):
        """
        Generate a unique, meaningful cluster name based on cluster context.
        Uses cluster description data to create human-readable names.
        
        Format: "{issue_type}-in-{primary_service}-affecting-{scope}" or
                "{issue_type}-across-{service_count}-services-{impact}"
        """
        if not cluster_alerts:
            return f"cluster_{cluster_id}_empty"
        
        # Use description data if available (generated during description phase)
        if description_data:
            return self._generate_name_from_description(cluster_id, description_data)
        
        # Fallback: Generate name from alert data directly
        services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
        categories = [a.get('alert_category', '') for a in cluster_alerts if a.get('alert_category')]
        subcategories = [a.get('alert_subcategory', '') for a in cluster_alerts if a.get('alert_subcategory')]
        severities = [a.get('severity', '').lower() for a in cluster_alerts]
        namespaces = [a.get('namespace', '') for a in cluster_alerts if a.get('namespace')]
        
        # Get primary issue type (category + subcategory)
        category = Counter(categories).most_common(1)[0][0] if categories else 'unknown'
        subcategory = Counter(subcategories).most_common(1)[0][0] if subcategories else ''
        
        # Build issue type string
        if subcategory:
            issue_type = f"{subcategory}-{category}".lower().replace(' ', '-')
        else:
            issue_type = category.lower().replace(' ', '-')
        
        # Get service context
        service_count = len(set(services))
        if service_count == 0:
            service_context = "unmapped-services"
        elif service_count == 1:
            # Single service - use service name
            service_name = list(set(services))[0]
            service_short = self._shorten_service_name(service_name)
            service_context = f"{service_short}"
        else:
            # Multiple services
            service_context = f"{service_count}-services"
        
        # Get severity context
        critical_count = severities.count('critical')
        high_count = severities.count('high')
        if critical_count > len(cluster_alerts) * 0.3:
            severity_context = "critical"
        elif high_count > len(cluster_alerts) * 0.5:
            severity_context = "high-severity"
        else:
            severity_context = "alerts"
        
        # Build cluster name
        if service_count == 1:
            # Single service issue
            cluster_name = f"{issue_type}-in-{service_context}-{severity_context}"
        else:
            # Multi-service issue
            cluster_name = f"{issue_type}-across-{service_context}-{severity_context}"
        
        # Add cluster ID to ensure uniqueness
        cluster_name = f"c{cluster_id}_{cluster_name}"
        
        # Clean up and limit length
        cluster_name = re.sub(r'[^a-z0-9\-_]', '', cluster_name.lower())
        cluster_name = re.sub(r'[-_]+', '-', cluster_name)  # Remove duplicate separators
        cluster_name = cluster_name.strip('-_')
        cluster_name = cluster_name[:80]  # Limit length
        
        return cluster_name
    
    def _generate_name_from_description(self, cluster_id, description_data):
        """
        Generate cluster name from rich description data.
        More contextual and relatable to the cluster description.
        """
        # Extract key information
        root_cause = description_data.get('root_cause_summary', '')
        primary_services = description_data.get('primary_services', '')
        affected_count = description_data.get('affected_service_count', 0)
        category = description_data.get('most_common_category', 'unknown')
        subcategory = description_data.get('most_common_subcategory', '')
        
        # Parse primary root cause (first one)
        if root_cause:
            main_cause = root_cause.split(';')[0].strip().lower()
            # Simplify root cause to key terms
            if 'memory saturation' in main_cause:
                issue = 'memory-saturation'
            elif 'cpu saturation' in main_cause:
                issue = 'cpu-saturation'
            elif 'hpa scaling' in main_cause:
                issue = 'hpa-scaling-issue'
            elif 'cascading failure' in main_cause:
                issue = 'cascading-failure'
            elif 'upstream service failure' in main_cause:
                issue = 'upstream-failure'
            elif 'latency anomaly' in main_cause:
                issue = 'latency-anomaly'
            elif 'service error' in main_cause:
                issue = 'service-errors'
            elif 'critical service' in main_cause:
                issue = 'critical-service-issue'
            elif 'widespread issue' in main_cause:
                issue = 'widespread-issue'
            else:
                # Fallback to category-subcategory
                if subcategory:
                    issue = f"{subcategory}-{category}".lower().replace(' ', '-')
                else:
                    issue = category.lower().replace(' ', '-')
        else:
            # No root cause - use category
            if subcategory:
                issue = f"{subcategory}-{category}".lower().replace(' ', '-')
            else:
                issue = category.lower().replace(' ', '-')
        
        # Parse primary service (first one)
        if primary_services and primary_services != 'Unmapped services':
            # Extract first service name (format: "service-name (count)")
            first_service = primary_services.split(',')[0].split('(')[0].strip()
            service_short = self._shorten_service_name(first_service)
            service_count = len(primary_services.split(','))
            
            if service_count == 1:
                service_context = f"in-{service_short}"
            else:
                service_context = f"in-{service_short}-plus-{service_count-1}-more"
        else:
            service_context = "in-unmapped-services"
        
        # Add impact context
        if affected_count > 10:
            impact = f"impacting-{affected_count}-services"
        elif affected_count > 0:
            impact = f"affecting-downstream"
        else:
            impact = "isolated"
        
        # Build name
        cluster_name = f"c{cluster_id}_{issue}_{service_context}_{impact}"
        
        # Clean up and limit length
        cluster_name = re.sub(r'[^a-z0-9\-_]', '', cluster_name.lower())
        cluster_name = re.sub(r'[-_]+', '-', cluster_name)
        cluster_name = cluster_name.strip('-_')
        cluster_name = cluster_name[:80]
        
        return cluster_name
    
    def _shorten_service_name(self, service_name):
        """Extract short, meaningful name from full service name"""
        if not service_name:
            return 'unknown'
        
        # Remove common prefixes and suffixes
        clean_name = service_name.lower()
        clean_name = re.sub(r'^(service-|svc-|app-)', '', clean_name)
        clean_name = re.sub(r'(-service|-svc|-app)$', '', clean_name)
        
        # Extract last meaningful part if it has separators
        if ':' in clean_name:
            clean_name = clean_name.split(':')[-1]
        if '/' in clean_name:
            clean_name = clean_name.split('/')[-1]
        if '.' in clean_name:
            parts = clean_name.split('.')
            # Use last non-empty part
            clean_name = next((p for p in reversed(parts) if p), parts[0])
        
        # Limit length
        clean_name = clean_name[:20]
        
        # Clean up
        clean_name = re.sub(r'[^a-z0-9]', '', clean_name)
        
        return clean_name if clean_name else 'unknown'
    
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
    
    def _calculate_cluster_confidence(self, cluster_alerts, clustering_method):
        """
        Calculate confidence score for a cluster (0-100).
        Higher confidence = more certain about the grouping.
        """
        if not cluster_alerts:
            return 0.0
        
        confidence = 0.0
        
        # Factor 1: Mapping confidence (30% weight)
        mapping_confidences = [a.get('mapping_confidence', 0) for a in cluster_alerts]
        avg_mapping_confidence = np.mean(mapping_confidences) if mapping_confidences else 0
        confidence += avg_mapping_confidence * 30
        
        # Factor 2: Clustering method quality (25% weight)
        if clustering_method == 'graph_based':
            # Graph-based grouping is high confidence if well-mapped
            mapped_count = sum(1 for a in cluster_alerts if a.get('graph_service'))
            mapping_rate = mapped_count / len(cluster_alerts)
            confidence += mapping_rate * 25
        elif clustering_method == 'ml_kmeans':
            # ML clustering confidence based on silhouette score if available
            if hasattr(self, 'final_silhouette_score') and self.final_silhouette_score:
                # Normalize silhouette score (-1 to 1) to 0 to 25
                normalized_score = (self.final_silhouette_score + 1) / 2 * 25
                confidence += normalized_score
            else:
                confidence += 15  # Default moderate confidence
        elif clustering_method == 'sub_clustered':
            confidence += 20  # Slightly lower confidence for sub-clusters
        else:
            confidence += 10  # Low confidence for other methods
        
        # Factor 3: Alert similarity (25% weight)
        alert_types = [a.get('alert_name', '') for a in cluster_alerts]
        categories = [a.get('alert_category', '') for a in cluster_alerts if a.get('alert_category')]
        
        # Higher similarity = higher confidence
        if alert_types:
            unique_ratio = len(set(alert_types)) / len(alert_types)
            similarity_score = (1 - unique_ratio) * 25  # Lower unique ratio = more similar
            confidence += similarity_score
        
        # Factor 4: Temporal proximity (20% weight)
        timestamps = [a.get('start_timestamp', 0) for a in cluster_alerts if a.get('start_timestamp')]
        if timestamps and len(timestamps) > 1:
            time_span = max(timestamps) - min(timestamps)
            # Shorter time span = higher confidence (alerts happening together)
            if time_span < 300:  # Within 5 minutes
                confidence += 20
            elif time_span < 900:  # Within 15 minutes
                confidence += 15
            elif time_span < 1800:  # Within 30 minutes
                confidence += 10
            else:
                confidence += 5
        elif len(timestamps) == 1:
            confidence += 20  # Single timestamp = simultaneous
        
        # Cap at 100
        return min(confidence, 100.0)
    
    def _rank_and_name_clusters(self):
        """Rank clusters, assign names, and generate detailed descriptions"""
        print("\n[7.5/8] Naming and ranking clusters with detailed descriptions...")
        
        # Group by cluster_id
        cluster_dict = {}
        for alert in self.enriched_alerts:
            cluster_id = alert.get('cluster_id', -1)
            if cluster_id not in cluster_dict:
                cluster_dict[cluster_id] = []
            cluster_dict[cluster_id].append(alert)
        
        # Calculate scores, generate names, and create descriptions
        cluster_metadata = []
        
        for cluster_id, cluster_alerts in cluster_dict.items():
            if cluster_id == -1:
                continue  # Skip noise/outliers
            
            # Generate comprehensive cluster description FIRST
            description_data = self._generate_cluster_description(cluster_alerts, cluster_id)
            
            # Generate distinctive name based on description data
            cluster_name = self._generate_cluster_name(cluster_alerts, cluster_id, description_data)
            
            # Calculate ranking score
            score = self._calculate_cluster_score(cluster_alerts, cluster_id)
            
            # Calculate confidence score
            clustering_method = cluster_alerts[0].get('clustering_method', 'unknown') if cluster_alerts else 'unknown'
            confidence = self._calculate_cluster_confidence(cluster_alerts, clustering_method)
            
            # Combine metadata
            metadata = {
                'cluster_id': cluster_id,
                'cluster_name': cluster_name,
                'ranking_score': round(score, 2),
                'confidence_score': round(confidence, 2),
                'alert_count': len(cluster_alerts),
                'clustering_method': clustering_method,
                **description_data  # Merge description data
            }
            
            cluster_metadata.append(metadata)
        
        # Sort by ranking score (highest first)
        cluster_metadata.sort(key=lambda x: x['ranking_score'], reverse=True)
        
        # Assign ranks
        for rank, metadata in enumerate(cluster_metadata, start=1):
            metadata['rank'] = rank
            
            # Add rank, name, and description to alerts in this cluster
            cluster_id = metadata['cluster_id']
            if cluster_id in cluster_dict:
                for alert in cluster_dict[cluster_id]:
                    alert['cluster_rank'] = rank
                    alert['cluster_name'] = metadata['cluster_name']
                    alert['cluster_score'] = metadata['ranking_score']
        
        # Handle alerts with cluster_id = -1 (outliers/unmapped)
        if -1 in cluster_dict:
            for alert in cluster_dict[-1]:
                alert['cluster_rank'] = -1
                alert['cluster_name'] = 'outlier_or_unmapped'
                alert['cluster_score'] = 0.0
        
        # Export ranked clusters
        self._export_ranked_clusters(cluster_metadata)
        
        # Export detailed cluster descriptions
        self._export_cluster_descriptions(cluster_metadata)
        
        # Export cluster-level summary (main dashboard view)
        self._export_cluster_level_summary(cluster_metadata)
        
        # Export per-cluster drill-down (deduplicated alerts per cluster)
        self._export_per_cluster_drill_down()
        
        print(f"\n Ranked {len(cluster_metadata)} clusters with descriptions")
        print(f"\n Top 5 clusters by importance:")
        for i, meta in enumerate(cluster_metadata[:5], 1):
            print(f"\n      Rank {i}: {meta['cluster_name']}")
            print(f"         Score: {meta['ranking_score']:.1f} | Confidence: {meta.get('confidence_score', 0):.1f}% | Alerts: {meta['alert_count']}")
            print(f"         Primary Services: {meta.get('primary_services', 'N/A')[:65]}")
            
            # Show root cause services if identified
            root_cause_svcs = meta.get('root_cause_services', 'None identified')
            if root_cause_svcs != 'None identified':
                print(f"         Root Cause Services: {root_cause_svcs[:65]}")
            
            print(f"         Root Cause: {meta.get('root_cause_summary', 'N/A')[:75]}")
    
    def _generate_cluster_description(self, cluster_alerts, cluster_id):
        """
        Generate comprehensive description for a cluster including:
        - Primary services (services with alerts)
        - Most affected services (downstream dependencies)
        - Potential root causes (based on alert patterns and topology)
        """
        # Extract alert information
        alert_types = [a.get('alert_name', '') for a in cluster_alerts]
        services = [a.get('graph_service', '') for a in cluster_alerts if a.get('graph_service')]
        categories = [a.get('alert_category', '') for a in cluster_alerts if a.get('alert_category')]
        subcategories = [a.get('alert_subcategory', '') for a in cluster_alerts if a.get('alert_subcategory')]
        severities = [a.get('severity', '') for a in cluster_alerts]
        namespaces = [a.get('namespace', '') for a in cluster_alerts if a.get('namespace')]
        
        # --- PRIMARY SERVICES ---
        # Services that have alerts in this cluster
        service_counts = Counter(services)
        primary_services_list = [f"{svc} ({count})" for svc, count in service_counts.most_common(5)]
        primary_services_str = ', '.join(primary_services_list) if primary_services_list else 'Unmapped services'
        
        # --- ROOT CAUSE SERVICES (Calculate first) ---
        # Identify services that are likely root causes (have significant downstream impact)
        root_cause_services_preliminary = []
        root_cause_services_set = set()
        ROOT_CAUSE_THRESHOLD = 3  # Services affecting 3+ downstream services are potential root causes
        
        for service in set(services):
            if service and self.service_graph.has_node(service):
                downstream = self._get_transitive_downstream(service, max_depth=2)
                downstream_count = len(downstream)
                if downstream_count >= ROOT_CAUSE_THRESHOLD:
                    root_cause_services_preliminary.append({
                        'service': service,
                        'downstream_count': downstream_count,
                        'downstream_services': downstream
                    })
                    root_cause_services_set.add(service)
        
        # Sort by downstream count - most impactful services first
        root_cause_services_preliminary.sort(key=lambda x: x['downstream_count'], reverse=True)
        
        # --- MOST AFFECTED SERVICES ---
        # Calculate affected services conservatively to avoid inflated numbers
        affected_services = set()
        MAX_AFFECTED_DISPLAY = 30  # Cap at 30 services maximum for realistic reporting
        
        if root_cause_services_preliminary:
            # Take ONLY the #1 root cause service to avoid double-counting
            top_root_cause = root_cause_services_preliminary[0]
            service = top_root_cause['service']
            downstream = top_root_cause['downstream_services']
            
            # Apply strict cap - if a service impacts >50 services, it's likely shared infrastructure
            # We cap reporting at 30 to keep it realistic and actionable
            if len(downstream) > 50:
                # This is likely a shared service (API gateway, message queue, etc.)
                # Use more conservative estimate - only count direct dependencies (depth=1)
                conservative_downstream = self._find_impacted_services(service, max_depth=1)
                if len(conservative_downstream) <= MAX_AFFECTED_DISPLAY:
                    affected_services = conservative_downstream
                else:
                    # Even direct deps are too many - cap at max display
                    affected_services = set(list(conservative_downstream)[:MAX_AFFECTED_DISPLAY])
            else:
                affected_services = downstream
        else:
            # No root causes with high impact - use primary service's immediate downstream only
            if services:
                primary_service = Counter(services).most_common(1)[0][0]
                if primary_service and self.service_graph.has_node(primary_service):
                    # Only look at direct downstream (depth=1) for conservative estimate
                    downstream = self._find_impacted_services(primary_service, max_depth=1)
                    affected_services = downstream if len(downstream) <= MAX_AFFECTED_DISPLAY else set(list(downstream)[:MAX_AFFECTED_DISPLAY])
        
        # Remove services that are already in the primary list (they're the cause, not affected)
        affected_services = affected_services - set(services)
        
        # Final cap at 30 services for display
        affected_services_list = list(affected_services)[:MAX_AFFECTED_DISPLAY]
        affected_services_str = ', '.join(affected_services_list[:10]) if affected_services_list else 'None identified'
        affected_count = len(affected_services_list)
        
        # --- POTENTIAL ROOT CAUSES ---
        root_causes = []
        
        # Analyze alert patterns
        category_dist = Counter(categories)
        subcategory_dist = Counter(subcategories)
        alert_type_dist = Counter(alert_types)
        
        # Most common category and subcategory
        most_common_category = category_dist.most_common(1)[0][0] if category_dist else 'unknown'
        most_common_subcategory = subcategory_dist.most_common(1)[0][0] if subcategory_dist else 'unknown'
        most_common_alert = alert_type_dist.most_common(1)[0][0] if alert_type_dist else 'unknown'
        
        # Root cause inference based on patterns
        if most_common_category == 'saturation':
            if most_common_subcategory == 'memory':
                root_causes.append("Memory saturation")
            elif most_common_subcategory == 'cpu':
                root_causes.append("CPU saturation")
            elif most_common_subcategory == 'hpa':
                root_causes.append("HPA scaling issues")
            else:
                root_causes.append("Resource saturation")
        
        if most_common_category == 'anomaly':
            if most_common_subcategory == 'latency':
                root_causes.append("Latency anomaly")
            else:
                root_causes.append("Anomalous behavior detected")
        
        if most_common_category == 'error':
            root_causes.append("Service errors")
        
        if most_common_category == 'critical' or most_common_category == 'failure':
            root_causes.append("Critical service failure")
        
        # Check for cascading failures (multiple services in dependency chain)
        if len(set(services)) > 1:
            # Check if services are in dependency relationship
            service_list = list(set(services))
            has_dependencies = False
            for i, svc1 in enumerate(service_list):
                for svc2 in service_list[i+1:]:
                    if (self.service_graph.has_edge(svc1, svc2) or 
                        self.service_graph.has_edge(svc2, svc1)):
                        has_dependencies = True
                        break
                if has_dependencies:
                    break
            
            if has_dependencies:
                root_causes.append("Cascading failure across dependent services")
        
        # Check for widespread issues (multiple namespaces or clusters)
        if len(set(namespaces)) > 1:
            root_causes.append(f"Widespread issue across {len(set(namespaces))} namespaces")
        
        # Topology-based root cause analysis
        if services:
            # Analyze primary services for root cause indicators
            primary_service = service_counts.most_common(1)[0][0] if service_counts else None
            if primary_service and self.service_graph.has_node(primary_service):
                # Get transitive downstream dependencies (potential impact)
                downstream_transitive = self._get_transitive_downstream(primary_service, max_depth=2)
                downstream_count = len(downstream_transitive)
                
                # High downstream count = this service is likely ROOT CAUSE
                if downstream_count > 10:
                    root_causes.append(f"Root cause service ({primary_service}) impacting {downstream_count} downstream dependencies")
                elif downstream_count > 5:
                    root_causes.append(f"Upstream service failure ({primary_service}) affecting {downstream_count} dependencies")
                
                # Check centrality - high centrality services are critical
                if self._pagerank_cache and primary_service in self._pagerank_cache:
                    pagerank = self._pagerank_cache[primary_service]
                    if pagerank > 0.01:  # Threshold for "important" service
                        root_causes.append(f"Critical service in dependency graph")
                
                # Check if this service is a bottleneck (high in-degree and out-degree)
                in_degree = self.service_graph.in_degree(primary_service)
                out_degree = self.service_graph.out_degree(primary_service)
                if in_degree > 3 and out_degree > 3:
                    root_causes.append(f"Bottleneck service with {in_degree} upstream and {out_degree} downstream connections")
        
        # Default root cause if none identified
        if not root_causes:
            root_causes.append(f"{most_common_category.capitalize()} in {most_common_subcategory}")
        
        root_cause_summary = '; '.join(root_causes[:3])  # Top 3 root causes
        
        # --- DETAILED DESCRIPTION ---
        # Generate human-readable description
        severity_dist = Counter(severities)
        critical_count = severity_dist.get('critical', 0)
        high_count = severity_dist.get('high', 0)
        
        severity_desc = ""
        if critical_count > 0:
            severity_desc = f"{critical_count} critical"
        if high_count > 0:
            if severity_desc:
                severity_desc += f", {high_count} high severity"
            else:
                severity_desc = f"{high_count} high severity"
        if not severity_desc:
            severity_desc = f"{len(cluster_alerts)} alerts"
        
        description_parts = [
            f"Cluster of {severity_desc} alerts",
            f"affecting {len(set(services))} service(s)" if services else "from unmapped services",
        ]
        
        if affected_count > 0:
            description_parts.append(f"potentially impacting {affected_count} downstream service(s)")
        
        description_parts.append(f"Primary issue: {root_cause_summary}")
        
        detailed_description = '. '.join(description_parts) + '.'
        
        # --- ALERT TIMELINE ---
        timestamps = [a.get('start_timestamp', 0) for a in cluster_alerts if a.get('start_timestamp')]
        if timestamps:
            earliest = min(timestamps)
            latest = max(timestamps)
            duration_minutes = (latest - earliest) / 60
            timeline = f"{duration_minutes:.1f} minutes" if duration_minutes > 0 else "simultaneous"
        else:
            timeline = "unknown"
        
        # --- ROOT CAUSE SERVICES (Format for output) ---
        # Report only top 3 root causes for clarity
        root_cause_services = []
        
        # Format the top root causes (up to 3)
        top_n = min(3, len(root_cause_services_preliminary))
        for rc in root_cause_services_preliminary[:top_n]:
            service = rc['service']
            count = rc['downstream_count']
            # Cap the displayed count at MAX_AFFECTED_DISPLAY for consistency
            display_count = min(count, 30)
            root_cause_services.append(f"{service} (impacts {display_count}+ services)" if count > 30 else f"{service} (impacts {count} services)")
        
        root_cause_services_str = ', '.join(root_cause_services) if root_cause_services else 'None identified'
        
        return {
            'primary_services': primary_services_str,
            'primary_service_count': len(set(services)),
            'affected_services': affected_services_str,
            'affected_service_count': affected_count,
            'root_cause_services': root_cause_services_str,
            'root_cause_summary': root_cause_summary,
            'detailed_description': detailed_description,
            'most_common_alert': most_common_alert,
            'most_common_category': most_common_category,
            'most_common_subcategory': most_common_subcategory,
            'unique_alert_types': len(set(alert_types)),
            'severity_distribution': str(dict(severity_dist)),
            'alert_timeline': timeline,
            'namespaces': ', '.join(sorted(set(namespaces))[:5]) if namespaces else 'N/A',
        }
    
    def _export_ranked_clusters(self, cluster_metadata):
        """Export ranked cluster summary"""
        df_ranked = pd.DataFrame(cluster_metadata)
        ranked_path = f'{self.output_dir}/ranked_clusters.csv'
        df_ranked.to_csv(ranked_path, index=False)
        print(f" Ranked clusters: {ranked_path}")
    
    def _export_cluster_descriptions(self, cluster_metadata):
        """Export detailed cluster descriptions with root cause analysis"""
        # Create a focused description view
        descriptions = []
        
        for meta in cluster_metadata:
            descriptions.append({
                'rank': meta['rank'],
                'cluster_id': meta['cluster_id'],
                'cluster_name': meta['cluster_name'],
                'alert_count': meta['alert_count'],
                'ranking_score': meta['ranking_score'],
                'confidence_score': meta.get('confidence_score', 0),
                'clustering_method': meta.get('clustering_method', ''),
                'detailed_description': meta.get('detailed_description', ''),
                'primary_services': meta.get('primary_services', ''),
                'root_cause_services': meta.get('root_cause_services', ''),
                'affected_services': meta.get('affected_services', ''),
                'affected_service_count': meta.get('affected_service_count', 0),
                'root_cause_summary': meta.get('root_cause_summary', ''),
                'most_common_alert': meta.get('most_common_alert', ''),
                'severity_distribution': meta.get('severity_distribution', ''),
                'alert_timeline': meta.get('alert_timeline', ''),
                'namespaces': meta.get('namespaces', ''),
            })
        
        df_descriptions = pd.DataFrame(descriptions)
        desc_path = f'{self.output_dir}/cluster_descriptions.csv'
        df_descriptions.to_csv(desc_path, index=False)
        print(f" Cluster descriptions: {desc_path}")
    
    def _export_cluster_level_summary(self, cluster_metadata):
        """
        Export cluster-level summary view.
        This is the main dashboard view showing all clusters at a glance.
        """
        print("    Generating cluster-level summary...")
        
        summary = []
        for meta in cluster_metadata:
            summary.append({
                'rank': meta['rank'],
                'cluster_id': meta['cluster_id'],
                'cluster_name': meta['cluster_name'],
                'total_alerts': meta['alert_count'],
                'unique_deduplicated_alerts': meta.get('unique_count', meta['alert_count']),
                'clustering_method': meta.get('clustering_method', ''),
                'confidence_score': meta.get('confidence_score', 0),
                'ranking_score': meta['ranking_score'],
                'primary_services': meta.get('primary_services', ''),
                'primary_service_count': meta.get('primary_service_count', 0),
                'root_cause_services': meta.get('root_cause_services', ''),
                'most_common_issue': meta.get('most_common_alert', ''),
                'issue_category': meta.get('most_common_category', ''),
                'affected_services': meta.get('affected_services', ''),
                'affected_service_count': meta.get('affected_service_count', 0),
                'root_cause_summary': meta.get('root_cause_summary', ''),
                'detailed_description': meta.get('detailed_description', ''),
                'severity_distribution': meta.get('severity_distribution', ''),
                'alert_timeline': meta.get('alert_timeline', ''),
                'namespaces': meta.get('namespaces', ''),
            })
        
        df_summary = pd.DataFrame(summary)
        summary_path = f'{self.output_dir}/cluster_level_summary.csv'
        df_summary.to_csv(summary_path, index=False)
        print(f" Cluster-level summary: {summary_path}")
        
        return df_summary
    
    def _export_per_cluster_drill_down(self):
        """
        Export per-cluster drill-down view showing deduplicated alerts.
        This allows users to see the actual alerts within each cluster
        with both graph-based and cluster-based grouping metadata.
        """
        print("    Generating per-cluster drill-down views...")
        
        # Group deduplicated alerts by cluster_id
        cluster_alerts_dict = {}
        for alert in self.deduplicated_alerts:
            cluster_id = alert.get('cluster_id', -1)
            if cluster_id == -1:
                continue  # Skip outliers
            
            if cluster_id not in cluster_alerts_dict:
                cluster_alerts_dict[cluster_id] = []
            cluster_alerts_dict[cluster_id].append(alert)
        
        # Create drill-down data
        drill_down_data = []
        
        for cluster_id in sorted(cluster_alerts_dict.keys()):
            alerts = cluster_alerts_dict[cluster_id]
            
            # Get cluster metadata (name, rank, etc.)
            cluster_name = alerts[0].get('cluster_name', f'cluster_{cluster_id}') if alerts else f'cluster_{cluster_id}'
            cluster_rank = alerts[0].get('cluster_rank', -1) if alerts else -1
            
            for alert in alerts:
                drill_down_data.append({
                    # Cluster metadata
                    'cluster_rank': cluster_rank,
                    'cluster_id': cluster_id,
                    'cluster_name': cluster_name,
                    
                    # Graph-based grouping metadata
                    'initial_group_id': alert.get('initial_group_id', -1),
                    'graph_service': alert.get('graph_service', ''),
                    'mapping_method': alert.get('mapping_method', ''),
                    'mapping_confidence': alert.get('mapping_confidence', 0),
                    
                    # Cluster-based grouping metadata
                    'clustering_method': alert.get('clustering_method', ''),
                    
                    # Alert details
                    'alert_name': alert.get('alert_name', ''),
                    'alert_category': alert.get('alert_category', ''),
                    'alert_subcategory': alert.get('alert_subcategory', ''),
                    'severity': alert.get('severity', ''),
                    'description': alert.get('description', ''),
                    
                    # Service/location info
                    'service_name': alert.get('service_name', ''),
                    'namespace': alert.get('namespace', ''),
                    'pod': alert.get('pod', ''),
                    'node': alert.get('node', ''),
                    'cluster': alert.get('cluster', ''),
                    'workload_type': alert.get('workload_type', ''),
                    
                    # Service graph topology
                    'upstream_count': len(alert.get('dependencies', {}).get('upstream', [])),
                    'downstream_count': len(alert.get('dependencies', {}).get('downstream', [])),
                    
                    # Temporal
                    'starts_at': alert.get('startsAt', ''),
                    'start_timestamp': alert.get('start_timestamp', 0),
                })
        
        # Create DataFrame and export
        df_drill_down = pd.DataFrame(drill_down_data)
        
        # Sort by cluster_rank, then cluster_id, then start_timestamp
        df_drill_down = df_drill_down.sort_values(
            ['cluster_rank', 'cluster_id', 'start_timestamp'],
            ascending=[True, True, True]
        )
        
        drill_down_path = f'{self.output_dir}/per_cluster_drill_down.csv'
        df_drill_down.to_csv(drill_down_path, index=False)
        print(f" Per-cluster drill-down: {drill_down_path}")
        
        # Also export per-cluster individual files for easy filtering
        self._export_individual_cluster_files(cluster_alerts_dict)
        
        return df_drill_down
    
    def _export_individual_cluster_files(self, cluster_alerts_dict):
        """Export individual CSV files per cluster for easy drill-down"""
        clusters_dir = f'{self.output_dir}/clusters'
        os.makedirs(clusters_dir, exist_ok=True)
        
        for cluster_id, alerts in cluster_alerts_dict.items():
            if not alerts:
                continue
            
            cluster_name = alerts[0].get('cluster_name', f'cluster_{cluster_id}')
            
            # Create detailed alert data for this cluster
            cluster_data = []
            for alert in alerts:
                cluster_data.append({
                    'alert_name': alert.get('alert_name', ''),
                    'severity': alert.get('severity', ''),
                    'alert_category': alert.get('alert_category', ''),
                    'service_name': alert.get('service_name', ''),
                    'graph_service': alert.get('graph_service', ''),
                    'namespace': alert.get('namespace', ''),
                    'pod': alert.get('pod', ''),
                    'starts_at': alert.get('startsAt', ''),
                    'description': alert.get('description', ''),
                    'mapping_method': alert.get('mapping_method', ''),
                    'mapping_confidence': alert.get('mapping_confidence', 0),
                    'clustering_method': alert.get('clustering_method', ''),
                })
            
            df_cluster = pd.DataFrame(cluster_data)
            
            # Safe filename
            safe_name = re.sub(r'[^a-z0-9\-_]', '', cluster_name.lower())
            cluster_file = f'{clusters_dir}/cluster_{cluster_id}_{safe_name}.csv'
            df_cluster.to_csv(cluster_file, index=False)
        
        print(f"    Exported {len(cluster_alerts_dict)} individual cluster files to {clusters_dir}/")
    
    # ========================================================================
    # PHASE 7: EXPORT RESULTS
    # ========================================================================
    
    def _export_results(self):
        """Export consolidated and clustered results"""
        print("\n[8/8] Exporting results...")
        
        # Prepare final output - include ALL alerts (both duplicates and unique)
        output_data = []
        
        # We need to export all enriched_alerts, not just deduplicated ones
        # Duplicates are marked with is_duplicate flag
        for i, alert in enumerate(self.enriched_alerts):
            graph_service = alert.get('graph_service', '')
            
            # Get service graph-level metrics
            service_pagerank = 0.0
            service_betweenness = 0.0
            service_degree = 0
            service_upstream_count = 0
            service_downstream_count = 0
            
            if graph_service and self.service_graph.has_node(graph_service):
                # PageRank
                if self._pagerank_cache and graph_service in self._pagerank_cache:
                    service_pagerank = round(self._pagerank_cache[graph_service], 6)
                
                # Betweenness centrality
                if self._betweenness_cache and graph_service in self._betweenness_cache:
                    service_betweenness = round(self._betweenness_cache[graph_service], 6)
                
                # Degree metrics
                service_degree = self.service_graph.degree(graph_service)
                service_upstream_count = self.service_graph.in_degree(graph_service)
                service_downstream_count = self.service_graph.out_degree(graph_service)
            
            # Get group-level impact propagation and criticality scores
            impact_propagation_score = 0.0
            criticality_score = 0.0
            
            # Find the consolidated group for this alert
            initial_group_id = alert.get('initial_group_id', -1)
            for group in self.consolidated_groups:
                if group.get('group_id') == initial_group_id:
                    impact_propagation_score = group.get('impact_propagation_score', 0.0)
                    criticality_score = group.get('criticality_score', 0.0)
                    break
            
            output_data.append({
                'alert_id': i,
                'final_group_id': alert.get('cluster_id', -1),
                'initial_group_id': initial_group_id,
                'clustering_method': alert.get('clustering_method', ''),
                'is_duplicate': alert.get('is_duplicate', False),
                'duplicate_of': alert.get('duplicate_of', ''),
                
                # Cluster ranking and naming
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
                'graph_service': graph_service,
                'mapping_method': alert.get('mapping_method', ''),
                'mapping_confidence': alert.get('mapping_confidence', 0),
                
                # Service graph-level metrics
                'service_pagerank': service_pagerank,
                'service_betweenness': service_betweenness,
                'service_degree': service_degree,
                'service_upstream_count': service_upstream_count,
                'service_downstream_count': service_downstream_count,
                'impact_propagation_score': impact_propagation_score,
                'criticality_score': criticality_score,
                
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
        print(f"\nAlert Flow Summary:")
        print(f"  1. Firing alerts loaded: {len(self.firing_alerts)}")
        print(f"  2. After enrichment: {len(self.enriched_alerts)}")
        
        total_in_groups = sum(len(g['alerts']) for g in self.consolidated_groups)
        print(f"  3. In consolidated groups: {total_in_groups}")
        
        if hasattr(self, 'deduplicated_alerts'):
            print(f"  4. After deduplication: {len(self.deduplicated_alerts)}")
            dedup_rate = ((total_in_groups - len(self.deduplicated_alerts)) / total_in_groups * 100) if total_in_groups > 0 else 0
            print(f"     Deduplication rate: {dedup_rate:.1f}%")
        
        print(f"  5. Final clusters: {len(set(df_output['final_group_id']))}")
        
        # Mapping statistics
        mapped_count = len([a for a in self.enriched_alerts if a.get('graph_service')])
        unmapped_count = len(self.enriched_alerts) - mapped_count
        print(f"\nMapping Statistics:")
        print(f"  Mapped to graph: {mapped_count} ({mapped_count/len(self.enriched_alerts)*100:.1f}%)")
        print(f"  Unmapped: {unmapped_count} ({unmapped_count/len(self.enriched_alerts)*100:.1f}%)")
        
        print(f"\nKey Output Files:")
        print(f"  1. Cluster-Level Summary: cluster_level_summary.csv")
        print(f"     - Main dashboard view with all clusters")
        print(f"  2. Per-Cluster Drill-Down: per_cluster_drill_down.csv")
        print(f"     - Deduplicated alerts with graph + cluster metadata")
        print(f"  3. Individual Cluster Files: clusters/cluster_*.csv")
        print(f"     - Separate file per cluster for easy filtering")
        print(f"  4. Cluster Descriptions: cluster_descriptions.csv")
        print(f"     - Detailed root cause analysis per cluster")
        
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
            
            # Get corresponding consolidated group for metrics
            consolidated_group = None
            for g in self.consolidated_groups:
                if g.get('group_id') == cluster_id:
                    consolidated_group = g
                    break
            
            summary_item = {
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
            }
            
            # Add service relationship metrics if available
            if consolidated_group:
                summary_item['total_upstream_services'] = consolidated_group.get('total_upstream_services', 0)
                summary_item['total_downstream_services'] = consolidated_group.get('total_downstream_services', 0)
                summary_item['total_calls_relationships'] = consolidated_group.get('total_calls_relationships', 0)
                summary_item['total_owns_relationships'] = consolidated_group.get('total_owns_relationships', 0)
                summary_item['total_belongs_to_relationships'] = consolidated_group.get('total_belongs_to_relationships', 0)
                summary_item['impact_propagation_score'] = consolidated_group.get('impact_propagation_score', 0.0)
                summary_item['criticality_score'] = consolidated_group.get('criticality_score', 0.0)
            
            cluster_summary.append(summary_item)
        
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
        
        print(f"\n    Total enriched alerts: {len(self.enriched_alerts)}")
        self._group_alerts_by_relationships()  # Includes deduplication in Phase 4.5
        self._create_consolidated_output()
        self._engineer_features()  # Works on deduplicated alerts only
        self._apply_clustering()  # Graph-based groups + ML for uncertain
        self._rank_and_name_clusters()
        self._export_results()
        
        return self.consolidated_groups


if __name__ == "__main__":
    # Initialize consolidator
    consolidator = ComprehensiveAlertConsolidator(
        alerts_csv_path='C:/Users/jurat.shayidin/aiops/alert_data_classification_beta/gamma/alert_data.csv',  
        graph_json_path='C:/Users/jurat.shayidin/aiops/alert_data_classification_beta/gamma/graph_data.json',
        output_dir='temp'
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
