
import json
import os
import pandas as pd
from typing import Dict, List, Optional, Tuple
from collections import Counter
import numpy as np
from datetime import datetime, timezone
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from config import Config


UNGROUPABLE_CLUSTER_ID = -999
UNGROUPABLE_CLUSTER_NAME = "ungroupable_insufficient_data"


class CatalogManager:
    
    def __init__(self, 
                 cluster_ttl_minutes: int = Config.cluster_ttl_minutes):

        self.catalog_path = Config.CATALOG_PATH
        self.cluster_ttl_minutes = cluster_ttl_minutes
        
        self.cluster_catalog = {}
        self.next_cluster_id = 0
        self.load_catalog()
        self.vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 2))
        self.catalog_vectors = None
        
        self.service_graph = None
        self.service_features_cache = {}
    
    def set_graph_context(self, service_graph, service_features_cache=None):
        self.service_graph = service_graph
        self.service_features_cache = service_features_cache or {}
    
    def _extract_service_names(self, impacted_services: List) -> List[str]:
        """Extract service names from impacted_services list of objects."""
        service_names = []
        for service_obj in impacted_services:
            if isinstance(service_obj, dict):
                service_names.extend(service_obj.keys())
            elif isinstance(service_obj, str):
                service_names.append(service_obj)
        return service_names
    
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
                queue.append((successor, depth + 1))
        
        return impacted
    
    def _normalize_last_seen(self, last_seen) -> float:
        if last_seen is None:
            return 0.0
        if isinstance(last_seen, (int, float)):
            return float(last_seen)
        if isinstance(last_seen, str):
            try:
                return pd.Timestamp(last_seen).timestamp()
            except Exception:
                return 0.0
        try:
            return pd.Timestamp(last_seen).timestamp()
        except Exception:
            return 0.0

    def _compute_cluster_status(self, last_seen) -> str:
        current_time = pd.Timestamp.now().timestamp()
        last_seen_ts = self._normalize_last_seen(last_seen)
        hours_since_seen = (current_time - last_seen_ts) / 3600
        return 'active' if hours_since_seen <= Config.CLUSTER_ACTIVE_HOURS_THRESHOLD else 'inactive'
    
    def _compute_service_metrics(self, services: List[str]) -> Dict:
        metrics = {
            'symptom_service': [],
            'impacted_services': [],
            'root_cause_services': []
        }
        
        if not services:
            return metrics
        
        # Filter out empty strings
        services = [s for s in services if s]
        
        if not services:
            return metrics
        
        # Get the service with the highest alert count
        service_counts = Counter(services)
        symptom_service = service_counts.most_common(1)[0][0] if service_counts else None
        metrics['symptom_service'] = [symptom_service] if symptom_service else []
        
        # Build list of objects with service name, alert_count, and graph metrics
        impacted_services_with_properties = []
        for service_name, alert_count in service_counts.items():
            # Start with alert_count
            service_properties = {
                'alert_count': alert_count
            }
            
            # Add graph-derived metrics from service_features_cache (pagerank, blast radius, etc.)
            if self.service_features_cache and service_name in self.service_features_cache:
                features = self.service_features_cache[service_name]
                # Add key graph metrics
                service_properties['pagerank'] = round(features.get('pagerank', 0), 6)
                service_properties['betweenness'] = round(features.get('betweenness', 0), 6)
                service_properties['blast_radius'] = features.get('num_downstream', 0)
                service_properties['upstream_dependencies'] = features.get('num_upstream', 0)
                service_properties['degree_total'] = features.get('degree_total', 0)
                service_properties['in_degree'] = features.get('in_degree', 0)
                service_properties['out_degree'] = features.get('out_degree', 0)
                service_properties['clustering_coef'] = round(features.get('clustering_coef', 0), 4)
            
            # Add basic node properties from service graph (namespace, cluster, env, etc.)
            if self.service_graph and self.service_graph.has_node(service_name):
                node_data = self.service_graph.nodes[service_name]
                # Add only key identifying properties, not all node attributes
                for key in ['namespace', 'cluster', 'env', 'platform', 'workload_type', 'region']:
                    if key in node_data:
                        service_properties[key] = node_data[key]
            
            # Create object with service name as key and properties as value
            service_obj = {service_name: service_properties}
            impacted_services_with_properties.append(service_obj)
        
        metrics['impacted_services'] = impacted_services_with_properties
        return metrics
    
    def load_catalog(self) -> Dict:
        if not os.path.exists(self.catalog_path):
            print(f"  No existing catalog found at {self.catalog_path}")
            return {}
        
        try:
            with open(self.catalog_path, 'r') as f:
                catalog_data = json.load(f)
            self.cluster_catalog = {k: v for k, v in catalog_data.get('clusters', {}).items()}
            self.next_cluster_id = catalog_data.get('next_cluster_id', 0)
            current_time = pd.Timestamp.now().timestamp()
            expired_clusters = []
            
            for cluster_id, cluster_info in self.cluster_catalog.items():
                last_seen = cluster_info.get('last_seen', 0)
                last_seen_ts = self._normalize_last_seen(last_seen)
                age_minutes = (current_time - last_seen_ts) / 60
                
                cluster_info['status'] = self._compute_cluster_status(last_seen)
                
                if age_minutes > self.cluster_ttl_minutes:
                    expired_clusters.append(cluster_id)
            for cluster_id in expired_clusters:
                del self.cluster_catalog[cluster_id]
            
            active_clusters = len(self.cluster_catalog)
            expired_count = len(expired_clusters)
            
            print(f"  Loaded catalog: {active_clusters} active clusters, {expired_count} expired")
            print(f"  Next cluster ID: {self.next_cluster_id}")
            
            return self.cluster_catalog
            
        except Exception as e:
            print(f"  Error loading catalog: {e}")
            print("  Starting with empty catalog")
            self.cluster_catalog = {}
            self.next_cluster_id = 0
            return {}
    
    def save_catalog(self):
        try:
            os.makedirs(os.path.dirname(self.catalog_path), exist_ok=True)
            
            def clean_value(value):
                """Recursively clean values for JSON serialization"""
                if value is None:
                    return None
                elif isinstance(value, pd.Timestamp):
                    return value.isoformat() if pd.notna(value) else None
                elif isinstance(value, (int, float)):
                    # Keep numeric timestamps as-is
                    return value
                elif isinstance(value, (list, tuple)):
                    return [clean_value(v) for v in value]
                elif isinstance(value, dict):
                    return {k: clean_value(v) for k, v in value.items()}
                else:
                    return value
            
            # Clean up the catalog data before saving
            cleaned_clusters = {}
            for k, v in self.cluster_catalog.items():
                cleaned_cluster = {}
                for field, value in v.items():
                    cleaned_cluster[field] = clean_value(value)
                cleaned_clusters[str(k)] = cleaned_cluster
            
            catalog_data = {
                'clusters': cleaned_clusters,
                'next_cluster_id': len(self.cluster_catalog),
                'last_updated': pd.Timestamp.now().isoformat(),
                'cluster_ttl_minutes': int(self.cluster_ttl_minutes)
            }
            with open(self.catalog_path, 'w') as f:
                json.dump(catalog_data, f, indent=2)
            
            print(f"  Catalog saved: {len(self.cluster_catalog)} clusters")
            
        except Exception as e:
            print(f"  Error saving catalog: {e}")
            import traceback
            traceback.print_exc()
    
    def _build_alert_signature(self, alert: Dict) -> str:
        category = (alert.get('alert_category') or '').lower().strip()
        subcategory = (alert.get('alert_subcategory') or '').lower().strip()
        service = (alert.get('graph_service') or '').lower().strip()
        namespace = (alert.get('namespace') or '').lower().strip()
        
        signature = f"{category} {category} {subcategory} {service} {namespace}"
        return signature.strip()
    
    def _build_catalog_signature(self, cluster_info: Dict) -> str:
        category = cluster_info.get('alert_category', '').lower().strip()
        subcategory = cluster_info.get('alert_subcategory', '').lower().strip()
        
        raw_symptom_service = cluster_info.get('symptom_service', '')
        if isinstance(raw_symptom_service, list):
            symptom_service = ' '.join([s for s in raw_symptom_service if s]).lower().strip()
        else:
            symptom_service = (raw_symptom_service or '').lower().strip()
        impacted_services_list = cluster_info.get('impacted_services', [])
        # Extract service names from list of objects
        service_names = self._extract_service_names(impacted_services_list)
        impacted_services = ' '.join(service_names).lower().strip()
        
        signature = f"{category} {category} {subcategory} {symptom_service} {impacted_services}"
        return signature.strip()
    
    def match_alert_to_catalog_ml(self, alert: Dict, similarity_threshold: float = None) -> Optional[int]:
        """ML-based matching using TF-IDF cosine similarity."""
        if similarity_threshold is None:
            similarity_threshold = Config.ML_SIMILARITY_THRESHOLD
        
        if not self.cluster_catalog:
            return None
        
        alert_signature = self._build_alert_signature(alert)
        if not alert_signature:
            return None
        
        try:
            catalog_signatures = [
                self._build_catalog_signature(cluster_info) 
                for cluster_info in self.cluster_catalog.values()
            ]
            
            if not catalog_signatures:
                return None
            
            all_signatures = [alert_signature] + catalog_signatures
            vectors = self.vectorizer.fit_transform(all_signatures)
            
            alert_vector = vectors[0]
            similarities = cosine_similarity(alert_vector, vectors[1:])[0]
            
            best_idx = np.argmax(similarities)
            best_similarity = similarities[best_idx]
            
            if best_similarity >= similarity_threshold:
                cluster_ids = list(self.cluster_catalog.keys())
                return cluster_ids[best_idx]
            
            return None
            
        except Exception as e:
            print(f"  Error in ML matching: {e}")
            return None
    def match_alert_to_catalog(self, alert: Dict) -> Tuple[Optional[int], Optional[str]]:
        """
        Match an alert to catalog using hybrid approach (exact + ML fallback).
        Returns (cluster_id, match_type) where match_type is 'priority_1', 'priority_2', or 'ml'.
        Default matching method for the class.
        """
        if not self.cluster_catalog:
            return None, None
        alert_category = (alert.get('alert_category') or '').lower().strip()
        alert_subcategory = (alert.get('alert_subcategory') or '').lower().strip()
        alert_service = (alert.get('graph_service') or '').strip()
        # Priority 1: category + subcategory + impacted_service
        if alert_category and alert_subcategory and alert_service:
            for cluster_id, cluster_info in self.cluster_catalog.items():
                catalog_category = cluster_info.get('alert_category', '').lower().strip()
                catalog_subcategory = cluster_info.get('alert_subcategory', '').lower().strip()
                catalog_impacted_services_list = cluster_info.get('impacted_services', [])
                # Extract service names from list of objects
                catalog_service_names = self._extract_service_names(catalog_impacted_services_list)
                if (alert_category == catalog_category and 
                    alert_subcategory == catalog_subcategory and 
                    alert_service in catalog_service_names):
                    return cluster_id, 'priority_1'
        # Priority 2: category + impacted_service only
        if alert_category and alert_service:
            for cluster_id, cluster_info in self.cluster_catalog.items():
                catalog_category = cluster_info.get('alert_category', '').lower().strip()
                catalog_impacted_services_list = cluster_info.get('impacted_services', [])
                # Extract service names from list of objects
                catalog_service_names = self._extract_service_names(catalog_impacted_services_list)
                if (alert_category == catalog_category and 
                    alert_service in catalog_service_names):
                    return cluster_id, 'priority_2'
        # ML fallback
        ml_match = self.match_alert_to_catalog_ml(alert)
        if ml_match is not None:
            return ml_match, 'ml'
        return None, None
    def match_alerts_to_catalog(self, alerts: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Tuple[str, str]]]:
        """
        Match multiple alerts to existing catalog clusters.
        Returns:
            - matched_alerts: List of alerts that matched catalog clusters
            - unmatched_alerts: List of alerts that need clustering
            - match_types: List of tuples (alert_id, match_type) for matched alerts
        """
        print(f"  Matching {len(alerts)} alerts to catalog...")
        matched_alerts = []
        unmatched_alerts = []
        match_types = []
        for alert in alerts:
            catalog_cluster_id, match_type = self.match_alert_to_catalog(alert)
            if catalog_cluster_id is not None:
                # Mark alert with catalog cluster
                alert['catalog_cluster_id'] = catalog_cluster_id
                alert['cluster_id'] = catalog_cluster_id
                alert['clustering_method'] = 'catalog_match'
                cluster_info = self.cluster_catalog[catalog_cluster_id]
                alert['cluster_name'] = cluster_info.get('cluster_name', f'catalog_c{catalog_cluster_id}')
                alert['cluster_rank'] = cluster_info.get('rank', -1)
                matched_alerts.append(alert)
                match_types.append((alert.get('_id', None), match_type))
            else:
                unmatched_alerts.append(alert)
        print(f"    Matched: {len(matched_alerts)} alerts")
        print(f"    Unmatched: {len(unmatched_alerts)} alerts need clustering")
        return matched_alerts, unmatched_alerts, match_types
    
    def update_catalog_with_clusters(self, deduplicated_alerts: List[Dict]):
        print(f"  Updating catalog with new cluster patterns...")
        
        cluster_alerts_dict = {}
        new_clusters_created = []
        for alert in deduplicated_alerts:
            cluster_id = alert.get('cluster_id', -1)
            if cluster_id == -1:
                continue
            
            if cluster_id not in cluster_alerts_dict:
                cluster_alerts_dict[cluster_id] = []
            cluster_alerts_dict[cluster_id].append(alert)
        
        for cluster_id, alerts in cluster_alerts_dict.items():
            if not alerts:
                continue
            
            # Get cluster_name from the alerts in this cluster
            cluster_name = alerts[0].get('cluster_name', '')
            description_prefix = cluster_name.split('-', 1)[0].strip() if cluster_name else ''
            
            # Extract alert_category and alert_subcategory (use most common across alerts)
            categories = [a.get('alert_category', '') for a in alerts if a.get('alert_category')]
            subcategories = [a.get('alert_subcategory', '') for a in alerts if a.get('alert_subcategory')]
            alert_category = Counter(categories).most_common(1)[0][0] if categories else ''
            alert_subcategory = Counter(subcategories).most_common(1)[0][0] if subcategories else ''
            
            # Get services - use graph_service first, fallback to service_name
            services = []
            for a in alerts:
                svc = a.get('graph_service') or a.get('service_name') or ''
                if svc:
                    services.append(svc)
            
            alert_ids = [a.get('_id', '') for a in alerts if a.get('_id')]
            
            service_counts = Counter(services)
            top_service = service_counts.most_common(1)[0][0] if service_counts else None
            
            service_metrics = self._compute_service_metrics(services)
            symptom_service = service_metrics['symptom_service']
            impacted_services = service_metrics['impacted_services']
            
            # Calculate min/max timestamps from alerts - normalize to pandas Timestamp for comparison
            starts_at_timestamps = []
            for a in alerts:
                ts = a.get('starts_at')
                if ts:
                    try:
                        starts_at_timestamps.append(pd.Timestamp(ts))
                    except:
                        pass
            
            start_ts = min(starts_at_timestamps).isoformat() if starts_at_timestamps else None
            end_ts = ""
            
            if cluster_id in self.cluster_catalog:
                self.cluster_catalog[cluster_id]['status'] = 'active'
                self.cluster_catalog[cluster_id]['total_alert_count'] += len(alerts)
                self.cluster_catalog[cluster_id]['cluster_name'] = cluster_name
                self.cluster_catalog[cluster_id]['last_seen'] = pd.Timestamp.now(tz=timezone.utc).isoformat()
                
                # Update timestamps - keep earliest start and latest end
                if start_ts:
                    existing_start = self.cluster_catalog[cluster_id].get('start_ts')
                    if not existing_start:
                        self.cluster_catalog[cluster_id]['start_ts'] = start_ts
                    else:
                        # Compare as Timestamps to handle different formats
                        try:
                            if pd.Timestamp(start_ts) < pd.Timestamp(existing_start):
                                self.cluster_catalog[cluster_id]['start_ts'] = start_ts
                        except:
                            # If comparison fails, keep the new one
                            self.cluster_catalog[cluster_id]['start_ts'] = start_ts
                
                self.cluster_catalog[cluster_id]['end_ts'] = end_ts
            
                existing_alert_ids = set(self.cluster_catalog[cluster_id].get('alert_ids', []))
                
                # Update symptom_service (list of strings)
                self.cluster_catalog[cluster_id]['symptom_service'] = symptom_service
                
                # Merge impacted_services: combine objects by service name, summing alert_counts
                existing_impacted_list = self.cluster_catalog[cluster_id].get('impacted_services', [])
                existing_service_map = {}
                for item in existing_impacted_list:
                    if isinstance(item, dict):
                        for svc_name, svc_props in item.items():
                            existing_service_map[svc_name] = svc_props if isinstance(svc_props, dict) else {}
                
                # Merge new impacted services, adding up alert_counts
                for item in impacted_services:
                    if isinstance(item, dict):
                        for svc_name, svc_props in item.items():
                            new_props = svc_props if isinstance(svc_props, dict) else {}
                            if svc_name in existing_service_map:
                                # Sum up alert_counts
                                existing_count = existing_service_map[svc_name].get('alert_count', 0)
                                new_count = new_props.get('alert_count', 0)
                                existing_service_map[svc_name]['alert_count'] = existing_count + new_count
                                # Merge other properties (keep existing, add new ones)
                                for k, v in new_props.items():
                                    if k != 'alert_count' and k not in existing_service_map[svc_name]:
                                        existing_service_map[svc_name][k] = v
                            else:
                                existing_service_map[svc_name] = new_props
                
                # Convert back to list of objects
                merged_impacted = [{k: v} for k, v in list(existing_service_map.items())]
                self.cluster_catalog[cluster_id]['impacted_services'] = merged_impacted
                self.cluster_catalog[cluster_id]['impacted_services_count'] = len(merged_impacted)
                
                self.cluster_catalog[cluster_id]['alert_ids'] = list(existing_alert_ids | set(alert_ids))
            else:
                alert_ids_list = list(set(alert_ids))
                current_timestamp = pd.Timestamp.now(tz=timezone.utc).isoformat()
                self.cluster_catalog[cluster_id] = {
                    'cluster_id': cluster_id,
                    'cluster_name': cluster_name,
                    'alert_category': alert_category,
                    'alert_subcategory': alert_subcategory,
                    'description': (
                        f"Observing {description_prefix} issues pertaining to "
                        f"{top_service} and its dependencies"
                    ),
                    'created_ts': datetime.now(tz=timezone.utc).isoformat(),
                    'start_ts': start_ts,
                    'end_ts': end_ts,
                    'last_seen': current_timestamp,
                    'impacted_services': impacted_services,
                    'impacted_services_count': len(impacted_services),
                    'alert_ids': alert_ids_list,
                    'symptom_service': symptom_service,
                    'total_alert_count': len(alert_ids_list),
                    'target_metric': '',
                    'top_drivers': [],
                    'ranked_root_causes': [],
                    'confirmed_root_cause': '',
                    'status': 'active',
                    'status_mode': 'auto | manual | snow',
                    'snow_inc': '',
                    'snow_status': ''
                }
                new_clusters_created.append(cluster_id)
                self.next_cluster_id = len(self.cluster_catalog)
        print(f"  New clusters created in this run: {new_clusters_created}")
        return new_clusters_created
    
    def get_next_cluster_id(self) -> int:
        cluster_id = self.next_cluster_id
        self.next_cluster_id += 1
        return cluster_id
    #newly added method to compute similarity between clusters for merging
    def _compute_cluster_similarity(self, cluster_id_a: str, cluster_id_b: str) -> float:
        """Compute similarity between two catalog clusters using signature comparison."""
        if cluster_id_a not in self.cluster_catalog or cluster_id_b not in self.cluster_catalog:
            return 0.0
        
        sig_a = self._build_catalog_signature(self.cluster_catalog[cluster_id_a])
        sig_b = self._build_catalog_signature(self.cluster_catalog[cluster_id_b])
        
        if not sig_a or not sig_b:
            return 0.0
        
        try:
            vectors = self.vectorizer.fit_transform([sig_a, sig_b])
            similarity = cosine_similarity(vectors[0], vectors[1])[0][0]
            return similarity
        except Exception as e:
            print(f"  Error computing cluster similarity: {e}")
            return 0.0
    
    def _merge_clusters(self, target_cluster_id: str, source_cluster_id: str):
        """Merge source cluster into target cluster and remove source."""
        if target_cluster_id not in self.cluster_catalog or source_cluster_id not in self.cluster_catalog:
            return
        
        target = self.cluster_catalog[target_cluster_id]
        source = self.cluster_catalog[source_cluster_id]
        
        # Merge alert counts
        target['total_alert_count'] = target.get('total_alert_count', 0) + source.get('total_alert_count', 0)
        
        # Merge alert_ids
        target_ids = set(target.get('alert_ids', []))
        source_ids = set(source.get('alert_ids', []))
        target['alert_ids'] = list(target_ids | source_ids)
        
        # Merge impacted_services
        existing_service_map = {}
        for item in target.get('impacted_services', []):
            if isinstance(item, dict):
                for svc_name, svc_props in item.items():
                    existing_service_map[svc_name] = svc_props if isinstance(svc_props, dict) else {}
        
        for item in source.get('impacted_services', []):
            if isinstance(item, dict):
                for svc_name, svc_props in item.items():
                    new_props = svc_props if isinstance(svc_props, dict) else {}
                    if svc_name in existing_service_map:
                        existing_count = existing_service_map[svc_name].get('alert_count', 0)
                        new_count = new_props.get('alert_count', 0)
                        existing_service_map[svc_name]['alert_count'] = existing_count + new_count
                    else:
                        existing_service_map[svc_name] = new_props
        
        target['impacted_services'] = [{k: v} for k, v in existing_service_map.items()]
        target['impacted_services_count'] = len(target['impacted_services'])
        
        # Merge symptom_service
        target_symptom = target.get('symptom_service', [])
        source_symptom = source.get('symptom_service', [])
        if isinstance(target_symptom, list) and isinstance(source_symptom, list):
            all_symptoms = list(set(target_symptom + source_symptom))
            target['symptom_service'] = all_symptoms[:5]  # Keep top 5
        
        # Use earlier start_ts
        if source.get('start_ts') and target.get('start_ts'):
            try:
                if pd.Timestamp(source['start_ts']) < pd.Timestamp(target['start_ts']):
                    target['start_ts'] = source['start_ts']
            except:
                pass
        elif source.get('start_ts') and not target.get('start_ts'):
            target['start_ts'] = source['start_ts']
        
        # Update last_seen to latest
        try:
            source_last = pd.Timestamp(source.get('last_seen', 0))
            target_last = pd.Timestamp(target.get('last_seen', 0))
            if source_last > target_last:
                target['last_seen'] = source['last_seen']
        except:
            pass
        
        # Remove source cluster
        del self.cluster_catalog[source_cluster_id]
        print(f"    Merged cluster {source_cluster_id} into {target_cluster_id}")
    
    def merge_similar_clusters(self, threshold: float = None) -> int:
        """
        Find and merge clusters that are very similar to each other.
        Returns number of merges performed.
        """
        if threshold is None:
            threshold = Config.CLUSTER_MERGE_THRESHOLD
        
        if len(self.cluster_catalog) < 2:
            return 0
        
        cluster_ids = list(self.cluster_catalog.keys())
        merges_done = 0
        merged_ids = set()
        
        # Build similarity matrix
        pairs_to_merge = []
        for i, id_a in enumerate(cluster_ids):
            if id_a in merged_ids:
                continue
            for id_b in cluster_ids[i+1:]:
                if id_b in merged_ids:
                    continue
                similarity = self._compute_cluster_similarity(id_a, id_b)
                if similarity >= threshold:
                    pairs_to_merge.append((similarity, id_a, id_b))
        
        # Sort by similarity (highest first) and merge
        pairs_to_merge.sort(reverse=True, key=lambda x: x[0])
        
        for similarity, id_a, id_b in pairs_to_merge:
            if id_a in merged_ids or id_b in merged_ids:
                continue
            if id_a not in self.cluster_catalog or id_b not in self.cluster_catalog:
                continue
            
            # Merge smaller into larger (by alert count)
            count_a = self.cluster_catalog[id_a].get('total_alert_count', 0)
            count_b = self.cluster_catalog[id_b].get('total_alert_count', 0)
            
            if count_a >= count_b:
                self._merge_clusters(id_a, id_b)
                merged_ids.add(id_b)
            else:
                self._merge_clusters(id_b, id_a)
                merged_ids.add(id_a)
            
            merges_done += 1
        
        if merges_done > 0:
            print(f"  Merged {merges_done} similar cluster pairs (threshold: {threshold})")
        
        return merges_done
    
    def enforce_cluster_limit(self, max_clusters: int = None) -> int:
        """
        Enforce maximum cluster limit by merging smallest clusters.
        Returns number of clusters removed.
        """
        if max_clusters is None:
            max_clusters = Config.CATALOG_MAX_CLUSTERS
        
        if len(self.cluster_catalog) <= max_clusters:
            return 0
        
        print(f"  Catalog has {len(self.cluster_catalog)} clusters, limit is {max_clusters}. Merging...")
        
        initial_count = len(self.cluster_catalog)
        
        # First try merging similar clusters
        self.merge_similar_clusters()
        
        # If still over limit, merge smallest clusters into their closest match
        while len(self.cluster_catalog) > max_clusters:
            # Find smallest cluster (by total_alert_count)
            cluster_ids = list(self.cluster_catalog.keys())
            if len(cluster_ids) < 2:
                break
            
            smallest_id = min(
                cluster_ids,
                key=lambda cid: self.cluster_catalog[cid].get('total_alert_count', 0)
            )
            
            # Find best match for smallest cluster
            best_match_id = None
            best_similarity = 0.0
            
            for other_id in cluster_ids:
                if other_id == smallest_id:
                    continue
                similarity = self._compute_cluster_similarity(smallest_id, other_id)
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match_id = other_id
            
            if best_match_id and best_similarity > 0.1:
                self._merge_clusters(best_match_id, smallest_id)
            else:
                # No good match - just delete smallest (very rare case)
                print(f"    Removing orphan cluster {smallest_id} (no similar match)")
                del self.cluster_catalog[smallest_id]
        
        removed = initial_count - len(self.cluster_catalog)
        if removed > 0:
            print(f"  Reduced catalog from {initial_count} to {len(self.cluster_catalog)} clusters")
        
        return removed
    #==========================================
    def get_catalog_stats(self) -> Dict:
        if not self.cluster_catalog:
            return {
                'total_clusters': 0,
                'total_alerts_seen': 0,
                'categories': {},
                'avg_cluster_size': 0
            }
        
        categories = [c.get('alert_category', '') for c in self.cluster_catalog.values()]
        total_alerts = sum(c.get('total_count', 0) for c in self.cluster_catalog.values())
        
        return {
            'total_clusters': len(self.cluster_catalog),
            'total_alerts_seen': total_alerts,
            'categories': dict(Counter(categories)),
            'avg_cluster_size': total_alerts / len(self.cluster_catalog) if self.cluster_catalog else 0
        }
    
    def clear_catalog(self):
        self.cluster_catalog = {}
        self.next_cluster_id = 0
    
    def export_catalog_summary(self, output_path: str):
        if not self.cluster_catalog:
            print("  No catalog data to export")
            return
        
        catalog_data = []
        for cluster_id, cluster_info in self.cluster_catalog.items():
            issue_data = cluster_info.get('issue', {})
            if issue_data:
                issue_id = issue_data.get('issue_id', '')
                child_alert_ids = issue_data.get('child_alert_ids', [])
            else:
                issue_id = cluster_info.get('issue_id', '')
                child_alert_ids = cluster_info.get('child_alert_ids', []) or cluster_info.get('_ids', [])
            
            catalog_data.append({
                'cluster_id': cluster_id,
                'issue_id': issue_id,
                'status': cluster_info.get('status', 'inactive'),
                'cluster_name': cluster_info.get('cluster_name', ''),
                'symptom_service': ', '.join(cluster_info.get('symptom_service', []))
                if isinstance(cluster_info.get('symptom_service', []), list)
                else cluster_info.get('symptom_service', ''),
                'impacted_services': ', '.join(self._extract_service_names(cluster_info.get('impacted_services', []))),
                'root_cause_services': ', '.join(cluster_info.get('root_cause_services', [])),
                'child_alert_ids': ', '.join(child_alert_ids),
                'child_alert_count': len(child_alert_ids),
                'total_count': cluster_info.get('total_count', 0),
                'last_seen': pd.to_datetime(cluster_info.get('last_seen', 0), unit='s'),
                'rank': cluster_info.get('rank', -1)
            })
        
        df = pd.DataFrame(catalog_data)
        df = df.sort_values('total_count', ascending=False)
        df.to_csv(output_path, index=False)
