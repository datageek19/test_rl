
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
    
    def __init__(self):

        self.catalog_path = Config.CATALOG_PATH
        self.cluster_catalog = {}
        self.next_cluster_id = 0
        self.load_catalog()
        self.vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 2))
        self.catalog_vectors = None
        
        self.service_graph = None
    # ++++++++++++++++++++++++++++++++++
    def set_graph_context(self, service_graph):
        self.service_graph = service_graph
    # ++++++++++++++++++++++++++++++++++
    def _extract_service_names(self, impacted_services: List) -> List[str]:
        """Extract service names from impacted_services list of objects."""
        service_names = []
        for service_obj in impacted_services:
            if isinstance(service_obj, dict):
                service_names.extend(service_obj.keys())
            elif isinstance(service_obj, str):
                service_names.append(service_obj)
        return service_names
    # +++++++++++++++++++++++++++++++++++
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
    # ++++++++++++++++++++++++++++++++++
    def refresh_cluster_statuses(
        self,
        active_hours_threshold: int = Config.CLUSTER_ACTIVE_HOURS_THRESHOLD,
    ) -> None:
        """Recompute cluster status based on last_seen within given hours threshold."""
        if not self.cluster_catalog:
            return
        current_time = pd.Timestamp.now().timestamp()
        inactive_cluster_ids = []
        for cluster_info in self.cluster_catalog.values():
            last_seen = cluster_info.get('last_seen', 0)
            last_seen_ts = self._normalize_last_seen(last_seen)
            hours_since_seen = (current_time - last_seen_ts) / 3600
            cluster_info['status'] = (
                'active' if hours_since_seen <= active_hours_threshold else 'inactive'
            )
        for cluster_id, cluster_info in self.cluster_catalog.items():
            if cluster_info.get('status') == 'inactive':
                inactive_cluster_ids.append(cluster_id)

        if inactive_cluster_ids:
            inactive_clusters = {
                cluster_id: self.cluster_catalog[cluster_id]
                for cluster_id in inactive_cluster_ids
            }
            self._export_clusters_snapshot(
                clusters=inactive_clusters,
                output_dir=Config.FUDGED_CLUSTER_DIR,
                filename_prefix="inactive_clusters",
            )
            for cluster_id in inactive_cluster_ids:
                del self.cluster_catalog[cluster_id]
    # ++++++++++++++++++++++++++++++++++
    def _export_clusters_snapshot(self, clusters: Dict, output_dir, filename_prefix: str) -> None:
        """Export a snapshot of clusters to a JSON file in the target directory."""
        if not clusters:
            return
        output_dir = os.fspath(output_dir)
        os.makedirs(output_dir, exist_ok=True)

        def clean_value(value):
            if value is None:
                return None
            if isinstance(value, pd.Timestamp):
                return value.isoformat() if pd.notna(value) else None
            if isinstance(value, (int, float)):
                return value
            if isinstance(value, (list, tuple)):
                return [clean_value(v) for v in value]
            if isinstance(value, dict):
                return {k: clean_value(v) for k, v in value.items()}
            return value

        cleaned_clusters = {str(k): clean_value(v) for k, v in clusters.items()}
        snapshot = {
            'clusters': cleaned_clusters,
            'count': len(cleaned_clusters),
            'status': filename_prefix,
            'exported_at': pd.Timestamp.now(tz=timezone.utc).isoformat(),
        }
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        output_path = os.path.join(output_dir, f"{filename_prefix}_{timestamp}.json")
        with open(output_path, 'w') as f:
            json.dump(snapshot, f, indent=2)
    # +++++++++++++++++++++++++++++++++
    def _find_downstream_services(self, service_name: str, max_depth: int = 2) -> set:
        """Find downstream services that would be impacted by issues in service_name."""
        impacted = set()
        
        if not self.service_graph or not self.service_graph.has_node(service_name):
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
    # +++++++++++++++++++++++++++++++++
    def _compute_service_metrics(self, services: List[str]) -> Dict:
        metrics = {
            'symptom_service': [],
            'impacted_services': []
        }
        
        if not services:
            return metrics
        
        # Get the service with the highest alert count
        service_counts = Counter(services)
        symptom_service = service_counts.most_common(1)[0][0] if service_counts else None
        metrics['symptom_service'] = [symptom_service] if symptom_service else []
        
        unique_services = list(set(s for s in services if s))
        
        # Build list of objects with service name and properties from service graph
        impacted_services_with_properties = []
        for service_name in unique_services:
            service_properties = {}
            
            # Get all properties from service graph if available
            if self.service_graph and self.service_graph.has_node(service_name):
                # In NetworkX, when nodes are added with **props, the properties are stored
                # as direct node attributes, not in a nested 'properties' dict
                node_data = self.service_graph.nodes[service_name]
                # Copy all node attributes as properties
                service_properties = dict(node_data)
            
            # Add alert count for this service
            service_properties['alert_count'] = service_counts.get(service_name, 0)

            # Create object with service name as key and properties as value
            service_obj = {service_name: service_properties}
            impacted_services_with_properties.append(service_obj)
        
        metrics['impacted_services'] = impacted_services_with_properties
        return metrics
    # +++++++++++++++++++++++++++++++++
    def load_catalog(self) -> Dict:
        if not os.path.exists(self.catalog_path):
            print(f"  No existing catalog found at {self.catalog_path}")
            return {}
        
        try:
            with open(self.catalog_path, 'r') as f:
                catalog_data = json.load(f)
            self.cluster_catalog = {k: v for k, v in catalog_data.get('clusters', {}).items()}
            self.next_cluster_id = catalog_data.get('next_cluster_id', 0)
            active_clusters = len(self.cluster_catalog)
            print(f"  Loaded catalog: {active_clusters} active clusters")
            print(f"  Next cluster ID: {self.next_cluster_id}")
            
            return self.cluster_catalog
            
        except Exception as e:
            print(f"  Error loading catalog: {e}")
            print("  Starting with empty catalog")
            self.cluster_catalog = {}
            self.next_cluster_id = 0
            return {}
    # +++++++++++++++++++++++++++++++++
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
            fields_to_remove = {
                'target_metric',
                'top_drivers',
                'snow_inc',
                'snow_status'
            }
            cleaned_clusters = {}
            for k, v in self.cluster_catalog.items():
                cleaned_cluster = {}
                for field, value in v.items():
                    if field in fields_to_remove:
                        continue
                    cleaned_cluster[field] = clean_value(value)
                cleaned_clusters[str(k)] = cleaned_cluster
            
            catalog_data = {
                'clusters': cleaned_clusters,
                'next_cluster_id': len(self.cluster_catalog),
                'last_updated': pd.Timestamp.now().isoformat()
            }
            with open(self.catalog_path, 'w') as f:
                json.dump(catalog_data, f, indent=2)
            
            print(f"  Catalog saved: {len(self.cluster_catalog)} clusters")
            
        except Exception as e:
            print(f"  Error saving catalog: {e}")
            import traceback
            traceback.print_exc()
    # +++++++++++++++++++++++++++++++++
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
    # +++++++++++++++++++++++++++++++++
    def _fuzzy_match_using_ml(
        self,
        alert: Dict,
        similarity_threshold: float = Config.CATALOG_ML_SIMILARITY_THRESHOLD,
    ) -> Optional[int]:
        """Internal helper: Use TF-IDF + cosine similarity for fuzzy matching."""
        if not self.cluster_catalog:
            return None
        if not (alert.get('graph_service') or '').strip():
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
                matched_cluster_id = cluster_ids[best_idx]
                cluster_info = self.cluster_catalog.get(matched_cluster_id, {})
                catalog_impacted_services_list = cluster_info.get('impacted_services', [])
                catalog_service_names = self._extract_service_names(catalog_impacted_services_list)
                alert_service = (alert.get('graph_service') or '').strip()
                if alert_service in catalog_service_names:
                    return matched_cluster_id
                return None
            return None
        except Exception as e:
            print(f"  Error in ML matching: {e}")
            return None
    
    def match_alert_to_catalog(self, alert: Dict) -> Tuple[Optional[int], Optional[str]]:
        """
        Match a single alert to existing catalog clusters.
        
        Strategy (in priority order):
        1. Exact match: category + subcategory + service
        2. Exact match: category + service  
        3. Fuzzy match: ML-based similarity (TF-IDF + cosine)
        
        Returns:
            (cluster_id, match_type) where match_type is 'priority_1', 'priority_2', or 'ml'
            Returns (None, None) if no match found
        """
        if not self.cluster_catalog:
            return None, None
        alert_category = (alert.get('alert_category') or '').lower().strip()
        alert_subcategory = (alert.get('alert_subcategory') or '').lower().strip()
        alert_service = (alert.get('graph_service') or '').strip()
        
        # Strategy 1: Most specific - exact match on category + subcategory + service
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
        
        # Strategy 2: Less specific - exact match on category + service
        if alert_category and alert_service:
            for cluster_id, cluster_info in self.cluster_catalog.items():
                catalog_category = cluster_info.get('alert_category', '').lower().strip()
                catalog_impacted_services_list = cluster_info.get('impacted_services', [])
                # Extract service names from list of objects
                catalog_service_names = self._extract_service_names(catalog_impacted_services_list)
                if (alert_category == catalog_category and 
                    alert_service in catalog_service_names):
                    return cluster_id, 'priority_2'
        
        # Strategy 3: Fuzzy matching using ML when exact matches fail
        ml_match = self._fuzzy_match_using_ml(
            alert,
            similarity_threshold=Config.CATALOG_ML_SIMILARITY_THRESHOLD,
        )
        if ml_match is not None:
            return ml_match, 'ml'
        return None, None
    def match_alerts_to_catalog(self, alerts: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Tuple[str, str]]]:
        """
        Batch processor: Match multiple alerts to existing catalog clusters.
        Calls match_alert_to_catalog() for each alert.
        
        Returns:
            - matched_alerts: Alerts that found a catalog match
            - unmatched_alerts: Alerts that need new clustering
            - match_types: List of (alert_id, match_type) tuples
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
    # ++++++++++++++++++++++++++++++++++
    def update_catalog_with_clusters(self, deduplicated_alerts: List[Dict]) -> Tuple[List[int], List[str]]:
        print("  Updating catalog with new cluster patterns...")

        def _normalize_alert_ids_by_runtime(value) -> List[Dict[str, List[str]]]:
            """Normalize alert_ids to list of {runtime: [ids]} objects."""
            if not value:
                return []
            if isinstance(value, list) and all(isinstance(v, dict) for v in value):
                normalized = []
                for entry in value:
                    if not entry:
                        continue
                    runtime, ids = next(iter(entry.items()))
                    if isinstance(ids, list):
                        normalized.append({runtime: ids})
                    else:
                        normalized.append({runtime: [ids]})
                return normalized
            if isinstance(value, list):
                return [{"unknown": [v for v in value if v]}]
            if isinstance(value, dict):
                return [{k: v if isinstance(v, list) else [v]} for k, v in value.items()]
            return [{"unknown": [value]}]
        
        cluster_alerts_dict = {}
        new_clusters_created = []
        new_cluster_alert_ids = []
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
            
            # Get services - try graph_service first, fall back to service_name
            services = []
            for a in alerts:
                svc = a.get('graph_service') or a.get('service_name') or ''
                if svc:
                    services.append(svc)
            
            alert_ids = [a.get('_id', '') for a in alerts if a.get('_id')]
            categories = [a.get('alert_category', '') for a in alerts if a.get('alert_category')]
            subcategories = [a.get('alert_subcategory', '') for a in alerts if a.get('alert_subcategory')]
            top_category = Counter(categories).most_common(1)[0][0] if categories else ''
            top_subcategory = Counter(subcategories).most_common(1)[0][0] if subcategories else ''
            
            service_counts = Counter(services)
            top_service = service_counts.most_common(1)[0][0] if service_counts else None
            
            service_metrics = self._compute_service_metrics(services)
            symptom_service = service_metrics['symptom_service']
            
            # Compute impacted_services: find downstream services from all services in this cluster
            # First try to get from alerts (propagated from group_alerts_by_relationships)
            all_impacted_services = set()
            for a in alerts:
                alert_impacted = a.get('impacted_services', [])
                if alert_impacted:
                    all_impacted_services.update(alert_impacted)
            
            # If no impacted_services from alerts, compute directly from service graph
            if not all_impacted_services and self.service_graph:
                for service_name in set(services):
                    if service_name and self.service_graph.has_node(service_name):
                        # Find downstream (impacted) services up to depth 2
                        downstream = self._find_downstream_services(service_name, max_depth=2)
                        all_impacted_services.update(downstream)
            
            # Build impacted_services with properties from service graph
            impacted_services = []
            for service_name in all_impacted_services:
                if not service_name:
                    continue
                service_properties = {}
                if self.service_graph and self.service_graph.has_node(service_name):
                    node_data = self.service_graph.nodes[service_name]
                    service_properties = dict(node_data)
                service_obj = {service_name: service_properties}
                impacted_services.append(service_obj)
            
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
                current_timestamp = pd.Timestamp.now(tz=timezone.utc).isoformat()
                self.cluster_catalog[cluster_id]['last_seen'] = current_timestamp
                self.cluster_catalog[cluster_id]['alert_category'] = top_category
                self.cluster_catalog[cluster_id]['alert_subcategory'] = top_subcategory
                
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
            
                existing_alert_ids_by_runtime = _normalize_alert_ids_by_runtime(
                    self.cluster_catalog[cluster_id].get('alert_ids', [])
                )
                
                # Update symptom_service (list of strings)
                self.cluster_catalog[cluster_id]['symptom_service'] = symptom_service
                
                # Merge impacted_services: combine objects by service name
                existing_impacted_list = self.cluster_catalog[cluster_id].get('impacted_services', [])
                existing_service_map = {}
                for item in existing_impacted_list:
                    if isinstance(item, dict):
                        existing_service_map.update(item)
                
                # Add new impacted services
                for item in impacted_services:
                    if isinstance(item, dict):
                        for svc_name, svc_props in item.items():
                            if svc_name in existing_service_map:
                                existing_props = existing_service_map[svc_name]
                                existing_count = existing_props.get('alert_count', 0)
                                new_count = svc_props.get('alert_count', 0)
                                existing_props['alert_count'] = existing_count + new_count
                                existing_service_map[svc_name] = existing_props
                            else:
                                existing_service_map[svc_name] = svc_props
                
                # Convert back to list of objects
                merged_impacted = [{k: v} for k, v in list(existing_service_map.items())]
                self.cluster_catalog[cluster_id]['impacted_services'] = merged_impacted
                self.cluster_catalog[cluster_id]['impacted_services_count'] = len(merged_impacted)
                
                current_run_entry = {current_timestamp: list(set(alert_ids))}
                self.cluster_catalog[cluster_id]['alert_ids'] = (
                    existing_alert_ids_by_runtime + [current_run_entry]
                )
            else:
                current_timestamp = pd.Timestamp.now(tz=timezone.utc).isoformat()
                alert_ids_list = list(set(alert_ids))
                self.cluster_catalog[cluster_id] = {
                    'cluster_id': cluster_id,
                    'cluster_name': cluster_name,
                    'description': (
                        f"Observing {description_prefix} issues pertaining to "
                        f"{top_service} and its dependencies"
                    ),
                    'created_ts': datetime.now(tz=timezone.utc).isoformat(),
                    'start_ts': start_ts,
                    'end_ts': end_ts,
                    'last_seen': current_timestamp,
                    'alert_category': top_category,
                    'alert_subcategory': top_subcategory,
                    'impacted_services': impacted_services,
                    'impacted_services_count': len(impacted_services),
                    'alert_ids': [{current_timestamp: alert_ids_list}],
                    'symptom_service': symptom_service,
                    'total_alert_count': len(alert_ids_list),
                    'status': 'active',
                    'status_mode': 'auto',
                    
                }
                new_clusters_created.append(cluster_id)
                new_cluster_alert_ids.extend(alert_ids_list)
                self.next_cluster_id = len(self.cluster_catalog)
        print(f"  New clusters created in this run: {new_clusters_created}")
        return new_clusters_created, new_cluster_alert_ids
    
