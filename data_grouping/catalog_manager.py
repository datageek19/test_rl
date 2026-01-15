"""
Cluster Catalog Manager: Manages persistent cluster catalog for incremental processing. Tracks cluster patterns across workflow runs to maintain consistency.
"""

import json
import os
import pandas as pd
from typing import Dict, List, Optional, Tuple
from collections import Counter
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# Special cluster ID for alerts that cannot be properly grouped
UNGROUPABLE_CLUSTER_ID = -999
UNGROUPABLE_CLUSTER_NAME = "ungroupable_insufficient_data"


class CatalogManager:
    """
    Manages cluster catalog for incremental alert processing:
    1. Load existing cluster catalog from previous runs
    2. Match new alerts to existing catalog clusters
    3. Update catalog with new cluster patterns
    4. Handle catalog TTL (time-to-live) for stale clusters
    5. Handle ungroupable alerts (insufficient data to cluster)
    """
    
    def __init__(self, catalog_path: str = 'data/models/cluster_catalog.json', 
                 cluster_ttl_minutes: int = 60):
        """
        Initialize catalog manager
        
        Args:
            catalog_path: Path to cluster catalog JSON file
            cluster_ttl_minutes: TTL for cluster patterns (minutes)
        """
        self.catalog_path = catalog_path
        self.cluster_ttl_minutes = cluster_ttl_minutes
        
        # Cluster catalog structure:
        # {
        #   cluster_id: {
        #     'cluster_id': int,
        #     'alert_category': str,
        #     'alert_subcategory': str,
        #     'services': [str],
        #     'namespaces': [str],
        #     '_ids': [str],
        #     'last_seen': timestamp,
        #     'total_count': int,
        #     'cluster_name': str
        #   }
        # }
        self.cluster_catalog = {}
        self.next_cluster_id = 0
        self.load_catalog()
        self.vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 2))
        self.catalog_vectors = None
    
    def load_catalog(self) -> Dict:
        """Load existing cluster catalog from previous runs"""
        if not os.path.exists(self.catalog_path):
            print(f"  No existing catalog found at {self.catalog_path}")
            return {}
        
        try:
            with open(self.catalog_path, 'r') as f:
                catalog_data = json.load(f)
            # Support both string and integer cluster IDs
            self.cluster_catalog = {k: v for k, v in catalog_data.get('clusters', {}).items()}
            self.next_cluster_id = catalog_data.get('next_cluster_id', 0)
            current_time = pd.Timestamp.now().timestamp()
            expired_clusters = []
            
            for cluster_id, cluster_info in self.cluster_catalog.items():
                last_seen = cluster_info.get('last_seen', 0)
                age_minutes = (current_time - last_seen) / 60
                
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
        """Save cluster catalog for future runs"""
        try:
            os.makedirs(os.path.dirname(self.catalog_path), exist_ok=True)
            catalog_data = {
                'clusters': {str(k): v for k, v in self.cluster_catalog.items()},
                'next_cluster_id': len(self.cluster_catalog),
                'last_updated': pd.Timestamp.now().isoformat(),
                'cluster_ttl_minutes': int(self.cluster_ttl_minutes)
            }
            with open(self.catalog_path, 'w') as f:
                json.dump(catalog_data, f, indent=2)
            
            print(f"  Catalog saved: {len(self.cluster_catalog)} clusters")
            
        except Exception as e:
            print(f"  Error saving catalog: {e}")
    
    def _build_alert_signature(self, alert: Dict) -> str:
        """
        Build a signature string from alert attributes for ML matching.
        Combines category, subcategory, service, and namespace into a single string.
        """
        category = (alert.get('alert_category') or '').lower().strip()
        subcategory = (alert.get('alert_subcategory') or '').lower().strip()
        service = (alert.get('graph_service') or '').lower().strip()
        namespace = (alert.get('namespace') or '').lower().strip()
        
        # Create signature with weights - category is most important
        signature = f"{category} {category} {subcategory} {service} {namespace}"
        return signature.strip()
    
    def _build_catalog_signature(self, cluster_info: Dict) -> str:
        """
        Build a signature string from catalog cluster attributes for ML matching.
        """
        category = cluster_info.get('alert_category', '').lower().strip()
        subcategory = cluster_info.get('alert_subcategory', '').lower().strip()
        
        # Services and namespaces as space-separated tokens
        services = ' '.join(cluster_info.get('services', [])).lower().strip()
        namespaces = ' '.join(cluster_info.get('namespaces', [])).lower().strip()
        
        signature = f"{category} {category} {subcategory} {services} {namespaces}"
        return signature.strip()
    
    def match_alert_to_catalog_ml(self, alert: Dict, similarity_threshold: float = 0.5) -> Optional[int]:
        """
        Match an alert to a catalog cluster using ML similarity (TF-IDF + Cosine Similarity).
        
        Args:
            alert: Alert to match
            similarity_threshold: Minimum similarity score (0.0-1.0) to consider as match
        
        Returns:
            cluster_id if match found, None otherwise
        """
        if not self.cluster_catalog:
            return None
        
        alert_signature = self._build_alert_signature(alert)
        if not alert_signature:
            return None
        
        try:
            # Build signatures for all catalog clusters
            catalog_signatures = [
                self._build_catalog_signature(cluster_info) 
                for cluster_info in self.cluster_catalog.values()
            ]
            
            if not catalog_signatures:
                return None
            
            # Vectorize alert and catalog signatures using TF-IDF
            all_signatures = [alert_signature] + catalog_signatures
            vectors = self.vectorizer.fit_transform(all_signatures)
            
            # Calculate similarity between alert and each catalog cluster
            alert_vector = vectors[0]
            similarities = cosine_similarity(alert_vector, vectors[1:])[0]
            
            # Find best match
            best_idx = np.argmax(similarities)
            best_similarity = similarities[best_idx]
            
            if best_similarity >= similarity_threshold:
                # Get the cluster_id from the best matching index
                cluster_ids = list(self.cluster_catalog.keys())
                return cluster_ids[best_idx]
            
            return None
            
        except Exception as e:
            print(f"  Error in ML matching: {e}")
            return None
    
    def match_alert_to_catalog_hybrid(self, alert: Dict, ml_threshold: float = 0.5) -> Optional[int]:
        """
        Hybrid matching: Try exact matching first, fall back to ML similarity.
        
        This combines the best of both approaches:
        - Exact matching for clear, deterministic cases
        - ML matching for fuzzy/similar cases
        
        Args:
            alert: Alert to match
            ml_threshold: Minimum ML similarity threshold (0.0-1.0)
        
        Returns:
            cluster_id if match found, None otherwise
        """
        if not self.cluster_catalog:
            return None
        
        # First try exact matching (from original method)
        exact_match = self._match_alert_to_catalog_exact(alert)
        if exact_match is not None:
            return exact_match
        
        # Fall back to ML-based similarity matching
        return self.match_alert_to_catalog_ml(alert, ml_threshold)
    
    def _match_alert_to_catalog_exact(self, alert: Dict) -> Optional[int]:
        """
        Match using exact attribute matching (original priority-based method).
        
        Matching Strategy (in priority order):
        1. EXACT MATCH: category + subcategory + service (most specific)
        2. EXACT MATCH: category + subcategory + namespace (alternative specific)
        3. EXACT MATCH: category + service only (broader match)
        4. EXACT MATCH: category + namespace only (fallback match)
        """
        if not self.cluster_catalog:
            return None
        
        alert_category = (alert.get('alert_category') or '').lower().strip()
        alert_subcategory = (alert.get('alert_subcategory') or '').lower().strip()
        alert_service = (alert.get('graph_service') or '').strip()
        alert_namespace = (alert.get('namespace') or '').strip()
        
        # Must have category
        if not alert_category:
            return None
        
        # Try matching in priority order (most specific to broader)
        
        # Priority 1: category + subcategory + service (most specific match)
        if alert_subcategory and alert_service:
            for cluster_id, cluster_info in self.cluster_catalog.items():
                catalog_category = cluster_info.get('alert_category', '').lower().strip()
                catalog_subcategory = cluster_info.get('alert_subcategory', '').lower().strip()
                catalog_services = cluster_info.get('services', [])
                
                if (alert_category == catalog_category and 
                    alert_subcategory == catalog_subcategory and 
                    alert_service in catalog_services):
                    return cluster_id
        
        # Priority 2: category + subcategory + namespace (alternative specific match)
        if alert_subcategory and alert_namespace:
            for cluster_id, cluster_info in self.cluster_catalog.items():
                catalog_category = cluster_info.get('alert_category', '').lower().strip()
                catalog_subcategory = cluster_info.get('alert_subcategory', '').lower().strip()
                catalog_namespaces = cluster_info.get('namespaces', [])
                
                if (alert_category == catalog_category and 
                    alert_subcategory == catalog_subcategory and 
                    alert_namespace in catalog_namespaces):
                    return cluster_id
        
        # Priority 3: category + service only (broader match)
        if alert_service:
            for cluster_id, cluster_info in self.cluster_catalog.items():
                catalog_category = cluster_info.get('alert_category', '').lower().strip()
                catalog_services = cluster_info.get('services', [])
                
                if (alert_category == catalog_category and 
                    alert_service in catalog_services):
                    return cluster_id
        
        # Priority 4: category + namespace only (fallback match)
        if alert_namespace:
            for cluster_id, cluster_info in self.cluster_catalog.items():
                catalog_category = cluster_info.get('alert_category', '').lower().strip()
                catalog_namespaces = cluster_info.get('namespaces', [])
                
                if (alert_category == catalog_category and 
                    alert_namespace in catalog_namespaces):
                    return cluster_id
        
        # No match found
        return None
    
    def match_alert_to_catalog(self, alert: Dict) -> Optional[int]:
        """
        Match an alert to catalog using hybrid approach (exact + ML fallback).
        Default matching method for the class.
        """
        return self.match_alert_to_catalog_hybrid(alert, ml_threshold=0.5)
    
    def match_alerts_to_catalog(self, alerts: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Match multiple alerts to existing catalog clusters.
        
        Returns:
            - matched_alerts: List of alerts that matched catalog clusters
            - unmatched_alerts: List of alerts that need clustering
        """
        print(f"  Matching {len(alerts)} alerts to catalog...")
        
        matched_alerts = []
        unmatched_alerts = []
        
        for alert in alerts:
            catalog_cluster_id = self.match_alert_to_catalog(alert)
            
            if catalog_cluster_id is not None:
                # Mark alert with catalog cluster
                alert['catalog_cluster_id'] = catalog_cluster_id
                alert['cluster_id'] = catalog_cluster_id
                alert['clustering_method'] = 'catalog_match'
                
                cluster_info = self.cluster_catalog[catalog_cluster_id]
                alert['cluster_name'] = cluster_info.get('cluster_name', f'catalog_c{catalog_cluster_id}')
                alert['cluster_rank'] = cluster_info.get('rank', -1)
                
                matched_alerts.append(alert)
            else:
                unmatched_alerts.append(alert)
        
        print(f"    Matched: {len(matched_alerts)} alerts")
        print(f"    Unmatched: {len(unmatched_alerts)} alerts need clustering")
        
        return matched_alerts, unmatched_alerts
    
    def update_catalog_with_clusters(self, deduplicated_alerts: List[Dict]):
        """
        Update cluster catalog with current run's clusters.
        
        Args:
            deduplicated_alerts: List of deduplicated alerts with cluster assignments
        """
        print(f"  Updating catalog with new cluster patterns...")
        
        current_time = pd.Timestamp.now().timestamp()
        cluster_alerts_dict = {}
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
            categories = [a.get('alert_category', '') for a in alerts]
            subcategories = [a.get('alert_subcategory', '') for a in alerts]
            services = [a.get('graph_service', '') for a in alerts if a.get('graph_service')]
            namespaces = [a.get('namespace', '') for a in alerts if a.get('namespace')]
            alert_ids = [a.get('_id', '') for a in alerts if a.get('_id')]
            
            most_common_category = Counter(categories).most_common(1)[0][0] if categories else ''
            most_common_subcategory = Counter(subcategories).most_common(1)[0][0] if subcategories else ''
            cluster_name = alerts[0].get('cluster_name', f'c{cluster_id}')
            cluster_rank = alerts[0].get('cluster_rank', -1)
            if cluster_id in self.cluster_catalog:
                self.cluster_catalog[cluster_id]['last_seen'] = current_time
                self.cluster_catalog[cluster_id]['total_count'] += len(alerts)
                existing_services = set(self.cluster_catalog[cluster_id].get('services', []))
                existing_namespaces = set(self.cluster_catalog[cluster_id].get('namespaces', []))
                existing_ids = set(self.cluster_catalog[cluster_id].get('_ids', []))
                
                self.cluster_catalog[cluster_id]['services'] = list(existing_services | set(services))
                self.cluster_catalog[cluster_id]['namespaces'] = list(existing_namespaces | set(namespaces))
                self.cluster_catalog[cluster_id]['_ids'] = list(existing_ids | set(alert_ids))
            else:
                self.cluster_catalog[cluster_id] = {
                    'cluster_id': cluster_id,
                    'alert_category': most_common_category,
                    'alert_subcategory': most_common_subcategory,
                    'services': list(set(services)),
                    'namespaces': list(set(namespaces)),
                    '_ids': list(set(alert_ids)),
                    'last_seen': current_time,
                    'total_count': len(alerts),
                    'cluster_name': cluster_name,
                    'rank': cluster_rank
                }
                # Track count of clusters (string IDs don't need sequential tracking)
                self.next_cluster_id = len(self.cluster_catalog)
    
    def get_next_cluster_id(self) -> int:
        """Get next available cluster ID"""
        cluster_id = self.next_cluster_id
        self.next_cluster_id += 1
        return cluster_id
    
    def get_catalog_stats(self) -> Dict:
        """Get statistics about the catalog"""
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
        """Clear the catalog (for testing/reset)"""
        self.cluster_catalog = {}
        self.next_cluster_id = 0
    
    def export_catalog_summary(self, output_path: str):
        """Export catalog summary to CSV"""
        if not self.cluster_catalog:
            print("  No catalog data to export")
            return
        
        catalog_data = []
        for cluster_id, cluster_info in self.cluster_catalog.items():
            catalog_data.append({
                'cluster_id': cluster_id,
                'cluster_name': cluster_info.get('cluster_name', ''),
                'alert_category': cluster_info.get('alert_category', ''),
                'alert_subcategory': cluster_info.get('alert_subcategory', ''),
                'services': ', '.join(cluster_info.get('services', [])),
                'namespaces': ', '.join(cluster_info.get('namespaces', [])),
                '_ids': ', '.join(cluster_info.get('_ids', [])),
                'total_count': cluster_info.get('total_count', 0),
                'last_seen': pd.to_datetime(cluster_info.get('last_seen', 0), unit='s'),
                'rank': cluster_info.get('rank', -1)
            })
        
        df = pd.DataFrame(catalog_data)
        df = df.sort_values('total_count', ascending=False)
        df.to_csv(output_path, index=False)
