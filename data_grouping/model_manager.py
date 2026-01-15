"""
cluster model manager: handles cluster model training, versioning, serialization, and metadata management
"""

import joblib
import json
import os
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from collections import Counter
from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from sklearn.decomposition import PCA


class ModelManager:
    """Manage clustering model lifecycle: training, versioning, serialization"""
    
    def __init__(self, model_dir='data/models', catalog_manager=None):
        self.model_dir = model_dir
        self.catalog_manager = catalog_manager
        os.makedirs(model_dir, exist_ok=True)
        os.makedirs(f'{model_dir}/metadata', exist_ok=True)
        
        self.current_version = self._get_latest_version()
    
    def _get_latest_version(self) -> int:
        """Get the latest model version from the model directory"""
        if not os.path.exists(self.model_dir):
            return 0
        model_files = [f for f in os.listdir(self.model_dir) 
                      if f.startswith('alert_clustering_v') and f.endswith('.pkl')]
        
        if not model_files:
            return 0
        versions = []
        for f in model_files:
            try:
                version_str = f.replace('alert_clustering_v', '').replace('.pkl', '')
                versions.append(int(version_str))
            except ValueError:
                continue
        
        return max(versions) if versions else 0
    
    def train_new_model(self, feature_matrix_scaled: np.ndarray, 
                       alerts: List[Dict],
                       scaler: StandardScaler,
                       pca: PCA,
                       feature_names: List[str],
                       algorithm: str = 'kmeans',
                       n_clusters: int = None) -> Tuple[int, Dict, np.ndarray]:
        """
        Train a new clustering model and save with versioning
        
        Returns:
            - version: Model version number
            - metadata: Model metadata dict
            - labels: Cluster labels from training
        """
        if n_clusters is None:
            n_clusters = self._find_optimal_k(feature_matrix_scaled, 
                                              max_k=min(20, len(feature_matrix_scaled) // 2))
        if algorithm == 'kmeans':
            model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        elif algorithm == 'dbscan':
            eps = self._estimate_dbscan_eps(feature_matrix_scaled)
            model = DBSCAN(eps=eps, min_samples=max(2, len(feature_matrix_scaled) // 100))
        elif algorithm == 'hierarchical':
            model = AgglomerativeClustering(n_clusters=n_clusters, linkage='ward')
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        labels = model.fit_predict(feature_matrix_scaled)
        try:
            silhouette = silhouette_score(feature_matrix_scaled, labels)
        except:
            silhouette = 0.0
        version = self._increment_version()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Ensure model directory exists before saving
        os.makedirs(self.model_dir, exist_ok=True)
        
        model_path = f'{self.model_dir}/alert_clustering_v{version}.pkl'
        scaler_path = f'{self.model_dir}/scaler_v{version}.pkl'
        pca_path = f'{self.model_dir}/pca_v{version}.pkl'
        
        joblib.dump(model, model_path)
        joblib.dump(scaler, scaler_path)
        joblib.dump(pca, pca_path)
        
        # Ensure metadata directory exists
        metadata_dir = f'{self.model_dir}/metadata'
        os.makedirs(metadata_dir, exist_ok=True)
        
        # Store PCA component count as the actual feature count (what the model was trained on)
        pca_components = int(pca.n_components_) if pca and hasattr(pca, 'n_components_') else len(feature_names)
        
        metadata = {
            'version': int(version),
            'algorithm': str(algorithm),
            'n_clusters': int(n_clusters if algorithm != 'dbscan' else len(set(labels)) - (1 if -1 in labels else 0)),
            'silhouette_score': float(silhouette),
            'n_samples': int(len(alerts)),
            'timestamp': str(timestamp),
            'feature_count': pca_components,  # Use PCA component count, not original feature count
            'feature_names': [str(f) for f in feature_names],  # Original feature names for reference
            'pca_components': pca_components,
            'cluster_centers': model.cluster_centers_.tolist() if hasattr(model, 'cluster_centers_') else None
        }
        
        metadata_path = f'{metadata_dir}/v{version}_metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        cluster_profiles = self._profile_clusters(labels, alerts)
        profile_path = f'{metadata_dir}/v{version}_profiles.json'
        with open(profile_path, 'w') as f:
            json.dump(cluster_profiles, f, indent=2)
        
        print(f"  Model v{version} trained and saved")
        print(f"    - Model: {model_path}")
        print(f"    - Metadata: {metadata_path}")
        print(f"    - Profiles: {profile_path}")
        print(f"  Algorithm: {algorithm}, Clusters: {metadata['n_clusters']}, Silhouette: {silhouette:.3f}")
        
        self.current_version = version
        
        return version, metadata, labels
    
    def _increment_version(self) -> int:
        """Increment version number"""
        return self.current_version + 1
    
    def _find_optimal_k(self, feature_matrix: np.ndarray, max_k: int = 20) -> int:
        """Find optimal number of clusters using silhouette score"""
        n_samples = len(feature_matrix)
        max_k = min(max_k, n_samples // 5)
        
        if max_k < 3:
            return 2
        
        silhouette_scores = []
        K_range = range(2, max_k + 1)
        for k in K_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(feature_matrix)
            try:
                score = silhouette_score(feature_matrix, labels)
                silhouette_scores.append(score)
            except:
                silhouette_scores.append(0)
        
        if silhouette_scores:
            best_k = K_range[np.argmax(silhouette_scores)]
            print(f"     Optimal k: {best_k} (silhouette: {max(silhouette_scores):.3f})")
            return best_k
        return 2
    
    def _estimate_dbscan_eps(self, feature_matrix: np.ndarray) -> float:
        """Estimate DBSCAN eps parameter"""
        n_samples = len(feature_matrix)
        k = min(5, max(2, n_samples // 10))
        
        nbrs = NearestNeighbors(n_neighbors=k).fit(feature_matrix)
        distances, _ = nbrs.kneighbors(feature_matrix)
        
        eps = np.percentile(distances[:, -1], 75)
        return max(0.3, min(eps, 2.0))
    
    def _profile_clusters(self, labels: np.ndarray, alerts: List[Dict]) -> Dict:
        """Generate readable profiles for each cluster"""
        cluster_profiles = {}
        
        unique_labels = set(labels)
        for cluster_id in unique_labels:
            if cluster_id == -1:  # Skip noise
                continue
            
            cluster_alerts = [alert for i, alert in enumerate(alerts) if labels[i] == cluster_id]
            
            if not cluster_alerts:
                continue
            alert_types = [a.get('alert_name', '') for a in cluster_alerts]
            services = [a.get('service_name', '') for a in cluster_alerts]
            categories = [a.get('alert_category', '') for a in cluster_alerts]
            severities = [a.get('severity', '') for a in cluster_alerts]
            
            profile = {
                'cluster_id': int(cluster_id),
                'size': int(len(cluster_alerts)),
                'top_alert_types': {str(k): int(v) for k, v in Counter(alert_types).most_common(5)},
                'top_services': {str(k): int(v) for k, v in Counter(services).most_common(5)},
                'severity_distribution': {str(k): int(v) for k, v in Counter(severities).items()},
                'category_distribution': {str(k): int(v) for k, v in Counter(categories).items()},
                'most_common_alert': str(Counter(alert_types).most_common(1)[0][0]) if alert_types else '',
                'most_common_service': str(Counter(services).most_common(1)[0][0]) if services else '',
            }
            
            cluster_profiles[str(cluster_id)] = profile
        
        return cluster_profiles
    
    def load_model(self, version: int = None) -> Dict:
        """Load a specific model version (or latest)"""
        if version is None:
            version = self.current_version
        
        if version == 0:
            raise ValueError("No trained model found. Please train a model first.")
        
        model_path = f'{self.model_dir}/alert_clustering_v{version}.pkl'
        scaler_path = f'{self.model_dir}/scaler_v{version}.pkl'
        pca_path = f'{self.model_dir}/pca_v{version}.pkl'
        metadata_path = f'{self.model_dir}/metadata/v{version}_metadata.json'
        profile_path = f'{self.model_dir}/metadata/v{version}_profiles.json'
        
        # Store expected feature names from metadata
        # This will be used to enforce consistent features
        expected_features_path = f'{self.model_dir}/metadata/v{version}_expected_features.txt'
        
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model v{version} not found at {model_path}")
        
        model = joblib.load(model_path)
        scaler = joblib.load(scaler_path)
        pca = joblib.load(pca_path) if os.path.exists(pca_path) else None
        
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
        
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        if not os.path.exists(profile_path):
            print(f"Warning: Profile file not found: {profile_path}")
            print(f"Creating empty profiles...")
            profiles = {}
        else:
            with open(profile_path, 'r') as f:
                profiles = json.load(f)
        
        return {
            'model': model,
            'scaler': scaler,
            'pca': pca,
            'metadata': metadata,
            'profiles': profiles,
            'version': version
        }
    
    def compare_versions(self, v1: int, v2: int) -> Dict:
        """Compare two model versions"""
        meta1 = self._load_metadata(v1)
        meta2 = self._load_metadata(v2)
        
        return {
            'v1': v1,
            'v2': v2,
            'silhouette_improvement': meta2['silhouette_score'] - meta1['silhouette_score'],
            'cluster_count_change': meta2['n_clusters'] - meta1['n_clusters'],
            'sample_count_change': meta2['n_samples'] - meta1['n_samples'],
            'recommended': 'v2' if meta2['silhouette_score'] > meta1['silhouette_score'] else 'v1'
        }
    
    def _load_metadata(self, version: int) -> Dict:
        """Load metadata for a specific version"""
        metadata_path = f'{self.model_dir}/metadata/v{version}_metadata.json'
        with open(metadata_path, 'r') as f:
            return json.load(f)
    
    def get_all_versions(self) -> List[Dict]:
        """Get metadata for all available model versions"""
        metadata_dir = f'{self.model_dir}/metadata'
        
        if not os.path.exists(metadata_dir):
            return []
        
        versions = []
        for filename in os.listdir(metadata_dir):
            if filename.endswith('_metadata.json'):
                filepath = os.path.join(metadata_dir, filename)
                with open(filepath, 'r') as f:
                    metadata = json.load(f)
                    versions.append(metadata)
        versions.sort(key=lambda x: x['version'], reverse=True)
        return versions
    
    def sync_with_catalog(self, deduplicated_alerts: List[Dict]):
        """
        Sync trained clusters with catalog manager.
        Updates catalog with new cluster patterns after model training.
        
        Args:
            deduplicated_alerts: Alerts with cluster assignments from training
        """
        if not self.catalog_manager:
            print("  No catalog manager configured - skipping catalog sync")
            return

        self.catalog_manager.update_catalog_with_clusters(deduplicated_alerts)
        self.catalog_manager.save_catalog()
        
        catalog_stats = self.catalog_manager.get_catalog_stats()
        print(f"  Catalog updated: {catalog_stats['total_clusters']} total clusters")
    
    def get_catalog_stats(self) -> Optional[Dict]:
        """Get statistics from catalog manager if available"""
        if not self.catalog_manager:
            return None
        
        return self.catalog_manager.get_catalog_stats()
    
    def enforce_consistent_features(self, feature_matrix: np.ndarray, 
                                   feature_names: List[str], 
                                   expected_feature_names: List[str] = None) -> Tuple[np.ndarray, List[str]]:
        """
        Enforce consistent features by:
        1. Reordering features to match expected order
        2. Adding missing features as zeros
        3. Removing extra features
        
        Args:
            feature_matrix: Current feature matrix (n_samples x n_features)
            feature_names: Names of current features
            expected_feature_names: Expected feature names in order (if None, will not enforce)
        
        Returns:
            - adjusted_feature_matrix: Feature matrix with consistent features
            - adjusted_feature_names: Consistent feature names
        """
        if expected_feature_names is None:
            return feature_matrix, feature_names
        
        n_samples = feature_matrix.shape[0]
        adjusted_matrix = np.zeros((n_samples, len(expected_feature_names)))
        
        # Map current features to expected positions
        current_feature_dict = {name: i for i, name in enumerate(feature_names)}
        
        missing_features = []
        for i, expected_name in enumerate(expected_feature_names):
            if expected_name in current_feature_dict:
                # Copy the feature from current matrix
                current_idx = current_feature_dict[expected_name]
                adjusted_matrix[:, i] = feature_matrix[:, current_idx]
            else:
                # Feature is missing - fill with zeros
                missing_features.append(expected_name)
                adjusted_matrix[:, i] = 0.0
        
        if missing_features:
            print(f"  WARNING: {len(missing_features)} missing features, filling with zeros:")
            for f in missing_features:
                print(f"    - {f}")
        
        extra_features = set(feature_names) - set(expected_feature_names)
        if extra_features:
            print(f"  WARNING: {len(extra_features)} extra features being ignored:")
            for f in extra_features:
                print(f"    - {f}")
        
        return adjusted_matrix, expected_feature_names
