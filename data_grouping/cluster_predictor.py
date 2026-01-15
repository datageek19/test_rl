"""
Cluster Predictor: predicts cluster for new alerts using a trained model
"""

import numpy as np
from typing import Dict, List, Tuple, Optional


class ClusterPredictor:
    """Assign new alerts to existing clusters using trained model or catalog"""
    
    # Expected features in fixed order - enforce consistency across runs
    EXPECTED_FEATURE_NAMES = None  # Will be loaded from model metadata
    
    def __init__(self, model_manager, catalog_manager=None):
        self.model_manager = model_manager
        self.catalog_manager = catalog_manager
        self.loaded_model = None
        self.model_version = None
    
    def load_current_model(self):
        """Load the current production model"""
        if self.loaded_model is None:
            self.loaded_model = self.model_manager.load_model()
            self.model_version = self.loaded_model['metadata']['version']
            # Store expected feature names from model metadata
            self.EXPECTED_FEATURE_NAMES = self.loaded_model['metadata'].get('feature_names', [])
    
    def predict_clusters(self, feature_matrix_scaled: np.ndarray, feature_names: List[str] = None) -> Tuple[np.ndarray, str, float]:
        """
        Predict cluster assignments for new alerts
        
        Args:
            feature_matrix_scaled: Feature matrix (scaled)
            feature_names: List of feature names (for validation)
        
        Returns:
            - cluster_labels: np.ndarray of cluster assignments
            - clustering_method: str name of the clustering algorithm
            - confidence_scores: confidence score
        """
        if self.loaded_model is None:
            self.load_current_model()
        
        model = self.loaded_model['model']
        metadata = self.loaded_model['metadata']
        
        # Validate feature count using pca_components (actual training features)
        # Note: feature_count and pca_components should be the same value
        expected_features = metadata.get('pca_components', metadata.get('feature_count', None))
        actual_features = feature_matrix_scaled.shape[1]
        
        if expected_features and expected_features != actual_features:
            raise ValueError(
                f"Feature mismatch! Model expects {expected_features} PCA components, "
                f"but got {actual_features}. This indicates the model needs retraining."
            )
        
        cluster_labels = model.predict(feature_matrix_scaled)
        avg_confidence = None
        if hasattr(model, 'cluster_centers_'):
            confidences = []
            for i, feature_vector in enumerate(feature_matrix_scaled):
                conf = self._calculate_confidence(feature_vector, cluster_labels[i])
                confidences.append(conf)
            avg_confidence = np.mean(confidences) if confidences else None

        if avg_confidence:
            print(f"  Average confidence: {avg_confidence:.3f}")
        
        return cluster_labels, metadata['algorithm'], avg_confidence
    
    def predict_with_catalog(self, alerts: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Predict clusters using catalog first, then model for unmatched alerts.
        
        Args:
            alerts: List of alerts to classify
            
        Returns:
            - matched_alerts: Alerts matched to catalog clusters
            - unmatched_alerts: Alerts that need model prediction
        """
        if not self.catalog_manager:
            print("  No catalog manager configured - using model only")
            return [], alerts

        matched_alerts, unmatched_alerts = self.catalog_manager.match_alerts_to_catalog(alerts)
        return matched_alerts, unmatched_alerts
    
    def predict_for_unmatched(self, unmatched_alerts: List[Dict], 
                             feature_matrix_scaled: np.ndarray) -> np.ndarray:
        """
        Predict clusters for alerts that didn't match catalog.
        
        Args:
            unmatched_alerts: Alerts not matched to catalog
            feature_matrix_scaled: Feature matrix for unmatched alerts
            
        Returns:
            cluster_labels: Predicted cluster IDs
        """
        if len(unmatched_alerts) == 0:
            return np.array([])
        
        # next cluster ID from catalog
        next_cluster_id = 0
        if self.catalog_manager:
            next_cluster_id = self.catalog_manager.next_cluster_id
        
        # Predict using model
        # cluster_labels, clustering_method, avg_confidence = self.predict_clusters(feature_matrix_scaled)
        cluster_labels, _, _ = self.predict_clusters(feature_matrix_scaled)
        cluster_labels = cluster_labels + next_cluster_id
        return cluster_labels
    
    def _calculate_confidence(self, feature_vector: np.ndarray, predicted_cluster: int) -> float:
        """Calculate confidence score based on distance to cluster center"""
        model = self.loaded_model['model']
        
        if not hasattr(model, 'cluster_centers_'):
            return None
        
        center = model.cluster_centers_[predicted_cluster]
        distance = np.linalg.norm(feature_vector - center)
        all_distances = [np.linalg.norm(feature_vector - c) for c in model.cluster_centers_]
        max_dist = max(all_distances)
        confidence = 1 - (distance / max_dist) if max_dist > 0 else 1.0
        
        return round(confidence, 3)
    
    def get_cluster_profile(self, cluster_id: int) -> Dict:
        """Get the profile/characteristics of a specific cluster"""
        if self.loaded_model is None:
            self.load_current_model()
        
        profiles = self.loaded_model['profiles']
        return profiles.get(str(cluster_id), {})
    
    def get_all_cluster_profiles(self) -> Dict:
        """Get profiles for all clusters"""
        if self.loaded_model is None:
            self.load_current_model()
        
        return self.loaded_model['profiles']
    
    def reload_model(self, version: int = None):
        """Reload model ( after retraining cluster model)"""
        self.loaded_model = None
        if version:
            self.loaded_model = self.model_manager.load_model(version)
            self.model_version = version
            self.EXPECTED_FEATURE_NAMES = self.loaded_model['metadata'].get('feature_names', [])
        else:
            self.load_current_model()
    
    def get_expected_features(self) -> List[str]:
        """Get the list of expected feature names from the loaded model"""
        if self.EXPECTED_FEATURE_NAMES is None:
            self.load_current_model()
        return self.EXPECTED_FEATURE_NAMES
    
    def validate_features(self, feature_names: List[str], feature_matrix: np.ndarray) -> bool:
        """
        Validate that current features match expected features
        
        Returns:
            True if features are consistent, False if they don't match
        """
        expected = self.get_expected_features()
        
        if not expected:
            return True  # No expected features stored, can't validate
        
        if len(expected) != len(feature_names):
            print(f"  Feature count mismatch: expected {len(expected)}, got {len(feature_names)}")
            return False
        
        if expected != feature_names:
            print(f"  Feature names mismatch!")
            print(f"    Expected: {expected}")
            print(f"    Got: {feature_names}")
            return False
        
        if feature_matrix.shape[1] != len(expected):
            print(f"  Feature matrix shape mismatch: expected {len(expected)} features, got {feature_matrix.shape[1]}")
            return False
        
        return True
