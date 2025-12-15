"""
Cluster Predictor

Predicts cluster assignments for new alerts using a trained model.
"""

import numpy as np
from typing import Dict, List, Tuple


class ClusterPredictor:
    """Assign new alerts to existing clusters using trained model"""
    
    def __init__(self, model_manager):
        self.model_manager = model_manager
        self.loaded_model = None
        self.model_version = None
    
    def load_current_model(self):
        """Load the current production model"""
        if self.loaded_model is None:
            self.loaded_model = self.model_manager.load_model()
            self.model_version = self.loaded_model['metadata']['version']
    
    def predict_clusters(self, feature_matrix_scaled: np.ndarray) -> Tuple[np.ndarray, str, float]:
        """
        Predict cluster assignments for new alerts
        
        Returns:
            - cluster_labels: np.ndarray of cluster assignments
            - clustering_method: str name of the clustering algorithm
            - confidence_scores: confidence score
        """
        # Ensure model is loaded
        if self.loaded_model is None:
            self.load_current_model()
        
        model = self.loaded_model['model']
        metadata = self.loaded_model['metadata']
        
        # Predict cluster assignments
        cluster_labels = model.predict(feature_matrix_scaled)
        
        # Calculate average confidence if model has cluster centers
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
    
    def _calculate_confidence(self, feature_vector: np.ndarray, predicted_cluster: int) -> float:
        """Calculate confidence score based on distance to cluster center"""
        model = self.loaded_model['model']
        
        if not hasattr(model, 'cluster_centers_'):
            return None
        
        center = model.cluster_centers_[predicted_cluster]
        distance = np.linalg.norm(feature_vector - center)
        
        # Normalize to 0-1 (closer = higher confidence)
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
        else:
            self.load_current_model()
