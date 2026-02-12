
import numpy as np
from typing import Dict, List, Tuple, Optional


class ClusterPredictor:
    
    EXPECTED_FEATURE_NAMES = None
    
    def __init__(self, model_manager, catalog_manager=None):
        self.model_manager = model_manager
        self.catalog_manager = catalog_manager
        self.loaded_model = None
        self.model_version = None
    # +++++++++++++++++++++++++++++++++++++++++++++++++++
    # checked
    def load_current_model(self):
        if self.loaded_model is None:
            self.loaded_model = self.model_manager.load_model()
            self.model_version = self.loaded_model['metadata']['version']
            self.EXPECTED_FEATURE_NAMES = self.loaded_model['metadata'].get('feature_names', [])
    
    # +++++++++++++++++++++++++++++++++++++++++++++++++++
    # checked
    def predict_clusters(self, feature_matrix_scaled: np.ndarray) -> Tuple[np.ndarray, str, float]:
        if self.loaded_model is None:
            self.load_current_model()
        
        model = self.loaded_model['model']
        metadata = self.loaded_model['metadata']
        
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
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # todo: need to bug this, this must be used in job scheduler
    # def predict_for_unmatched(self, unmatched_alerts: List[Dict], 
    #                          feature_matrix_scaled: np.ndarray) -> np.ndarray:
    #     if len(unmatched_alerts) == 0:
    #         return np.array([])
        
    #     next_cluster_id = 0
    #     if self.catalog_manager:
    #         next_cluster_id = self.catalog_manager.next_cluster_id
        
    #     cluster_labels, _, _ = self.predict_clusters(feature_matrix_scaled)
    #     cluster_labels = cluster_labels + next_cluster_id
    #     return cluster_labels
    
    def _calculate_confidence(self, feature_vector: np.ndarray, predicted_cluster: int) -> Optional[float]:
        model = self.loaded_model['model']
        
        if not hasattr(model, 'cluster_centers_'):
            return None
        
        center = model.cluster_centers_[predicted_cluster]
        distance = np.linalg.norm(feature_vector - center)
        all_distances = [np.linalg.norm(feature_vector - c) for c in model.cluster_centers_]
        max_dist = max(all_distances)
        confidence = 1 - (distance / max_dist) if max_dist > 0 else 1.0
        
        return round(confidence, 3)
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # checked and being called in job scheduler, make sure the model is reloaded after retraining
    def reload_model(self, version: int = None):
        self.loaded_model = None
        if version:
            self.loaded_model = self.model_manager.load_model(version)
            self.model_version = version
            self.EXPECTED_FEATURE_NAMES = self.loaded_model['metadata'].get('feature_names', [])
        else:
            self.load_current_model()
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
