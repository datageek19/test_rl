"""
Causal Inference-Based Root Cause Analysis for Microservices

This module implements a causal ML approach for RCA following DoWhy's GCM methodology.
Reference: https://www.pywhy.org/dowhy/main/example_notebooks/gcm_rca_microservice_architecture.html
"""

import pandas as pd
import numpy as np
import json
import networkx as nx
from pathlib import Path
from dowhy import gcm
from sklearn.metrics import r2_score, mean_squared_error
from typing import Dict, List, Tuple, Optional


class CausalRCA:
    """
    Causal Inference-based Root Cause Analysis for microservices latency.
    """
    
    def __init__(self, latency_folder: str, graph_json_path: str):
        self.latency_folder = latency_folder
        self.graph_json_path = graph_json_path
        self.latency_data = None
        self.causal_graph = None
        self.causal_model = None
        self.normal_data = None
        self.anomalous_data = None
        
    def load_latency_data(self) -> pd.DataFrame:
        """Load and consolidate latency data from CSV files"""
        folder = Path(self.latency_folder)
        csv_files = list(folder.glob("*.csv"))
        
        if not csv_files:
            raise ValueError(f"No CSV files found in {self.latency_folder}")
        
        dataframes = []
        for csv_file in csv_files:
            df = pd.read_csv(csv_file)
            
            time_col = None
            for col in df.columns:
                if 'time' in col.lower():
                    time_col = col
                    break
            
            if time_col is None:
                continue
            
            df[time_col] = pd.to_datetime(df[time_col])
            service_cols = [c for c in df.columns if c != time_col]
            
            for service_col in service_cols:
                df[service_col] = df[service_col].astype(str).str.replace(' ms', '').astype(float, errors='ignore')
                service_df = pd.DataFrame({
                    'Time': df[time_col],
                    service_col: df[service_col]
                }).set_index('Time')
                dataframes.append(service_df)
        
        if not dataframes:
            raise ValueError("No valid data loaded")
        
        result = dataframes[0]
        for df in dataframes[1:]:
            result = result.join(df, how='outer')
        
        result = result.sort_index()
        result = result.ffill().bfill().interpolate(method='time')
        
        self.latency_data = result
        print(f"✓ Loaded latency data: {result.shape}")
        return result
    
    def load_service_graph(self) -> nx.DiGraph:
        """Load service dependency graph from JSON"""
        with open(self.graph_json_path, 'r') as f:
            relationships = json.load(f)
        
        graph = nx.DiGraph()
        
        for rel in relationships:
            source_props = rel.get('source_properties', {})
            target_props = rel.get('target_properties', {})
            source_name = source_props.get('name', '')
            target_name = target_props.get('name', '')
            rel_type = rel.get('relationship_type', '')
            
            if source_name and target_name and rel_type:
                graph.add_edge(source_name, target_name, relationship_type=rel_type)
        
        self.causal_graph = graph
        print(f"✓ Loaded service graph: {len(graph.nodes())} nodes, {len(graph.edges())} edges")
        return graph
    
    def match_and_filter(self) -> Tuple[nx.DiGraph, pd.DataFrame]:
        """Match latency data with service graph"""
        if self.latency_data is None or self.causal_graph is None:
            raise ValueError("Load data and graph first")
        
        latency_services = set(self.latency_data.columns)
        graph_services = set(self.causal_graph.nodes())
        common_services = latency_services.intersection(graph_services)
        
        print(f"✓ Matched services: {len(common_services)}/{len(latency_services)}")
        
        subgraph = self.causal_graph.subgraph(common_services).copy()
        filtered_data = self.latency_data[list(common_services)]
        
        self.causal_graph = subgraph
        self.latency_data = filtered_data
        
        return subgraph, filtered_data
    
    def split_data(self, normal_ratio: float = 0.8) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Split data into normal and anomalous periods"""
        if self.latency_data is None:
            raise ValueError("Load data first")
        
        split_idx = int(len(self.latency_data) * normal_ratio)
        self.normal_data = self.latency_data.iloc[:split_idx]
        self.anomalous_data = self.latency_data.iloc[split_idx:]
        
        print(f"✓ Split data: {len(self.normal_data)} normal, {len(self.anomalous_data)} anomalous")
        return self.normal_data, self.anomalous_data
    
    def build_causal_model(self) -> gcm.ProbabilisticCausalModel:
        """Build and fit probabilistic causal model"""
        if self.causal_graph is None or self.normal_data is None:
            raise ValueError("Load and split data first")
        
        model = gcm.ProbabilisticCausalModel(self.causal_graph)
        
        for node in self.causal_graph.nodes():
            if self.causal_graph.in_degree(node) == 0:
                model.set_causal_mechanism(node, gcm.EmpiricalDistribution())
            else:
                model.set_causal_mechanism(node, gcm.AdditiveNoiseModel(gcm.ml.create_linear_regressor()))
        
        print("✓ Built causal model")
        self.causal_model = model
        return model
    
    def fit_model(self):
        """Fit causal model on normal data"""
        if self.causal_model is None or self.normal_data is None:
            raise ValueError("Build model and split data first")
        
        gcm.fit(self.causal_model, self.normal_data)
        print("✓ Fitted causal model on normal data")
    
    def evaluate_model(self) -> Dict:
        """Evaluate model fit on normal data"""
        if self.causal_model is None or self.normal_data is None:
            raise ValueError("Fit model first")
        
        results = {}
        print("\nModel Evaluation:")
        print("=" * 60)
        
        for node in self.causal_graph.nodes():
            if self.causal_graph.in_degree(node) > 0:
                parents = list(self.causal_graph.predecessors(node))
                X = self.normal_data[parents].values
                y_true = self.normal_data[node].values
                
                mechanism = self.causal_model.causal_mechanism(node)
                y_pred = mechanism.estimate_target(X)
                
                r2 = r2_score(y_true, y_pred)
                mse = mean_squared_error(y_true, y_pred)
                rmse = np.sqrt(mse)
                
                results[node] = {'r2': r2, 'rmse': rmse, 'parents': parents}
                print(f"{node}: R²={r2:.4f}, RMSE={rmse:.4f}")
        
        print("=" * 60)
        return results
    
    def perform_rca(self, target_node: Optional[str] = None) -> pd.DataFrame:
        """Perform root cause analysis on anomalous data"""
        if self.causal_model is None or self.anomalous_data is None:
            raise ValueError("Fit model and have anomalous data first")
        
        if target_node is None:
            target_node = list(self.causal_graph.nodes())[0]
        
        print(f"\nPerforming RCA (target: {target_node})...")
        attributions = gcm.attribute_anomalies(self.causal_model, 
                                              target_node=target_node,
                                              anomaly_samples=self.anomalous_data)
        
        median_attributions = attributions.median().to_dict()
        sorted_causes = sorted(median_attributions.items(), 
                             key=lambda x: abs(x[1]), 
                             reverse=True)
        
        print("\nRoot Causes (sorted by contribution):")
        print("=" * 60)
        for i, (node, contribution) in enumerate(sorted_causes, 1):
            print(f"{i}. {node}: {contribution:.4f}")
        print("=" * 60)
        
        return attributions
    
    def run_full_pipeline(self):
        """Run complete RCA pipeline"""
        print("="*60)
        print("CAUSAL ML ROOT CAUSE ANALYSIS")
        print("="*60)
        
        self.load_latency_data()
        self.load_service_graph()
        self.match_and_filter()
        self.split_data()
        self.build_causal_model()
        self.fit_model()
        self.evaluate_model()
        attributions = self.perform_rca()
        
        print("\n✓ RCA pipeline complete")
        return attributions


if __name__ == "__main__":
    latency_folder = r"C:/Users/jurat.shayidin/aiops/causal_ml/causal_ml_enhanced/latency_data"
    graph_path = r"C:/Users/jurat.shayidin/aiops/graph_data.json"
    
    rca = CausalRCA(latency_folder, graph_path)
    attributions = rca.run_full_pipeline()

