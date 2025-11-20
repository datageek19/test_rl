import pandas as pd
import numpy as np
import json
import networkx as nx
from pathlib import Path
from dowhy import gcm
from sklearn.metrics import r2_score, mean_squared_error
from typing import Dict, List, Tuple, Optional


class CausalRCA:
    
    def __init__(self, latency_folder: str, graph_json_path: str):
        self.latency_folder = latency_folder
        self.graph_json_path = graph_json_path
        self.latency_data = None
        self.causal_graph = None
        self.causal_model = None
        self.normal_data = None
        self.anomalous_data = None
        self.anomaly_info = None
        
    def load_latency_data(self) -> pd.DataFrame:
        """Load and consolidate latency data from CSV files"""
        folder = Path(self.latency_folder)
        csv_files = list(folder.glob("*.csv"))
        
        if not csv_files:
            raise ValueError(f"No CSV files found in {self.latency_folder}")
        
        dataframes = []
        for csv_file in csv_files:
            df = pd.read_csv(csv_file)
            
            time_col = next((col for col in df.columns if 'time' in col.lower()), None)
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
        
        result = result.sort_index().ffill().bfill().interpolate(method='time')
        result.columns = [col.replace(' - inbound', '').strip() for col in result.columns]
        
        self.latency_data = result
        print(f" Loaded latency data: {result.shape[0]} samples, {result.shape[1]} services")
        print(f"  Services: {list(result.columns)}")
        return result
    
    def load_service_graph(self) -> nx.DiGraph:
        """Load service service graph from JSON"""
        with open(self.graph_json_path, 'r') as f:
            relationships = json.load(f)
        
        graph = nx.DiGraph()
        
        for rel in relationships:
            source_props = rel.get('source_properties') or {}
            target_props = rel.get('target_properties') or {}
            source_name = source_props.get('name', '')
            target_name = target_props.get('name', '')
            rel_type = rel.get('relationship_type', '')
            
            if source_name and target_name and rel_type:
                graph.add_edge(source_name, target_name, relationship_type=rel_type)
        
        print(f" Loaded service service graph: {len(graph.nodes())} nodes, {len(graph.edges())} edges")
        return graph
    
    def build_latency_causal_graph(self, service_call_graph: Optional[nx.DiGraph] = None) -> nx.DiGraph:
        """
        Build causal graph for latency analysis.
        
        Critical: Latency causation is REVERSED from service calls.
        If Service A calls Service B (A→B in service graph), then B's latency 
        causes A's latency (B→A in latency causal graph).
        """
        if self.latency_data is None:
            raise ValueError("Load latency data first")
        
        latency_services = set(self.latency_data.columns)
        
        if service_call_graph is not None:
            graph_services = set(service_call_graph.nodes())
            common_services = latency_services.intersection(graph_services)
            
            if len(common_services) > 1:
                print(f" Matched {len(common_services)}/{len(latency_services)} services with service graph")
                
                subgraph = service_call_graph.subgraph(common_services).copy()
                latency_graph = subgraph.reverse()
                
                self.latency_data = self.latency_data[list(common_services)]
            else:
                print(f" Only {len(common_services)} services matched, using correlation-based graph")
                latency_graph = self._build_graph_from_correlations()
        else:
            latency_graph = self._build_graph_from_correlations()
        
        latency_graph = self._convert_to_dag(latency_graph)
        
        self.causal_graph = latency_graph
        print(f" Built latency causal graph: {len(latency_graph.nodes())} nodes, {len(latency_graph.edges())} edges")
        return latency_graph
    
    def _build_graph_from_correlations(self) -> nx.DiGraph:
        """Build causal graph from correlation structure (fallback method)"""
        print("  Building graph from correlation structure...")
        
        services = list(self.latency_data.columns)
        graph = nx.DiGraph()
        graph.add_nodes_from(services)
        
        corr_matrix = self.latency_data.corr()
        threshold = 0.3
        
        for i, service1 in enumerate(services):
            for j, service2 in enumerate(services):
                if i != j and abs(corr_matrix.iloc[i, j]) > threshold:
                    if self.latency_data[service1].var() > self.latency_data[service2].var():
                        graph.add_edge(service1, service2)
        
        print(f"  Created {len(graph.edges())} edges based on correlations (threshold={threshold})")
        return graph
    
    def _convert_to_dag(self, graph: nx.DiGraph) -> nx.DiGraph:
        """Convert cyclic graph to DAG by removing cycle-creating edges"""
        dag = graph.copy()
        
        if nx.is_directed_acyclic_graph(dag):
            return dag
        
        edges_removed = []
        while not nx.is_directed_acyclic_graph(dag):
            try:
                cycle = nx.find_cycle(dag, orientation='original')
                edge_to_remove = cycle[-1][:2]
                dag.remove_edge(*edge_to_remove)
                edges_removed.append(edge_to_remove)
            except nx.NetworkXNoCycle:
                break
        
        if edges_removed:
            print(f"  Removed {len(edges_removed)} edges to break cycles: {edges_removed[:3]}{'...' if len(edges_removed) > 3 else ''}")
        
        return dag
    
    def detect_anomalies(self, method: str = 'zscore', threshold: float = 2.5) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Detect anomalous periods using statistical methods.
        
        Args:
            method: 'zscore', 'iqr', or 'percentile'
            threshold: Threshold for anomaly detection
        """
        if self.latency_data is None:
            raise ValueError("Load data first")
        
        print(f"\n Detecting anomalies using {method} (threshold={threshold})...")
        
        anomaly_scores = pd.DataFrame(index=self.latency_data.index)
        
        for service in self.latency_data.columns:
            values = self.latency_data[service]
            
            if method == 'zscore':
                mean, std = values.mean(), values.std()
                if std > 0:
                    anomaly_scores[service] = np.abs((values - mean) / std)
                else:
                    anomaly_scores[service] = 0
                    
            elif method == 'iqr':
                q1, q3 = values.quantile(0.25), values.quantile(0.75)
                iqr = q3 - q1
                lower, upper = q1 - threshold * iqr, q3 + threshold * iqr
                anomaly_scores[service] = ((values < lower) | (values > upper)).astype(float) * 10
                
            elif method == 'percentile':
                percentile_threshold = values.quantile(1 - threshold / 100)
                anomaly_scores[service] = (values > percentile_threshold).astype(float) * 10
        
        if method == 'zscore':
            is_anomalous = (anomaly_scores > threshold).any(axis=1)
        else:
            is_anomalous = (anomaly_scores > 0).any(axis=1)
        
        self.normal_data = self.latency_data[~is_anomalous]
        self.anomalous_data = self.latency_data[is_anomalous]
        
        self.anomaly_info = {
            'method': method,
            'threshold': threshold,
            'total_samples': len(self.latency_data),
            'normal_samples': len(self.normal_data),
            'anomalous_samples': len(self.anomalous_data),
            'affected_services': {}
        }
        
        for service in self.latency_data.columns:
            normal_mean = self.normal_data[service].mean()
            anomalous_mean = self.anomalous_data[service].mean() if len(self.anomalous_data) > 0 else normal_mean
            increase_pct = ((anomalous_mean - normal_mean) / normal_mean) * 100 if normal_mean > 0 else 0
            
            self.anomaly_info['affected_services'][service] = {
                'normal_mean': normal_mean,
                'anomalous_mean': anomalous_mean,
                'increase_pct': increase_pct
            }
        
        print(f"  Normal: {len(self.normal_data)} samples ({len(self.normal_data)/len(self.latency_data)*100:.1f}%)")
        print(f"  Anomalous: {len(self.anomalous_data)} samples ({len(self.anomalous_data)/len(self.latency_data)*100:.1f}%)")
        
        if len(self.anomalous_data) == 0:
            print("   No anomalies detected - consider lowering threshold")
        elif len(self.anomalous_data) < 10:
            print("   Very few anomalies - results may be unreliable")
        
        return self.normal_data, self.anomalous_data
    
    def build_causal_model(self, use_auto: bool = True) -> gcm.ProbabilisticCausalModel:
        """Build probabilistic causal model"""
        if self.causal_graph is None or self.normal_data is None:
            raise ValueError("Build graph and detect anomalies first")
        
        model = gcm.ProbabilisticCausalModel(self.causal_graph)
        
        if use_auto:
            print("\n Building causal model with auto-assigned mechanisms...")
            gcm.auto.assign_causal_mechanisms(model, self.normal_data, quality=gcm.auto.AssignmentQuality.GOOD)
        else:
            print("\n Building causal model with manual mechanisms...")
            for node in self.causal_graph.nodes():
                if self.causal_graph.in_degree(node) == 0:
                    model.set_causal_mechanism(node, gcm.EmpiricalDistribution())
                else:
                    model.set_causal_mechanism(node, gcm.AdditiveNoiseModel(gcm.ml.create_linear_regressor()))
        
        self.causal_model = model
        return model
    
    def fit_model(self):
        """Fit causal model on normal data"""
        if self.causal_model is None or self.normal_data is None:
            raise ValueError("Build model first")
        
        gcm.fit(self.causal_model, self.normal_data)
        print(" Fitted causal model on normal data")
    
    def evaluate_model(self) -> Dict:
        """Evaluate model fit quality"""
        if self.causal_model is None or self.normal_data is None:
            raise ValueError("Fit model first")
        
        results = {}
        print("\n" + "=" * 70)
        print("MODEL EVALUATION (on normal data)")
        print("=" * 70)
        
        for node in self.causal_graph.nodes():
            if self.causal_graph.in_degree(node) == 0:
                print(f"{node:30s} [ROOT NODE]")
                results[node] = {'type': 'root'}
            else:
                parents = list(self.causal_graph.predecessors(node))
                X = self.normal_data[parents].values
                y_true = self.normal_data[node].values
                
                mechanism = self.causal_model.causal_mechanism(node)
                
                try:
                    if hasattr(mechanism, 'prediction_model'):
                        y_pred = mechanism.prediction_model.predict(X)
                    elif hasattr(mechanism, 'predict'):
                        y_pred = mechanism.predict(X)
                    else:
                        continue
                    
                    r2 = r2_score(y_true, y_pred)
                    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
                    nrmse = (rmse / y_true.mean()) * 100 if y_true.mean() > 0 else 0
                    
                    results[node] = {'type': 'dependent', 'parents': parents, 'r2': r2, 'rmse': rmse, 'nrmse': nrmse}
                    
                    quality = "EXCELLENT" if r2 > 0.9 else "GOOD" if r2 > 0.7 else "FAIR" if r2 > 0.5 else "POOR"
                    print(f"{node:30s} R²={r2:6.4f} RMSE={rmse:8.4f} [{quality}]")
                    
                except Exception as e:
                    print(f"{node:30s} [Eval failed: {str(e)[:30]}]")
        
        print("=" * 70)
        
        dependent_nodes = [n for n, r in results.items() if r.get('type') == 'dependent']
        if dependent_nodes:
            avg_r2 = np.mean([results[n]['r2'] for n in dependent_nodes])
            print(f"Average R² for dependent nodes: {avg_r2:.4f}\n")
        
        return results
    
    def identify_most_affected_services(self, top_n: int = 3) -> List[Tuple[str, float]]:
        """Identify services most affected by anomalies"""
        if self.anomaly_info is None:
            raise ValueError("Run anomaly detection first")
        
        affected = self.anomaly_info['affected_services']
        sorted_services = sorted(affected.items(), key=lambda x: x[1]['increase_pct'], reverse=True)
        
        print(f"TOP {top_n} MOST AFFECTED SERVICES")
        print(f"{'-' * 70}")
        for i, (service, info) in enumerate(sorted_services[:top_n], 1):
            print(f"{i}. {service:30s} +{info['increase_pct']:7.1f}% ({info['normal_mean']:.2f} → {info['anomalous_mean']:.2f} ms)")
        print()
        
        return [(s, info['increase_pct']) for s, info in sorted_services[:top_n]]
    
    def perform_rca(self, target_node: Optional[str] = None) -> Dict:
        """Perform root cause analysis"""
        if self.causal_model is None or self.anomalous_data is None:
            raise ValueError("Fit model and detect anomalies first")
        
        if target_node is None:
            affected_services = self.identify_most_affected_services(top_n=1)
            target_node = affected_services[0][0] if affected_services else list(self.causal_graph.nodes())[0]
        
        print(f"Performing RCA for: {target_node} ({len(self.anomalous_data)} anomalous samples)\n")
        
        try:
            attributions = gcm.attribute_anomalies(
                self.causal_model,
                target_node=target_node,
                anomaly_samples=self.anomalous_data
            )
        except Exception as e:
            print(f" RCA failed: {str(e)}")
            return None
        
        if isinstance(attributions, dict):
            median_attributions = {k: np.median(v) for k, v in attributions.items()}
        else:
            median_attributions = attributions.median().to_dict()
        
        sorted_causes = sorted(median_attributions.items(), key=lambda x: abs(x[1]), reverse=True)
        
        print(f"{'=' * 70}")
        print(f"ROOT CAUSE ANALYSIS - Target: {target_node}")
        print(f"{'=' * 70}")
        print(f"{'Rank':<6} {'Service':<30} {'Contribution':<15} {'Percent'}")
        print(f"{'-' * 70}")
        
        total_contribution = sum(abs(v) for v in median_attributions.values())
        
        for i, (node, contribution) in enumerate(sorted_causes, 1):
            pct = (abs(contribution) / total_contribution * 100) if total_contribution > 0 else 0
            bar = "█" * int(pct / 2)
            print(f"{i:<6} {node:<30} {contribution:>8.4f} ms    {pct:>5.1f}% {bar}")
        
        print(f"{'=' * 70}\n")
        
        return {
            'target': target_node,
            'attributions': median_attributions,
            'sorted_causes': sorted_causes,
            'raw_attributions': attributions
        }
    
    def run_full_pipeline(
        self,
        anomaly_method: str = 'zscore',
        anomaly_threshold: float = 2.5,
        use_auto_mechanisms: bool = True,
        analyze_top_n: int = 1
    ) -> Dict:
        """
        Run complete RCA pipeline.
        
        Args:
            anomaly_method: 'zscore', 'iqr', or 'percentile'
            anomaly_threshold: Threshold for anomaly detection
            use_auto_mechanisms: Use automatic causal mechanism assignment (recommended)
            analyze_top_n: Number of top affected services to analyze
        """
        print("=" * 70)
        print("CAUSAL INFERENCE ROOT CAUSE ANALYSIS")
        print("=" * 70)
        print()
        
        self.load_latency_data()
        
        try:
            service_call_graph = self.load_service_graph()
        except Exception as e:
            print(f" Could not load service graph: {e}")
            service_call_graph = None
        
        self.build_latency_causal_graph(service_call_graph)
        self.detect_anomalies(method=anomaly_method, threshold=anomaly_threshold)
        
        if len(self.anomalous_data) == 0:
            print("\n No anomalies detected. Cannot perform RCA.")
            return None
        
        self.build_causal_model(use_auto=use_auto_mechanisms)
        self.fit_model()
        self.evaluate_model()
        
        print("\n" + "=" * 70)
        top_affected = self.identify_most_affected_services(top_n=analyze_top_n)
        print("=" * 70 + "\n")
        
        results = {}
        for service, severity in top_affected:
            result = self.perform_rca(target_node=service)
            if result:
                results[service] = result
        
        print("=" * 70)
        print(" RCA PIPELINE COMPLETE")
        print("=" * 70)
        
        return results


if __name__ == "__main__":
    latency_folder = r"C:/Users/jurat.shayidin/aiops/causal_ml_enhanced/latency_data"
    graph_path = r"C:/Users/jurat.shayidin/aiops/causal_ml_enhanced/graph_data.json"
    
    rca = CausalRCA(latency_folder, graph_path)
    
    results = rca.run_full_pipeline(
        anomaly_method='zscore',
        anomaly_threshold=2.5,
        use_auto_mechanisms=True,
        analyze_top_n=1
    )
    
    # Alternative: analyze specific service
    # results = rca.perform_rca(target_node='ppapi')

