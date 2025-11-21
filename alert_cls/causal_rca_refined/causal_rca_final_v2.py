import pandas as pd
import numpy as np
import json
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path
from dowhy import gcm
from sklearn.metrics import r2_score, mean_squared_error
from typing import Any, Dict, List, Tuple, Optional


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
        folder = Path(self.latency_folder)
        csv_files = list[Path](folder.glob("*.csv"))
        
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
        print(f" Loaded {result.shape[0]} samples, {result.shape[1]} services: {list(result.columns)}")
        return result
    
    def load_service_graph(self) -> nx.DiGraph:
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
        
        print(f" Loaded service graph: {len(graph.nodes())} nodes, {len(graph.edges())} edges")
        return graph
    
    def build_latency_causal_graph(self, service_call_graph: Optional[nx.DiGraph] = None) -> nx.DiGraph:
        """
        Build causal graph where edges represent latency causation.
        Critical: If A calls B, then B's latency CAUSES A's latency (reversed direction).
        """
        if self.latency_data is None:
            raise ValueError("Load latency data first")
        
        latency_services = set(self.latency_data.columns)
        
        if service_call_graph is not None:
            graph_services = set[Any](service_call_graph.nodes())
            common_services = latency_services.intersection(graph_services)
            
            if len(common_services) > 1:
                print(f" Matched {len(common_services)}/{len(latency_services)} services")
                subgraph = service_call_graph.subgraph(common_services).copy()
                latency_graph = subgraph.reverse()
                self.latency_data = self.latency_data[list(common_services)]
            else:
                print(f"Only {len(common_services)} matched, building from correlations")
                latency_graph = self._build_graph_from_correlations()
        else:
            latency_graph = self._build_graph_from_correlations()
        
        latency_graph = self._convert_to_dag(latency_graph)
        self.causal_graph = latency_graph
        print(f" Causal graph: {len(latency_graph.nodes())} nodes, {len(latency_graph.edges())} edges")
        return latency_graph
    
    def _build_graph_from_correlations(self) -> nx.DiGraph:
        services = list[Any](self.latency_data.columns)
        graph = nx.DiGraph()
        graph.add_nodes_from(services)
        
        corr_matrix = self.latency_data.corr()
        threshold = 0.3
        
        for i, service1 in enumerate[Any](services):
            for j, service2 in enumerate[Any](services):
                if i != j and abs(corr_matrix.iloc[i, j]) > threshold:
                    if self.latency_data[service1].var() > self.latency_data[service2].var():
                        graph.add_edge(service1, service2)
        
        print(f"  Created {len(graph.edges())} edges from correlations")
        return graph
    
    def _convert_to_dag(self, graph: nx.DiGraph) -> nx.DiGraph:
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
            print(f"  Removed {len(edges_removed)} edges to break cycles")
        return dag
    
    def detect_anomalies(self, method: str = 'zscore', threshold: float = 2.5) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if self.latency_data is None:
            raise ValueError("Load data first")
        
        print(f" Detecting anomalies ({method}, threshold={threshold})")
        
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
        
        pct_normal = len(self.normal_data) / len(self.latency_data) * 100
        pct_anomalous = len(self.anomalous_data) / len(self.latency_data) * 100
        print(f"  Normal: {len(self.normal_data)} ({pct_normal:.1f}%), Anomalous: {len(self.anomalous_data)} ({pct_anomalous:.1f}%)")
        
        if len(self.anomalous_data) < 10:
            print(f"  Very few anomalies detected")
        
        return self.normal_data, self.anomalous_data
    
    def build_and_fit_model(self, use_auto: bool = True):
        if self.causal_graph is None or self.normal_data is None:
            raise ValueError("Build graph and detect anomalies first")
        
        print(f" Building causal model ({'auto' if use_auto else 'manual'} mechanisms)")
        
        self.causal_model = gcm.ProbabilisticCausalModel(self.causal_graph)
        
        if use_auto:
            gcm.auto.assign_causal_mechanisms(self.causal_model, self.normal_data, quality=gcm.auto.AssignmentQuality.GOOD)
        else:
            for node in self.causal_graph.nodes():
                if self.causal_graph.in_degree(node) == 0:
                    self.causal_model.set_causal_mechanism(node, gcm.EmpiricalDistribution())
                else:
                    self.causal_model.set_causal_mechanism(node, gcm.AdditiveNoiseModel(gcm.ml.create_linear_regressor()))
        
        gcm.fit(self.causal_model, self.normal_data)
        print(f" Fitted model on {len(self.normal_data)} normal samples")
    
    def evaluate_model(self) -> Dict:
        if self.causal_model is None or self.normal_data is None:
            raise ValueError("Fit model first")
        
        results = {}
        print("\nModel Quality:")
        
        for node in self.causal_graph.nodes():
            if self.causal_graph.in_degree(node) == 0:
                print(f"  {node}: ROOT NODE")
                results[node] = {'type': 'root'}
            else:
                parents = list[Any](self.causal_graph.predecessors(node))
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
                    results[node] = {'r2': r2, 'rmse': rmse, 'parents': parents}
                    
                    quality = "EXCELLENT" if r2 > 0.9 else "GOOD" if r2 > 0.7 else "FAIR" if r2 > 0.5 else "POOR"
                    print(f"  {node}: R²={r2:.3f} [{quality}]")
                except Exception as e:
                    print(f"  {node}: Eval failed")
        
        return results
    
    def attribute_anomalies(self, target_node: str) -> Dict:
        """
        Attribute anomalies to root causes (anomaly attribution for specific samples)
        """
        if self.causal_model is None or self.anomalous_data is None:
            raise ValueError("Fit model and detect anomalies first")
        
        print(f"\n Anomaly Attribution for {target_node} ({len(self.anomalous_data)} samples)")
        
        attributions = gcm.attribute_anomalies(
            self.causal_model,
            target_node=target_node,
            anomaly_samples=self.anomalous_data
        )
        
        if isinstance(attributions, dict):
            median_attrs = {k: np.median(v) for k, v in attributions.items()}
        else:
            median_attrs = attributions.median().to_dict()
        
        return {
            'target': target_node,
            'attributions': median_attrs
        }
    
    def distribution_change_analysis(self, target_node: str) -> Dict:
        """
        Analyze distribution change between normal and anomalous periods (distribution change)
        """
        if self.causal_model is None or self.normal_data is None or self.anomalous_data is None:
            raise ValueError("Fit model and have both normal and anomalous data")
        
        print(f"\n Distribution Change Analysis for {target_node}")
        print(f"  Comparing normal ({len(self.normal_data)}) vs anomalous ({len(self.anomalous_data)}) periods")
        
        attributions = gcm.distribution_change(
            self.causal_model,
            self.normal_data,
            self.anomalous_data,
            target_node=target_node,
            difference_estimation_func=lambda x, y: np.mean(y) - np.mean(x)
        )
        
        return {
            'target': target_node,
            'attributions': attributions
        }
    
    def visualize_attributions(self, result: Dict, title: str = "Root Cause Analysis", save_path: Optional[str] = None):
        """
        Visualize attributions with bar plot (from DoWhy blog visualization)
        """
        attributions = result['attributions']
        
        sorted_items = sorted(attributions.items(), key=lambda x: abs(x[1]), reverse=True)
        services = [item[0] for item in sorted_items]
        values = [item[1] for item in sorted_items]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        colors = ['#d62728' if v < 0 else '#1f77b4' for v in values]
        bars = ax.barh(services, values, color=colors, alpha=0.7, edgecolor='black', linewidth=0.5)
        
        ax.axvline(x=0, color='black', linestyle='-', linewidth=1.5)
        ax.set_xlabel('Attribution Score (ms)', fontsize=13, fontweight='bold')
        ax.set_title(title, fontsize=15, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3, linestyle='--')
        
        for i, (service, value) in enumerate(zip(services, values)):
            ax.text(value, i, f' {value:.2f}', va='center', ha='left' if value >= 0 else 'right', fontsize=10)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"  Saved: {save_path}")
        
        plt.show()
        
        return fig
    
    def print_rca_results(self, result: Dict):
        """Print RCA results in table format"""
        attributions = result['attributions']
        sorted_items = sorted(attributions.items(), key=lambda x: abs(x[1]), reverse=True)
        
        print(f"\n{'='*70}")
        print(f"ROOT CAUSE ANALYSIS: {result['target']}")
        print(f"{'='*70}")
        print(f"{'Rank':<6} {'Service':<25} {'Contribution (ms)':<20} {'%'}")
        print(f"{'-'*70}")
        
        total = sum(abs(v) for v in attributions.values())
        for i, (service, value) in enumerate(sorted_items, 1):
            pct = (abs(value) / total * 100) if total > 0 else 0
            bar = '█' * int(pct / 2)
            print(f"{i:<6} {service:<25} {value:>8.4f}           {pct:>5.1f}% {bar}")
        
        print(f"{'='*70}\n")
    
    def run_complete_analysis(
        self,
        anomaly_method: str = 'percentile',
        anomaly_threshold: float = 5,
        use_auto_mechanisms: bool = True,
        target_node: Optional[str] = None,
        visualize: bool = True
    ) -> Dict:
        """
        Run complete RCA analysis with both anomaly attribution and distribution change
        """
        print("="*70)
        print("CAUSAL INFERENCE ROOT CAUSE ANALYSIS")
        print("="*70)
        
        self.load_latency_data()
        
        try:
            service_graph = self.load_service_graph()
        except:
            service_graph = None
        
        self.build_latency_causal_graph(service_graph)
        self.detect_anomalies(method=anomaly_method, threshold=anomaly_threshold)
        
        if len(self.anomalous_data) == 0:
            print("\n No anomalies detected")
            return None
        
        self.build_and_fit_model(use_auto=use_auto_mechanisms)
        self.evaluate_model()
        
        if target_node is None:
            affected = sorted(
                self.anomaly_info['affected_services'].items(),
                key=lambda x: x[1]['increase_pct'],
                reverse=True
            )
            target_node = affected[0][0]
            print(f"\n Auto-selected target: {target_node} (most affected)")
        
        results = {}
        
        # Method 1: Anomaly Attribution
        print(f"\n{'='*70}")
        print("METHOD 1: ANOMALY ATTRIBUTION")
        print(f"{'='*70}")
        anomaly_result = self.attribute_anomalies(target_node=target_node)
        results['anomaly_attribution'] = anomaly_result
        self.print_rca_results(anomaly_result)
        
        if visualize:
            self.visualize_attributions(
                anomaly_result,
                title=f"Anomaly Attribution: {target_node}",
                save_path="anomaly_attribution.png"
            )
        
        # Method 2: Distribution Change (comparing normal vs anomalous periods)
        print(f"\n{'='*70}")
        print("METHOD 2: DISTRIBUTION CHANGE ANALYSIS")
        print(f"{'='*70}")
        dist_change_result = self.distribution_change_analysis(target_node=target_node)
        results['distribution_change'] = dist_change_result
        self.print_rca_results(dist_change_result)
        
        if visualize:
            self.visualize_attributions(
                dist_change_result,
                title=f"Distribution Change: {target_node}",
                save_path="distribution_change.png"
            )
        
        print(f"\n{'='*70}")
        print(" ANALYSIS COMPLETE")
        print(f"{'='*70}")
        
        return results


if __name__ == "__main__":
    latency_folder = r"C:/Users/jurat.shayidin/aiops/causal_ml_enhanced/latency_data"
    graph_path = r"C:/Users/jurat.shayidin/aiops/causal_ml_enhanced/graph_data.json"
    
    rca = CausalRCA(latency_folder, graph_path)
    
    results = rca.run_complete_analysis(
        anomaly_method='percentile',
        anomaly_threshold=5,
        use_auto_mechanisms=True,
        visualize=True
    )

