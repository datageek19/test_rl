"""
Causal Inference Root Cause Analysis for Microservices Latency Issues

This module implements causal inference-based root cause analysis using DoWhy/GCM
to attribute elevated latencies in microservices to upstream services.

Reference: https://www.pywhy.org/dowhy/main/example_notebooks/gcm_rca_microservice_architecture.html
"""

import networkx as nx
import pandas as pd
import numpy as np
import json
import os
from typing import Dict, List, Tuple, Optional, Any
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

from dowhy import gcm
from dowhy.gcm import (
    ProbabilisticCausalModel,
    AdditiveNoiseModel,
    fit,
    attribute_anomalies
)

# Import regression models for continuous latency data
try:
    from dowhy.gcm.ml import create_linear_regressor, create_hist_gradient_boost_regressor
    HAS_REGRESSION_MODELS = True
except ImportError:
    HAS_REGRESSION_MODELS = False

# Try to import ShapleyConfig 
try:
    from dowhy.gcm import ShapleyConfig
except ImportError:
    try:
        from dowhy.gcm.shapley import ShapleyConfig
    except ImportError:
        ShapleyConfig = None

# Try to import EmpiricalDistribution - may be in different location depending on version
try:
    from dowhy.gcm import EmpiricalDistribution
except ImportError:
    try:
        from dowhy.gcm.stochastic_models import EmpiricalDistribution
    except ImportError:
        try:
            from dowhy.gcm.auto import EmpiricalDistribution
        except ImportError:
            # Fallback: use EmpiricalDistribution from gcm namespace
            EmpiricalDistribution = gcm.EmpiricalDistribution

from scipy.stats import truncexpon, halfnorm


class CausalRCAAnalyzer:
    """
    Causal Inference Root Cause Analyzer for Microservices Architecture
    
    This class implements:
    1. Building causal models from service dependency graphs
    2. Collecting and preparing latency data (normal + anomalous)
    3. Attributing elevated latencies to upstream services
    4. Identifying root causes of latency issues
    """
    
    def __init__(self, 
                 service_graph: nx.DiGraph,
                 output_dir: str = '.'):
        """
        Initialize the Causal RCA Analyzer
        
        Args:
            service_graph: NetworkX directed graph representing service dependencies
            output_dir: Directory for output files
            
        Note: Latency data will be inferred from alerts if not provided.
        """
        
        self.service_graph = service_graph
        self.output_dir = output_dir
        
        # Causal model components
        self.causal_model = None
        self.causal_graph = None
        self.gcm_model = None
        
        # Latency data
        self.normal_latency_data = None
        self.anomalous_latency_data = None
        self.latency_baseline = {}
        
        # Service mapping
        self.service_to_node = {}  # Map service names to causal graph nodes
        self.node_to_service = {}  # Reverse mapping
        
        # Results
        self.attribution_results = []
        self.root_causes = []
        
        # Configuration
        self.min_samples_for_analysis = 10
        self.anomaly_threshold_percentile = 95  # 95th percentile for anomaly detection
        
    def build_causal_graph_from_service_graph(self) -> nx.DiGraph:
        """
        Build a causal graph from the service dependency graph.
        
        In microservices, latency propagates downstream:
        - If Service A calls Service B, then B's latency affects A's latency
        - Causal direction: downstream services depend on upstream services
        
        Returns:
            Causal graph as NetworkX DiGraph
        """
        print("\n[1/6] Building causal graph from service dependencies...")
        
        # Create a copy of the service graph for causal modeling
        causal_graph = self.service_graph.copy()
        
        # Map service names to causal nodes (use service names as node IDs)
        for service_name in self.service_graph.nodes():
            self.service_to_node[service_name] = service_name
            self.node_to_service[service_name] = service_name
        
        # Validate graph structure
        if len(causal_graph.nodes()) == 0:
            raise ValueError("Service graph is empty. Cannot build causal model.")
        
        print(f"    Causal graph built: {len(causal_graph.nodes())} services, "
              f"{causal_graph.number_of_edges()} dependencies")
        
        # Identify root services (no incoming edges) and sink services (no outgoing edges)
        root_services = [n for n in causal_graph.nodes() if causal_graph.in_degree(n) == 0]
        sink_services = [n for n in causal_graph.nodes() if causal_graph.out_degree(n) == 0]
        
        print(f"    Root services (entry points): {len(root_services)}")
        print(f"    Sink services (end points): {len(sink_services)}")
        
        self.causal_graph = causal_graph
        return causal_graph
    
    def prepare_latency_data(self,
                            alerts_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Prepare latency data for causal analysis from alerts and service graph.
        
        This method INFERS latency from alert patterns:
        1. Services with alerts = elevated latency (inferred)
        2. Alert-free periods = normal baseline latency (estimated)
        3. Latency propagates downstream through service dependencies
        
        Args:
            alerts_data: DataFrame with alert information (from consolidation)
            
        Returns:
            Tuple of (normal_latency_data, anomalous_latency_data) DataFrames
        """
        print("\n[2/6] Preparing latency data from alerts...")
        print("    Inferring latency from alert patterns...")
        print("    Using alert frequency and severity as proxy for latency impact")
        
        normal_data, anomalous_data = self._infer_latency_from_alerts(alerts_data)
        
        return normal_data, anomalous_data
    
    def _infer_latency_from_alerts(self, alerts_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Infer latency data from alert patterns.
        
        Approach:
        1. Identify time windows with alerts (anomalous periods) vs without (normal)
        2. For services with alerts: infer elevated latency based on alert frequency/severity
        3. For services without alerts: estimate baseline latency from graph structure
        4. Propagate latency downstream through service dependencies
        
        Args:
            alerts_data: DataFrame with alert information
            
        Returns:
            Tuple of (normal_latency_data, anomalous_latency_data)
        """
        print("    Analyzing alert patterns to infer latency...")
        
        # Get services from graph
        services = list(self.service_graph.nodes())
        if len(services) == 0:
            raise ValueError("Service graph is empty. Cannot infer latency.")
        
        # Parse timestamps
        if 'starts_at' in alerts_data.columns:
            alerts_data['alert_time'] = pd.to_datetime(alerts_data['starts_at'], errors='coerce')
        elif 'start_timestamp' in alerts_data.columns:
            alerts_data['alert_time'] = pd.to_datetime(alerts_data['start_timestamp'], unit='s', errors='coerce')
        else:
            raise ValueError("Alerts data must have 'starts_at' or 'start_timestamp' column")
        
        # Filter to mapped services
        if 'graph_service' in alerts_data.columns:
            alerts_data = alerts_data[alerts_data['graph_service'].notna()].copy()
            alerts_data = alerts_data[alerts_data['graph_service'].isin(services)].copy()
        
        if len(alerts_data) == 0:
            raise ValueError("No alerts mapped to service graph. Cannot infer latency.")
        
        # Identify time windows
        time_windows = self._create_time_windows(alerts_data)
        
        # Separate normal and anomalous periods
        normal_windows = [w for w in time_windows if not w['has_alerts']]
        anomalous_windows = [w for w in time_windows if w['has_alerts']]
        
        print(f"    Time windows: {len(normal_windows)} normal, {len(anomalous_windows)} anomalous")
        
        # Estimate baseline latency for each service (from graph structure)
        baseline_latencies = self._estimate_baseline_latency(services)
        
        # Generate normal latency data
        normal_data = self._generate_latency_from_windows(
            services, normal_windows, baseline_latencies, is_anomalous=False
        )
        
        # Generate anomalous latency data (with elevated latency for services with alerts)
        anomalous_data = self._generate_latency_from_windows(
            services, anomalous_windows, baseline_latencies, 
            is_anomalous=True, alerts_data=alerts_data
        )
        
        print(f"    Generated {len(normal_data)} normal samples")
        print(f"    Generated {len(anomalous_data)} anomalous samples")
        
        return normal_data, anomalous_data
    
    def _create_time_windows(self, alerts_data: pd.DataFrame, window_size_minutes: int = 15) -> List[Dict]:
        """
        Create time windows from alert data.
        
        Args:
            alerts_data: DataFrame with alert timestamps
            window_size_minutes: Size of each time window in minutes
            
        Returns:
            List of time window dictionaries
        """
        if len(alerts_data) == 0:
            return []
        
        # Get time range
        min_time = alerts_data['alert_time'].min()
        max_time = alerts_data['alert_time'].max()
        
        # Create windows
        windows = []
        current_time = min_time
        
        while current_time < max_time:
            window_end = current_time + pd.Timedelta(minutes=window_size_minutes)
            
            # Check if this window has alerts
            window_alerts = alerts_data[
                (alerts_data['alert_time'] >= current_time) &
                (alerts_data['alert_time'] < window_end)
            ]
            
            windows.append({
                'start_time': current_time,
                'end_time': window_end,
                'has_alerts': len(window_alerts) > 0,
                'alert_count': len(window_alerts),
                'alerts': window_alerts
            })
            
            current_time = window_end
        
        return windows
    
    def _estimate_baseline_latency(self, services: List[str]) -> Dict[str, float]:
        """
        Estimate baseline latency for each service based on graph structure.
        
        Heuristic:
        - Root services (no upstream): base latency 0.1-0.3s
        - Services with dependencies: base latency increases with depth
        - Database services: higher base latency (0.2-0.5s)
        
        Args:
            services: List of service names
            
        Returns:
            Dictionary mapping service -> baseline latency
        """
        baseline = {}
        
        # Get topological depth (distance from root)
        # Handle cycles in the graph (common in microservices)
        try:
            topo_order = list(nx.topological_sort(self.service_graph))
        except (nx.NetworkXError, nx.NetworkXUnfeasible):
            # Graph has cycles - use alternative ordering
            # Use BFS-based ordering starting from root nodes
            topo_order = self._get_bfs_order(services)
        
        # Calculate depth for each service using BFS to handle cycles
        depth = {}
        visited = set()
        queue = []
        
        # Start with root services (no incoming edges)
        for service in services:
            if self.service_graph.in_degree(service) == 0:
                depth[service] = 0
                queue.append((service, 0))
                visited.add(service)
        
        # BFS to assign depths
        while queue:
            current, current_depth = queue.pop(0)
            
            for successor in self.service_graph.successors(current):
                if successor not in visited:
                    visited.add(successor)
                    depth[successor] = current_depth + 1
                    queue.append((successor, current_depth + 1))
        
        # For any unvisited nodes (part of cycles), assign minimum depth
        for service in services:
            if service not in depth:
                # Find minimum depth of predecessors, or use 1
                pred_depths = [depth.get(pred, 0) for pred in self.service_graph.predecessors(service)]
                depth[service] = max(pred_depths) + 1 if pred_depths else 1
        
        # Estimate baseline latency
        for service in services:
            # Base latency based on depth
            base = 0.1 + (depth[service] * 0.05)  # 0.1s base + 0.05s per depth level
            
            # Check if it's a database (heuristic: name contains 'db', 'database', etc.)
            service_lower = service.lower()
            if any(keyword in service_lower for keyword in ['db', 'database', 'mongo', 'postgres', 'mysql', 'redis']):
                base += 0.2  # Databases typically have higher latency
            
            # Add some randomness
            base += np.random.uniform(-0.05, 0.05)
            baseline[service] = max(0.05, base)  # Minimum 0.05s
        
        return baseline
    
    def _generate_latency_from_windows(self,
                                      services: List[str],
                                      windows: List[Dict],
                                      baseline_latencies: Dict[str, float],
                                      is_anomalous: bool = False,
                                      alerts_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Generate latency samples from time windows.
        
        Args:
            services: List of service names
            windows: List of time window dictionaries
            baseline_latencies: Baseline latency for each service
            is_anomalous: Whether these are anomalous windows
            alerts_data: Alert data for anomalous periods
            
        Returns:
            DataFrame with latency samples (one row per window)
        """
        if len(windows) == 0:
            # Create at least one sample with baseline latencies
            sample = {service: baseline_latencies.get(service, 0.1) for service in services}
            return pd.DataFrame([sample])
        
        samples = []
        
        for window in windows:
            sample = {}
            
            # Count alerts per service for this window
            service_alert_counts = {}
            service_alert_severity = {}
            
            if is_anomalous and alerts_data is not None:
                window_alerts = alerts_data[
                    (alerts_data['alert_time'] >= window['start_time']) &
                    (alerts_data['alert_time'] < window['end_time'])
                ]
                
                if 'graph_service' in window_alerts.columns:
                    for service in services:
                        service_alerts = window_alerts[window_alerts['graph_service'] == service]
                        service_alert_counts[service] = len(service_alerts)
                        
                        # Calculate severity score (if available)
                        if 'severity' in service_alerts.columns:
                            severity_map = {'critical': 4, 'high': 3, 'warning': 2, 'info': 1}
                            severity_scores = [severity_map.get(s.lower(), 1) for s in service_alerts['severity']]
                            service_alert_severity[service] = max(severity_scores) if severity_scores else 1
                        else:
                            service_alert_severity[service] = 2  # Default
                else:
                    service_alert_counts = {s: 0 for s in services}
                    service_alert_severity = {s: 1 for s in services}
            else:
                service_alert_counts = {s: 0 for s in services}
                service_alert_severity = {s: 1 for s in services}
            
            # Calculate latency for each service
            # Process in topological order to propagate downstream
            # Handle cycles in the graph
            try:
                topo_order = list(nx.topological_sort(self.service_graph))
            except (nx.NetworkXError, nx.NetworkXUnfeasible):
                # Graph has cycles - use BFS-based ordering
                topo_order = self._get_bfs_order(services)
            
            for service in topo_order:
                base_latency = baseline_latencies.get(service, 0.1)
                
                if is_anomalous:
                    # Elevate latency based on alerts
                    alert_count = service_alert_counts.get(service, 0)
                    severity = service_alert_severity.get(service, 1)
                    
                    if alert_count > 0:
                        # Latency increase: base * (1 + alert_count * severity_factor)
                        severity_factor = severity * 0.3  # 0.3x per severity level
                        latency_multiplier = 1 + (alert_count * severity_factor)
                        # Cap at 5x baseline
                        latency_multiplier = min(latency_multiplier, 5.0)
                        service_latency = base_latency * latency_multiplier
                    else:
                        # No alerts, but might be affected by upstream
                        service_latency = base_latency
                else:
                    # Normal operation: baseline + small noise
                    noise = np.random.uniform(-0.02, 0.02)
                    service_latency = base_latency + noise
                
                # Add downstream service latencies (propagation)
                for successor in self.service_graph.successors(service):
                    if successor in sample:
                        service_latency += sample[successor] * 0.3  # 30% of downstream latency affects upstream
                
                sample[service] = max(0.05, service_latency)  # Minimum 0.05s
            
            samples.append(sample)
        
        return pd.DataFrame(samples)
    
    def _get_bfs_order(self, services: List[str]) -> List[str]:
        """
        Get BFS-based ordering of services starting from root nodes.
        This works even when the graph has cycles.
        
        Args:
            services: List of service names
            
        Returns:
            List of services in BFS order
        """
        visited = set()
        order = []
        queue = []
        
        # Start with root services (no incoming edges)
        root_services = [s for s in services if self.service_graph.in_degree(s) == 0]
        
        if not root_services:
            # No root services (all nodes in cycles), start with any node
            root_services = [services[0]] if services else []
        
        # Initialize queue with root services
        for root in root_services:
            if root not in visited:
                queue.append(root)
                visited.add(root)
        
        # BFS traversal
        while queue:
            current = queue.pop(0)
            order.append(current)
            
            # Add successors to queue
            for successor in self.service_graph.successors(current):
                if successor not in visited:
                    visited.add(successor)
                    queue.append(successor)
        
        # Add any remaining unvisited services (isolated nodes or cycle components)
        for service in services:
            if service not in visited:
                order.append(service)
        
        return order
    
    def _generate_latency_samples(self,
                                   services: List[str],
                                   n_samples: int,
                                   is_anomalous: bool = False) -> pd.DataFrame:
        """
        Generate latency samples for services based on dependency structure.
        
        Latency model:
        - Intrinsic latency: Service's own processing time
        - Observed latency: Intrinsic + sum of downstream service latencies
        """
        # Get topological order (process services in dependency order)
        # Handle cycles in the graph
        try:
            topo_order = list(nx.topological_sort(self.service_graph))
        except (nx.NetworkXError, nx.NetworkXUnfeasible):
            # Graph has cycles - use BFS-based ordering
            topo_order = self._get_bfs_order(services)
        
        # Initialize intrinsic latencies
        intrinsic_latencies = {}
        
        for service in services:
            if is_anomalous and np.random.random() < 0.2:  # 20% chance of anomaly
                # Elevated intrinsic latency (2-5x normal)
                multiplier = np.random.uniform(2.0, 5.0)
                base_latency = np.random.uniform(0.1, 0.5)
                intrinsic_latencies[service] = base_latency * multiplier
            else:
                # Normal intrinsic latency
                intrinsic_latencies[service] = np.random.uniform(0.05, 0.3)
        
        # Generate samples
        samples = []
        for _ in range(n_samples):
            sample = {}
            
            # Generate intrinsic latencies for this sample
            for service in services:
                base = intrinsic_latencies[service]
                # Add noise
                noise = np.random.exponential(scale=base * 0.2)
                sample[f'{service}_intrinsic'] = base + noise
            
            # Calculate observed latencies (intrinsic + downstream dependencies)
            for service in topo_order:
                observed_latency = sample[f'{service}_intrinsic']
                
                # Add latencies from downstream services (services this service calls)
                for successor in self.service_graph.successors(service):
                    if f'{successor}_observed' in sample:
                        observed_latency += sample[f'{successor}_observed']
                
                sample[service] = observed_latency
            
            samples.append(sample)
        
        # Convert to DataFrame (only observed latencies, not intrinsic)
        df = pd.DataFrame(samples)
        service_cols = [col for col in df.columns if col not in [c for c in df.columns if '_intrinsic' in c]]
        df = df[service_cols]
        
        return df
    
    def fit_causal_model(self, normal_latency_data: pd.DataFrame):
        """
        Fit a Graphical Causal Model (GCM) to the normal latency data.
        
        This models the causal relationships between service latencies.
        
        Args:
            normal_latency_data: DataFrame with normal operation latencies
        """
        print("\n[3/6] Fitting causal model to normal latency data...")
        
        if len(normal_latency_data) < self.min_samples_for_analysis:
            raise ValueError(f"Need at least {self.min_samples_for_analysis} samples for causal analysis. "
                           f"Got {len(normal_latency_data)}")
        
        # Ensure all graph services are in the data
        graph_services = set(self.service_graph.nodes())
        data_services = set(normal_latency_data.columns)
        
        # Add missing services with zero latency
        for service in graph_services:
            if service not in data_services:
                normal_latency_data[service] = 0.0
        
        # Filter to only services in graph
        normal_latency_data = normal_latency_data[[s for s in normal_latency_data.columns if s in graph_services]]
        
        # Build GCM model
        self.gcm_model = ProbabilisticCausalModel(self.causal_graph)
        
        # Assign causal mechanisms to each node
        for node in self.causal_graph.nodes():
            # Get parent nodes (upstream dependencies)
            parents = list(self.causal_graph.predecessors(node))
            
            if len(parents) == 0:
                # Root node: no parents, use empirical distribution
                self.gcm_model.set_causal_mechanism(
                    node,
                    EmpiricalDistribution()
                )
            else:
                if HAS_REGRESSION_MODELS:
                    prediction_model = create_hist_gradient_boost_regressor()
                else:
                    prediction_model = None
                
                self.gcm_model.set_causal_mechanism(
                    node,
                    AdditiveNoiseModel(prediction_model=prediction_model)
                )
        
        # Fit the model to normal data
        print("    Fitting GCM mechanisms...")
        fit(self.gcm_model, normal_latency_data)
        
        print(f"    Causal model fitted to {len(normal_latency_data)} normal samples")
        print(f"    Model covers {len(self.causal_graph.nodes())} services")
        
        # Store baseline statistics
        for service in normal_latency_data.columns:
            self.latency_baseline[service] = {
                'mean': normal_latency_data[service].mean(),
                'std': normal_latency_data[service].std(),
                'p95': normal_latency_data[service].quantile(0.95),
                'p99': normal_latency_data[service].quantile(0.99)
            }
        
        self.normal_latency_data = normal_latency_data
    
    def attribute_latency_anomalies(self,
                                   anomalous_latency_data: pd.DataFrame,
                                   target_service: Optional[str] = None,
                                   alerts_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Attribute elevated latency at target service to upstream services.
        
        This uses GCM-based anomaly attribution to identify which upstream
        services are causing the elevated latency.
        
        Args:
            anomalous_latency_data: DataFrame with anomalous latency measurements
            target_service: Service with elevated latency (if None, uses all services)
            
        Returns:
            DataFrame with attribution results
        """
        print("\n[4/6] Attributing latency anomalies to upstream services...")
        
        if self.gcm_model is None:
            raise ValueError("Causal model not fitted. Call fit_causal_model() first.")
        
        if len(anomalous_latency_data) == 0:
            raise ValueError("No anomalous latency data provided.")
        
        # Ensure all graph services are in the data
        graph_services = set(self.service_graph.nodes())
        data_services = set(anomalous_latency_data.columns)
        
        for service in graph_services:
            if service not in data_services:
                anomalous_latency_data[service] = 0.0
        
        # Filter to only services in graph
        anomalous_latency_data = anomalous_latency_data[
            [s for s in anomalous_latency_data.columns if s in graph_services]
        ]
        
        # Determine target services
        if target_service:
            if target_service not in anomalous_latency_data.columns:
                raise ValueError(f"Target service '{target_service}' not in data.")
            target_services = [target_service]
        else:
            # Find services with elevated latency
            target_services = self._identify_elevated_latency_services(anomalous_latency_data, alerts_data)
        
        print(f"    Analyzing {len(target_services)} target service(s) with elevated latency")
        
        # Perform attribution for each target service
        attribution_results = []
        
        for target in target_services:
            print(f"    Attributing latency for {target}...")
            
            # Get upstream services (potential causes)
            upstream_services = list(nx.ancestors(self.causal_graph, target))
            
            if len(upstream_services) == 0:
                print(f"      ⚠ {target} has no upstream dependencies (root service)")
                continue
            
            # Perform anomaly attribution using Shapley values
            try:
                # Prepare attribution parameters - check function signature
                # attribute_anomalies(causal_model, target_node, anomaly_samples, ...)
                attr_params = {
                    'causal_model': self.gcm_model,
                    'target_node': target,
                    'anomaly_samples': anomalous_latency_data
                }
                
                # Add ShapleyConfig if available
                if ShapleyConfig is not None:
                    attr_params['shapley_config'] = ShapleyConfig(n_jobs=1)
                
                attributions = attribute_anomalies(**attr_params)
                
                # Extract attribution scores
                # attributions is Dict[str, np.ndarray] - each value is an array of scores per sample
                # We aggregate by taking the mean (or sum) of scores across all samples
                for upstream_service, score_array in attributions.items():
                    if upstream_service in upstream_services:
                        # Aggregate scores: use mean of absolute values (or sum)
                        aggregated_score = float(np.mean(np.abs(score_array)))
                        
                        attribution_results.append({
                            'target_service': target,
                            'upstream_service': upstream_service,
                            'attribution_score': aggregated_score,
                            'baseline_mean': self.latency_baseline.get(upstream_service, {}).get('mean', 0),
                            'baseline_p95': self.latency_baseline.get(upstream_service, {}).get('p95', 0),
                            'anomalous_mean': anomalous_latency_data[upstream_service].mean() if upstream_service in anomalous_latency_data.columns else 0,
                            'latency_increase': self._calculate_latency_increase(
                                upstream_service, anomalous_latency_data
                            )
                        })
            except Exception as e:
                print(f"      ⚠ Attribution failed for {target}: {e}")
                continue
        
        if len(attribution_results) == 0:
            print("    ⚠ No attribution results generated")
            return pd.DataFrame()
        
        # Convert to DataFrame and sort by attribution score
        attribution_df = pd.DataFrame(attribution_results)
        attribution_df = attribution_df.sort_values('attribution_score', ascending=False)
        
        self.attribution_results = attribution_df
        
        print(f"    Generated attributions for {len(attribution_df)} upstream services")
        
        return attribution_df
    
    def _identify_elevated_latency_services(self, anomalous_data: pd.DataFrame, 
                                            alerts_data: Optional[pd.DataFrame] = None) -> List[str]:
        """Identify services with significantly elevated latency."""
        elevated_services = []
        
        # First, try statistical method
        for service in anomalous_data.columns:
            if service not in self.latency_baseline:
                continue
            
            baseline_p95 = self.latency_baseline[service]['p95']
            anomalous_mean = anomalous_data[service].mean()
            
            # Service is elevated if anomalous mean > baseline 95th percentile
            if anomalous_mean > baseline_p95 * 1.2:  # 20% above baseline
                elevated_services.append(service)
        
        # If no services found statistically, use services with alerts as fallback
        if len(elevated_services) == 0 and alerts_data is not None:
            print("      ⚠ No services found with statistical elevation, using services with alerts...")
            # Get services that have alerts in the anomalous windows
            if 'graph_service' in alerts_data.columns:
                services_with_alerts = alerts_data['graph_service'].dropna().unique().tolist()
                # Filter to services that exist in the graph and have data
                elevated_services = [s for s in services_with_alerts 
                                   if s in anomalous_data.columns and s in self.service_graph]
                # Limit to top services by alert count
                if len(elevated_services) > 10:
                    alert_counts = alerts_data['graph_service'].value_counts()
                    elevated_services = [s for s in elevated_services 
                                       if s in alert_counts.index][:10]
                    elevated_services = sorted(elevated_services, 
                                             key=lambda x: alert_counts.get(x, 0), 
                                             reverse=True)
        
        # If still no services, use services with highest anomalous latency
        if len(elevated_services) == 0:
            print("      ⚠ No services found with alerts, using top services by anomalous latency...")
            service_means = anomalous_data.mean().sort_values(ascending=False)
            # Take top 5 services with non-zero latency
            elevated_services = [s for s in service_means.index 
                               if service_means[s] > 0][:5]
        
        return elevated_services
    
    def _calculate_latency_increase(self, service: str, anomalous_data: pd.DataFrame) -> float:
        """Calculate latency increase for a service."""
        if service not in self.latency_baseline or service not in anomalous_data.columns:
            return 0.0
        
        baseline_mean = self.latency_baseline[service]['mean']
        anomalous_mean = anomalous_data[service].mean()
        
        if baseline_mean > 0:
            return (anomalous_mean - baseline_mean) / baseline_mean * 100  # Percentage increase
        else:
            return anomalous_mean if anomalous_mean > 0 else 0.0
    
    def identify_root_causes(self, attribution_df: pd.DataFrame, top_k: int = 10) -> pd.DataFrame:
        """
        Identify root causes from attribution results.
        
        Root causes are upstream services with:
        1. High attribution scores
        2. Significant latency increases
        3. No upstream dependencies (or minimal dependencies)
        
        Args:
            attribution_df: DataFrame with attribution results
            top_k: Number of top root causes to return
            
        Returns:
            DataFrame with root cause rankings
        """
        print("\n[5/6] Identifying root causes...")
        
        if len(attribution_df) == 0:
            print("    ⚠ No attribution data available")
            return pd.DataFrame()
        
        root_causes = []
        
        for _, row in attribution_df.iterrows():
            upstream_service = row['upstream_service']
            target_service = row['target_service']
            
            # Calculate root cause score
            attribution_score = row['attribution_score']
            latency_increase = row['latency_increase']
            
            # Check if this is a root service (no upstream dependencies)
            is_root_service = self.causal_graph.in_degree(upstream_service) == 0
            
            # Calculate root cause score
            # Higher score = more likely to be root cause
            root_cause_score = (
                attribution_score * 0.5 +  # Attribution importance
                min(latency_increase / 100, 1.0) * 100 * 0.3 +  # Latency increase (capped at 100%)
                (100 if is_root_service else 0) * 0.2  # Bonus for root services
            )
            
            # Get upstream dependencies count
            upstream_count = self.causal_graph.in_degree(upstream_service)
            downstream_count = self.causal_graph.out_degree(upstream_service)
            
            root_causes.append({
                'root_cause_service': upstream_service,
                'affected_target': target_service,
                'root_cause_score': root_cause_score,
                'attribution_score': attribution_score,
                'latency_increase_pct': latency_increase,
                'is_root_service': is_root_service,
                'upstream_dependencies': upstream_count,
                'downstream_dependencies': downstream_count,
                'baseline_latency': row['baseline_mean'],
                'anomalous_latency': row['anomalous_mean']
            })
        
        # Convert to DataFrame and sort
        root_causes_df = pd.DataFrame(root_causes)
        root_causes_df = root_causes_df.sort_values('root_cause_score', ascending=False)
        
        # Remove duplicates (same root cause affecting multiple targets)
        root_causes_df = root_causes_df.drop_duplicates(subset=['root_cause_service'], keep='first')
        
        # Take top K
        root_causes_df = root_causes_df.head(top_k)
        
        # Add rank
        root_causes_df['rank'] = range(1, len(root_causes_df) + 1)
        
        self.root_causes = root_causes_df
        
        print(f"    Identified {len(root_causes_df)} root causes")
        if len(root_causes_df) > 0:
            print(f"    Top root cause: {root_causes_df.iloc[0]['root_cause_service']} "
                  f"(score: {root_causes_df.iloc[0]['root_cause_score']:.2f})")
        
        return root_causes_df
    
    def export_results(self, attribution_df: pd.DataFrame, root_causes_df: pd.DataFrame):
        """Export RCA results to CSV files."""
        print("\n[6/6] Exporting results...")
        
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Export attribution results
        if len(attribution_df) > 0:
            attr_path = os.path.join(self.output_dir, 'latency_attributions.csv')
            attribution_df.to_csv(attr_path, index=False)
            print(f"    Attribution results: {attr_path}")
        
        # Export root causes
        if len(root_causes_df) > 0:
            rca_path = os.path.join(self.output_dir, 'root_causes.csv')
            root_causes_df.to_csv(rca_path, index=False)
            print(f"    Root causes: {rca_path}")
        
        # Export baseline statistics
        baseline_df = pd.DataFrame(self.latency_baseline).T
        baseline_df.index.name = 'service'
        baseline_path = os.path.join(self.output_dir, 'latency_baseline_stats.csv')
        baseline_df.to_csv(baseline_path)
        print(f"    Baseline statistics: {baseline_path}")
        
        print(f"\n    All results exported to: {self.output_dir}")
    
    def run_complete_analysis(self,
                             alerts_data: pd.DataFrame,
                             target_service: Optional[str] = None,
                             top_k_root_causes: int = 10) -> Dict[str, pd.DataFrame]:
        """
        Run complete causal RCA analysis pipeline.
        
        Args:
            alerts_data: DataFrame with alert information
            target_service: Specific service to analyze (optional)
            top_k_root_causes: Number of top root causes to return
            
        Returns:
            Dictionary with analysis results
        """
        print("\n" + "=" * 70)
        print("CAUSAL INFERENCE ROOT CAUSE ANALYSIS")
        print("=" * 70)
        
        # Step 1: Build causal graph
        self.build_causal_graph_from_service_graph()
        
        # Step 2: Prepare latency data
        normal_data, anomalous_data = self.prepare_latency_data(alerts_data)
        
        # Step 3: Fit causal model
        self.fit_causal_model(normal_data)
        
        # Step 4: Attribute anomalies
        attribution_df = self.attribute_latency_anomalies(anomalous_data, target_service, alerts_data)
        
        # Step 5: Identify root causes
        root_causes_df = self.identify_root_causes(attribution_df, top_k_root_causes)
        
        # Step 6: Export results
        self.export_results(attribution_df, root_causes_df)
        
        print("\n" + "=" * 70)
        print("ANALYSIS COMPLETE!")
        print("=" * 70)
        
        return {
            'attributions': attribution_df,
            'root_causes': root_causes_df,
            'baseline_stats': pd.DataFrame(self.latency_baseline).T
        }

