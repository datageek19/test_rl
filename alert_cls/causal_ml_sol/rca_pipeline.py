"""
End-to-End Pipeline: Alert Consolidation + Causal RCA

This module integrates alert consolidation with causal inference root cause analysis.
"""

import pandas as pd
import networkx as nx
import json
import os
import sys
from typing import Optional, Dict

# Import alert consolidation
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from alert_consolidation_complete import ComprehensiveAlertConsolidator
except ImportError:
    # Try importing from parent directory
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from causal_ml.alert_consolidation_complete import ComprehensiveAlertConsolidator

# Import causal RCA
from causal_rca_analyzer import CausalRCAAnalyzer


class RCAPipeline:
    """
    Complete pipeline: Alert Consolidation → Causal RCA
    
    This pipeline includes:
    1. Consolidates alerts using service graph
    2. Prepares latency data from alerts and measurements
    3. Performs causal inference root cause analysis
    4. Identifies root causes of elevated latencies
    """
    
    def __init__(self,
                 alerts_csv_path: str,
                 graph_json_path: str,
                 output_dir: str = '.'):
        """
        Initialize the RCA Pipeline
        
        Latency is inferred from alert patterns automatically.
        """
        self.alerts_csv_path = alerts_csv_path
        self.graph_json_path = graph_json_path
        self.output_dir = output_dir
        
        # Pipeline components
        self.consolidator = None
        self.rca_analyzer = None
        self.service_graph = None
        
        # Results
        self.consolidated_alerts = None
        self.rca_results = None
        
    def run_alert_consolidation(self) -> pd.DataFrame:
        """
        Step 1: Run alert consolidation to group and enrich alerts.
        
        Returns:
            DataFrame with consolidated alerts
        """
        print("\n" + "=" * 70)
        print("PHASE 1: ALERT CONSOLIDATION")
        print("=" * 70)
        
        # Initialize consolidator
        self.consolidator = ComprehensiveAlertConsolidator(
            alerts_csv_path=self.alerts_csv_path,
            graph_json_path=self.graph_json_path,
            output_dir=self.output_dir
        )
        
        # Run consolidation
        consolidated_groups = self.consolidator.run_consolidation()
        
        # Load consolidated results
        consolidated_path = os.path.join(self.output_dir, 'alert_consolidation_final.csv')
        if os.path.exists(consolidated_path):
            self.consolidated_alerts = pd.read_csv(consolidated_path)
            print(f"\n Loaded {len(self.consolidated_alerts)} consolidated alerts")
        else:
            # Fallback: use enriched alerts from consolidator
            self.consolidated_alerts = pd.DataFrame(self.consolidator.enriched_alerts)
            print(f"\n Using {len(self.consolidated_alerts)} enriched alerts")
        
        # Get service graph from consolidator
        self.service_graph = self.consolidator.service_graph
        
        return self.consolidated_alerts
    
    def prepare_for_rca(self) -> pd.DataFrame:
        """
        Prepare alert data for RCA analysis.
        """
        print("\n" + "=" * 70)
        print("PHASE 2: PREPARING DATA FOR RCA")
        print("=" * 70)
        
        if self.consolidated_alerts is None:
            raise ValueError("Run alert consolidation first")
        
        # Filter to firing alerts
        if 'status' in self.consolidated_alerts.columns:
            firing_alerts = self.consolidated_alerts[
                self.consolidated_alerts['status'].str.strip().str.lower() == 'firing'
            ].copy()
        else:
            # No status column, use all alerts
            firing_alerts = self.consolidated_alerts.copy()
        
        if len(firing_alerts) == 0:
            # No firing alerts found, use all alerts
            firing_alerts = self.consolidated_alerts.copy()
        
        print(f"    Total alerts: {len(firing_alerts)}")
        
        # Filter to alerts mapped to service graph
        if 'graph_service' in firing_alerts.columns:
            mapped_alerts = firing_alerts[
                firing_alerts['graph_service'].notna() &
                (firing_alerts['graph_service'] != '')
            ].copy()
            print(f"    Mapped to graph: {len(mapped_alerts)}")
        else:
            mapped_alerts = firing_alerts
            print(f"    No graph_service column found")
        
        # Identify latency-related alerts
        latency_keywords = ['latency', 'slow', 'timeout', 'delay', 'response_time', 
                          'p95', 'p99', 'duration']
        
        if 'alert_name' in mapped_alerts.columns:
            latency_alerts = mapped_alerts[
                mapped_alerts['alert_name'].str.lower().str.contains(
                    '|'.join(latency_keywords), na=False
                )
            ]
            print(f"    Latency-related alerts: {len(latency_alerts)}")
        else:
            latency_alerts = mapped_alerts
            print(f"    No alert_name column found, using all alerts")
        
        # If no latency-specific alerts, use all mapped alerts
        if len(latency_alerts) == 0:
            print("    No latency-specific alerts found, using all mapped alerts")
            latency_alerts = mapped_alerts
        
        return latency_alerts
    
    def run_causal_rca(self, alerts_for_rca: pd.DataFrame, target_service: Optional[str] = None) -> Dict:
        """
        Run causal inference root cause analysis.
        """
        print("\n" + "=" * 70)
        print("PHASE 3: CAUSAL INFERENCE ROOT CAUSE ANALYSIS")
        print("=" * 70)
        
        if self.service_graph is None:
            raise ValueError("Service graph not available. Run alert consolidation first.")
        
        if len(self.service_graph.nodes()) == 0:
            raise ValueError("Service graph is empty. Cannot perform RCA.")
        
        # Initialize RCA analyzer
        self.rca_analyzer = CausalRCAAnalyzer(
            service_graph=self.service_graph,
            output_dir=self.output_dir
        )
        
        # Run complete RCA analysis
        rca_results = self.rca_analyzer.run_complete_analysis(
            alerts_data=alerts_for_rca,
            target_service=target_service,
            top_k_root_causes=10
        )
        
        self.rca_results = rca_results
        
        return rca_results
    
    def generate_summary_report(self) -> str:
        """
        Generate a summary report of the analysis.
        """
        report_lines = []
        report_lines.append("\n" + "=" * 70)
        report_lines.append("ROOT CAUSE ANALYSIS SUMMARY REPORT")
        report_lines.append("=" * 70)
        
        # Alert consolidation summary
        if self.consolidated_alerts is not None:
            report_lines.append(f"\n📊 Alert Consolidation:")
            report_lines.append(f"   Total alerts processed: {len(self.consolidated_alerts)}")
            if 'cluster_id' in self.consolidated_alerts.columns:
                n_clusters = self.consolidated_alerts['cluster_id'].nunique()
                report_lines.append(f"   Clusters identified: {n_clusters}")
            if 'graph_service' in self.consolidated_alerts.columns:
                mapped = self.consolidated_alerts['graph_service'].notna().sum()
                report_lines.append(f"   Mapped to service graph: {mapped}")
        
        # RCA summary
        if self.rca_results and 'root_causes' in self.rca_results:
            root_causes_df = self.rca_results['root_causes']
            if len(root_causes_df) > 0:
                report_lines.append(f"\n🔍 Root Cause Analysis:")
                report_lines.append(f"   Root causes identified: {len(root_causes_df)}")
                report_lines.append(f"\n   Top 5 Root Causes:")
                for idx, row in root_causes_df.head(5).iterrows():
                    service = row['root_cause_service']
                    score = row['root_cause_score']
                    latency_inc = row['latency_increase_pct']
                    report_lines.append(
                        f"   {idx+1}. {service} (Score: {score:.2f}, "
                        f"Latency +{latency_inc:.1f}%)"
                    )
        
        # Attribution summary
        if self.rca_results and 'attributions' in self.rca_results:
            attributions_df = self.rca_results['attributions']
            if len(attributions_df) > 0:
                report_lines.append(f"\n📈 Attribution Analysis:")
                report_lines.append(f"   Service attributions: {len(attributions_df)}")
                top_attr = attributions_df.head(3)
                report_lines.append(f"\n   Top Attributions:")
                for _, row in top_attr.iterrows():
                    upstream = row['upstream_service']
                    target = row['target_service']
                    score = row['attribution_score']
                    report_lines.append(
                        f"   - {upstream} → {target} (Score: {score:.4f})"
                    )
        
        report_lines.append("\n" + "=" * 70)
        report_lines.append("See output CSV files for detailed results")
        report_lines.append("=" * 70 + "\n")
        
        return "\n".join(report_lines)
    
    def run_complete_pipeline(self, target_service: Optional[str] = None) -> Dict:
        """
        Run the complete end-to-end pipeline
        """
        print("\n" + "=" * 70)
        print("END-TO-END RCA PIPELINE")
        print("=" * 70)
        
        # Alert Consolidation
        consolidated_alerts = self.run_alert_consolidation()
        
        # Prepare for RCA
        alerts_for_rca = self.prepare_for_rca()
        
        # Causal RCA
        rca_results = self.run_causal_rca(alerts_for_rca, target_service)
        
        # Generate summary
        summary = self.generate_summary_report()
        print(summary)
        
        # Save summary to file 
        summary_path = os.path.join(self.output_dir, 'rca_summary_report.txt')
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(summary)
        print(f" Summary report saved: {summary_path}")
        
        return {
            'consolidated_alerts': consolidated_alerts,
            'alerts_for_rca': alerts_for_rca,
            'rca_results': rca_results,
            'summary': summary
        }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Run end-to-end Alert Consolidation + Causal RCA Pipeline'
    )
    parser.add_argument('--alerts', type=str, default='alert_data.csv',
                       help='Path to alerts CSV file')
    parser.add_argument('--graph', type=str, default='graph_data.json',
                       help='Path to service graph JSON file')
    parser.add_argument('--output', type=str, default='.',
                       help='Output directory for results')
    parser.add_argument('--target-service', type=str, default=None,
                       help='Specific service to analyze (optional)')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = RCAPipeline(
        alerts_csv_path=args.alerts,
        graph_json_path=args.graph,
        output_dir=args.output
    )
    
    # Run complete pipeline
    results = pipeline.run_complete_pipeline(target_service=args.target_service)
    
    print("\n Pipeline completed successfully!")
    print(f" Results saved to: {args.output}")

