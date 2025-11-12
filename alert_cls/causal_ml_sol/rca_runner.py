"""
Example usage of Causal RCA Pipeline

This script demonstrates how to use the causal inference root cause analysis
for identifying root causes of elevated latencies in microservices.
"""

import os
import sys

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from rca_pipeline import RCAPipeline


def rca_runner():
    """Basic example - latency inferred from alerts"""
    print("=" * 70)
    print("Example 1: Basic Usage (Alerts Only)")
    print("=" * 70)
    
    pipeline = RCAPipeline(
        alerts_csv_path='alert_data.csv',
        graph_json_path='graph_data.json',
        output_dir='./rca_results'
        # Latency is automatically inferred from alerts!
    )
    
    results = pipeline.run_complete_pipeline()
    
    print("\n Analysis complete!")
    print(f" Check results in: ./rca_results/")
    print(f"  - root_causes.csv: Top root causes")
    print(f"  - latency_attributions.csv: Detailed attributions")
    print(f"  - rca_summary_report.txt: Summary report")


def example_target_service():
    """Example analyzing a specific service"""
    print("=" * 70)
    print("Example 2: Analyzing Specific Service")
    print("=" * 70)
    
    pipeline = RCAPipeline(
        alerts_csv_path='C:/Users/jurat.shayidin/aiops/causal_ml/alert_data.csv',
        graph_json_path='C:/Users/jurat.shayidin/aiops/causal_ml/graph_data.json',
        output_dir='./rca_results'
    )
    
    # Analyze specific service (e.g., API gateway)
    results = pipeline.run_complete_pipeline(target_service='api-gateway')
    
    print("\n Analysis complete for target service!")


def example_step_by_step():
    """Example running pipeline step by step"""
    print("=" * 70)
    print("Example 3: Step-by-Step Pipeline")
    print("=" * 70)
    
    pipeline = RCAPipeline(
        alerts_csv_path='C:/Users/jurat.shayidin/aiops/causal_ml/alert_data.csv',
        graph_json_path='C:/Users/jurat.shayidin/aiops/causal_ml/graph_data.json',
        output_dir='./rca_results'
    )
    
    # Step 1: Alert consolidation
    consolidated = pipeline.run_alert_consolidation()
    print(f"\n Consolidated {len(consolidated)} alerts")
    
    # Step 2: Prepare for RCA
    alerts_for_rca = pipeline.prepare_for_rca()
    print(f" Prepared {len(alerts_for_rca)} alerts for RCA")
    
    # Step 3: Run RCA
    rca_results = pipeline.run_causal_rca(alerts_for_rca)
    print(f" RCA complete: {len(rca_results['root_causes'])} root causes identified")
    
    # Step 4: Generate summary
    summary = pipeline.generate_summary_report()
    print(summary)


if __name__ == "__main__":
    rca_runner()


