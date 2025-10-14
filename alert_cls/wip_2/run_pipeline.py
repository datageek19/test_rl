"""
Example script demonstrating how to run the alert consolidation pipeline.
This script shows various usage patterns and analysis capabilities.
"""

import os
import pandas as pd
from alert_consolidation_complete import ComprehensiveAlertConsolidator

def main():
    print("=" * 80)
    print("ALERT CONSOLIDATION - EXAMPLE RUN")
    print("=" * 80)
    
    # ========================================================================
    # STEP 1: Configure paths
    # ========================================================================
    
    ALERTS_CSV = 'C:/Users/jurat.shayidin/aiops/wip_3/alert_data.csv'
    GRAPH_JSON = 'C:/Users/jurat.shayidin/aiops/wip_3/graph_data.json'
    OUTPUT_DIR = './consolidation_results'
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # ========================================================================
    # STEP 2: Initialize and run consolidation
    # ========================================================================
    
    consolidator = ComprehensiveAlertConsolidator(
        alerts_csv_path=ALERTS_CSV,
        graph_json_path=GRAPH_JSON,
        output_dir=OUTPUT_DIR
    )
    
    # Run the complete pipeline
    consolidated_groups = consolidator.run_consolidation()
    
    # ========================================================================
    # STEP 3: Analyze results
    # ========================================================================
    
    print("\n" + "=" * 80)
    print("POST-PROCESSING ANALYSIS")
    print("=" * 80)
    
    # Load the main results
    results_df = pd.read_csv(f'{OUTPUT_DIR}/alert_consolidation_final.csv')
    cluster_summary = pd.read_csv(f'{OUTPUT_DIR}/cluster_summary.csv')
    
    print(f"\n📊 RESULTS SUMMARY")
    print(f"{'─' * 80}")
    print(f"  Total alerts processed: {len(results_df)}")
    print(f"  Unique alerts (post-dedup): {len(results_df[~results_df['is_duplicate']])}")
    print(f"  Number of final clusters: {len(cluster_summary)}")
    print(f"  Duplicate alerts removed: {len(results_df[results_df['is_duplicate']])}")
    
    # ========================================================================
    # STEP 4: Analyze mapping effectiveness
    # ========================================================================
    
    print(f"\n📍 MAPPING EFFECTIVENESS")
    print(f"{'─' * 80}")
    
    mapping_counts = results_df['mapping_method'].value_counts()
    total_alerts = len(results_df)
    
    for method, count in mapping_counts.items():
        percentage = (count / total_alerts) * 100
        print(f"  {method:30s}: {count:6d} ({percentage:5.1f}%)")
    
    avg_confidence = results_df['mapping_confidence'].mean()
    print(f"\n  Average mapping confidence: {avg_confidence:.3f}")
    
    # ========================================================================
    # STEP 5: Analyze top clusters
    # ========================================================================
    
    print(f"\n🔝 TOP 10 LARGEST CLUSTERS")
    print(f"{'─' * 80}")
    print(f"{'Cluster ID':<12} {'Alerts':<10} {'Services':<10} {'Primary Alert Type':<40}")
    print(f"{'─' * 80}")
    
    for idx, row in cluster_summary.head(10).iterrows():
        cluster_id = row['cluster_id']
        alert_count = row['alert_count']
        service_count = row['unique_services']
        alert_type = row['most_common_alert'][:38]
        
        print(f"{cluster_id:<12} {alert_count:<10} {service_count:<10} {alert_type:<40}")
    
    # ========================================================================
    # STEP 6: Identify potential root causes
    # ========================================================================
    
    print(f"\n🎯 POTENTIAL ROOT CAUSE ANALYSIS")
    print(f"{'─' * 80}")
    
    # For each cluster, identify the service with most alerts
    root_causes = []
    
    for cluster_id in cluster_summary['cluster_id'].head(5):
        cluster_alerts = results_df[results_df['final_group_id'] == cluster_id]
        
        if len(cluster_alerts) == 0:
            continue
        
        # Count alerts per service
        service_counts = cluster_alerts['graph_service'].value_counts()
        
        if len(service_counts) > 0:
            top_service = service_counts.index[0]
            top_count = service_counts.iloc[0]
            
            # Get severity distribution for this service
            service_alerts = cluster_alerts[cluster_alerts['graph_service'] == top_service]
            severity_dist = service_alerts['severity'].value_counts().to_dict()
            
            root_causes.append({
                'cluster_id': cluster_id,
                'service': top_service,
                'alert_count': top_count,
                'severity_distribution': severity_dist,
                'alert_types': service_alerts['alert_name'].nunique()
            })
    
    print(f"{'Cluster':<10} {'Service':<40} {'Alerts':<10} {'Severities':<30}")
    print(f"{'─' * 80}")
    
    for rc in root_causes:
        cluster_id = rc['cluster_id']
        service = rc['service'][:38] if rc['service'] else 'N/A'
        alert_count = rc['alert_count']
        severities = ', '.join([f"{k}:{v}" for k, v in rc['severity_distribution'].items()])[:28]
        
        print(f"{cluster_id:<10} {service:<40} {alert_count:<10} {severities:<30}")
    
    # ========================================================================
    # STEP 7: Clustering quality metrics
    # ========================================================================
    
    print(f"\n📈 CLUSTERING QUALITY")
    print(f"{'─' * 80}")
    
    clustering_method = results_df['clustering_method'].iloc[0] if len(results_df) > 0 else 'N/A'
    print(f"  Selected clustering algorithm: {clustering_method}")
    
    # Analyze cluster sizes
    cluster_sizes = results_df['final_group_id'].value_counts()
    print(f"\n  Cluster size statistics:")
    print(f"    Mean: {cluster_sizes.mean():.1f} alerts/cluster")
    print(f"    Median: {cluster_sizes.median():.1f} alerts/cluster")
    print(f"    Min: {cluster_sizes.min()} alerts/cluster")
    print(f"    Max: {cluster_sizes.max()} alerts/cluster")
    print(f"    Std Dev: {cluster_sizes.std():.1f}")
    
    # ========================================================================
    # STEP 8: Export additional insights
    # ========================================================================
    
    print(f"\n💾 EXPORTING ADDITIONAL INSIGHTS")
    print(f"{'─' * 80}")
    
    # Export root cause candidates
    root_causes_df = pd.DataFrame(root_causes)
    root_causes_path = f'{OUTPUT_DIR}/root_cause_candidates.csv'
    root_causes_df.to_csv(root_causes_path, index=False)
    print(f"  ✓ Root cause candidates: {root_causes_path}")
    
    # Export high-priority alerts (critical/high severity, not duplicates)
    high_priority = results_df[
        (results_df['severity'].isin(['critical', 'high'])) & 
        (~results_df['is_duplicate'])
    ]
    high_priority_path = f'{OUTPUT_DIR}/high_priority_alerts.csv'
    high_priority.to_csv(high_priority_path, index=False)
    print(f"  ✓ High priority alerts: {high_priority_path}")
    
    # Export alerts by namespace for operational teams
    for namespace in results_df['namespace'].unique()[:5]:  # Top 5 namespaces
        if pd.notna(namespace) and namespace:
            namespace_alerts = results_df[results_df['namespace'] == namespace]
            namespace_path = f'{OUTPUT_DIR}/alerts_namespace_{namespace}.csv'
            namespace_alerts.to_csv(namespace_path, index=False)
            print(f"  ✓ Namespace {namespace}: {namespace_path} ({len(namespace_alerts)} alerts)")
    
    # ========================================================================
    # STEP 9: Generate recommendations
    # ========================================================================
    
    print(f"\n💡 RECOMMENDATIONS")
    print(f"{'─' * 80}")
    
    # Recommendation 1: Unmapped alerts
    unmapped_count = len(results_df[results_df['mapping_method'] == 'unmapped'])
    if unmapped_count > 0:
        pct_unmapped = (unmapped_count / len(results_df)) * 100
        print(f"  ⚠ {unmapped_count} ({pct_unmapped:.1f}%) alerts could not be mapped to graph")
        print(f"     → Review service naming consistency between alerts and graph")
        print(f"     → Consider enhancing fallback mapping strategies")
    
    # Recommendation 2: Large clusters
    large_clusters = cluster_summary[cluster_summary['alert_count'] > 100]
    if len(large_clusters) > 0:
        print(f"\n  ⚠ {len(large_clusters)} clusters have >100 alerts")
        print(f"     → Consider increasing number of clusters (reduce max_k)")
        print(f"     → May indicate widespread infrastructure issues")
    
    # Recommendation 3: Singleton clusters
    singleton_clusters = cluster_summary[cluster_summary['alert_count'] == 1]
    if len(singleton_clusters) > 10:
        print(f"\n  ⚠ {len(singleton_clusters)} clusters have only 1 alert")
        print(f"     → May indicate noise or outliers")
        print(f"     → Consider using DBSCAN to identify outliers explicitly")
    
    # Recommendation 4: High duplicate rate
    duplicate_rate = (len(results_df[results_df['is_duplicate']]) / len(results_df)) * 100
    if duplicate_rate > 30:
        print(f"\n  ✓ High duplicate rate ({duplicate_rate:.1f}%)")
        print(f"     → Good - deduplication is working effectively")
        print(f"     → Consider adjusting time window if needed")
    
    print("\n" + "=" * 80)
    print("CONSOLIDATION ANALYSIS COMPLETE!")
    print("=" * 80)
    print(f"\nAll results saved to: {OUTPUT_DIR}/")
    print("\nNext steps:")
    print("  1. Review cluster_summary.csv for overview")
    print("  2. Check high_priority_alerts.csv for critical issues")
    print("  3. Investigate root_cause_candidates.csv for incident response")
    print("  4. Use alert_consolidation_final.csv for detailed analysis")


if __name__ == "__main__":
    main()

