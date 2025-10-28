import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import Counter
import os

def load_and_visualize_clusters(base_dir='.'):
    """Create simplified, intuitive visualizations for end users"""
    print("Loading cluster data...")
    
    # Load main results
    alerts_path = os.path.join(base_dir, 'alert_consolidation_final.csv')
    df = pd.read_csv(alerts_path)
    
    # Load ranked clusters
    try:
        ranked_path = os.path.join(base_dir, 'ranked_clusters.csv')
        df_ranked = pd.read_csv(ranked_path)
    except Exception as e:
        print(f"⚠ ranked_clusters.csv not found: {e}")
        df_ranked = None
    
    # Create figure with 2x2 layout
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('Alert Consolidation Dashboard', fontsize=20, fontweight='bold', y=0.98)
    
    # ============================================================
    # 1. TOP 10 CRITICAL CLUSTERS (Top Left)
    # ============================================================
    ax1 = axes[0, 0]
    
    if df_ranked is not None and len(df_ranked) > 0:
        top_10 = df_ranked.head(10)
        
        # Create horizontal bar chart
        colors = ['#dc3545', '#fd7e14', '#ff9800', '#ffc107', '#ffeb3b', 
                 '#8bc34a', '#4caf50', '#009688', '#2196f3', '#6c757d']
        
        bars = ax1.barh(range(10), top_10['alert_count'], color=colors)
        ax1.set_yticks(range(10))
        ax1.set_yticklabels(top_10['cluster_name'], fontsize=10)
        ax1.set_xlabel('Number of Alerts', fontsize=12, fontweight='bold')
        ax1.set_title('🔴 Top 10 Most Critical Clusters (by Alert Count)', 
                      fontsize=14, fontweight='bold', pad=10)
        ax1.grid(axis='x', alpha=0.3, linestyle='--')
        ax1.invert_yaxis()
        
        # Add value labels on bars
        for i, (idx, row) in enumerate(top_10.iterrows()):
            ax1.text(row['alert_count'] + max(top_10['alert_count'])*0.02, i, 
                    f"{int(row['alert_count'])} alerts", 
                    va='center', fontsize=9, fontweight='bold')
    else:
        ax1.text(0.5, 0.5, 'No cluster data available', ha='center', va='center', 
                fontsize=14, transform=ax1.transAxes)
        ax1.set_title('Top 10 Most Critical Clusters', fontsize=14, fontweight='bold')
    
    # ============================================================
    # 2. SEVERITY DISTRIBUTION (Top Right)
    # ============================================================
    ax2 = axes[0, 1]
    
    if 'severity' in df.columns:
        severity_counts = df['severity'].value_counts()
        severity_order = ['critical', 'high', 'warning', 'info']
        severity_colors = ['#dc3545', '#fd7e14', '#ffc107', '#28a745']
        
        # Filter to existing severities
        existing_severities = [s for s in severity_order if s in severity_counts.index]
        existing_counts = [severity_counts[s] for s in existing_severities]
        existing_colors = severity_colors[:len(existing_severities)]
        
        if existing_counts:
            # Create pie chart with explode for critical/high
            explode = [0.05 if s in ['critical', 'high'] else 0 for s in existing_severities]
            
            wedges, texts, autotexts = ax2.pie(existing_counts, 
                                               labels=existing_severities, 
                                               autopct='%1.1f%%',
                                               colors=existing_colors,
                                               explode=explode,
                                               startangle=90,
                                               textprops={'fontsize': 11, 'fontweight': 'bold'})
            
            # Make percentage text larger
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontsize(12)
                autotext.set_fontweight('bold')
            
            ax2.set_title('⚠️ Overall Severity Distribution', 
                         fontsize=14, fontweight='bold', pad=10)
    else:
        ax2.text(0.5, 0.5, 'No severity data', ha='center', va='center', 
                fontsize=14, transform=ax2.transAxes)
        ax2.set_title('Severity Distribution', fontsize=14, fontweight='bold')
    
    # ============================================================
    # 3. TOP SERVICES AFFECTED (Bottom Left)
    # ============================================================
    ax3 = axes[1, 0]
    
    if 'graph_service' in df.columns:
        top_services = df['graph_service'].value_counts().head(10)
        top_services_sorted = top_services.sort_values(ascending=True)
        
        bars = ax3.barh(range(len(top_services_sorted)), top_services_sorted.values, 
                       color='#2196f3')
        ax3.set_yticks(range(len(top_services_sorted)))
        
        # Truncate service names if too long
        labels = [name[:40] + '...' if len(name) > 40 else name 
                 for name in top_services_sorted.index]
        ax3.set_yticklabels(labels, fontsize=9)
        ax3.set_xlabel('Number of Alerts', fontsize=12, fontweight='bold')
        ax3.set_title('🔧 Top 10 Services Most Affected by Alerts', 
                      fontsize=14, fontweight='bold', pad=10)
        ax3.grid(axis='x', alpha=0.3, linestyle='--')
        
        # Add value labels
        for i, val in enumerate(top_services_sorted.values):
            ax3.text(val + max(top_services_sorted.values)*0.01, i, 
                    f"{int(val)}", va='center', fontsize=10, fontweight='bold')
    else:
        ax3.text(0.5, 0.5, 'No service data available', ha='center', va='center', 
                fontsize=14, transform=ax3.transAxes)
        ax3.set_title('Top Services Affected', fontsize=14, fontweight='bold')
    
    # ============================================================
    # 4. KEY METRICS DASHBOARD (Bottom Right)
    # ============================================================
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    # Calculate key metrics
    total_alerts = len(df)
    unique_clusters = df['final_group_id'].nunique()
    
    if 'is_duplicate' in df.columns:
        duplicates = df['is_duplicate'].sum()
        unique_after_dedup = total_alerts - duplicates
        dedup_percentage = (duplicates / total_alerts * 100) if total_alerts > 0 else 0
    else:
        duplicates = 0
        unique_after_dedup = total_alerts
        dedup_percentage = 0
    
    if 'severity' in df.columns:
        critical_count = (df['severity'] == 'critical').sum()
        high_count = (df['severity'] == 'high').sum()
    else:
        critical_count = 0
        high_count = 0
    
    # Create text dashboard
    metrics_text = f"""
    📊 KEY METRICS

    Total Alerts: {total_alerts:,}
    Consolidated into {unique_clusters} Clusters
    
    Unique Alerts (after dedup): {unique_after_dedup:,}
    Duplicates Removed: {duplicates:,} ({dedup_percentage:.1f}%)
    
    Critical Alerts: {critical_count:,}
    High Alerts: {high_count:,}
    
    Most Common Issue:
    """
    
    # Add most common issue
    if df_ranked is not None and len(df_ranked) > 0:
        top_issue = df_ranked.iloc[0]
        issue_name = top_issue.get('cluster_name', 'Unknown')
        issue_count = top_issue.get('alert_count', 0)
        metrics_text += f"  • {issue_name}\n    ({issue_count} alerts)"
    
    ax4.text(0.1, 0.5, metrics_text, transform=ax4.transAxes,
            fontsize=13, verticalalignment='center',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3, pad=15),
            family='monospace')
    
    ax4.set_title('📈 Consolidation Impact', fontsize=14, fontweight='bold', pad=10)
    
    # Adjust layout and save
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    output_path = os.path.join(base_dir, 'cluster_visualization.png')
    plt.savefig(output_path, dpi=200, bbox_inches='tight', facecolor='white')
    print(f"✓ Saved simplified visualization: {output_path}")
    
    # Print summary statistics
    print("\n" + "=" * 70)
    print("ALERT CONSOLIDATION SUMMARY")
    print("=" * 70)
    print(f"📊 Total alerts processed: {total_alerts:,}")
    print(f"🎯 Consolidated into {unique_clusters} clusters")
    print(f"✓ Unique alerts (after dedup): {unique_after_dedup:,}")
    print(f"🔁 Duplicates removed: {duplicates:,} ({dedup_percentage:.1f}%)")
    
    if df_ranked is not None and len(df_ranked) > 0:
        print(f"\n🔥 Top 3 Most Critical Clusters:")
        for i, (idx, row) in enumerate(df_ranked.head(3).iterrows(), 1):
            print(f"  {i}. {row['cluster_name']}")
            print(f"     → {int(row['alert_count'])} alerts, Score: {row['ranking_score']:.1f}")
    
    print("\n" + "=" * 70)
    
    return fig

def generate_cluster_summary_table(base_dir='.'):
    """Generate a detailed summary table of clusters"""
    print("\nGenerating cluster summary table...")
    
    alerts_path = os.path.join(base_dir, 'alert_consolidation_final.csv')
    ranked_path = os.path.join(base_dir, 'ranked_clusters.csv')
    
    df = pd.read_csv(alerts_path)
    df_ranked = pd.read_csv(ranked_path)
    
    # Rename cluster_id to final_group_id for merge
    df_ranked = df_ranked.rename(columns={'cluster_id': 'final_group_id'})
    
    # Create detailed summary
    summary = []
    
    for cluster_id in sorted(df['final_group_id'].unique()):
        if cluster_id == -1:  # Skip outliers
            continue
        
        cluster_data = df[df['final_group_id'] == cluster_id]
        ranked_info = df_ranked[df_ranked['final_group_id'] == cluster_id]
        
        if len(ranked_info) > 0:
            row = ranked_info.iloc[0]
            cluster_name = row['cluster_name']
            rank = row['rank']
            score = row['ranking_score']
        else:
            cluster_name = f"cluster_{cluster_id}"
            rank = -1
            score = 0
        
        # Calculate statistics
        alert_types = cluster_data['alert_name'].unique() if 'alert_name' in cluster_data.columns else []
        services = cluster_data['graph_service'].dropna().unique() if 'graph_service' in cluster_data.columns else []
        severities = cluster_data['severity'].value_counts().to_dict() if 'severity' in cluster_data.columns else {}
        
        # Safely get primary alert
        primary_alert = ''
        if 'alert_name' in cluster_data.columns and len(cluster_data) > 0:
            mode_result = cluster_data['alert_name'].mode()
            primary_alert = mode_result[0] if len(mode_result) > 0 else ''
        
        # Safely get primary service
        primary_service = ''
        if 'graph_service' in cluster_data.columns and len(cluster_data.dropna(subset=['graph_service'])) > 0:
            mode_result = cluster_data['graph_service'].mode()
            primary_service = mode_result[0] if len(mode_result) > 0 else ''
        
        summary.append({
            'Rank': rank,
            'Cluster_ID': cluster_id,
            'Cluster_Name': cluster_name,
            'Score': score,
            'Total_Alerts': len(cluster_data),
            'Unique_Alert_Types': len(alert_types),
            'Primary_Alert': primary_alert,
            'Primary_Service': primary_service,
            'Unique_Services': len(services),
            'Critical': severities.get('critical', 0),
            'High': severities.get('high', 0),
            'Warning': severities.get('warning', 0),
            'Info': severities.get('info', 0),
        })
    
    df_summary = pd.DataFrame(summary)
    df_summary = df_summary.sort_values('Rank')
    
    # Export
    output_path = os.path.join(base_dir, 'cluster_detailed_summary.csv')
    df_summary.to_csv(output_path, index=False)
    print(f"✓ Saved detailed summary: {output_path}")
    
    # Print top 10
    print("\n" + "=" * 100)
    print("TOP 10 CLUSTERS - DETAILED VIEW")
    print("=" * 100)
    for idx, row in df_summary.head(10).iterrows():
        print(f"\nRank {row['Rank']}: {row['Cluster_Name']}")
        print(f"  ID: {row['Cluster_ID']}, Score: {row['Score']:.2f}")
        print(f"  Alerts: {row['Total_Alerts']} (Unique types: {row['Unique_Alert_Types']})")
        print(f"  Primary: {row['Primary_Alert']} on {row['Primary_Service']}")
        print(f"  Severity: Critical={row['Critical']}, High={row['High']}, Warning={row['Warning']}, Info={row['Info']}")
        print(f"  Services: {row['Unique_Services']} unique")
    
    return df_summary

if __name__ == "__main__":
    # Generate visualizations
    fig = load_and_visualize_clusters()
    
    # Generate detailed summary
    summary_df = generate_cluster_summary_table()
    
    print("\n✓ Visualization complete!")
    print("✓ Check 'cluster_visualization.png' for simplified overview")
    print("✓ Check 'cluster_detailed_summary.csv' for detailed cluster information")

