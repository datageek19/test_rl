"""
alert classification workflow scheduler

    Orchestrates the complete alert classification workflow and schedules periodic execution.
"""

import time
from datetime import datetime
from typing import Dict
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from data_ingestion import DataIngestionService
from alert_processor import AlertProcessor
from model_manager import ModelManager
from cluster_predictor import ClusterPredictor
from result_storage import ResultStorage


class AlertWorkflowScheduler:
    """
    Orchestrates the complete alert classification workflow:
        1. Every 15 minutes: Run classification pipeline
        2. Weekly: Retrain clustering model
    """
    
    def __init__(self, alerts_csv_path: str, graph_json_path: str, 
                 output_dir: str = 'results', model_dir: str = 'data/models'):
        """
        Initialize the workflow scheduler
        
        Args:
            alerts_csv_path: Path to alerts CSV file in azure blob storage
            graph_json_path: Path to service graph JSON file in azure blob storage
            output_dir: Directory to store results in azure blob storage
            model_dir: Directory to store models in azure blob storage
        """
        self.alerts_csv_path = alerts_csv_path
        self.graph_json_path = graph_json_path
        self.output_dir = output_dir
        self.model_dir = model_dir
        
        # Initialize services
        self.data_ingestion = DataIngestionService()
        self.model_manager = ModelManager(model_dir=model_dir)
        self.cluster_predictor = ClusterPredictor(self.model_manager)
        self.result_storage = ResultStorage(output_dir=output_dir)
        
        # Scheduler
        self.scheduler = BackgroundScheduler()
        
        # State
        self.is_running = False
        self.last_run_id = None
        self.last_run_status = None
    
    def start(self, workflow_interval_minutes: int = 15, enable_retraining: bool = True):
        """
        Start the scheduler
        
        Args:
            workflow_interval_minutes: Interval for running classification workflow (default: 15 minutes)
            enable_retraining: Whether to enable weekly model retraining (default: True)
        """
        print("\n" + "=" * 20)
        print("ALERT CLASSIFICATION WORKFLOW SCHEDULER")
        print("=" * 20)
        print(f"Workflow interval: Every {workflow_interval_minutes} minutes")
        print(f"Model retraining: {'Enabled (Sundays 2 AM)' if enable_retraining else 'Disabled'}")
        print("=" * 20)
        
        # Schedule alert data classification workflow every 15 minutes
        self.scheduler.add_job(
            func=self.run_workflow,
            trigger=IntervalTrigger(minutes=workflow_interval_minutes),
            id='alert_classification_workflow',
            name='Alert Classification Workflow',
            max_instances=1,  # Prevent overlapping runs
            replace_existing=True
        )
        
        print(f"\n Scheduled workflow: Every {workflow_interval_minutes} minutes")
        
        # Schedule model retraining (weekly on Mondays at 7 AM)
        if enable_retraining:
            self.scheduler.add_job(
                func=self.retrain_model,
                trigger=CronTrigger(day_of_week='mon', hour=7, minute=0),
                id='model_retraining',
                name='Model Retraining',
                replace_existing=True
            )
            print(" Scheduled retraining: mondays at 7:00 AM")
        
        # Start scheduler
        self.scheduler.start()
        self.is_running = True
        
        print("\n Scheduler started successfully!")
        print("=" * 20)
    
    def run_workflow(self):
        """Execute the complete alert classification workflow"""
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        print("\n" + "=" * 20)
        print(f"WORKFLOW RUN: {run_id}")
        print("=" * 20)
        
        start_time = time.time()
        
        try:
            # ============================================================
            # STEP 1: Data Ingestion
            # ============================================================
            alerts = self.data_ingestion.load_firing_alerts(self.alerts_csv_path)
            service_graph, service_to_graph = self.data_ingestion.load_graph_data(self.graph_json_path)
            
            if len(alerts) == 0:
                print("\n No alerts to process - skipping workflow")
                self.last_run_status = 'skipped_no_alerts'
                return
            
            # Get cached graph features
            service_features_cache = self.data_ingestion.get_service_features_cache()
            pagerank_cache = self.data_ingestion.get_pagerank_cache()
            
            # ============================================================
            # STEP 2: Alert Processing - Enrichment & Consolidation
            # ============================================================
            alert_processor = AlertProcessor(
                service_graph=service_graph,
                service_to_graph=service_to_graph,
                service_features_cache=service_features_cache,
                pagerank_cache=pagerank_cache
            )
            
            enriched_alerts = alert_processor.enrich_alerts(alerts)
            consolidated_groups = alert_processor.group_alerts_by_relationships()
            
            # ============================================================
            # STEP 3: Feature Engineering
            # ============================================================
            feature_matrix_scaled, scaler, pca, feature_names = alert_processor.engineer_features()
            
            # ============================================================
            # STEP 4: Cluster Prediction (using trained cluster model)
            # ============================================================
            # Check if model exists, if not train one
            if self.model_manager.current_version == 0:
                print("\n No trained model found - training initial model...")
                self.model_manager.train_new_model(
                    feature_matrix_scaled=feature_matrix_scaled,
                    alerts=enriched_alerts,
                    scaler=scaler,
                    pca=pca,
                    feature_names=feature_names,
                    algorithm='kmeans'
                )
            
            # Load current model and predict
            self.cluster_predictor.load_current_model()
            cluster_labels, clustering_method, avg_confidence = self.cluster_predictor.predict_clusters(
                feature_matrix_scaled
            )
            
            # Assign cluster labels to alerts
            alert_processor.assign_cluster_labels(cluster_labels, clustering_method)
            
            # ============================================================
            # STEP 5: Deduplication
            # ============================================================
            deduplicated_alerts = alert_processor.deduplicate_alerts()
            
            # ============================================================
            # STEP 6: Ranking and Naming
            # ============================================================
            cluster_metadata = alert_processor.rank_and_name_clusters()
            
            # ============================================================
            # STEP 7: Save Results
            # ============================================================
            clustering_stats = {
                'method': clustering_method,
                'n_clusters': len(set(cluster_labels)),
                'avg_confidence': avg_confidence,
                'model_version': self.cluster_predictor.model_version
            }
            
            output_paths = self.result_storage.save_results(
                run_id=run_id,
                enriched_alerts=alert_processor.enriched_alerts,
                cluster_metadata=cluster_metadata,
                deduplicated_alerts=deduplicated_alerts,
                clustering_stats=clustering_stats
            )
            
            # ============================================================
            # Summary
            # ============================================================
            end_time = time.time()
            duration = end_time - start_time
            
            print("\n" + "=" * 20)
            print("WORKFLOW COMPLETED SUCCESSFULLY")
            print("=" * 20)
            print(f"Run ID: {run_id}")
            print(f"Duration: {duration:.2f} seconds")
            print(f"Total alerts processed: {len(alerts)}")
            print(f"Unique alerts (after dedup): {len(deduplicated_alerts)}")
            print(f"Total clusters: {len(cluster_metadata)}")
            print(f"Model version: v{self.cluster_predictor.model_version}")
            print(f"Clustering method: {clustering_method}")
            if avg_confidence:
                print(f"Average confidence: {avg_confidence:.3f}")
            print(f"\nResults saved to: {self.output_dir}/{run_id}")
            print("=" * 20)
            
            self.last_run_id = run_id
            self.last_run_status = 'success'
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            print("\n" + "=" * 20)
            print("WORKFLOW FAILED")
            print("=" * 20)
            print(f"Run ID: {run_id}")
            print(f"Duration: {duration:.2f} seconds")
            print(f"Error: {str(e)}")
            print("=" * 20)
            
            self.last_run_id = run_id
            self.last_run_status = 'failed'
            raise
    
    def retrain_model(self):
        """Retrain clustering model with latest data"""
        print("\n" + "=" * 20)
        print("MODEL RETRAINING")
        print("=" * 20)
        
        start_time = time.time()
        
        try:
            # Load data
            print("\n[Retraining] Loading training data...")
            alerts = self.data_ingestion.load_firing_alerts(self.alerts_csv_path)   # todo: only load new alerts
            service_graph, service_to_graph = self.data_ingestion.load_graph_data(self.graph_json_path)   # todo: only load new graph data
            
            if len(alerts) < 100:
                print(" Insufficient data for retraining (need at least 100 alerts)")
                return
            
            service_features_cache = self.data_ingestion.get_service_features_cache()
            pagerank_cache = self.data_ingestion.get_pagerank_cache()
            
            # Process alerts
            alert_processor = AlertProcessor(
                service_graph=service_graph,
                service_to_graph=service_to_graph,
                service_features_cache=service_features_cache,
                pagerank_cache=pagerank_cache
            )
            
            enriched_alerts = alert_processor.enrich_alerts(alerts)
            alert_processor.group_alerts_by_relationships()
            feature_matrix_scaled, scaler, pca, feature_names = alert_processor.engineer_features()
            
            # Train new model
            print("\n[Retraining] Training new model...")
            version, metadata = self.model_manager.train_new_model(
                feature_matrix_scaled=feature_matrix_scaled,
                alerts=enriched_alerts,
                scaler=scaler,
                pca=pca,
                feature_names=feature_names,
                algorithm='kmeans'
            )
            
            # Compare with current model
            current_version = self.model_manager.current_version - 1  # Previous version
            if current_version > 0:
                comparison = self.model_manager.compare_versions(current_version, version)
                
                print(f"\n[Retraining] Model Comparison:")
                print(f"  v{current_version} -> v{version}")
                print(f"  Silhouette improvement: {comparison['silhouette_improvement']:.3f}")
                print(f"  Cluster count change: {comparison['cluster_count_change']}")
                print(f"  Recommended: v{comparison['recommended']}")
                
                if comparison['silhouette_improvement'] > 0.05:
                    print(f"\n Promoting v{version} to production")
                    self.cluster_predictor.reload_model(version)
                else:
                    print(f"\n Keeping v{current_version} (new model not significantly better)")
                    self.model_manager.current_version = current_version
            else:
                # First model
                print(f"\n v{version} is now the active model")
                self.cluster_predictor.reload_model(version)
            
            end_time = time.time()
            duration = end_time - start_time
            
            print("\n" + "=" * 20)
            print("RETRAINING COMPLETED")
            print("=" * 20)
            print(f"Duration: {duration:.2f} seconds")
            print(f"New model: v{version}")
            print("=" * 20)
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            print("\n" + "=" * 20)
            print("RETRAINING FAILED")
            print("=" * 20)
            print(f"Duration: {duration:.2f} seconds")
            print(f"Error: {str(e)}")
            print("=" * 20)
            
            raise
    
    def run_once(self):
        """Run the workflow (manual trigger)"""
        self.run_workflow()
    
    def stop(self):
        """Stop the scheduler"""
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            print("\n Scheduler stopped")
    
    def get_status(self) -> Dict:
        """Get current scheduler status"""
        return {
            'is_running': self.is_running,
            'last_run_id': self.last_run_id,
            'last_run_status': self.last_run_status,
            'current_model_version': self.model_manager.current_version,
            'scheduled_jobs': len(self.scheduler.get_jobs()) if self.is_running else 0
        }
    
    def get_next_run_time(self):
        """Get the next scheduled run time"""
        if not self.is_running:
            return None
        
        jobs = self.scheduler.get_jobs()
        if not jobs:
            return None
        
        workflow_job = self.scheduler.get_job('alert_classification_workflow')
        if workflow_job:
            return workflow_job.next_run_time
        
        return None


def main():
    """Main entry point for running the scheduler"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Alert Classification Workflow Scheduler')
    parser.add_argument('--alerts', type=str, required=True, help='Path to alerts CSV file')
    parser.add_argument('--graph', type=str, required=True, help='Path to service graph JSON file')
    parser.add_argument('--output', type=str, default='results', help='Output directory')
    parser.add_argument('--models', type=str, default='data/models', help='Model directory')
    parser.add_argument('--interval', type=int, default=15, help='Workflow interval in minutes')
    parser.add_argument('--no-retraining', action='store_true', help='Disable weekly retraining')
    parser.add_argument('--run-once', action='store_true', help='Run once and exit (no scheduling)')
    
    args = parser.parse_args()
    
    # Initialize scheduler
    scheduler = AlertWorkflowScheduler(
        alerts_csv_path=args.alerts,
        graph_json_path=args.graph,
        output_dir=args.output,
        model_dir=args.models
    )
    
    if args.run_once:
        # Run once and exit (manual trigger)
        scheduler.run_once()
    else:
        # Start scheduled execution
        scheduler.start(
            workflow_interval_minutes=args.interval,
            enable_retraining=not args.no_retraining
        )
        
        try:
            # Keep the script running
            while True:
                time.sleep(60)
                
                # Print status every 5 minutes
                if datetime.now().minute % 5 == 0:
                    status = scheduler.get_status()
                    next_run = scheduler.get_next_run_time()
                    print(f"\n[Status] Running: {status['is_running']}, "
                          f"Last: {status['last_run_id']} ({status['last_run_status']}), "
                          f"Model: v{status['current_model_version']}, "
                          f"Next run: {next_run}")
        
        except KeyboardInterrupt:
            print("\n\n[Scheduler] Received shutdown signal...")
            scheduler.stop()
            print("[Scheduler] Shutdown complete")


if __name__ == "__main__":
    main()
