"""
alert classification workflow scheduler: Orchestrates the complete alert classification workflow and schedules customized execution.
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
from catalog_manager import CatalogManager
from config import Config
import os
import glob
import shutil
import pandas as pd
import requests


class AlertWorkflowScheduler:
    """
    Orchestrates the complete alert classification workflow:
        1. Every 5 minutes: Run classification pipeline
        2. Weekly: Retrain clustering model
    """
    
    def __init__(self, alerts_csv_path: str, graph_json_path: str, 
                 output_dir: str = 'results', model_dir: str = 'data/models',
                 catalog_path: str = 'data/models/cluster_catalog.json'):
        """
        Initialize the workflow scheduler
        
        Args:
            alerts_csv_path: Path to alerts CSV file in azure blob storage
            graph_json_path: Path to service graph JSON file in azure blob storage
            output_dir: Directory to store results in azure blob storage
            model_dir: Directory to store models in azure blob storage
            catalog_path: Path to cluster catalog JSON file for persistent tracking
        """
        # self.alerts_csv_path = alerts_csv_path
        self.graph_json_path = graph_json_path
        self.output_dir = output_dir
        self.model_dir = model_dir
        self.catalog_path = catalog_path
        self.data_ingestion = DataIngestionService()
        self.model_manager = ModelManager(model_dir=model_dir)
        self.catalog_manager = CatalogManager(catalog_path=catalog_path)
        self.cluster_predictor = ClusterPredictor(self.model_manager, self.catalog_manager)
        self.result_storage = ResultStorage(output_dir=output_dir)
        self.scheduler = BackgroundScheduler()
        self.is_running = False
        self.last_run_id = None
        self.last_run_status = None
    
    def start(self, workflow_interval_minutes: int = 5, enable_retraining: bool = True):
        """
        Start the scheduler
        
        Args:
            workflow_interval_minutes: Interval for running classification workflow (default: 5 minutes)
            enable_retraining: Whether to enable weekly model retraining (default: True)
        """
        self.scheduler.add_job(
            func=self.run_workflow,
            trigger=IntervalTrigger(minutes=workflow_interval_minutes),
            id='alert_classification_workflow',
            name='Alert Classification Workflow',
            max_instances=1,  # Prevent overlapping runs
            replace_existing=True
        )
        
        print(f"\n Scheduled workflow: Every {workflow_interval_minutes} minutes")
        if enable_retraining:
            self.scheduler.add_job(
                func=self.retrain_model,
                trigger=CronTrigger(day_of_week='mon', hour=7, minute=0),
                id='model_retraining',
                name='Model Retraining',
                replace_existing=True
            )
            print(" Scheduled retraining: mondays at 7:00 AM")
        self.scheduler.start()
        self.is_running = True
    
    def _trigger_data_export(self) -> bool:
        """
        Trigger the data extraction API to export alert data from MongoDB to parquet files.
        
        Returns:
            bool: True if API call succeeded, False otherwise
        """
        try:
            print(f"\n Triggering data export from API: {Config.CURL_ALERT_EXPORT_URL}")
            response = requests.get(
                Config.CURL_ALERT_EXPORT_URL,
                timeout=Config.CURL_REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f" Data export successful: {result.get('message', 'OK')}")
                if 'count' in result:
                    print(f" Exported {result['count']} alerts")
                if 'filePath' in result:
                    print(f" Output file: {result['filePath']}")
                return True
            else:
                print(f" Data export API returned status {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.ConnectionError:
            print(f" Cannot connect to data extraction API at {Config.CURL_ALERT_EXPORT_URL}")
            print(" Make sure the API server is running: cd data_extraction_api && npm start")
            return False
        except requests.exceptions.Timeout:
            print(f" Data export API request timed out after {Config.CURL_REQUEST_TIMEOUT}s")
            return False
        except Exception as e:
            print(f" Data export API error: {e}")
            return False
    
    def run_workflow(self):
        """Execute the complete alert classification workflow"""
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        print("\n" + "=" * 20)
        print(f"WORKFLOW RUN: {run_id}")
        print("=" * 20)
        
        start_time = time.time()
        
        try:
            # Step 1: Trigger data export from MongoDB via API
            api_success = self._trigger_data_export()
            if not api_success:
                print(" Warning: Data export API call failed, will check for existing parquet files...")
            
            # Load catalog at start of workflow
            print("\n Loading cluster catalog...")
            self.catalog_manager.load_catalog()
            print(f" Loaded catalog: {len(self.catalog_manager.cluster_catalog)} existing clusters")
            
            # Pick parquet files from output folder defined in config
            output_dir = Config.PARQUET_IN_DIR
            archive_dir = os.path.join(output_dir, 'archive')
            os.makedirs(archive_dir, exist_ok=True)
            parquet_files = sorted(
                glob.glob(os.path.join(output_dir, '*.parquet'))
            )
            alerts = []
            
            if parquet_files:
                print(f"\n Found {len(parquet_files)} parquet file(s)")
                for parquet_file in parquet_files:
                    print(f"  Processing parquet file: {parquet_file}")
                    df = pd.read_parquet(parquet_file)
                    alerts.extend(df.to_dict('records'))
                    # Move processed file to archive
                    dest_path = os.path.join(archive_dir, os.path.basename(parquet_file))
                    shutil.move(parquet_file, dest_path)
                    print(f"  Archived processed file: {dest_path}")
            else:
                # Fallback to CSV if no parquet files found
                csv_path = os.path.join('..', 'local_data', 'saafe_alerts.csv')
                if os.path.exists(csv_path):
                    print(f"\n No parquet files found, loading from CSV: {csv_path}")
                    df = pd.read_csv(csv_path)
                    alerts.extend(df.to_dict('records'))
                    print(f"  Loaded {len(alerts)} alerts from CSV")
                else:
                    print(f"\n No parquet files found in: {output_dir}")
                    print(" Ensure the data extraction API exported data successfully")
            
            service_graph, service_to_graph = self.data_ingestion.load_graph_data(self.graph_json_path)
            
            if len(alerts) == 0:
                print("\n No alerts to process - skipping workflow")
                self.last_run_status = 'skipped_no_alerts'
                return
            
            service_features_cache = self.data_ingestion.get_service_features_cache()
            pagerank_cache = self.data_ingestion.get_pagerank_cache()
            
            # Create processor for ALL alerts (needed for enrichment)
            alert_processor = AlertProcessor(
                service_graph=service_graph,
                service_to_graph=service_to_graph,
                service_features_cache=service_features_cache,
                pagerank_cache=pagerank_cache
            )
            
            enriched_alerts = alert_processor.enrich_alerts(alerts)
            
            # Try to match alerts to existing catalog clusters
            print("\n Matching alerts to existing catalog clusters...")
            matched_alerts, unmatched_alerts = self.catalog_manager.match_alerts_to_catalog(enriched_alerts)
            print(f" Matched: {len(matched_alerts)} alerts to existing clusters")
            print(f" Unmatched: {len(unmatched_alerts)} alerts need new cluster assignment")
            
            # Determine target PCA components from existing model (for stability)
            target_pca_components = None
            if self.model_manager.current_version > 0:
                try:
                    loaded_model = self.model_manager.load_model()
                    target_pca_components = loaded_model['metadata'].get('pca_components', None)
                    print(f"\n Using fixed PCA components from model: {target_pca_components}")
                except Exception as e:
                    print(f"\n Could not load model metadata: {e}")
                    # Fallback to config default
                    target_pca_components = Config.FIXED_PCA_COMPONENTS
            else:
                # No model exists - use config default for initial training
                target_pca_components = Config.FIXED_PCA_COMPONENTS
                print(f"\n Using default PCA components from config: {target_pca_components}")
            
            # Initialize variables for tracking
            clustering_method = 'catalog_match'
            avg_confidence = None
            new_cluster_metadata = []
            
            # CASE 1: All alerts matched - no clustering needed
            if len(unmatched_alerts) == 0:
                print("\n All alerts matched to existing catalog - no clustering needed")
                all_deduplicated = matched_alerts
                clustering_method = 'catalog_match'
                avg_confidence = 1.0
                
            # CASE 2: No model exists - train on ALL alerts
            elif self.model_manager.current_version == 0:
                print("\n No trained model found - training initial model with all alerts...")
                
                # Process ALL alerts for initial training with fixed PCA components
                consolidated_groups = alert_processor.group_alerts_by_relationships()
                feature_matrix_scaled, scaler, pca, feature_names = alert_processor.engineer_features(
                    target_pca_components=target_pca_components
                )
                
                print("\n" + "="*60)
                print("TRAINING NEW MODEL")
                print("="*60)
                
                n_clusters = max(5, min(len(enriched_alerts) // 5, 20))
                print(f" Number of clusters: {n_clusters}")
                print(f" Training data shape: {feature_matrix_scaled.shape}")
                print(f" Features: {feature_names}")
                
                # Save model with training
                cleaned_alerts = alert_processor.alerts_df.to_dict('records')
                version, metadata, cluster_labels = self.model_manager.train_new_model(
                    feature_matrix_scaled=feature_matrix_scaled,
                    alerts=cleaned_alerts,
                    scaler=scaler,
                    pca=pca,
                    feature_names=feature_names,
                    algorithm='kmeans',
                    n_clusters=n_clusters
                )
                print(" Model trained and saved successfully")
                
                # Assign cluster labels to alerts
                alert_processor.assign_cluster_labels(cluster_labels, 'kmeans')
                all_deduplicated = alert_processor.deduplicate_alerts()
                new_cluster_metadata = alert_processor.rank_and_name_clusters()
                clustering_method = 'kmeans'
                avg_confidence = 0.0
                
                # Update catalog with ALL clusters from initial training
                print("\n Updating catalog with initial cluster patterns...")
                self.catalog_manager.update_catalog_with_clusters(all_deduplicated)
                
            # CASE 3: Model exists - cluster only unmatched alerts
            else:
                print(f"\n Processing {len(unmatched_alerts)} unmatched alerts...")
                
                # Create a separate processor for ONLY unmatched alerts
                unmatched_processor = AlertProcessor(
                    service_graph=service_graph,
                    service_to_graph=service_to_graph,
                    service_features_cache=service_features_cache,
                    pagerank_cache=pagerank_cache
                )
                
                # Enrich and process only unmatched alerts
                unmatched_processor.enrich_alerts(unmatched_alerts)
                unmatched_processor.group_alerts_by_relationships()
                
                # Use FIXED PCA components from existing model for stability
                feature_matrix_scaled, scaler, pca, feature_names = unmatched_processor.engineer_features(
                    target_pca_components=target_pca_components
                )
                
                print(f"\n Feature engineering complete: {feature_matrix_scaled.shape[1]} PCA components")
                
                # Load model and check compatibility
                loaded_model = self.model_manager.load_model()
                expected_pca = loaded_model['metadata'].get('pca_components', 0)
                actual_pca = feature_matrix_scaled.shape[1]
                
                print(f" Model expects {expected_pca} PCA components, got {actual_pca}")
                
                # If still mismatched after using fixed components, retrain with config default
                if expected_pca != actual_pca:
                    print(f"\n PCA mismatch persists - RETRAINING MODEL with {Config.FIXED_PCA_COMPONENTS} fixed components")
                    print("="*60)
                    print("TRAINING NEW MODEL")
                    print("="*60)
                    
                    n_clusters = max(5, min(len(enriched_alerts) // 5, 20))
                    
                    # Train on ALL alerts for new model with fixed PCA from config
                    full_processor = AlertProcessor(
                        service_graph=service_graph,
                        service_to_graph=service_to_graph,
                        service_features_cache=service_features_cache,
                        pagerank_cache=pagerank_cache
                    )
                    full_processor.enrich_alerts(alerts)
                    full_processor.group_alerts_by_relationships()
                    full_feature_matrix, full_scaler, full_pca, full_feature_names = full_processor.engineer_features(
                        target_pca_components=Config.FIXED_PCA_COMPONENTS
                    )
                    
                    cleaned_alerts = full_processor.alerts_df.to_dict('records')
                    version, metadata, cluster_labels = self.model_manager.train_new_model(
                        feature_matrix_scaled=full_feature_matrix,
                        alerts=cleaned_alerts,
                        scaler=full_scaler,
                        pca=full_pca,
                        feature_names=full_feature_names,
                        algorithm='kmeans',
                        n_clusters=n_clusters
                    )
                    print(" Model trained and saved successfully")
                    
                    full_processor.assign_cluster_labels(cluster_labels, 'kmeans')
                    all_deduplicated = full_processor.deduplicate_alerts()
                    new_cluster_metadata = full_processor.rank_and_name_clusters()
                    clustering_method = 'kmeans'
                    avg_confidence = 0.0
                    
                    # For retraining: clear catalog and rebuild
                    print("\n Rebuilding catalog with new model clusters...")
                    self.catalog_manager.clear_catalog()
                    self.catalog_manager.update_catalog_with_clusters(all_deduplicated)
                    
                else:
                    # Feature validation passed - use existing model
                    print("\n" + "="*60)
                    print("USING EXISTING MODEL")
                    print("="*60)
                    print(f" Model version: {self.model_manager.current_version}")
                    
                    self.cluster_predictor.load_current_model()
                    
                    try:
                        # Predict clusters for unmatched alerts
                        cluster_labels, clustering_method, avg_confidence = self.cluster_predictor.predict_clusters(
                            feature_matrix_scaled, feature_names
                        )
                        
                        # Offset cluster labels to avoid collision with catalog clusters
                        next_cluster_id = self.catalog_manager.next_cluster_id
                        cluster_labels = cluster_labels + next_cluster_id
                        
                    except ValueError as e:
                        print(f"\n ERROR: {e}")
                        raise
                    
                    # Assign labels to unmatched alerts
                    unmatched_processor.assign_cluster_labels(cluster_labels, clustering_method)
                    unmatched_deduplicated = unmatched_processor.deduplicate_alerts()
                    new_cluster_metadata = unmatched_processor.rank_and_name_clusters()
                    
                    # Combine matched (already have cluster info) + newly clustered
                    all_deduplicated = matched_alerts + unmatched_deduplicated
                    
                    # Update catalog ONLY with newly created clusters (not matched ones)
                    print(f"\n Updating catalog with {len(new_cluster_metadata)} new cluster patterns...")
                    self.catalog_manager.update_catalog_with_clusters(unmatched_deduplicated)
            
            # Save catalog for next run
            print(" Saving cluster catalog...")
            self.catalog_manager.save_catalog()
            
            # Build cluster metadata from combined results
            all_cluster_metadata = new_cluster_metadata  # New clusters from this run
            
            # Add matched cluster info from catalog
            matched_cluster_ids = set()
            for alert in matched_alerts:
                cid = alert.get('cluster_id')
                if cid is not None and cid not in matched_cluster_ids:
                    matched_cluster_ids.add(cid)
                    if cid in self.catalog_manager.cluster_catalog:
                        cat_info = self.catalog_manager.cluster_catalog[cid]
                        all_cluster_metadata.append({
                            'cluster_id': cid,
                            'cluster_name': cat_info.get('cluster_name', f'catalog_{cid}'),
                            'alert_count': sum(1 for a in matched_alerts if a.get('cluster_id') == cid),
                            'source': 'catalog_match'
                        })
            
            clustering_stats = {
                'method': clustering_method,
                'n_clusters': len(self.catalog_manager.cluster_catalog),
                'avg_confidence': avg_confidence if avg_confidence else 0.0,
                'model_version': self.model_manager.current_version,
                'matched_alerts': len(matched_alerts),
                'unmatched_alerts': len(unmatched_alerts),
                'new_clusters': len(new_cluster_metadata)
            }
            
            output_paths = self.result_storage.save_results(
                run_id=run_id,
                enriched_alerts=enriched_alerts,
                cluster_metadata=all_cluster_metadata,
                deduplicated_alerts=all_deduplicated,
                clustering_stats=clustering_stats
            )
            end_time = time.time()
            duration = end_time - start_time
            
            print("\n" + "=" * 20)
            print("WORKFLOW COMPLETED SUCCESSFULLY")
            print("=" * 20)
            print(f"Run ID: {run_id}")
            print(f"Duration: {duration:.2f} seconds")
            print(f"Total alerts processed: {len(alerts)}")
            print(f"Matched to existing clusters: {len(matched_alerts)}")
            print(f"Newly clustered: {len(unmatched_alerts)}")
            print(f"Unique alerts (after dedup): {len(all_deduplicated)}")
            print(f"New clusters created: {len(new_cluster_metadata)}")
            print(f"Total clusters in catalog: {len(self.catalog_manager.cluster_catalog)}")
            print(f"Model version: v{self.model_manager.current_version}")
            print(f"Clustering method: {clustering_method}")
            if avg_confidence and avg_confidence > 0:
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
            print("\n[Retraining] Loading training data...")
            alerts = self.data_ingestion.load_firing_alerts(self.alerts_csv_path)   # todo: only load new alerts
            service_graph, service_to_graph = self.data_ingestion.load_graph_data(self.graph_json_path)   # todo: only load new graph data
            
            if len(alerts) < 100:
                print(" Insufficient data for retraining (need at least 100 alerts)")
                return
            
            service_features_cache = self.data_ingestion.get_service_features_cache()
            pagerank_cache = self.data_ingestion.get_pagerank_cache()
            alert_processor = AlertProcessor(
                service_graph=service_graph,
                service_to_graph=service_to_graph,
                service_features_cache=service_features_cache,
                pagerank_cache=pagerank_cache
            )
            
            enriched_alerts = alert_processor.enrich_alerts(alerts)
            alert_processor.group_alerts_by_relationships()
            feature_matrix_scaled, scaler, pca, feature_names = alert_processor.engineer_features()
            print("\n[Retraining] Training new model...")
            cleaned_alerts = alert_processor.alerts_df.to_dict('records')
            version, metadata = self.model_manager.train_new_model(
                feature_matrix_scaled=feature_matrix_scaled,
                alerts=cleaned_alerts,
                scaler=scaler,
                pca=pca,
                feature_names=feature_names,
                algorithm='kmeans'
            )
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
    parser.add_argument('--graph', type=str, required=True, help='Path to service graph JSON file')
    parser.add_argument('--output', type=str, default='results', help='Output directory')
    parser.add_argument('--models', type=str, default='data/models', help='Model directory')
    parser.add_argument('--interval', type=int, default=1, help='Workflow interval in minutes')
    parser.add_argument('--no-retraining', action='store_true', help='Disable weekly retraining')
    parser.add_argument('--run-once', action='store_true', help='Run once and exit (no scheduling)')
    
    args = parser.parse_args()
    scheduler = AlertWorkflowScheduler(
        alerts_csv_path=None,  # Not used anymore
        graph_json_path=args.graph,
        output_dir=args.output,
        model_dir=args.models,
        catalog_path=os.path.join(args.models, 'cluster_catalog.json')
    )
    
    if args.run_once:
        scheduler.run_once()
    else:
        scheduler.start(
            workflow_interval_minutes=args.interval,
            enable_retraining=not args.no_retraining
        )
        
        try:
            while True:
                time.sleep(60)
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
