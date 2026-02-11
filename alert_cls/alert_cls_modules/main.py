import logging
import sys
import os
import threading
from job_scheduler import main as scheduler_main
from health_server import app as health_app
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
def start_health_server():
    # Run the flask health server in a separate thread
    # threaded=True inside Flask isn't needed here because we just need a simple endpoint
    health_app.run(host="0.0.0.0", port=8080)
def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("\n" + "="*60)
    logger.info("      Starting Scheduler")
    logger.info("="*60 + "\n")

    try:
        threading.Thread(target=start_health_server, name="health-server", daemon=True).start()
        scheduler = scheduler_main()
        scheduler.start()
    except Exception as e:
        logger.error(f"Scheduler failed to start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
