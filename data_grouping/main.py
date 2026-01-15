import logging
import sys
import os

# Add current directory to path for local imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from job_scheduler import main as scheduler_main

def setup_logging():
    """Configure logging for the scheduler application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

def main():
    """Main entry point for the scheduler."""
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("\n" + "="*60)
    logger.info("      Starting Scheduler")
    logger.info("="*60 + "\n")

    try:
        scheduler = scheduler_main()
        scheduler.start()
    except Exception as e:
        logger.error(f"Scheduler failed to start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
