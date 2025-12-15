"""
Main Entry Point for Alert Classification

Usage:
    # Run with custom configuration
    python main.py --alerts alert_data.csv --graph graph_data.json --interval 15 --output results --models data/models
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import main as scheduler_main

if __name__ == "__main__":
    scheduler_main()

