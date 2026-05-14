"""
F1 Race Intelligence Lakehouse - Data Downloader
Downloads historical F1 data from Ergast API / Kaggle sources
"""

import os
import requests
import zipfile
import logging
from typing import List

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_SOURCES = {
    "circuits": "http://ergast.com/api/f1/circuits.json?limit=100",
    "drivers": "http://ergast.com/api/f1/drivers.json?limit=1000",
    "races": "http://ergast.com/api/f1/races.json?limit=2000"
}

def download_file(url: str, target_path: str):
    """Download a file from URL"""
    try:
        logger.info(f"Downloading from {url}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"Successfully saved to {target_path}")
    except Exception as e:
        logger.error(f"Failed to download {url}: {str(e)}")

def main():
    """Main download process"""
    logger.info("Starting F1 data download process...")
    
    # In a real scenario, we would download CSVs or use an API client
    # For this project, data is already partially present in data/raw
    
    raw_path = "data/raw"
    os.makedirs(raw_path, exist_ok=True)
    
    logger.info(f"Verification: {len(os.listdir(raw_path))} files/directories found in {raw_path}")
    
    # Instruction for the user
    print("\n" + "="*50)
    print("F1 DATA DOWNLOADER")
    print("="*50)
    print("The project uses historical F1 data.")
    print(f"Current raw data directory: {os.path.abspath(raw_path)}")
    print("\nTo refresh data, please visit: https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020")
    print("And place the CSV files in the 'data/raw' directory.")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()
