"""
F1 Race Intelligence Lakehouse - Mock Data Generator
Generates high-fidelity mock data for local dashboard demonstrations
Allows the project to be 'runnable' without a full Spark/Hadoop setup on Windows
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def generate_mock_lakehouse():
    """Generate mock data for Bronze, Silver, and Gold layers"""
    print("Starting F1 Mock Data Generation...")
    
    # Ensure directories exist
    for layer in ['bronze', 'silver', 'gold']:
        os.makedirs(f"data/{layer}", exist_ok=True)

    # 1. Generate Drivers (Silver)
    drivers = pd.DataFrame({
        'driverId': [1, 2, 3, 4, 5],
        'forename': ['Max', 'Lewis', 'Fernando', 'Charles', 'Lando'],
        'surname': ['Verstappen', 'Hamilton', 'Alonso', 'Leclerc', 'Norris'],
        'nationality': ['Dutch', 'British', 'Spanish', 'Monegasque', 'British'],
        'full_name': ['Max Verstappen', 'Lewis Hamilton', 'Fernando Alonso', 'Charles Leclerc', 'Lando Norris']
    })
    drivers.to_parquet("data/silver/drivers.parquet", index=False)
    
    # 2. Generate Performance (Silver)
    performance = pd.DataFrame({
        'raceId': np.repeat(range(1, 21), 5),
        'driverId': np.tile([1, 2, 3, 4, 5], 20),
        'year': 2023,
        'position': np.random.randint(1, 20, 100),
        'points': np.random.randint(0, 25, 100),
        'podium_finish': np.random.choice([True, False], 100),
        'constructorName': np.random.choice(['Red Bull', 'Mercedes', 'Ferrari', 'McLaren', 'Aston Martin'], 100)
    })
    performance.to_parquet("data/silver/performance.parquet", index=False)

    # 3. Generate Driver Statistics (Gold)
    driver_stats = pd.DataFrame({
        'driverId': [1, 2, 3, 4, 5],
        'surname': ['Verstappen', 'Hamilton', 'Alonso', 'Leclerc', 'Norris'],
        'total_wins': [15, 0, 0, 1, 0],
        'total_podiums': [18, 5, 8, 4, 6],
        'win_rate': [0.75, 0.0, 0.0, 0.05, 0.0],
        'total_points': [454, 210, 206, 188, 175],
        'year': 2023
    })
    driver_stats.to_parquet("data/gold/driver_statistics.parquet", index=False)

    # 4. Generate Constructor Rankings (Gold)
    constructor_stats = pd.DataFrame({
        'constructorName': ['Red Bull', 'Mercedes', 'Ferrari', 'McLaren', 'Aston Martin'],
        'total_points': [750, 410, 380, 250, 230],
        'total_wins': [18, 0, 1, 0, 0],
        'team_win_rate': [0.9, 0.0, 0.05, 0.0, 0.0],
        'year': 2023
    })
    constructor_stats.to_parquet("data/gold/constructor_rankings.parquet", index=False)

    print("Success: Mock Lakehouse populated in data/ directory.")

if __name__ == "__main__":
    generate_mock_lakehouse()
