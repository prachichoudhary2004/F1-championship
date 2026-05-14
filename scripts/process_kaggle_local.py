"""
F1 Race Intelligence Lakehouse - Local Fallback Pipeline (Pandas)
Processes real Kaggle data into Silver/Gold layers for local demo
Use this if Spark/Hadoop is not configured on your local Windows machine
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

def run_local_pipeline():
    print("Starting Local Fallback Pipeline (Pandas)...")
    raw_path = "data/raw"
    silver_path = "data/silver"
    gold_path = "data/gold"
    
    # Ensure directories exist
    for p in [silver_path, gold_path]:
        os.makedirs(p, exist_ok=True)

    # 1. Process Drivers (Raw -> Silver)
    print("Processing Drivers...")
    drivers = None
    try:
        # Prefer CSV as it's more standard in Kaggle
        if os.path.exists(f"{raw_path}/drivers.csv"):
            drivers = pd.read_csv(f"{raw_path}/drivers.csv")
        elif os.path.exists(f"{raw_path}/drivers.json"):
            drivers = pd.read_json(f"{raw_path}/drivers.json")
        
        if drivers is not None:
            # Cleanup & Engineering
            # Handle possible missing columns or different case
            drivers.columns = [c.lower() for c in drivers.columns]
            if 'forename' in drivers.columns and 'surname' in drivers.columns:
                drivers['full_name'] = drivers['forename'] + " " + drivers['surname']
            drivers.to_parquet(f"{silver_path}/drivers.parquet", index=False)
        else:
            print("Warning: Drivers data not found.")
    except Exception as e:
        print(f"Error processing drivers: {e}")

    # 2. Process Results/Performance (Raw -> Silver)
    print("Processing Performance...")
    try:
        results = pd.read_csv(f"{raw_path}/results.csv")
        races = pd.read_csv(f"{raw_path}/races.csv")
        constructors = pd.read_csv(f"{raw_path}/constructors.csv")
        
        # Force lowercase for consistency
        results.columns = [c.lower() for c in results.columns]
        races.columns = [c.lower() for c in races.columns]
        constructors.columns = [c.lower() for c in constructors.columns]
        
        # Merge for enrichment
        perf = results.merge(races[['raceid', 'year', 'name', 'date']], on='raceid')
        # Rename constructor name before merge to avoid suffix confusion
        constructors = constructors.rename(columns={'name': 'constructor_name'})
        perf = perf.merge(constructors[['constructorid', 'constructor_name', 'nationality']], 
                         on='constructorid')
        
        perf['podium_finish'] = perf['positionorder'] <= 3
        perf.to_parquet(f"{silver_path}/performance.parquet", index=False)
        
        # 3. Aggregate Driver Statistics (Silver -> Gold)
        print("Engineering Driver Statistics (Gold)...")
        # Ensure points is numeric
        perf['points'] = pd.to_numeric(perf['points'], errors='coerce').fillna(0)
        
        # Latest year stats
        latest_year = perf['year'].max()
        year_perf = perf[perf['year'] == latest_year]
        
        driver_stats = year_perf.groupby(['driverid']).agg({
            'points': 'sum',
            'raceid': 'count',
            'podium_finish': 'sum'
        }).reset_index()
        
        driver_stats.columns = ['driverid', 'total_points', 'total_races', 'total_podiums']
        
        # Get wins
        wins = year_perf[year_perf['positionorder'] == 1].groupby('driverid').size().reset_index(name='total_wins')
        driver_stats = driver_stats.merge(wins, on='driverid', how='left').fillna(0)
        
        driver_stats['win_rate'] = driver_stats['total_wins'] / driver_stats['total_races']
        driver_stats['year'] = latest_year
        
        # Join driver names
        if drivers is not None:
            driver_stats = driver_stats.merge(drivers[['driverid', 'surname']], on='driverid')
        
        driver_stats = driver_stats.sort_values('total_points', ascending=False)
        
        driver_stats.to_parquet(f"{gold_path}/driver_statistics.parquet", index=False)

        # 4. Aggregate Constructor Rankings (Gold)
        print("Engineering Constructor Rankings (Gold)...")
        # Work on a clean copy
        c_df = year_perf[['constructor_name', 'points', 'raceid', 'positionorder']].copy()
        c_df['points'] = pd.to_numeric(c_df['points'], errors='coerce').fillna(0)
        
        # Manual aggregation to avoid mysterious Pandas type conversion errors
        stats_dict = {}
        for idx, row in c_df.iterrows():
            c_name = row['constructor_name']
            if c_name not in stats_dict:
                stats_dict[c_name] = {'total_points': 0, 'race_ids': set(), 'total_wins': 0}
            
            stats_dict[c_name]['total_points'] += row['points']
            stats_dict[c_name]['race_ids'].add(row['raceid'])
            if row['positionorder'] == 1:
                stats_dict[c_name]['total_wins'] += 1
        
        # Convert dict back to DataFrame
        const_stats_list = []
        for name, data in stats_dict.items():
            const_stats_list.append({
                'constructorName': name,
                'total_points': data['total_points'],
                'total_races': len(data['race_ids']),
                'total_wins': data['total_wins']
            })
        
        const_stats = pd.DataFrame(const_stats_list)
        const_stats['team_win_rate'] = const_stats['total_wins'] / const_stats['total_races']
        const_stats['year'] = latest_year
        
        const_stats.to_parquet(f"{gold_path}/constructor_rankings.parquet", index=False)
        
        print("Local Fallback Pipeline completed successfully.")
        print(f"Data processed for Season: {latest_year}")
        
    except Exception as e:
        print(f"Error in pipeline: {e}")

if __name__ == "__main__":
    run_local_pipeline()
