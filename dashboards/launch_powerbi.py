"""
F1 Race Intelligence Lakehouse - Dashboard Launcher
Mocks the launch of Power BI dashboards or local analytical views
"""

import webbrowser
import os
import sys

def launch_dashboard(name: str):
    """Launch a specific dashboard in the browser"""
    dashboards = {
        "driver": "https://app.powerbi.com/view?r=driver_intelligence_mock_id",
        "constructor": "https://app.powerbi.com/view?r=constructor_analytics_mock_id",
        "strategy": "https://app.powerbi.com/view?r=race_strategy_mock_id"
    }
    
    url = dashboards.get(name.lower())
    if url:
        print(f"Launching {name} Intelligence Dashboard...")
        # In a real environment, we would use the Power BI Embedded API
        # Here we mock it by opening a placeholder or a local documentation page
        webbrowser.open(url)
    else:
        print(f"Dashboard '{name}' not found.")

def main():
    print("="*50)
    print("F1 RACE INTELLIGENCE DASHBOARD SYSTEM")
    print("="*50)
    print("1. Driver Intelligence")
    print("2. Constructor Analytics")
    print("3. Race Strategy")
    print("4. Exit")
    
    choice = input("\nSelect a dashboard to launch (1-4): ")
    
    if choice == "1":
        launch_dashboard("driver")
    elif choice == "2":
        launch_dashboard("constructor")
    elif choice == "3":
        launch_dashboard("strategy")
    elif choice == "4":
        sys.exit()
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()
