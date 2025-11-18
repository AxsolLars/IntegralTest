from scripts.connection.connectToSnowflake import connect_to_bok
import os 
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib
import numpy as np
import requests
from datetime import datetime
from collections import defaultdict
matplotlib.use("Agg")

def plot_comparison_data():
    conn = connect_to_bok()
    os.makedirs(f"scripts/plots/comparison", exist_ok=True)
    
    query = f"""
        SELECT
            DATE(timestamp) AS date,
            DATE_TRUNC('hour', timestamp) AS hour,
            MAX_BY(cumulative_sum, timestamp) -
            MIN_BY(cumulative_sum, timestamp) AS diff
        FROM power_production_sum
        GROUP BY
            date,
            hour
        ORDER BY
            date,
            hour;
    """

    df = pd.read_sql(query, conn)
    
    production_values = {}
    for date, group in df.groupby("DATE"):
        production_values[str(date)] = dict(zip(
        group["HOUR"].dt.hour, 
        group["DIFF"]          
    ))
    print(production_values['2025-10-21'])
    start_date = min(production_values.keys())     
    url = "https://api.open-meteo.com/v1/forecast"
    
    params = {
        "latitude": "52.2799",
        "longitude": "8.0472",
        "hourly": "shortwave_radiation,cloud_cover",
        "start_date": f"{start_date}",
        "end_date":
        f"{date.today().isoformat()}"
    }
    
    response = requests.get(url, params=params)
    data = response.json()["hourly"]
    times = data["time"]
    radiation = data["shortwave_radiation"]
    cloud = data["cloud_cover"]
    
    cloud_cover_data = defaultdict(dict)
    radiation_data = defaultdict(dict)
    for t, r in zip(times, radiation):
        dt = datetime.fromisoformat(t)
        date_str = dt.date().isoformat()
        hour = dt.hour
        radiation_data[date_str][hour] = r

    for t, r in zip(times, cloud):
        dt = datetime.fromisoformat(t)
        date_str = dt.date().isoformat()
        hour = dt.hour
        cloud_cover_data[date_str][hour] = r
    
    print("production_values", production_values)
    print("radiation_data", radiation_data)
    print("cloud_cover_data", cloud_cover_data)
    
    
    timeline = []

    prod_list = []
    rad_list = []
    cloud_list = []
    for date, hour_dict in production_values.items():
        for hour in sorted(hour_dict.keys()):
            timeline.append(datetime.fromisoformat(f"{date}T{hour:02d}:00"))
            
    for dt in timeline:
        d = dt.date().isoformat()
        h = dt.hour
        
        prod_list.append(production_values[d][h])
        rad_list.append(radiation_data.get(d, {}).get(h, None))
        cloud_list.append(cloud_cover_data.get(d, {}).get(h, None))
        
    
    fig, ax1 = plt.subplots(figsize=(12, 6))

    color_prod = "tab:blue"
    ax1.plot(timeline, prod_list, linewidth=1.4, color=color_prod)
    ax1.set_ylabel("Production", color = color_prod)
    
    color_rad = "tab:green"
    ax2 = ax1.twinx()
    ax2.plot(timeline, rad_list, linewidth=1.4, color=color_rad)
    ax2.set_ylabel("Shortwave Radiation", color=color_rad)
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"scripts/plots/comparison/production_radiation.png", dpi=200)
    plt.close()
    fig, ax1 = plt.subplots(figsize=(12, 6))

    color_prod = "tab:blue"
    ax1.plot(timeline, prod_list, linewidth=1.4, color=color_prod)
    ax1.set_ylabel("Production", color = color_prod)
    
    color_cloud = "tab:orange"
    ax2 = ax1.twinx()
    ax2.plot(timeline, cloud_list, linewidth=1.4, color=color_cloud)
    ax2.set_ylabel("Cloud Cover", color = color_cloud)
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"scripts/plots/comparison/production_cloud.png", dpi=200)
    plt.close()
    

if __name__ == "__main__":
    plot_comparison_data()