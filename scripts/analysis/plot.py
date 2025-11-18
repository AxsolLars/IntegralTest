from scripts.connection.connectToSnowflake import connect_to_bok
import os 
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib
import numpy as np
matplotlib.use("Agg")

def plot_series(column, df, tableName):
    unique_dates = pd.unique(df["date_only"])
    tick_positions = [
    df.loc[df["date_only"] == d, "TIMESTAMP"].iloc[0] for d in unique_dates
    ]

    tick_labels = [d.strftime("%d.%m") for d in unique_dates]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df["TIMESTAMP"], df[column], marker=".", ms=1, lw=0.5)
    ax.set_title(f"{column} over time")
    ax.set_xlabel("Datum")
    ax.set_ylabel(column)

    # Force every label to show
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=45, ha="right")

    plt.tight_layout()
    print("plotted")
    plt.savefig(f"scripts/plots/{tableName}/{column}.png", dpi=200)
    plt.close(fig)


def plot_overlay(df, y_left, y_right, title, filename, tableName):
    fig, ax1 = plt.subplots(figsize=(10, 5))
    unique_dates = pd.unique(df["date_only"])
    tick_positions = [
    df.loc[df["date_only"] == d, "TIMESTAMP"].iloc[0] for d in unique_dates
    ]

    tick_labels = [d.strftime("%d.%m") for d in unique_dates]
    # Left axis
    color_left = "tab:blue"
    ax1.set_xlabel("Datum")
    ax1.set_ylabel(y_left, color=color_left)
    ax1.plot(df["TIMESTAMP"], df[y_left], color=color_left, linewidth=0.8)
    ax1.tick_params(axis="y", labelcolor=color_left)
    ax1.set_xticks(tick_positions)
    ax1.set_xticklabels(tick_labels, rotation=45, ha="right")
    # Right axis
    ax2 = ax1.twinx()
    color_right = "tab:orange"
    ax2.set_ylabel(y_right, color=color_right)
    ax2.plot(df["TIMESTAMP"], df[y_right], color=color_right, linewidth=0.8)
    ax2.tick_params(axis="y", labelcolor=color_right)
    ax2.set_xticks(tick_positions)
    ax2.set_xticklabels(tick_labels, rotation=45, ha="right")
    plt.title(title)
    fig.tight_layout()
    plt.savefig(f"scripts/plots/{tableName}/{filename}", dpi=200)
    plt.close(fig)
    print(f"✅ Saved {filename}")
    
def plot_table(tableName):
    conn = connect_to_bok()
    os.makedirs(f"scripts/plots/{tableName}", exist_ok=True)
    
    query = f"""
        SELECT
            timestamp,
            cumulative_sum,
            integral,
            value
        FROM power_{tableName}_sum
        ORDER BY timestamp;
        """
    df = pd.read_sql(query, conn)

    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
    df["date_only"] = df["TIMESTAMP"].dt.date
    with pd.option_context('display.max_rows', 1000, 'display.max_columns', None):
        print(df["CUMULATIVE_SUM"])
    plot_series("CUMULATIVE_SUM", df, tableName)
    plot_series("VALUE", df, tableName)
    plot_series("INTEGRAL", df, tableName)
    plot_overlay(df, "CUMULATIVE_SUM", "INTEGRAL", "Cumulative Sum vs Integral", "sum_vs_integral.png", tableName)
    plot_overlay(df, "VALUE", "INTEGRAL", "Value vs Integral", "value_vs_integral.png", tableName)

def plot_consumption_rate():
    conn = connect_to_bok()
    os.makedirs(f"scripts/plots/consumption_rate", exist_ok=True)
    
    query = f"""
        SELECT
            pe.timestamp AS timestamp_power_export,
            pe.cumulative_sum AS power_export_sum,
            pp.timestamp AS timestamp_power_production,
            pp.cumulative_sum AS power_production_sum
        FROM power_export_sum pe
        JOIN power_production_sum pp
            ON ABS(DATEDIFF(millisecond, pe.timestamp, pp.timestamp)) <= 1000  -- max 1s difference
        WHERE pe.timestamp >= TO_TIMESTAMP_TZ('2025-10-22 00:00:00 +00:00')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pe.timestamp
            ORDER BY ABS(DATEDIFF(millisecond, pe.timestamp, pp.timestamp))
        ) = 1
        ORDER BY pe.timestamp;
    """
    
    df = pd.read_sql(query, conn)
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP_POWER_EXPORT"])
    df["date_only"] = df["TIMESTAMP"].dt.date
    df["export_base_sub"] = df.groupby("date_only")["POWER_EXPORT_SUM"].transform(lambda x: x - x.iloc[0])
    df["production_base_sub"] = df.groupby("date_only")["POWER_PRODUCTION_SUM"].transform(lambda x: x - x.iloc[0])
    df["daily_consumption_rate"] = (
        (df["production_base_sub"] - df["export_base_sub"])
        / df["production_base_sub"].replace(0, np.nan)
    )
    df["daily_consumption_rate"] = df["daily_consumption_rate"].replace([np.inf, -np.inf], np.nan)
    df["consumption_rate"] = (
        (df["POWER_PRODUCTION_SUM"] - df["POWER_EXPORT_SUM"])
        / df["POWER_PRODUCTION_SUM"]
    )
    
    unique_dates = pd.unique(df["date_only"])
    tick_positions = [
    df.loc[df["date_only"] == d, "TIMESTAMP"].iloc[0] for d in unique_dates
    ]
    
    fig, ax1 = plt.subplots(figsize=(10, 5))
    

    tick_labels = [d.strftime("%d.%m") for d in unique_dates]
    color_left = "tab:blue"
    ax1.set_xlabel("Datum")
    ax1.set_ylabel("POWER_EXPORT_SUM", color=color_left)
    ax1.plot(df["TIMESTAMP"], df["POWER_EXPORT_SUM"], color=color_left, linewidth=0.8)
    ax1.tick_params(axis="y", labelcolor=color_left)
    ax1.set_xticks(tick_positions)
    ax1.set_xticklabels(tick_labels, rotation=45, ha="right")
    
    ax2 = ax1.twinx()
    color_right = "tab:orange"
    ax2.set_ylabel("POWER_PRODUCTION_SUM", color=color_right)
    ax2.plot(df["TIMESTAMP"], df["POWER_PRODUCTION_SUM"], color=color_right, linewidth=0.8)
    ax2.tick_params(axis="y", labelcolor=color_right)
    ax2.set_xticks(tick_positions)
    ax2.set_xticklabels(tick_labels, rotation=45, ha="right")
    
    ax3 = ax1.twinx()
    color_third = "tab:red"
    ax3.spines["right"].set_position(("outward", 60))  # move it slightly right
    ax3.set_ylabel("CONSUMPTION_RATE", color=color_third)
    ax3.yaxis.set_label_position("right")
    ax3.yaxis.tick_right()
    ax3.plot(df["TIMESTAMP"], df["consumption_rate"], color=color_third, linewidth=1.0)
    ax3.tick_params(axis="y", labelcolor=color_third)
    plt.tight_layout()
    plt.savefig(f"scripts/plots/consumption_rate/total_consumption_rate_with_sums.png", dpi=200)
    
    ax4 = ax1.twinx()
    color4 = "purple"
    ax4.spines["left"].set_position(("outward", 60))   # move it outward to the left
    ax4.yaxis.set_label_position("left")
    ax4.yaxis.tick_left()
    ax4.set_ylabel("DAILY_CONSUMPTION_RATE", color=color4)
    ax4.plot(df["TIMESTAMP"], df["daily_consumption_rate"], color=color4, linewidth=1.0)
    ax4.tick_params(axis="y", labelcolor=color4)
    
    
    fig.tight_layout()
    plt.savefig(f"scripts/plots/consumption_rate/consumption_rate_with_sums.png", dpi=200)
    ax3.set_visible(False)
    plt.tight_layout()
    plt.savefig(f"scripts/plots/consumption_rate/daily_consumption_rate_with_sums.png", dpi=200)
    plt.close(fig)
    print(f"✅ Saved consumption_rate_with_sums")
if __name__ == "__main__":
    # plot_table("export")
    # plot_table("production")
    plot_consumption_rate()
