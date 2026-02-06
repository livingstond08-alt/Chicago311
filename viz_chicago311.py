"""
Chicago 311 — Chart Generator (SQLite -> PNG)

How to run:
  1) Put this file in the same folder as your Chicago311.db
     OR edit DB_PATH below to point to your .db file.
  2) Install deps:
       pip install pandas matplotlib
  3) Run:
       python viz_chicago311.py

Outputs:
  Saves PNG charts into ./output/
"""

import os
import sqlite3
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# =======================
# CONFIG
# =======================
DB_PATH = r"Chicago311.db"          # <-- change if needed (absolute path also OK)
TABLE_NAME = "service_requests"     # <-- change if your table name differs
OUT_DIR = Path("output")
OUT_DIR.mkdir(exist_ok=True)


# =======================
# HELPERS
# =======================
def connect_db(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    if not p.exists():
        raise FileNotFoundError(
            f"Database not found at: {p.resolve()}\n"
            f"Tip: set DB_PATH to the full path of your Chicago311.db."
        )
    return sqlite3.connect(str(p))


def read_sql(conn: sqlite3.Connection, query: str) -> pd.DataFrame:
    return pd.read_sql_query(query, conn)


def savefig(filename: str) -> None:
    out_path = OUT_DIR / filename
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    print(f"Saved: {out_path}")


def ensure_table_and_columns(conn: sqlite3.Connection) -> None:
    # Confirm table exists
    tables = read_sql(conn, "SELECT name FROM sqlite_master WHERE type='table';")["name"].tolist()
    if TABLE_NAME not in tables:
        raise ValueError(
            f"Table '{TABLE_NAME}' not found in DB. Tables present: {tables}\n"
            f"Tip: update TABLE_NAME at the top of this script."
        )

    # Check for resolution_hours; warn if missing
    cols = read_sql(conn, f"PRAGMA table_info({TABLE_NAME});")["name"].tolist()
    if "resolution_hours" not in cols:
        print("WARNING: 'resolution_hours' column not found.")
        print("Run these in SQLite first to add it:")
        print(f"  ALTER TABLE {TABLE_NAME} ADD COLUMN resolution_hours REAL;")
        print(f"  UPDATE {TABLE_NAME} SET resolution_hours = "
              f"(julianday(closed_date) - julianday(created_date)) * 24 "
              f"WHERE closed_date IS NOT NULL;")
        print("Continuing anyway; charts requiring resolution_hours may be skipped.\n")


# =======================
# CHARTS
# =======================
def chart_top_request_types(conn: sqlite3.Connection) -> None:
    df = read_sql(conn, f"""
        SELECT sr_type, COUNT(*) AS request_count
        FROM {TABLE_NAME}
        GROUP BY sr_type
        ORDER BY request_count DESC
        LIMIT 10;
    """)
    if df.empty:
        print("Skipping top request types: query returned no rows.")
        return

    plt.figure()
    plt.barh(df["sr_type"][::-1], df["request_count"][::-1])
    plt.title("Top 10 Chicago 311 Request Types (Count)")
    plt.xlabel("Requests")
    savefig("01_top_request_types.png")


def chart_resolution_histograms(conn: sqlite3.Connection) -> None:
    cols = read_sql(conn, f"PRAGMA table_info({TABLE_NAME});")["name"].tolist()
    if "resolution_hours" not in cols:
        print("Skipping resolution histograms: resolution_hours missing.")
        return

    df = read_sql(conn, f"""
        SELECT resolution_hours
        FROM {TABLE_NAME}
        WHERE resolution_hours IS NOT NULL;
    """)
    if df.empty:
        print("Skipping resolution histograms: no non-null resolution_hours found.")
        return

    # Trim at 99th percentile for readability
    p99 = float(df["resolution_hours"].quantile(0.99))
    df_trim = df[df["resolution_hours"] <= p99]

    plt.figure()
    plt.hist(df_trim["resolution_hours"], bins=50)
    plt.title("Resolution Time Distribution (Trimmed at 99th Percentile)")
    plt.xlabel("Resolution hours")
    plt.ylabel("Count")
    savefig("02_resolution_hist_trimmed.png")

    # Log-scale y to show tail
    plt.figure()
    plt.hist(df["resolution_hours"], bins=60)
    plt.yscale("log")
    plt.title("Resolution Time Distribution (Log Y-Scale)")
    plt.xlabel("Resolution hours")
    plt.ylabel("Count (log scale)")
    savefig("03_resolution_hist_log.png")


def chart_resolution_buckets(conn: sqlite3.Connection) -> None:
    cols = read_sql(conn, f"PRAGMA table_info({TABLE_NAME});")["name"].tolist()
    if "resolution_hours" not in cols:
        print("Skipping resolution buckets: resolution_hours missing.")
        return

    df = read_sql(conn, f"""
        SELECT
          CASE
            WHEN resolution_hours IS NULL THEN 'Open'
            WHEN resolution_hours = 0 THEN '0 hours'
            WHEN resolution_hours <= 24 THEN '0–24 hours'
            WHEN resolution_hours <= 72 THEN '1–3 days'
            WHEN resolution_hours <= 168 THEN '3–7 days'
            ELSE '7+ days'
          END AS resolution_bucket,
          COUNT(*) AS cnt
        FROM {TABLE_NAME}
        GROUP BY resolution_bucket
        ORDER BY cnt DESC;
    """)
    if df.empty:
        print("Skipping resolution buckets: query returned no rows.")
        return

    plt.figure()
    plt.bar(df["resolution_bucket"], df["cnt"])
    plt.title("Resolution Time Buckets")
    plt.xlabel("Bucket")
    plt.ylabel("Count")
    plt.xticks(rotation=25, ha="right")
    savefig("04_resolution_buckets.png")


def chart_avg_resolution_by_department(conn: sqlite3.Connection) -> None:
    cols = read_sql(conn, f"PRAGMA table_info({TABLE_NAME});")["name"].tolist()
    if "resolution_hours" not in cols:
        print("Skipping dept performance: resolution_hours missing.")
        return

    df = read_sql(conn, f"""
        SELECT owner_department,
               COUNT(*) AS n,
               AVG(resolution_hours) AS avg_hours
        FROM {TABLE_NAME}
        WHERE resolution_hours IS NOT NULL
        GROUP BY owner_department
        ORDER BY avg_hours DESC;
    """)
    if df.empty:
        print("Skipping dept performance: query returned no rows.")
        return

    plt.figure()
    plt.barh(df["owner_department"][::-1], df["avg_hours"][::-1])
    plt.title("Average Resolution Hours by Department")
    plt.xlabel("Avg resolution hours")
    savefig("05_avg_resolution_by_department.png")


def chart_community_area_volume_and_speed(conn: sqlite3.Connection) -> None:
    cols = read_sql(conn, f"PRAGMA table_info({TABLE_NAME});")["name"].tolist()
    if "resolution_hours" not in cols:
        print("Skipping community area charts: resolution_hours missing.")
        return

    # Top 15 by volume (n >= 50)
    df_vol = read_sql(conn, f"""
        SELECT community_area,
               COUNT(*) AS n,
               AVG(resolution_hours) AS avg_hours
        FROM {TABLE_NAME}
        WHERE resolution_hours IS NOT NULL
          AND community_area IS NOT NULL
        GROUP BY community_area
        HAVING n >= 50
        ORDER BY n DESC
        LIMIT 15;
    """)
    if not df_vol.empty:
        plt.figure()
        plt.bar(df_vol["community_area"].astype(str), df_vol["n"])
        plt.title("Top Community Areas by Request Volume (n ≥ 50)")
        plt.xlabel("Community area")
        plt.ylabel("Requests")
        savefig("06a_top_community_areas_by_volume.png")
    else:
        print("Skipping volume chart: no rows returned (check community_area values).")

    # Top 15 slowest avg resolution (n >= 50)
    df_slow = read_sql(conn, f"""
        SELECT community_area,
               COUNT(*) AS n,
               AVG(resolution_hours) AS avg_hours
        FROM {TABLE_NAME}
        WHERE resolution_hours IS NOT NULL
          AND community_area IS NOT NULL
        GROUP BY community_area
        HAVING n >= 50
        ORDER BY avg_hours DESC
        LIMIT 15;
    """)
    if not df_slow.empty:
        plt.figure()
        plt.bar(df_slow["community_area"].astype(str), df_slow["avg_hours"])
        plt.title("Slowest Avg Resolution by Community Area (n ≥ 50)")
        plt.xlabel("Community area")
        plt.ylabel("Avg resolution hours")
        savefig("06b_slowest_community_areas_by_avg_resolution.png")
    else:
        print("Skipping slowest chart: no rows returned (check resolution_hours).")


def chart_requests_heatmap(conn: sqlite3.Connection) -> None:
    df = read_sql(conn, f"""
        SELECT created_day_of_week, created_hour, COUNT(*) AS requests
        FROM {TABLE_NAME}
        GROUP BY created_day_of_week, created_hour
        ORDER BY created_day_of_week, created_hour;
    """)
    if df.empty:
        print("Skipping heatmap: query returned no rows.")
        return

    heat = (
        df.pivot(index="created_day_of_week", columns="created_hour", values="requests")
          .fillna(0)
          .sort_index()
    )

    plt.figure()
    plt.imshow(heat.values, aspect="auto")
    plt.title("Requests Heatmap: Day of Week × Hour")
    plt.xlabel("Hour of day")
    plt.ylabel("Day of week")
    plt.xticks(range(len(heat.columns)), heat.columns, rotation=90)
    plt.yticks(range(len(heat.index)), heat.index)
    plt.colorbar(label="Requests")
    savefig("07_requests_heatmap.png")


def chart_map_scatter_optional(conn: sqlite3.Connection, max_points: int = 5000) -> None:
    cols = read_sql(conn, f"PRAGMA table_info({TABLE_NAME});")["name"].tolist()
    if "resolution_hours" not in cols:
        print("Skipping map scatter: resolution_hours missing.")
        return

    df = read_sql(conn, f"""
        SELECT latitude, longitude,
          CASE
            WHEN resolution_hours IS NULL THEN 'Open'
            WHEN resolution_hours = 0 THEN '0 hours'
            WHEN resolution_hours <= 24 THEN '0–24 hours'
            WHEN resolution_hours <= 168 THEN '1–7 days'
            ELSE '7+ days'
          END AS bucket
        FROM {TABLE_NAME}
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
    """)
    if df.empty:
        print("Skipping map scatter: no lat/long rows.")
        return

    if len(df) > max_points:
        df = df.sample(max_points, random_state=42)

    plt.figure()
    for b in df["bucket"].unique():
        sub = df[df["bucket"] == b]
        plt.scatter(sub["longitude"], sub["latitude"], s=4, label=b)

    plt.title("Chicago 311 Requests (sample) by Resolution Bucket")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.legend(markerscale=3, fontsize=8)
    savefig("08_map_scatter_by_bucket.png")


# =======================
# MAIN
# =======================
def main() -> None:
    with connect_db(DB_PATH) as conn:
        ensure_table_and_columns(conn)

        chart_top_request_types(conn)
        chart_resolution_histograms(conn)
        chart_resolution_buckets(conn)
        chart_avg_resolution_by_department(conn)
        chart_community_area_volume_and_speed(conn)
        chart_requests_heatmap(conn)

        # Optional: comment out if you don't want it
        chart_map_scatter_optional(conn)

    print("\nDone. Check the 'output' folder for PNG charts.")


if __name__ == "__main__":
    main()
