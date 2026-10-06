"""Row counts, duplicates and gaps for the gold volatility and cross-asset tables."""
from s3_duckdb import connect, table

con = connect()

# ---- volatility-features ----
vol_path = table("volatility")
print("=" * 60)
print("VOLATILITY-FEATURES")
print("=" * 60)

print(con.execute(f"""
    SELECT symbol, COUNT(*) AS cnt, MIN(window_start) AS earliest, MAX(window_start) AS latest
    FROM delta_scan('{vol_path}')
    GROUP BY symbol
""").df())

dupes = con.execute(f"""
    SELECT symbol, window_start, COUNT(*) AS cnt
    FROM delta_scan('{vol_path}')
    GROUP BY symbol, window_start
    HAVING COUNT(*) > 1
""").df()
print(f"duplicate (symbol, window_start) rows: {len(dupes)}")
if len(dupes) > 0:
    print(dupes)

vol_df = con.execute(f"""
    SELECT symbol, window_start FROM delta_scan('{vol_path}')
    ORDER BY symbol, window_start
""").df()
vol_df["gap_minutes"] = vol_df.groupby("symbol")["window_start"].diff().dt.total_seconds() / 60
print("\ngap distribution:")
print(vol_df["gap_minutes"].value_counts().sort_index())

# ---- cross-asset-signal (one row per window, both symbols already joined) ----
cross_path = table("cross_asset")
print("\n" + "=" * 60)
print("CROSS-ASSET-SIGNAL")
print("=" * 60)

print(con.execute(f"""
    SELECT COUNT(*) AS cnt, MIN(window_start) AS earliest, MAX(window_start) AS latest
    FROM delta_scan('{cross_path}')
""").df())

dupes = con.execute(f"""
    SELECT window_start, COUNT(*) AS cnt
    FROM delta_scan('{cross_path}')
    GROUP BY window_start
    HAVING COUNT(*) > 1
""").df()
print(f"duplicate window_start rows: {len(dupes)}")
if len(dupes) > 0:
    print(dupes)

cross_df = con.execute(f"""
    SELECT window_start FROM delta_scan('{cross_path}')
    ORDER BY window_start
""").df()
cross_df["gap_minutes"] = cross_df["window_start"].diff().dt.total_seconds() / 60
print("\ngap distribution:")
print(cross_df["gap_minutes"].value_counts().sort_index())
