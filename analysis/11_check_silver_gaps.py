# Find gaps (more than 2 minutes) between consecutive silver windows, per symbol.
from s3_duckdb import connect, table

con = connect()

df = con.execute("""f
    SELECT symbol, window_start
    FROM delta_scan('{table("silver")}')
    ORDER BY symbol, window_start
""").df()

df["gap_minutes"] = df.groupby("symbol")["window_start"].diff().dt.total_seconds() / 60
df["prev_window_start"] = df.groupby("symbol")["window_start"].shift(1)

print("gap distribution (largest 20 values):")
print(df["gap_minutes"].value_counts().sort_index().tail(20))

print("\ngaps over 2 minutes:")
for _, row in df[df["gap_minutes"] > 2].iterrows():
    print(f"  {row['symbol']}: {row['prev_window_start']} -> {row['window_start']}  "
          f"({row['gap_minutes']:.1f} min)")
