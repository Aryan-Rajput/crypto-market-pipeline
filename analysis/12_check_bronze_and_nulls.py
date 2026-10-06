# Latest bronze ticks per symbol, and where the null volatility rows are in gold.
from s3_duckdb import connect, table

SINCE = "2026-09-26"      # only look at bronze partitions from this date on

con = connect()

print(con.execute(f"""
    SELECT symbol, COUNT(*) AS cnt, MIN(event_time) AS earliest, MAX(event_time) AS latest
    FROM delta_scan('{table("bronze")}')
    WHERE event_date >= '{SINCE}'
    GROUP BY symbol
""").df())

nulls = con.execute(f"""
    SELECT symbol, window_start, volatility
    FROM delta_scan('{table("volatility")}')
    WHERE volatility IS NULL
    ORDER BY symbol, window_start
""").df()
print(f"\nnull volatility rows: {len(nulls)}")
print(nulls)
