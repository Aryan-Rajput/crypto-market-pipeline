# Row count and time range of every pipeline table.
from s3_duckdb import connect, table

con = connect()

for name in ["silver", "ofi", "volatility", "cross_asset"]:
    try:
        result = con.execute(f"""
            SELECT COUNT(*) AS row_count, MAX(window_start) AS latest, MIN(window_start) AS earliest
            FROM delta_scan('{table(name)}')
        """).df()
        print(f"\n|| {name} ||")
        print(result)
    except Exception as e:
        print(f"\n|| {name} || ERROR: {e}")
