"""
crypto_metrics.py
Pulls resume metrics for the Crypto Market Microstructure Pipeline.

Setup:
    pip install duckdb pandas
    aws configure   (so credentials are available)

Edit the CONFIG block, run:  python crypto_metrics.py
Prints everything. Also saves crypto_metrics.txt.
"""

import io
import subprocess
import sys
from contextlib import redirect_stdout
import boto3
import pyarrow as pa
from deltalake import DeltaTable

import duckdb
import pandas as pd

# ------------------------------------------------------------------ CONFIG
BUCKET = "crypto-pipeline-ar-v3"          # no s3:// prefix
REGION = "ap-southeast-1"
TABLES = {
    "bronze": "bronze-ticks/",       # path inside bucket
    "silver": "silver-features/",       # path inside bucket
    "ofi": "gold/ofi-features/",      
    "vol": "gold/volatility-features/",
    "css": "gold/cross-asset-signal/", 
}

TIME_COL = "event_time"           # tick time column in Bronze
SYMBOL_COL = "symbol"             # used only in the sample section
INGEST_COL = None                 # e.g. "ingest_ts" for latency, else None
TIME_IS_EPOCH_MS = False          # True if TIME_COL is bigint epoch millis
 
SAMPLE_FILES = 500                # contiguous Bronze files to read for peak/latency. 0 = skip
# -------------------------------------------------------------------------
 
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 50)
pd.set_option("display.max_rows", 200)
 
 
def section(t):
    print("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70)
 
 
def human(b):
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.2f} {u}"
        b /= 1024
    return f"{b:.2f} PB"
 
 
def storage_options():
    c = boto3.Session().get_credentials().get_frozen_credentials()
    o = {
        "AWS_REGION": REGION,
        "AWS_ACCESS_KEY_ID": c.access_key,
        "AWS_SECRET_ACCESS_KEY": c.secret_key,
    }
    if c.token:
        o["AWS_SESSION_TOKEN"] = c.token
    return o
 
 
def load_actions(path):
    dt = DeltaTable(f"s3://{BUCKET}/{path}", storage_options=storage_options())
    acts = dt.get_add_actions(flatten=True)
    try:
        df = acts.to_pandas()
    except AttributeError:
        df = pa.table(acts).to_pandas()
    return dt, df
 
 
def to_ts(s):
    if TIME_IS_EPOCH_MS or pd.api.types.is_integer_dtype(s):
        return pd.to_datetime(s, unit="ms")
    return pd.to_datetime(s)
 
 
def summarize(name, path, is_bronze=False):
    section(f"TABLE: {name}")
    dt, a = load_actions(path)
    print(f"delta version: {dt.version()}")
 
    files = len(a)
    size = a["size_bytes"].sum()
    print(f"data files:      {files:,}")
    print(f"total size:      {human(size)}  (data files only)")
    print(f"avg file size:   {human(size / max(files, 1))}")
    print(f"median file size:{human(a['size_bytes'].median())}")
    print(f"files < 1 MB:    {(a['size_bytes'] < 1_048_576).mean() * 100:.1f}%")
 
    if "num_records" in a.columns and a["num_records"].notna().any():
        if a["num_records"].isna().any():
            print("WARNING: some files have no stats. Row count is a lower bound.")
        rows = int(a["num_records"].sum())
        print(f"total rows:      {rows:,}")
        print(f"median rows/file:{a['num_records'].median():,.0f}")
    else:
        rows = None
        print("No num_records stats in log. Row count not available from log.")
 
    part_cols = [c for c in a.columns if c.startswith("partition.")]
    for pc in part_cols:
        print(f"\nrows by {pc}:")
        if rows is not None:
            print(a.groupby(pc)["num_records"].sum().sort_values(ascending=False).head(15))
 
    if is_bronze:
        mn, mx = f"min.{TIME_COL}", f"max.{TIME_COL}"
        if mn not in a.columns or mx not in a.columns:
            print(f"\nNo min/max stats for '{TIME_COL}'. Check column name:")
            print([c for c in a.columns if c.startswith("min.")][:30])
            return a, None
        a["_lo"] = to_ts(a[mn])
        a["_hi"] = to_ts(a[mx])
        first, last = a["_lo"].min(), a["_hi"].max()
        span = (last - first).total_seconds()
 
        a["_day"] = a["_lo"].dt.date
        per_day = a.groupby("_day")["num_records"].sum()
        active = len(per_day)
 
        print(f"\nfirst tick:      {first}")
        print(f"last tick:       {last}")
        print(f"calendar span:   {span / 86400:.1f} days")
        print(f"active days:     {active}")
        if rows:
            print(f"avg ticks/sec over span:        {rows / span:.2f}")
            print(f"avg ticks per active day:       {rows / active:,.0f}")
            print(f"avg ticks/sec per active day:   {rows / active / 86400:.2f}")
        if active < (span / 86400) * 0.9:
            print("NOTE: stream had gaps. Quote the 'per active day' numbers.")
        print("\nticks per day (bucketed by file start time, approximate at midnight):")
        print(per_day.to_string())
    return a, True
 
 
def sample_bronze(a, path):
    """Read a contiguous slice of Bronze files for peak ticks/sec and latency."""
    import duckdb
 
    section(f"BRONZE SAMPLE ({SAMPLE_FILES} contiguous files). Sample stats, not full data.")
    a = a.sort_values("_lo").reset_index(drop=True)
    n = min(SAMPLE_FILES, len(a))
    start = (len(a) - n) // 2
    chunk = a.iloc[start:start + n]
    root = f"s3://{BUCKET}/{path.strip('/')}/"
    files = [root + p for p in chunk["path"]]
    print(f"sample window: {chunk['_lo'].min()}  ->  {chunk['_hi'].max()}")
 
    con = duckdb.connect()
    for ext in ("httpfs", "aws"):
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    con.execute(
        f"CREATE SECRET s3_cred (TYPE s3, PROVIDER credential_chain, REGION '{REGION}')"
    )
    ts = f"epoch_ms({TIME_COL})" if TIME_IS_EPOCH_MS else TIME_COL
    con.execute(
        f"CREATE VIEW s AS SELECT *, {ts} AS _ts FROM read_parquet(?, union_by_name=true)",
        [files],
    )
 
    print("\nsample rows / span:")
    print(con.execute(
        "SELECT count(*) AS rows, min(_ts) AS first, max(_ts) AS last, "
        "count(*)/nullif(epoch(max(_ts)-min(_ts)),0) AS avg_ticks_per_sec FROM s"
    ).df().T)
 
    print("\npeak ticks/sec in sample (top 5):")
    print(con.execute(
        "SELECT date_trunc('second', _ts) AS sec, count(*) AS ticks "
        "FROM s GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
    ).df())
 
    try:
        print(f"\nticks by {SYMBOL_COL} in sample:")
        print(con.execute(
            f"SELECT {SYMBOL_COL} AS symbol, count(*) AS ticks FROM s GROUP BY 1 ORDER BY 2 DESC"
        ).df())
    except Exception as e:
        print(f"symbol split skipped: {e}")
 
    if INGEST_COL:
        print("\nBinance -> Bronze latency in sample (NOT end-to-end):")
        print(con.execute(
            f"SELECT avg(epoch({INGEST_COL}-_ts)) AS avg_s, "
            f"quantile_cont(epoch({INGEST_COL}-_ts),0.5) AS p50_s, "
            f"quantile_cont(epoch({INGEST_COL}-_ts),0.95) AS p95_s, "
            f"max(epoch({INGEST_COL}-_ts)) AS max_s FROM s"
        ).df())
    else:
        print("\nINGEST_COL not set. Latency skipped. Do not estimate it.")
 
 
def main():
    bronze_actions = None
    for name, path in TABLES.items():
        try:
            a, ok = summarize(name, path, is_bronze=(name == "bronze"))
            if name == "bronze" and ok:
                bronze_actions = a
        except Exception as e:
            print(f"\n{name}: FAILED -> {e}")
 
    if SAMPLE_FILES and bronze_actions is not None:
        try:
            sample_bronze(bronze_actions, TABLES["bronze"])
        except Exception as e:
            print(f"\nSample step failed -> {e}")
 
    section("DONE. Paste this whole output back to Claude.")
 
 
if __name__ == "__main__":
    buf = io.StringIO()
 
    class Tee:
        def write(self, x):
            sys.__stdout__.write(x)
            buf.write(x)
 
        def flush(self):
            sys.__stdout__.flush()
 
    with redirect_stdout(Tee()):
        main()
    with open("crypto_metrics_fast.txt", "w") as f:
        f.write(buf.getvalue())
