# Step-by-step investigation of the silver first/last price bug.

# Each iteration answers one question. Run one or more by number:
#     python analysis/20_silver_bug_investigation.py 1
#     python analysis/20_silver_bug_investigation.py 2 3 4

# 1  table health: gaps, duplicates, nulls
# 2  merge gold OFI + volatility, same-window and lag-1 correlation (saves a file)
# 3  FLAT direction labels: how common, and when
# 4  compare FLAT gold rows with silver first/last prices
# 5  compare sampled FLAT windows with the raw bronze ticks (this found the bug)
# 6  export a slice of bronze ticks to parquet
# 7  find which minute a known price range really belongs to (timezone shift check)
# 8  true first/last trade of one minute from the exported parquet
# 9  export the gold OFI table

# Finding: silver first_price / last_price used F.first / F.last, which depend on
# row order, so momentum, log_return and direction in gold volatility were wrong.
# OFI, min/max and trade counts were not affected.

import sys
import pandas as pd

from common import EXPORTS_DIR
from s3_duckdb import connect, table

TZ = "Asia/Calcutta"
MERGED_FILE = EXPORTS_DIR / "merged_ofi_vol.tsv"
BRONZE_SLICE = EXPORTS_DIR / "bronze_btc_0929_1001.parquet"


def to_ist(series):
    if series.dt.tz is None:
        return series.dt.tz_localize(TZ)
    return series.dt.tz_convert(TZ)


def load_merged():
    df = pd.read_csv(MERGED_FILE, sep="\t", parse_dates=["window_start", "window_end"])
    df = df.sort_values(["symbol", "window_start"]).reset_index(drop=True)
    df["gap"] = df.groupby("symbol")["window_start"].diff()
    df["segment"] = (df["gap"] > pd.Timedelta(minutes=1)).groupby(df["symbol"]).cumsum()
    df["date"] = df["window_start"].dt.date
    return df


def iteration_1_table_health(con):
    vol = con.execute(f"""
        SELECT symbol, window_start FROM delta_scan('{table("volatility")}')
        ORDER BY symbol, window_start
    """).df()
    vol["gap_minutes"] = vol.groupby("symbol")["window_start"].diff().dt.total_seconds() / 60
    print("volatility gap distribution:")
    print(vol["gap_minutes"].value_counts().sort_index())

    dupes = con.execute(f"""
        SELECT symbol, window_start, COUNT(*) AS cnt
        FROM delta_scan('{table("silver")}')
        GROUP BY symbol, window_start
        HAVING COUNT(*) > 1
    """).df()
    print("\nduplicate silver rows:", len(dupes))

    nulls = con.execute(f"""
        SELECT COUNT(*) FROM delta_scan('{table("volatility")}') WHERE volatility IS NULL
    """).fetchone()[0]
    print("null volatility rows:", nulls)

    print("\ndirection counts:")
    print(con.execute(f"""
        SELECT direction, COUNT(*) AS cnt
        FROM delta_scan('{table("volatility")}')
        GROUP BY direction
    """).df())


def iteration_2_merge_ofi_and_volatility(con):
    ofi = con.execute(f"""
        SELECT window_start, window_end, symbol, buy_volume, sell_volume,
               total_volume, ofi, ofi_norm
        FROM delta_scan('{table("ofi")}')
    """).df()
    vol = con.execute(f"""
        SELECT window_start, window_end, symbol, total_quantity, trade_count,
               first_price, last_price, min_price, max_price, log_return,
               momentum, volatility, price_range, direction
        FROM delta_scan('{table("volatility")}')
    """).df()

    merged = pd.merge(ofi, vol, on=["window_start", "window_end", "symbol"])
    merged = merged.sort_values(["symbol", "window_start"]).reset_index(drop=True)
    print("rows  ofi:", len(ofi), " vol:", len(vol), " merged:", len(merged))

    merged["ofi_lag1"] = merged.groupby("symbol")["ofi_norm"].shift(1)
    merged["gap_minutes"] = merged.groupby("symbol")["window_start"].diff().dt.total_seconds() / 60

    for sym, g in merged.groupby("symbol"):
        print(sym, len(g),
              "lag1:", round(g["ofi_lag1"].corr(g["momentum"]), 4),
              "same-window:", round(g["ofi_norm"].corr(g["momentum"]), 4))

    merged.to_csv(MERGED_FILE, sep="\t", index=False)
    print("saved", MERGED_FILE)


def iteration_3_flat_direction(con=None):
    df = load_merged()

    gaps = df[df["gap"] > pd.Timedelta(minutes=1)]
    print("total gaps:", len(gaps))
    print(gaps.groupby("symbol").size())
    print(df.groupby(["symbol", "segment"]).agg(
        rows=("window_start", "size"), start=("window_start", "min"), end=("window_start", "max")))

    print("\ndirection share:")
    print(df.groupby("symbol")["direction"].value_counts(normalize=True).round(3))

    print("\nshare of FLAT rows per day:")
    print(df.groupby(["symbol", "date"])["direction"].apply(lambda s: (s == "FLAT").mean()).round(3).unstack(0))

    print("\ntrade_count for FLAT vs not FLAT:")
    print(df.groupby(["symbol", df["direction"] == "FLAT"])["trade_count"].describe())

    flat = df[df["direction"] == "FLAT"]
    print("\nFLAT rows:", len(flat))
    print("momentum is NaN:", flat["momentum"].isna().sum())
    print("momentum == 0:", (flat["momentum"] == 0).sum())
    print("price_range == 0:", (flat["price_range"] == 0).sum())
    print("first == last but range > 0:",
          ((flat["first_price"] == flat["last_price"]) & (flat["price_range"] > 0)).sum())


def iteration_4_flat_vs_silver(con):
    df = load_merged()
    flat = df[df["direction"] == "FLAT"][["symbol", "window_start", "first_price", "last_price"]].copy()

    silver = con.execute(f"""
        SELECT symbol, window_start, first_price AS s_first, last_price AS s_last
        FROM delta_scan('{table("silver")}')
    """).df()

    flat["window_start"] = pd.to_datetime(flat["window_start"], utc=True)
    silver["window_start"] = pd.to_datetime(silver["window_start"], utc=True)

    m = flat.merge(silver, on=["symbol", "window_start"], how="left")
    print("matched:", m["s_first"].notna().sum(), "of", len(m))
    print("silver first != last:", (m["s_first"] != m["s_last"]).sum())
    print("gold first != silver first:", (m["first_price"] != m["s_first"]).sum())
    print("gold last != silver last:", (m["last_price"] != m["s_last"]).sum())


def iteration_5_check_against_bronze(con, since="2026-09-29", n=3):
    df = load_merged()
    window_start = to_ist(df["window_start"])
    flat = df[(df["direction"] == "FLAT") & (df["symbol"] == "BTCUSDT")
              & (window_start >= pd.Timestamp(since, tz=TZ))].sample(n, random_state=1)

    for _, r in flat.iterrows():
        ws = r["window_start"]
        if ws.tzinfo is None:
            ws = ws.tz_localize(TZ)
        start_ms = int(ws.timestamp() * 1000)
        end_ms = start_ms + 60_000

        t = con.execute(f"""
            SELECT trade_id, trade_time, CAST(price AS DOUBLE) AS price
            FROM delta_scan('{table("bronze")}')
            WHERE symbol = 'BTCUSDT' AND trade_time >= {start_ms} AND trade_time < {end_ms}
        """).df().sort_values(["trade_time", "trade_id"])

        if t.empty:
            print(ws, "| no bronze rows for this window")
            continue

        print(ws, "| bronze n:", len(t), "| silver trade_count:", r["trade_count"],
              "| silver first/last:", r["first_price"], r["last_price"],
              "| true first/last:", t["price"].iloc[0], t["price"].iloc[-1],
              "| silver min/max:", r["min_price"], r["max_price"],
              "| true min/max:", t["price"].min(), t["price"].max())


def iteration_6_export_bronze_slice(con, start="2026-09-29 00:00", end="2026-10-02 00:00"):
    start_ms = int(pd.Timestamp(start, tz=TZ).timestamp() * 1000)
    end_ms = int(pd.Timestamp(end, tz=TZ).timestamp() * 1000)

    con.execute(f"""
        COPY (
            SELECT symbol, trade_id, trade_time,
                   CAST(price AS DOUBLE) AS price,
                   CAST(quantity AS DOUBLE) AS quantity
            FROM delta_scan('{table("bronze")}')
            WHERE symbol = 'BTCUSDT' AND trade_time >= {start_ms} AND trade_time < {end_ms}
        ) TO '{BRONZE_SLICE}' (FORMAT PARQUET)
    """)
    print("saved", BRONZE_SLICE)


def iteration_7_find_time_shift(con, ws="2026-09-30 01:05:00", low=83410.0, high=83446.01):
    # which real minutes (within +/- 12h of ws) contain this exact price range?
    start_ms = int(pd.Timestamp(ws, tz=TZ).timestamp() * 1000)
    pad_ms = 12 * 3600 * 1000

    m = con.execute(f"""
        SELECT (trade_time // 60000) * 60000 AS minute_ms,
               COUNT(*) AS n,
               MIN(CAST(price AS DOUBLE)) AS mn,
               MAX(CAST(price AS DOUBLE)) AS mx
        FROM read_parquet('{BRONZE_SLICE}')
        WHERE symbol = 'BTCUSDT' AND trade_time BETWEEN {start_ms - pad_ms} AND {start_ms + pad_ms}
        GROUP BY 1
        HAVING MIN(CAST(price AS DOUBLE)) = {low} OR MAX(CAST(price AS DOUBLE)) = {high}
    """).df()
    m["minute_ist"] = pd.to_datetime(m["minute_ms"], unit="ms", utc=True).dt.tz_convert(TZ)
    print(m)


def iteration_8_true_first_last(con, minute_ms=1790730300000):
    t = con.execute(f"""
        SELECT trade_id, trade_time, price
        FROM read_parquet('{BRONZE_SLICE}')
        WHERE trade_time >= {minute_ms} AND trade_time < {minute_ms} + 60000
        ORDER BY trade_time, trade_id
    """).df()
    print(len(t), "trades | true first/last:", t["price"].iloc[0], t["price"].iloc[-1])


def iteration_9_export_gold_ofi(con):
    out = EXPORTS_DIR / "gold_ofi.tsv"
    con.execute(f"SELECT * FROM delta_scan('{table('ofi')}') ORDER BY window_start DESC").df() \
        .to_csv(out, sep="\t", index=False)
    print("saved", out)

def iteration_10_check_tables(con):
    None

ITERATIONS = {
    1: iteration_1_table_health,
    2: iteration_2_merge_ofi_and_volatility,
    3: iteration_3_flat_direction,
    4: iteration_4_flat_vs_silver,
    5: iteration_5_check_against_bronze,
    6: iteration_6_export_bronze_slice,
    7: iteration_7_find_time_shift,
    8: iteration_8_true_first_last,
    9: iteration_9_export_gold_ofi,
    10: iteration_10_check_tables
}

if __name__ == "__main__":
    wanted = [int(a) for a in sys.argv[1:]]
    if not wanted:
        print(__doc__)
        sys.exit(0)
    con = connect()
    for n in wanted:
        print(f"\n iteration {n}: {ITERATIONS[n].__name__}")
        ITERATIONS[n](con)
