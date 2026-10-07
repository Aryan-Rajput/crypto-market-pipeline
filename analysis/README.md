# analysis table

| Script | What it does |
|---|---|
| `01_direction_eda.py` | Does OFI predict next-minute direction? (no) |
| `02_volatility_baselines.py` | Baselines for next-minute volatility |
| `03_linear_ladder.py` | Linear model, feature sets A / C / B |
| `04_xgb_ladder.py` | XGBoost, same feature sets, train vs test R2 |
| `05_xgb_walk_forward.py` | Same models on several time periods |
| `10_check_tables.py` | Row counts and time range per table |
| `11_check_silver_gaps.py` | Gaps in silver windows |
| `12_check_bronze_and_nulls.py` | Latest bronze ticks, null volatility rows |
| `13_check_gold_quality.py` | Duplicates and gaps in gold tables |
| `silver_bug_investigation.py` | Step-by-step debugging of the first/last price bug |

`common.py` holds the kline loader and feature definitions, `s3_duckdb.py` the S3 connection.

## Findings

- OFI gives no usable signal for next-minute direction (lag-1 correlation ~0.01)
- Next-minute volatility is predictable from its own history: R2 0.482 (15-minute average) and 0.511 (mix of windows)
- Volatility history alone gives test R2 of about 0.41 (walk-forward, 4 years of BTCUSDT).
- Volume, trade count and hour of day add a small, consistent gain (+0.002 R2, better in 5 of 5 folds).
- |OFI| adds about +0.0001 R2, which is negligible, even with 2M rows and walk-forward validation.
- Results on a 3-month sample were misleading (extra features looked harmful), so the longer history mattered.