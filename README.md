# Crypto Market Microstructure Pipeline

A real-time streaming pipeline that ingests BTC/USDT and ETH/USDT tick data and engineers market microstructure features (order flow imbalance, VWAP) through a medallion architecture. Bronze, Silver and Gold are built; the streaming infrastructure is complete. ML work (next-minute volatility prediction) is complete and documented in Findings. Serving layer re-scoped to a local Streamlit dashboard.

---

## Why this project exists

I wanted to work with Kafka/Redpanda

---

## Status

| Layer | Status |
|---|---|
| Bronze (raw tick ingestion) | Complete |
| Silver (VWAP features) | Complete, with a known first/last price bug (see Known issues) |
| Gold OFI features | Complete, ran end-to-end |
| Gold volatility, cross-asset signal | Built as batch jobs on GitHub Actions. |
| ML (volatility model) | Complete, local (see Findings) |
| ML (direction classifier) | Tested, no usable signal from OFI (documented null result) |
| Dashboard | Planned: local Streamlit replay dashboard |
| Redis / TimescaleDB serving layer | Not started |
| FastAPI + React frontend | Not started, replaced by the Streamlit plan |

## Stack (actual, not original plan)

```
Binance public WebSocket (BTC/USDT, ETH/USDT trade stream)
  -> Redpanda Cloud Serverless (Kafka-API compatible)
  -> PySpark 4.1.0 Structured Streaming, running on EC2 (m7i-flex.large)
  -> AWS S3, Delta Lake format
      Bronze -> Silver -> Gold
```

Orchestration: GitHub Actions runs the batch Gold jobs (the earlier Airflow DAG was removed).

ML and analysis (local): pandas, scikit-learn, XGBoost, LightGBM, arch (GARCH), Binance historical klines from data.binance.vision.

Planned: Streamlit dashboard, MLflow for experiment tracking.

## Deviations from the original design, and why

- **Binance instead of Alpaca** -- Binance's public WebSocket needs no auth and delivers the same tick fields. The tradeoff: Binance's free stream gives trade ticks, not full Level 2 order book, so OFI here is approximated using taker side (`is_market_maker`) rather than true book-based OFI -- a standard proxy in the literature, not a shortcut unique to this project.
- **Redpanda Cloud instead of local Docker** -- Redpanda has no native Windows binary. Redpanda Cloud Serverless is Kafka-API compatible, so no code changed, only the bootstrap URL and SASL credentials.
- **Spark on EC2 instead of local Docker** -- local Spark on Windows hit a `UnixStreamServer` socket error (PySpark tries to open a Unix domain socket at driver startup, which doesn't exist on Windows). EC2 running Amazon Linux avoids this entirely.
- **Airflow replaced by GitHub Actions** -- the batch Gold jobs run on a schedule on GitHub Actions runners instead of an Airflow DAG.
- **Local modeling on Binance klines instead of more live ingestion** --  Binance's free 1-minute klines reproduce the pipeline's OFI (correlation 0.9995), which allowed 4 years of data (about 2.1M one-minute rows per symbol) to be used locally at zero cost.
- **Target changed from direction to volatility** -- see Findings.

## What's built

**Bronze** — raw trade ticks land in Delta format on S3, streamed from Redpanda with `failOnDataLoss=false` (Redpanda ages out old messages, and a stale checkpoint pointing at expired offsets would otherwise kill the stream).

**Silver** — 1-minute VWAP, trade count, high/low/first/last price per symbol, computed with a watermark to handle late-arriving ticks.

**Gold** — order flow imbalance per symbol per 1-minute window: buy volume (taker = buyer) minus sell volume (taker = seller), normalized to a -1 to +1 range. Batch Gold jobs compute volatility, momentum, direction features and a BTC/ETH cross-asset signal.

## Findings

All modeling results are on BTCUSDT, 4 years of 1-minute klines (about 2.1M rows), scored on held-out data in time order (chronological 70/30 split, plus 5-fold walk-forward validation).

- **Direction: no usable signal from OFI.** Lag-1 correlation between OFI and next-minute price move is about 0.01 (BTC) and 0.005 (ETH), on both 3 days and 3 months of data. A bucket test showed 48.1% UP in the most-selling bucket vs 51.3% in the most-buying bucket for BTC, too small to trade.
- **Volatility: strongly predictable from its own history.** Minute-to-minute volatility persistence is about 0.63 for both symbols. Predicting next-minute log-volatility (log of the high-low range) with "same as now" gives R² 0.014, and a 15-minute average gives R² 0.379. That 0.379 is the bar every model has to beat.
- **Models beat the baseline by about +0.047 R².** The boosted trees tie, and simple models are close behind:

| Model (feature set C, test R²) | R² |
|---|---|
| LightGBM (tuned) | 0.4265 |
| XGBoost (tuned) | 0.4264 |
| XGBoost (default, depth 3, 300 trees) | 0.4260 |
| Decision tree (depth 8) | 0.4179 |
| Ridge | 0.4143 |
| 15-minute average (baseline) | 0.3790 |
| GARCH(1,1), calibrated | 0.2939 |

  Tree depth (3, 5, 7) made no difference (validation R² within 0.0003). GARCH sees only close-to-close returns, while the target and baseline use the high-low range, so it is a benchmark, not an even contest.
- **Feature sets (XGBoost, walk-forward average test R²):** volatility history only 0.4090; plus day and week windows 0.4073; plus volume, trades and hour of day 0.4110; plus |OFI| 0.4111. Volume, trades and hour give a small, consistent gain (about +0.002, better in 5 of 5 folds). |OFI| adds essentially nothing (between -0.0003 and +0.0003 per fold). Day and week windows add nothing, and neither does ETH's volatility as an input for BTC (0.4107 vs 0.4110, helped in 1 of 5 folds).
- **Longer horizons are easier to predict, but the lift shrinks.** Averaging removes minute-level noise:

| Horizon | Baseline R² | Model R² | Lift |
|---|---|---|---|
| 1 min | 0.017 | 0.426 | +0.409 |
| 5 min | 0.505 | 0.686 | +0.181 |
| 15 min | 0.704 | 0.786 | +0.082 |
| 60 min | 0.771 | 0.819 | +0.049 |

  For the 1-minute row the baseline is "same as now"; against the 15-minute average (0.379) the lift is about +0.047.
- **Sample size matters.** On the first 3 months of data the extra features looked harmful (volume, trades and hour scored below volatility history alone with XGBoost) and the linear ladder gave identical R² (0.511) for all feature sets. With 4 years the same features showed the small consistent gain above.
- **Limitations:** one asset for modeling (BTC), one horizon for the feature tests (1 minute), OFI is a taker-side proxy rather than full order-book OFI, and ETH was not modeled on its own. There is no trading simulation, so none of this says anything about profitability.

## Known issues

- **Silver first/last price bug.** `first_price` / `last_price` use order-unsafe aggregates (`F.first` / `F.last`), so they are non-deterministic. This corrupted `momentum`, `log_return` and `direction` in the Gold volatility table (confirmed by recomputing from raw bronze ticks). OFI, min/max and trade counts are not affected. Fix: ordered aggregates, e.g. `min_by("price", struct("trade_time", "trade_id"))` and `max_by(...)`, with a new checkpoint location and a backfill from bronze. The fixed job is written;
- **Inflated FLAT direction labels** in the Gold volatility table, a side effect of the silver bug.

## Other relevant bugs fixed along the way

- `AMBIGUOUS_REFERENCE` on column names differing only by case --> `spark.sql.caseSensitive=true`
- Stream killed by stale checkpoint offsets after a Redpanda topic reset --> cleared S3 checkpoint contents, added `failOnDataLoss` handling
- Early EDA used the corrupted columns --> rebuilt features from Binance klines and re-ran the tests

## Setup (high-level)

- AWS account (S3, EC2, IAM) -- only needed to run the streaming pipeline
- Redpanda Cloud Serverless account -- only needed to run the streaming pipeline
- Python 3.11, PySpark 4.1.0, `kafka-python`, `delta-spark`
- For the local ML work: pandas, scikit-learn, XGBoost, LightGBM, arch, and Binance 1-minute kline CSVs from data.binance.vision (placed in `data/raw/klines/`; scripts and run order in `analysis/README.md`)
- `.env` for AWS and Redpanda credentials -- nothing hardcoded

The repo produces Delta tables in S3 (queryable directly via Spark) from previously collected data.