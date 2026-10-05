# Crypto Market Microstructure Pipeline

A real-time streaming pipeline that ingests BTC/USDT and ETH/USDT tick data and engineers market microstructure features (order flow imbalance, VWAP) through a medallion architecture. Bronze, Silver and Gold are built; the streaming infrastructure is complete. ML work (next-minute volatility prediction) is in progress. Serving layer re-scoped to a local Streamlit dashboard.

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
| ML (volatility model) | In progress, local |
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

ML and analysis (local): pandas, scikit-learn, XGBoost, Binance historical klines from data.binance.vision.

Planned: Streamlit dashboard, MLflow for experiment tracking.

## Deviations from the original design, and why

- **Binance instead of Alpaca** -- Binance's public WebSocket needs no auth and delivers the same tick fields. The tradeoff: Binance's free stream gives trade ticks, not full Level 2 order book, so OFI here is approximated using taker side (`is_market_maker`) rather than true book-based OFI -- a standard proxy in the literature, not a shortcut unique to this project.
- **Redpanda Cloud instead of local Docker** -- Redpanda has no native Windows binary. Redpanda Cloud Serverless is Kafka-API compatible, so no code changed, only the bootstrap URL and SASL credentials.
- **Spark on EC2 instead of local Docker** -- local Spark on Windows hit a `UnixStreamServer` socket error (PySpark tries to open a Unix domain socket at driver startup, which doesn't exist on Windows). EC2 running Amazon Linux avoids this entirely.
- **Airflow replaced by GitHub Actions** -- the batch Gold jobs run on a schedule on GitHub Actions runners instead of an Airflow DAG.
- **Local modeling on Binance klines instead of more live ingestion** --  Binance's free 1-minute klines reproduce the pipeline's OFI (correlation 0.9995), which allowed months of data to be used locally at zero cost.
- **Target changed from direction to volatility** -- see Findings.

## What's built

**Bronze** — raw trade ticks land in Delta format on S3, streamed from Redpanda with `failOnDataLoss=false` (Redpanda ages out old messages, and a stale checkpoint pointing at expired offsets would otherwise kill the stream).

**Silver** — 1-minute VWAP, trade count, high/low/first/last price per symbol, computed with a watermark to handle late-arriving ticks.

**Gold** — order flow imbalance per symbol per 1-minute window: buy volume (taker = buyer) minus sell volume (taker = seller), normalized to a -1 to +1 range. Batch Gold jobs compute volatility, momentum, direction features and a BTC/ETH cross-asset signal.

## Findings so far

- **Direction: no usable signal from OFI.** Lag-1 correlation between OFI and next-minute price move is about 0.01 (BTC) and 0.005 (ETH), on both 3 days and 3 months of data. A bucket test showed 48.1% UP in the most-selling bucket vs 51.3% in the most-buying bucket for BTC, too small to trade.
- **Volatility: strongly predictable from its own history.** Minute-to-minute volatility persistence is about 0.63 for both symbols. On a held-out test set (BTCUSDT, 3 months, chronological 70/30 split), predicting next-minute log-volatility with "same as now" gives R² 0.205 and a 15-minute average gives R² 0.482.
- **Linear model ladder (BTCUSDT):** volatility history only R² 0.511; adding volume, trades and hour of day R² 0.511; adding |OFI| R² 0.511. So a mix of volatility horizons beats the plain 15-minute average, while volume, trades, time of day and |OFI| add nothing in a linear model.
- **Not yet shown:** this is one split on one asset, and a linear model can't capture non-linear effects. Next: XGBoost on the same features, then walk-forward validation, ETH, and a shuffled-target sanity check.

## Known issues

- **Silver first/last price bug.** `first_price` / `last_price` use order-unsafe aggregates (`F.first` / `F.last`), so they are non-deterministic. This corrupted `momentum`, `log_return` and `direction` in the Gold volatility table (confirmed by recomputing from raw bronze ticks). OFI, min/max and trade counts are not affected. Fix: ordered aggregates, e.g. `min_by("price", struct("trade_time", "trade_id"))` and `max_by(...)`, with a new checkpoint location and a backfill from bronze. 
- **Inflated FLAT direction labels** in the Gold volatility table, a side effect of the silver bug.

## Other relevant bugs fixed along the way

- `AMBIGUOUS_REFERENCE` on column names differing only by case --> `spark.sql.caseSensitive=true`
- Stream killed by stale checkpoint offsets after a Redpanda topic reset --> cleared S3 checkpoint contents, added `failOnDataLoss` handling
- Early EDA used the corrupted columns --> rebuilt features from Binance klines and re-ran the tests

## Setup (high-level)

- AWS account (S3, EC2, IAM) -- only needed to run the streaming pipeline
- Redpanda Cloud Serverless account -- only needed to run the streaming pipeline
- Python 3.11, PySpark 4.1.0, `kafka-python`, `delta-spark`
- For the local ML work: pandas, scikit-learn, XGBoost, and Binance 1-minute kline CSVs from data.binance.vision
- `.env` for AWS and Redpanda credentials -- nothing hardcoded

The repo produces Delta tables in S3 (queryable directly via Spark) from previously collected data.
