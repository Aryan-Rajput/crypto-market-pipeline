# Crypto Market Microstructure Pipeline

A real-time streaming pipeline that ingests BTC/USDT and ETH/USDT tick data and engineers market microstructure features (order flow imbalance, VWAP) through a medallion architecture. **In progress Bronze and Silver complete, Gold partially built, ML and serving layer not started.**

---

## Why this project exists

I wanted to work with Kafka/Redpanda

---

## Status

| Layer | Status |
|---|---|
| Bronze (raw tick ingestion) | Complete |
| Silver (VWAP features) |  Complete |
| Gold OFI features | Written, not yet confirmed running end-to-end |
| Gold volatility, cross-asset signal | Not started |
| ML (XGBoost classifier) | Deliberately deferred not enough Gold-layer data accumulated yet to train on |
| Redis / TimescaleDB serving layer | Not started |
| FastAPI + React frontend | Not started |

## Stack (actual, not original plan)

```
Binance public WebSocket (BTC/USDT, ETH/USDT trade stream)
  Redpanda Cloud Serverless (Kafka-API compatible)
  PySpark 4.1.0 Structured Streaming, running on EC2 (m7i-flex.large)
  AWS S3, Delta Lake format
      Bronze -> Silver -> Gold
```

Planned but not yet built: MLflow (model tracking), FastAPI (serving), ElastiCache Redis + RDS TimescaleDB (low-latency + historical serving), React (frontend).

## Deviations from the original design, and why

- **Binance instead of Alpaca** Alpaca requires full KYC even for paper trading, which would have blocked the project start by days. Binance's public WebSocket needs no auth and delivers the same tick fields. The tradeoff: Binance's free stream gives trade ticks, not full Level 2 order book, so OFI here is approximated using taker side (`is_market_maker`) rather than true book-based OFI a standard proxy in the literature, not a shortcut unique to this project.
- **Redpanda Cloud instead of local Docker** the dev machine doesn't have WSL2, and Redpanda has no native Windows binary. Redpanda Cloud Serverless is Kafka-API compatible, so no code changed only the bootstrap URL and SASL credentials.
- **Spark on EC2 instead of local Docker**  local Spark on Windows hit a `UnixStreamServer` socket error (PySpark tries to open a Unix domain socket at driver startup, which doesn't exist on Windows). EC2 running Amazon Linux avoids this entirely.

## What's built

**Bronze** raw trade ticks land in Delta format on S3, streamed from Redpanda with `failOnDataLoss=false` (Redpanda ages out old messages, and a stale checkpoint pointing at expired offsets would otherwise kill the stream).

**Silver** 1-minute VWAP, trade count, high/low/first/last price per symbol, computed with a watermark to handle late-arriving ticks.

**Gold (in progress)** order flow imbalance per symbol per 1-minute window: buy volume (taker = buyer) minus sell volume (taker = seller), normalized to a -1 to +1 range. Written, checkpointed to S3, but not yet confirmed running cleanly end-to-end on EC2.

## Why ML hasn't started

An XGBoost classifier is planned once there's a stable multi-day accumulation of Gold-layer features training on a few hours of data isn't representative of the market regimes the model would need to generalize across. Rather than fit something on too little data to make an ML bullet point, this stage is explicitly parked until there's enough signal history.

## Interview-relevant bugs fixed along the way

- `AMBIGUOUS_REFERENCE` on column names differing only by case `spark.sql.caseSensitive=true`
- Silent Kafka SASL auth failure from a typo'd config key (`kafka.sasl.jaas` vs `kafka.sasl.jaas.config`)
- Spark 4.1.0 not available via direct binary download installed via `pip install pyspark==4.1.0`
- Stream killed by stale checkpoint offsets after a Redpanda topic reset â†’ cleared S3 checkpoint contents, added `failOnDataLoss` handling

## Setup (high-level)

- AWS account (S3, EC2, IAM)
- Redpanda Cloud Serverless account
- Python 3.11, PySpark 4.1.0, `kafka-python`, `delta-spark`
- `.env` for AWS and Redpanda credentials nothing hardcoded

No frontend, API, or caching layer exists yet this repo currently produces Delta tables in S3, queryable directly via Spark.
