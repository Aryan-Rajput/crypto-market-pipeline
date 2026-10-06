import os

import duckdb

from common import ROOT

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

BUCKET = os.environ.get("S3_BUCKET", "crypto-pipeline-ar-v3")
REGION = os.environ.get("AWS_REGION", "ap-southeast-1")

TABLES = {
    "bronze": "bronze-ticks/",
    "silver": "silver-features/",
    "ofi": "gold/ofi-features/",
    "volatility": "gold/volatility-features/",
    "cross_asset": "gold/cross-asset-signal/",
}


def table(name):
    return f"s3://{BUCKET}/{TABLES[name]}"


def connect():
    key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not key or not secret:
        raise RuntimeError("Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your environment or .env")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL delta; LOAD delta;")
    con.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{key}',
            SECRET '{secret}',
            REGION '{REGION}'
        )
    """)
    return con
