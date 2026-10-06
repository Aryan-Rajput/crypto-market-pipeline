"""Download Binance monthly 1-minute klines into data/raw/klines/.

Run from the repo root:  python analysis/download_klines.py
Files that already exist are skipped, so it is safe to re-run.
"""
import io
import urllib.error
import urllib.request
import zipfile

from common import KLINES_DIR

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
START = (2022, 10)      # first month to download (year, month)
END = (2024, 10)         # last month to download
BASE = "https://data.binance.vision/data/spot/monthly/klines"


def months(start, end):
    y, m = start
    while (y, m) <= end:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


KLINES_DIR.mkdir(parents=True, exist_ok=True)

for symbol in SYMBOLS:
    for y, m in months(START, END):
        name = f"{symbol}-1m-{y}-{m:02d}"
        target = KLINES_DIR / f"{name}.csv"
        if target.exists():
            print("skip (already have)", name)
            continue

        url = f"{BASE}/{symbol}/1m/{name}.zip"
        try:
            with urllib.request.urlopen(url, timeout=120) as resp:
                data = resp.read()
        except urllib.error.HTTPError as e:
            print("not available", name, e.code)
            continue

        with zipfile.ZipFile(io.BytesIO(data)) as z:
            z.extract(f"{name}.csv", KLINES_DIR)
        print("downloaded", name)