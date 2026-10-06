# Does OFI predict the next minute's price direction as in  using Binance 1-minute klines

# Result --> no Lag-1 correlation is about 0.01 (BTC) and 0.005 (ETH) and the
# most-selling vs most-buying OFI buckets differ by only ~3 points in % UP.
# Volatility persistence is ~0.63, which is why the project moved to volatility.
import pandas as pd
from common import load_klines

SYMBOLS = ["BTCUSDT", "ETHUSDT"]

def correlations(k, sym):
    ok = k[k["gap"] == 60000]
    print(sym, "rows:", len(k), "pairs:", len(ok),
          "same:", round(k["ofi_norm"].corr(k["momentum"]), 3),
          "lag1:", round(ok["ofi_lag1"].corr(ok["momentum"]), 4))

def bucket_test(k, sym):
    # 5 OFI buckets (0 = most selling, 4 = most buying) --> % of next minutes that went up
    k = k.copy()
    k["next_mom"] = k["momentum"].shift(-1)
    b = k.dropna(subset=["ofi_norm", "next_mom"])
    b = b[b["next_mom"] != 0].copy()
    b["bucket"] = pd.qcut(b["ofi_norm"], 5, labels=False, duplicates="drop")
    print(sym, "% UP next minute by OFI bucket -->")
    print(b.groupby("bucket")["next_mom"].apply(lambda s: (s > 0).mean()).round(4))


def smoothed_ofi_and_persistence(k, sym):
    k = k.copy()
    k["next_mom"] = k["momentum"].shift(-1)
    k["ofi5"] = k["ofi_norm"].rolling(5).mean()
    print(sym, "ofi5 vs next momentum:", round(k["ofi5"].corr(k["next_mom"]), 4))

    k["vol"] = (k["high"] - k["low"]) / k["open"]
    print(sym, "volatility persistence:", round(k["vol"].corr(k["vol"].shift(-1)), 3))


if __name__ == "__main__":
    for sym in SYMBOLS:
        correlations(load_klines(sym), sym)
    for sym in SYMBOLS:
        k = load_klines(sym)
        bucket_test(k, sym)
        smoothed_ofi_and_persistence(k, sym)
