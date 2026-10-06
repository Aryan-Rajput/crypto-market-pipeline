from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
KLINES_DIR = ROOT / "data" / "raw" / "klines"
EXPORTS_DIR = ROOT / "data" / "exports"

KLINE_COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
              "quote_vol", "trades", "taker_buy_base", "taker_buy_quote", "ignore"]

A_FEATURES = ["lv_avg1", "lv_avg5", "lv_avg15", "lv_avg60"]
B_FEATURES = A_FEATURES + ["lvolume", "ltrades", "abs_ofi5", "hsin", "hcos"]
C_FEATURES = [c for c in B_FEATURES if c != "abs_ofi5"]
FEATURE_SETS = {"A": A_FEATURES, "C": C_FEATURES, "B": B_FEATURES}
FEATURE_LABELS = {"A": "vol history only",
                  "C": "+ volume, trades, hour (no OFI)",
                  "B": "C + |OFI|"}

XGB_PARAMS = dict(n_estimators=300, max_depth=3, learning_rate=0.05)


def load_klines(symbol):
    files = sorted(KLINES_DIR.glob(f"{symbol}-1m-*.csv"))
    if not files:
        raise FileNotFoundError(f"No {symbol}-1m-*.csv files in {KLINES_DIR}")
    frames = []
    for f in files:
        k = pd.read_csv(f, header=None, names=KLINE_COLS)
        k = k.apply(pd.to_numeric, errors="coerce").dropna(subset=["open_time"])
        frames.append(k)
    k = pd.concat(frames).sort_values("open_time").drop_duplicates("open_time")
    k["open_time"] = k["open_time"].astype("int64")
    k.loc[k["open_time"] > 1e14, "open_time"] //= 1000    # microseconds -> ms
    k = k.reset_index(drop=True)
    k["ofi_norm"] = (2 * k["taker_buy_base"] - k["volume"]) / k["volume"]
    k["momentum"] = k["close"] - k["open"]
    k["gap"] = k["open_time"].diff()                      # 60000 = consecutive minutes
    k["ofi_lag1"] = k["ofi_norm"].shift(1)
    return k


def add_volatility_features(k):
    k = k.copy()
    k["vol"] = (k["high"] - k["low"]) / k["open"]
    k["lv"] = np.log(k["vol"].replace(0, np.nan))
    k["target"] = k["lv"].shift(-1)                       # next minute's log-range

    for w in (1, 5, 15, 60):
        k[f"lv_avg{w}"] = k["lv"].rolling(w).mean()

    k["lvolume"] = np.log(k["volume"].replace(0, np.nan)).rolling(5).mean()
    k["ltrades"] = np.log(k["trades"].replace(0, np.nan)).rolling(5).mean()
    k["abs_ofi5"] = k["ofi_norm"].abs().rolling(5).mean()

    hour = pd.to_datetime(k["open_time"], unit="ms", utc=True).dt.hour
    k["hsin"] = np.sin(2 * np.pi * hour / 24)
    k["hcos"] = np.cos(2 * np.pi * hour / 24)
    return k


def get_model_frame(symbol="BTCUSDT"):
    k = add_volatility_features(load_klines(symbol))
    return k.dropna(subset=B_FEATURES + ["target"]).reset_index(drop=True)


def chrono_split(d, train_frac=0.7):
    cut = int(len(d) * train_frac)
    return d.iloc[:cut], d.iloc[cut:]


def r2(y, p):
    return 1 - ((y - p) ** 2).sum() / ((y - y.mean()) ** 2).sum()
