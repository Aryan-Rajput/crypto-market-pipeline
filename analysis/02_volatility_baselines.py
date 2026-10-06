# Baselines for next-minute log-volatility BTCUSDT, last 30% of time as test.
# Result: same-as-now R2 = 0.205, average of last 15 minutes R2 = 0.482.

from common import add_volatility_features, chrono_split, load_klines, r2

k = add_volatility_features(load_klines("BTCUSDT"))
k["base_last"] = k["lv"]
k["base_avg15"] = k["lv"].rolling(15).mean()

d = k.dropna(subset=["target", "base_last", "base_avg15"])
_, test = chrono_split(d)

print("test rows:", len(test))
print("R2 same-as-now:", round(r2(test["target"], test["base_last"]), 3))
print("R2 avg-15min:  ", round(r2(test["target"], test["base_avg15"]), 3))
