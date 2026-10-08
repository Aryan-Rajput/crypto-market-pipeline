
# Result (average test R2): BTC only 0.41095, BTC + ETH 0.41067. ETH gain per fold:
# -0.0005, -0.0001, -0.0007, +0.00004, -0.0001; ETH helped in 1 of 5 folds. No gain.
#

import numpy as np
from xgboost import XGBRegressor

from common import C_FEATURES, L_FEATURES, XGB_PARAMS, add_volatility_features, load_klines, r2

N_CHUNKS = 6
WINDOWS = (1, 5, 15, 60)

btc = add_volatility_features(load_klines("BTCUSDT"))
eth = add_volatility_features(load_klines("ETHUSDT"))

eth_cols = [f"lv_avg{w}" for w in WINDOWS]
eth = eth[["open_time"] + eth_cols].rename(columns={c: f"eth_{c}" for c in eth_cols})
ETH_FEATURES = [f"eth_{c}" for c in eth_cols]

d = btc.merge(eth, on="open_time", how="inner").sort_values("open_time")
d = d.dropna(subset=C_FEATURES + L_FEATURES + ETH_FEATURES + ["target"]).reset_index(drop=True)
print("rows:", len(d))

SETS = {"BTC only": C_FEATURES, "BTC + ETH": C_FEATURES + ETH_FEATURES}
edges = np.linspace(0, len(d), N_CHUNKS + 1).astype(int)
results = {name: [] for name in SETS}

for i in range(1, N_CHUNKS):
    train = d.iloc[: edges[i]]
    test = d.iloc[edges[i]: edges[i + 1]]
    line = f"fold {i} |"
    for name, cols in SETS.items():
        model = XGBRegressor(**XGB_PARAMS)
        model.fit(train[cols], train["target"])
        score = r2(test["target"], model.predict(test[cols]))
        results[name].append(score)
        line += f" {name}: {score:.5f}"
    print(line)

print("average test R2:", {n: round(float(np.mean(v)), 5) for n, v in results.items()})
diff = np.array(results["BTC + ETH"]) - np.array(results["BTC only"])
print("ETH gain per fold:", np.round(diff, 5), "| folds where ETH helps:", int((diff > 0).sum()), "of", len(diff))