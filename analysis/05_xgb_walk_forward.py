# Walk-forward validation (feature sets A, L = A + day/week windows, C, B): the data is cut into blocks in time order, and each
# fold trains on all earlier blocks and tests on the next one.

# Result (average test R2): A 0.40896, L 0.40734, C 0.41095, B 0.41107.
# C - A: positive in 5 of 5 folds (+0.0001 to +0.0039): volume, trades, hour help a little.
# B - C: positive in 4 of 5 folds but only -0.0003 to +0.0003: |OFI| adds nothing.
# L - A: -0.0095 in fold 1, +0.0001 to +0.0006 in the rest: day/week windows add nothing.

import numpy as np
from xgboost import XGBRegressor

from common import FEATURE_SETS, XGB_PARAMS, get_model_frame, r2

N_CHUNKS = 6

d = get_model_frame("BTCUSDT")
edges = np.linspace(0, len(d), N_CHUNKS + 1).astype(int)
results = {name: [] for name in FEATURE_SETS}

for i in range(1, N_CHUNKS):
    train = d.iloc[: edges[i]]
    test = d.iloc[edges[i]: edges[i + 1]]
    line = f"fold {i}: train {len(train)} rows, test {len(test)} rows |"
    for name, cols in FEATURE_SETS.items():
        model = XGBRegressor(**XGB_PARAMS)
        model.fit(train[cols], train["target"])
        score = r2(test["target"], model.predict(test[cols]))
        results[name].append(score)
        line += f" {name}: {score:.5f}"
    print(line)

print()
print("average test R2:", {n: round(float(np.mean(v)), 5) for n, v in results.items()})
diff_LA = np.array(results["L"]) - np.array(results["A"])
print("L minus A per fold:", np.round(diff_LA, 5), "| folds where L > A:", int((diff_LA > 0).sum()), "of", len(diff_LA))
diff_BC = np.array(results["B"]) - np.array(results["C"])
diff_CA = np.array(results["C"]) - np.array(results["A"])
print("B minus C per fold:", np.round(diff_BC, 5), "| folds where B > C:", int((diff_BC > 0).sum()), "of", len(diff_BC))
print("C minus A per fold:", np.round(diff_CA, 5), "| folds where C > A:", int((diff_CA > 0).sum()), "of", len(diff_CA))