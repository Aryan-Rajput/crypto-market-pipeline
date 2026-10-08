# Result (test R2):
#     h = 1 min   baseline 0.017 | model 0.426 | lift +0.409
#     h = 5 min   baseline 0.505 | model 0.686 | lift +0.181
#     h = 15 min  baseline 0.704 | model 0.786 | lift +0.082
#     h = 60 min  baseline 0.771 | model 0.819 | lift +0.049
# R2 rises with horizon because averaging removes noise; the lift over the baseline shrinks.
# For h = 1 the fair comparison is the 15-min average (0.379): lift ~+0.047.


from xgboost import XGBRegressor

from common import (B_FEATURES, C_FEATURES, L_FEATURES, XGB_PARAMS, add_volatility_features,
                    chrono_split, load_klines, r2)

HORIZONS = (1, 5, 15, 60)
k = add_volatility_features(load_klines("BTCUSDT"))

for h in HORIZONS:
    k["target_h"] = k["lv"].rolling(h).mean().shift(-h)
    d = k.dropna(subset=B_FEATURES + L_FEATURES + ["target_h"]).reset_index(drop=True)
    train, test = chrono_split(d)
    train = train.iloc[:-h]

    model = XGBRegressor(**XGB_PARAMS)
    model.fit(train[C_FEATURES], train["target_h"])
    score = r2(test["target_h"], model.predict(test[C_FEATURES]))
    base = r2(test["target_h"], test[f"lv_avg{h}"])
    print(f"next {h} min: baseline R2 {base:.4f} | model R2 {score:.4f} | lift {score - base:.4f}")