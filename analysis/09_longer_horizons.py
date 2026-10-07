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