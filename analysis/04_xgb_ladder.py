# Same A / C / B ladder with XGBoost, train and test R2 side by side.

# Result (BTCUSDT, 70/30 split):
#     A  train 0.40498 | test 0.51157
#     C  train 0.41071 | test 0.50356
#     B  train 0.41176 | test 0.50747
# Train R2 is below test R2, so this is not overfitting. The test period is more
# spread out (target std 2.81 vs 1.90), which inflates its R2. Only compare
# feature sets on the same rows, not R2 across periods.

from xgboost import XGBRegressor

from common import FEATURE_LABELS, FEATURE_SETS, XGB_PARAMS, chrono_split, get_model_frame, r2

train, test = chrono_split(get_model_frame("BTCUSDT"))
print("train rows:", len(train), "test rows:", len(test))
print("target std  train:", round(train["target"].std(), 4), "| test:", round(test["target"].std(), 4))
print("target mean train:", round(train["target"].mean(), 4), "| test:", round(test["target"].mean(), 4))

for key, cols in FEATURE_SETS.items():
    model = XGBRegressor(**XGB_PARAMS)
    model.fit(train[cols], train["target"])
    pred_train = model.predict(train[cols])
    pred_test = model.predict(test[cols])
    print(f"{key}: {FEATURE_LABELS[key]}",
          "| train R2:", round(r2(train["target"], pred_train), 5),
          "| test R2:", round(r2(test["target"], pred_test), 5))
