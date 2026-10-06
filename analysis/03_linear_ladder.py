# Linear regression on feature sets
#    A (vol history)
#    C (+ volume, trades, hour)
#    B (C + |OFI|)

# Result -> all three score R2 = 0.511 hence volume and trades and hour and |OFI| add nothing here.

from sklearn.linear_model import LinearRegression

from common import FEATURE_LABELS, FEATURE_SETS, chrono_split, get_model_frame, r2

train, test = chrono_split(get_model_frame("BTCUSDT"))

for key, cols in FEATURE_SETS.items():
    model = LinearRegression().fit(train[cols], train["target"])
    print(f"{key}: {FEATURE_LABELS[key]} R2:", round(r2(test["target"], model.predict(test[cols])), 5))
