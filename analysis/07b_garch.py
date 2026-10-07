import numpy as np
from arch import arch_model
from sklearn.linear_model import LinearRegression

from common import B_FEATURES, L_FEATURES, add_volatility_features, chrono_split, load_klines, r2

FIT_ROWS = 500_000        # fit GARCH on the last part of train only (None = use all)

k = add_volatility_features(load_klines("BTCUSDT"))
k["ret"] = 1e4 * np.log(k["close"] / k["close"].shift(1))     # 1-minute return in basis points

# same rows and split as the other models
d = k.dropna(subset=B_FEATURES + L_FEATURES + ["target", "ret"])
train, test = chrono_split(d)
cut = train.index[-1]

# 1) fit GARCH(1,1) on training returns only
r_train = k.loc[:cut, "ret"].dropna()
if FIT_ROWS:
    r_train = r_train.iloc[-FIT_ROWS:]
fit = arch_model(r_train, mean="Zero", vol="GARCH", p=1, q=1).fit(disp="off")
print(fit.params)

# 2) apply the fixed parameters to the whole series (train + test)
r_all = k["ret"].dropna()
full = arch_model(r_all, mean="Zero", vol="GARCH", p=1, q=1).fix(fit.params)
sigma = full.conditional_volatility
k["garch_lv"] = np.log(sigma.shift(-1))      # forecast made at minute t for minute t+1

d = k.dropna(subset=B_FEATURES + L_FEATURES + ["target", "ret", "garch_lv"])
train, test = d.loc[:cut], d.loc[cut + 1:]
print("train rows:", len(train), "test rows:", len(test))

# 3) convert GARCH's forecast to the scale of our target (log high-low range)
cal = LinearRegression().fit(train[["garch_lv"]], train["target"])
pred = cal.predict(test[["garch_lv"]])
print("GARCH(1,1) test R2:", round(r2(test["target"], pred), 5))
print("15-min average test R2:", round(r2(test["target"], test["lv_avg15"]), 5))