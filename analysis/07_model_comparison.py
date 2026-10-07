"""Model comparison on one fixed feature set (C) and one fixed train/test split.

Tuned models pick their depth and number of trees on a validation slice taken
from the END of the training data (early stopping), so the test set is only
used once, for the final score.

Result: add the printed table here after running.
"""
import time

import lightgbm as lgb
import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.linear_model import Ridge
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor

from common import C_FEATURES, XGB_PARAMS, chrono_split, get_model_frame, r2

COLS = C_FEATURES
DEPTHS = (3, 5, 7)

train, test = chrono_split(get_model_frame("BTCUSDT"))
fit_part, val_part = chrono_split(train, 0.8)
print("fit rows:", len(fit_part), "| validation rows:", len(val_part), "| test rows:", len(test))

rows = []


def add_row(name, settings, pred, seconds):
    score = r2(test["target"], pred)
    rows.append({"model": name, "settings": settings, "test_R2": round(score, 5),
                 "seconds": round(seconds, 1)})
    print(f"{name}: test R2 {score:.5f} ({seconds:.1f}s)")


def timed_fit(model, X, y):
    start = time.time()
    model.fit(X, y)
    return time.time() - start


# baseline: average of the last 15 minutes, no model
add_row("15-min average (baseline)", "no model", test["lv_avg15"], 0.0)

# Ridge
m = Ridge(alpha=1.0)
sec = timed_fit(m, train[COLS], train["target"])
add_row("Ridge", "alpha=1", m.predict(test[COLS]), sec)

# single decision tree
m = DecisionTreeRegressor(max_depth=8, min_samples_leaf=200, random_state=0)
sec = timed_fit(m, train[COLS], train["target"])
add_row("Decision tree", "depth=8", m.predict(test[COLS]), sec)

# XGBoost with the settings used in scripts 03-06
m = XGBRegressor(**XGB_PARAMS)
sec = timed_fit(m, train[COLS], train["target"])
add_row("XGBoost default", f"depth=3, trees=300", m.predict(test[COLS]), sec)

# XGBoost tuned: pick depth and tree count on the validation slice
best = None
for depth in DEPTHS:
    m = XGBRegressor(n_estimators=1500, max_depth=depth, learning_rate=0.05,
                     early_stopping_rounds=50)
    m.fit(fit_part[COLS], fit_part["target"],
          eval_set=[(val_part[COLS], val_part["target"])], verbose=False)
    val_r2 = r2(val_part["target"], m.predict(val_part[COLS]))
    trees = m.best_iteration + 1
    print(f"  XGB depth {depth}: validation R2 {val_r2:.5f}, trees kept {trees}")
    if best is None or val_r2 > best[0]:
        best = (val_r2, depth, trees)

_, depth, trees = best
m = XGBRegressor(n_estimators=trees, max_depth=depth, learning_rate=0.05)
sec = timed_fit(m, train[COLS], train["target"])
add_row("XGBoost tuned", f"depth={depth}, trees={trees}", m.predict(test[COLS]), sec)

# LightGBM tuned the same way (num_leaves matched to depth)
best = None
for depth in DEPTHS:
    m = LGBMRegressor(n_estimators=1500, max_depth=depth, num_leaves=2 ** depth,
                      learning_rate=0.05, verbose=-1)
    m.fit(fit_part[COLS], fit_part["target"],
          eval_X=val_part[COLS], eval_y=val_part["target"],
          callbacks=[lgb.early_stopping(50, verbose=False)])
    val_r2 = r2(val_part["target"], m.predict(val_part[COLS]))
    trees = m.best_iteration_
    print(f"  LGBM depth {depth}: validation R2 {val_r2:.5f}, trees kept {trees}")
    if best is None or val_r2 > best[0]:
        best = (val_r2, depth, trees)

_, depth, trees = best
m = LGBMRegressor(n_estimators=trees, max_depth=depth, num_leaves=2 ** depth,
                  learning_rate=0.05, verbose=-1)
sec = timed_fit(m, train[COLS], train["target"])
add_row("LightGBM tuned", f"depth={depth}, trees={trees}", m.predict(test[COLS]), sec)

# results table
table = pd.DataFrame(rows).sort_values("test_R2", ascending=False).reset_index(drop=True)
baseline = table.loc[table["model"] == "15-min average (baseline)", "test_R2"].iloc[0]
table["lift_vs_baseline"] = (table["test_R2"] - baseline).round(5)
print()
print(table.to_string(index=False))