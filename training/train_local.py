from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import joblib
import pandas as pd
import yaml

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


ARTIFACT_DIR = "artifacts"


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _band(pd_val: float, low_max: float, med_max: float) -> str:
    if pd_val < low_max:
        return "LOW"
    if pd_val < med_max:
        return "MEDIUM"
    return "HIGH"


def train_local(
    model_cfg_path: str = os.path.join("training", "configs", "model.yaml"),
    data_cfg_path: str = os.path.join("training", "configs", "data.yaml"),
) -> None:
    model_cfg = _load_yaml(model_cfg_path)
    data_cfg = _load_yaml(data_cfg_path)

    horizon = int(model_cfg["horizon_months"])
    label_col = data_cfg.get("label_col") or data_cfg["label_col_template"].format(horizon=horizon)

    df = pd.read_parquet(data_cfg["features_parquet"])

    id_cols = data_cfg.get("id_cols", [])
    cat_cols = [c for c in data_cfg.get("categorical_cols", []) if c in df.columns]

    drop_cols = set(id_cols + [label_col] + data_cfg.get("drop_cols", []))
    num_cols = [c for c in df.columns if c not in drop_cols and c not in cat_cols]

    assert label_col in df.columns, f"Missing {label_col} in features parquet"
    y = df[label_col].astype(int)

    X = df[num_cols + cat_cols].copy()

    group_col = model_cfg["split"]["group_col"]
    g = df[group_col].astype(str)

    gss = GroupShuffleSplit(
        n_splits=1,
        test_size=float(model_cfg["split"]["test_size"]),
        random_state=int(model_cfg["split"]["random_state"]),
    )
    (train_idx, test_idx) = next(gss.split(X, y, groups=g))

    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("imp", SimpleImputer(strategy="median"))]), num_cols),
            ("cat", Pipeline([
                ("imp", SimpleImputer(strategy="most_frequent")),
                ("ohe", OneHotEncoder(handle_unknown="ignore")),
            ]), cat_cols),
        ],
        remainder="drop",
    )

    clf = LogisticRegression(max_iter=200, n_jobs=None)
    pipe = Pipeline([("pre", pre), ("clf", clf)])

    pipe.fit(X_train, y_train)

    p_test = pipe.predict_proba(X_test)[:, 1]
    metrics = {
        "horizon_months": horizon,
        "roc_auc": float(roc_auc_score(y_test, p_test)) if len(set(y_test)) > 1 else None,
        "pr_auc": float(average_precision_score(y_test, p_test)) if len(set(y_test)) > 1 else None,
        "brier": float(((p_test - y_test.values) ** 2).mean()),
        "n_train": int(len(train_idx)),
        "n_test": int(len(test_idx)),
        "label_rate_test": float(y_test.mean()),
        "model_version": str(model_cfg["model_version"]),
    }

    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    joblib.dump(
        {
            "pipeline": pipe,
            "num_cols": num_cols,
            "cat_cols": cat_cols,
            "horizon_months": horizon,
            "risk_thresholds": model_cfg["risk_thresholds"],
            "model_version": model_cfg["model_version"],
        },
        os.path.join(ARTIFACT_DIR, "model.joblib"),
    )

    with open(os.path.join(ARTIFACT_DIR, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    with open(os.path.join(ARTIFACT_DIR, "model_version.txt"), "w") as f:
        f.write(str(model_cfg["model_version"]))

    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    train_local()