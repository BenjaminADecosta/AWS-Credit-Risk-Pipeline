from __future__ import annotations

import os
from typing import Any

import pandas as pd
import yaml

from label_builder import LabelConfig, build_label_next_nm

DATA_DIR = "data"
MONTHLY_CSV = os.path.join(DATA_DIR, "sample_monthly.csv")
STATIC_CSV = os.path.join(DATA_DIR, "sample_static.csv")
OUTPUT_PARQUET = os.path.join(DATA_DIR, "sample_features.parquet")
DEV_CFG = os.path.join("pipelines", "configs", "dev.yaml")


def _load_cfg(path: str) -> dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _to_period(s: pd.Series) -> pd.Series:
    return pd.PeriodIndex(s.astype(str), freq="M")


def _is_30p(df: pd.DataFrame, dpd_col: str, status_col: str, delinquent_statuses: list[str]) -> pd.Series:
    if dpd_col in df.columns:
        return pd.to_numeric(df[dpd_col], errors="coerce").fillna(0).astype(int).ge(30)
    if status_col in df.columns:
        return df[status_col].astype(str).isin(set(delinquent_statuses))
    raise ValueError(f"Need either '{dpd_col}' or '{status_col}' in monthly data")


def build_local_features(cfg_path: str = DEV_CFG) -> None:
    cfg = _load_cfg(cfg_path)

    horizon = int(cfg["horizon_months"])
    windows = [int(x) for x in cfg.get("windows_months", [3, 6, 12])]

    mcfg = cfg["monthly"]
    id_col = mcfg["id_col"]
    month_col = mcfg["month_col"]
    dpd_col = mcfg.get("dpd_col", "days_past_due")
    status_col = mcfg.get("status_col", "status")
    delinquent_statuses = mcfg.get("delinquent_statuses", [])

    monthly = pd.read_csv(MONTHLY_CSV)
    monthly.columns = monthly.columns.str.strip().str.replace("\ufeff", "")
    monthly[month_col] = _to_period(monthly[month_col])

    monthly[id_col] = monthly[id_col].astype(str)

    monthly = monthly.sort_values([id_col, month_col])

    monthly["is_30p"] = _is_30p(monthly, dpd_col, status_col, delinquent_statuses)
    monthly["dpd_num"] = (
        pd.to_numeric(monthly[dpd_col], errors="coerce").fillna(0).astype(int)
        if dpd_col in monthly.columns else 0
    )

    out = monthly[[id_col, month_col]].copy()

    g = monthly.groupby(id_col, sort=False)

    for w in windows:
        out[f"delinq_30p_count_{w}m"] = (
            g["is_30p"].rolling(window=w, min_periods=1).sum()
            .reset_index(level=0, drop=True).astype(int)
        )
        out[f"max_dpd_{w}m"] = (
            g["dpd_num"].rolling(window=w, min_periods=1).max()
            .reset_index(level=0, drop=True).astype(int)
        )

    def _months_since_last_30p(group: pd.DataFrame) -> pd.Series:
        is_30p = group["is_30p"].to_numpy()
        since = []
        last = None
        for i, flag in enumerate(is_30p):
            if flag:
                last = i
                since.append(0)
            else:
                since.append(i - last if last is not None else 999)
        return pd.Series(since, index=group.index)

    try:
        out["months_since_last_30p"] = (
            g.apply(_months_since_last_30p, include_groups=False)
            .reset_index(level=0, drop=True).astype(int)
        )
    except TypeError:
        out["months_since_last_30p"] = (
            g.apply(_months_since_last_30p)
            .reset_index(level=0, drop=True).astype(int)
        )

    out["on_time_count_6m"] = (
        g["is_30p"].rolling(window=6, min_periods=1)
        .apply(lambda x: (~x.astype(bool)).sum())
        .reset_index(level=0, drop=True).astype(int)
    )

    lcfg = LabelConfig(
        horizon_months=horizon,
        id_col=id_col,
        month_col=month_col,
        dpd_col=dpd_col,
        status_col=status_col,
        delinquent_statuses=tuple(delinquent_statuses),
    )
    labels = build_label_next_nm(monthly, lcfg)
    out = out.merge(labels, on=[id_col, month_col], how="inner")

    if os.path.exists(STATIC_CSV):
        scfg = cfg["static"]
        s_id = scfg["id_col"]

        static = pd.read_csv(STATIC_CSV, low_memory=False)
        static.columns = static.columns.str.strip().str.replace("\ufeff", "")

        # rename to common key if needed
        if s_id != id_col:
            static = static.rename(columns={s_id: id_col})

        static[id_col] = static[id_col].astype(str)
        out[id_col] = out[id_col].astype(str)

        out = out.merge(static, on=id_col, how="left")

    out[month_col] = out[month_col].astype(str)

    os.makedirs(DATA_DIR, exist_ok=True)
    out.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"Wrote {OUTPUT_PARQUET} rows={len(out)} cols={len(out.columns)}")


if __name__ == "__main__":
    build_local_features()