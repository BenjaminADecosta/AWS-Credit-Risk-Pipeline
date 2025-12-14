from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass(frozen=True)
class LabelConfig:
    horizon_months: int
    # Column names in the monthly data
    id_col: str = "id"
    month_col: str = "as_of_month"
    dpd_col: str = "days_past_due"
    status_col: str = "status"

    delinquent_statuses: tuple[str, ...] = ("Late (31-120 days)", "Default", "Charged Off")


def _to_period_month(s: pd.Series) -> pd.PeriodIndex:
    return pd.PeriodIndex(s.astype(str), freq="M")


def _build_is_30p(df: pd.DataFrame, cfg: LabelConfig) -> pd.Series:
    if cfg.dpd_col in df.columns:
        dpd = pd.to_numeric(df[cfg.dpd_col], errors="coerce").fillna(0).astype(int)
        return dpd.ge(30)

    if cfg.status_col in df.columns:
        return df[cfg.status_col].astype(str).isin(set(cfg.delinquent_statuses))

    raise ValueError(
        f"Monthly data must contain either '{cfg.dpd_col}' or '{cfg.status_col}'. "
        f"Found columns: {list(df.columns)}"
    )


def build_label_next_nm(monthly: pd.DataFrame, cfg: LabelConfig) -> pd.DataFrame:
    if cfg.id_col not in monthly.columns:
        raise ValueError(f"Missing id_col '{cfg.id_col}' in monthly data. Columns: {list(monthly.columns)}")
    if cfg.month_col not in monthly.columns:
        raise ValueError(f"Missing month_col '{cfg.month_col}' in monthly data. Columns: {list(monthly.columns)}")

    df = monthly.copy()

    df[cfg.month_col] = _to_period_month(df[cfg.month_col])

    df["_is_30p"] = _build_is_30p(df, cfg)

    df = df.sort_values([cfg.id_col, cfg.month_col])

    g = df.groupby(cfg.id_col, sort=False)["_is_30p"]

    future_hits = pd.Series(False, index=df.index)
    for k in range(1, int(cfg.horizon_months) + 1):
        future_hits = future_hits | g.shift(-k).fillna(False)

    out = df[[cfg.id_col, cfg.month_col]].copy()
    out["label_next_nm"] = future_hits.astype(int)
    return out