"""
Build model-ready features locally from data/sample_static.csv (small dev sample).

Outputs:
- data/sample_features.parquet (features + label_default)
"""

from __future__ import annotations

import os
import json
import pandas as pd

from feature_spec_v1 import (
    REQUIRED_COLUMNS, OPTIONAL_COLUMNS,
    DEFAULT_BAD, DEFAULT_GOOD, DROP_STATUSES
)

INPUT_CSV = os.path.join("data", "sample_static.csv")
OUTPUT_PARQUET = os.path.join("data", "sample_features.parquet")


def _clean_term_to_months(term_val: str) -> int | None:
    if term_val is None:
        return None
    s = str(term_val).strip().lower()
    digits = "".join(ch for ch in s if ch.isdigit())
    return int(digits) if digits else None


def _clean_rate_to_decimal(rate_val: str) -> float | None:
    if rate_val is None:
        return None
    s = str(rate_val).strip()
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except ValueError:
            return None
    try:
        x = float(s)
        return x / 100.0 if x > 1.5 else x
    except ValueError:
        return None


def _parse_emp_length_years(emp: str) -> int | None:
    if emp is None:
        return None
    s = str(emp).strip().lower()
    if s in {"n/a", "na", ""}:
        return None
    if s.startswith("10+"):
        return 10
    if "<" in s:
        return 0
    digits = "".join(ch for ch in s if ch.isdigit())
    return int(digits) if digits else None


def build_local_features(input_csv: str = INPUT_CSV, output_parquet: str = OUTPUT_PARQUET) -> None:
    df = pd.read_csv(input_csv, low_memory=False)

    keep_cols = [c for c in REQUIRED_COLUMNS + OPTIONAL_COLUMNS if c in df.columns]
    missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_required:
        raise ValueError(f"Missing required columns in CSV: {missing_required}")

    df = df[keep_cols].copy()

    def label_from_status(status):
        if pd.isna(status):
            return None
        s = str(status).strip()
        if s in DROP_STATUSES:
            return None
        if s in DEFAULT_GOOD:
            return 0
        if s in DEFAULT_BAD:
            return 1
        return None

    df["label_default"] = df["loan_status"].apply(label_from_status)

    before = len(df)
    df = df.dropna(subset=["label_default"]).copy()
    after = len(df)

    df["term_months"] = df["term"].apply(_clean_term_to_months)

    df["interest_rate"] = df["int_rate"].apply(_clean_rate_to_decimal)

    if "emp_length" in df.columns:
        df["emp_length_years"] = df["emp_length"].apply(_parse_emp_length_years)

    if "fico_range_low" in df.columns:
        df["fico_low"] = pd.to_numeric(df["fico_range_low"], errors="coerce")

    for col in ["revol_util", "bc_util", "percent_bc_gt_75", "pct_tl_nvr_dlq"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    numeric_cols = [
        "loan_amnt", "installment", "annual_inc", "dti", "delinq_2yrs", "inq_last_6mths",
        "open_acc", "total_acc", "pub_rec", "revol_bal", "collections_12_mths_ex_med",
        "acc_now_delinq", "tot_cur_bal", "tot_hi_cred_lim", "avg_cur_bal",
        "total_rev_hi_lim", "acc_open_past_24mths", "num_rev_tl_bal_gt_0",
        "num_accts_ever_120_pd", "pub_rec_bankruptcies", "tax_liens"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    feature_cols = [
        "loan_amnt", "term_months", "interest_rate", "installment",
        "annual_inc", "dti", "emp_length_years", "home_ownership", "verification_status",
        "purpose", "application_type", "initial_list_status",
        "fico_low", "delinq_2yrs", "inq_last_6mths", "open_acc", "total_acc",
        "pub_rec", "revol_bal", "revol_util", "collections_12_mths_ex_med",
        # optional
        "acc_now_delinq", "tot_cur_bal", "tot_hi_cred_lim", "bc_util",
        "percent_bc_gt_75", "avg_cur_bal", "total_rev_hi_lim", "acc_open_past_24mths",
        "num_rev_tl_bal_gt_0", "pct_tl_nvr_dlq", "num_accts_ever_120_pd",
        "pub_rec_bankruptcies", "tax_liens",
        "grade", "sub_grade",
    ]

    final_cols = [c for c in feature_cols if c in df.columns] + ["label_default"]
    out = df[final_cols].copy()

    # Save parquet
    os.makedirs(os.path.dirname(output_parquet), exist_ok=True)
    out.to_parquet(output_parquet, index=False)

    # Quick report
    report = {
        "input_rows": before,
        "kept_rows": after,
        "output_path": output_parquet,
        "columns": list(out.columns),
        "label_rate": float(out["label_default"].mean()),
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    build_local_features()