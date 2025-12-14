from __future__ import annotations
import pandas as pd

def check_parquet(path: str, horizon: int = 3) -> None:
    df = pd.read_parquet(path)

    req = ["loan_id", "as_of_month", f"label_next_{horizon}m"]
    for c in req:
        assert c in df.columns, f"Missing required column: {c}"

    y = df[f"label_next_{horizon}m"]
    assert y.isna().sum() == 0, "Found null labels"
    bad = set(y.unique()) - {0, 1}
    assert not bad, f"Non-binary labels found: {bad}"

    for w in [3, 6, 12]:
        for c in [f"delinq_30p_count_{w}m", f"max_dpd_{w}m"]:
            if c in df.columns:
                assert df[c].ge(0).all(), f"{c} has negatives"

    print(f"OK: rows={len(df)} label_rate={y.mean():.4f} cols={len(df.columns)}")

if __name__ == "__main__":
    check_parquet("data/sample_features.parquet", horizon=3)