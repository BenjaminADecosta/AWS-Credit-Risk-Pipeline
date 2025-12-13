"""
Quick dataset sanity checks.
Use locally on the produced Parquet before training.
"""

from __future__ import annotations
import pandas as pd

def check_parquet(path: str) -> None:
    df = pd.read_parquet(path)
    assert "label_default" in df.columns, "Missing label_default"
    assert df["label_default"].isna().sum() == 0, "Found null labels"
    bad = set(df["label_default"].unique()) - {0, 1}
    assert not bad, f"Non-binary labels found: {bad}"
    rate = df["label_default"].mean()
    print(f"Loaded {len(df)} rows. Default-rate={rate:.3f}")


if __name__ == "__main__":
    check_parquet("data/sample_features.parquet")