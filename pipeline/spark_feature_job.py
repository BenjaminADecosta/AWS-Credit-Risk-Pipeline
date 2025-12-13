"""
EMR Spark job: build curated Parquet features from raw LendingClub accepted-loans CSV in S3.

Run as a Spark step. Example args:
  --input s3://BUCKET/raw/static/accepted_2007_to_2018Q4.csv.gz
  --output s3://BUCKET/curated/features/
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F

from feature_spec_v1 import (
    REQUIRED_COLUMNS, OPTIONAL_COLUMNS,
    DEFAULT_BAD, DEFAULT_GOOD, DROP_STATUSES
)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("credit-risk-feature-job-v1").getOrCreate()

    df = spark.read.option("header", True).csv(args.input)

    cols = df.columns
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise ValueError(f"Missing required columns in input: {missing}")

    keep_cols = [c for c in REQUIRED_COLUMNS + OPTIONAL_COLUMNS if c in cols]
    df = df.select(*keep_cols)

    bad_set = F.array([F.lit(x) for x in DEFAULT_BAD])
    good_set = F.array([F.lit(x) for x in DEFAULT_GOOD])
    drop_set = F.array([F.lit(x) for x in DROP_STATUSES])

    df = df.withColumn(
        "label_default",
        F.when(F.col("loan_status").isin([x for x in DROP_STATUSES]), F.lit(None))
         .when(F.col("loan_status").isin([x for x in DEFAULT_GOOD]), F.lit(0))
         .when(F.col("loan_status").isin([x for x in DEFAULT_BAD]), F.lit(1))
         .otherwise(F.lit(None))
    )

    df = df.na.drop(subset=["label_default"])

    df = df.withColumn("term_months", F.regexp_extract(F.col("term"), r"(\d+)", 1).cast("int"))

    df = df.withColumn(
        "interest_rate",
        F.when(F.col("int_rate").endswith("%"),
               (F.regexp_replace(F.col("int_rate"), "%", "").cast("double") / F.lit(100.0)))
         .otherwise(
               F.when(F.col("int_rate").cast("double").isNotNull(),
                      F.when(F.col("int_rate").cast("double") > 1.5,
                             F.col("int_rate").cast("double") / F.lit(100.0))
                       .otherwise(F.col("int_rate").cast("double"))
               )
         )
    )

    df = df.withColumn("fico_low", F.col("fico_range_low").cast("int"))

    df = df.withColumn(
        "emp_length_years",
        F.when(F.lower(F.col("emp_length")).startswith("10+"), F.lit(10))
         .when(F.lower(F.col("emp_length")).contains("<"), F.lit(0))
         .otherwise(F.regexp_extract(F.col("emp_length"), r"(\d+)", 1).cast("int"))
    )

    numeric_cols = [
        "loan_amnt","installment","annual_inc","dti","delinq_2yrs","inq_last_6mths","open_acc",
        "total_acc","pub_rec","revol_bal","revol_util","collections_12_mths_ex_med",
        "acc_now_delinq","tot_cur_bal","tot_hi_cred_lim","bc_util","percent_bc_gt_75",
        "avg_cur_bal","total_rev_hi_lim","acc_open_past_24mths","num_rev_tl_bal_gt_0",
        "pct_tl_nvr_dlq","num_accts_ever_120_pd","pub_rec_bankruptcies","tax_liens"
    ]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    feature_cols = [
        "loan_amnt","term_months","interest_rate","installment",
        "annual_inc","dti","emp_length_years","home_ownership","verification_status",
        "purpose","application_type","initial_list_status",
        "fico_low","delinq_2yrs","inq_last_6mths","open_acc","total_acc","pub_rec",
        "revol_bal","revol_util","collections_12_mths_ex_med",
        "acc_now_delinq","tot_cur_bal","tot_hi_cred_lim","bc_util","percent_bc_gt_75",
        "avg_cur_bal","total_rev_hi_lim","acc_open_past_24mths","num_rev_tl_bal_gt_0",
        "pct_tl_nvr_dlq","num_accts_ever_120_pd","pub_rec_bankruptcies","tax_liens",
        "grade","sub_grade",
    ]
    final_cols = [c for c in feature_cols if c in df.columns] + ["label_default"]

    out = df.select(*final_cols)

    out.write.mode("overwrite").parquet(args.output)
    spark.stop()


if __name__ == "__main__":
    main()