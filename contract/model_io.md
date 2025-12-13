# Model IO Contract (V1)

This document defines:
- What the model predicts
- How training labels are constructed from the dataset
- Which inputs are allowed (and which are forbidden to avoid leakage)
- How `pd` is converted into `risk_band`

## Prediction target

`pd` = predicted probability that a loan will end in **default-like outcome**.

V1 is an **origination / bureau snapshot** model: inputs are known at or before issuance.

## Label construction (from `loan_status`)

Create a binary label:

- `label_default = 1` if `loan_status` is one of:
  - `Charged Off`
  - `Default`
  - `Late (31-120 days)`  *(optional — include if you want a stricter “bad” definition)*

- `label_default = 0` if:
  - `Fully Paid`

Drop rows where outcome is unknown or ambiguous (recommended for V1):
- `Current`
- `Issued`
- `In Grace Period`
- `Late (16-30 days)`  *(optional — many teams drop this to reduce label noise)*

Notes:
- Keep the exact string matches you observe in your dataset (some versions differ).
- Document any deviations you choose.

## Allowed inputs (V1)

The request payload must match the Pydantic schema:
- `contract/schema.py :: LoanApplicationV1`

In general, V1 allows:
- loan terms (amount, term, interest rate, installment)
- borrower capacity (income, dti, employment length)
- credit bureau snapshot / aggregates (fico, utilization, delinquencies, inquiries, etc.)
- purpose, home ownership, verification status, application type

## Forbidden inputs (data leakage)

Do NOT use any post-origination performance or outcome-related fields in training or inference.
Examples from LendingClub-style data include (non-exhaustive):
- `total_pymnt*`, `total_rec_*`, `recoveries`, `collection_recovery_fee`
- `out_prncp*`
- `last_pymnt_*`, `next_pymnt_*`
- `last_fico_*`, `last_credit_pull_d`
- hardship / settlement fields: `hardship_*`, `debt_settlement_*`, `settlement_*`

Rationale: these fields contain information that occurs after the loan has been issued and will cause the model to “cheat”.

## Missing values policy

V1 expectation:
- Optional fields may be missing in the request.
- Preprocessing must fill missing values deterministically (e.g., 0 for counts, median for continuous, or an explicit “unknown” category).

Record the chosen imputation rules in:
- `inference/preprocess.py`
- `pipelines/local_features.py`
- `pipelines/spark_feature_job.py`

## Output format

The model returns:
- `pd`: float in [0, 1]
- `risk_band`: one of `LOW`, `MEDIUM`, `HIGH`
- `model_version`: string (date or git hash)

## Risk band thresholds (start here; tune later)

Initial thresholds (editable after validation/calibration):
- `LOW` if `pd < 0.02`
- `MEDIUM` if `0.02 <= pd < 0.08`
- `HIGH` if `pd >= 0.08`

You may adjust these thresholds based on:
- desired precision/recall tradeoff
- operational constraints (e.g., investigation capacity)
- calibration results