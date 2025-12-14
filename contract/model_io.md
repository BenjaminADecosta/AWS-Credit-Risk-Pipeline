# Model IO Contract (V1)

## Task

Predict probability that an account becomes **30+ days past due** at any point in the next **N months**.

- Output: `pd_next_{N}m`
- Risk band: derived from thresholds in `training/configs/model.yaml`

This is an **as_of_month** model: all features must be computable using information up to and including `as_of_month`.

## Label

For each (loan_id, as_of_month):

`label_next_{N}m = 1` if any month in (as_of_month+1 ... as_of_month+N) is delinquent (30+ DPD).  
Otherwise `label_next_{N}m = 0`.

We define delinquent (30+ DPD) as:
- `days_past_due >= 30` if you have a DPD column
- OR monthly status in a delinquent set (configurable), e.g. `"Late (31-120 days)"`, `"Default"`, `"Charged Off"`

## Allowed inputs

- Rolled-up / windowed behavior features (counts and maxima in trailing windows)
- Optional static/origination attributes joined by `loan_id`

## Forbidden inputs (leakage)

Never use any field that contains future knowledge relative to `as_of_month`, including:
- forward payment amounts, recoveries, future balance, future status, etc.
- any columns created using months > as_of_month during feature building

## Output

- `pd_next_3m`: float [0, 1]
- `risk_band`: LOW | MEDIUM | HIGH
- `horizon_months`: integer (e.g. 3)
- `model_version`: string