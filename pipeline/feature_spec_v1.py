"""
V1 Feature spec for LendingClub accepted-loans static table.

Assumptions:
- One row per loan.
- Label comes from 'loan_status' (Fully Paid vs Charged Off/Default etc.)
- Only uses origination-time / bureau snapshot features (no post-payment leakage fields).
"""

# Column names vary by dataset. Adjust these constants once and keep the rest of the code unchanged.

REQUIRED_COLUMNS = [
    # label + minimal identifiers
    "loan_status",

    # loan terms
    "loan_amnt", "term", "int_rate", "installment",

    # borrower capacity
    "annual_inc", "dti", "emp_length", "home_ownership", "verification_status",

    # purpose/application
    "purpose", "application_type", "initial_list_status",

    # bureau snapshot
    "fico_range_low", "delinq_2yrs", "inq_last_6mths", "open_acc", "total_acc",
    "pub_rec", "revol_bal", "revol_util", "collections_12_mths_ex_med",
]

OPTIONAL_COLUMNS = [
    # richer bureau aggregates (safe at origination if present)
    "acc_now_delinq", "tot_cur_bal", "tot_hi_cred_lim", "bc_util",
    "percent_bc_gt_75", "avg_cur_bal", "total_rev_hi_lim", "acc_open_past_24mths",
    "num_rev_tl_bal_gt_0", "pct_tl_nvr_dlq", "num_accts_ever_120_pd",
    "pub_rec_bankruptcies", "tax_liens",

    # optional LC internal ratings (use if you want)
    "grade", "sub_grade",
]

# Map loan_status strings to label_default
DEFAULT_BAD = {"Charged Off", "Default", "Late (31-120 days)"}
DEFAULT_GOOD = {"Fully Paid"}

# Statuses to DROP (unknown / ambiguous outcome)
DROP_STATUSES = {"Current", "Issued", "In Grace Period", "Late (16-30 days)"}