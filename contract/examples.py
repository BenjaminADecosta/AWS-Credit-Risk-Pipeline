from __future__ import annotations

from contract.schema import LoanApplicationV1, RiskScoreResponseV1


# A realistic example request (values are plausible, not guaranteed)
SAMPLE_REQUEST_DICT_V1 = {
    "loan_amount": 12000,
    "term_months": 36,
    "interest_rate": 0.14,          # 14%
    "installment": 410.25,

    "grade": "C",
    "sub_grade": "C3",

    "annual_income": 85000,
    "dti": 0.28,
    "emp_length_years": 4,
    "home_ownership": "RENT",
    "verification_status": "Verified",
    "purpose": "debt_consolidation",
    "application_type": "Individual",

    "fico_low": 690,
    "delinq_2yrs": 0,
    "inq_last_6mths": 1,
    "open_acc": 7,
    "total_acc": 22,
    "pub_rec": 0,

    "revol_bal": 5000,
    "revol_util": 32.4,
    "collections_12_mths_ex_med": 0,

    # Optional richer bureau features
    "acc_now_delinq": 0,
    "tot_cur_bal": 24000,
    "tot_hi_cred_lim": 88000,
    "bc_util": 41.2,
    "percent_bc_gt_75": 0.0,
    "avg_cur_bal": 1090,
    "total_rev_hi_lim": 30000,
    "acc_open_past_24mths": 2,
    "num_rev_tl_bal_gt_0": 4,
    "pct_tl_nvr_dlq": 97.0,
    "num_accts_ever_120_pd": 0,
    "pub_rec_bankruptcies": 0,
    "tax_liens": 0
}


def sample_request_obj_v1() -> LoanApplicationV1:
    """Validated Pydantic object (raises if schema/ranges are violated)."""
    return LoanApplicationV1(**SAMPLE_REQUEST_DICT_V1)


# Example response (structure only; numbers are placeholders)
SAMPLE_RESPONSE_DICT_V1 = {
    "pd": 0.034,
    "risk_band": "MEDIUM",
    "model_version": "2025-12-13"
}


def sample_response_obj_v1() -> RiskScoreResponseV1:
    """Validated Pydantic response object."""
    return RiskScoreResponseV1(**SAMPLE_RESPONSE_DICT_V1)


if __name__ == "__main__":
    req = sample_request_obj_v1()
    resp = sample_response_obj_v1()
    print("Request validated:", req.model_dump())
    print("Response validated:", resp.model_dump())