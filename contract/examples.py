from __future__ import annotations

from contract.schema import AccountBehaviorV1, RiskScoreResponseV1

SAMPLE_REQUEST_DICT_V1 = {
    "loan_id": "LC_00012345",
    "as_of_month": "2016-07",
    "delinq_30p_count_3m": 0,
    "delinq_30p_count_6m": 1,
    "delinq_30p_count_12m": 1,
    "max_dpd_3m": 0,
    "max_dpd_6m": 35,
    "max_dpd_12m": 35,
    "months_since_last_30p": 2,
    "on_time_count_6m": 5,
    "loan_amount": 12000,
    "term_months": 36,
    "interest_rate": 0.14,
    "annual_income": 85000,
    "dti": 0.28,
    "fico_low": 690,
    "home_ownership": "RENT",
    "verification_status": "Verified",
    "purpose": "debt_consolidation",
    "application_type": "Individual",
}


def sample_request_obj_v1() -> AccountBehaviorV1:
    return AccountBehaviorV1(**SAMPLE_REQUEST_DICT_V1)


SAMPLE_RESPONSE_DICT_V1 = {
    "pd_next_3m": 0.062,
    "risk_band": "MEDIUM",
    "horizon_months": 3,
    "model_version": "v1-local",
}


def sample_response_obj_v1() -> RiskScoreResponseV1:
    return RiskScoreResponseV1(**SAMPLE_RESPONSE_DICT_V1)


if __name__ == "__main__":
    print("Request OK:", sample_request_obj_v1().model_dump())
    print("Response OK:", sample_response_obj_v1().model_dump())