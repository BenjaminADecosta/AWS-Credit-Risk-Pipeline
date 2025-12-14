"""
V1 Contract (AccountBehavior at as_of_month; NO future leakage)

The API scores an account based on a behavior summary as of a month.
The model predicts delinquency in the next N months (configured in training/configs/model.yaml).
"""

from __future__ import annotations

from typing import Literal, Optional
from pydantic import BaseModel, Field, ConfigDict

RiskBand = Literal["LOW", "MEDIUM", "HIGH"]


class AccountBehaviorV1(BaseModel):
    """
    One scoring request for an account at a given month.

    Notes:
    - as_of_month uses YYYY-MM
    - Provide rolled-up behavior features (not raw history) to keep API simple and fast.
    - Optional origination/static attributes can be included if you have them.
    """

    model_config = ConfigDict(extra="forbid")

    # identifiers / time anchor
    loan_id: str = Field(..., min_length=1)
    as_of_month: Optional[str] = Field(
        None, pattern=r"^\d{4}-\d{2}$", description="YYYY-MM (optional in API; required in offline data)"
    )

    delinq_30p_count_3m: int = Field(..., ge=0)
    delinq_30p_count_6m: int = Field(..., ge=0)
    delinq_30p_count_12m: int = Field(..., ge=0)

    max_dpd_3m: int = Field(..., ge=0)
    max_dpd_6m: int = Field(..., ge=0)
    max_dpd_12m: int = Field(..., ge=0)

    months_since_last_30p: int = Field(..., ge=0, description="0 if current month is 30+ DPD")
    on_time_count_6m: int = Field(..., ge=0)

    loan_amount: Optional[float] = Field(None, gt=0)
    term_months: Optional[Literal[36, 60]] = None
    interest_rate: Optional[float] = Field(None, ge=0, le=1, description="decimal, e.g. 0.14")
    annual_income: Optional[float] = Field(None, gt=0)
    dti: Optional[float] = Field(None, ge=0, le=10)
    fico_low: Optional[int] = Field(None, ge=300, le=850)

    home_ownership: Optional[str] = None
    verification_status: Optional[str] = None
    purpose: Optional[str] = None
    application_type: Optional[str] = None


class RiskScoreResponseV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pd_next_3m: float = Field(..., ge=0, le=1)
    risk_band: RiskBand
    horizon_months: int = Field(..., ge=1, le=24)
    model_version: str = Field(..., min_length=1)