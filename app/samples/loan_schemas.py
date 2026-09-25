from __future__ import annotations

from pydantic import BaseModel, Field


class LoanRequestCreate(BaseModel):
    sample_id: int = Field(gt=0)
    borrower_user_id: int | None = Field(default=None, gt=0)
    quantity: float = Field(gt=0)
    requested_due_at: str = Field(min_length=10, max_length=40)
    priority: int = Field(default=100, ge=0, le=1000)
    request_code: str | None = Field(default=None, max_length=64)


class LoanRenewalCreate(BaseModel):
    requested_due_at: str = Field(min_length=10, max_length=40)
    priority: int = Field(default=100, ge=0, le=1000)
    request_code: str | None = Field(default=None, max_length=64)


class LoanRejectRequest(BaseModel):
    reason: str = Field(min_length=2, max_length=500)


class LoanReturnRequest(BaseModel):
    quantity: float = Field(gt=0)
    note: str = Field(default="", max_length=500)
