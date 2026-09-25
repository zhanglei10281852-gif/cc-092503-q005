from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection, transaction
from app.samples.loans import LoanWorkflowService
from app.samples.schemas import (
    InventoryAdjust,
    LoanCreate,
    LoanRenewalCreate,
    LoanRenewalDecide,
    LoanRequestCreate,
    LoanRequestDecide,
    LoanReturn,
    QuarantineSet,
)

router = APIRouter(prefix="/api/samples", tags=["借用工作流"])


# ---------------------------------------------------------------------------
# loan requests (queue)
# ---------------------------------------------------------------------------
@router.post("/loan-requests", status_code=status.HTTP_201_CREATED)
def create_loan_request(payload: LoanRequestCreate, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).create_request(principal, payload.model_dump())


@router.get("/loan-requests")
def list_loan_requests(
    state: str | None = Query(default=None),
    sample_id: int | None = Query(default=None),
    mine: bool = Query(default=False),
    principal: Principal = Depends(current_principal),
):
    return LoanWorkflowService(get_connection()).list_requests(principal, state=state, sample_id=sample_id, mine=mine)


@router.get("/{sample_id}/loan-availability")
def loan_availability(sample_id: int, principal: Principal = Depends(current_principal)):
    return LoanWorkflowService(get_connection()).availability(principal, sample_id)


@router.get("/loan-requests/{request_id}")
def get_loan_request(request_id: int, principal: Principal = Depends(current_principal)):
    return LoanWorkflowService(get_connection()).get_request(principal, request_id)


@router.post("/loan-requests/{request_id}/approve")
def approve_loan_request(request_id: int, payload: LoanRequestDecide, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).approve_request(principal, request_id, payload.note)


@router.post("/loan-requests/{request_id}/reject")
def reject_loan_request(request_id: int, payload: LoanRequestDecide, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).reject_request(principal, request_id, payload.note)


@router.post("/loan-requests/{request_id}/cancel")
def cancel_loan_request(request_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).cancel_request(principal, request_id)


# ---------------------------------------------------------------------------
# loans
# ---------------------------------------------------------------------------
@router.post("/loans", status_code=status.HTTP_201_CREATED)
def create_loan(payload: LoanCreate, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).create_direct_loan(principal, payload.model_dump())


@router.get("/loans")
def list_loans(
    state: str | None = Query(default=None),
    sample_id: int | None = Query(default=None),
    principal: Principal = Depends(current_principal),
):
    return LoanWorkflowService(get_connection()).list_loans(principal, state=state, sample_id=sample_id)


@router.post("/loans/overdue-sweep")
def run_overdue_sweep(
    sample_id: int | None = Query(default=None),
    principal: Principal = Depends(current_principal),
):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).run_overdue_sweep(principal, sample_id=sample_id)


@router.get("/loans/{loan_id}")
def get_loan(loan_id: int, principal: Principal = Depends(current_principal)):
    return LoanWorkflowService(get_connection()).get_loan(principal, loan_id)


@router.post("/loans/{loan_id}/returns")
def return_loan(loan_id: int, payload: LoanReturn, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).return_loan(principal, loan_id, payload.model_dump())


# ---------------------------------------------------------------------------
# renewals
# ---------------------------------------------------------------------------
@router.post("/loans/{loan_id}/renewals", status_code=status.HTTP_201_CREATED)
def create_renewal(loan_id: int, payload: LoanRenewalCreate, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).create_renewal(principal, loan_id, payload.model_dump())


@router.get("/loan-renewals")
def list_renewals(
    state: str | None = Query(default=None),
    loan_id: int | None = Query(default=None),
    principal: Principal = Depends(current_principal),
):
    return LoanWorkflowService(get_connection()).list_renewals(principal, state=state, loan_id=loan_id)


@router.post("/loan-renewals/{renewal_id}/approve")
def approve_renewal(renewal_id: int, payload: LoanRenewalDecide, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).decide_renewal(principal, renewal_id, "approve", payload.note)


@router.post("/loan-renewals/{renewal_id}/reject")
def reject_renewal(renewal_id: int, payload: LoanRenewalDecide, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).decide_renewal(principal, renewal_id, "reject", payload.note)


# ---------------------------------------------------------------------------
# recalls
# ---------------------------------------------------------------------------
@router.get("/loan-recalls")
def list_recalls(
    state: str | None = Query(default=None),
    loan_id: int | None = Query(default=None),
    principal: Principal = Depends(current_principal),
):
    return LoanWorkflowService(get_connection()).list_recalls(principal, state=state, loan_id=loan_id)


# ---------------------------------------------------------------------------
# quarantine / quantity re-validation triggers
# ---------------------------------------------------------------------------
@router.post("/{sample_id}/quarantine")
def quarantine_sample(sample_id: int, payload: QuarantineSet, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).quarantine_sample(principal, sample_id, payload.reason)


@router.post("/{sample_id}/quarantine/release")
def release_quarantine(sample_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).release_quarantine(principal, sample_id)


@router.post("/{sample_id}/inventory-adjustment")
def adjust_quantity(sample_id: int, payload: InventoryAdjust, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).adjust_quantity(principal, sample_id, payload.model_dump())
