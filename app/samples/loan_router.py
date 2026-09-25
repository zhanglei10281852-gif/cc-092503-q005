from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection, transaction
from app.samples.loan_schemas import (
    LoanRejectRequest,
    LoanRenewalCreate,
    LoanRequestCreate,
    LoanReturnRequest,
)
from app.samples.loans import LoanWorkflowService

router = APIRouter(prefix="/api/loans", tags=["借用工作流"])


@router.post("/requests", status_code=status.HTTP_201_CREATED)
def create_loan_request(payload: LoanRequestCreate, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).apply(principal, payload.model_dump())


@router.get("/requests")
def list_loan_requests(
    sample_id: int | None = Query(default=None),
    state: str | None = Query(default=None),
    mine: bool = Query(default=False),
    principal: Principal = Depends(current_principal),
):
    return LoanWorkflowService(get_connection()).list_requests(
        principal, sample_id=sample_id, state=state, mine=mine
    )


@router.get("/requests/{request_id}")
def get_loan_request(request_id: int, principal: Principal = Depends(current_principal)):
    return LoanWorkflowService(get_connection()).get_request(principal, request_id)


@router.post("/requests/{request_id}/approve")
def approve_loan_request(request_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).approve(principal, request_id)


@router.post("/requests/{request_id}/reject")
def reject_loan_request(request_id: int, payload: LoanRejectRequest, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).reject(principal, request_id, payload.reason)


@router.post("/requests/{request_id}/cancel")
def cancel_loan_request(request_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).cancel(principal, request_id)


@router.get("/samples/{sample_id}/reservation")
def sample_reservation(sample_id: int, principal: Principal = Depends(current_principal)):
    return LoanWorkflowService(get_connection()).reservation_status(principal, sample_id)


@router.post("/overdue/scan")
def scan_overdue_loans(principal: Principal = Depends(current_principal)):
    principal.require("loans.manage")
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).process_due_recalls(worker=principal.display_name)


@router.get("/recalls")
def list_recalls(
    loan_id: int | None = Query(default=None),
    resolved: bool | None = Query(default=None),
    principal: Principal = Depends(current_principal),
):
    return LoanWorkflowService(get_connection()).list_recalls(principal, loan_id=loan_id, resolved=resolved)


@router.get("")
def list_loans(
    sample_id: int | None = Query(default=None),
    state: str | None = Query(default=None),
    principal: Principal = Depends(current_principal),
):
    return LoanWorkflowService(get_connection()).list_loans(principal, sample_id=sample_id, state=state)


@router.post("/{loan_id}/renewals", status_code=status.HTTP_201_CREATED)
def request_renewal(loan_id: int, payload: LoanRenewalCreate, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).request_renewal(principal, loan_id, payload.model_dump())


@router.post("/{loan_id}/returns")
def return_loan(loan_id: int, payload: LoanReturnRequest, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return LoanWorkflowService(connection).return_loan(principal, loan_id, payload.model_dump())


@router.get("/{loan_id}")
def get_loan(loan_id: int, principal: Principal = Depends(current_principal)):
    return LoanWorkflowService(get_connection()).get_loan(principal, loan_id)
