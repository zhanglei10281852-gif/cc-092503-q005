from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import current_principal
from app.core.errors import DomainError
from app.core.security import Principal
from app.database import get_connection, transaction
from app.samples.loans import dispatch_loan_job
from app.services.jobs import JobService

router = APIRouter(prefix="/api/system", tags=["系统运维"])


@router.get("/health")
def health() -> dict:
    connection = get_connection()
    foreign_keys = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
    journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
    return {"status": "ok", "foreign_keys": foreign_keys, "journal_mode": journal_mode}


@router.post("/jobs/example", status_code=201)
def enqueue_example(principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.run")
    with transaction(immediate=True) as connection:
        return JobService(connection).enqueue("system.example", f"example:{principal.user_id}", {"actor": principal.user_id})


@router.get("/jobs")
def list_jobs(
    job_type: str | None = None,
    status: str | None = None,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("jobs.run")
    connection = get_connection()
    clauses: list[str] = []
    params: list = []
    if job_type:
        clauses.append("job_type=?")
        params.append(job_type)
    if status:
        clauses.append("status=?")
        params.append(status)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    total = int(connection.execute("SELECT COUNT(*) FROM background_jobs" + where, tuple(params)).fetchone()[0])
    params.extend([size, (page - 1) * size])
    rows = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM background_jobs" + where + " ORDER BY id DESC LIMIT ? OFFSET ?",
            tuple(params),
        ).fetchall()
    ]
    return {"total": total, "page": page, "size": size, "pages": (total + size - 1) // size, "data": rows}


@router.post("/jobs/loan-overdue-sweep", status_code=201)
def enqueue_loan_overdue_sweep(
    sample_id: int | None = None,
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("jobs.run")
    with transaction(immediate=True) as connection:
        payload = {"sample_id": sample_id}
        scope = f"sweep:{sample_id if sample_id is not None else 'all'}"
        return JobService(connection).enqueue(
            "loan.overdue_sweep", f"loan-overdue:{scope}", payload, active_dedup=True
        )


@router.post("/jobs/run-next")
def run_next_job(
    worker: str = Query(default="api-runner", min_length=1, max_length=64),
    principal: Principal = Depends(current_principal),
) -> dict:
    """Claim one due job, execute it, and persist its result. Safe to re-run."""
    principal.require("jobs.run")
    with transaction(immediate=True) as connection:
        service = JobService(connection)
        job = service.claim(worker)
        if job is None:
            return {"claimed": False}
        try:
            result = dispatch_loan_job(connection, job)
        except DomainError as exc:
            # business-level failure is permanent; mark failed so the job does not spin
            service.fail(job["id"], worker, f"{exc.code}: {exc.message}")
            return {"claimed": True, "job_id": job["id"], "status": "failed", "error": exc.message}
        except Exception as exc:
            # transient failure: requeue for a retry, but the task itself stays re-runnable
            service.fail(job["id"], worker, str(exc), retry_seconds=60)
            return {"claimed": True, "job_id": job["id"], "status": "failed", "error": str(exc)}
        completed = service.complete(job["id"], worker, {"summary": result})
        return {
            "claimed": True,
            "job_id": job["id"],
            "job_type": job["job_type"],
            "status": completed["status"],
            "result": result,
        }
