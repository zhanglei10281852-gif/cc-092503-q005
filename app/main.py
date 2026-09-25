from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api import audit, auth, roles, system, users
from app.core.clock import to_storage, utc_now
from app.core.errors import DomainError, PermissionDeniedError
from app.database import close_connection, get_connection, init_db
from app.samples.router import router as samples_router
from app.samples.extended_router import router as sample_operations_router
from app.samples.loan_router import router as loan_workflow_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    del app
    init_db()
    yield
    close_connection()


app = FastAPI(title="科研样品全生命周期管理服务", version="1.0.0", lifespan=lifespan)


def _record_permission_denial(request: Request, exc: PermissionDeniedError) -> None:
    """Persist a 403 decision so permission refusals are queryable via the audit API."""
    import json

    try:
        from app.services.auth import AuthService

        header = request.headers.get("authorization", "")
        if not header.startswith("Bearer "):
            return
        principal = AuthService(get_connection()).principal(header[7:].strip())
        get_connection().execute(
            """INSERT INTO audit_events(
                   actor_user_id,actor_name,action,resource_type,resource_id,outcome,
                   before_json,after_json,metadata_json,correlation_id,created_at
               ) VALUES(?,?,?,?,?, 'denied',NULL,NULL,?,?,?)""",
            (
                principal.user_id,
                principal.display_name,
                "permission.denied",
                "api",
                request.url.path,
                json.dumps(
                    {"method": request.method, "path": request.url.path, "reason": exc.message},
                    ensure_ascii=False,
                ),
                None,
                to_storage(utc_now()),
            ),
        )
    except Exception:
        return


@app.exception_handler(DomainError)
async def handle_domain_error(request: Request, exc: DomainError) -> JSONResponse:
    if isinstance(exc, PermissionDeniedError):
        _record_permission_denial(request, exc)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message, "context": exc.context}},
    )


app.include_router(auth.router)
app.include_router(users.router)
app.include_router(roles.router)
app.include_router(audit.router)
app.include_router(system.router)
app.include_router(loan_workflow_router)
app.include_router(samples_router)
app.include_router(sample_operations_router)


@app.get("/")
def root() -> dict:
    return {"service": "科研样品全生命周期管理服务", "version": "1.0.0"}
