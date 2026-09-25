from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api import audit, auth, roles, system, users
from app.core.clock import SystemClock, to_storage
from app.core.errors import DomainError, PermissionDeniedError
from app.database import close_connection, get_connection, init_db, transaction
from app.repositories.audit import AuditRepository
from app.services.auth import AuthService
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


def _audit_permission_denial(request: Request, exc: PermissionDeniedError) -> None:
    """将权限拒绝写入审计，便于通过 /api/audit 查询 outcome='denied'。"""
    authorization = request.headers.get("authorization", "")
    actor_user_id: int | None = None
    actor_name = "匿名"
    if authorization.startswith("Bearer "):
        token = authorization[7:].strip()
        try:
            principal = AuthService(get_connection()).principal(token)
        except Exception:
            return
        actor_user_id = principal.user_id
        actor_name = principal.display_name
    else:
        return
    try:
        with transaction(immediate=True) as connection:
            AuditRepository(connection).append(
                actor_user_id=actor_user_id,
                actor_name=actor_name,
                action=f"access.denied:{exc.code}",
                resource_type="api_endpoint",
                resource_id=request.url.path,
                outcome="denied",
                before=None,
                after=None,
                metadata={"method": request.method, "path": request.url.path, "message": exc.message},
                correlation_id=None,
                created_at=to_storage(SystemClock().now()),
            )
    except Exception:
        # 审计失败不应掩盖原始拒绝响应。
        pass


@app.exception_handler(DomainError)
async def handle_domain_error(request: Request, exc: DomainError) -> JSONResponse:
    if isinstance(exc, PermissionDeniedError):
        _audit_permission_denial(request, exc)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message, "context": exc.context}},
    )


app.include_router(auth.router)
app.include_router(users.router)
app.include_router(roles.router)
app.include_router(audit.router)
app.include_router(system.router)
app.include_router(samples_router)
app.include_router(sample_operations_router)
app.include_router(loan_workflow_router)


@app.get("/")
def root() -> dict:
    return {"service": "科研样品全生命周期管理服务", "version": "1.0.0"}
