"""带库存预留的借用工作流。

核心模型：
- ``loan_requests``：借用/续借申请，按 (priority 降序, id 升序) 排队。
- ``loans``：获批后原子占用可借数量产生的实际占用记录。
- ``samples.reserved_quantity``：库存预留总量，由 ``loans`` 中未归还的数量聚合而来，
  所有变化写入 ``loan_reservation_ledger``。

设计约束：
- 所有写操作都在 ``BEGIN IMMEDIATE`` 事务中运行，借助单库串行化保证"检查-占用"原子性。
- 归还、隔离、销毁审批、数量变化后都会调用 ``_advance_queue`` 推进满足条件的候补。
- 续借是一种特殊的排队申请，获批且没有被高优先级等待者阻挡时才顺延到期时间。
"""

from __future__ import annotations

import sqlite3
import uuid
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, PermissionDeniedError, ValidationError
from app.core.security import Principal
from app.samples.repository import SampleRepository
from app.services.audit import AuditContext, AuditService
from app.services.jobs import JobService

BLOCKING_SAMPLE_STATES = {"quarantined", "pending_destruction", "destroyed", "consumed"}
# 仍占用库存、可以继续流转的借用状态。
ACTIVE_LOAN_STATES = ("active", "partially_returned", "overdue", "disputed")

RECALL_JOB_TYPE = "loan.recall"


def _row(row: sqlite3.Row | None, message: str) -> dict[str, Any]:
    if row is None:
        raise NotFoundError(message)
    return dict(row)


class LoanRequestRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def get(self, request_id: int) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM loan_requests WHERE id=?", (request_id,)
        ).fetchone()
        return _row(row, "借用申请不存在")

    def open_for_sample(self, sample_id: int) -> list[dict[str, Any]]:
        """按排队顺序（优先级降序、提交顺序升序）返回未结束申请。"""
        rows = self.connection.execute(
            "SELECT * FROM loan_requests WHERE sample_id=? AND state IN ('queued','approved') "
            "ORDER BY priority DESC, id",
            (sample_id,),
        ).fetchall()
        return [dict(row) for row in rows]


class LoanWorkflowService:
    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        self.samples = SampleRepository(connection)
        self.requests = LoanRequestRepository(connection)
        self.audit = AuditService(connection, self.clock)
        self.jobs = JobService(connection, self.clock)

    # ------------------------------------------------------------------ 查询

    def get_request(self, principal: Principal, request_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        request = self.requests.get(request_id)
        request["loan"] = self._loan_view(request["fulfilled_loan_id"]) if request["fulfilled_loan_id"] else None
        request["recalls"] = self._request_recalls(request)
        if not self._can_view(principal, request):
            raise PermissionDeniedError("不能查看其他课题组的借用申请")
        return request

    def list_requests(
        self,
        principal: Principal,
        *,
        sample_id: int | None = None,
        state: str | None = None,
        mine: bool = False,
    ) -> list[dict[str, Any]]:
        principal.require("samples.read")
        clauses: list[str] = []
        params: list[Any] = []
        if sample_id is not None:
            clauses.append("sample_id=?")
            params.append(sample_id)
        if state:
            clauses.append("state=?")
            params.append(state)
        if mine:
            clauses.append("applicant_user_id=?")
            params.append(principal.user_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(200)
        rows = self.connection.execute(
            f"SELECT * FROM loan_requests{where} ORDER BY priority DESC, id LIMIT ?",
            tuple(params),
        ).fetchall()
        items = [dict(row) for row in rows]
        if not (principal.can("loans.manage") or principal.can("*")):
            items = [item for item in items if self._can_view(principal, item)]
        return items

    def get_loan(self, principal: Principal, loan_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        loan = self._loan_view(loan_id)
        loan["requests"] = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM loan_requests WHERE parent_loan_id=? OR fulfilled_loan_id=? ORDER BY id",
                (loan_id, loan_id),
            ).fetchall()
        ]
        loan["recalls"] = [dict(row) for row in self.connection.execute(
            "SELECT * FROM loan_recalls WHERE loan_id=? ORDER BY id", (loan_id,)
        ).fetchall()]
        return loan

    def list_loans(self, principal: Principal, *, sample_id: int | None = None, state: str | None = None) -> list[dict[str, Any]]:
        principal.require("samples.read")
        clauses: list[str] = []
        params: list[Any] = []
        if sample_id is not None:
            clauses.append("l.sample_id=?")
            params.append(sample_id)
        if state:
            clauses.append("l.state=?")
            params.append(state)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(200)
        rows = self.connection.execute(
            "SELECT l.* FROM loans l" + where + " ORDER BY l.id DESC LIMIT ?",
            tuple(params),
        ).fetchall()
        return [dict(row) for row in rows]

    def reservation_status(self, principal: Principal, sample_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        sample = self.samples.get(sample_id)
        ledger = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM loan_reservation_ledger WHERE sample_id=? ORDER BY id DESC LIMIT 100",
                (sample_id,),
            ).fetchall()
        ]
        active_loans = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM loans WHERE sample_id=? AND state IN ('active','partially_returned','overdue','disputed') ORDER BY id",
                (sample_id,),
            ).fetchall()
        ]
        return {
            "sample_id": sample_id,
            "sample_code": sample["sample_code"],
            "quantity": sample["quantity"],
            "reserved_quantity": sample["reserved_quantity"],
            "available_quantity": round(sample["quantity"] - sample["reserved_quantity"], 9),
            "lifecycle_state": sample["lifecycle_state"],
            "queue": [r for r in self.requests.open_for_sample(sample_id) if not r["fulfilled_loan_id"]],
            "active_loans": active_loans,
            "ledger": ledger,
        }

    def list_recalls(self, principal: Principal, *, loan_id: int | None = None, resolved: bool | None = None) -> list[dict[str, Any]]:
        principal.require("samples.read")
        clauses: list[str] = []
        params: list[Any] = []
        if loan_id is not None:
            clauses.append("r.loan_id=?")
            params.append(loan_id)
        if resolved is True:
            clauses.append("r.resolved_at IS NOT NULL")
        elif resolved is False:
            clauses.append("r.resolved_at IS NULL")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(200)
        rows = self.connection.execute(
            """SELECT r.*,l.loan_code,l.sample_id,l.borrower_user_id
               FROM loan_recalls r JOIN loans l ON l.id=r.loan_id"""
            + where + " ORDER BY r.id DESC LIMIT ?",
            tuple(params),
        ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------ 申请

    def apply(self, principal: Principal, data: dict[str, Any]) -> dict[str, Any]:
        if not (principal.can("loans.apply") or principal.can("loans.manage")):
            raise PermissionDeniedError("缺少权限：loans.apply")
        sample = self.samples.get(data["sample_id"])
        borrower_id = data.get("borrower_user_id") or principal.user_id
        if borrower_id != principal.user_id and not principal.can("loans.manage"):
            raise PermissionDeniedError("只能为自己提交借用申请")
        self._require_user(borrower_id)
        if sample["lifecycle_state"] in {"destroyed", "consumed"}:
            raise ConflictError("样品已不存在，无法借用")
        if data["quantity"] > sample["quantity"]:
            raise ValidationError("申请数量超过样品总数量")
        if sample["unit"] and data["quantity"] <= 0:
            raise ValidationError("借用数量必须大于零")
        now = to_storage(self.clock.now())
        code = data.get("request_code") or f"LREQ-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO loan_requests(
                   request_code,sample_id,applicant_user_id,borrower_user_id,kind,parent_loan_id,
                   priority,quantity,requested_due_at,state,created_at,updated_at
               ) VALUES(?,?,?,?,'borrow',NULL,?,?,?,'queued',?,?)""",
            (
                code, data["sample_id"], principal.user_id, borrower_id,
                int(data.get("priority", 100)), data["quantity"], data["requested_due_at"], now, now,
            ),
        )
        request = self.requests.get(cursor.lastrowid)
        self.samples.append_event(
            sample["id"], "loan.requested", principal.user_id, now,
            details={"request_id": request["id"], "quantity": data["quantity"], "priority": request["priority"]},
        )
        self.audit.record(
            AuditContext(principal.user_id, principal.display_name),
            action="loan.apply", resource_type="loan_request", resource_id=request["id"],
            after=request, metadata={"sample_id": sample["id"], "quantity": data["quantity"]},
        )
        # 申请仅排队；是否占用库存取决于审批与可借数量，不在提交时直接占用。
        return self.requests.get(request["id"])

    def request_renewal(self, principal: Principal, loan_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("loans.apply")
        loan = self._loan_view(loan_id)
        if loan["state"] not in ACTIVE_LOAN_STATES:
            raise ConflictError("借用已结束，不能续借")
        outstanding = round(loan["quantity"] - loan["returned_quantity"], 9)
        if outstanding <= 0:
            raise ConflictError("借用已全部归还，无需续借")
        if loan["borrower_user_id"] != principal.user_id and not principal.can("loans.manage"):
            raise PermissionDeniedError("只能为自己的借用申请续借")
        new_due = data["requested_due_at"]
        if from_storage(new_due) <= from_storage(loan["due_at"]):
            raise ValidationError("续借到期时间必须晚于当前到期时间")
        # 同一笔借用已有待审批续借时，避免重复排队。
        duplicate = self.connection.execute(
            "SELECT id FROM loan_requests WHERE parent_loan_id=? AND kind='renewal' AND state IN ('queued','approved')",
            (loan_id,),
        ).fetchone()
        if duplicate:
            raise ConflictError("该借用已有待处理的续借申请")
        now = to_storage(self.clock.now())
        code = data.get("request_code") or f"LREN-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO loan_requests(
                   request_code,sample_id,applicant_user_id,borrower_user_id,kind,parent_loan_id,
                   priority,quantity,requested_due_at,state,created_at,updated_at
               ) VALUES(?,?,?,?,'renewal',?,?,?,?,'queued',?,?)""",
            (
                code, loan["sample_id"], principal.user_id, loan["borrower_user_id"], loan_id,
                int(data.get("priority", 100)), outstanding, new_due, now, now,
            ),
        )
        request = self.requests.get(cursor.lastrowid)
        self.audit.record(
            AuditContext(principal.user_id, principal.display_name),
            action="loan.renewal.apply", resource_type="loan_request", resource_id=request["id"],
            after=request, metadata={"loan_id": loan_id, "requested_due_at": new_due},
        )
        return request

    # ------------------------------------------------------------------ 审批

    def approve(self, principal: Principal, request_id: int) -> dict[str, Any]:
        principal.require("loans.manage")
        request = self.requests.get(request_id)
        if request["state"] != "queued":
            raise ConflictError("申请当前状态不能审批")
        sample = self.samples.get(request["sample_id"])
        self._ensure_sample_loanable(request, sample)
        if request["kind"] == "renewal":
            self._ensure_renewal_unblocked(request, sample)
        now = to_storage(self.clock.now())
        self.connection.execute(
            "UPDATE loan_requests SET state='approved',decided_by=?,decided_at=?,version=version+1,updated_at=? WHERE id=?",
            (principal.user_id, now, now, request_id),
        )
        request = self.requests.get(request_id)
        self.audit.record(
            AuditContext(principal.user_id, principal.display_name),
            action="loan.approve", resource_type="loan_request", resource_id=request_id,
            after=request,
        )
        # 批准后立即尝试原子占用（续借则顺延到期）；可借不足时保留为已批准候补，等待释放后推进。
        self._advance_queue(sample["id"], actor=principal)
        return self.requests.get(request_id)

    def reject(self, principal: Principal, request_id: int, reason: str) -> dict[str, Any]:
        principal.require("loans.manage")
        request = self.requests.get(request_id)
        if request["state"] not in ("queued", "approved"):
            raise ConflictError("申请当前状态不能驳回")
        now = to_storage(self.clock.now())
        before = dict(request)
        self.connection.execute(
            "UPDATE loan_requests SET state='rejected',reject_reason=?,decided_by=?,decided_at=?,version=version+1,updated_at=? WHERE id=?",
            (reason, principal.user_id, now, now, request_id),
        )
        # 已批准但尚未占用的申请被驳回，无需释放库存（尚未占用）。
        after = self.requests.get(request_id)
        self.audit.record(
            AuditContext(principal.user_id, principal.display_name),
            action="loan.reject", resource_type="loan_request", resource_id=request_id,
            before=before, after=after, metadata={"reason": reason},
        )
        self._advance_queue(request["sample_id"], actor=principal)
        return after

    def cancel(self, principal: Principal, request_id: int) -> dict[str, Any]:
        principal.require("loans.apply")
        request = self.requests.get(request_id)
        if request["applicant_user_id"] != principal.user_id and not principal.can("loans.manage"):
            raise PermissionDeniedError("只能取消自己提交的申请")
        if request["state"] not in ("queued", "approved"):
            raise ConflictError("申请当前状态不能取消")
        before = dict(request)
        now = to_storage(self.clock.now())
        self.connection.execute(
            "UPDATE loan_requests SET state='cancelled',version=version+1,updated_at=? WHERE id=?",
            (now, request_id),
        )
        after = self.requests.get(request_id)
        self.audit.record(
            AuditContext(principal.user_id, principal.display_name),
            action="loan.cancel", resource_type="loan_request", resource_id=request_id,
            before=before, after=after,
        )
        self._advance_queue(request["sample_id"], actor=principal)
        return after

    # ------------------------------------------------------------------ 归还 / 占用

    def return_loan(self, principal: Principal, loan_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("loans.manage")
        loan = self._loan_view(loan_id)
        if loan["state"] not in ACTIVE_LOAN_STATES:
            raise ConflictError("借用记录已经结束")
        remaining = round(loan["quantity"] - loan["returned_quantity"], 9)
        quantity = float(data["quantity"])
        if quantity <= 0:
            raise ValidationError("归还数量必须大于零")
        if quantity > remaining + 1e-9:
            raise ValidationError("归还数量超过未归还数量")
        quantity = min(quantity, remaining)
        now = to_storage(self.clock.now())
        returned = round(loan["returned_quantity"] + quantity, 9)
        state = "returned" if abs(returned - loan["quantity"]) < 1e-9 else "partially_returned"
        before = dict(loan)
        self.connection.execute(
            "UPDATE loans SET returned_quantity=?,state=?,version=version+1,updated_at=? WHERE id=?",
            (returned, state, now, loan_id),
        )
        self._release_reservation(
            loan["sample_id"], loan_id, quantity, now, principal.user_id,
            reason=f"return:{state}", note=data.get("note", ""),
        )
        self._resolve_recalls(loan_id, now, resolution=f"returned:{state}")
        self.samples.append_event(
            loan["sample_id"], "returned", principal.user_id, now,
            details={"loan_id": loan_id, "returned_quantity": quantity, "state": state},
        )
        result = self._loan_view(loan_id)
        self.audit.record(
            AuditContext(principal.user_id, principal.display_name),
            action="loan.return", resource_type="loan", resource_id=loan_id,
            before=before, after=result, metadata={"returned_quantity": quantity},
        )
        # 归还释放库存后，自动推进满足条件的候补。
        self._advance_queue(loan["sample_id"], actor=principal)
        return result

    # ------------------------------------------------------------------ 队列推进

    def _advance_queue(self, sample_id: int, *, actor: Principal | AuditContext | None = None) -> list[dict[str, Any]]:
        """按排队顺序原子满足所有当前可满足的已批准申请。返回新满足的申请。

        排队位次由 (priority 降序, id 升序) 决定：
        - 借用申请：只有已批准（state='approved'）且尚未占用的候选可被满足；可借数量
          不足时立即停止，保证排在后面的申请不能越过队首候选占用释放出的库存。
          尚未审批（state='queued'）的申请不占用库存，也不阻挡已获批的后位候选。
        - 续借申请：仅当其前方没有任何等待中的借用申请（含待审批与已批准候补）时才顺延。
        """
        now = to_storage(self.clock.now())
        progressed: list[dict[str, Any]] = []
        # 反复扫描，处理续借顺延改变前方条件后借用可继续满足的交错情况。
        while True:
            changed = False
            sample = self.samples.get(sample_id)
            queue = self.requests.open_for_sample(sample_id)
            for request in queue:
                if request["fulfilled_loan_id"]:
                    continue
                if sample["lifecycle_state"] in BLOCKING_SAMPLE_STATES:
                    # 隔离 / 销毁审批期间冻结一切借用与续借推进。
                    break
                if request["kind"] == "renewal":
                    if self._renewal_blocked_by_waiting(request, queue):
                        continue
                    if self._fulfill_renewal(request, sample, now, actor):
                        progressed.append(request)
                        changed = True
                    continue
                # 借用申请
                if request["state"] != "approved":
                    # 尚未审批：不参与占用，也不阻挡已获批的后位候选。
                    continue
                if self._available(self.samples.get(sample_id)) + 1e-9 < request["quantity"]:
                    break  # 队首已批准候选数量不足，后面的候选不得越过
                self._fulfill_borrow(request, now, actor)
                progressed.append(request)
                changed = True
            if not changed:
                break
        return progressed

    def _fulfill_borrow(self, request: dict[str, Any], now: str, actor: Principal | AuditContext | None) -> dict[str, Any]:
        sample = self.samples.get(request["sample_id"])
        loan_code = f"LOAN-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO loans(
                   loan_code,sample_id,borrower_user_id,request_id,quantity,due_at,
                   original_due_at,renewals_count,returned_quantity,state,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,0,0,'active',?,?)""",
            (
                loan_code, sample["id"], request["borrower_user_id"], request["id"],
                request["quantity"], request["requested_due_at"], request["requested_due_at"], now, now,
            ),
        )
        loan_id = int(cursor.lastrowid)
        self._hold_reservation(
            sample["id"], loan_id, request["id"], request["quantity"], now,
            actor_user_id=getattr(actor, "user_id", None), reason="borrow.fulfill",
        )
        self.connection.execute(
            "UPDATE loan_requests SET fulfilled_loan_id=?,fulfilled_at=?,version=version+1,updated_at=? WHERE id=?",
            (loan_id, now, now, request["id"]),
        )
        # 有未归还占用时样品处于 loaned。
        self._refresh_sample_state(sample["id"], now)
        self.samples.append_event(
            sample["id"], "loan.fulfilled", getattr(actor, "user_id", None), now,
            details={"request_id": request["id"], "loan_id": loan_id, "quantity": request["quantity"]},
        )
        self.audit.record(
            self._actor_context(actor),
            action="loan.fulfill", resource_type="loan", resource_id=loan_id,
            after={"loan_id": loan_id, "request_id": request["id"]},
            metadata={"sample_id": sample["id"], "quantity": request["quantity"]},
        )
        return self._loan_view(loan_id)

    def _fulfill_renewal(self, request: dict[str, Any], sample: dict[str, Any], now: str, actor: Principal | AuditContext | None) -> bool:
        loan = self._loan_view(request["parent_loan_id"])
        if loan["state"] not in ACTIVE_LOAN_STATES:
            # 借用已结束，续借自动失效。
            self._invalidate(request, "关联借用已结束", now)
            return True
        before = dict(loan)
        new_state = loan["state"]
        if loan["state"] == "overdue":
            new_state = "partially_returned" if float(loan["returned_quantity"]) > 0 else "active"
        self.connection.execute(
            "UPDATE loans SET due_at=?,state=?,renewals_count=renewals_count+1,version=version+1,updated_at=? WHERE id=?",
            (request["requested_due_at"], new_state, now, loan["id"]),
        )
        self.connection.execute(
            "UPDATE loan_requests SET fulfilled_loan_id=?,fulfilled_at=?,version=version+1,updated_at=? WHERE id=?",
            (loan["id"], now, now, request["id"]),
        )
        self._resolve_recalls(loan["id"], now, resolution="renewed")
        self.samples.append_event(
            sample["id"], "loan.renewed", getattr(actor, "user_id", None), now,
            details={"loan_id": loan["id"], "request_id": request["id"], "due_at": request["requested_due_at"]},
        )
        self.audit.record(
            self._actor_context(actor),
            action="loan.renewal.fulfill", resource_type="loan", resource_id=loan["id"],
            before=before, after=self._loan_view(loan["id"]),
            metadata={"request_id": request["id"], "due_at": request["requested_due_at"]},
        )
        return True

    # ------------------------------------------------------------------ 重校验

    def revalidate_sample(self, sample_id: int, *, reason: str, actor: Principal | AuditContext | None = None) -> dict[str, Any]:
        """样品隔离、进入销毁审批或数量变化后重新校验未执行申请。

        - 隔离 / 销毁审批 / 销毁 / 耗尽：未占用的开放申请全部失效，已批准占用的借用保留。
        - 数量变化：逐单核对，数量超过样品总量的申请失效；其余继续排队并尝试推进。
        """
        sample = self.samples.get(sample_id)
        now = to_storage(self.clock.now())
        open_requests = [r for r in self.requests.open_for_sample(sample_id) if not r["fulfilled_loan_id"]]
        invalidated: list[int] = []
        blocking = sample["lifecycle_state"] in BLOCKING_SAMPLE_STATES
        for request in open_requests:
            if blocking:
                self._invalidate(request, reason, now)
                invalidated.append(request["id"])
            elif request["kind"] == "borrow" and request["quantity"] > sample["quantity"] + 1e-9:
                self._invalidate(request, f"样品数量变化，申请数量超过当前总量（{reason}）", now)
                invalidated.append(request["id"])
        if invalidated:
            self.samples.append_event(
                sample_id, "loan.requests.invalidated", getattr(actor, "user_id", None), now,
                details={"request_ids": invalidated, "reason": reason},
            )
            self.audit.record(
                self._actor_context(actor),
                action="loan.revalidate", resource_type="sample", resource_id=sample_id,
                after={"invalidated_request_ids": invalidated, "lifecycle_state": sample["lifecycle_state"]},
                metadata={"reason": reason},
            )
        # 数量恢复/增加等情况下尝试继续推进。
        self._advance_queue(sample_id, actor=actor)
        return {
            "sample_id": sample_id,
            "lifecycle_state": sample["lifecycle_state"],
            "invalidated_request_ids": invalidated,
            "queue": self.requests.open_for_sample(sample_id),
        }

    def revalidate_after_quantity_change(self, sample_id: int, *, actor: Principal | AuditContext | None = None) -> dict[str, Any]:
        return self.revalidate_sample(sample_id, reason="quantity_changed", actor=actor)

    # ------------------------------------------------------------------ 逾期召回

    def process_due_recalls(self, *, worker: str = "scheduler", limit: int = 100) -> dict[str, Any]:
        """扫描到期未还的借用，为每个逾期周期至多生成一条召回记录。

        任务可安全重跑：同一 (loan, episode_due_at) 已存在召回记录时不重复生成。
        """
        now_dt = self.clock.now()
        now = to_storage(now_dt)
        rows = self.connection.execute(
            """SELECT * FROM loans
               WHERE state IN ('active','partially_returned','overdue','disputed')
                 AND due_at < ? AND (quantity-returned_quantity) > 0
               ORDER BY due_at, id LIMIT ?""",
            (now, limit),
        ).fetchall()
        recalls: list[dict[str, Any]] = []
        for row in rows:
            loan = dict(row)
            episode = loan["due_at"]
            existing = self.connection.execute(
                "SELECT id FROM loan_recalls WHERE loan_id=? AND episode_due_at=?",
                (loan["id"], episode),
            ).fetchone()
            if existing:
                continue
            job = self.jobs.enqueue(
                RECALL_JOB_TYPE,
                f"{RECALL_JOB_TYPE}:loan:{loan['id']}:episode:{episode}",
                {"loan_id": loan["id"], "episode_due_at": episode},
            )
            outstanding = round(loan["quantity"] - loan["returned_quantity"], 9)
            cursor = self.connection.execute(
                """INSERT INTO loan_recalls(
                       loan_id,episode_due_at,reason,outstanding_quantity,triggered_by_user_id,job_id,created_at
                   ) VALUES(?,?,?,?,NULL,?,?)""",
                (loan["id"], episode, "overdue", outstanding, job["id"], now),
            )
            if loan["state"] != "overdue":
                self.connection.execute(
                    "UPDATE loans SET state='overdue',version=version+1,updated_at=? WHERE id=?",
                    (now, loan["id"]),
                )
            recall = dict(self.connection.execute(
                "SELECT * FROM loan_recalls WHERE id=?", (cursor.lastrowid,)
            ).fetchone())
            self.samples.append_event(
                loan["sample_id"], "loan.recalled", None, now,
                details={"loan_id": loan["id"], "recall_id": recall["id"], "outstanding_quantity": outstanding},
            )
            self.audit.record(
                AuditContext(None, worker),
                action="loan.recall", resource_type="loan_recall", resource_id=recall["id"],
                after=recall, metadata={"loan_id": loan["id"]},
            )
            recalls.append(recall)
        return {"generated": len(recalls), "recalls": recalls, "checked_at": now}

    # ------------------------------------------------------------------ 内部工具

    def _hold_reservation(self, sample_id, loan_id, request_id, quantity, now, *, actor_user_id, reason) -> None:
        sample = self.samples.get(sample_id)
        if self._available(sample) + 1e-9 < quantity:
            # 理论上不应发生（调用前已校验），兜底保护 CHECK 约束。
            raise ConflictError("可借数量不足，占用失败")
        new_reserved = round(sample["reserved_quantity"] + quantity, 9)
        cursor = self.connection.execute(
            "UPDATE samples SET reserved_quantity=?,version=version+1,updated_at=? WHERE id=?",
            (new_reserved, now, sample_id),
        )
        if cursor.rowcount != 1:
            raise ConflictError("库存预留更新失败")
        self._ledger(sample_id, loan_id, request_id, +quantity, sample["quantity"], new_reserved, reason, actor_user_id, now)

    def _release_reservation(self, sample_id, loan_id, quantity, now, actor_user_id, *, reason, note="") -> None:
        sample = self.samples.get(sample_id)
        new_reserved = round(sample["reserved_quantity"] - quantity, 9)
        if new_reserved < -1e-9:
            raise ConflictError("释放数量超过已预留数量")
        new_reserved = max(new_reserved, 0.0)
        self.connection.execute(
            "UPDATE samples SET reserved_quantity=?,version=version+1,updated_at=? WHERE id=?",
            (new_reserved, now, sample_id),
        )
        self._ledger(sample_id, loan_id, None, -quantity, sample["quantity"], new_reserved, reason, actor_user_id, now, note=note)
        self._refresh_sample_state(sample_id, now)

    def _ledger(self, sample_id, loan_id, request_id, change, quantity, reserved_after, reason, actor_user_id, now, *, note="") -> None:
        self.connection.execute(
            """INSERT INTO loan_reservation_ledger(
                   sample_id,loan_id,loan_request_id,change,quantity,reserved_after,reason,actor_user_id,correlation_id,created_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (sample_id, loan_id, request_id, change, quantity, reserved_after, reason + (f":{note}" if note else ""), actor_user_id, None, now),
        )

    def _refresh_sample_state(self, sample_id: int, now: str) -> None:
        sample = self.samples.get(sample_id)
        if sample["lifecycle_state"] in BLOCKING_SAMPLE_STATES:
            return
        held = self.connection.execute(
            "SELECT COALESCE(SUM(quantity-returned_quantity),0) FROM loans "
            "WHERE sample_id=? AND state IN ('active','partially_returned','overdue','disputed')",
            (sample_id,),
        ).fetchone()[0]
        held = round(float(held), 9)
        if held > 0:
            target = "loaned"
        elif sample["quantity"] <= 0:
            target = "consumed"
        elif sample["lifecycle_state"] == "loaned":
            target = "available"
        else:
            target = sample["lifecycle_state"]
        if target != sample["lifecycle_state"]:
            self.connection.execute(
                "UPDATE samples SET lifecycle_state=?,version=version+1,updated_at=? WHERE id=?",
                (target, now, sample_id),
            )

    def _resolve_recalls(self, loan_id: int, now: str, *, resolution: str) -> None:
        self.connection.execute(
            "UPDATE loan_recalls SET resolved_at=?,resolution=? WHERE loan_id=? AND resolved_at IS NULL",
            (now, resolution, loan_id),
        )

    def _invalidate(self, request: dict[str, Any], reason: str, now: str) -> None:
        if request["state"] == "invalidated":
            return
        self.connection.execute(
            "UPDATE loan_requests SET state='invalidated',invalidate_reason=?,version=version+1,updated_at=? WHERE id=?",
            (reason, now, request["id"]),
        )

    def _available(self, sample: dict[str, Any]) -> float:
        return round(float(sample["quantity"]) - float(sample["reserved_quantity"]), 9)

    def _ensure_sample_loanable(self, request: dict[str, Any], sample: dict[str, Any]) -> None:
        if sample["lifecycle_state"] in BLOCKING_SAMPLE_STATES:
            raise ConflictError(
                "样品当前不可借用",
                context={"lifecycle_state": sample["lifecycle_state"]},
            )

    def _ensure_renewal_unblocked(self, request: dict[str, Any], sample: dict[str, Any]) -> None:
        queue = self.requests.open_for_sample(sample["id"])
        if self._renewal_blocked_by_waiting(request, queue):
            blocker = next(
                (
                    q
                    for q in queue
                    if q["kind"] == "borrow"
                    and not q["fulfilled_loan_id"]
                    and q["state"] in ("queued", "approved")
                ),
                None,
            )
            raise ConflictError(
                "已有等待中的借用申请，续借不得越过",
                context={"blocking_request_id": blocker["id"] if blocker else None},
            )

    def _renewal_blocked_by_waiting(self, renewal: dict[str, Any], queue: list[dict[str, Any]]) -> bool:
        """续借前方（严格更早的排队位次）存在等待中的借用申请时被阻挡。

        等待中包含待审批（queued）和已批准但尚未占用（approved 候补）的借用，
        二者都意味着已有申请在等待当前借用释放库存。
        """
        for other in queue:
            if other["id"] == renewal["id"]:
                return False
            if other["kind"] == "borrow" and not other["fulfilled_loan_id"] and other["state"] in ("queued", "approved"):
                return True
        return False

    def _loan_view(self, loan_id: int | None) -> dict[str, Any] | None:
        if loan_id is None:
            return None
        row = self.connection.execute(
            """SELECT l.*,s.sample_code,u.display_name AS borrower_name
               FROM loans l JOIN samples s ON s.id=l.sample_id
               JOIN users u ON u.id=l.borrower_user_id WHERE l.id=?""",
            (loan_id,),
        ).fetchone()
        return _row(row, "借用记录不存在")

    def _request_recalls(self, request: dict[str, Any]) -> list[dict[str, Any]]:
        loan_id = request["fulfilled_loan_id"] or request["parent_loan_id"]
        if not loan_id:
            return []
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM loan_recalls WHERE loan_id=? ORDER BY id", (loan_id,)
        ).fetchall()]

    def _require_user(self, user_id: int) -> None:
        row = self.connection.execute(
            "SELECT id,status FROM users WHERE id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("借用用户不存在")
        if dict(row)["status"] != "active":
            raise ValidationError("借用用户账号不可用")

    def _can_view(self, principal: Principal, request: dict[str, Any]) -> bool:
        if principal.can("loans.manage") or principal.can("*"):
            return True
        return request["applicant_user_id"] == principal.user_id or request["borrower_user_id"] == principal.user_id

    def _actor_context(self, actor: Principal | AuditContext | None) -> AuditContext:
        if isinstance(actor, AuditContext):
            return actor
        if actor is not None:
            return AuditContext(actor.user_id, actor.display_name)
        return AuditContext(None, "系统")
