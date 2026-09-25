from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, PermissionDeniedError, ValidationError
from app.core.security import Principal
from app.samples.repository import SampleRepository
from app.services.audit import AuditContext, AuditService

LOANABLE_STATES = ("available", "partially_consumed", "loaned")
OPEN_REQUEST_STATES = ("pending", "waiting")
DEFAULT_LOAN_DAYS = 30
EPSILON = 1e-9

PRIORITY_LABELS = {1: "紧急", 2: "高", 3: "普通"}

BLOCK_REASON_LABELS = {
    "sample_quarantined": "样品已隔离",
    "destruction_pending": "样品进入销毁审批",
    "sample_destroyed": "样品已销毁",
    "quantity_reduced": "样品可用数量不足",
}

SYSTEM_ACTOR = AuditContext(actor_user_id=None, actor_name="系统")


def _parse_moment(value: str, field: str) -> str:
    try:
        parsed = from_storage(value)
    except ValueError as exc:
        raise ValidationError(f"{field}时间格式不正确") from exc
    if parsed is None:
        raise ValidationError(f"{field}不能为空")
    return to_storage(parsed)


def _transition_request(
    connection: sqlite3.Connection,
    request: sqlite3.Row | dict[str, Any],
    to_state: str,
    event_type: str,
    actor_user_id: int | None,
    now: str,
    details: dict[str, Any],
    *,
    block_reason: str | None = None,
    decided_by: int | None = None,
    decision_note: str | None = None,
) -> None:
    if to_state == "blocked":
        previous_state, stored_reason = request["state"], block_reason
    else:
        previous_state, stored_reason = None, None
    connection.execute(
        """UPDATE loan_requests SET state=?,block_reason=?,previous_state=?,
           decided_by=COALESCE(?,decided_by),decided_at=COALESCE(?,decided_at),
           decision_note=COALESCE(?,decision_note),version=version+1,updated_at=? WHERE id=?""",
        (
            to_state,
            stored_reason,
            previous_state,
            decided_by,
            now if decided_by is not None else None,
            decision_note,
            now,
            request["id"],
        ),
    )
    connection.execute(
        """INSERT INTO loan_request_events(request_id,event_type,actor_user_id,from_state,to_state,details_json,created_at)
           VALUES(?,?,?,?,?,?,?)""",
        (request["id"], event_type, actor_user_id, request["state"], to_state, json.dumps(details, ensure_ascii=False), now),
    )


def refresh_request_queue(
    connection: sqlite3.Connection,
    sample_id: int,
    *,
    actor_user_id: int | None = None,
    clock: Clock | None = None,
) -> dict[str, list[int]]:
    """Recompute which queued requests are currently fulfillable.

    Requests are scanned in queue order (priority, then submission order). A
    request becomes ``pending`` when the currently available quantity covers
    it; requests that do not fit stay ``waiting`` and do not block later
    satisfiable candidates. No inventory is reserved here — the atomic
    reservation happens at approval time.
    """
    clock = clock or SystemClock()
    now_moment = clock.now()
    now = to_storage(now_moment)
    sample = connection.execute(
        "SELECT id,quantity,reserved_quantity,lifecycle_state FROM samples WHERE id=?", (sample_id,)
    ).fetchone()
    if sample is None:
        return {"promoted": [], "demoted": []}
    rows = connection.execute(
        "SELECT * FROM loan_requests WHERE sample_id=? AND state IN ('pending','waiting') ORDER BY priority ASC,id ASC",
        (sample_id,),
    ).fetchall()
    available = float(sample["quantity"]) - float(sample["reserved_quantity"])
    loanable = sample["lifecycle_state"] in LOANABLE_STATES
    promoted: list[int] = []
    demoted: list[int] = []
    for row in rows:
        # requests past their needed_by no longer hold a queue slot; the overdue sweep expires them
        if row["needed_by"] is not None and row["needed_by"] <= now:
            continue
        fulfillable = loanable and float(row["quantity"]) <= available + EPSILON
        if fulfillable:
            available -= float(row["quantity"])
            if row["state"] != "pending":
                _transition_request(connection, row, "pending", "queue.promoted", actor_user_id, now, {})
                promoted.append(row["id"])
        elif row["state"] != "waiting":
            _transition_request(connection, row, "waiting", "queue.demoted", actor_user_id, now, {})
            demoted.append(row["id"])
    return {"promoted": promoted, "demoted": demoted}


def revalidate_sample_requests(
    connection: sqlite3.Connection,
    sample_id: int,
    *,
    trigger: str,
    actor: Any = None,
    clock: Clock | None = None,
) -> dict[str, Any]:
    """Re-validate unexecuted loan requests after sample state/quantity changes.

    Open requests are blocked when the sample is quarantined, destroyed, under
    destruction approval, or no longer holds enough total quantity. Blocked
    requests are restored automatically once the blocking condition clears.
    """
    clock = clock or SystemClock()
    now = to_storage(clock.now())
    sample = connection.execute("SELECT * FROM samples WHERE id=?", (sample_id,)).fetchone()
    if sample is None:
        return {"blocked": [], "unblocked": [], "promoted": [], "demoted": []}
    actor_user_id = getattr(actor, "user_id", None)
    audit_context = actor if actor is not None else SYSTEM_ACTOR
    audit = AuditService(connection, clock)
    quarantined = sample["lifecycle_state"] == "quarantined"
    destroyed = sample["lifecycle_state"] == "destroyed"
    pending_destruction = sample["lifecycle_state"] == "pending_destruction"
    destruction_open = bool(
        connection.execute(
            """SELECT 1 FROM approval_requests
               WHERE action_type='destruction' AND resource_type='sample' AND resource_id=? AND state IN ('pending','approved')""",
            (sample_id,),
        ).fetchone()
    )
    blocked: list[int] = []
    unblocked: list[int] = []
    open_requests = connection.execute(
        "SELECT * FROM loan_requests WHERE sample_id=? AND state IN ('pending','waiting','blocked') ORDER BY priority ASC,id ASC",
        (sample_id,),
    ).fetchall()
    for request in open_requests:
        reason = None
        if quarantined:
            reason = "sample_quarantined"
        elif destroyed:
            reason = "sample_destroyed"
        elif pending_destruction or destruction_open:
            reason = "destruction_pending"
        elif float(request["quantity"]) > float(sample["quantity"]) + EPSILON:
            reason = "quantity_reduced"
        if reason is None:
            continue
        if request["state"] == "blocked":
            if request["block_reason"] != reason:
                connection.execute(
                    "UPDATE loan_requests SET block_reason=?,version=version+1,updated_at=? WHERE id=?",
                    (reason, now, request["id"]),
                )
                connection.execute(
                    """INSERT INTO loan_request_events(request_id,event_type,actor_user_id,from_state,to_state,details_json,created_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (
                        request["id"],
                        "request.block_reason_changed",
                        actor_user_id,
                        "blocked",
                        "blocked",
                        json.dumps({"trigger": trigger, "reason": reason, "reason_label": BLOCK_REASON_LABELS[reason]}, ensure_ascii=False),
                        now,
                    ),
                )
            continue
        _transition_request(
            connection,
            request,
            "blocked",
            "request.blocked",
            actor_user_id,
            now,
            {"trigger": trigger, "reason": reason, "reason_label": BLOCK_REASON_LABELS[reason]},
            block_reason=reason,
        )
        audit.record(
            audit_context,
            "loan.request.block",
            "loan_request",
            str(request["id"]),
            before={"state": request["state"]},
            after={"state": "blocked", "block_reason": reason},
            metadata={"trigger": trigger},
        )
        blocked.append(request["id"])
    if not (quarantined or destroyed or pending_destruction or destruction_open):
        blocked_rows = connection.execute(
            "SELECT * FROM loan_requests WHERE sample_id=? AND state='blocked' ORDER BY id", (sample_id,)
        ).fetchall()
        for request in blocked_rows:
            if float(request["quantity"]) > float(sample["quantity"]) + EPSILON:
                continue
            _transition_request(
                connection,
                request,
                "waiting",
                "request.unblocked",
                actor_user_id,
                now,
                {"trigger": trigger, "previous_block_reason": request["block_reason"]},
            )
            audit.record(
                audit_context,
                "loan.request.unblock",
                "loan_request",
                str(request["id"]),
                before={"state": "blocked", "block_reason": request["block_reason"]},
                after={"state": "waiting"},
                metadata={"trigger": trigger},
            )
            unblocked.append(request["id"])
    queue = refresh_request_queue(connection, sample_id, actor_user_id=actor_user_id, clock=clock)
    return {"blocked": blocked, "unblocked": unblocked, **queue}


class LoanWorkflowService:
    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.samples = SampleRepository(connection)
        self.audit = AuditService(connection, self.clock)

    # ------------------------------------------------------------------
    # lookups
    # ------------------------------------------------------------------
    def _require_request(self, request_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM loan_requests WHERE id=?", (request_id,)).fetchone()
        if row is None:
            raise NotFoundError("借用申请不存在")
        return dict(row)

    def _require_loan(self, loan_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM loans WHERE id=?", (loan_id,)).fetchone()
        if row is None:
            raise NotFoundError("借用记录不存在")
        return dict(row)

    def _require_renewal(self, renewal_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM loan_renewals WHERE id=?", (renewal_id,)).fetchone()
        if row is None:
            raise NotFoundError("续借申请不存在")
        return dict(row)

    def _request_detail(self, request_id: int) -> dict[str, Any]:
        row = self.connection.execute(
            """SELECT r.*,s.sample_code,u.display_name AS applicant_name
               FROM loan_requests r JOIN samples s ON s.id=r.sample_id
               JOIN users u ON u.id=r.applicant_user_id WHERE r.id=?""",
            (request_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError("借用申请不存在")
        request = dict(row)
        request["priority_label"] = PRIORITY_LABELS.get(request["priority"], "普通")
        request["block_reason_label"] = BLOCK_REASON_LABELS.get(request.get("block_reason") or "")
        if request["state"] in ("pending", "waiting"):
            position_row = self.connection.execute(
                """SELECT COUNT(*) FROM loan_requests
                   WHERE sample_id=? AND state IN ('pending','waiting')
                     AND (priority<? OR (priority=? AND id<=?))""",
                (request["sample_id"], request["priority"], request["priority"], request_id),
            ).fetchone()
            request["queue_position"] = int(position_row[0])
        else:
            request["queue_position"] = None
        request["events"] = [
            {**dict(event), "details": json.loads(dict(event)["details_json"])}
            for event in self.connection.execute(
                "SELECT * FROM loan_request_events WHERE request_id=? ORDER BY id", (request_id,)
            ).fetchall()
        ]
        loan = self.connection.execute("SELECT * FROM loans WHERE approved_request_id=?", (request_id,)).fetchone()
        request["loan"] = dict(loan) if loan else None
        return request

    def _loan_detail(self, loan_id: int) -> dict[str, Any]:
        row = self.connection.execute(
            """SELECT l.*,s.sample_code,u.display_name AS borrower_name
               FROM loans l JOIN samples s ON s.id=l.sample_id
               JOIN users u ON u.id=l.borrower_user_id WHERE l.id=?""",
            (loan_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError("借用记录不存在")
        loan = dict(row)
        loan["renewals"] = [
            dict(item)
            for item in self.connection.execute(
                "SELECT * FROM loan_renewals WHERE loan_id=? ORDER BY id", (loan_id,)
            ).fetchall()
        ]
        loan["recalls"] = [
            dict(item)
            for item in self.connection.execute(
                "SELECT * FROM loan_recalls WHERE loan_id=? ORDER BY id", (loan_id,)
            ).fetchall()
        ]
        if loan.get("approved_request_id"):
            request = self.connection.execute(
                "SELECT * FROM loan_requests WHERE id=?", (loan["approved_request_id"],)
            ).fetchone()
            loan["request"] = dict(request) if request else None
        else:
            loan["request"] = None
        return loan

    def _destruction_open(self, sample_id: int) -> bool:
        return bool(
            self.connection.execute(
                """SELECT 1 FROM approval_requests
                   WHERE action_type='destruction' AND resource_type='sample' AND resource_id=? AND state IN ('pending','approved')""",
                (sample_id,),
            ).fetchone()
        )

    # ------------------------------------------------------------------
    # loan requests
    # ------------------------------------------------------------------
    def create_request(self, principal: Principal, data: dict[str, Any]) -> dict[str, Any]:
        if not (principal.can("loans.apply") or principal.can("loans.manage")):
            raise PermissionDeniedError("缺少权限：loans.apply")
        sample = self.samples.get(data["sample_id"])
        if sample["lifecycle_state"] not in LOANABLE_STATES:
            raise ConflictError("样品当前状态不可申请借用")
        if float(data["quantity"]) > float(sample["quantity"]) + EPSILON:
            raise ValidationError("申请数量超过样品现存总量")
        if self._destruction_open(sample["id"]):
            raise ConflictError("样品已进入销毁审批，暂不接受新的借用申请")
        needed_by = None
        if data.get("needed_by"):
            needed_by = _parse_moment(data["needed_by"], "期望归还")
            if from_storage(needed_by) <= self.clock.now():
                raise ValidationError("期望归还时间必须晚于当前时间")
        now = to_storage(self.clock.now())
        request_code = f"LR-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO loan_requests(request_code,sample_id,applicant_user_id,quantity,priority,needed_by,note,state,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,'waiting',?,?)""",
            (request_code, sample["id"], principal.user_id, data["quantity"], data["priority"], needed_by, data.get("note", ""), now, now),
        )
        request_id = int(cursor.lastrowid)
        self.connection.execute(
            """INSERT INTO loan_request_events(request_id,event_type,actor_user_id,from_state,to_state,details_json,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (
                request_id,
                "request.created",
                principal.user_id,
                None,
                "waiting",
                json.dumps({"quantity": data["quantity"], "priority": data["priority"]}, ensure_ascii=False),
                now,
            ),
        )
        self.audit.record(
            principal,
            "loan.request.create",
            "loan_request",
            str(request_id),
            after={"sample_id": sample["id"], "quantity": data["quantity"], "priority": data["priority"]},
        )
        refresh_request_queue(self.connection, sample["id"], actor_user_id=principal.user_id, clock=self.clock)
        return self._request_detail(request_id)

    def list_requests(
        self,
        principal: Principal,
        *,
        state: str | None = None,
        sample_id: int | None = None,
        mine: bool = False,
    ) -> list[dict[str, Any]]:
        if not (principal.can("loans.manage") or principal.can("loans.apply")):
            raise PermissionDeniedError("缺少权限：loans.apply")
        clauses: list[str] = []
        params: list[Any] = []
        if state:
            clauses.append("r.state=?")
            params.append(state)
        if sample_id is not None:
            clauses.append("r.sample_id=?")
            params.append(sample_id)
        if mine or not principal.can("loans.manage"):
            clauses.append("r.applicant_user_id=?")
            params.append(principal.user_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.connection.execute(
            """SELECT r.*,s.sample_code,u.display_name AS applicant_name
               FROM loan_requests r JOIN samples s ON s.id=r.sample_id
               JOIN users u ON u.id=r.applicant_user_id"""
            + where
            + " ORDER BY CASE WHEN r.state IN ('pending','waiting') THEN 0 ELSE 1 END,r.priority ASC,r.id ASC",
            tuple(params),
        ).fetchall()
        result = []
        position = 0
        for row in rows:
            item = dict(row)
            item["priority_label"] = PRIORITY_LABELS.get(item["priority"], "普通")
            item["block_reason_label"] = BLOCK_REASON_LABELS.get(item.get("block_reason") or "")
            if item["state"] in ("pending", "waiting"):
                position += 1
                item["queue_position"] = position
            else:
                item["queue_position"] = None
            result.append(item)
        return result

    def get_request(self, principal: Principal, request_id: int) -> dict[str, Any]:
        detail = self._request_detail(request_id)
        if not principal.can("loans.manage") and detail["applicant_user_id"] != principal.user_id:
            raise PermissionDeniedError("只能查看本人的借用申请")
        return detail

    def approve_request(self, principal: Principal, request_id: int, note: str = "") -> dict[str, Any]:
        principal.require("loans.manage")
        request = self._require_request(request_id)
        if request["state"] == "waiting":
            raise ConflictError("申请仍在候补队列中，可用库存不足或排队未到")
        if request["state"] == "blocked":
            raise ConflictError("申请当前处于限制状态，不能批准")
        if request["state"] != "pending":
            raise ConflictError("申请已结束，不能批准")
        if request["applicant_user_id"] == principal.user_id:
            raise ValidationError("申请人不能批准自己的借用申请")
        if request["needed_by"] and from_storage(request["needed_by"]) <= self.clock.now():
            raise ConflictError("申请已超过期望归还时间，请先运行逾期任务将其置为过期")
        sample = self.samples.get(request["sample_id"])
        now = to_storage(self.clock.now())
        cursor = self.connection.execute(
            """UPDATE samples SET reserved_quantity=reserved_quantity+?,lifecycle_state='loaned',version=version+1,updated_at=?
               WHERE id=? AND lifecycle_state IN ('available','partially_consumed','loaned') AND quantity-reserved_quantity>=?""",
            (request["quantity"], now, sample["id"], request["quantity"]),
        )
        if cursor.rowcount != 1:
            refresh_request_queue(self.connection, sample["id"], actor_user_id=principal.user_id, clock=self.clock)
            raise ConflictError("样品状态或可用数量已变化，无法完成库存占用")
        due_at = request["needed_by"] or to_storage(self.clock.now() + timedelta(days=DEFAULT_LOAN_DAYS))
        loan_code = f"LOAN-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO loans(loan_code,sample_id,borrower_user_id,approved_request_id,quantity,due_at,state,created_at,updated_at)
               VALUES(?,?,?,?,?,?,'active',?,?)""",
            (loan_code, sample["id"], request["applicant_user_id"], request_id, request["quantity"], due_at, now, now),
        )
        loan_id = int(cursor.lastrowid)
        _transition_request(
            self.connection,
            request,
            "fulfilled",
            "request.fulfilled",
            principal.user_id,
            now,
            {"loan_id": loan_id, "loan_code": loan_code},
            decided_by=principal.user_id,
            decision_note=note,
        )
        self.samples.append_event(
            sample["id"],
            "loan.created",
            principal.user_id,
            now,
            from_state=sample["lifecycle_state"],
            to_state="loaned",
            details={
                "loan_id": loan_id,
                "request_id": request_id,
                "borrower_user_id": request["applicant_user_id"],
                "reserved_delta": request["quantity"],
            },
        )
        self.audit.record(
            principal,
            "loan.request.approve",
            "loan_request",
            str(request_id),
            before={"state": request["state"]},
            after={"state": "fulfilled", "loan_id": loan_id},
            metadata={"reserved_delta": request["quantity"]},
        )
        queue = refresh_request_queue(self.connection, sample["id"], actor_user_id=principal.user_id, clock=self.clock)
        return {"request": self._request_detail(request_id), "loan": self._loan_detail(loan_id), "queue": queue}

    def reject_request(self, principal: Principal, request_id: int, note: str = "") -> dict[str, Any]:
        principal.require("loans.manage")
        request = self._require_request(request_id)
        if request["state"] not in OPEN_REQUEST_STATES + ("blocked",):
            raise ConflictError("申请已结束，不能驳回")
        if request["applicant_user_id"] == principal.user_id:
            raise ValidationError("申请人不能驳回自己的借用申请")
        now = to_storage(self.clock.now())
        _transition_request(
            self.connection,
            request,
            "rejected",
            "request.rejected",
            principal.user_id,
            now,
            {"note": note},
            decided_by=principal.user_id,
            decision_note=note,
        )
        self.audit.record(
            principal,
            "loan.request.reject",
            "loan_request",
            str(request_id),
            before={"state": request["state"]},
            after={"state": "rejected"},
        )
        queue = refresh_request_queue(self.connection, request["sample_id"], actor_user_id=principal.user_id, clock=self.clock)
        return {"request": self._request_detail(request_id), "queue": queue}

    def cancel_request(self, principal: Principal, request_id: int) -> dict[str, Any]:
        request = self._require_request(request_id)
        if not principal.can("loans.manage") and request["applicant_user_id"] != principal.user_id:
            raise PermissionDeniedError("只能撤销本人的借用申请")
        if request["state"] not in OPEN_REQUEST_STATES + ("blocked",):
            raise ConflictError("申请已结束，不能撤销")
        now = to_storage(self.clock.now())
        _transition_request(self.connection, request, "cancelled", "request.cancelled", principal.user_id, now, {})
        self.audit.record(
            principal,
            "loan.request.cancel",
            "loan_request",
            str(request_id),
            before={"state": request["state"]},
            after={"state": "cancelled"},
        )
        queue = refresh_request_queue(self.connection, request["sample_id"], actor_user_id=principal.user_id, clock=self.clock)
        return {"request": self._request_detail(request_id), "queue": queue}

    # ------------------------------------------------------------------
    # loans
    # ------------------------------------------------------------------
    def list_loans(
        self,
        principal: Principal,
        *,
        state: str | None = None,
        sample_id: int | None = None,
    ) -> list[dict[str, Any]]:
        if not (principal.can("loans.manage") or principal.can("loans.apply")):
            raise PermissionDeniedError("缺少权限：loans.apply")
        clauses: list[str] = []
        params: list[Any] = []
        if state:
            clauses.append("l.state=?")
            params.append(state)
        if sample_id is not None:
            clauses.append("l.sample_id=?")
            params.append(sample_id)
        if not principal.can("loans.manage"):
            clauses.append("l.borrower_user_id=?")
            params.append(principal.user_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.connection.execute(
            """SELECT l.*,s.sample_code,u.display_name AS borrower_name
               FROM loans l JOIN samples s ON s.id=l.sample_id
               JOIN users u ON u.id=l.borrower_user_id"""
            + where
            + " ORDER BY l.id DESC",
            tuple(params),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_loan(self, principal: Principal, loan_id: int) -> dict[str, Any]:
        detail = self._loan_detail(loan_id)
        if not principal.can("loans.manage") and detail["borrower_user_id"] != principal.user_id:
            raise PermissionDeniedError("只能查看本人的借用记录")
        return detail

    def create_direct_loan(self, principal: Principal, data: dict[str, Any]) -> dict[str, Any]:
        """Manager-created loan without a queued request; reserves atomically."""
        principal.require("loans.manage")
        sample = self.samples.get(data["sample_id"])
        if sample["lifecycle_state"] not in LOANABLE_STATES:
            raise ConflictError("样品当前不可借用")
        due_at = _parse_moment(data["due_at"], "到期")
        now = to_storage(self.clock.now())
        cursor = self.connection.execute(
            """UPDATE samples SET reserved_quantity=reserved_quantity+?,lifecycle_state='loaned',version=version+1,updated_at=?
               WHERE id=? AND lifecycle_state IN ('available','partially_consumed','loaned') AND quantity-reserved_quantity>=?""",
            (data["quantity"], now, sample["id"], data["quantity"]),
        )
        if cursor.rowcount != 1:
            raise ConflictError("可借数量不足或样品状态已变化")
        loan_code = data.get("loan_code") or f"LOAN-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO loans(loan_code,sample_id,borrower_user_id,quantity,due_at,state,created_at,updated_at)
               VALUES(?,?,?,?,?,'active',?,?)""",
            (loan_code, data["sample_id"], data["borrower_user_id"], data["quantity"], due_at, now, now),
        )
        loan_id = int(cursor.lastrowid)
        self.samples.append_event(
            data["sample_id"],
            "loaned",
            principal.user_id,
            now,
            from_state=sample["lifecycle_state"],
            to_state="loaned",
            details={"loan_id": loan_id, "borrower_user_id": data["borrower_user_id"], "reserved_delta": data["quantity"]},
        )
        self.audit.record(principal, "loan.create", "loan", str(loan_id), after=self._loan_detail(loan_id))
        queue = refresh_request_queue(self.connection, data["sample_id"], actor_user_id=principal.user_id, clock=self.clock)
        return {**self._loan_detail(loan_id), "queue": queue}

    def return_loan(self, principal: Principal, loan_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("loans.manage")
        loan = self._require_loan(loan_id)
        if loan["state"] not in ("active", "partially_returned", "overdue"):
            raise ConflictError("借用记录已经结束")
        quantity = float(data["quantity"])
        remaining = float(loan["quantity"]) - float(loan["returned_quantity"])
        if quantity > remaining + EPSILON:
            raise ValidationError("归还数量超过未归还数量")
        sample = self.samples.get(loan["sample_id"])
        now = to_storage(self.clock.now())
        returned = round(float(loan["returned_quantity"]) + quantity, 9)
        fully_returned = returned + EPSILON >= float(loan["quantity"])
        if fully_returned:
            new_state = "returned"
        elif loan["state"] == "overdue":
            new_state = "overdue"
        else:
            new_state = "partially_returned"
        new_reserved = max(0.0, round(float(sample["reserved_quantity"]) - quantity, 9))
        if sample["lifecycle_state"] in ("quarantined", "pending_destruction", "destroyed", "consumed"):
            sample_state = sample["lifecycle_state"]
        elif new_reserved > 0:
            sample_state = "loaned"
        elif float(sample["quantity"]) == 0:
            sample_state = "consumed"
        else:
            sample_state = "available"
        self.connection.execute(
            "UPDATE loans SET returned_quantity=?,state=?,version=version+1,updated_at=? WHERE id=?",
            (returned, new_state, now, loan_id),
        )
        self.connection.execute(
            "UPDATE samples SET reserved_quantity=?,lifecycle_state=?,version=version+1,updated_at=? WHERE id=?",
            (new_reserved, sample_state, now, sample["id"]),
        )
        self.samples.append_event(
            sample["id"],
            "loan.returned" if fully_returned else "loan.partial_return",
            principal.user_id,
            now,
            details={"loan_id": loan_id, "returned_quantity": quantity, "remaining": max(0.0, remaining - quantity)},
        )
        closed_recalls: list[dict[str, Any]] = []
        if fully_returned:
            open_recalls = self.connection.execute(
                "SELECT * FROM loan_recalls WHERE loan_id=? AND state='open' ORDER BY id", (loan_id,)
            ).fetchall()
            for recall in open_recalls:
                self.connection.execute(
                    "UPDATE loan_recalls SET state='returned',closed_at=? WHERE id=?",
                    (now, recall["id"]),
                )
                closed_recalls.append(dict(recall))
                self.audit.record(
                    principal,
                    "loan.recall.close",
                    "loan_recall",
                    str(recall["id"]),
                    before={"state": "open"},
                    after={"state": "returned"},
                )
        self.audit.record(
            principal,
            "loan.return",
            "loan",
            str(loan_id),
            before={"state": loan["state"], "returned_quantity": loan["returned_quantity"]},
            after={"state": new_state, "returned_quantity": returned},
            metadata={"returned_quantity": quantity, "fully_returned": fully_returned},
        )
        queue = refresh_request_queue(self.connection, sample["id"], actor_user_id=principal.user_id, clock=self.clock)
        auto_fulfilled = self._auto_fulfill_queue(sample["id"], principal, now)
        return {
            "loan": self._loan_detail(loan_id),
            "sample": self.samples.get(sample["id"]),
            "closed_recalls": closed_recalls,
            "queue": queue,
            "auto_fulfilled": auto_fulfilled,
        }

    def _auto_fulfill_queue(self, sample_id: int, actor: Any, now: str) -> list[dict[str, Any]]:
        """Atomically grant queued requests that fit the freed stock, in queue order.

        Scans the whole queue once by priority then submission order, so a
        large request that still does not fit never blocks a smaller request
        behind it from being granted.
        """
        rows = self.connection.execute(
            """SELECT id FROM loan_requests
               WHERE sample_id=? AND state IN ('pending','waiting')
               ORDER BY priority ASC,id ASC""",
            (sample_id,),
        ).fetchall()
        fulfilled: list[dict[str, Any]] = []
        skipped_stale = 0
        for (request_id,) in rows:
            request = dict(
                self.connection.execute("SELECT * FROM loan_requests WHERE id=?", (request_id,)).fetchone()
            )
            if request["state"] not in ("pending", "waiting"):
                continue
            if request["needed_by"] is not None and request["needed_by"] <= now:
                _transition_request(
                    self.connection,
                    request,
                    "expired",
                    "request.expired",
                    actor.user_id,
                    now,
                    {"needed_by": request["needed_by"], "trigger": "return"},
                )
                skipped_stale += 1
                continue
            cursor = self.connection.execute(
                """UPDATE samples SET reserved_quantity=reserved_quantity+?,lifecycle_state='loaned',version=version+1,updated_at=?
                   WHERE id=? AND lifecycle_state IN ('available','partially_consumed','loaned') AND quantity-reserved_quantity>=?""",
                (request["quantity"], now, sample_id, request["quantity"]),
            )
            if cursor.rowcount != 1:
                continue
            due_at = request["needed_by"] or to_storage(self.clock.now() + timedelta(days=DEFAULT_LOAN_DAYS))
            loan_code = f"LOAN-{uuid.uuid4().hex[:12]}"
            cursor = self.connection.execute(
                """INSERT INTO loans(loan_code,sample_id,borrower_user_id,approved_request_id,quantity,due_at,state,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,'active',?,?)""",
                (loan_code, sample_id, request["applicant_user_id"], request["id"], request["quantity"], due_at, now, now),
            )
            loan_id = int(cursor.lastrowid)
            _transition_request(
                self.connection,
                request,
                "fulfilled",
                "request.auto_fulfilled",
                actor.user_id,
                now,
                {"loan_id": loan_id, "loan_code": loan_code, "trigger": "return"},
                decided_by=actor.user_id,
                decision_note="归还后自动候补发放",
            )
            self.samples.append_event(
                sample_id,
                "loan.auto_granted",
                actor.user_id,
                now,
                details={"loan_id": loan_id, "request_id": request["id"], "reserved_delta": request["quantity"]},
            )
            self.audit.record(
                actor,
                "loan.request.auto_approve",
                "loan_request",
                str(request["id"]),
                after={"state": "fulfilled", "loan_id": loan_id},
                metadata={"trigger": "return", "reserved_delta": request["quantity"]},
            )
            fulfilled.append({"request_id": request["id"], "loan_id": loan_id, "loan_code": loan_code})
        refresh_request_queue(self.connection, sample_id, actor_user_id=actor.user_id, clock=self.clock)
        if skipped_stale:
            self.audit.record(
                actor,
                "loan.request.expire",
                "loan_request",
                None,
                metadata={"count": skipped_stale, "trigger": "return"},
            )
        return fulfilled

    # ------------------------------------------------------------------
    # renewals
    # ------------------------------------------------------------------
    def create_renewal(self, principal: Principal, loan_id: int, data: dict[str, Any]) -> dict[str, Any]:
        loan = self._require_loan(loan_id)
        if not (principal.can("loans.manage") or (principal.user_id == loan["borrower_user_id"] and principal.can("loans.apply"))):
            raise PermissionDeniedError("只有借用人或借用管理员可以申请续借")
        if loan["state"] not in ("active", "partially_returned", "overdue"):
            raise ConflictError("已结束的借用不能续借")
        existing = self.connection.execute(
            "SELECT 1 FROM loan_renewals WHERE loan_id=? AND state='pending'", (loan_id,)
        ).fetchone()
        if existing:
            raise ConflictError("已存在待审批的续借申请")
        new_due_at = _parse_moment(data["new_due_at"], "新的到期")
        if from_storage(new_due_at) <= from_storage(loan["due_at"]):
            raise ValidationError("新的到期时间必须晚于当前到期时间")
        now = to_storage(self.clock.now())
        renewal_code = f"LRN-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO loan_renewals(renewal_code,loan_id,requested_by,new_due_at,reason,state,created_at,updated_at)
               VALUES(?,?,?,?,?,'pending',?,?)""",
            (renewal_code, loan_id, principal.user_id, new_due_at, data.get("reason", ""), now, now),
        )
        renewal_id = int(cursor.lastrowid)
        self.audit.record(
            principal,
            "loan.renewal.create",
            "loan_renewal",
            str(renewal_id),
            after={"loan_id": loan_id, "new_due_at": new_due_at},
        )
        return self._require_renewal(renewal_id)

    def decide_renewal(self, principal: Principal, renewal_id: int, decision: str, note: str = "") -> dict[str, Any]:
        principal.require("loans.manage")
        renewal = self._require_renewal(renewal_id)
        if renewal["state"] != "pending":
            raise ConflictError("续借申请已经处理")
        if renewal["requested_by"] == principal.user_id:
            raise ValidationError("申请人不能审批自己的续借")
        loan = self._require_loan(renewal["loan_id"])
        now = to_storage(self.clock.now())
        if decision == "reject":
            self.connection.execute(
                "UPDATE loan_renewals SET state='rejected',decided_by=?,decided_at=?,decision_note=?,version=version+1,updated_at=? WHERE id=?",
                (principal.user_id, now, note, now, renewal_id),
            )
            self.audit.record(
                principal,
                "loan.renewal.reject",
                "loan_renewal",
                str(renewal_id),
                before={"state": "pending"},
                after={"state": "rejected"},
            )
            return {"renewal": self._require_renewal(renewal_id), "loan": self._loan_detail(loan["id"])}
        priority = 3
        if loan.get("approved_request_id"):
            linked = self.connection.execute(
                "SELECT priority FROM loan_requests WHERE id=?", (loan["approved_request_id"],)
            ).fetchone()
            if linked:
                priority = int(linked["priority"])
        blocking = [
            dict(row)
            for row in self.connection.execute(
                """SELECT id,request_code,priority,quantity FROM loan_requests
                   WHERE sample_id=? AND state IN ('pending','waiting') AND priority<? ORDER BY priority ASC,id ASC""",
                (loan["sample_id"], priority),
            ).fetchall()
        ]
        if blocking:
            raise ConflictError(
                "存在更高优先级的排队申请，续借不能越过",
                context={"blocking_request_ids": [item["id"] for item in blocking]},
            )
        base_state = "partially_returned" if float(loan["returned_quantity"]) > 0 else "active"
        new_state = base_state if from_storage(renewal["new_due_at"]) > self.clock.now() else "overdue"
        self.connection.execute(
            "UPDATE loans SET due_at=?,state=?,version=version+1,updated_at=? WHERE id=?",
            (renewal["new_due_at"], new_state, now, loan["id"]),
        )
        self.connection.execute(
            "UPDATE loan_renewals SET state='approved',decided_by=?,decided_at=?,decision_note=?,version=version+1,updated_at=? WHERE id=?",
            (principal.user_id, now, note, now, renewal_id),
        )
        if new_state != "overdue":
            open_recall = self.connection.execute(
                "SELECT * FROM loan_recalls WHERE loan_id=? AND state='open'", (loan["id"],)
            ).fetchone()
            if open_recall:
                self.connection.execute(
                    "UPDATE loan_recalls SET state='cancelled',closed_at=? WHERE id=?",
                    (now, open_recall["id"]),
                )
                self.samples.append_event(
                    loan["sample_id"],
                    "loan.recall_closed",
                    principal.user_id,
                    now,
                    details={"loan_id": loan["id"], "recall_id": open_recall["id"], "reason": "renewed"},
                )
                self.audit.record(
                    principal,
                    "loan.recall.close",
                    "loan_recall",
                    str(open_recall["id"]),
                    before={"state": "open"},
                    after={"state": "cancelled", "reason": "renewed"},
                )
        self.samples.append_event(
            loan["sample_id"],
            "loan.renewed",
            principal.user_id,
            now,
            details={"loan_id": loan["id"], "renewal_id": renewal_id, "new_due_at": renewal["new_due_at"]},
        )
        self.audit.record(
            principal,
            "loan.renewal.approve",
            "loan_renewal",
            str(renewal_id),
            before={"state": "pending"},
            after={"state": "approved", "new_due_at": renewal["new_due_at"]},
        )
        return {"renewal": self._require_renewal(renewal_id), "loan": self._loan_detail(loan["id"])}

    def list_renewals(
        self,
        principal: Principal,
        *,
        state: str | None = None,
        loan_id: int | None = None,
    ) -> list[dict[str, Any]]:
        if not (principal.can("loans.manage") or principal.can("loans.apply")):
            raise PermissionDeniedError("缺少权限：loans.apply")
        clauses: list[str] = []
        params: list[Any] = []
        if state:
            clauses.append("rn.state=?")
            params.append(state)
        if loan_id is not None:
            clauses.append("rn.loan_id=?")
            params.append(loan_id)
        if not principal.can("loans.manage"):
            clauses.append("rn.requested_by=?")
            params.append(principal.user_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.connection.execute(
            """SELECT rn.*,l.loan_code,l.sample_id,u.display_name AS requester_name
               FROM loan_renewals rn JOIN loans l ON l.id=rn.loan_id
               JOIN users u ON u.id=rn.requested_by"""
            + where
            + " ORDER BY rn.id DESC",
            tuple(params),
        ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # recalls and overdue sweep
    # ------------------------------------------------------------------
    def list_recalls(self, principal: Principal, *, state: str | None = None, loan_id: int | None = None) -> list[dict[str, Any]]:
        if not (principal.can("loans.manage") or principal.can("loans.apply")):
            raise PermissionDeniedError("缺少权限：loans.apply")
        clauses: list[str] = []
        params: list[Any] = []
        if state:
            clauses.append("rc.state=?")
            params.append(state)
        if loan_id is not None:
            clauses.append("rc.loan_id=?")
            params.append(loan_id)
        if not principal.can("loans.manage"):
            clauses.append("l.borrower_user_id=?")
            params.append(principal.user_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.connection.execute(
            """SELECT rc.*,l.loan_code,l.sample_id,l.borrower_user_id,u.display_name AS borrower_name
               FROM loan_recalls rc JOIN loans l ON l.id=rc.loan_id
               JOIN users u ON u.id=l.borrower_user_id"""
            + where
            + " ORDER BY rc.id DESC",
            tuple(params),
        ).fetchall()
        return [dict(row) for row in rows]

    def run_overdue_sweep(self, actor: Principal | None, *, sample_id: int | None = None) -> dict[str, Any]:
        """Mark overdue loans, issue one recall per overdue episode, expire stale requests.

        The sweep is idempotent: re-running it never creates a second recall
        for an episode that already has an open recall record.
        """
        if actor is not None:
            actor.require("loans.manage")
        audit_context: Any = actor if actor is not None else AuditContext(None, "系统任务")
        actor_user_id = getattr(actor, "user_id", None)
        now_moment = self.clock.now()
        now = to_storage(now_moment)
        clauses = ["l.state IN ('active','partially_returned','overdue')", "l.due_at<?"]
        params: list[Any] = [now]
        if sample_id is not None:
            clauses.append("l.sample_id=?")
            params.append(sample_id)
        loans = self.connection.execute(
            "SELECT l.* FROM loans l WHERE " + " AND ".join(clauses) + " ORDER BY l.id",
            tuple(params),
        ).fetchall()
        marked_overdue = 0
        recalls_created: list[dict[str, Any]] = []
        affected_samples: set[int] = set()
        for loan in loans:
            affected_samples.add(loan["sample_id"])
            if loan["state"] != "overdue":
                self.connection.execute(
                    "UPDATE loans SET state='overdue',version=version+1,updated_at=? WHERE id=?",
                    (now, loan["id"]),
                )
                self.samples.append_event(
                    loan["sample_id"],
                    "loan.overdue",
                    actor_user_id,
                    now,
                    details={"loan_id": loan["id"], "due_at": loan["due_at"]},
                )
                marked_overdue += 1
            open_recall = self.connection.execute(
                "SELECT id FROM loan_recalls WHERE loan_id=? AND state='open'", (loan["id"],)
            ).fetchone()
            if open_recall:
                continue
            episode = int(
                self.connection.execute(
                    "SELECT COALESCE(MAX(episode),0) FROM loan_recalls WHERE loan_id=?", (loan["id"],)
                ).fetchone()[0]
            ) + 1
            due_moment = from_storage(loan["due_at"])
            overdue_days = round(max(0.0, (now_moment - due_moment).total_seconds()) / 86400, 3)
            recall_code = f"RCL-{uuid.uuid4().hex[:12]}"
            cursor = self.connection.execute(
                """INSERT INTO loan_recalls(recall_code,loan_id,episode,overdue_days,reason,state,created_by,created_at)
                   VALUES(?,?,?,?,?,'open',?,?)""",
                (recall_code, loan["id"], episode, overdue_days, "逾期未归还", actor_user_id, now),
            )
            recall_id = int(cursor.lastrowid)
            self.samples.append_event(
                loan["sample_id"],
                "loan.recall_issued",
                actor_user_id,
                now,
                details={"loan_id": loan["id"], "recall_id": recall_id, "episode": episode, "overdue_days": overdue_days},
            )
            self.audit.record(
                audit_context,
                "loan.recall.create",
                "loan_recall",
                str(recall_id),
                after={"loan_id": loan["id"], "episode": episode, "overdue_days": overdue_days},
            )
            recalls_created.append(dict(self.connection.execute("SELECT * FROM loan_recalls WHERE id=?", (recall_id,)).fetchone()))
        request_clauses = ["state IN ('pending','waiting')", "needed_by IS NOT NULL", "needed_by<?"]
        request_params: list[Any] = [now]
        if sample_id is not None:
            request_clauses.append("sample_id=?")
            request_params.append(sample_id)
        stale_requests = self.connection.execute(
            "SELECT * FROM loan_requests WHERE " + " AND ".join(request_clauses) + " ORDER BY id",
            tuple(request_params),
        ).fetchall()
        expired: list[int] = []
        for request in stale_requests:
            affected_samples.add(request["sample_id"])
            _transition_request(
                self.connection,
                dict(request),
                "expired",
                "request.expired",
                actor_user_id,
                now,
                {"needed_by": request["needed_by"]},
            )
            self.audit.record(
                audit_context,
                "loan.request.expire",
                "loan_request",
                str(request["id"]),
                before={"state": request["state"]},
                after={"state": "expired"},
            )
            expired.append(int(request["id"]))
        for affected in affected_samples:
            refresh_request_queue(self.connection, affected, actor_user_id=actor_user_id, clock=self.clock)
        summary = {
            "loans_marked_overdue": marked_overdue,
            "recalls_created": len(recalls_created),
            "requests_expired": len(expired),
            "expired_request_ids": expired,
            "recalls": recalls_created,
        }
        self.audit.record(
            audit_context,
            "loan.sweep",
            "loan",
            None,
            metadata={
                "loans_marked_overdue": marked_overdue,
                "recalls_created": len(recalls_created),
                "requests_expired": len(expired),
                "sample_id": sample_id,
            },
        )
        return summary

    # ------------------------------------------------------------------
    # availability summary
    # ------------------------------------------------------------------
    def availability(self, principal: Principal, sample_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        sample = self.samples.get(sample_id)
        rows = self.connection.execute(
            """SELECT state,priority,quantity FROM loan_requests
               WHERE sample_id=? AND state IN ('pending','waiting','blocked')""",
            (sample_id,),
        ).fetchall()
        pending_demand = 0.0
        waiting_demand = 0.0
        blocked_demand = 0.0
        for row in rows:
            amount = float(row["quantity"])
            if row["state"] == "pending":
                pending_demand += amount
            elif row["state"] == "waiting":
                waiting_demand += amount
            else:
                blocked_demand += amount
        reason_rows = self.connection.execute(
            "SELECT DISTINCT block_reason FROM loan_requests WHERE sample_id=? AND state='blocked' AND block_reason IS NOT NULL",
            (sample_id,),
        ).fetchall()
        blocked_reasons = {row[0] for row in reason_rows}
        active_loans = self.connection.execute(
            "SELECT COALESCE(SUM(quantity-returned_quantity),0) FROM loans WHERE sample_id=? AND state IN ('active','partially_returned','overdue')",
            (sample_id,),
        ).fetchone()[0]
        quantity = float(sample["quantity"])
        reserved = float(sample["reserved_quantity"])
        return {
            "sample_id": sample_id,
            "sample_code": sample["sample_code"],
            "lifecycle_state": sample["lifecycle_state"],
            "quantity": quantity,
            "reserved_quantity": reserved,
            "loanable_quantity": max(0.0, round(quantity - reserved, 9)),
            "active_borrowed_quantity": float(active_loans),
            "pending_demand_quantity": pending_demand,
            "waiting_demand_quantity": waiting_demand,
            "blocked_demand_quantity": blocked_demand,
            "blocked_reasons": sorted(blocked_reasons),
            "blocked_reason_labels": [BLOCK_REASON_LABELS[r] for r in sorted(blocked_reasons)],
        }

    # ------------------------------------------------------------------
    # sample quarantine and quantity adjustment
    # ------------------------------------------------------------------
    def quarantine_sample(self, principal: Principal, sample_id: int, reason: str) -> dict[str, Any]:
        principal.require("samples.write")
        sample = self.samples.get(sample_id)
        if sample["lifecycle_state"] == "quarantined":
            raise ConflictError("样品已处于隔离状态")
        if sample["lifecycle_state"] in ("destroyed", "consumed", "pending_destruction"):
            raise ConflictError("当前状态不能隔离")
        now = to_storage(self.clock.now())
        cursor = self.connection.execute(
            "UPDATE samples SET lifecycle_state='quarantined',quarantine_reason=?,version=version+1,updated_at=? WHERE id=? AND lifecycle_state=?",
            (reason, now, sample_id, sample["lifecycle_state"]),
        )
        if cursor.rowcount != 1:
            raise ConflictError("样品状态已变化，请刷新后重试")
        self.samples.append_event(
            sample_id,
            "quarantined",
            principal.user_id,
            now,
            from_state=sample["lifecycle_state"],
            to_state="quarantined",
            details={"reason": reason},
        )
        self.audit.record(
            principal,
            "sample.quarantine",
            "sample",
            str(sample_id),
            before={"lifecycle_state": sample["lifecycle_state"]},
            after={"lifecycle_state": "quarantined", "reason": reason},
        )
        revalidation = revalidate_sample_requests(self.connection, sample_id, trigger="quarantine", actor=principal, clock=self.clock)
        return {"sample": self.samples.get(sample_id), "revalidation": revalidation}

    def release_quarantine(self, principal: Principal, sample_id: int) -> dict[str, Any]:
        principal.require("samples.write")
        sample = self.samples.get(sample_id)
        if sample["lifecycle_state"] != "quarantined":
            raise ConflictError("样品不在隔离状态")
        if float(sample["reserved_quantity"]) > 0:
            target_state = "loaned"
        elif float(sample["quantity"]) == 0:
            target_state = "consumed"
        else:
            target_state = "available"
        now = to_storage(self.clock.now())
        cursor = self.connection.execute(
            "UPDATE samples SET lifecycle_state=?,quarantine_reason='',version=version+1,updated_at=? WHERE id=? AND lifecycle_state='quarantined'",
            (target_state, now, sample_id),
        )
        if cursor.rowcount != 1:
            raise ConflictError("样品状态已变化，请刷新后重试")
        self.samples.append_event(
            sample_id,
            "quarantine_released",
            principal.user_id,
            now,
            from_state="quarantined",
            to_state=target_state,
            details={},
        )
        self.audit.record(
            principal,
            "sample.quarantine_release",
            "sample",
            str(sample_id),
            before={"lifecycle_state": "quarantined"},
            after={"lifecycle_state": target_state},
        )
        revalidation = revalidate_sample_requests(self.connection, sample_id, trigger="quarantine_released", actor=principal, clock=self.clock)
        return {"sample": self.samples.get(sample_id), "revalidation": revalidation}

    def adjust_quantity(self, principal: Principal, sample_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.write")
        sample = self.samples.get(sample_id)
        if sample["lifecycle_state"] == "destroyed":
            raise ConflictError("已销毁样品不能调整数量")
        new_quantity = float(data["new_quantity"])
        if new_quantity < float(sample["reserved_quantity"]) - EPSILON:
            raise ConflictError("调整后的数量不能低于已预留数量")
        delta = round(new_quantity - float(sample["quantity"]), 9)
        if abs(delta) < EPSILON:
            return {"sample": sample, "revalidation": None, "replayed": True}
        now = to_storage(self.clock.now())
        cursor = self.connection.execute(
            "UPDATE samples SET quantity=?,adjusted_at=?,version=version+1,updated_at=? WHERE id=? AND version=?",
            (new_quantity, now, now, sample_id, sample["version"]),
        )
        if cursor.rowcount != 1:
            raise ConflictError("样品版本已变化，请刷新后重试")
        self.samples.append_event(
            sample_id,
            "inventory_adjusted",
            principal.user_id,
            now,
            quantity_delta=delta,
            details={"reason": data["reason"], "new_quantity": new_quantity},
        )
        self.audit.record(
            principal,
            "sample.inventory_adjust",
            "sample",
            str(sample_id),
            before={"quantity": sample["quantity"]},
            after={"quantity": new_quantity},
            metadata={"reason": data["reason"], "delta": delta},
        )
        revalidation = revalidate_sample_requests(self.connection, sample_id, trigger="quantity_adjusted", actor=principal, clock=self.clock)
        return {"sample": self.samples.get(sample_id), "revalidation": revalidation, "replayed": False}


def dispatch_loan_job(connection: sqlite3.Connection, job: dict[str, Any], clock: Clock | None = None) -> dict[str, Any]:
    """Execute a claimed background job; used by the job runner endpoint."""
    payload = json.loads(job["payload_json"])
    if job["job_type"] == "loan.overdue_sweep":
        return LoanWorkflowService(connection, clock).run_overdue_sweep(None, sample_id=payload.get("sample_id"))
    if job["job_type"] == "system.example":
        return {"echo": payload}
    raise ValidationError(f"未知任务类型：{job['job_type']}")
