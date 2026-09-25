from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, ValidationError
from app.core.security import Principal
from app.samples.repository import AnomalyRepository, ApprovalRepository, BatchRepository, LocationRepository, SampleRepository
from app.services.audit import AuditService


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class LocationService:
    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.locations = LocationRepository(connection)
        self.audit = AuditService(connection, self.clock)

    def create(self, principal: Principal, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.write")
        if self.locations.by_code(data["code"]):
            raise ConflictError("位置编码已经存在")
        now = to_storage(self.clock.now())
        location = self.locations.create(data, now)
        self.audit.record(principal, "location.create", "storage_location", str(location["id"]), after=location)
        return self.present(principal, location)

    def present(self, principal: Principal, location: dict[str, Any]) -> dict[str, Any]:
        result = dict(location)
        exact = "*" in principal.permissions or "locations.read_sensitive" in principal.permissions
        if not exact and location["sensitivity"] != "normal":
            result["building"] = "受限区域"
            result["room"] = "***"
            result["cabinet"] = "***"
            result["shelf"] = "***"
            result["code"] = f"MASKED-{location['id']:04d}"
        return result

    def list(self, principal: Principal) -> list[dict[str, Any]]:
        principal.require("samples.read")
        return [self.present(principal, item) for item in self.locations.list()]


class SampleLifecycleService:
    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.samples = SampleRepository(connection)
        self.batches = BatchRepository(connection)
        self.locations = LocationRepository(connection)
        self.audit = AuditService(connection, self.clock)

    def create_batch(self, principal: Principal, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.write")
        now = to_storage(self.clock.now())
        payload = f"sample-batch:{data['batch_code']}:{data['project_code']}"
        batch = self.batches.create(data, principal.user_id, payload, now)
        self.audit.record(principal, "batch.receive", "receipt_batch", str(batch["id"]), after=batch)
        return batch

    def register_sample(self, principal: Principal, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.write")
        if self.samples.by_code(data["sample_code"]):
            raise ConflictError("样品编码已经存在")
        self.batches.get(data["batch_id"])
        if data.get("location_id"):
            self.locations.get(data["location_id"])
        now = to_storage(self.clock.now())
        values = dict(data)
        values.update(lifecycle_state="available", custody_user_id=principal.user_id, lineage_depth=0)
        sample = self.samples.create(values, now)
        self.samples.append_event(sample["id"], "received", principal.user_id, now, to_state="available", details={"batch_id": data["batch_id"]})
        self.batches.update_counts(data["batch_id"], now)
        self.audit.record(principal, "sample.register", "sample", str(sample["id"]), after=sample)
        return sample

    def list_samples(self, principal: Principal, state: str | None, batch_id: int | None) -> list[dict[str, Any]]:
        principal.require("samples.read")
        exact = "*" in principal.permissions or "locations.read_sensitive" in principal.permissions
        result = self.samples.list(state=state, batch_id=batch_id)
        for sample in result:
            if sample.get("location_sensitivity") != "normal" and not exact:
                sample["location_code"] = f"MASKED-{sample['location_id']:04d}" if sample.get("location_id") else None
        return result

    def detail(self, principal: Principal, sample_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        sample = self.samples.get(sample_id)
        sample["events"] = self.samples.events(sample_id)
        sample["children"] = self.samples.children(sample_id)
        if sample.get("location_sensitivity") != "normal" and not (
            "*" in principal.permissions or "locations.read_sensitive" in principal.permissions
        ):
            sample["location_code"] = f"MASKED-{sample['location_id']:04d}" if sample.get("location_id") else None
        return sample

    def aliquot(self, principal: Principal, sample_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.write")
        parent = self.samples.get(sample_id)
        total = round(sum(item["quantity"] for item in data["children"]) + data.get("loss_quantity", 0), 9)
        if abs(total - data["requested_quantity"]) > 1e-6:
            raise ValidationError("子样数量与损耗之和必须等于分装数量")
        if parent["quantity"] - parent["reserved_quantity"] < data["requested_quantity"]:
            raise ConflictError("可用数量不足")
        now = to_storage(self.clock.now())
        updated_parent = self.samples.change_quantity(sample_id, -data["requested_quantity"], parent["version"], now)
        children = []
        for item in data["children"]:
            child = self.samples.create(
                {
                    "sample_code": item["sample_code"],
                    "batch_id": parent["batch_id"],
                    "collection_event_id": parent["collection_event_id"],
                    "parent_sample_id": sample_id,
                    "root_sample_id": parent["root_sample_id"],
                    "sample_type": parent["sample_type"],
                    "quantity": item["quantity"],
                    "unit": parent["unit"],
                    "lifecycle_state": "available",
                    "location_id": item.get("location_id", parent["location_id"]),
                    "custody_user_id": principal.user_id,
                    "lineage_depth": parent["lineage_depth"] + 1,
                },
                now,
            )
            self.samples.append_event(child["id"], "aliquot.created", principal.user_id, now, to_state="available", details={"parent_sample_id": sample_id})
            children.append(child)
        operation_code = data.get("operation_code") or f"ALI-{uuid.uuid4().hex[:12]}"
        self.connection.execute(
            """INSERT INTO aliquot_operations(operation_code,parent_sample_id,requested_quantity,produced_quantity,loss_quantity,operator_user_id,occurred_at,note,created_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (operation_code, sample_id, data["requested_quantity"], sum(item["quantity"] for item in data["children"]), data.get("loss_quantity", 0), principal.user_id, now, data.get("note", ""), now),
        )
        self.samples.append_event(sample_id, "aliquot.source", principal.user_id, now, quantity_delta=-data["requested_quantity"], details={"operation_code": operation_code, "child_ids": [item["id"] for item in children]})
        self.audit.record(principal, "sample.aliquot", "sample", str(sample_id), before=parent, after=updated_parent, metadata={"operation_code": operation_code})
        from app.samples.loans import revalidate_sample_requests

        revalidation = revalidate_sample_requests(self.connection, sample_id, trigger="aliquot", actor=principal, clock=self.clock)
        return {"operation_code": operation_code, "parent": self.samples.get(sample_id), "children": children, "revalidation": revalidation}

    def consume(self, principal: Principal, sample_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.consume")
        sample = self.samples.get(sample_id)
        if sample["lifecycle_state"] in {"destroyed", "pending_destruction", "quarantined"}:
            raise ConflictError("当前状态禁止消耗")
        existing = self.connection.execute(
            "SELECT * FROM consumption_records WHERE sample_id=? AND idempotency_key=?",
            (sample_id, data["idempotency_key"]),
        ).fetchone()
        if existing:
            return {"record": dict(existing), "sample": self.samples.get(sample_id), "replayed": True}
        if sample["quantity"] - sample["reserved_quantity"] < data["quantity"]:
            raise ConflictError("可用数量不足")
        now = to_storage(self.clock.now())
        updated = self.samples.change_quantity(sample_id, -data["quantity"], sample["version"], now)
        new_state = "consumed" if updated["quantity"] == 0 else "partially_consumed"
        updated = self.samples.set_state(sample_id, new_state, updated["version"], now)
        cursor = self.connection.execute(
            """INSERT INTO consumption_records(sample_id,experiment_code,quantity,operator_user_id,idempotency_key,occurred_at,note,created_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            (sample_id, data["experiment_code"], data["quantity"], principal.user_id, data["idempotency_key"], now, data.get("note", ""), now),
        )
        record = dict(self.connection.execute("SELECT * FROM consumption_records WHERE id=?", (cursor.lastrowid,)).fetchone())
        self.samples.append_event(sample_id, "consumed", principal.user_id, now, quantity_delta=-data["quantity"], from_state=sample["lifecycle_state"], to_state=new_state, details={"experiment_code": data["experiment_code"]})
        self.audit.record(principal, "sample.consume", "sample", str(sample_id), before=sample, after=updated)
        from app.samples.loans import revalidate_sample_requests

        revalidation = revalidate_sample_requests(self.connection, sample_id, trigger="consumed", actor=principal, clock=self.clock)
        return {"record": record, "sample": self.samples.get(sample_id), "replayed": False, "revalidation": revalidation}


class ApprovalService:
    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.approvals = ApprovalRepository(connection)
        self.audit = AuditService(connection, self.clock)

    def create(self, principal: Principal, data: dict[str, Any]) -> dict[str, Any]:
        if data["action_type"] == "destruction":
            principal.require("samples.destroy")
        elif data["action_type"] == "inventory_adjustment":
            principal.require("inventory.manage")
        else:
            principal.require("samples.write")
        now_dt = self.clock.now()
        values = dict(data)
        values["expires_at"] = values.get("expires_at") or to_storage(now_dt + timedelta(days=3))
        request_code = values.get("request_code") or f"APR-{uuid.uuid4().hex[:12]}"
        request = self.approvals.create(values, principal.user_id, request_code, to_storage(now_dt))
        self.audit.record(principal, "approval.request", "approval_request", str(request["id"]), after=request)
        revalidation = None
        if request["action_type"] == "destruction" and request["resource_type"] == "sample":
            from app.samples.loans import revalidate_sample_requests

            revalidation = revalidate_sample_requests(
                self.connection, request["resource_id"], trigger="destruction_requested", actor=principal, clock=self.clock
            )
        return {**request, "revalidation": revalidation}

    def decide(self, principal: Principal, request_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("approvals.decide")
        before = self.approvals.get(request_id)
        result = self.approvals.decide(request_id, principal.user_id, data["decision"], data.get("comment", ""), to_storage(self.clock.now()))
        self.audit.record(principal, "approval.decide", "approval_request", str(request_id), before=before, after=result)
        if result["action_type"] == "destruction" and result["resource_type"] == "sample":
            from app.samples.loans import revalidate_sample_requests

            result["revalidation"] = revalidate_sample_requests(
                self.connection, result["resource_id"], trigger=f"destruction_{result['state']}", actor=principal, clock=self.clock
            )
        return result


class AnomalyService:
    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.anomalies = AnomalyRepository(connection)
        self.samples = SampleRepository(connection)
        self.audit = AuditService(connection, self.clock)

    def create(self, principal: Principal, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("anomalies.manage")
        if not data.get("sample_id") and not data.get("batch_id"):
            raise ValidationError("异常必须关联样品或接收批次")
        if data.get("sample_id"):
            self.samples.get(data["sample_id"])
        case_code = data.get("case_code") or f"ANM-{uuid.uuid4().hex[:12]}"
        case = self.anomalies.create(data, principal.user_id, case_code, to_storage(self.clock.now()))
        self.audit.record(principal, "anomaly.create", "anomaly_case", str(case["id"]), after=case)
        return case

    def list(self, principal: Principal, state: str | None) -> list[dict[str, Any]]:
        principal.require("samples.read")
        return self.anomalies.list(state)
