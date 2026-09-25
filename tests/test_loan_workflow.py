from __future__ import annotations


def _make_sample(client, admin, quantity=100, code="WF-SAMPLE", batch_code="WF-BATCH", location_code="WF-LOC"):
    location = client.post(
        "/api/samples/locations",
        headers=admin["headers"],
        json={
            "code": location_code,
            "building": "样品楼",
            "room": "常温库",
            "cabinet": "三号柜",
            "shelf": "二层",
            "sensitivity": "normal",
            "capacity_units": 50,
        },
    )
    assert location.status_code == 201, location.text
    batch = client.post(
        "/api/samples/batches",
        headers=admin["headers"],
        json={"batch_code": batch_code, "project_code": "WF", "expected_count": 1},
    )
    assert batch.status_code == 201, batch.text
    sample = client.post(
        "/api/samples",
        headers=admin["headers"],
        json={
            "sample_code": code,
            "batch_id": batch.json()["id"],
            "sample_type": "水样",
            "quantity": quantity,
            "unit": "mL",
            "location_id": location.json()["id"],
        },
    )
    assert sample.status_code == 201, sample.text
    return sample.json()


def _make_user(client, admin, username, role_codes):
    password = "Researcher!1"
    created = client.post(
        "/api/users",
        headers=admin["headers"],
        json={
            "username": username,
            "password": password,
            "display_name": username,
            "role_codes": role_codes,
        },
    )
    assert created.status_code == 201, created.text
    login = client.post(
        "/api/auth/login",
        json={"username": username, "password": password, "client_label": "tests"},
    )
    assert login.status_code == 200, login.text
    body = login.json()
    return {"id": body["user"]["id"], "headers": {"Authorization": f"Bearer {body['token']}"}}


def _apply(client, headers, sample_id, quantity, priority=100, due="2026-12-31T00:00:00+00:00", borrower=None):
    payload = {
        "sample_id": sample_id,
        "quantity": quantity,
        "requested_due_at": due,
        "priority": priority,
    }
    if borrower is not None:
        payload["borrower_user_id"] = borrower
    resp = client.post("/api/loans/requests", headers=headers, json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


def test_apply_does_not_reserve_until_approved(client, admin):
    sample = _make_sample(client, admin)
    request = _apply(client, admin["headers"], sample["id"], 30)
    assert request["state"] == "queued"
    assert request["fulfilled_loan_id"] is None
    status = client.get(f"/api/loans/samples/{sample['id']}/reservation", headers=admin["headers"]).json()
    assert status["reserved_quantity"] == 0
    assert status["available_quantity"] == 100
    assert len(status["queue"]) == 1


def test_priority_ordering_and_atomic_occupation(client, admin):
    sample = _make_sample(client, admin)
    r_low = _apply(client, admin["headers"], sample["id"], 60, priority=100)
    r_high = _apply(client, admin["headers"], sample["id"], 60, priority=200)
    r_mid = _apply(client, admin["headers"], sample["id"], 40, priority=50)

    # 高优先级先获批并占用 60。
    high = client.post(f"/api/loans/requests/{r_high['id']}/approve", headers=admin["headers"])
    assert high.status_code == 200
    assert high.json()["fulfilled_loan_id"] is not None
    high_loan = high.json()["fulfilled_loan_id"]

    status = client.get(f"/api/loans/samples/{sample['id']}/reservation", headers=admin["headers"]).json()
    assert status["reserved_quantity"] == 60
    assert status["available_quantity"] == 40

    # 低优先级（排队位次在前）获批但可借不足，保持已批准候补。
    low = client.post(f"/api/loans/requests/{r_low['id']}/approve", headers=admin["headers"])
    assert low.json()["state"] == "approved"
    assert low.json()["fulfilled_loan_id"] is None

    # 即使可借 40 恰好满足中优先级，也不得越过排在前面的 60 候补。
    mid = client.post(f"/api/loans/requests/{r_mid['id']}/approve", headers=admin["headers"])
    assert mid.json()["state"] == "approved"
    assert mid.json()["fulfilled_loan_id"] is None

    # 归还 20：可借变为 60，推进队首 60 候补；40 候补继续等待。
    ret = client.post(f"/api/loans/{high_loan}/returns", headers=admin["headers"], json={"quantity": 20})
    assert ret.status_code == 200
    low_after = client.get(f"/api/loans/requests/{r_low['id']}", headers=admin["headers"]).json()
    mid_after = client.get(f"/api/loans/requests/{r_mid['id']}", headers=admin["headers"]).json()
    assert low_after["fulfilled_loan_id"] is not None
    assert mid_after["fulfilled_loan_id"] is None

    # 全部归还高优先级借用后，40 候补才被满足。
    client.post(f"/api/loans/{high_loan}/returns", headers=admin["headers"], json={"quantity": 40})
    mid_final = client.get(f"/api/loans/requests/{r_mid['id']}", headers=admin["headers"]).json()
    assert mid_final["fulfilled_loan_id"] is not None


def test_renewal_cannot_jump_over_waiting_request(client, admin):
    researcher = _make_user(client, admin, "renewuser", ["researcher"])
    sample = _make_sample(client, admin)
    # 现有借用 100（研究员为借用人）。
    request = _apply(client, admin["headers"], sample["id"], 100, borrower=researcher["id"])
    approved = client.post(f"/api/loans/requests/{request['id']}/approve", headers=admin["headers"]).json()
    loan_id = approved["fulfilled_loan_id"]

    # 另一个课题组提交等待中的借用申请。
    waiting = _apply(client, admin["headers"], sample["id"], 50, priority=200)

    # 借用人申请续借。
    renewal = client.post(
        f"/api/loans/{loan_id}/renewals",
        headers=researcher["headers"],
        json={"requested_due_at": "2027-06-30T00:00:00+00:00", "priority": 50},
    )
    assert renewal.status_code == 201, renewal.text
    renewal_id = renewal.json()["id"]

    # 即使续借优先级更低/更高，前方有等待借用时审批被拒绝。
    blocked = client.post(f"/api/loans/requests/{renewal_id}/approve", headers=admin["headers"])
    assert blocked.status_code == 409
    assert blocked.json()["error"]["context"]["blocking_request_id"] == waiting["id"]

    # 驳回等待申请后，续借可以获批并顺延到期时间。
    client.post(
        f"/api/loans/requests/{waiting['id']}/reject",
        headers=admin["headers"],
        json={"reason": "课题组撤回需求"},
    )
    ok = client.post(f"/api/loans/requests/{renewal_id}/approve", headers=admin["headers"])
    assert ok.status_code == 200
    loan = client.get(f"/api/loans/{loan_id}", headers=admin["headers"]).json()
    assert loan["due_at"] == "2027-06-30T00:00:00+00:00"
    assert loan["renewals_count"] == 1


def test_overdue_scan_is_rerunnable_and_records_once(client, admin):
    sample = _make_sample(client, admin)
    loan = client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={
            "sample_id": sample["id"],
            "borrower_user_id": admin["body"]["user"]["id"],
            "quantity": 40,
            "due_at": "2020-01-01T00:00:00+00:00",
        },
    )
    assert loan.status_code == 201, loan.text
    loan_id = loan.json()["id"]

    first = client.post("/api/loans/overdue/scan", headers=admin["headers"])
    assert first.status_code == 200
    assert first.json()["generated"] == 1
    # 任务可重跑，同一逾期周期只生成一条召回记录。
    second = client.post("/api/loans/overdue/scan", headers=admin["headers"])
    assert second.json()["generated"] == 0

    loan_after = client.get(f"/api/loans/{loan_id}", headers=admin["headers"]).json()
    assert loan_after["state"] == "overdue"
    assert len(loan_after["recalls"]) == 1
    assert loan_after["recalls"][0]["outstanding_quantity"] == 40

    # 归还后召回记录被标记为已解决。
    client.post(f"/api/loans/{loan_id}/returns", headers=admin["headers"], json={"quantity": 40})
    resolved = client.get("/api/loans/recalls", headers=admin["headers"], params={"resolved": True}).json()
    assert any(item["loan_id"] == loan_id for item in resolved)


def test_quarantine_invalidates_open_requests_but_keeps_active_loan(client, admin):
    sample = _make_sample(client, admin)
    active_req = _apply(client, admin["headers"], sample["id"], 30)
    active = client.post(f"/api/loans/requests/{active_req['id']}/approve", headers=admin["headers"]).json()
    active_loan = active["fulfilled_loan_id"]

    waiting = _apply(client, admin["headers"], sample["id"], 20)
    quarantined = client.post(
        f"/api/samples/{sample['id']}/quarantine",
        headers=admin["headers"],
        json={"reason": "疑似污染待复检"},
    )
    assert quarantined.status_code == 200
    waiting_after = client.get(f"/api/loans/requests/{waiting['id']}", headers=admin["headers"]).json()
    assert waiting_after["state"] == "invalidated"
    assert "隔离" in waiting_after["invalidate_reason"] or "quarantin" in waiting_after["invalidate_reason"].lower()

    # 已占用的借用与预留保留，不被隔离释放。
    status = client.get(f"/api/loans/samples/{sample['id']}/reservation", headers=admin["headers"]).json()
    assert status["reserved_quantity"] == 30
    loan = client.get(f"/api/loans/{active_loan}", headers=admin["headers"]).json()
    assert loan["state"] == "active"

    # 隔离期间新申请不能获批。
    blocked_apply = _apply(client, admin["headers"], sample["id"], 5)
    blocked = client.post(f"/api/loans/requests/{blocked_apply['id']}/approve", headers=admin["headers"])
    assert blocked.status_code == 409


def test_destruction_approval_invalidates_open_requests(client, admin):
    sample = _make_sample(client, admin)
    waiting = _apply(client, admin["headers"], sample["id"], 10)
    approval = client.post(
        "/api/samples/approvals",
        headers=admin["headers"],
        json={"action_type": "destruction", "resource_type": "sample", "resource_id": sample["id"], "payload": {"quantity": 10}},
    )
    assert approval.status_code == 201
    waiting_after = client.get(f"/api/loans/requests/{waiting['id']}", headers=admin["headers"]).json()
    assert waiting_after["state"] == "invalidated"

    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["lifecycle_state"] == "pending_destruction"


def test_quantity_change_revalidates_oversized_requests(client, admin):
    sample = _make_sample(client, admin)
    too_big = _apply(client, admin["headers"], sample["id"], 80)
    fit = _apply(client, admin["headers"], sample["id"], 30)
    # 消耗 50，总量降到 50。
    consumed = client.post(
        f"/api/samples/{sample['id']}/consumptions",
        headers=admin["headers"],
        json={"experiment_code": "EXP-Q", "quantity": 50, "idempotency_key": "wf-consume-1"},
    )
    assert consumed.status_code == 201, consumed.text
    assert client.get(f"/api/loans/requests/{too_big['id']}", headers=admin["headers"]).json()["state"] == "invalidated"
    assert client.get(f"/api/loans/requests/{fit['id']}", headers=admin["headers"]).json()["state"] == "queued"


def test_reservation_ledger_records_inventory_impact(client, admin):
    sample = _make_sample(client, admin)
    request = _apply(client, admin["headers"], sample["id"], 25)
    client.post(f"/api/loans/requests/{request['id']}/approve", headers=admin["headers"])
    loan_id = request["id"]
    # 通过申请详情找到实际借用。
    detail = client.get(f"/api/loans/requests/{request['id']}", headers=admin["headers"]).json()
    loan_id = detail["fulfilled_loan_id"]
    client.post(f"/api/loans/{loan_id}/returns", headers=admin["headers"], json={"quantity": 10})

    status = client.get(f"/api/loans/samples/{sample['id']}/reservation", headers=admin["headers"]).json()
    reasons = [entry["reason"] for entry in status["ledger"]]
    assert any("borrow.fulfill" in reason for reason in reasons)
    assert any("return" in reason for reason in reasons)
    assert status["reserved_quantity"] == 15


def test_researcher_permissions_and_denied_audit(client, admin):
    researcher = _make_user(client, admin, "permuser", ["researcher"])
    sample = _make_sample(client, admin)

    # 研究员可以为自己申请。
    own = _apply(client, researcher["headers"], sample["id"], 5)
    assert own["applicant_user_id"] == researcher["id"]

    # 研究员不能审批。
    denied = client.post(f"/api/loans/requests/{own['id']}/approve", headers=researcher["headers"])
    assert denied.status_code == 403

    # 研究员不能为他人申请。
    other_payload = {
        "sample_id": sample["id"],
        "borrower_user_id": admin["body"]["user"]["id"],
        "quantity": 5,
        "requested_due_at": "2026-12-31T00:00:00+00:00",
    }
    forbidden = client.post("/api/loans/requests", headers=researcher["headers"], json=other_payload)
    assert forbidden.status_code == 403

    # 权限拒绝可通过审计 API 查询。
    audit = client.get(
        "/api/audit",
        headers=admin["headers"],
        params={"outcome": "denied", "resource_type": "api_endpoint", "size": 50},
    )
    assert audit.status_code == 200
    assert audit.json()["total"] >= 2
    assert any("approve" in (item.get("metadata_json") or "") for item in audit.json()["data"])


def test_requester_can_cancel_own_waiting_request(client, admin):
    researcher = _make_user(client, admin, "canceluser", ["researcher"])
    sample = _make_sample(client, admin)
    own = _apply(client, researcher["headers"], sample["id"], 12)
    cancelled = client.post(f"/api/loans/requests/{own['id']}/cancel", headers=researcher["headers"])
    assert cancelled.status_code == 200
    assert cancelled.json()["state"] == "cancelled"
