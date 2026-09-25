from __future__ import annotations

from datetime import UTC, datetime, timedelta


def _setup_sample(client, admin, quantity=100):
    location = client.post(
        "/api/samples/locations",
        headers=admin["headers"],
        json={
            "code": "LW-LOC",
            "building": "样品楼",
            "room": "常温库",
            "cabinet": "一号柜",
            "shelf": "一层",
            "sensitivity": "normal",
            "capacity_units": 50,
        },
    ).json()
    batch = client.post(
        "/api/samples/batches",
        headers=admin["headers"],
        json={"batch_code": "LW-BATCH", "project_code": "LW", "expected_count": 1},
    ).json()
    sample = client.post(
        "/api/samples",
        headers=admin["headers"],
        json={
            "sample_code": "LW-SAMPLE",
            "batch_id": batch["id"],
            "sample_type": "水样",
            "quantity": quantity,
            "unit": "mL",
            "location_id": location["id"],
        },
    ).json()
    return location, batch, sample


def _make_researcher(client, admin, username):
    user = client.post(
        "/api/users",
        headers=admin["headers"],
        json={"username": username, "password": "Research!23456", "display_name": username, "role_codes": ["researcher"]},
    )
    assert user.status_code == 201, user.text
    login = client.post(
        "/api/auth/login",
        json={"username": username, "password": "Research!23456", "client_label": "tests"},
    )
    assert login.status_code == 200, login.text
    token = login.json()["token"]
    return {"headers": {"Authorization": f"Bearer {token}"}, "id": user.json()["id"]}


def _iso(dt):
    return dt.astimezone(UTC).isoformat(timespec="seconds")


def test_requests_queue_by_priority_and_submission_order(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.queue")
    bob = _make_researcher(client, admin, "bob.queue")
    carol = _make_researcher(client, admin, "carol.queue")

    # alice takes 60 directly
    loan = client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": sample["id"], "borrower_user_id": alice["id"], "quantity": 60, "due_at": _iso(datetime(2026, 12, 1, tzinfo=UTC))},
    )
    assert loan.status_code == 201, loan.text

    # bob requests 50 (cannot fit: 40 free), carol requests 30 (fits)
    r_bob = client.post(
        "/api/samples/loan-requests",
        headers=bob["headers"],
        json={"sample_id": sample["id"], "quantity": 50, "priority": 2},
    )
    assert r_bob.status_code == 201, r_bob.text
    assert r_bob.json()["state"] == "waiting"

    r_carol = client.post(
        "/api/samples/loan-requests",
        headers=carol["headers"],
        json={"sample_id": sample["id"], "quantity": 30, "priority": 3},
    )
    assert r_carol.json()["state"] == "pending"

    # a higher-priority urgent request that still doesn't fit stays waiting, does not demote carol
    dave = _make_researcher(client, admin, "dave.queue")
    r_dave = client.post(
        "/api/samples/loan-requests",
        headers=dave["headers"],
        json={"sample_id": sample["id"], "quantity": 80, "priority": 1},
    )
    assert r_dave.json()["state"] == "waiting"

    # cannot approve a waiting request out of order
    blocked = client.post(f"/api/samples/loan-requests/{r_bob.json()['id']}/approve", headers=admin["headers"], json={})
    assert blocked.status_code == 409

    # approving the fulfillable carol request reserves atomically
    approved = client.post(f"/api/samples/loan-requests/{r_carol.json()['id']}/approve", headers=admin["headers"], json={})
    assert approved.status_code == 200, approved.text
    assert approved.json()["request"]["state"] == "fulfilled"
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["reserved_quantity"] == 90


def test_return_releases_and_auto_advances_waitlist_in_priority_order(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.ret")
    bob = _make_researcher(client, admin, "bob.ret")
    carol = _make_researcher(client, admin, "carol.ret")

    loan = client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": sample["id"], "borrower_user_id": alice["id"], "quantity": 100, "due_at": _iso(datetime(2026, 12, 1, tzinfo=UTC))},
    ).json()

    urgent = client.post(
        "/api/samples/loan-requests",
        headers=bob["headers"],
        json={"sample_id": sample["id"], "quantity": 40, "priority": 1},
    ).json()
    normal = client.post(
        "/api/samples/loan-requests",
        headers=carol["headers"],
        json={"sample_id": sample["id"], "quantity": 60, "priority": 3},
    ).json()
    assert urgent["state"] == normal["state"] == "waiting"

    # partial return of 50 frees enough for the urgent (40) request; normal (60) still waits
    result = client.post(f"/api/samples/loans/{loan['id']}/returns", headers=admin["headers"], json={"quantity": 50})
    assert result.status_code == 200, result.text
    advanced = {item["request_id"] for item in result.json()["auto_fulfilled"]}
    assert advanced == {urgent["id"]}

    requests = client.get(f"/api/samples/loan-requests?sample_id={sample['id']}", headers=admin["headers"]).json()
    states = {item["id"]: item["state"] for item in requests}
    assert states[urgent["id"]] == "fulfilled"
    assert states[normal["id"]] == "waiting"

    # returning the rest fulfills the normal request
    result = client.post(f"/api/samples/loans/{loan['id']}/returns", headers=admin["headers"], json={"quantity": 50})
    advanced = {item["request_id"] for item in result.json()["auto_fulfilled"]}
    assert advanced == {normal["id"]}


def test_renewal_cannot_jump_over_higher_priority_waiting(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.ren")
    bob = _make_researcher(client, admin, "bob.ren")

    due = _iso(datetime(2026, 10, 1, tzinfo=UTC))
    loan = client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": sample["id"], "borrower_user_id": alice["id"], "quantity": 100, "due_at": due},
    ).json()
    # higher-priority request queued
    queued = client.post(
        "/api/samples/loan-requests",
        headers=bob["headers"],
        json={"sample_id": sample["id"], "quantity": 100, "priority": 1},
    ).json()
    assert queued["state"] == "waiting"

    renewal = client.post(
        f"/api/samples/loans/{loan['id']}/renewals",
        headers=alice["headers"],
        json={"new_due_at": _iso(datetime(2026, 11, 1, tzinfo=UTC)), "reason": "实验尚未完成"},
    )
    assert renewal.status_code == 201, renewal.text
    denied = client.post(f"/api/samples/loan-renewals/{renewal.json()['id']}/approve", headers=admin["headers"], json={})
    assert denied.status_code == 409
    assert queued["id"] in denied.json()["error"]["context"]["blocking_request_ids"]

    # once the waiting request is cancelled, renewal succeeds
    client.post(f"/api/samples/loan-requests/{queued['id']}/cancel", headers=bob["headers"])
    approved = client.post(f"/api/samples/loan-renewals/{renewal.json()['id']}/approve", headers=admin["headers"], json={})
    assert approved.status_code == 200, approved.text
    assert approved.json()["renewal"]["state"] == "approved"
    assert approved.json()["loan"]["due_at"].startswith("2026-11-01")


def test_overdue_sweep_is_rerunnable_and_creates_one_recall_per_episode(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.over")
    loan = client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": sample["id"], "borrower_user_id": alice["id"], "quantity": 30, "due_at": _iso(datetime(2026, 9, 1, tzinfo=UTC))},
    ).json()

    first = client.post("/api/samples/loans/overdue-sweep", headers=admin["headers"])
    assert first.status_code == 200, first.text
    assert first.json()["loans_marked_overdue"] == 1
    assert first.json()["recalls_created"] == 1
    recall_id = first.json()["recalls"][0]["id"]

    # rerun creates no duplicate recall and the loan is overdue
    second = client.post("/api/samples/loans/overdue-sweep", headers=admin["headers"])
    assert second.json()["recalls_created"] == 0

    loan_detail = client.get(f"/api/samples/loans/{loan['id']}", headers=admin["headers"]).json()
    assert loan_detail["state"] == "overdue"
    assert len([r for r in loan_detail["recalls"] if r["state"] == "open"]) == 1

    # after full return the recall is closed and a later overdue episode gets a new recall
    client.post(f"/api/samples/loans/{loan['id']}/returns", headers=admin["headers"], json={"quantity": 30})
    reloan = client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": sample["id"], "borrower_user_id": alice["id"], "quantity": 10, "due_at": _iso(datetime(2026, 9, 10, tzinfo=UTC))},
    ).json()
    third = client.post("/api/samples/loans/overdue-sweep", headers=admin["headers"]).json()
    assert third["recalls_created"] == 1
    assert third["recalls"][0]["id"] != recall_id
    recalls = client.get("/api/samples/loan-recalls", headers=admin["headers"]).json()
    by_loan = {}
    for recall in recalls:
        by_loan.setdefault(recall["loan_id"], []).append(recall)
    # each loan has exactly one recall episode after the two sweeps, and the old one is closed
    assert len(by_loan[loan["id"]]) == 1 and by_loan[loan["id"]][0]["state"] == "returned"
    assert len(by_loan[reloan["id"]]) == 1 and by_loan[reloan["id"]][0]["state"] == "open"


def test_overdue_sweep_runs_as_background_job(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.job")
    client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": sample["id"], "borrower_user_id": alice["id"], "quantity": 10, "due_at": _iso(datetime(2026, 9, 1, tzinfo=UTC))},
    )
    enqueue = client.post("/api/system/jobs/loan-overdue-sweep", headers=admin["headers"])
    assert enqueue.status_code == 201, enqueue.text
    # duplicate enqueue while pending is de-duplicated
    again = client.post("/api/system/jobs/loan-overdue-sweep", headers=admin["headers"])
    assert again.json()["id"] == enqueue.json()["id"]

    ran = client.post("/api/system/jobs/run-next?worker=w1", headers=admin["headers"])
    assert ran.status_code == 200, ran.text
    assert ran.json()["claimed"] is True
    assert ran.json()["result"]["recalls_created"] == 1
    # after completion the sweep can be enqueued again
    rerun = client.post("/api/system/jobs/loan-overdue-sweep", headers=admin["headers"])
    assert rerun.json()["id"] != enqueue.json()["id"]


def test_quarantine_blocks_and_release_restores_requests(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.q")
    request = client.post(
        "/api/samples/loan-requests",
        headers=alice["headers"],
        json={"sample_id": sample["id"], "quantity": 40, "priority": 2},
    ).json()
    assert request["state"] == "pending"

    quarantined = client.post(f"/api/samples/{sample['id']}/quarantine", headers=admin["headers"], json={"reason": "疑似污染"})
    assert quarantined.status_code == 200, quarantined.text
    assert quarantined.json()["revalidation"]["blocked"] == [request["id"]]
    detail = client.get(f"/api/samples/loan-requests/{request['id']}", headers=admin["headers"]).json()
    assert detail["state"] == "blocked"
    assert detail["block_reason"] == "sample_quarantined"

    # cannot approve a blocked request
    denied = client.post(f"/api/samples/loan-requests/{request['id']}/approve", headers=admin["headers"], json={})
    assert denied.status_code == 409

    released = client.post(f"/api/samples/{sample['id']}/quarantine/release", headers=admin["headers"])
    assert released.status_code == 200, released.text
    assert released.json()["revalidation"]["unblocked"] == [request["id"]]
    detail = client.get(f"/api/samples/loan-requests/{request['id']}", headers=admin["headers"]).json()
    assert detail["state"] == "pending"


def test_destruction_approval_blocks_open_requests(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.d")
    request = client.post(
        "/api/samples/loan-requests",
        headers=alice["headers"],
        json={"sample_id": sample["id"], "quantity": 40},
    ).json()

    approval = client.post(
        "/api/samples/approvals",
        headers=admin["headers"],
        json={"action_type": "destruction", "resource_type": "sample", "resource_id": sample["id"], "payload": {"quantity": 10}},
    )
    assert approval.status_code == 201
    assert approval.json()["revalidation"]["blocked"] == [request["id"]]

    # new applications are refused while destruction approval is open
    extra = client.post(
        "/api/samples/loan-requests",
        headers=alice["headers"],
        json={"sample_id": sample["id"], "quantity": 5},
    )
    assert extra.status_code == 409

    # an independent approver rejects the destruction request -> loan requests return to the queue
    approver = client.post(
        "/api/users",
        headers=admin["headers"],
        json={"username": "risk.officer", "password": "Approver!23456", "display_name": "风险官", "role_codes": ["approver"]},
    )
    assert approver.status_code == 201, approver.text
    officer_login = client.post(
        "/api/auth/login",
        json={"username": "risk.officer", "password": "Approver!23456", "client_label": "tests"},
    ).json()
    officer_headers = {"Authorization": f"Bearer {officer_login['token']}"}
    decision = client.post(
        f"/api/samples/approvals/{approval.json()['id']}/decisions",
        headers=officer_headers,
        json={"decision": "reject", "comment": "不予销毁"},
    )
    assert decision.status_code == 200, decision.text
    assert decision.json()["revalidation"]["unblocked"] == [request["id"]]
    detail = client.get(f"/api/samples/loan-requests/{request['id']}", headers=admin["headers"]).json()
    assert detail["state"] == "pending"


def test_quantity_reduction_blocks_oversized_requests(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.adj")
    request = client.post(
        "/api/samples/loan-requests",
        headers=alice["headers"],
        json={"sample_id": sample["id"], "quantity": 80},
    ).json()
    assert request["state"] == "pending"

    adjusted = client.post(
        f"/api/samples/{sample['id']}/inventory-adjustment",
        headers=admin["headers"],
        json={"new_quantity": 50, "reason": "盘点核减"},
    )
    assert adjusted.status_code == 200, adjusted.text
    assert adjusted.json()["revalidation"]["blocked"] == [request["id"]]

    # increasing the stock back so the request fits unblocks and re-queues it
    restored = client.post(
        f"/api/samples/{sample['id']}/inventory-adjustment",
        headers=admin["headers"],
        json={"new_quantity": 100, "reason": "补录入库"},
    )
    assert restored.json()["revalidation"]["unblocked"] == [request["id"]]


def test_researcher_permissions_are_scoped_and_denials_audited(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.perm")
    bob = _make_researcher(client, admin, "bob.perm")

    own = client.post(
        "/api/samples/loan-requests",
        headers=alice["headers"],
        json={"sample_id": sample["id"], "quantity": 10},
    ).json()
    # researchers cannot see each other's requests
    peek = client.get(f"/api/samples/loan-requests/{own['id']}", headers=bob["headers"])
    assert peek.status_code == 403
    # researchers cannot approve
    forbidden = client.post(f"/api/samples/loan-requests/{own['id']}/approve", headers=alice["headers"], json={})
    assert forbidden.status_code == 403

    events = client.get(
        "/api/audit?resource_type=loan_request&action=loan.request.create&size=20",
        headers=admin["headers"],
    ).json()
    assert events["total"] >= 1

    denials = client.get("/api/audit?action=permission.denied&outcome=denied&size=20", headers=admin["headers"]).json()
    assert denials["total"] >= 1
    paths = [item["resource_id"] for item in denials["data"]]
    assert any(str(own["id"]) in path for path in paths)


def test_request_lifecycle_events_are_queryable(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.evt")
    request = client.post(
        "/api/samples/loan-requests",
        headers=alice["headers"],
        json={"sample_id": sample["id"], "quantity": 20},
    ).json()
    client.post(f"/api/samples/loan-requests/{request['id']}/approve", headers=admin["headers"], json={})
    detail = client.get(f"/api/samples/loan-requests/{request['id']}", headers=admin["headers"]).json()
    event_types = [event["event_type"] for event in detail["events"]]
    assert "request.created" in event_types
    assert "request.fulfilled" in event_types
    assert detail["loan"] is not None


def test_availability_summary_reflects_queue_and_reservations(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.av")
    bob = _make_researcher(client, admin, "bob.av")

    client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": sample["id"], "borrower_user_id": alice["id"], "quantity": 60, "due_at": _iso(datetime(2026, 12, 1, tzinfo=UTC))},
    )
    fitting = client.post(
        "/api/samples/loan-requests",
        headers=bob["headers"],
        json={"sample_id": sample["id"], "quantity": 30, "priority": 2},
    ).json()
    waiting = client.post(
        "/api/samples/loan-requests",
        headers=alice["headers"],
        json={"sample_id": sample["id"], "quantity": 40, "priority": 3},
    ).json()
    assert fitting["state"] == "pending" and waiting["state"] == "waiting"

    summary = client.get(f"/api/samples/{sample['id']}/loan-availability", headers=admin["headers"]).json()
    assert summary["reserved_quantity"] == 60
    assert summary["loanable_quantity"] == 40
    assert summary["pending_demand_quantity"] == 30
    assert summary["waiting_demand_quantity"] == 40
    assert summary["active_borrowed_quantity"] == 60


def test_partial_return_frees_stock_without_advancing_unfulfillable_head(client, admin):
    _, _, sample = _setup_sample(client, admin, quantity=100)
    alice = _make_researcher(client, admin, "alice.pr")
    bob = _make_researcher(client, admin, "bob.pr")
    loan = client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": sample["id"], "borrower_user_id": alice["id"], "quantity": 80, "due_at": _iso(datetime(2026, 12, 1, tzinfo=UTC))},
    ).json()
    big = client.post(
        "/api/samples/loan-requests",
        headers=bob["headers"],
        json={"sample_id": sample["id"], "quantity": 50, "priority": 1},
    ).json()
    assert big["state"] == "waiting"

    # return 10 of 80 -> 30 free; the 50-request still cannot be granted and stays waiting
    result = client.post(f"/api/samples/loans/{loan['id']}/returns", headers=admin["headers"], json={"quantity": 10}).json()
    assert result["auto_fulfilled"] == []
    detail = client.get(f"/api/samples/loan-requests/{big['id']}", headers=admin["headers"]).json()
    assert detail["state"] == "waiting"
    assert result["sample"]["reserved_quantity"] == 70
