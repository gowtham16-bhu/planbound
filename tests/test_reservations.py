"""Effects nobody can see until they are certain, and a window to cancel before the point of no return."""

import asyncio

import pytest

from bench.scenarios import pin, refund_manifest, refund_plan
from planbound.manifest import Manifest


def reservation_manifest(env, hold_seconds=0.0):
    data = refund_manifest(env.up.catalog, auto_max=1e9).to_json()
    data["tools"]["billing.hold_refund"] = {
        "state": "compensatable", "idempotency_keys": True, "output_labels": ["financial"],
        "description": "Reserve a refund.", "cost": "$input.amount",
        "limits": {"max_per_call": 10000, "max_per_plan": 200000},
        "compensator": {"tool": "billing.cancel_refund", "args": {"refund_id": "$output.refund_id"}},
        "confirm": {"tool": "billing.confirm_refund", "args": {"refund_id": "$output.refund_id"}},
    }
    data["tools"]["billing.confirm_refund"] = {"state": "idempotent_write", "description": "Issue a held refund."}
    if hold_seconds:
        data["tools"]["email.send_email"]["hold_seconds"] = hold_seconds
    for entry in data["tools"].values():
        entry.pop("contract_sha256", None)
    return pin(data, env.up.catalog)


def reservation_plan():
    plan = refund_plan()
    plan["nodes"][4]["call"]["tool"] = "billing.hold_refund"
    return plan


def eligible_ids(world):
    return sorted(o.order_id for o in world.orders.values()
                  if o.status == "overdue" and o.amount < 5000 and o.order_id not in world.disputed)


async def test_reserved_refunds_become_visible_only_after_everything_succeeded(env):
    want = eligible_ids(env.world)
    res = await env.runtime(reservation_manifest(env)).submit(reservation_plan())
    assert res["status"] == "completed", res
    assert res["confirmation"]["confirmed"] == len(want)
    assert env.world.refunded() == want and sorted(env.world.ever_visible) == want


async def test_a_failure_before_the_point_of_no_return_is_never_seen_by_anyone(env):
    env.world.fail_email = True  # the email (point of no return) fails after every refund was reserved
    res = await env.runtime(reservation_manifest(env)).submit(reservation_plan())
    assert res["status"] == "failed_compensated", res
    assert env.world.ever_visible == set()  # with plain refunds, these would have been visible
    assert all(r.status == "cancelled" for r in env.world.refunds.values()) and env.world.refunds


async def test_unconfirmed_reservations_are_confirmed_by_recover_not_undone(env):
    want = eligible_ids(env.world)
    env.world.flaky_revoke = {"confirm_refund": 100}  # confirmations down after the email went out
    m = reservation_manifest(env)
    res = await env.runtime(m, compensation_attempts=1).submit(reservation_plan())
    assert res["status"] == "failed_partial" and len(env.world.emails) == 1, res
    env.world.flaky_revoke = {}
    reports = await env.runtime(m).recover()
    assert reports[0]["status"] == "completed", reports
    assert env.world.refunded() == want  # confirmed, never undone: the email already told the customer
    assert all(r.status == "issued" for r in env.world.refunds.values())


async def test_cancel_during_the_hold_before_an_email_undoes_everything(env):
    m = reservation_manifest(env, hold_seconds=5)
    rt = env.runtime(m, lease_ttl_s=60)
    task = asyncio.create_task(rt.submit(reservation_plan()))
    for _ in range(200):  # wait until the plan sits in the hold window before the email
        await asyncio.sleep(0.01)
        running = env.store.plans_with_status("running")
        if running and len([r for r in env.world.refunds.values() if r.status == "held"]) == \
                len(eligible_ids(env.world)):
            break
    [plan_id] = running
    assert rt.cancel(plan_id)["status"] == "cancel_requested"
    res = await task
    assert res["status"] == "cancelled" and res["failure"]["code"] == "E_CANCELLED", res
    assert env.world.emails == [] and env.world.ever_visible == set()


async def test_cancel_is_refused_once_past_the_point_of_no_return(env):
    res = await env.runtime(reservation_manifest(env)).submit(reservation_plan())
    rt = env.runtime(reservation_manifest(env))
    assert rt.cancel(res["plan_id"])["status"] == "error"


async def test_cancel_drops_a_plan_waiting_for_approval(env):
    m = reservation_manifest(env)
    m.auto_approve_max_cost = 0
    rt = env.runtime(m)
    res = await rt.submit(reservation_plan())
    assert res["status"] == "awaiting_approval"
    assert rt.cancel(res["plan_id"])["status"] == "cancelled"
    assert env.store.get_plan(res["plan_id"])["status"] == "cancelled"


def test_reservation_and_hold_need_the_right_kind_of_tool():
    with pytest.raises(ValueError, match="only a compensatable"):
        Manifest.from_json({"tools": {"x.do": {"state": "irreversible", "confirm": {"tool": "x.ok", "args": {}}}}})
    with pytest.raises(ValueError, match="only applies to irreversible"):
        Manifest.from_json({"tools": {"x.do": {"state": "idempotent_write", "hold_seconds": 5}}})
    with pytest.raises(ValueError, match="confirm 'x.ok' must be listed"):
        Manifest.from_json({"tools": {
            "x.do": {"state": "compensatable", "compensator": {"tool": "x.undo", "args": {}},
                     "confirm": {"tool": "x.ok", "args": {}}},
            "x.undo": {"state": "idempotent_write"}}})
