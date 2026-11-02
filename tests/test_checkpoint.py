"""Multi-stage plans: a later plan continues from results an earlier plan saved (op "checkpoint")."""

import json
import time

import pytest

from bench.scenarios import refund_manifest, refund_plan
from planbound.compiler import CompileError

CALLER = {"user": "ops"}


def rt(env, **kw):
    return env.runtime(refund_manifest(env.up.catalog, auto_max=1e9), caller=CALLER, **kw)


def eligible_ids(world):
    return sorted(o.order_id for o in world.orders.values()
                  if o.status == "overdue" and o.amount < 5000 and o.order_id not in world.disputed)


async def look(env, runtime=None):
    """Stage 1: an investigation that only reads."""
    res = await (runtime or rt(env)).submit({"planbound": "0.1", "goal": "look around", "nodes": [
        {"id": "cust", "op": "call", "tool": "crm.get_customer", "args": {"customer_id": "C-42"}},
        {"id": "orders", "op": "call", "tool": "billing.list_orders",
         "args": {"customer_id": "C-42", "status": "overdue"}},
        {"id": "disputes", "op": "call", "tool": "billing.list_disputes", "args": {"customer_id": "C-42"}},
    ]})
    assert res["status"] == "completed", res
    return {n: res["summaries"][n]["handle"] for n in ("cust", "orders", "disputes")}


def act(h, **checkpoint_extra):
    """Stage 2: decides what to do from stage 1's results, without reading them again."""
    plan = refund_plan()
    plan["nodes"][:3] = [
        {"id": "cust", "op": "checkpoint", "handle": h["cust"], **checkpoint_extra},
        {"id": "orders", "op": "checkpoint", "handle": h["orders"], **checkpoint_extra},
        {"id": "disputes", "op": "checkpoint", "handle": h["disputes"], **checkpoint_extra},
    ]
    return plan


async def test_second_stage_acts_on_first_stage_results(env):
    want = eligible_ids(env.world)
    h = await look(env)
    calls_before = dict(env.world.calls)
    res = await rt(env).submit(act(h))
    assert res["status"] == "completed", res
    assert env.world.refunded() == want and len(env.world.emails) == 1
    for read in ("crm.get_customer", "billing.list_orders", "billing.list_disputes"):
        assert env.world.calls[read] == calls_before[read]  # nothing was read again


async def test_checkpoint_keeps_labels_trust_and_hides_free_text(injected_env):
    env = injected_env
    h = await look(env)
    runtime = rt(env)
    compiled = runtime.compile(act(h))
    assert "pii" in compiled.info["cust"].labels and "financial" in compiled.info["orders"].labels
    assert compiled.info["orders"].trust == "untrusted"
    res = await runtime.submit({"planbound": "0.1", "nodes": [
        {"id": "orders", "op": "checkpoint", "handle": h["orders"]}]})
    out = json.dumps(res)
    assert "IGNORE" not in out and "Chennai" not in out


async def test_checkpoint_fields_are_checked_against_the_saved_data(env):
    h = await look(env)
    plan = act(h)
    plan["nodes"][3]["input"] = {"$ref": "orders.ordrs"}
    with pytest.raises(CompileError) as e:
        rt(env).compile(plan)
    assert [d.code for d in e.value.diagnostics] == ["E_UNKNOWN_FIELD"]


async def test_checkpoint_belongs_to_its_caller(env):
    h = await look(env)
    with pytest.raises(CompileError) as e:
        env.runtime(refund_manifest(env.up.catalog, auto_max=1e9), caller={"user": "mallory"}).compile(act(h))
    assert {d.code for d in e.value.diagnostics} == {"E_HANDLE_CALLER"}


async def test_checkpoint_age_limit(env):
    h = await look(env)
    env.store.db.execute("UPDATE handles SET created_at = ?", (time.time() - 3600,))
    with pytest.raises(CompileError) as e:
        rt(env).compile(act(h, max_age_seconds=600))
    assert {d.code for d in e.value.diagnostics} == {"E_HANDLE_EXPIRED"}
    rt(env).compile(act(h, max_age_seconds=7200))
    rt(env).compile(act(h))  # no limit unless the plan asks for one


async def test_checkpoint_refuses_results_of_a_plan_that_was_rolled_back(env):
    env.world.fail_email = True
    res = await rt(env).submit(refund_plan())
    assert res["status"] == "failed_compensated"
    plan = {"planbound": "0.1", "nodes": [
        {"id": "r", "op": "checkpoint", "handle": f"h://{res['plan_id']}/refunds"}]}
    with pytest.raises(CompileError) as e:
        rt(env).compile(plan)
    assert {d.code for d in e.value.diagnostics} == {"E_HANDLE_UNUSABLE"}


async def test_chains_of_any_length(env):
    h = (await look(env))["orders"]
    for stage in range(5):
        res = await rt(env).submit({"planbound": "0.1", "nodes": [
            {"id": "prev", "op": "checkpoint", "handle": h},
            {"id": "small", "op": "filter", "input": {"$ref": "prev.orders"} if stage == 0 else {"$ref": "prev"},
             "where": {"lt": [{"$item": "amount"}, 9000 - 1000 * stage]}},
        ]})
        assert res["status"] == "completed", res
        h = res["summaries"]["small"]["handle"]
    assert res["summaries"]["small"]["summary"]["count"] <= 8


async def test_checkpoint_that_expires_before_commit_writes_nothing(env):
    h = await look(env)
    runtime = rt(env)
    pv = await runtime.submit(act(h, max_age_seconds=600), mode="preview")
    assert pv["status"] == "preview", pv
    env.store.db.execute("UPDATE handles SET created_at = ?", (time.time() - 3600,))
    res = await runtime.commit(pv["plan_id"])
    assert res["status"] == "compile_error" and res["errors"][0]["code"] == "E_HANDLE_EXPIRED", res
    assert env.world.refunds == {}
