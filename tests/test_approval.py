import asyncio
import time

from bench.scenarios import refund_manifest, refund_plan
from planbound.approval import (
    BudgetPolicy,
    PolicySigner,
    generate_keys,
    verify_approval,
)


def setup(env, budget=1e9, max_plan_cost=None):
    keys = env.tmp / "keys"
    pubs = generate_keys(keys)
    m = refund_manifest(env.up.catalog, approvers=pubs)
    signer = PolicySigner(env.store, env.tmp / "signer.db", keys,
                          BudgetPolicy(budget=budget, max_plan_cost=max_plan_cost))
    return env.runtime(m), signer, pubs


async def test_policy_signer_approves_within_budget(env):
    rt, signer, _ = setup(env)
    pending = await rt.submit(refund_plan(), wait_seconds=0)
    assert pending["status"] == "awaiting_approval"
    assert env.world.refunds == {}
    [decision] = signer.process()
    assert decision["decision"] == "signed"
    res = await rt.commit(pending["plan_id"])
    assert res["status"] == "completed"
    signer.sync_outcomes()
    assert signer.used() == decision["cost"]


async def test_gateway_waits_for_a_running_signer(env):
    rt, signer, _ = setup(env)

    async def signer_loop():
        for _ in range(50):
            signer.process()
            await asyncio.sleep(0.05)

    task = asyncio.create_task(signer_loop())
    res = await rt.submit(refund_plan(), wait_seconds=5)
    task.cancel()
    assert res["status"] == "completed"


async def test_rolling_budget_escalates_to_human(env):
    rt, signer, _ = setup(env, budget=1)
    pending = await rt.submit(refund_plan(), wait_seconds=0)
    [decision] = signer.process()
    assert decision["decision"] == "escalated"
    assert "rolling budget" in decision["reasons"][0]
    still = await rt.commit(pending["plan_id"])
    assert still["status"] == "awaiting_approval" and still["approval"] == "escalated"
    assert signer.human_decide(pending["plan_id"], True)["decision"] == "signed"
    assert (await rt.commit(pending["plan_id"]))["status"] == "completed"


async def test_split_plans_hit_the_cumulative_budget(env):
    rt, signer, _ = setup(env)
    first = await rt.submit(refund_plan(), wait_seconds=0)
    [d1] = signer.process()
    signer.policy.budget = d1["cost"] + 1  # room for exactly one plan this window
    await rt.commit(first["plan_id"])
    for o in env.world.orders.values():  # new overdue orders arrive
        if o.status == "paid":
            o.status = "overdue"
    await rt.submit(refund_plan(), wait_seconds=0)
    [d2] = signer.process()
    assert d2["decision"] == "escalated"


async def test_denied_plan_writes_nothing(env):
    rt, signer, _ = setup(env, budget=1)
    pending = await rt.submit(refund_plan(), wait_seconds=0)
    signer.process()
    signer.human_decide(pending["plan_id"], False)
    res = await rt.commit(pending["plan_id"])
    assert res["status"] == "rejected"
    assert env.world.refunds == {}


async def test_forged_or_expired_approval_is_refused(env):
    rt, signer, pubs = setup(env)
    pending = await rt.submit(refund_plan(), wait_seconds=0)
    signer.process()
    approval = env.store.get_approval(pending["plan_id"])
    assert verify_approval(approval, pubs) is None
    forged = dict(approval, preview_hash="sha256:other")
    assert verify_approval(forged, pubs) == "signature does not verify"
    assert verify_approval(approval, pubs, now=time.time() + 3600) == "approval expired"
    env.store.set_approval(pending["plan_id"], signature=approval["signature"][:-4] + "AAAA")
    res = await rt.commit(pending["plan_id"])
    assert res["status"] == "approval_invalid"
    assert env.world.refunds == {}


async def test_released_hold_after_rollback(env):
    rt, signer, _ = setup(env)
    env.world.fail_email = True
    pending = await rt.submit(refund_plan(), wait_seconds=0)
    signer.process()
    res = await rt.commit(pending["plan_id"])
    assert res["status"] == "failed_compensated"
    signer.sync_outcomes()
    assert signer.used() == 0
