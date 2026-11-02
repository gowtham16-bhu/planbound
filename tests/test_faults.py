"""Fault injection: undo steps that fail, crashes during rollback, lost answers and concurrent writers.

`Faulty` wraps the real upstreams and breaks specific calls, so every failure here goes through the
same MCP path as production.
"""

import asyncio
import copy
import dataclasses
import time

import pytest

from bench.scenarios import hr_manifest, offboarding_plan, refund_manifest, refund_plan
from planbound.compiler import CompileError, compile_plan
from planbound.manifest import Manifest, UpstreamTool, infer_effects
from planbound.runtime import SimulatedCrash, UpstreamError


class Faulty:
    """Upstreams that fail on purpose. Each rule fires on the next `times` calls to `tool`."""

    def __init__(self, inner):
        self.inner = inner
        self.catalog = inner.catalog
        self.rules: list[list] = []

    def down(self, tool, times=1):
        """No answer, and the request never arrived."""
        async def act(tool, args, meta):
            raise UpstreamError(f"{tool}: connection refused", definite=False)
        self.rules.append([tool, times, act])

    def lose_answer(self, tool, times=1):
        """The request arrives and takes effect, but the answer is lost."""
        async def act(tool, args, meta):
            await self.inner.call(tool, args, meta)
            raise UpstreamError(f"{tool}: read timeout", definite=False)
        self.rules.append([tool, times, act])

    def crash(self, tool, times=1):
        async def act(tool, args, meta):
            raise SimulatedCrash(f"gateway died while calling {tool}")
        self.rules.append([tool, times, act])

    def before(self, tool, fn, times=1):
        """Something else changes the world just before this call reaches the server."""
        async def act(tool, args, meta):
            fn(args)
            return await self.inner.call(tool, args, meta)
        self.rules.append([tool, times, act])

    async def call(self, tool, args, meta=None):
        for rule in self.rules:
            if rule[0] == tool and rule[1] > 0:
                rule[1] -= 1
                return await rule[2](tool, args, meta)
        return await self.inner.call(tool, args, meta)


def eligible_ids(world):
    return sorted(o.order_id for o in world.orders.values()
                  if o.status == "overdue" and o.amount < 5000 and o.order_id not in world.disputed)


def manifest(env, **tool_overrides):
    m = refund_manifest(env.up.catalog, auto_max=1e9)
    for field, value in tool_overrides.items():
        setattr(m.tools["billing.refund_order"], field, value)
    return m


def journal_status(env, plan_id):
    return {f"{e['node_id']}[{e['item_index']}]": e["status"] for e in env.store.journal(plan_id)}


# ---------------------------------------------------------------- compensation that fails


async def test_undo_step_is_retried_until_the_service_is_back(env):
    up = Faulty(env.up)
    up.down("billing.cancel_refund", times=2)
    env.world.fail_refund_on = {eligible_ids(env.world)[-1]}
    res = await env.runtime(manifest(env), upstreams=up).submit(refund_plan())
    assert res["status"] == "failed_compensated", res
    assert env.world.refunded() == []


async def test_failed_undo_is_finished_by_recover(env):
    up = Faulty(env.up)
    up.down("billing.cancel_refund", times=100)  # the wallet is down for the whole rollback
    env.world.fail_refund_on = {eligible_ids(env.world)[-1]}
    res = await env.runtime(manifest(env), upstreams=up, compensation_attempts=2).submit(refund_plan())
    assert res["status"] == "failed_partial", res
    assert res["compensation"]["failed"] and env.world.refunded() != []
    assert "COMPENSATION_FAILED" in journal_status(env, res["plan_id"]).values()

    up.rules.clear()  # the service is back
    reports = await env.runtime(manifest(env)).recover()
    assert [r["status"] for r in reports] == ["recovered_compensated"], reports
    assert env.world.refunded() == []
    assert await env.runtime(manifest(env)).recover() == []  # nothing left to reconcile


async def test_crash_during_rollback_is_finished_on_restart(env):
    up = Faulty(env.up)
    env.world.fail_refund_on = {eligible_ids(env.world)[-1]}
    up.crash("billing.cancel_refund")
    with pytest.raises(SimulatedCrash):
        await env.runtime(manifest(env), upstreams=up).submit(refund_plan())
    assert env.world.refunded() != []
    reports = await env.runtime(manifest(env)).recover()
    assert reports[0]["status"] == "recovered_compensated", reports
    assert env.world.refunded() == []


# ---------------------------------------------------------------- lost answers


async def test_lost_answer_is_resent_with_the_same_key_and_not_repeated(env):
    up = Faulty(env.up)
    up.lose_answer("billing.refund_order")
    want = eligible_ids(env.world)
    res = await env.runtime(manifest(env), upstreams=up).submit(refund_plan())
    assert res["status"] == "completed", res
    assert env.world.refunded() == want
    assert len(env.world.refunds) == len(want)  # one refund per order: the key stopped a double refund


async def test_lost_answer_without_idempotency_keys_needs_a_human(env):
    up = Faulty(env.up)
    up.lose_answer("billing.refund_order")
    m = manifest(env, idempotency_keys=False)
    res = await env.runtime(m, upstreams=up).submit(refund_plan())
    assert res["status"] == "needs_attention", res
    assert res["failure"]["code"] == "E_OUTCOME_UNKNOWN"
    [step] = res["unknown"]
    assert env.world.emails == []
    # Every refund known to have happened was undone; the unknown one really happened.
    entry = next(e for e in env.store.journal(res["plan_id"]) if e["status"] == "UNKNOWN")
    assert env.world.refunded() == [entry["args"]["order_id"]]

    # A human checks billing, finds the refund and records it; recover then undoes it too.
    rt = env.runtime(m)
    assert rt.resolve(res["plan_id"], step, happened=True)["status"] == "error"  # undo needs the refund_id
    refund = next(r for r in env.world.refunds.values() if r.order_id == entry["args"]["order_id"])
    out = rt.resolve(res["plan_id"], step, happened=True, result={"refund_id": refund.refund_id})
    assert out["status"] == "resolved"
    reports = await rt.recover()
    assert reports[0]["status"] == "recovered_compensated", reports
    assert env.world.refunded() == []


async def test_recovery_never_reports_success_over_an_unknown_write(env):
    rt = env.runtime(manifest(env, idempotency_keys=False))
    rt.crash_after_writes = 1
    with pytest.raises(SimulatedCrash):
        await rt.submit(refund_plan())
    report = (await env.runtime(manifest(env, idempotency_keys=False)).recover())[0]
    assert report["status"] == "needs_attention", report
    # Every refund that was in flight at the crash is reported, none is silently skipped.
    assert len(report["unknown"]) == len(env.world.refunds) >= 1
    # Still unresolved: a second recover keeps saying so instead of guessing.
    again = await env.runtime(manifest(env, idempotency_keys=False)).recover()
    assert again[0]["status"] == "needs_attention"


async def test_resolve_rejects_steps_that_are_not_unknown(env):
    res = await env.runtime(manifest(env)).submit(refund_plan())
    rt = env.runtime(manifest(env))
    assert rt.resolve(res["plan_id"], "refunds[0]", happened=False)["status"] == "error"
    assert rt.resolve(res["plan_id"], "nonsense", happened=False)["status"] == "error"


# ---------------------------------------------------------------- the race after the re-read


def conditional_plan():
    plan = refund_plan()
    plan["nodes"][4]["call"]["args"]["expected_version"] = {"$item": "version"}
    return plan


async def test_conditional_write_closes_the_race_after_the_reread(env):
    up = Faulty(env.up)
    first = eligible_ids(env.world)[0]

    def concurrent_writer(args):
        env.world.orders[first].version += 1  # another service touched the order after our re-read

    up.before("billing.refund_order", concurrent_writer)
    m = manifest(env, conditional={"arg": "expected_version"})
    res = await env.runtime(m, upstreams=up).submit(conditional_plan())
    assert res["status"] == "failed_compensated", res
    assert res["failure"]["code"] == "E_WRITE_CONFLICT"
    assert env.world.refunded() == [] and env.world.emails == []


async def test_conditional_write_succeeds_when_nothing_changed(env):
    m = manifest(env, conditional={"arg": "expected_version"})
    res = await env.runtime(m).submit(conditional_plan())
    assert res["status"] == "completed", res


async def test_conditional_tool_needs_the_value_from_a_read(env):
    m = manifest(env, conditional={"arg": "expected_version"})
    with pytest.raises(CompileError) as e:
        compile_plan(refund_plan(), catalog=env.up.catalog, manifest=m, caller={})
    assert [d.code for d in e.value.diagnostics] == ["E_CONDITIONAL_REQUIRED"]
    literal = conditional_plan()
    literal["nodes"][4]["call"]["args"]["expected_version"] = 1
    with pytest.raises(CompileError):
        compile_plan(literal, catalog=env.up.catalog, manifest=m, caller={})


async def test_strict_writes_refuses_money_moving_tools_without_guarantees(env):
    m = manifest(env)
    m.strict_writes = True
    with pytest.raises(CompileError) as e:
        compile_plan(refund_plan(), catalog=env.up.catalog, manifest=m, caller={})
    [d] = e.value.diagnostics
    assert d.code == "E_UNSAFE_WRITE" and "conditional" in d.message
    m.tools["billing.refund_order"].conditional = {"arg": "expected_version"}
    compile_plan(conditional_plan(), catalog=env.up.catalog, manifest=m, caller={})
    m.tools["billing.refund_order"].idempotency_keys = False
    with pytest.raises(CompileError) as e:
        compile_plan(conditional_plan(), catalog=env.up.catalog, manifest=m, caller={})
    assert "idempotency keys" in e.value.diagnostics[0].message


# ---------------------------------------------------------------- retry-safe steps without a pivot


async def test_unfinished_retry_safe_steps_roll_forward_not_back(env):
    env.world.flaky_revoke = {"aws": 4}  # more failures than one run retries
    m = hr_manifest(env.up.catalog)
    m.auto_approve_max_cost = 1
    plan = offboarding_plan()
    plan["nodes"] = [n for n in plan["nodes"] if n["id"] != "notify"]  # no point of no return
    admin = {"employee_id": "E-9000", "roles": ["hr_admin"]}
    res = await env.runtime(m, caller=admin).submit(plan)
    assert res["status"] == "failed_partial", res  # revocations happened; they are not undone
    reports = await env.runtime(m, caller=admin).recover()
    assert reports[0]["status"] == "completed", reports
    assert not any(a.active for a in env.world.accounts["E-2002"])
    revokes = [e for e in env.store.journal(res["plan_id"]) if e["tool"] == "iam.revoke_access"]
    assert len(revokes) == 6  # one journal entry per account, reused on retry
    assert env.world.calls["iam.revoke_access"] == 6 + 4  # completed revocations were never re-sent


# ---------------------------------------------------------------- manifest mistakes


def test_read_only_label_on_a_write_tool_is_rejected():
    data = {"tools": {"billing.refund_order": {"state": "read_only"}}}
    with pytest.raises(ValueError, match="name says it changes"):
        Manifest.from_json(data)
    data["tools"]["billing.refund_order"]["confirm_read_only"] = True
    assert Manifest.from_json(data).tools["billing.refund_order"].state == "read_only"


async def test_read_only_label_on_a_destructive_tool_is_a_compile_error(env):
    m = manifest(env)
    catalog = dict(env.up.catalog)
    catalog["billing.list_orders"] = dataclasses.replace(catalog["billing.list_orders"], destructive_hint=True)
    with pytest.raises(CompileError) as e:
        compile_plan(refund_plan(), catalog=catalog, manifest=m, caller={})
    assert "E_EFFECTS_CONFLICT" in {d.code for d in e.value.diagnostics}


def test_enforce_mode_inference_does_not_trust_names_alone():
    tool = UpstreamTool("ops.get_and_reset_counter", "Returns the counter and resets it.", {}, None)
    assert infer_effects(tool, {}).state == "read_only"  # dev mode guesses from the name
    strict = infer_effects(tool, {}, strict=True)
    assert strict.state == "irreversible" and strict.requires_approval
    hinted = dataclasses.replace(tool, read_only_hint=True)
    assert infer_effects(hinted, {}, strict=True).state == "read_only"


def test_idempotency_keys_are_off_unless_declared():
    m = Manifest.from_json({"tools": {
        "billing.refund_order": {"state": "compensatable",
                                 "compensator": {"tool": "billing.cancel_refund", "args": {}}},
        "billing.cancel_refund": {"state": "idempotent_write"}}})
    assert m.tools["billing.refund_order"].idempotency_keys is False
    round_trip = Manifest.from_json(copy.deepcopy(manifest_json := {"tools": {"x.send_note": {
        "state": "irreversible", "idempotency_keys": True}}}))
    assert round_trip.to_json()["tools"]["x.send_note"]["idempotency_keys"] is True
    assert manifest_json  # unchanged input


def test_compensators_must_be_safe_to_retry():
    tools = {"billing.refund_order": {"state": "compensatable",
                                      "compensator": {"tool": "billing.cancel_refund", "args": {}}}}
    with pytest.raises(ValueError, match="must be listed"):
        Manifest.from_json({"tools": tools})
    tools["billing.cancel_refund"] = {"state": "irreversible"}
    with pytest.raises(ValueError, match="retried during rollback"):
        Manifest.from_json({"tools": tools})
    tools["billing.cancel_refund"]["idempotency_keys"] = True
    Manifest.from_json({"tools": tools})


# ---------------------------------------------------------------- leases: one driver per plan


async def test_recover_does_not_touch_a_plan_a_live_gateway_is_running(env):
    live = env.runtime(manifest(env), lease_ttl_s=60)
    live.crash_after_writes = 1  # stands in for a gateway that is merely slow, still holding its lease
    with pytest.raises(SimulatedCrash):
        await live.submit(refund_plan())
    done = env.world.refunded()
    reports = await env.runtime(manifest(env)).recover()
    assert [r["status"] for r in reports] == ["skipped"], reports
    assert env.world.refunded() == done  # nothing compensated under the live gateway's feet

    env.store.db.execute("UPDATE plans SET lease_until = ?", (time.time() - 1,))  # the lease runs out
    reports = await env.runtime(manifest(env)).recover()
    assert reports[0]["status"] == "recovered_compensated", reports
    assert env.world.refunded() == []


async def test_a_gateway_that_lost_its_lease_stops_writing(env):
    up = Faulty(env.up)
    plan = refund_plan()
    plan["nodes"][4]["concurrency"] = 1
    stalled = env.runtime(manifest(env), upstreams=up, lease_ttl_s=60)
    rescuer = env.runtime(manifest(env))

    def takeover(args):  # the gateway stalls past its lease and recovery claims the plan
        [plan_id] = env.store.plans_with_status("running")
        env.store.db.execute("UPDATE plans SET lease_until = ?", (time.time() - 1,))
        assert env.store.claim(plan_id, rescuer.instance_id, 60, ("running",))

    up.before("billing.refund_order", takeover)
    res = await stalled.submit(plan)
    assert res["status"] == "running" and "took the plan over" in res["message"], res
    assert len(env.world.refunds) == 1  # it stopped after the write in progress
    reports = await rescuer.recover()
    assert reports[0]["status"] == "recovered_compensated", reports
    assert env.world.refunded() == []


async def test_two_commits_of_one_plan_run_it_once(env):
    a, b = env.runtime(manifest(env)), env.runtime(manifest(env))
    want = eligible_ids(env.world)
    pv = await a.submit(refund_plan(), mode="preview")
    results = await asyncio.gather(a.commit(pv["plan_id"]), b.commit(pv["plan_id"]))
    winners = [r for r in results if "got there first" not in r.get("message", "")]
    assert len(winners) == 1 and winners[0]["status"] == "completed", results
    assert len(env.world.refunds) == len(want)  # every order refunded exactly once
    assert len(env.world.emails) == 1


# ---------------------------------------------------------------- abandoning what recovery cannot finish


async def test_abandon_closes_a_plan_recovery_cannot_finish(env):
    up = Faulty(env.up)
    up.lose_answer("billing.refund_order")
    m = manifest(env, idempotency_keys=False)
    res = await env.runtime(m, upstreams=up).submit(refund_plan())
    assert res["status"] == "needs_attention"
    rt = env.runtime(m)
    out = rt.abandon(res["plan_id"], "finance reversed the refund by hand")
    assert out["status"] == "abandoned" and out["left_as_is"]["unknown"] == res["unknown"]
    assert env.store.get_plan(res["plan_id"])["status"] == "abandoned"
    assert await rt.recover() == []  # no longer reconciled
    assert rt.abandon(res["plan_id"], "again")["status"] == "error"


async def test_only_unfinished_plans_can_be_abandoned(env):
    res = await env.runtime(manifest(env)).submit(refund_plan())
    assert env.runtime(manifest(env)).abandon(res["plan_id"], "oops")["status"] == "error"
