"""`planbound verify` tests what an upstream claims, so a manifest cannot simply assert it."""

import dataclasses

import pytest

from bench.scenarios import refund_manifest, refund_plan
from planbound.compiler import CompileError, compile_plan
from planbound.runtime import FENCE_META, IDEM_META
from planbound.verify import verify_tool

CLAIMS = {"idempotency_keys": True, "conditional": {"arg": "expected_version"}, "fencing": True,
          "outcome_check": {"tool": "billing.find_refunds", "args": {"order_id": "$input.order_id"},
                            "found": "$output.refunds"}}


def claiming_manifest(env):
    m = refund_manifest(env.up.catalog, auto_max=1e9)
    for k, v in CLAIMS.items():
        setattr(m.tools["billing.refund_order"], k, v)
    m.tools["billing.find_refunds"] = dataclasses.replace(m.tools["billing.list_disputes"], contract_sha256=None)
    return m


def sample(env):
    oid = min(o.order_id for o in env.world.orders.values() if o.status == "overdue")
    return {"order_id": oid, "amount": 100.0}


class Liar:
    """Says yes to everything, honours nothing: drops idempotency keys, fencing tokens and versions."""

    def __init__(self, inner):
        self.inner, self.catalog = inner, inner.catalog

    async def call(self, tool, args, meta=None):
        meta = {k: v for k, v in (meta or {}).items() if k not in (IDEM_META, FENCE_META)}
        if tool == "billing.refund_order":
            args = {k: v for k, v in args.items() if k != "expected_version"}
        return await self.inner.call(tool, args, meta or None)


async def test_an_honest_upstream_passes_and_is_left_as_it_was(env):
    out = await verify_tool(env.up, claiming_manifest(env), "billing.refund_order", sample(env))
    assert out["passed"], out
    assert out["checks"] == {"conditional": True, "idempotency_keys": True, "outcome_check": True, "fencing": True}
    assert env.world.refunded() == [] and all(r.status == "cancelled" for r in env.world.refunds.values())


async def test_an_upstream_that_only_claims_is_caught(env):
    out = await verify_tool(Liar(env.up), claiming_manifest(env), "billing.refund_order", sample(env))
    assert not out["passed"]
    assert set(out["failed"]) == {"idempotency_keys", "conditional", "fencing"}, out
    assert env.world.refunded() == []  # probes were undone even though checks failed


async def test_strict_mode_can_demand_proof(env):
    m = claiming_manifest(env)
    m.strict_writes = m.require_verified = True
    plan = refund_plan()
    plan["nodes"][4]["call"]["args"]["expected_version"] = {"$item": "version"}
    with pytest.raises(CompileError) as e:
        compile_plan(plan, catalog=env.up.catalog, manifest=m, caller={})
    assert [d.code for d in e.value.diagnostics] == ["E_UNVERIFIED"]

    m.tools["billing.refund_order"].verified = await verify_tool(env.up, m, "billing.refund_order", sample(env))
    compile_plan(plan, catalog=env.up.catalog, manifest=m, caller={})

    m.tools["billing.refund_order"].verified = {**m.tools["billing.refund_order"].verified,
                                                "contract_sha256": "stale"}
    with pytest.raises(CompileError) as e:
        compile_plan(plan, catalog=env.up.catalog, manifest=m, caller={})
    assert "changed since it was verified" in e.value.diagnostics[0].message


async def test_a_failed_attestation_does_not_count(env):
    m = claiming_manifest(env)
    m.strict_writes = m.require_verified = True
    m.tools["billing.refund_order"].verified = await verify_tool(Liar(env.up), m, "billing.refund_order",
                                                                  sample(env))
    plan = refund_plan()
    plan["nodes"][4]["call"]["args"]["expected_version"] = {"$item": "version"}
    with pytest.raises(CompileError) as e:
        compile_plan(plan, catalog=env.up.catalog, manifest=m, caller={})
    assert "did not pass verification" in e.value.diagnostics[0].message
