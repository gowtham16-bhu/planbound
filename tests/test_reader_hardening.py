"""Steered classifications cannot act alone: readers must agree, and reader-driven writes need a human."""

import pytest

from bench.scenarios import refund_manifest
from planbound.compiler import READER_LABEL, CompileError
from planbound.reader import RuleReader

SCHEMA = {"topic": {"enum": ["damage", "billing", "delivery", "other"]}, "urgent": {"type": "boolean"}}


def reader(topic_rules):
    return RuleReader({"topic": topic_rules, "urgent": [("escalate", True)]}, {"topic": "other", "urgent": False})


HONEST = reader([("damaged", "damage"), ("disput", "billing")])
STEERED = reader([("ignore", "damage"), ("damaged", "damage"), ("disput", "billing")])  # obeys the injection


def plan(**node_extra):
    return {"planbound": "0.1", "nodes": [
        {"id": "orders", "op": "call", "tool": "billing.list_orders", "args": {"customer_id": "C-42"}},
        {"id": "tagged", "op": "quarantine_read", "input": {"$ref": "orders.orders"}, "field": "note",
         "max_items": 50, "schema": SCHEMA, **node_extra},
        {"id": "damaged", "op": "filter", "input": {"$ref": "tagged"},
         "where": {"eq": [{"$item": "topic"}, "damage"]}},
    ]}


def refund_damaged():
    p = plan(fallback={"topic": "other"})
    p["nodes"].append({"id": "refunds", "op": "map", "input": {"$ref": "damaged"}, "max_items": 20,
                       "call": {"tool": "billing.refund_order",
                                "args": {"order_id": {"$item": "order_id"}, "amount": {"$item": "amount"}}}})
    return p


def rt(env, readers, **kw):
    return env.runtime(refund_manifest(env.up.catalog, auto_max=1e12), reader=readers, **kw)


async def test_one_steered_reader_is_outvoted(injected_env):
    env = injected_env
    alone = await rt(env, STEERED).submit(plan())
    together = await rt(env, [HONEST, STEERED]).submit(plan(fallback={"topic": "other"}))
    honest = await rt(env, HONEST).submit(plan())
    count = lambda res: res["summaries"]["damaged"]["summary"]["count"]
    assert count(alone) == count(honest) + 1  # alone, the steered reader lets the injected note through
    assert count(together) == count(honest)  # together, the disagreement falls back to "other"


async def test_disagreement_without_fallback_stops_the_plan(injected_env):
    res = await rt(injected_env, [HONEST, STEERED]).submit(plan())
    assert res["status"] == "read_failed" and "disagree" in res["message"], res


async def test_reader_driven_writes_always_need_a_human(env):
    runtime = rt(env, [HONEST])
    compiled = runtime.compile(refund_damaged())
    assert READER_LABEL in compiled.info["refunds"].labels
    res = await runtime.submit(refund_damaged(), mode="preview")
    assert res["approval"] == "policy" and res["requires_human"] and res["reader_driven"] == ["refunds"], res
    # Even inside the auto-approve budget nothing was written: it waits for a person.
    out = await runtime.submit(refund_damaged())
    assert out["status"] == "awaiting_approval", out
    assert env.world.refunds == {}


async def test_operator_can_opt_out_to_budget_rules(env):
    m = refund_manifest(env.up.catalog, auto_max=1e12)
    m.reader_driven_writes = "policy"
    res = await env.runtime(m, reader=[HONEST]).submit(refund_damaged(), mode="preview")
    assert res["approval"] == "auto" and not res["requires_human"], res


async def test_reader_label_does_not_block_data_flow(env):
    p = refund_damaged()
    p["nodes"].append({"id": "notify", "op": "call", "tool": "email.send_email", "depends_on": ["refunds"],
                       "args": {"to": "ops@example.com", "template": "refund_processed",
                                "vars": {"count": {"$agg": {"fn": "count", "of": "damaged"}}}}})
    rt(env, [HONEST]).compile(p)  # email allows pii/financial only; the provenance label is not a restriction


async def test_fallback_must_be_a_valid_answer(env):
    with pytest.raises(CompileError) as e:
        rt(env, [HONEST]).compile(plan(fallback={"topic": "free text"}))
    assert [d.code for d in e.value.diagnostics] == ["E_QUARANTINE_SCHEMA"]
