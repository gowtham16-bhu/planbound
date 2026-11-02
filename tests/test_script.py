"""Plan scripts and prepared plans: same checks as JSON plans, far fewer tokens."""

import json

import pytest

from bench.scenarios import refund_manifest, refund_plan
from planbound.gateway import search_index
from planbound.script import ScriptError, parse_script

REFUND_SCRIPT = '''
# goal: Refund C-42's overdue orders under 5000 without disputes, then email a receipt
cust = crm.get_customer(customer_id="C-42")
orders = billing.list_orders(customer_id=cust.customer_id, status="overdue")
disputes = billing.list_disputes(customer_id=cust.customer_id)
eligible = filter(orders.orders, item.amount < 5000 and item.order_id not in disputes.order_ids)
refunds = map(eligible, billing.refund_order(order_id=item.order_id, amount=item.amount),
              max_items=20, concurrency=8)
notify = email.send_email(to=cust.email, template="refund_processed",
                          vars={"count": count(eligible), "total": sum(eligible, "amount")},
                          when=count(eligible) > 0, after=[refunds])
return eligible, refunds, notify
'''


def rt(env, caller=None):
    return env.runtime(refund_manifest(env.up.catalog, auto_max=1e9), caller=caller)


def test_script_is_exactly_the_json_plan():
    assert parse_script(REFUND_SCRIPT) == refund_plan()


def test_script_is_about_half_the_size():
    assert len(REFUND_SCRIPT) < 0.6 * len(json.dumps(refund_plan()))


async def test_script_runs_like_the_json_plan(env):
    res = await rt(env).submit(REFUND_SCRIPT)
    assert res["status"] == "completed", res
    assert len(env.world.emails) == 1 and env.world.refunded()


@pytest.mark.parametrize("script, message", [
    ("x = (", "syntax error"),
    ("crm.get_customer(customer_id='C-42')", "name = <call>"),
    ("x = 5", "must be assigned"),
    ("x = __import__('os').system('rm -rf /')", "unknown step"),
    ("x = eval('1')", "unknown step"),
    ("a = crm.get_customer(customer_id='C-42')\nb = map(a, print(1), max_items=1)", "server.tool"),
    ("a = crm.get_customer(customer_id=open('/etc/passwd'))", "cannot use"),
    ("a = filter(nothing, item.x > 1)", "earlier step"),
    ("a = crm.get_customer('C-42')", "by name"),
])
def test_scripts_outside_the_subset_are_refused_never_run(script, message):
    with pytest.raises(ScriptError, match=message):
        parse_script(script)


async def test_script_errors_come_back_as_compile_errors(env):
    res = await rt(env).submit("x = (")
    assert res["status"] == "compile_error" and res["errors"][0]["code"] == "E_SCRIPT"
    assert res["errors"][0]["path"] == "line 1"
    res = await rt(env).submit("a = crm.get_custmer(customer_id='C-42')")
    assert res["errors"][0]["code"] == "E_UNKNOWN_TOOL"  # the usual compiler checks still apply


# ---------------------------------------------------------------- prepared plans

PREPARED = REFUND_SCRIPT.replace('"C-42"', "param.customer_id").replace("5000", "param.max_amount")


async def test_prepare_once_then_run_by_name(env):
    runtime = rt(env)
    out = await runtime.prepare("refund_overdue", PREPARED, {"customer_id": "C-42", "max_amount": 5000})
    assert out["status"] == "prepared" and out["params"] == {"customer_id": "str", "max_amount": "int"}, out
    assert env.world.refunds == {}  # preparing runs nothing
    assert search_index(runtime)["prepared"] == {"refund_overdue": {"customer_id": "str", "max_amount": "int"}}
    res = await runtime.run_prepared("refund_overdue", {"customer_id": "C-42", "max_amount": 5000})
    assert res["status"] == "completed", res
    assert len(env.world.emails) == 1


async def test_prepared_runs_get_every_check(env):
    runtime = rt(env)
    await runtime.prepare("refund_overdue", PREPARED, {"customer_id": "C-42", "max_amount": 5000})
    preview = await runtime.run_prepared("refund_overdue", {"customer_id": "C-42", "max_amount": 5000},
                                         mode="preview")
    assert preview["status"] == "preview" and env.world.refunds == {}
    assert (await runtime.run_prepared("refund_overdue", {"customer_id": "C-42"}))["status"] == "error"
    assert (await runtime.run_prepared("refund_overdue", {"customer_id": 42, "max_amount": 1}))["status"] == "error"
    unknown = await runtime.run_prepared("refund_overdue", {"customer_id": "C-99", "max_amount": 5000})
    assert unknown["status"] == "read_failed"  # real reads, real errors


async def test_prepared_plans_belong_to_their_caller(env):
    await rt(env, {"user": "ops"}).prepare("refund_overdue", PREPARED, {"customer_id": "C-42", "max_amount": 5000})
    other = await rt(env, {"user": "mallory"}).run_prepared("refund_overdue", {"customer_id": "C-42",
                                                                              "max_amount": 5000})
    assert other["status"] == "error"


async def test_a_bad_template_is_refused_at_prepare(env):
    bad = PREPARED.replace("max_items=20", "max_items=500")
    out = await rt(env).prepare("refund_overdue", bad, {"customer_id": "C-42", "max_amount": 5000})
    assert out["status"] == "compile_error" and out["errors"][0]["code"] == "E_COST_BOUND"
    out = await rt(env).prepare("x", PREPARED, {"customer_id": "C-42"})
    assert out["status"] == "error"


async def test_placeholders_cannot_be_submitted_directly(env):
    res = await rt(env).submit(PREPARED)
    assert res["errors"][0]["code"] == "E_PARAM"
