import asyncio
import json

import pytest
from mcp import Client

from bench.scenarios import refund_manifest, refund_plan
from bench.servers import build_servers, make_world
from planbound.gateway import Gateway, GatewayConfig, build_server
from planbound.runtime import Runtime, UpstreamError
from planbound.store import Store
from planbound.upstream import MCPUpstreams


async def test_agent_uses_planbound_over_plain_mcp(tmp_path):
    world = make_world(latency_s=0, inject_note=True)
    servers = build_servers(world)
    async with MCPUpstreams(servers) as up:
        manifest = refund_manifest(up.catalog, auto_max=1e9)
    (tmp_path / "planbound.effects.json").write_text(json.dumps(manifest.to_json()))
    cfg = GatewayConfig(servers=servers, manifest_path=str(tmp_path / "planbound.effects.json"),
                        data_dir=str(tmp_path / ".planbound"))
    async with Client(build_server(Gateway(cfg))) as agent:
        names = {t.name for t in (await agent.list_tools()).tools}
        assert names == {"planbound_search_tools", "planbound_submit_plan", "planbound_commit",
                         "planbound_inspect_handle"}
        index = (await agent.call_tool("planbound_search_tools", {})).structured_content
        sigs = [t["signature"] for t in index["tools"]]
        assert any(s.startswith("billing.refund_order(order_id, amount, reason?, expected_version?)") for s in sigs)
        assert not any("update_customer_tier" in s for s in sigs)  # unlisted tools are hidden
        res = await agent.call_tool("planbound_submit_plan", {"plan": refund_plan()})
        body = res.structured_content
        assert body["status"] == "completed", body
        assert "IGNORE" not in json.dumps(body)
    assert len(world.emails) == 1


def write_config(tmp_path, **extra):
    path = tmp_path / "planbound.config.json"
    path.write_text(json.dumps({"servers": {}, **extra}))
    return path


def test_runtime_options_come_from_the_config(tmp_path):
    cfg = GatewayConfig.load(write_config(tmp_path, runtime={
        "lease_seconds": 300, "retry_attempts": 5, "compensation_attempts": 8}))
    assert cfg.runtime == {"lease_ttl_s": 300.0, "retry_attempts": 5, "compensation_attempts": 8}
    assert cfg.reconcile_seconds == 0
    assert GatewayConfig.load(write_config(tmp_path)).runtime == {}
    assert GatewayConfig.load(write_config(tmp_path, runtime={"reconcile_seconds": 15})).reconcile_seconds == 15


@pytest.mark.parametrize("runtime, message", [
    ({"lease_seconds": 0}, "between"),
    ({"lease_seconds": "60"}, "must be a number"),
    ({"retry_attempts": 2.5}, "must be an integer"),
    ({"compensation_attempts": True}, "must be an integer"),
    ({"lease_secs": 60}, "unknown option"),
    ([60], "must be an object"),
])
def test_bad_runtime_options_are_refused(tmp_path, runtime, message):
    with pytest.raises(ValueError, match=message):
        GatewayConfig.load(write_config(tmp_path, runtime=runtime))


async def test_gateway_runtime_uses_the_configured_lease(tmp_path):
    world = make_world(latency_s=0)
    servers = build_servers(world)
    async with MCPUpstreams(servers) as up:
        manifest = refund_manifest(up.catalog, auto_max=1e9)
    (tmp_path / "planbound.effects.json").write_text(json.dumps(manifest.to_json()))
    cfg = GatewayConfig(servers=servers, manifest_path=str(tmp_path / "planbound.effects.json"),
                        data_dir=str(tmp_path / ".planbound"),
                        runtime={"lease_ttl_s": 123.0, "compensation_attempts": 7})
    gateway = Gateway(cfg)
    async with gateway.lifespan(None):
        rt = await gateway.get_runtime()
        assert (rt.lease_ttl_s, rt.compensation_attempts, rt.retry_attempts) == (123.0, 7, 3)


async def test_background_reconciler_finishes_a_stuck_rollback(tmp_path):
    world = make_world(latency_s=0)
    servers = build_servers(world)
    db = tmp_path / ".planbound" / "gateway.db"
    async with MCPUpstreams(servers) as up:
        manifest = refund_manifest(up.catalog, auto_max=1e9)
        (tmp_path / "planbound.effects.json").write_text(json.dumps(manifest.to_json()))
        eligible = sorted(o.order_id for o in world.orders.values()
                          if o.status == "overdue" and o.amount < 5000 and o.order_id not in world.disputed)
        world.fail_refund_on = {eligible[-1]}
        real_call = up.call

        async def billing_undo_down(tool, args, meta=None):
            if tool == "billing.cancel_refund":
                raise UpstreamError("billing unavailable", definite=False)
            return await real_call(tool, args, meta)

        up.call = billing_undo_down  # the undo service is down for the whole rollback
        rt = Runtime(Store(db), up, manifest, {}, retry_delay_s=0, compensation_attempts=1, lease_ttl_s=0,
                     display_dir=tmp_path / "display")
        res = await rt.submit(refund_plan())
    assert res["status"] == "failed_partial", res
    assert world.refunded() == eligible[:-1]

    # The service is back. The gateway's background reconciler finishes the rollback, no human involved.
    cfg = GatewayConfig(servers=servers, manifest_path=str(tmp_path / "planbound.effects.json"),
                        data_dir=str(tmp_path / ".planbound"), reconcile_seconds=0.05)
    gateway = Gateway(cfg)
    async with gateway.lifespan(None):
        for _ in range(100):
            if Store(db).get_plan(res["plan_id"])["status"] == "recovered_compensated":
                break
            await asyncio.sleep(0.05)
    assert Store(db).get_plan(res["plan_id"])["status"] == "recovered_compensated"
    assert world.refunded() == []
