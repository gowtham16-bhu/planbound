import json

from mcp import Client

from bench.scenarios import refund_manifest, refund_plan
from bench.servers import build_servers, make_world
from planbound.gateway import Gateway, GatewayConfig, build_server
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
