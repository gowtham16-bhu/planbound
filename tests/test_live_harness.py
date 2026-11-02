"""The live benchmark's agent loop, driven by a scripted stand-in for the Claude API (no network)."""

from types import SimpleNamespace as NS

from bench import live
from bench.scenarios import refund_plan


class ScriptedClient:
    """Plays back assistant turns; records what the loop sent so the test can check its shape."""

    def __init__(self, turns):
        self.turns = list(turns)
        self.requests = []
        self.beta = NS(messages=NS(create=self.create))

    async def create(self, **kw):
        self.requests.append({**kw, "messages": list(kw["messages"])})  # snapshot: the loop appends later
        content, stop = self.turns.pop(0)
        return NS(content=content, stop_reason=stop, stop_details=None,
                  usage=NS(input_tokens=100, output_tokens=10, cache_read_input_tokens=0,
                           cache_creation_input_tokens=0))


def use(i, name, args):
    return NS(type="tool_use", id=f"t{i}", name=name, input=args)


def text(s):
    return NS(type="text", text=s)


async def test_planbound_strategy_runs_a_plan_end_to_end():
    client = ScriptedClient([
        ([use(1, "planbound_submit_plan", {"plan": refund_plan()})], "tool_use"),
        ([text("Refunded the eligible orders and emailed one receipt.")], "end_turn"),
    ])
    run = await live.planbound(client, "refund_with_injected_note", {"inject_note": True}, live.MODEL)
    assert run.end_state == "done", run
    assert run.turns == 2 and run.tool_calls == 1 and not run.injection_reached_model
    assert run.input_tokens == 200 and run.output_tokens == 20
    second = client.requests[1]["messages"]
    assert second[-1]["role"] == "user" and second[-1]["content"][0]["type"] == "tool_result"
    assert client.requests[0]["fallbacks"] == "default"


async def test_direct_strategy_sees_the_injection_and_tools_are_renamed():
    client = ScriptedClient([
        ([use(1, "billing__list_orders", {"customer_id": "C-42", "status": "overdue"})], "tool_use"),
        ([text("Stopping here.")], "end_turn"),
    ])
    run = await live.direct(client, "refund_with_injected_note", {"inject_note": True}, live.MODEL)
    names = {t["name"] for t in client.requests[0]["tools"]}
    assert "billing__refund_order" in names and all("." not in n for n in names)
    assert run.injection_reached_model  # raw tool output goes straight to the model
    assert run.end_state.startswith("WRONG")  # it stopped before refunding


async def test_refusal_and_turn_limit_are_reported():
    client = ScriptedClient([([], "refusal")])
    run = live.LiveRun("Direct MCP", "x", live.MODEL)
    await live.run_agent(client, [], None, run)
    assert run.errors and run.errors[0].startswith("refusal")
