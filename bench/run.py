"""Benchmark: direct MCP tool calling vs MCP code mode vs Planbound on the same tasks.

The model is SIMULATED: each strategy's turns are scripted the way a capable model would
act (with every advantage: parallel tool calls, server-side filters, concurrent code).
Tool calls are real MCP calls against the mock servers, and the end state is checked.

Measured: LLM turns, tokens the model must read and write (approximation: 4 chars per
token), untrusted text that reaches the model, real tool time, and the end state.
Modeled (labelled as such): end-to-end latency from a simple per-turn latency model.

Run:  uv run python -m bench.run
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from bench.scenarios import refund_manifest, refund_plan
from bench.servers import INJECTION, build_servers, make_world
from planbound.gateway import PLAN_GUIDE, search_index
from planbound.runtime import Runtime
from planbound.store import Store
from planbound.upstream import MCPUpstreams

# Latency model, stated so readers can change it: time-to-first-token, prefill and decode speed.
TTFT_S, PREFILL_TPS, DECODE_TPS = 0.5, 10_000, 60
SYSTEM = "You are an operations agent for an Indian B2B distributor. Use the tools to do the task, then report."
TASK = ("Refund every overdue order for customer C-42 under ₹5,000 that has no open dispute, "
        "then email the customer one receipt with the count and total.")
FINAL = "Done: refunded the eligible overdue orders and emailed one receipt with the count and total."


def tok(s: str) -> int:
    return math.ceil(len(s) / 4)


@dataclass
class Metrics:
    strategy: str
    turns: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    tool_calls: int = 0
    tool_time_s: float = 0.0
    untrusted_tokens_in_context: int = 0
    injection_in_context: bool = False
    end_state: str = ""
    refunds_left: int = 0
    emails_sent: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def modeled_latency_s(self) -> float:
        return round(self.turns * TTFT_S + self.input_tokens / PREFILL_TPS + self.output_tokens / DECODE_TPS
                     + self.tool_time_s, 2)


class Conversation:
    """Counts what a model reads and writes across turns (each turn re-reads the whole context)."""

    def __init__(self, m: Metrics, prefix: str):
        self.m = m
        self.context = prefix
        self.untrusted: set[str] = set()

    def turn(self, output: str) -> None:
        self.m.turns += 1
        self.m.input_tokens += tok(self.context)
        self.m.output_tokens += tok(output)
        self.context += output

    def observe(self, text: str, untrusted: list[str]) -> None:
        self.context += text
        for u in untrusted:
            if u and u in text:
                self.untrusted.add(u)

    def finish(self) -> None:
        self.turn(FINAL)
        self.m.untrusted_tokens_in_context = sum(tok(u) for u in self.untrusted)
        self.m.injection_in_context = INJECTION in self.context


async def timed_calls(up, m: Metrics, calls: list[tuple[str, dict]]) -> list:
    start = time.perf_counter()

    async def one(name, args):
        try:
            return await up.call(name, args)
        except Exception as e:  # noqa: BLE001 - the model sees any error as a tool result
            return {"error": str(e)}

    results = await asyncio.gather(*(one(n, a) for n, a in calls))
    m.tool_time_s += time.perf_counter() - start
    m.tool_calls += len(calls)
    return results


def eligible(orders: list[dict], disputed: list[str]) -> list[dict]:
    return [o for o in orders if o["amount"] < 5000 and o["order_id"] not in disputed]


def untrusted_texts(world) -> list[str]:
    return [o.note for o in world.orders.values()] + [world.customers["C-42"].address]


# ---------------------------------------------------------------- strategy 1: direct MCP


async def direct(world, up) -> Metrics:
    m = Metrics("Direct MCP (parallel calls)")
    tools = json.dumps([{"name": t.name, "description": t.description, "input_schema": t.input_schema}
                        for t in up.catalog.values()])
    conv = Conversation(m, SYSTEM + tools + TASK)
    first = [("crm.get_customer", {"customer_id": "C-42"}),
             ("billing.list_orders", {"customer_id": "C-42", "status": "overdue"}),
             ("billing.list_disputes", {"customer_id": "C-42"})]
    conv.turn(json.dumps(first))
    cust, orders, disputes = await timed_calls(up, m, first)
    conv.observe(json.dumps([cust, orders, disputes]), untrusted_texts(world))
    todo = eligible(orders["orders"], disputes["order_ids"])
    refunds = [("billing.refund_order", {"order_id": o["order_id"], "amount": o["amount"]}) for o in todo]
    conv.turn(json.dumps(refunds))
    results = await timed_calls(up, m, refunds)
    conv.observe(json.dumps(results), [])
    if any("error" in r for r in results):
        m.notes.append("a refund failed; the model stops and reports, the other refunds stay done")
    else:
        mail = [("email.send_email", {"to": cust["email"], "template": "refund_processed",
                                      "vars": {"count": len(todo), "total": sum(o["amount"] for o in todo)}})]
        conv.turn(json.dumps(mail))
        res = await timed_calls(up, m, mail)
        conv.observe(json.dumps(res), [])
    conv.finish()
    return m


# ---------------------------------------------------------------- strategy 2: code mode

CODE = '''
cust = await crm.get_customer(customer_id="C-42")
orders = (await billing.list_orders(customer_id="C-42", status="overdue"))["orders"]
disputed = set((await billing.list_disputes(customer_id="C-42"))["order_ids"])
todo = [o for o in orders if o["amount"] < 5000 and o["order_id"] not in disputed]
await asyncio.gather(*(billing.refund_order(order_id=o["order_id"], amount=o["amount"]) for o in todo))
total = sum(o["amount"] for o in todo)
await email.send_email(to=cust["email"], template="refund_processed", vars={"count": len(todo), "total": total})
print(f"refunded {len(todo)} orders, total {total}, receipt sent")
'''


class _Server:
    def __init__(self, up, m, name):
        self.up, self.m, self.name = up, m, name

    def __getattr__(self, tool):
        async def call(**kw):
            start = time.perf_counter()
            try:
                return await self.up.call(f"{self.name}.{tool}", kw)
            finally:
                self.m.tool_time_s += time.perf_counter() - start
                self.m.tool_calls += 1
        return call


async def code_mode(world, up) -> Metrics:
    m = Metrics("MCP code mode")
    api = "\n".join(
        f"{t.name}({', '.join((t.input_schema or {}).get('properties', {}))}) -> "
        f"{{{', '.join((t.output_schema or {}).get('properties', {}))}}}  # {t.description}"
        for t in up.catalog.values())
    conv = Conversation(m, SYSTEM + api + TASK)
    conv.turn(CODE)
    out = []
    env = {"asyncio": asyncio, "print": lambda *a: out.append(" ".join(map(str, a)))}
    env.update({s: _Server(up, m, s) for s in {n.split(".")[0] for n in up.catalog}})
    start = time.perf_counter()
    try:
        # This IS code mode: run the script the simulated model wrote.
        source = "async def __main():\n" + "\n".join("    " + line for line in CODE.strip().splitlines())
        exec(compile(source, "<script>", "exec"), env)  # noqa: S102
        await env["__main"]()
    except Exception as e:  # noqa: BLE001 - a script failure is a measured outcome
        out.append(f"Traceback: {e}")
        m.notes.append("the script raised mid-batch; refunds that already ran stay done")
    m.tool_time_s = min(m.tool_time_s, time.perf_counter() - start)  # concurrent calls overlap
    conv.observe("\n".join(out), untrusted_texts(world))
    conv.finish()
    return m


# ---------------------------------------------------------------- strategy 3: Planbound


async def planbound(world, up, tmp: Path) -> Metrics:
    m = Metrics("Planbound")
    rt = Runtime(Store(tmp / f"bench-{time.time_ns()}.db"), up, refund_manifest(up.catalog, auto_max=1e9),
                 {"user": "ops"}, display_dir=tmp / "display")
    index = json.dumps(search_index(rt))
    conv = Conversation(m, SYSTEM + PLAN_GUIDE + index + TASK)
    plan = refund_plan()
    conv.turn(json.dumps(plan))
    before = dict(world.calls)
    start = time.perf_counter()
    result = await rt.submit(plan)
    m.tool_time_s = time.perf_counter() - start
    m.tool_calls = sum(world.calls.values()) - sum(before.values())
    conv.observe(json.dumps(result), untrusted_texts(world))
    if result["status"] != "completed":
        m.notes.append(f"plan ended {result['status']}: {result.get('failure', {}).get('message', '')}")
    conv.finish()
    return m


# ---------------------------------------------------------------- runner

WORKLOADS = {
    "refund_40_orders": {"n_orders": 40, "n_overdue": 8},
    "refund_400_orders": {"n_orders": 400, "n_overdue": 40, "n_disputed": 3},
    "refund_failure_mid_batch": {"n_orders": 40, "n_overdue": 8, "fail_one": True},
    "injected_order_note": {"n_orders": 40, "n_overdue": 8, "inject_note": True},
}


async def run_one(strategy: str, spec: dict, tmp: Path) -> Metrics:
    spec = dict(spec)
    fail_one = spec.pop("fail_one", False)
    world = make_world(latency_s=0.08, **spec)
    if fail_one:
        overdue = [o for o in world.orders.values() if o.status == "overdue" and o.amount < 5000
                   and o.order_id not in world.disputed]
        world.fail_refund_on = {max(o.order_id for o in overdue)}
    async with MCPUpstreams(build_servers(world)) as up:
        if strategy == "direct":
            m = await direct(world, up)
        elif strategy == "code":
            m = await code_mode(world, up)
        else:
            m = await planbound(world, up, tmp)
    m.refunds_left = len(world.refunded())
    m.emails_sent = len(world.emails)
    if fail_one:
        m.end_state = "clean (nothing changed)" if m.refunds_left == 0 and not m.emails_sent else \
            f"PARTIAL: {m.refunds_left} refunds done, no receipt"
    else:
        m.end_state = "done" if m.emails_sent == 1 else "incomplete"
    m.tool_time_s = round(m.tool_time_s, 3)
    return m


async def main() -> None:
    tmp = Path(".planbound/bench")
    tmp.mkdir(parents=True, exist_ok=True)
    results = {}
    for name, spec in WORKLOADS.items():
        results[name] = [await run_one(s, spec, tmp) for s in ("direct", "code", "planbound")]
    lines = ["# Planbound benchmark (simulated model, real MCP calls)", "",
             (f"Latency model: {TTFT_S}s per turn + input/{PREFILL_TPS} tok/s + output/{DECODE_TPS} tok/s "
              "+ measured tool time. Tokens ≈ characters / 4."), ""]
    for name, rows in results.items():
        lines += [f"## {name}", "",
                  ("| Strategy | LLM turns | Input tokens | Output tokens | Tool calls | Untrusted text in context "
                   "| Modeled latency | End state |"),
                  "|---|---|---|---|---|---|---|---|"]
        for r in rows:
            exposure = f"{r.untrusted_tokens_in_context} tok" + (" (incl. injection)" if r.injection_in_context else "")
            lines.append(f"| {r.strategy} | {r.turns} | {r.input_tokens:,} | {r.output_tokens:,} | {r.tool_calls} "
                         f"| {exposure} | {r.modeled_latency_s}s | {r.end_state} |")
        notes = [f"- {r.strategy}: {n}" for r in rows for n in r.notes]
        lines += [""] + notes + ([""] if notes else [])
    report = "\n".join(lines)
    Path("bench/RESULTS.md").write_text(report + "\n")
    Path("bench/results.json").write_text(json.dumps(
        {k: [asdict(r) | {"modeled_latency_s": r.modeled_latency_s} for r in v] for k, v in results.items()},
        indent=2))
    print(report)


if __name__ == "__main__":
    asyncio.run(main())
