"""The gateway: an ordinary MCP server that agents talk to instead of the upstream servers."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp.server.mcpserver import MCPServer

from .manifest import Manifest
from .runtime import Runtime
from .store import Store
from .upstream import MCPUpstreams

PLAN_GUIDE = """\
Planbound runs your tool calls as one bounded plan. Send it to planbound_submit_plan as a script
(compact Python-like syntax, only parsed, never run). You get summaries computed by code, never raw
text, and handles (h://plan/node) to the full data.

  # goal: what this does
  cust = crm.get_customer(customer_id="C-42")
  late = filter(orders.orders, item.amount < 5000 and item.id not in disputes.ids)
  done = map(late, billing.refund(order_id=item.id), max_items=20)       # every map needs max_items
  note = email.send(to=cust.email, vars={"n": count(late), "sum": sum(late, "amount")},
                    when=count(late) > 0, after=[done])
  return late, done

Steps: server.tool(name=value, when=cond, after=[steps]); filter(x, cond); partition(x, cond)
-> .match/.rest; map(x, server.tool(...), max_items=N); quarantine_read(x, field="text",
max_items=N, schema={"f": {"enum": [...]}|{"type": "boolean"|"integer"|"number"}}); display(x)
(human sees it, you don't); checkpoint("h://plan/node") (continue from an earlier result).
Values: literals, step.path, item.field, caller.attr, count|distinct_count|sum|min|max|mean(step, "field").
Conditions: < <= > >= == != in, not in, and, or, not, exists(x). No loops; `when` may only use reads.
At most one irreversible step, after every undoable one. Errors name the line and how to fix it.
Modes: "compile", "preview" (exact impact, nothing written), "execute" (default).
Repeated task? planbound_prepare(name, script with param.x placeholders, example), then
planbound_run(name, params): a few tokens per run, same checks.
"""


# Runtime knobs a config file may set under "runtime", with their bounds.
RUNTIME_OPTIONS = {
    # How long a plan's lease lasts without renewal. Longer than your slowest upstream write call,
    # shorter than you are willing to wait before `planbound recover` may take over a dead gateway.
    "lease_seconds": ("lease_ttl_s", float, 1.0, 86400.0),
    "retry_attempts": ("retry_attempts", int, 1, 20),
    "compensation_attempts": ("compensation_attempts", int, 1, 50),
    # Gateway-level: reconcile unfinished plans in the background every N seconds (0 = off).
    "reconcile_seconds": (None, float, 0.0, 86400.0),
}
log = logging.getLogger("planbound")


def parse_runtime_options(raw: Any) -> dict[str, Any]:
    """Validate the config's "runtime" section into Runtime keyword arguments."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError('config "runtime" must be an object')  # noqa: TRY004 - one error type for bad config
    unknown = set(raw) - set(RUNTIME_OPTIONS)
    if unknown:
        raise ValueError(f'config "runtime": unknown option(s) {sorted(unknown)}; '
                         f"known: {sorted(RUNTIME_OPTIONS)}")
    out: dict[str, Any] = {}
    for name, value in raw.items():
        kwarg, kind, low, high = RUNTIME_OPTIONS[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or (kind is int and value != int(value)):
            raise ValueError(f'config "runtime.{name}" must be {"an integer" if kind is int else "a number"}')
        if not low <= value <= high:
            raise ValueError(f'config "runtime.{name}" must be between {low:g} and {high:g}, got {value}')
        if kwarg is not None:
            out[kwarg] = kind(value)
    return out


def parse_alerts(raw: Any) -> dict | None:
    if raw is None:
        return None
    if not isinstance(raw, dict) or not isinstance(raw.get("command"), list) or not raw["command"] \
            or not all(isinstance(a, str) for a in raw["command"]):
        raise ValueError('config "alerts" needs "command": ["program", "arg", ...]')
    after = raw.get("after_seconds", 300)
    if isinstance(after, bool) or not isinstance(after, (int, float)) or after < 0:
        raise ValueError('config "alerts.after_seconds" must be a number >= 0')
    return {"command": list(raw["command"]), "after_seconds": float(after)}


async def send_alerts(store: Store, alerts: dict | None) -> list[str]:
    """Tell an operator about plans stuck too long: once per plan, through their own command."""
    if not alerts:
        return []
    sent = []
    for plan in store.stuck_plans(alerts["after_seconds"]):
        payload = json.dumps({"plan_id": plan["plan_id"], "status": plan["status"], "goal": plan["raw"].get("goal"),
                              "stuck_since": plan["stuck_since"], "result": plan["result"]}).encode()
        try:
            proc = await asyncio.create_subprocess_exec(*alerts["command"], stdin=asyncio.subprocess.PIPE)
            await proc.communicate(payload)
        except OSError:
            log.exception("alert command failed for %s", plan["plan_id"])
            continue
        if proc.returncode == 0:
            store.mark_alerted(plan["plan_id"])
            sent.append(plan["plan_id"])
        else:
            log.warning("alert command exited %s for %s; will retry", proc.returncode, plan["plan_id"])
    return sent


@dataclass
class GatewayConfig:
    servers: dict[str, Any]
    manifest_path: str
    data_dir: str = ".planbound"
    caller: dict | None = None
    reader: dict | None = None
    runtime: dict[str, Any] | None = None  # Runtime keyword arguments, from parse_runtime_options
    reconcile_seconds: float = 0.0
    # {"command": [argv...], "after_seconds": 300}: run when a plan stays failed_partial or needs_attention
    # that long. It gets the plan as JSON on stdin, once per plan.
    alerts: dict | None = None

    @classmethod
    def load(cls, path: str | Path) -> GatewayConfig:
        base = Path(path).resolve().parent
        data = json.loads(Path(path).read_text())
        caller = data.get("caller")
        if os.environ.get("PLANBOUND_CALLER"):
            caller = json.loads(os.environ["PLANBOUND_CALLER"])
        return cls(
            servers=data["servers"],
            manifest_path=str(base / data.get("manifest", "planbound.effects.json")),
            data_dir=str(base / data.get("data_dir", ".planbound")),
            caller=caller,
            reader=data.get("reader"),
            runtime=parse_runtime_options(data.get("runtime")),
            reconcile_seconds=float((data.get("runtime") or {}).get("reconcile_seconds", 0)),
            alerts=parse_alerts(data.get("alerts")),
        )

    @property
    def db_path(self) -> str:
        return str(Path(self.data_dir) / "gateway.db")


def make_reader(spec: dict | None):
    if not spec:
        return None
    from .reader import AnthropicReader

    return AnthropicReader(model=spec.get("model", "claude-opus-5-5"), effort=spec.get("effort", "low"))


def search_index(runtime: Runtime, query: str = "", guide: bool = False) -> dict:
    """Compact index of allowed tools; full contracts only for tools matching `query`."""
    m = runtime.manifest
    catalog = runtime.upstreams.catalog
    tools = []
    for name in sorted(catalog):
        t = catalog[name]
        eff = m.effects_for(t, catalog)
        if eff is None:
            continue
        reviewed = not eff.inferred
        desc = (eff.description if reviewed and eff.description else t.description).strip()
        props = (t.input_schema or {}).get("properties", {})
        required = set((t.input_schema or {}).get("required", []))
        args = ", ".join(p if p in required else p + "?" for p in props)
        outs = ", ".join((t.output_schema or {}).get("properties", {}))
        entry: dict[str, Any] = {
            "signature": f"{name}({args}) -> {outs or '?'}",
            "effect": eff.state + ("" if eff.egress == "internal" else ", external"),
            "description": desc if reviewed else f"[unverified] {desc}",
        }
        if query and (query.lower() in name.lower() or query.lower() in desc.lower()):
            entry["input_schema"] = t.input_schema
            entry["output_schema"] = t.output_schema
        elif query:
            continue
        tools.append(entry)
    out: dict[str, Any] = {"mode": m.mode, "tools": tools}
    prepared = runtime.store.list_prepared(runtime.caller)
    if prepared:
        out["prepared"] = prepared  # name -> params: run these with planbound_run
    # The plan guide is in the server instructions; send it again only when asked.
    if guide:
        out["plan_guide"] = PLAN_GUIDE
    return out


class Gateway:
    def __init__(self, config: GatewayConfig):
        self.config = config
        self.runtime: Runtime | None = None

    @asynccontextmanager
    async def lifespan(self, _server: MCPServer):
        """Upstream connections live as long as the gateway, in one task."""
        async with MCPUpstreams(self.config.servers) as upstreams:
            self.runtime = self._make_runtime(upstreams)
            reconciler = None
            if self.config.reconcile_seconds > 0:
                # Its own Runtime, so its own lease identity: it can only take over plans whose lease has
                # expired, never one this gateway is still running.
                reconciler = asyncio.create_task(self._reconcile(self._make_runtime(upstreams)))
            try:
                yield {}
            finally:
                if reconciler:
                    reconciler.cancel()
                self.runtime = None

    def _make_runtime(self, upstreams) -> Runtime:
        return Runtime(
            Store(self.config.db_path),
            upstreams,
            Manifest.load(self.config.manifest_path),
            self.config.caller or {},
            reader=make_reader(self.config.reader),
            display_dir=Path(self.config.data_dir) / "display",
            **(self.config.runtime or {}),
        )

    async def _reconcile(self, runtime: Runtime) -> None:
        while True:
            await asyncio.sleep(self.config.reconcile_seconds)
            try:
                for report in await runtime.recover():
                    if report["status"] != "skipped":
                        log.info("reconciled %s: %s", report["plan_id"], report["status"])
                await send_alerts(runtime.store, self.config.alerts)
            except Exception:
                log.exception("background reconciliation failed")

    async def get_runtime(self) -> Runtime:
        if self.runtime is None:
            raise RuntimeError("gateway is not running")
        return self.runtime


def build_server(gateway: Gateway) -> MCPServer:
    server = MCPServer("planbound", instructions=PLAN_GUIDE, lifespan=gateway.lifespan)

    @server.tool(structured_output=True)
    async def planbound_search_tools(query: str = "", guide: bool = False) -> dict[str, Any]:
        """List the tools you may use in a plan. Pass a query for full schemas; guide=true for the plan guide."""
        return search_index(await gateway.get_runtime(), query, guide)

    @server.tool(structured_output=True)
    async def planbound_submit_plan(script: str = "", plan: dict | None = None, mode: str = "execute",
                                    wait_seconds: float = 20) -> dict[str, Any]:
        """Check, preview and run a plan: a `script` (see the guide) or a JSON `plan`.
        mode: "compile", "preview" or "execute"."""
        raw: Any = plan if plan is not None else script
        return await (await gateway.get_runtime()).submit(raw, mode=mode, wait_seconds=wait_seconds)

    @server.tool(structured_output=True)
    async def planbound_prepare(name: str, script: str = "", plan: dict | None = None,
                                example: dict | None = None) -> dict[str, Any]:
        """Save a plan with param.x placeholders under a name; `example` gives sample params to check it."""
        raw: Any = plan if plan is not None else script
        return await (await gateway.get_runtime()).prepare(name, raw, example or {})

    @server.tool(structured_output=True)
    async def planbound_run(name: str, params: dict | None = None, mode: str = "execute",
                            wait_seconds: float = 20) -> dict[str, Any]:
        """Run a prepared plan with these params. Same checks, preview and approval as any plan."""
        return await (await gateway.get_runtime()).run_prepared(name, params or {}, mode=mode,
                                                                 wait_seconds=wait_seconds)

    @server.tool(structured_output=True)
    async def planbound_commit(plan_id: str, wait_seconds: float = 20) -> dict[str, Any]:
        """Continue a plan that was waiting for approval, or run a previewed plan."""
        return await (await gateway.get_runtime()).commit(plan_id, wait_seconds=wait_seconds)

    @server.tool(structured_output=True)
    async def planbound_cancel(plan_id: str) -> dict[str, Any]:
        """Stop a plan before its point of no return; every step that ran is undone."""
        return (await gateway.get_runtime()).cancel(plan_id)

    @server.tool(structured_output=True)
    async def planbound_inspect_handle(handle: str, path: str = "", where: dict | None = None,
                                       group_by: str | None = None) -> dict[str, Any]:
        """Deterministic views of a result handle (h://plan/node): a sub-path, a filter, or group-by counts."""
        return (await gateway.get_runtime()).inspect(handle, path, where, group_by)

    return server
