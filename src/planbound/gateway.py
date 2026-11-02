"""The gateway: an ordinary MCP server that agents talk to instead of the upstream servers."""

from __future__ import annotations

import json
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
Planbound runs your tool calls as one bounded plan. Instead of calling tools one by one,
send a plan to planbound_submit_plan. You get back summaries computed by code (counts, sums,
ids), never raw text, plus handles (h://plan/node) to the full data.

Plan: {"planbound": "0.1", "goal": "...", "nodes": [...], "return": ["node", ...]}
Nodes (each has a unique "id", lowercase):
  {"id","op":"call","tool":"server.tool","args":{...},"when"?:cond,"depends_on"?:[ids]}
  {"id","op":"filter","input":{"$ref":"node.path"},"where":cond}
  {"id","op":"partition","input":{"$ref":...},"where":cond}   -> output {"match":[...],"rest":[...]}
  {"id","op":"map","input":{"$ref":...},"max_items":N,"call":{"tool":...,"args":{...}}}
  {"id","op":"quarantine_read","input":{"$ref":...},"field":"text_field","max_items":N,
   "schema":{"name":{"enum":[...]} | {"type":"boolean"|"integer"|"number"}}}
  {"id","op":"display","input":{"$ref":...}}   shows data to the human only, never to you
Values: literals, {"$ref":"node.a.b"}, {"$item":"field"} (inside map args / where),
  {"$agg":{"fn":"count|distinct_count|sum|min|max|mean","of":"node.path","field":"f"}},
  {"$caller":"attr"} (the signed-in user; required where a tool acts on "your own" record).
Conditions: {"eq"|"ne"|"gt"|"gte"|"lt"|"lte":[a,b]}, {"in":[a,list]}, {"and":[...]}, {"or":[...]},
  {"not":c}, {"exists":a}. "when" may only use data that is known before any write.
Rules: every map needs max_items; no loops; at most one irreversible step and it runs after
every undoable step; errors come back with a code, the JSON path and a hint, so fix and resend.
Modes: "compile" (check only), "preview" (run reads, show exact impact), "execute" (default).
"""


# Runtime knobs a config file may set under "runtime", with their bounds.
RUNTIME_OPTIONS = {
    # How long a plan's lease lasts without renewal. Longer than your slowest upstream write call,
    # shorter than you are willing to wait before `planbound recover` may take over a dead gateway.
    "lease_seconds": ("lease_ttl_s", float, 1.0, 86400.0),
    "retry_attempts": ("retry_attempts", int, 1, 20),
    "compensation_attempts": ("compensation_attempts", int, 1, 50),
}


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
        out[kwarg] = kind(value)
    return out


@dataclass
class GatewayConfig:
    servers: dict[str, Any]
    manifest_path: str
    data_dir: str = ".planbound"
    caller: dict | None = None
    reader: dict | None = None
    runtime: dict[str, Any] | None = None  # Runtime keyword arguments, from parse_runtime_options

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
        )

    @property
    def db_path(self) -> str:
        return str(Path(self.data_dir) / "gateway.db")


def make_reader(spec: dict | None):
    if not spec:
        return None
    from .reader import AnthropicReader

    return AnthropicReader(model=spec.get("model", "claude-opus-5-5"), effort=spec.get("effort", "low"))


def search_index(runtime: Runtime, query: str = "") -> dict:
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
    return {"mode": m.mode, "tools": tools, "plan_guide": PLAN_GUIDE if not query else None}


class Gateway:
    def __init__(self, config: GatewayConfig):
        self.config = config
        self.runtime: Runtime | None = None

    @asynccontextmanager
    async def lifespan(self, _server: MCPServer):
        """Upstream connections live as long as the gateway, in one task."""
        async with MCPUpstreams(self.config.servers) as upstreams:
            self.runtime = Runtime(
                Store(self.config.db_path),
                upstreams,
                Manifest.load(self.config.manifest_path),
                self.config.caller or {},
                reader=make_reader(self.config.reader),
                display_dir=Path(self.config.data_dir) / "display",
                **(self.config.runtime or {}),
            )
            try:
                yield {}
            finally:
                self.runtime = None

    async def get_runtime(self) -> Runtime:
        if self.runtime is None:
            raise RuntimeError("gateway is not running")
        return self.runtime


def build_server(gateway: Gateway) -> MCPServer:
    server = MCPServer("planbound", instructions=PLAN_GUIDE, lifespan=gateway.lifespan)

    @server.tool(structured_output=True)
    async def planbound_search_tools(query: str = "") -> dict[str, Any]:
        """List the tools you may use in a plan. Pass a query to get full schemas for matching tools."""
        return search_index(await gateway.get_runtime(), query)

    @server.tool(structured_output=True)
    async def planbound_submit_plan(plan: dict, mode: str = "execute", wait_seconds: float = 20) -> dict[str, Any]:
        """Check, preview and run a plan. mode: "compile", "preview" or "execute"."""
        return await (await gateway.get_runtime()).submit(plan, mode=mode, wait_seconds=wait_seconds)

    @server.tool(structured_output=True)
    async def planbound_commit(plan_id: str, wait_seconds: float = 20) -> dict[str, Any]:
        """Continue a plan that was waiting for approval, or run a previewed plan."""
        return await (await gateway.get_runtime()).commit(plan_id, wait_seconds=wait_seconds)

    @server.tool(structured_output=True)
    async def planbound_inspect_handle(handle: str, path: str = "", where: dict | None = None,
                                       group_by: str | None = None) -> dict[str, Any]:
        """Deterministic views of a result handle (h://plan/node): a sub-path, a filter, or group-by counts."""
        return (await gateway.get_runtime()).inspect(handle, path, where, group_by)

    return server
