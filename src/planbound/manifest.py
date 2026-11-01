"""The effects manifest: what each upstream tool really does, owned by the operator.

Upstream MCP annotations are hints written by the server author. Planbound only
trusts this file (enforce mode) or a conservative inference from hints (dev mode).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

STATES = ("read_only", "idempotent_write", "compensatable", "irreversible")
EGRESS = ("internal", "external")
WRITE_STATES = ("idempotent_write", "compensatable", "irreversible")

_READ_PREFIXES = ("get_", "list_", "search_", "read_", "fetch_", "find_", "describe_", "lookup_")
_UNDO_PREFIXES = ("cancel_", "delete_", "undo_", "revert_", "void_")


@dataclass
class UpstreamTool:
    """A tool as advertised by an upstream MCP server, namespaced as `server.tool`."""

    name: str
    description: str
    input_schema: dict
    output_schema: dict | None
    read_only_hint: bool = False

    def contract_sha256(self) -> str:
        """Hash of everything the model reads or relies on: name, description, schemas."""
        contract = {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
            "outputSchema": self.output_schema,
        }
        return hashlib.sha256(canonical(contract).encode()).hexdigest()


@dataclass
class ToolEffects:
    state: str
    egress: str = "internal"
    description: str | None = None
    contract_sha256: str | None = None
    output_labels: list[str] = field(default_factory=list)
    allowed_input_labels: list[str] | None = None
    output_trust: str = "untrusted"
    compensator: dict | None = None
    cost: str | None = None
    limits: dict = field(default_factory=dict)
    identity: dict | None = None
    requires_approval: bool = False
    idempotency_keys: bool = True
    inferred: bool = False

    def to_json(self) -> dict:
        out: dict[str, Any] = {"state": self.state, "egress": self.egress}
        for key in (
            "description",
            "contract_sha256",
            "compensator",
            "cost",
            "identity",
            "allowed_input_labels",
        ):
            value = getattr(self, key)
            if value is not None:
                out[key] = value
        if self.output_labels:
            out["output_labels"] = self.output_labels
        if self.output_trust != "untrusted":
            out["output_trust"] = self.output_trust
        if self.limits:
            out["limits"] = self.limits
        if self.requires_approval:
            out["requires_approval"] = True
        if not self.idempotency_keys:
            out["idempotency_keys"] = False
        return out


@dataclass
class Manifest:
    mode: str = "enforce"
    unlisted_tool_policy: str = "reject"
    max_plan_cost: float | None = None
    auto_approve_max_cost: float = 0.0
    approval_ttl_s: int = 300
    approvers: dict[str, str] = field(default_factory=dict)
    tools: dict[str, ToolEffects] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str | Path) -> Manifest:
        return cls.from_json(json.loads(Path(path).read_text()))

    @classmethod
    def from_json(cls, data: dict) -> Manifest:
        defaults = data.get("defaults", {})
        approval = data.get("approval", {})
        tools = {}
        for name, raw in data.get("tools", {}).items():
            state = raw.get("state")
            if state not in STATES:
                raise ValueError(f"tool {name}: state must be one of {STATES}, got {state!r}")
            egress = raw.get("egress", "internal")
            if egress not in EGRESS:
                raise ValueError(f"tool {name}: egress must be one of {EGRESS}, got {egress!r}")
            if state == "compensatable" and not raw.get("compensator"):
                raise ValueError(f"tool {name}: compensatable tools need a compensator")
            tools[name] = ToolEffects(
                state=state,
                egress=egress,
                description=raw.get("description"),
                contract_sha256=raw.get("contract_sha256"),
                output_labels=list(raw.get("output_labels", [])),
                allowed_input_labels=raw.get("allowed_input_labels"),
                output_trust=raw.get("output_trust", "untrusted"),
                compensator=raw.get("compensator"),
                cost=raw.get("cost"),
                limits=dict(raw.get("limits", {})),
                identity=raw.get("identity"),
                requires_approval=bool(raw.get("requires_approval", False)),
                idempotency_keys=bool(raw.get("idempotency_keys", True)),
            )
        return cls(
            mode=data.get("mode", "enforce"),
            unlisted_tool_policy=defaults.get("unlisted_tool_policy", "reject"),
            max_plan_cost=defaults.get("max_plan_cost"),
            auto_approve_max_cost=float(approval.get("auto_approve_max_cost", 0)),
            approval_ttl_s=int(approval.get("ttl_seconds", 300)),
            approvers=dict(approval.get("approvers", {})),
            tools=tools,
        )

    def to_json(self) -> dict:
        defaults: dict[str, Any] = {"unlisted_tool_policy": self.unlisted_tool_policy}
        if self.max_plan_cost is not None:
            defaults["max_plan_cost"] = self.max_plan_cost
        return {
            "version": "0.1",
            "mode": self.mode,
            "defaults": defaults,
            "approval": {
                "auto_approve_max_cost": self.auto_approve_max_cost,
                "ttl_seconds": self.approval_ttl_s,
                "approvers": self.approvers,
            },
            "tools": {name: t.to_json() for name, t in sorted(self.tools.items())},
        }

    def effects_for(self, tool: UpstreamTool, catalog: dict[str, UpstreamTool]) -> ToolEffects | None:
        """The effects Planbound enforces for a tool, or None if the tool may not be used."""
        if tool.name in self.tools:
            return self.tools[tool.name]
        if self.mode == "dev" or self.unlisted_tool_policy == "infer":
            return infer_effects(tool, catalog)
        return None


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _tokens(tool_name: str) -> set[str]:
    return set(tool_name.split(".")[-1].split("_"))


def infer_effects(tool: UpstreamTool, catalog: dict[str, UpstreamTool]) -> ToolEffects:
    """Conservative dev-mode guess. Anything not clearly read-only is irreversible."""
    short = tool.name.split(".")[-1]
    if tool.read_only_hint or short.startswith(_READ_PREFIXES):
        return ToolEffects(state="read_only", inferred=True, contract_sha256=tool.contract_sha256())
    compensator = _guess_compensator(tool, catalog)
    if compensator:
        return ToolEffects(
            state="compensatable",
            compensator=compensator,
            inferred=True,
            contract_sha256=tool.contract_sha256(),
        )
    return ToolEffects(state="irreversible", inferred=True, contract_sha256=tool.contract_sha256())


def _guess_compensator(tool: UpstreamTool, catalog: dict[str, UpstreamTool]) -> dict | None:
    server = tool.name.split(".")[0]
    mine = _tokens(tool.name)
    out_fields = set((tool.output_schema or {}).get("properties", {}))
    for other in catalog.values():
        short = other.name.split(".")[-1]
        if other.name == tool.name or not other.name.startswith(server + ".") or not short.startswith(_UNDO_PREFIXES):
            continue
        if not (_tokens(other.name) - {short.split("_")[0]}) & mine:
            continue
        required = other.input_schema.get("required", [])
        if len(required) == 1 and required[0] in out_fields:
            return {"tool": other.name, "args": {required[0]: f"$output.{required[0]}"}}
    return None


_PATH_RE = re.compile(r"^\$(input|output)\.([A-Za-z0-9_.]+)$")


def parse_io_path(expr: str) -> tuple[str, list[str]]:
    """Parse `$input.a.b` / `$output.a` into ("input", ["a", "b"])."""
    m = _PATH_RE.match(expr)
    if not m:
        raise ValueError(f"expected $input.<path> or $output.<path>, got {expr!r}")
    return m.group(1), m.group(2).split(".")
