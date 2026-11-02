"""The compiler: every check that can run before any upstream call.

All diagnostics are returned together so the model can fix a plan in one retry.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .manifest import (
    Manifest,
    ToolEffects,
    UpstreamTool,
    attestation_problems,
    canonical,
)
from .plan import (
    AGG_FNS,
    ID_RE,
    OPS,
    Node,
    Plan,
    dependencies,
    is_special,
    node_conditions,
    node_value_fields,
    parse_plan,
    ref_root,
    split_path,
    walk_condition,
    walk_values,
)

SUPPORTED_VERSION = "0.1"
QUARANTINE_TYPES = ("boolean", "integer", "number")
# Provenance label on everything a quarantined reader decided, and everything computed from it. It is
# not a data-flow restriction (any tool may receive it); it sends writes it reaches to a human.
READER_LABEL = "reader_derived"
HANDLE_RE = re.compile(r"^h://(p_[0-9a-f]+)/([a-z][a-z0-9_]{0,31})$")
# Plans whose saved results may seed a later plan: completed ones, and ones that never wrote anything
# (every handle they hold is a real read).
CHECKPOINT_SOURCE_STATUSES = ("completed", "previewed", "awaiting_approval", "stale", "rejected")


@dataclass
class Diagnostic:
    code: str
    node_id: str | None
    path: str
    message: str
    hint: str | None = None

    def to_json(self) -> dict:
        out = {"code": self.code, "node_id": self.node_id, "path": self.path, "message": self.message}
        if self.hint:
            out["hint"] = self.hint
        return out


class CompileError(Exception):
    def __init__(self, diagnostics: list[Diagnostic]):
        super().__init__(f"{len(diagnostics)} compile error(s)")
        self.diagnostics = diagnostics

    def to_json(self) -> dict:
        return {"status": "compile_error", "errors": [d.to_json() for d in self.diagnostics]}


@dataclass
class NodeInfo:
    node: Node
    kind: str  # "read" | "pure" | "write"
    state: str | None = None
    tool: str | None = None
    effects: ToolEffects | None = None
    labels: set[str] = field(default_factory=set)
    trust: str = "untrusted"
    checkpoint: dict | None = None  # for op "checkpoint": the saved result it loads


@dataclass
class CompiledPlan:
    plan: Plan
    plan_hash: str
    info: dict[str, NodeInfo]
    deps: dict[str, set[str]]
    sched_deps: dict[str, set[str]]
    order: list[str]
    waves: list[list[str]]
    read_frontier: set[str]
    pivot: str | None
    worst_case: dict
    warnings: list[str]

    def summary(self) -> dict:
        return {
            "status": "compiled",
            "plan_hash": self.plan_hash,
            "waves": self.waves,
            "read_frontier": [n for n in self.order if n in self.read_frontier],
            "pivot": self.pivot,
            "worst_case": self.worst_case,
            "warnings": self.warnings,
        }


# ------------------------------------------------------------------ schemas


class SchemaView:
    """Walks JSON Schema (with local $refs) to check field paths. None = unknown shape."""

    def __init__(self, schema: dict | None, root: dict | None = None):
        self.root = root if root is not None else schema
        self.schema = self._deref(schema)

    def _deref(self, s: dict | None) -> dict | None:
        seen = 0
        while isinstance(s, dict) and "$ref" in s and seen < 10:
            ref = s["$ref"]
            if not ref.startswith("#/") or self.root is None:
                return None
            node: Any = self.root
            for part in ref[2:].split("/"):
                node = node.get(part) if isinstance(node, dict) else None
            s = node
            seen += 1
        if isinstance(s, dict) and "anyOf" in s:
            branches = [b for b in s["anyOf"] if isinstance(b, dict) and b.get("type") != "null"]
            if len(branches) == 1:
                return self._deref(branches[0])
            return None
        return s if isinstance(s, dict) else None

    @property
    def known(self) -> bool:
        return self.schema is not None

    @property
    def type(self) -> str | None:
        return self.schema.get("type") if self.schema else None

    def fields(self) -> list[str]:
        return sorted((self.schema or {}).get("properties", {}))

    def child(self, seg: str) -> SchemaView:
        s = self.schema
        if s is None:
            return SchemaView(None, self.root)
        if s.get("type") == "array" or "items" in s:
            return SchemaView(s.get("items"), self.root)
        props = s.get("properties")
        if props is None:
            return SchemaView(None, self.root)
        if seg not in props:
            if s.get("additionalProperties") is True:
                return SchemaView(None, self.root)
            raise KeyError(seg)
        return SchemaView(props[seg], self.root)

    def items(self) -> SchemaView:
        s = self.schema
        if s is None:
            return SchemaView(None, self.root)
        return SchemaView(s.get("items"), self.root)

    def walk(self, segments: list[str]) -> SchemaView:
        view = self
        for seg in segments:
            view = view.child(seg)
            if not view.known:
                return view
        return view


def _list_schema(items: SchemaView) -> SchemaView:
    if not items.known:
        return SchemaView(None)
    return SchemaView({"type": "array", "items": items.schema}, items.root)


def _literal_type_ok(value: Any, schema: SchemaView) -> bool:
    if not schema.known or is_special(value) or schema.type is None:
        return True
    t = schema.type
    if isinstance(t, list):
        return True
    checks = {
        "string": lambda v: isinstance(v, str),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "boolean": lambda v: isinstance(v, bool),
        "object": lambda v: isinstance(v, dict),
        "array": lambda v: isinstance(v, list),
        "null": lambda v: v is None,
    }
    if isinstance(value, (dict, list)) and t in ("object", "array"):
        return True
    return checks.get(t, lambda v: True)(value)


# ------------------------------------------------------------------ compiler


def plan_hash(raw: dict) -> str:
    return "sha256:" + hashlib.sha256(canonical(raw).encode()).hexdigest()


def compile_plan(
    raw: Any,
    *,
    catalog: dict[str, UpstreamTool],
    manifest: Manifest,
    caller: dict,
    reader_configured: bool = False,
    handles: Callable[[str, str], dict | None] | None = None,
    now: float | None = None,
) -> CompiledPlan:
    """`handles(plan_id, node_id)` loads a saved result for `checkpoint` nodes. `now` enables their
    `max_age_seconds` check (left out when recovering a plan that already started)."""
    diags: list[Diagnostic] = []

    def err(code: str, node: Node | None, suffix: str, message: str, hint: str | None = None) -> None:
        base = node.path if node else "$"
        diags.append(Diagnostic(code, node.id if node else None, base + suffix, message, hint))

    if not isinstance(raw, dict) or not isinstance(raw.get("nodes"), list) or not raw["nodes"]:
        raise CompileError([Diagnostic("E_PLAN_SHAPE", None, "$", "A plan is an object with a non-empty 'nodes' list.",
                                       'Send {"planbound": "0.1", "nodes": [...]}.')])
    if raw.get("planbound") != SUPPORTED_VERSION:
        err("E_PLAN_SHAPE", None, ".planbound", f"Plan must declare \"planbound\": \"{SUPPORTED_VERSION}\".")

    plan = parse_plan(raw)
    info: dict[str, NodeInfo] = {}
    roles = set(caller.get("roles", []))

    # ---- ids, ops, per-op shape, tools
    for nid, node in plan.nodes.items():
        r = node.raw
        if "#dup" in nid:
            err("E_DUPLICATE_ID", node, ".id", f"Node id {nid.split('#')[0]!r} is used more than once.")
            continue
        if not isinstance(r.get("id"), str) or not ID_RE.match(r["id"]):
            err("E_BAD_ID", node, ".id", f"Node id {r.get('id')!r} must match {ID_RE.pattern}.")
        if node.op not in OPS:
            err("E_UNKNOWN_OP", node, ".op", f"Unknown op {node.op!r}.", f"Use one of: {', '.join(OPS[:6])}.")
            continue
        if node.op == "checkpoint":
            ni = NodeInfo(node=node, kind="read")
            info[nid] = ni
            _check_checkpoint(node, ni, handles, caller, now, err)
            continue
        if node.op == "quarantine_read" and not reader_configured:
            err("E_QUARANTINE_READER_UNCONFIGURED", node, ".op",
                "This gateway has no quarantined reader model configured.",
                "Start the gateway with a reader (see README), or avoid reading free text.")
        if (node.op in ("map", "filter", "partition", "quarantine_read", "display")
                and is_special(r.get("input")) != "$ref"):
            err("E_PLAN_SHAPE", node, ".input", f"'{node.op}' needs \"input\": {{\"$ref\": \"<node>\"}}.")
        if node.op in ("filter", "partition") and not isinstance(r.get("where"), dict):
            err("E_PLAN_SHAPE", node, ".where", f"'{node.op}' needs a 'where' condition.")
        if node.op in ("map", "quarantine_read"):
            bound = r.get("max_items")
            if not isinstance(bound, int) or isinstance(bound, bool) or bound < 1:
                err("E_MISSING_MAP_BOUND", node, ".max_items",
                    f"Every '{node.op}' must declare an integer 'max_items' bound (>= 1).",
                    f"Add \"max_items\": <int> to node '{nid}'.")
        if node.op == "quarantine_read":
            _check_quarantine_schema(node, err)

        tool_name = None
        call_spec = None
        if node.op == "call":
            tool_name, call_spec = r.get("tool"), r
        elif node.op == "map":
            call_spec = r.get("call") if isinstance(r.get("call"), dict) else None
            tool_name = call_spec.get("tool") if call_spec else None
            if call_spec is None:
                err("E_PLAN_SHAPE", node, ".call", "'map' needs \"call\": {\"tool\": ..., \"args\": {...}}.")
        ni = NodeInfo(node=node, kind="pure" if node.op != "quarantine_read" else "read")
        if node.op == "display":
            ni.trust = "trusted"  # its output is the gateway's own receipt, not upstream data
        info[nid] = ni
        if node.op not in ("call", "map"):
            continue
        if not isinstance(tool_name, str):
            err("E_PLAN_SHAPE", node, ".tool" if node.op == "call" else ".call.tool", "Missing tool name.")
            continue
        args_suffix = ".args" if node.op == "call" else ".call.args"
        tool = catalog.get(tool_name)
        if tool is None:
            near = [t for t in catalog if t.split(".")[-1] in tool_name or tool_name.split(".")[-1] in t]
            err("E_UNKNOWN_TOOL", node, args_suffix.replace(".args", ".tool"), f"No upstream tool named {tool_name!r}.",
                f"Did you mean: {', '.join(near[:3])}?" if near else "Use planbound_search_tools to list tools.")
            continue
        eff = manifest.effects_for(tool, catalog)
        if eff is None:
            err("E_UNLISTED_TOOL", node, args_suffix.replace(".args", ".tool"),
                f"Tool {tool_name!r} is not in the effects manifest and unlisted tools are rejected.",
                "Ask the operator to review it and add it to planbound.effects.json.")
            continue
        if eff.contract_sha256 and not eff.inferred and eff.contract_sha256 != tool.contract_sha256():
            err("E_SCHEMA_CHANGED", node, args_suffix.replace(".args", ".tool"),
                f"Tool {tool_name!r} changed (name, description or schema) since the operator reviewed it.",
                "The operator must re-review the tool and update contract_sha256.")
            continue
        if eff.state == "read_only" and tool.destructive_hint:
            err("E_EFFECTS_CONFLICT", node, args_suffix.replace(".args", ".tool"),
                f"The manifest says {tool_name!r} is read_only, but the server marks it destructive.",
                "The operator must check the tool and correct the effects manifest.")
            continue
        ni.tool, ni.effects, ni.state = tool_name, eff, eff.state
        ni.kind = "read" if eff.state == "read_only" else "write"
        ni.trust = eff.output_trust
        args = call_spec.get("args", {}) if call_spec else {}
        if not isinstance(args, dict):
            err("E_PLAN_SHAPE", node, args_suffix, "'args' must be an object.")
            continue
        _check_args(node, args_suffix, args, tool, err)
        if eff.identity and eff.identity.get("unless_role") not in roles:
            arg = eff.identity["arg"]
            want = {"$caller": eff.identity.get("caller", arg)}
            if args.get(arg) != want:
                err("E_IDENTITY", node, f"{args_suffix}.{arg}",
                    f"Tool {tool_name!r} may only act on the caller's own record.",
                    f"Set \"{arg}\": {canonical(want)}. Acting on others needs role "
                    f"{eff.identity.get('unless_role')!r}.")
        if eff.conditional:
            carg = eff.conditional["arg"]
            value = args.get(carg)
            if not (isinstance(value, dict) and ("$ref" in value or "$item" in value)):
                err("E_CONDITIONAL_REQUIRED", node, f"{args_suffix}.{carg}",
                    f"Tool {tool_name!r} is a conditional write: {carg!r} must carry the value read earlier "
                    "in this plan, so the server can refuse the write if it changed since.",
                    f"Set \"{carg}\" to a {{\"$ref\"}} or {{\"$item\"}} from the read that produced it.")
        if manifest.strict_writes and eff.sensitive:
            missing = []
            if not eff.idempotency_keys:
                missing.append("idempotency keys")
            if eff.conditional is None:
                missing.append("a conditional write")
            if not eff.fencing:
                missing.append("fencing tokens")
            if missing:
                err("E_UNSAFE_WRITE", node, args_suffix.replace(".args", ".tool"),
                    f"Strict writes: {tool_name!r} moves money or needs approval, but the manifest does not "
                    f"declare {' or '.join(missing)} for it.",
                    "The operator must confirm the upstream supports them (idempotency_keys, conditional, "
                    "fencing) or turn off defaults.strict_writes.")
            elif manifest.require_verified:
                problems = attestation_problems(eff, tool.contract_sha256())
                if problems:
                    err("E_UNVERIFIED", node, args_suffix.replace(".args", ".tool"),
                        f"{tool_name!r}: " + "; ".join(problems) + ".",
                        f"Run `planbound verify {tool_name} --args '<sample args>'` against a sandbox and add "
                        "the attestation to the manifest as \"verified\".")

    if diags:
        raise CompileError(diags)

    # ---- references and dependencies
    deps: dict[str, set[str]] = {}
    for nid, node in plan.nodes.items():
        deps[nid] = dependencies(node)
        for dep in deps[nid]:
            if dep not in plan.nodes:
                err("E_UNKNOWN_REF", node, "", f"Node {nid!r} references unknown node {dep!r}.",
                    f"Known nodes: {', '.join(plan.nodes)}.")
        _check_specials(node, caller, err)
    for rid in plan.returns:
        if rid not in plan.nodes:
            err("E_UNKNOWN_REF", None, ".return", f"'return' lists unknown node {rid!r}.")
    if diags:
        raise CompileError(diags)

    order = _toposort(plan.nodes, deps)
    if order is None:
        raise CompileError([Diagnostic("E_CYCLE", None, "$.nodes", "The plan's references form a cycle.",
                                       "A plan must be a DAG: a node can only use outputs of nodes before it.")])

    # ---- schemas: field paths and $agg types
    schemas: dict[str, SchemaView] = {}
    for nid in order:
        _check_fields(info[nid], schemas, catalog, err)
        schemas[nid] = _output_schema(info[nid], schemas, catalog)
    if diags:
        raise CompileError(diags)

    # ---- read frontier and conditions
    read_frontier: set[str] = set()
    for nid in order:
        if info[nid].kind in ("read", "pure") and deps[nid] <= read_frontier:
            read_frontier.add(nid)
    for nid in order:
        node = plan.nodes[nid]
        when = node.raw.get("when")
        if when is not None:
            for kind, v in walk_condition(when):
                root = ref_root(kind, v)
                if root and root not in read_frontier:
                    err("E_WHEN_ON_WRITE", node, ".when",
                        f"'when' may only use values known before any write; {root!r} depends on a write.",
                        "Base the condition on the read data that feeds the write instead.")

    # ---- labels
    for nid in order:
        ni = info[nid]
        incoming: set[str] = set()
        for value in node_value_fields(ni.node).values():
            for kind, v in walk_values(value):
                root = ref_root(kind, v)
                if root:
                    incoming |= info[root].labels
        if ni.checkpoint is not None:
            continue  # labels and trust come with the saved result
        for cond in node_conditions(ni.node).values():  # a write gated on a reader's answer is driven by it
            for kind, v in walk_condition(cond):
                root = ref_root(kind, v)
                if root and READER_LABEL in info[root].labels:
                    incoming.add(READER_LABEL)
        if ni.node.op == "quarantine_read":
            incoming.add(READER_LABEL)
        if ni.effects:
            ok = True
            restricted = incoming - {READER_LABEL}
            if ni.effects.allowed_input_labels is not None:
                bad = restricted - set(ni.effects.allowed_input_labels)
                ok = not bad
            elif ni.effects.egress == "external":
                bad = restricted
                ok = not bad
            if not ok:
                err("E_LABEL_FLOW", ni.node, "",
                    f"Data labelled {sorted(bad)} would flow into {ni.tool!r}, which does not accept it"
                    + (" (external egress)." if ni.effects.egress == "external" else "."),
                    "Remove the labelled fields from this call, or route through an approved internal tool.")
            ni.labels = incoming | set(ni.effects.output_labels)
        else:
            ni.labels = incoming

    # ---- saga shape
    writes = [n for n in order if info[n].kind == "write"]
    irreversible = [n for n in writes if info[n].state == "irreversible"]
    compensatable = [n for n in writes if info[n].state == "compensatable"]
    idempotent = [n for n in writes if info[n].state == "idempotent_write"]
    descendants = _descendants(order, deps)
    if len(irreversible) > 1 and not plan.allow_partial_irreversible:
        for n in irreversible[1:]:
            err("E_MULTIPLE_PIVOTS", plan.nodes[n], "",
                f"Plan has more than one irreversible step ({', '.join(irreversible)}). If the second fails, "
                "the first cannot be undone.",
                "Split into two plans, or set \"allow_partial_irreversible\": true to accept that risk.")
    for p in irreversible:
        for d in descendants[p]:
            if info[d].state in ("compensatable", "irreversible"):
                err("E_IRREVERSIBLE_NOT_TERMINAL", plan.nodes[d], "",
                    f"{d!r} ({info[d].state}) runs after irreversible step {p!r}; it could not be undone safely.",
                    "Only read-only or idempotent_write steps may follow the point of no return.")
    if diags:
        raise CompileError(diags)

    sched = {n: set(deps[n]) for n in order}
    for p in irreversible:
        sched[p] |= {c for c in compensatable if c not in descendants[p]}
    # Retry-safe writes run after everything that could still be undone or that is the point of no return.
    for i in idempotent:
        sched[i] |= set(compensatable + irreversible)
    sched_order = _toposort(plan.nodes, sched)
    if sched_order is None:
        for i in idempotent:
            for w in compensatable + irreversible:
                if i in _ancestors_of(w, deps):
                    err("E_SAGA_ORDER", plan.nodes[i], "",
                        f"idempotent_write {i!r} feeds {w!r}; if {w!r} fails, {i!r} could not be undone.",
                        "Make the idempotent write depend on the other writes, not the other way round.")
        raise CompileError(diags or [Diagnostic("E_SAGA_ORDER", None, "$.nodes", "Writes cannot be ordered safely.")])

    # ---- static worst case and limits
    worst = _worst_case(plan, info, writes)
    for tool_name, cost in worst["cost_by_tool"].items():
        eff = next(info[n].effects for n in writes if info[n].tool == tool_name)
        cap = eff.limits.get("max_per_plan")
        if cost is not None and cap is not None and cost > cap:
            per_call = eff.limits.get("max_per_call")
            node = next(plan.nodes[n] for n in writes if info[n].tool == tool_name)
            err("E_COST_BOUND", node, "",
                f"Worst case for {tool_name!r} is {cost:g}, above its per-plan limit {cap:g}.",
                f"Lower max_items to at most {int(cap // per_call)}." if per_call else None)
    if manifest.max_plan_cost is not None and worst["cost"] is not None and worst["cost"] > manifest.max_plan_cost:
        err("E_COST_BOUND", None, "", f"Worst-case plan cost {worst['cost']:g} exceeds the plan limit "
            f"{manifest.max_plan_cost:g}.", "Lower the max_items bounds.")
    if diags:
        raise CompileError(diags)

    warnings = []
    if any(info[n].effects and info[n].effects.inferred for n in order):
        warnings.append("dev mode: some effects are inferred, not reviewed. Results are unverified.")
    return CompiledPlan(
        plan=plan,
        plan_hash=plan_hash(raw),
        info=info,
        deps=deps,
        sched_deps=sched,
        order=sched_order,
        waves=_waves(sched_order, sched),
        read_frontier=read_frontier,
        pivot=irreversible[0] if irreversible else None,
        worst_case={k: v for k, v in worst.items() if k != "cost_by_tool"},
        warnings=warnings,
    )


# ------------------------------------------------------------------ helpers


def _check_quarantine_schema(node: Node, err) -> None:
    schema = node.raw.get("schema")
    if not isinstance(node.raw.get("field"), str):
        err("E_PLAN_SHAPE", node, ".field", "'quarantine_read' needs the text 'field' to read from each item.")
    if not isinstance(schema, dict) or not schema:
        err("E_QUARANTINE_SCHEMA", node, ".schema", "'quarantine_read' needs a non-empty output 'schema'.")
        return
    for name, spec in schema.items():
        closed = isinstance(spec, dict) and (
            (isinstance(spec.get("enum"), list) and spec["enum"] and all(isinstance(e, str) and len(e) <= 32 for e in spec["enum"]))
            or spec.get("type") in QUARANTINE_TYPES
        )
        if not closed:
            err("E_QUARANTINE_SCHEMA", node, f".schema.{name}",
                f"Field {name!r} must be a closed type: an enum of short strings, boolean, integer or number.",
                "Free text from a quarantined reader would reopen the prompt-injection channel.")
    fallback = node.raw.get("fallback")
    if fallback is not None:
        if not isinstance(fallback, dict):
            err("E_QUARANTINE_SCHEMA", node, ".fallback", "'fallback' maps schema fields to closed values.")
            return
        for name, value in fallback.items():
            spec = schema.get(name) if isinstance(schema, dict) else None
            kinds = {"boolean": (bool,), "integer": (int,), "number": (int, float)}
            ok = isinstance(spec, dict) and (
                value in spec["enum"] if "enum" in spec else
                isinstance(value, kinds.get(spec.get("type"), ())) and
                (spec.get("type") == "boolean" or not isinstance(value, bool)))
            if not ok:
                err("E_QUARANTINE_SCHEMA", node, f".fallback.{name}",
                    f"fallback {name!r} must be a valid value of the schema field {name!r}.")


def _check_args(node: Node, suffix: str, args: dict, tool: UpstreamTool, err) -> None:
    schema = tool.input_schema or {}
    props = schema.get("properties", {})
    for req in schema.get("required", []):
        if req not in args:
            err("E_MISSING_ARG", node, f"{suffix}.{req}", f"Tool {tool.name!r} requires argument {req!r}.",
                f"Arguments: {', '.join(sorted(props))}.")
    if schema.get("additionalProperties") is not True:
        for name in args:
            if name not in props:
                err("E_UNKNOWN_ARG", node, f"{suffix}.{name}", f"Tool {tool.name!r} has no argument {name!r}.",
                    f"Arguments: {', '.join(sorted(props))}.")
    root = schema
    for name, value in args.items():
        if name in props and not _literal_type_ok(value, SchemaView(props[name], root)):
            err("E_ARG_TYPE", node, f"{suffix}.{name}",
                f"Argument {name!r} of {tool.name!r} expects {SchemaView(props[name], root).type}, "
                f"got {type(value).__name__}.")


def _check_specials(node: Node, caller: dict, err) -> None:
    item_ok = {"call.args", "where"}
    for key, value in {**node_value_fields(node), **node_conditions(node)}.items():
        walker = walk_condition(value) if key in ("when", "where") else walk_values(value)
        for kind, v in walker:
            if kind == "$item" and key not in item_ok:
                err("E_ITEM_OUTSIDE_MAP", node, f".{key}", "'$item' is only allowed inside map call args "
                    "and filter/partition 'where'.")
            if kind == "$caller" and v["$caller"] not in caller:
                err("E_UNKNOWN_CALLER_ATTR", node, f".{key}", f"The caller has no attribute {v['$caller']!r}.",
                    f"Caller attributes: {', '.join(sorted(caller)) or '(none)'}.")
            if kind == "$agg":
                spec = v["$agg"]
                if not isinstance(spec, dict) or spec.get("fn") not in AGG_FNS or not isinstance(spec.get("of"), str):
                    err("E_BAD_AGG", node, f".{key}", f"$agg needs 'fn' in {AGG_FNS} and 'of': \"<node>\".")


def _check_checkpoint(node: Node, ni: NodeInfo, handles, caller: dict, now: float | None, err) -> None:
    r = node.raw
    handle = r.get("handle")
    m = HANDLE_RE.match(handle) if isinstance(handle, str) else None
    if not m:
        err("E_PLAN_SHAPE", node, ".handle", "'checkpoint' needs \"handle\": \"h://<plan_id>/<node_id>\".",
            "Use a handle returned in an earlier plan's summaries.")
        return
    age_limit = r.get("max_age_seconds")
    if age_limit is not None and (isinstance(age_limit, bool) or not isinstance(age_limit, (int, float))
                                  or age_limit <= 0):
        err("E_PLAN_SHAPE", node, ".max_age_seconds", "'max_age_seconds' must be a positive number.")
        return
    saved = handles(m.group(1), m.group(2)) if handles else None
    if saved is None:
        err("E_UNKNOWN_HANDLE", node, ".handle", f"No saved result {handle!r}.",
            "Use a handle exactly as an earlier plan's summary returned it.")
        return
    if saved["caller"] != caller:
        err("E_HANDLE_CALLER", node, ".handle", f"{handle!r} belongs to another caller's plan.",
            "A plan may only continue from its own caller's results.")
        return
    if saved["plan_status"] not in CHECKPOINT_SOURCE_STATUSES:
        err("E_HANDLE_UNUSABLE", node, ".handle",
            f"{handle!r} comes from a plan that ended {saved['plan_status']}; its results may describe writes "
            "that were undone or never finished.", "Re-read the data in this plan instead.")
        return
    if saved["skipped"]:
        err("E_UNKNOWN_HANDLE", node, ".handle", f"{handle!r} was skipped, so it holds no data.")
        return
    if now is not None and age_limit is not None and now - saved["created_at"] > age_limit:
        err("E_HANDLE_EXPIRED", node, ".handle",
            f"{handle!r} is {now - saved['created_at']:.0f}s old, above max_age_seconds {age_limit:g}.",
            "Re-read the data, or raise max_age_seconds if older data is acceptable.")
        return
    ni.checkpoint = saved
    ni.labels = set(saved["labels"])
    ni.trust = saved["trust"]


def schema_of(value: Any) -> dict:
    """A JSON Schema describing concrete saved data, so later plans get field checks on it."""
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, int):
        return {"type": "integer"}
    if isinstance(value, float):
        return {"type": "number"}
    if isinstance(value, str):
        return {"type": "string"}
    if isinstance(value, dict):
        return {"type": "object", "properties": {k: schema_of(v) for k, v in value.items()}}
    if isinstance(value, list):
        props: dict[str, dict] = {}
        scalar: dict | None = None
        for item in value:
            if isinstance(item, dict):
                for k, v in item.items():
                    props.setdefault(k, schema_of(v))
            elif item is not None:
                scalar = scalar or schema_of(item)
        if props:
            return {"type": "array", "items": {"type": "object", "properties": props}}
        return {"type": "array", "items": scalar} if scalar else {"type": "array"}
    return {}


def _output_schema(ni: NodeInfo, schemas: dict[str, SchemaView], catalog: dict[str, UpstreamTool]) -> SchemaView:
    node = ni.node
    if node.op == "checkpoint" and ni.checkpoint is not None:
        return SchemaView(schema_of(ni.checkpoint["data"]))
    if node.op == "call" and ni.tool:
        return SchemaView(catalog[ni.tool].output_schema)
    if node.op == "map" and ni.tool:
        return _list_schema(SchemaView(catalog[ni.tool].output_schema))
    if node.op in ("filter", "quarantine_read", "partition"):
        src = _ref_schema(node.raw["input"]["$ref"], schemas)
        if node.op == "partition":
            if not src.known:
                return SchemaView(None)
            return SchemaView({"type": "object", "properties": {"match": src.schema, "rest": src.schema}}, src.root)
        if node.op == "quarantine_read" and src.known and src.items().known:
            item = dict(src.items().schema)
            props = dict(item.get("properties", {}))
            for name, spec in (node.raw.get("schema") or {}).items():
                props[name] = {"type": "string", "enum": spec["enum"]} if "enum" in spec else {"type": spec["type"]}
            item["properties"] = props
            return SchemaView({"type": "array", "items": item}, src.root)
        return src
    return SchemaView(None)


def _ref_schema(ref: str, schemas: dict[str, SchemaView]) -> SchemaView:
    segs = split_path(ref)
    base = schemas.get(segs[0], SchemaView(None))
    try:
        return base.walk(segs[1:])
    except KeyError:
        return SchemaView(None)


def _check_fields(ni: NodeInfo, schemas: dict[str, SchemaView], catalog, err) -> None:
    node = ni.node
    item_schema = SchemaView(None)
    if node.op in ("map", "filter", "partition"):
        item_schema = _ref_schema(node.raw["input"]["$ref"], schemas).items()

    def check_path(base: SchemaView, segs: list[str], where: str, label: str) -> SchemaView | None:
        view = base
        for i, seg in enumerate(segs):
            try:
                view = view.child(seg)
            except KeyError:
                err("E_UNKNOWN_FIELD", node, where, f"{label} has no field {seg!r} at '{'.'.join(segs[:i + 1])}'.",
                    f"Available fields: {', '.join(view.fields()) or '(none)'}.")
                return None
            if not view.known:
                return view
        return view

    for key, value in {**node_value_fields(node), **node_conditions(node)}.items():
        walker = walk_condition(value) if key in ("when", "where") else walk_values(value)
        for kind, v in walker:
            if kind == "$ref":
                segs = split_path(v["$ref"])
                if segs[0] in schemas:
                    check_path(schemas[segs[0]], segs[1:], f".{key}", f"Output of {segs[0]!r}")
            elif kind == "$item":
                check_path(item_schema, split_path(v["$item"]), f".{key}", "Each item")
            elif kind == "$agg":
                spec = v["$agg"]
                segs = split_path(spec["of"])
                if segs[0] not in schemas:
                    continue
                lst = check_path(schemas[segs[0]], segs[1:], f".{key}", f"Output of {segs[0]!r}")
                if lst is None or not lst.known:
                    continue
                if lst.type != "array":
                    err("E_BAD_AGG", node, f".{key}", f"$agg 'of' must point at a list; {spec['of']!r} is {lst.type}.")
                    continue
                if spec.get("field"):
                    fv = check_path(lst.items(), split_path(spec["field"]), f".{key}", f"Items of {spec['of']!r}")
                    if fv and fv.known and spec["fn"] in ("sum", "min", "max", "mean") and fv.type not in ("number", "integer"):
                        err("E_BAD_AGG", node, f".{key}", f"$agg {spec['fn']} needs a numeric field; "
                            f"{spec['field']!r} is {fv.type}.")


def _toposort(nodes: dict[str, Node], deps: dict[str, set[str]]) -> list[str] | None:
    order: list[str] = []
    done: set[str] = set()
    pending = [n for n in sorted(nodes, key=lambda x: nodes[x].index)]
    while pending:
        progressed = False
        for n in list(pending):
            if deps[n] <= done:
                order.append(n)
                done.add(n)
                pending.remove(n)
                progressed = True
        if not progressed:
            return None
    return order


def _waves(order: list[str], deps: dict[str, set[str]]) -> list[list[str]]:
    level: dict[str, int] = {}
    for n in order:
        level[n] = 1 + max((level[d] for d in deps[n]), default=-1)
    waves: list[list[str]] = [[] for _ in range(max(level.values(), default=-1) + 1)]
    for n in order:
        waves[level[n]].append(n)
    return waves


def _descendants(order: list[str], deps: dict[str, set[str]]) -> dict[str, set[str]]:
    children: dict[str, set[str]] = {n: set() for n in order}
    for n in order:
        for d in deps[n]:
            children[d].add(n)
    out: dict[str, set[str]] = {}
    for n in reversed(order):
        acc: set[str] = set()
        for c in children[n]:
            acc |= {c} | out[c]
        out[n] = acc
    return out


def _ancestors_of(n: str, deps: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    stack = list(deps.get(n, ()))
    while stack:
        d = stack.pop()
        if d not in seen:
            seen.add(d)
            stack.extend(deps.get(d, ()))
    return seen


def _worst_case(plan: Plan, info: dict[str, NodeInfo], writes: list[str]) -> dict:
    calls_by_tool: dict[str, int] = {}
    cost_by_tool: dict[str, float | None] = {}
    effects = {"read_only": 0, "idempotent_write": 0, "compensatable": 0, "irreversible": 0}
    upstream = 0
    for nid, ni in info.items():
        if not ni.tool:
            continue
        n = ni.node.raw.get("max_items", 1) if ni.node.op == "map" else 1
        upstream += n
        calls_by_tool[ni.tool] = calls_by_tool.get(ni.tool, 0) + n
        effects[ni.state] += n
        if nid in writes and ni.effects.cost:
            per = ni.effects.limits.get("max_per_call")
            prev = cost_by_tool.get(ni.tool, 0)
            cost_by_tool[ni.tool] = None if per is None or prev is None else prev + n * per
    costs = list(cost_by_tool.values())
    total = None if any(c is None for c in costs) else sum(costs)
    return {
        "upstream_calls": upstream,
        "calls_by_tool": calls_by_tool,
        "effects": {k: v for k, v in effects.items() if v},
        "cost": total if costs else 0,
        "cost_by_tool": cost_by_tool,
    }
