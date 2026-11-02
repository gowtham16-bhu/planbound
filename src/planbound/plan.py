"""Plan IR: parsing, value references and conditions.

A plan is a loop-free DAG. Values are JSON literals or one of:
  {"$ref": "node.path"}       a node's output (or a field inside it)
  {"$item": "path"}           the current item inside map / filter / partition
  {"$agg": {...}}             count / distinct_count / sum / min / max / mean over a list
  {"$caller": "attr"}         the authenticated caller, set by the host, never by the model
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

OPS = ("call", "filter", "partition", "map", "quarantine_read", "display", "checkpoint")
AGG_FNS = ("count", "distinct_count", "sum", "min", "max", "mean")
COMPARE = ("eq", "ne", "gt", "gte", "lt", "lte")
ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")


class Pending(Exception):
    """A value depends on a write that has not run yet (preview only)."""


class Skipped(Exception):
    """A value depends on a node whose `when` condition was false."""


class EvalError(Exception):
    pass


PENDING = object()
SKIPPED = object()


@dataclass
class Node:
    id: str
    op: str
    raw: dict
    index: int

    @property
    def path(self) -> str:
        return f"$.nodes[{self.index}]"


@dataclass
class Plan:
    goal: str
    nodes: dict[str, Node]
    returns: list[str]
    allow_partial_irreversible: bool = False
    raw: dict = field(default_factory=dict)


def split_path(path: str) -> list[str]:
    return [p for p in path.split(".") if p != ""]


def get_path(obj: Any, segments: list[str]) -> Any:
    for seg in segments:
        if obj is PENDING:
            raise Pending()
        if obj is SKIPPED:
            raise Skipped()
        if isinstance(obj, dict):
            if seg not in obj:
                raise EvalError(f"no field {seg!r}")
            obj = obj[seg]
        elif isinstance(obj, list):
            if not seg.isdigit() or int(seg) >= len(obj):
                raise EvalError(f"no list index {seg!r}")
            obj = obj[int(seg)]
        else:
            raise EvalError(f"cannot read {seg!r} from a {type(obj).__name__}")
    if obj is PENDING:
        raise Pending()
    if obj is SKIPPED:
        raise Skipped()
    return obj


def is_special(value: Any) -> str | None:
    if isinstance(value, dict) and len(value) == 1:
        key = next(iter(value))
        if key in ("$ref", "$item", "$agg", "$caller"):
            return key
    return None


@dataclass
class Ctx:
    outputs: dict[str, Any]
    caller: dict
    item: Any = None
    has_item: bool = False

    def with_item(self, item: Any) -> Ctx:
        return Ctx(self.outputs, self.caller, item, True)


def resolve(value: Any, ctx: Ctx) -> Any:
    kind = is_special(value)
    if kind == "$ref":
        segs = split_path(value["$ref"])
        if segs[0] not in ctx.outputs:
            raise EvalError(f"unknown node {segs[0]!r}")
        return get_path(ctx.outputs[segs[0]], segs[1:])
    if kind == "$item":
        if not ctx.has_item:
            raise EvalError("$item used outside map/filter/partition")
        return get_path(ctx.item, split_path(value["$item"]))
    if kind == "$caller":
        attr = value["$caller"]
        if attr not in ctx.caller:
            raise EvalError(f"caller has no attribute {attr!r}")
        return ctx.caller[attr]
    if kind == "$agg":
        return _aggregate(value["$agg"], ctx)
    if isinstance(value, dict):
        return {k: resolve(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v, ctx) for v in value]
    return value


def _aggregate(spec: dict, ctx: Ctx) -> Any:
    items = resolve({"$ref": spec["of"]}, ctx)
    if not isinstance(items, list):
        raise EvalError(f"$agg 'of' must be a list, got {type(items).__name__}")
    fn = spec["fn"]
    fld = spec.get("field")
    values = items if fld is None else [get_path(i, split_path(fld)) for i in items]
    if fn == "count":
        return len([v for v in values if v is not None])
    if fn == "distinct_count":
        return len({repr(v) for v in values if v is not None})
    nums = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if fn == "sum":
        return sum(nums)
    if not nums:
        if "default" in spec:
            return spec["default"]
        raise EvalError(f"$agg {fn} over an empty list needs a 'default'")
    if fn == "min":
        return min(nums)
    if fn == "max":
        return max(nums)
    return sum(nums) / len(nums)


def evaluate(cond: Any, ctx: Ctx) -> bool:
    if not isinstance(cond, dict) or len(cond) != 1:
        raise EvalError("a condition is an object with one operator")
    (op, arg), = cond.items()
    if op in COMPARE:
        a, b = resolve(arg[0], ctx), resolve(arg[1], ctx)
        if op == "eq":
            return a == b
        if op == "ne":
            return a != b
        if a is None or b is None:
            return False
        return {"gt": a > b, "gte": a >= b, "lt": a < b, "lte": a <= b}[op]
    if op == "in":
        return resolve(arg[0], ctx) in resolve(arg[1], ctx)
    if op == "and":
        return all(evaluate(c, ctx) for c in arg)
    if op == "or":
        return any(evaluate(c, ctx) for c in arg)
    if op == "not":
        return not evaluate(arg, ctx)
    if op == "exists":
        try:
            return resolve(arg, ctx) is not None
        except EvalError:
            return False
    raise EvalError(f"unknown condition operator {op!r}")


# ---------------------------------------------------------------- static walks


def walk_values(value: Any):
    """Yield every special value ($ref, $item, $agg, $caller) inside a value tree."""
    kind = is_special(value)
    if kind:
        yield kind, value
        return
    if isinstance(value, dict):
        for v in value.values():
            yield from walk_values(v)
    elif isinstance(value, list):
        for v in value:
            yield from walk_values(v)


def walk_condition(cond: Any):
    if not isinstance(cond, dict) or len(cond) != 1:
        return
    (op, arg), = cond.items()
    if op in COMPARE or op == "in":
        for v in arg if isinstance(arg, list) else []:
            yield from walk_values(v)
    elif op in ("and", "or"):
        for c in arg if isinstance(arg, list) else []:
            yield from walk_condition(c)
    elif op == "not":
        yield from walk_condition(arg)
    elif op == "exists":
        yield from walk_values(arg)


def ref_root(kind: str, value: dict) -> str | None:
    if kind == "$ref":
        return split_path(value["$ref"])[0] if value["$ref"] else None
    if kind == "$agg":
        of = value["$agg"].get("of") if isinstance(value["$agg"], dict) else None
        return split_path(of)[0] if isinstance(of, str) and of else None
    return None


def node_value_fields(node: Node) -> dict[str, Any]:
    """The value-bearing parts of a node, keyed by their JSON path suffix."""
    raw = node.raw
    out: dict[str, Any] = {}
    if node.op == "call":
        out["args"] = raw.get("args", {})
    elif node.op == "map":
        out["input"] = raw.get("input")
        out["call.args"] = (raw.get("call") or {}).get("args", {})
    elif node.op in ("filter", "partition", "quarantine_read", "display"):
        out["input"] = raw.get("input")
    return out


def node_conditions(node: Node) -> dict[str, Any]:
    out = {}
    if "when" in node.raw:
        out["when"] = node.raw["when"]
    if node.op in ("filter", "partition") and "where" in node.raw:
        out["where"] = node.raw["where"]
    return out


def dependencies(node: Node) -> set[str]:
    deps: set[str] = set(node.raw.get("depends_on", []) or [])
    for value in node_value_fields(node).values():
        for kind, v in walk_values(value):
            root = ref_root(kind, v)
            if root:
                deps.add(root)
    for cond in node_conditions(node).values():
        for kind, v in walk_condition(cond):
            root = ref_root(kind, v)
            if root:
                deps.add(root)
    return deps


def parse_plan(data: Any) -> Plan:
    """Shape-only parse. Semantic checks live in the compiler."""
    if not isinstance(data, dict):
        raise ValueError("plan must be a JSON object")  # noqa: TRY004 - callers catch ValueError for bad plans
    nodes: dict[str, Node] = {}
    for i, raw in enumerate(data.get("nodes", []) or []):
        if not isinstance(raw, dict):
            raw = {}
        nid = raw.get("id", f"<node {i}>")
        if nid in nodes:
            nid = f"{nid}#dup{i}"
        nodes[nid] = Node(id=nid, op=raw.get("op", ""), raw=raw, index=i)
    returns = data.get("return") or []
    return Plan(
        goal=str(data.get("goal", "")),
        nodes=nodes,
        returns=list(returns),
        allow_partial_irreversible=bool(data.get("allow_partial_irreversible", False)),
        raw=data,
    )
