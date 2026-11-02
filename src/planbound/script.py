"""Plan scripts: a compact way to write a plan, about half the tokens of the JSON form.

A script is a small subset of Python syntax. It is only parsed (with `ast`), never executed, and
translates one-to-one into the JSON plan, so every compiler check still applies:

    # goal: refund C-42's overdue orders under 5000 without disputes, email one receipt
    cust = crm.get_customer(customer_id="C-42")
    orders = billing.list_orders(customer_id=cust.customer_id, status="overdue")
    disputes = billing.list_disputes(customer_id=cust.customer_id)
    eligible = filter(orders.orders, item.amount < 5000 and item.order_id not in disputes.order_ids)
    refunds = map(eligible, billing.refund_order(order_id=item.order_id, amount=item.amount), max_items=20)
    notify = email.send_email(to=cust.email, template="refund_processed",
                              vars={"count": count(eligible), "total": sum(eligible, "amount")},
                              when=count(eligible) > 0, after=[refunds])
    return eligible, refunds, notify

Statements: `name = server.tool(arg=value, ..., when=<cond>, after=[names])`, and
`name = filter(x, cond)`, `partition(x, cond)`, `map(x, server.tool(...), max_items=N, concurrency=N)`,
`quarantine_read(x, field="f", max_items=N, schema={...}, fallback={...})`, `display(x)`,
`checkpoint("h://plan/node", max_age_seconds=N)`; `return a, b`; `allow_partial_irreversible = True`.
Values: literals, lists, dicts, `node.path`, `item.field`, `caller.attr`, `param.name` (prepared plans),
`count|distinct_count|sum|min|max|mean(node.path, "field", default=d)`.
Conditions: `< <= > >= == !=`, `in`, `not in`, `and`, `or`, `not`, `exists(x)`.
"""

from __future__ import annotations

import ast
from typing import Any

from .plan import AGG_FNS

COMPARE = {ast.Lt: "lt", ast.LtE: "lte", ast.Gt: "gt", ast.GtE: "gte", ast.Eq: "eq", ast.NotEq: "ne"}
OPS = ("filter", "partition", "map", "quarantine_read", "display", "checkpoint")


class ScriptError(Exception):
    def __init__(self, line: int | None, message: str):
        super().__init__(message)
        self.line = line
        self.message = message

    def to_json(self) -> dict:
        where = f"line {self.line}" if self.line else "script"
        return {"status": "compile_error", "errors": [{
            "code": "E_SCRIPT", "node_id": None, "path": where, "message": self.message,
            "hint": "See the plan guide: statements are `name = server.tool(...)` or one of "
                    + ", ".join(OPS) + "."}]}


def is_script(value: Any) -> bool:
    return isinstance(value, str)


def parse_script(source: str) -> dict:
    """Translate a plan script into the JSON plan it stands for."""
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        raise ScriptError(e.lineno, f"syntax error: {e.msg}") from e
    goal = _goal(source)
    nodes: list[dict] = []
    names: set[str] = set()
    plan: dict[str, Any] = {"planbound": "0.1", "nodes": nodes}
    if goal:
        plan["goal"] = goal
    for stmt in tree.body:
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
            plan.setdefault("goal", stmt.value.value.strip())
            continue
        if isinstance(stmt, ast.Return):
            items = stmt.value.elts if isinstance(stmt.value, ast.Tuple) else [stmt.value]
            plan["return"] = [_name(i, names, "return") for i in items if i is not None]
            continue
        if not isinstance(stmt, ast.Assign) or len(stmt.targets) != 1 or not isinstance(stmt.targets[0], ast.Name):
            raise ScriptError(stmt.lineno, "each statement must be `name = <call>`")
        target = stmt.targets[0].id
        if target == "allow_partial_irreversible":
            plan["allow_partial_irreversible"] = _const(stmt.value, bool, "allow_partial_irreversible")
            continue
        if not isinstance(stmt.value, ast.Call):
            raise ScriptError(stmt.lineno, f"{target!r} must be assigned a tool call or one of {', '.join(OPS)}")
        node = _Builder(names).node(target, stmt.value)
        names.add(target)
        nodes.append(node)
    if not nodes:
        raise ScriptError(None, "the script has no steps")
    return plan


def _goal(source: str) -> str:
    for line in source.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("#"):
            text = s.lstrip("#").strip()
            return text[5:].strip() if text.lower().startswith("goal:") else text
        return ""
    return ""


def _name(expr: ast.expr, names: set[str], what: str) -> str:
    if isinstance(expr, ast.Name) and expr.id in names:
        return expr.id
    raise ScriptError(getattr(expr, "lineno", None), f"{what} must name an earlier step")


def _const(expr: ast.expr, kind: type, what: str) -> Any:
    if isinstance(expr, ast.Constant) and isinstance(expr.value, kind):
        return expr.value
    raise ScriptError(expr.lineno, f"{what} must be a {kind.__name__} literal")


class _Builder:
    def __init__(self, names: set[str]):
        self.names = names

    # ---------------------------------------------------------------- statements

    def node(self, nid: str, call: ast.Call) -> dict:
        func = call.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            node = {"id": nid, "op": "call", "tool": f"{func.value.id}.{func.attr}"}
            node["args"], extra = self._tool_args(call, item_ok=False)
            node.update(extra)
            return node
        if not isinstance(func, ast.Name) or func.id not in OPS:
            raise ScriptError(call.lineno, f"unknown step {ast.unparse(func)!r}; use server.tool(...) or "
                                           + ", ".join(OPS))
        op = func.id
        node: dict[str, Any] = {"id": nid, "op": op}
        kw = {k.arg: k.value for k in call.keywords}
        if op == "checkpoint":
            node["handle"] = _const(self._arg(call, 0, "handle"), str, "the checkpoint handle")
            if "max_age_seconds" in kw:
                node["max_age_seconds"] = self.value(kw.pop("max_age_seconds"))
        else:
            node["input"] = self._ref(self._arg(call, 0, "input"))
        if op in ("filter", "partition"):
            node["where"] = self.cond(self._arg(call, 1, "condition"), item_ok=True)
        elif op == "map":
            inner = self._arg(call, 1, "tool call")
            if not (isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute)
                    and isinstance(inner.func.value, ast.Name)):
                raise ScriptError(call.lineno, "map's second argument must be server.tool(...)")
            args, extra = self._tool_args(inner, item_ok=True)
            if extra:
                raise ScriptError(call.lineno, "put when=/after= on map(...), not on the inner call")
            node["call"] = {"tool": f"{inner.func.value.id}.{inner.func.attr}", "args": args}
        for key in ("max_items", "concurrency", "field", "schema", "fallback"):
            if key in kw:
                node[key] = self.value(kw.pop(key))
        self._when_after(kw, node, call)
        if kw:
            raise ScriptError(call.lineno, f"{op}() does not take {', '.join(sorted(kw))}")
        return node

    def _tool_args(self, call: ast.Call, *, item_ok: bool) -> tuple[dict, dict]:
        if call.args:
            raise ScriptError(call.lineno, "pass tool arguments by name, e.g. tool(customer_id=...)")
        kw = {k.arg: k.value for k in call.keywords}
        if None in kw:
            raise ScriptError(call.lineno, "**kwargs is not supported")
        extra: dict[str, Any] = {}
        self._when_after(kw, extra, call)
        return {k: self.value(v, item_ok=item_ok) for k, v in kw.items()}, extra

    def _when_after(self, kw: dict, node: dict, call: ast.Call) -> None:
        if "when" in kw:
            node["when"] = self.cond(kw.pop("when"))
        if "after" in kw:
            after = kw.pop("after")
            items = after.elts if isinstance(after, (ast.List, ast.Tuple)) else [after]
            node["depends_on"] = [_name(i, self.names, "after=") for i in items]

    @staticmethod
    def _arg(call: ast.Call, i: int, what: str) -> ast.expr:
        if len(call.args) <= i:
            raise ScriptError(call.lineno, f"{ast.unparse(call.func)}() needs its {what}")
        return call.args[i]

    # ---------------------------------------------------------------- values

    def _path(self, expr: ast.expr) -> tuple[str, str] | None:
        parts: list[str] = []
        while isinstance(expr, ast.Attribute):
            parts.append(expr.attr)
            expr = expr.value
        if not isinstance(expr, ast.Name):
            return None
        return expr.id, ".".join(reversed(parts))

    def _ref(self, expr: ast.expr) -> dict:
        p = self._path(expr)
        if p is None or p[0] not in self.names:
            raise ScriptError(expr.lineno, f"{ast.unparse(expr)!r} must refer to an earlier step")
        return {"$ref": f"{p[0]}.{p[1]}" if p[1] else p[0]}

    def value(self, expr: ast.expr, *, item_ok: bool = True) -> Any:
        if isinstance(expr, ast.Constant):
            return expr.value
        if isinstance(expr, ast.UnaryOp) and isinstance(expr.op, ast.USub) and isinstance(expr.operand, ast.Constant):
            return -expr.operand.value
        if isinstance(expr, (ast.List, ast.Tuple)):
            return [self.value(e, item_ok=item_ok) for e in expr.elts]
        if isinstance(expr, ast.Dict):
            if not all(isinstance(k, ast.Constant) and isinstance(k.value, str) for k in expr.keys):
                raise ScriptError(expr.lineno, "dict keys must be string literals")
            return {k.value: self.value(v, item_ok=item_ok) for k, v in zip(expr.keys, expr.values, strict=True)}
        if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Name) and expr.func.id in AGG_FNS:
            return self._agg(expr)
        p = self._path(expr)
        if p is not None:
            root, rest = p
            if root in self.names:
                return {"$ref": f"{root}.{rest}" if rest else root}
            if root == "item" and rest:
                return {"$item": rest}
            if root == "caller" and rest:
                return {"$caller": rest}
            if root == "param" and rest:
                return {"$param": rest}
        raise ScriptError(getattr(expr, "lineno", None), f"cannot use {ast.unparse(expr)!r} as a value here")

    def _agg(self, call: ast.Call) -> dict:
        spec: dict[str, Any] = {"fn": call.func.id, "of": self._ref(self._arg(call, 0, "list"))["$ref"]}
        if len(call.args) > 1:
            spec["field"] = _const(call.args[1], str, "the aggregated field")
        for k in call.keywords:
            if k.arg != "default":
                raise ScriptError(call.lineno, f"{call.func.id}() takes only default=")
            spec["default"] = self.value(k.value)
        return {"$agg": spec}

    # ---------------------------------------------------------------- conditions

    def cond(self, expr: ast.expr, *, item_ok: bool = False) -> dict:
        if isinstance(expr, ast.BoolOp):
            return {"and" if isinstance(expr.op, ast.And) else "or": [self.cond(v) for v in expr.values]}
        if isinstance(expr, ast.UnaryOp) and isinstance(expr.op, ast.Not):
            return {"not": self.cond(expr.operand)}
        if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Name) and expr.func.id == "exists":
            return {"exists": self.value(self._arg(expr, 0, "value"))}
        if isinstance(expr, ast.Compare):
            if len(expr.ops) != 1:
                raise ScriptError(expr.lineno, "write chained comparisons as `a < b and b < c`")
            op, left, right = expr.ops[0], self.value(expr.left), self.value(expr.comparators[0])
            if isinstance(op, ast.In):
                return {"in": [left, right]}
            if isinstance(op, ast.NotIn):
                return {"not": {"in": [left, right]}}
            if type(op) in COMPARE:
                return {COMPARE[type(op)]: [left, right]}
        raise ScriptError(getattr(expr, "lineno", None), f"{ast.unparse(expr)!r} is not a condition")
