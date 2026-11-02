"""Deterministic summaries: what the model sees instead of raw data.

Computed by code, never by an LLM. Untrusted free text never appears; only numbers,
booleans, short identifier-like strings and shapes do.
"""

from __future__ import annotations

import re
from typing import Any

SAFE_STRING = re.compile(r"^[A-Za-z0-9_\-:.@]{1,32}$")
MAX_HISTOGRAM = 20
MAX_DEPTH = 4
TRUSTED_TEXT_LIMIT = 4000


def _safe(s: str) -> bool:
    # '@' and '.' allow ids like "ORD-1001" or dates, but an address-like value with both is still
    # a free-form identifier for a person, so emails are redacted.
    return bool(SAFE_STRING.match(s)) and not ("@" in s and "." in s)


def summarize(value: Any, *, trusted: bool = False, depth: int = 0) -> Any:
    if trusted:
        return _trusted(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value if _safe(value) else {"type": "string", "len": len(value)}
    if depth >= MAX_DEPTH:
        return {"shape": "list" if isinstance(value, list) else "object"}
    if isinstance(value, dict):
        return {"shape": "object", "fields": {k: summarize(v, depth=depth + 1) for k, v in value.items()}}
    if isinstance(value, list):
        return _list(value, depth)
    return {"type": type(value).__name__}


def _list(items: list, depth: int) -> dict:
    out: dict[str, Any] = {"shape": "list", "count": len(items)}
    if not items:
        return out
    if all(isinstance(i, dict) for i in items):
        keys: list[str] = []
        for i in items:
            keys.extend(k for k in i if k not in keys)
        out["fields"] = {k: _column([i.get(k) for i in items], depth) for k in keys}
    else:
        out["values"] = _column(items, depth)
    return out


def _column(values: list, depth: int) -> dict:
    present = [v for v in values if v is not None]
    if not present:
        return {"type": "null"}
    if all(isinstance(v, bool) for v in present):
        return {"type": "boolean", "true": sum(present), "false": len(present) - sum(present)}
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in present):
        total = sum(present)
        return {
            "type": "number",
            "min": min(present),
            "max": max(present),
            "sum": round(total, 2),
            "mean": round(total / len(present), 2),
        }
    if all(isinstance(v, str) for v in present):
        if all(_safe(v) for v in present):
            distinct = sorted(set(present))
            if len(distinct) == 1:
                return {"type": "string", "value": distinct[0]}
            if len(distinct) == len(present):  # all different (ids): the list is in the handle
                return {"type": "string", "unique": len(distinct)}
            if len(distinct) <= MAX_HISTOGRAM:
                return {"type": "string", "histogram": {d: present.count(d) for d in distinct}}
            return {"type": "string", "distinct": len(distinct)}
        return {"type": "string", "non_null": len(present), "max_len": max(len(v) for v in present)}
    if all(isinstance(v, dict) for v in present):
        return {"type": "object", "count": len(present)}
    if all(isinstance(v, list) for v in present):
        return {"type": "list", "count": len(present)}
    return {"type": "mixed", "non_null": len(present)}


def _trusted(value: Any) -> Any:
    """Operator-trusted sources (for example HR policy docs) may reach the model, size-limited."""
    if isinstance(value, str) and len(value) > TRUSTED_TEXT_LIMIT:
        return value[:TRUSTED_TEXT_LIMIT] + f"… [{len(value) - TRUSTED_TEXT_LIMIT} more chars]"
    if isinstance(value, dict):
        return {k: _trusted(v) for k, v in value.items()}
    if isinstance(value, list):
        if len(value) > 50:
            return [_trusted(v) for v in value[:50]] + [f"… {len(value) - 50} more items"]
        return [_trusted(v) for v in value]
    return value
