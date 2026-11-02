"""`planbound verify`: test the guarantees an upstream claims instead of taking the operator's word.

Run it against a sandbox or test tenant. Every probe that takes effect is undone with the tool's
compensator, so the tool must be compensatable (or the operator must pass allow_irreversible).

Checks, for each property the manifest claims:
- idempotency_keys: the same call twice with one key returns the same result (a server that ignores
  the key makes a second effect, or refuses the repeat).
- conditional: the call with a deliberately wrong conditional value is refused with a conflict.
- fencing: after a call with epoch 2, a call with epoch 1 for the same plan is refused.
- outcome_check: after a probe write, the lookup finds it.

The result is an attestation to paste into the manifest as the tool's "verified" entry. It is bound to
the tool's contract hash, so it stops counting when the tool changes. `defaults.require_verified`
makes `strict_writes` demand it.
"""

from __future__ import annotations

import re
import secrets
import time
from typing import Any

from .manifest import (
    DEFAULT_CONFLICT_PATTERN,
    Manifest,
    attestation_problems,
    canonical,
    parse_io_path,
)
from .plan import get_path
from .runtime import FENCE_META, FENCING_REJECTED, IDEM_META, UpstreamError, _io_call

_PROBE_VALUES = {"integer": -987654321, "number": -987654321.5, "string": "__planbound_probe__"}


async def verify_tool(upstreams: Any, manifest: Manifest, tool: str, args: dict, *,
                      allow_irreversible: bool = False) -> dict:
    eff = manifest.tools.get(tool)
    if eff is None or eff.state == "read_only":
        raise ValueError(f"{tool} is not a write in the manifest")
    if eff.compensator is None and not allow_irreversible:
        raise ValueError(f"{tool} has no compensator, so probes could not be undone; run against a sandbox "
                         "with allow_irreversible=True")
    upstream = upstreams.catalog.get(tool)
    if upstream is None:
        raise ValueError(f"no upstream tool {tool}")
    probe = "verify_" + secrets.token_hex(4)
    done: list[Any] = []  # results of probe writes that took effect, undone at the end
    checks: dict[str, bool] = {}
    notes: dict[str, str] = {}

    async def call(call_args: dict, meta: dict) -> tuple[bool, Any]:
        try:
            result = await upstreams.call(tool, call_args, meta)
        except UpstreamError as e:
            return False, str(e)
        done.append((call_args, result))
        return True, result

    try:
        if eff.conditional:
            arg = eff.conditional["arg"]
            wrong = _PROBE_VALUES.get(_arg_type(upstream.input_schema, arg), "__planbound_probe__")
            ok, out = await call({**args, arg: wrong}, {IDEM_META: probe + ":conditional"})
            pattern = re.compile(eff.conditional.get("conflict_pattern", DEFAULT_CONFLICT_PATTERN))
            checks["conditional"] = not ok and bool(pattern.search(str(out)))
            if ok:
                notes["conditional"] = "a write with a wrong conditional value was accepted"
            elif not checks["conditional"]:
                notes["conditional"] = f"refused, but not with a conflict error: {out}"
            await _undo_all(upstreams, eff, done, probe)
        if eff.idempotency_keys or eff.outcome_check:
            key = probe + ":idempotency"
            ok1, first = await call(args, {IDEM_META: key})
            if not ok1:
                raise ValueError(f"the probe write itself failed, fix the sample args: {first}")
            if eff.outcome_check:
                checks["outcome_check"] = await _outcome_found(upstreams, eff.outcome_check, args, first)
            if eff.idempotency_keys:
                ok2, second = await call(args, {IDEM_META: key})
                checks["idempotency_keys"] = ok2 and canonical(second) == canonical(first)
                if checks["idempotency_keys"]:
                    done.pop()  # deduplicated: one effect, undone once
                elif ok2:
                    notes["idempotency_keys"] = "the repeat made a second effect"
                else:
                    notes["idempotency_keys"] = f"the repeat was refused instead of deduplicated: {second}"
            await _undo_all(upstreams, eff, done, probe)
        if eff.fencing:
            fence_plan = probe + ":fence"
            ok, out = await call(args, {IDEM_META: probe + ":epoch2", FENCE_META: {"plan": fence_plan, "epoch": 2}})
            if not ok:
                raise ValueError(f"the probe write itself failed, fix the sample args: {out}")
            await _undo_all(upstreams, eff, done, probe)
            ok, out = await call(args, {IDEM_META: probe + ":epoch1", FENCE_META: {"plan": fence_plan, "epoch": 1}})
            checks["fencing"] = not ok and FENCING_REJECTED in str(out)
            if ok:
                notes["fencing"] = "a write with an older epoch was accepted"
    finally:
        await _undo_all(upstreams, eff, done, probe)
    claimed = [c for c, on in (("idempotency_keys", eff.idempotency_keys), ("conditional", bool(eff.conditional)),
                               ("fencing", eff.fencing), ("outcome_check", bool(eff.outcome_check))) if on]
    return {
        "tool": tool,
        "contract_sha256": upstream.contract_sha256(),
        "checks": checks,
        "passed": all(checks.get(c) for c in claimed),
        "failed": [c for c in claimed if not checks.get(c)],
        "notes": notes,
        "verified_at": int(time.time()),
    }


def _arg_type(schema: dict, arg: str) -> str:
    spec = (schema or {}).get("properties", {}).get(arg) or {}
    for branch in [spec, *spec.get("anyOf", [])]:
        kind = branch.get("type")
        if isinstance(kind, str) and kind != "null":
            return kind
    return "string"


async def _outcome_found(upstreams: Any, check: dict, args: dict, result: Any) -> bool:
    try:
        tool, call_args = _io_call(check, args, result)
        answer = await upstreams.call(tool, call_args)
        _, segs = parse_io_path(check["found"])
        return get_path(answer, segs) not in (None, [], {}, "")
    except Exception:  # noqa: BLE001 - a lookup that fails does not prove the write
        return False


async def _undo_all(upstreams: Any, eff, done: list, probe: str) -> None:
    while done:
        call_args, result = done.pop()
        if eff.compensator is None:
            continue
        tool, undo_args = _io_call(eff.compensator, call_args, result)
        await upstreams.call(tool, undo_args, {IDEM_META: f"{probe}:undo:{len(done)}:{secrets.token_hex(2)}"})


__all__ = ["attestation_problems", "verify_tool"]
