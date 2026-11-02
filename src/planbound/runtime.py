"""The runtime: preview, approval, recheck, saga execution and crash recovery."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .approval import verify_approval
from .compiler import READER_LABEL, CompiledPlan, CompileError, compile_plan
from .manifest import (
    DEFAULT_CONFLICT_PATTERN,
    Manifest,
    ToolEffects,
    UpstreamTool,
    canonical,
    parse_io_path,
)
from .plan import (
    ID_RE,
    PENDING,
    SKIPPED,
    Ctx,
    EvalError,
    Pending,
    Skipped,
    evaluate,
    get_path,
    resolve,
    split_path,
)
from .script import ScriptError, is_script, parse_script
from .store import Store
from .summary import summarize

IDEM_META = "io.planbound/idempotency-key"
# Sent with every write and undo call: {"plan": <plan_id>, "epoch": <lease epoch>}. An upstream that
# supports fencing remembers the highest epoch it has seen per plan and refuses lower ones with an
# error containing FENCING_REJECTED.
FENCE_META = "io.planbound/fencing-token"
FENCING_REJECTED = "io.planbound/fencing-token rejected"
SKIP_MARK = {"__planbound_skipped__": True}


class UpstreamError(Exception):
    """An upstream call failed.

    `definite=True`: the server answered with an error, so the effect did not happen.
    `definite=False`: no answer (timeout, dropped connection). The effect may or may not have happened.
    """

    def __init__(self, message: str, *, definite: bool = True):
        super().__init__(message)
        self.definite = definite


class OutcomeUnknown(UpstreamError):
    """A write got no answer and could not safely be re-sent to find out what happened."""

    def __init__(self, message: str):
        super().__init__(message, definite=False)


class WriteConflict(UpstreamError):
    """A conditional write was refused because the data changed after the commit re-read."""


class Upstreams(Protocol):
    catalog: dict[str, UpstreamTool]

    async def call(self, tool: str, args: dict, meta: dict | None = None) -> Any: ...


class QuarantineReader(Protocol):
    async def extract(self, text: str, schema: dict) -> dict: ...


class NodeFailure(Exception):
    def __init__(self, node_id: str, message: str, code: str | None = None):
        super().__init__(f"{node_id}: {message}")
        self.node_id = node_id
        self.message = message
        self.code = code


class SimulatedCrash(Exception):
    """Test hook: stands in for the gateway process dying mid-plan."""


class PlanCancelled(Exception):
    """Someone ran `planbound cancel` before the point of no return."""


class LeaseLost(Exception):
    """Another gateway or `planbound recover` took this plan over: stop at once, touch nothing more."""


@dataclass
class Run:
    plan_id: str
    compiled: CompiledPlan
    caller: dict
    outputs: dict[str, Any] = field(default_factory=dict)
    writes: list[dict] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    limit_errors: list[dict] = field(default_factory=list)
    pivot_done: bool = False


class Runtime:
    def __init__(
        self,
        store: Store,
        upstreams: Upstreams,
        manifest: Manifest,
        caller: dict | None = None,
        *,
        reader: QuarantineReader | None = None,
        display_dir: str | Path | None = None,
        retry_attempts: int = 3,
        retry_delay_s: float = 0.2,
        compensation_attempts: int = 5,
        lease_ttl_s: float = 60.0,
    ):
        self.store = store
        self.upstreams = upstreams
        self.manifest = manifest
        self.caller = caller or {}
        # One reader, or several independent ones (different models or prompts) that must agree.
        self.readers: list[QuarantineReader] = list(reader) if isinstance(reader, (list, tuple)) else (
            [reader] if reader is not None else [])
        self.reader = self.readers[0] if self.readers else None
        self.display_dir = Path(display_dir) if display_dir else Path(".planbound/display")
        self.retry_attempts = retry_attempts
        self.retry_delay_s = retry_delay_s
        self.compensation_attempts = compensation_attempts
        self.lease_ttl_s = lease_ttl_s
        self.instance_id = "rt_" + secrets.token_hex(6)
        self.crash_after_writes: int | None = None
        self._crashed = False
        self._writes_done = 0
        self._reader_cache: dict[str, dict] = {}

    # ================================================================ public API

    def compile(self, raw: Any, caller: dict | None = None, *, check_age: bool = True) -> CompiledPlan:
        return compile_plan(raw, catalog=self.upstreams.catalog, manifest=self.manifest,
                            caller=caller if caller is not None else self.caller,
                            reader_configured=self.reader is not None,
                            handles=self._saved_result, now=time.time() if check_age else None)

    def _saved_result(self, plan_id: str, node_id: str) -> dict | None:
        """A result an earlier plan saved, for `checkpoint` nodes."""
        h = self.store.get_handle(plan_id, node_id)
        rec = self.store.get_plan(plan_id) if h else None
        if h is None or rec is None:
            return None
        return {"data": h["data"], "labels": h["labels"], "trust": h["trust"], "created_at": h["created_at"],
                "caller": rec["caller"], "plan_status": rec["status"], "skipped": h["data"] == SKIP_MARK}

    async def submit(self, raw: Any, mode: str = "execute", wait_seconds: float = 0) -> dict:
        if is_script(raw):
            try:
                raw = parse_script(raw)
            except ScriptError as e:
                return e.to_json()
        if _has_params(raw):
            return {"status": "compile_error", "errors": [{
                "code": "E_PARAM", "node_id": None, "path": "$",
                "message": "This plan has param.* placeholders: prepare it with planbound_prepare, then run "
                           "it with planbound_run."}]}
        try:
            compiled = self.compile(raw)
        except CompileError as e:
            return e.to_json()
        if mode == "compile":
            return compiled.summary()
        plan_id = "p_" + secrets.token_hex(5)
        self.store.create_plan(plan_id, compiled.plan_hash, raw, self.caller)
        run = Run(plan_id, compiled, self.caller)
        preview = await self._preview(run)
        self.store.update_plan(plan_id, status="previewed", preview_hash=preview.get("preview_hash"), preview=preview)
        if preview["status"] != "preview" or mode == "preview":
            if preview["status"] != "preview":
                self.store.update_plan(plan_id, status="rejected")
            return preview
        if preview["approval"] in ("none", "auto"):
            # No approval wait means no time for the data to change: reuse the preview's reads.
            return await self._commit(plan_id, compiled, preview["preview_hash"], fresh_run=run)
        self.store.request_approval(plan_id, compiled.plan_hash, preview["preview_hash"], preview,
                                    self.manifest.approval_ttl_s)
        self.store.update_plan(plan_id, status="awaiting_approval")
        return await self.commit(plan_id, wait_seconds=wait_seconds, _preview=preview)

    # ---------------------------------------------------------------- prepared plans

    async def prepare(self, name: str, raw: Any, example: dict) -> dict:
        """Store a plan with param.* placeholders under a name, after checking it compiles with `example`.

        Later runs send only the name and parameters: a few dozen output tokens instead of a whole plan.
        Every run is still substituted, compiled, previewed and approved like any other plan."""
        if is_script(raw):
            try:
                raw = parse_script(raw)
            except ScriptError as e:
                return e.to_json()
        if not isinstance(name, str) or not ID_RE.match(name):
            return {"status": "error", "message": "name must be lowercase letters, digits and _"}
        names = sorted(_param_names(raw))
        if set(example) != set(names):
            return {"status": "error", "message": f"example must give exactly the params {names}"}
        bad = [k for k, v in example.items() if not isinstance(v, (str, int, float, bool))]
        if bad:
            return {"status": "error", "message": f"params must be strings, numbers or booleans: {bad}"}
        try:
            compiled = self.compile(_bind(raw, example))
        except CompileError as e:
            return e.to_json()
        params = {k: type(v).__name__ for k, v in example.items()}
        self.store.put_prepared(name, self.caller, raw, params)
        return {"status": "prepared", "name": name, "params": params, "worst_case": compiled.worst_case}

    async def run_prepared(self, name: str, params: dict, mode: str = "execute", wait_seconds: float = 0) -> dict:
        prepared = self.store.get_prepared(name, self.caller)
        if prepared is None:
            return {"status": "error", "message": f"no prepared plan {name!r}",
                    "prepared": sorted(self.store.list_prepared(self.caller))}
        want = prepared["params"]
        if set(params) != set(want):
            return {"status": "error", "message": f"{name} takes exactly {sorted(want)}"}
        wrong = [k for k, v in params.items() if type(v).__name__ != want[k]
                 and not (want[k] == "float" and type(v) is int)]
        if wrong:
            return {"status": "error", "message": f"wrong types for {wrong}; expected {want}"}
        return await self.submit(_bind(prepared["raw"], params), mode=mode, wait_seconds=wait_seconds)

    async def commit(self, plan_id: str, wait_seconds: float = 0, _preview: dict | None = None) -> dict:
        rec = self.store.get_plan(plan_id)
        if rec is None:
            return {"status": "error", "message": f"unknown plan {plan_id}"}
        if rec["status"] not in ("awaiting_approval", "previewed"):
            return {"status": rec["status"], "plan_id": plan_id, "result": rec["result"]}
        preview = _preview or rec["preview"]
        try:
            compiled = self.compile(rec["raw"], rec["caller"])
        except CompileError as e:  # e.g. a checkpoint expired, or a tool changed, while waiting for approval
            self.store.update_plan(plan_id, status="rejected", result=e.to_json())
            return {**e.to_json(), "plan_id": plan_id,
                    "message": "The plan no longer compiles, so nothing was written. Fix it and submit again."}
        if preview["approval"] not in ("none", "auto"):
            if self.store.get_approval(plan_id) is None:
                self.store.request_approval(plan_id, compiled.plan_hash, preview["preview_hash"], preview,
                                            self.manifest.approval_ttl_s)
                self.store.update_plan(plan_id, status="awaiting_approval")
            deadline = time.time() + wait_seconds
            while True:
                approval = self.store.get_approval(plan_id)
                if approval and approval["status"] in ("signed", "denied"):
                    break
                if time.time() >= deadline:
                    return {
                        "status": "awaiting_approval",
                        "plan_id": plan_id,
                        "approval": approval["status"] if approval else "missing",
                        "preview": _public_preview(preview),
                        "message": "Waiting for the policy signer"
                        + (" or a human" if approval and approval["status"] == "escalated" else "")
                        + ". Call planbound_commit with this plan_id to continue.",
                    }
                await asyncio.sleep(0.1)
            if approval["status"] == "denied":
                self.store.update_plan(plan_id, status="rejected", result={"status": "rejected"})
                return {"status": "rejected", "plan_id": plan_id, "message": "Approval was denied. Nothing was written."}
            reason = verify_approval(approval, self.manifest.approvers)
            if reason is not None:
                return {"status": "approval_invalid", "plan_id": plan_id, "message": reason}
            if approval["preview_hash"] != preview["preview_hash"]:
                return {"status": "approval_invalid", "plan_id": plan_id, "message": "approval is for another preview"}
        return await self._commit(plan_id, compiled, preview["preview_hash"])

    async def recover(self) -> list[dict]:
        """Reconcile every plan that did not end cleanly.

        `running` (the gateway crashed), `failed_partial` (an undo step failed, or retry-safe steps after
        the point of no return are unfinished) and `needs_attention` (a write's outcome is unknown, until
        a human resolves it). Before the point of no return: compensate. After it: roll forward.
        Safe to run again and again; finished steps are never repeated.
        """
        reports = []
        statuses = ("running", "failed_partial", "needs_attention")
        for plan_id in self.store.plans_with_status(*statuses):
            if not self.store.claim(plan_id, self.instance_id, self.lease_ttl_s, statuses):
                # A live gateway is still driving it (its lease is fresh). Never recover a running plan.
                reports.append({"status": "skipped", "plan_id": plan_id,
                                "message": "Another gateway holds this plan's lease; it is still running. "
                                           "Try again once the lease expires."})
                continue
            try:
                reports.append(await self._recover_one(plan_id))
            except LeaseLost:
                reports.append({"status": "skipped", "plan_id": plan_id,
                                "message": "Another process took this plan over during recovery."})
        return reports

    def abandon(self, plan_id: str, reason: str) -> dict:
        """A human closes a plan that recovery cannot finish. Nothing upstream is touched; the journal stays
        as the record of what happened, and `recover` no longer picks the plan up."""
        rec = self.store.get_plan(plan_id)
        if rec is None:
            return {"status": "error", "message": f"unknown plan {plan_id}"}
        if not self.store.claim(plan_id, self.instance_id, self.lease_ttl_s, ("failed_partial", "needs_attention")):
            return {"status": "error",
                    "message": f"only a failed_partial or needs_attention plan that no gateway is running can be "
                               f"abandoned (this one is {rec['status']})"}
        journal = self.store.journal(plan_id)
        left = {s: [_step(e) for e in journal if e["status"] == s]
                for s in ("COMPLETED", "UNKNOWN", "COMPENSATION_FAILED")}
        result = {"status": "abandoned", "plan_id": plan_id, "reason": reason, "previous": rec["result"],
                  "left_as_is": {k.lower(): v for k, v in left.items() if v}}
        self.store.update_plan(plan_id, status="abandoned", result=result)
        return result

    def resolve(self, plan_id: str, step: str, happened: bool, result: Any = None) -> dict:
        """A human settles a write whose outcome was unknown: `step` is `node[index]`, as reported."""
        m = re.fullmatch(r"([a-z][a-z0-9_]*)\[(\d+)\]", step)
        if not m:
            return {"status": "error", "message": "step must look like node[index], e.g. refunds[3]"}
        entry = next((e for e in self.store.journal(plan_id)
                      if e["node_id"] == m.group(1) and e["item_index"] == int(m.group(2))), None)
        if entry is None or entry["status"] != "UNKNOWN":
            return {"status": "error",
                    "message": f"{step} is not an unknown write ({entry['status'] if entry else 'no such step'})"}
        rec = self.store.get_plan(plan_id)
        if not self.store.claim(plan_id, self.instance_id, self.lease_ttl_s, (rec["status"],)):
            return {"status": "error", "message": "a gateway or recover is working on this plan right now; "
                                                  "try again in a moment"}
        try:
            return self._resolve(plan_id, step, entry, happened, result)
        finally:
            self.store.update_plan(plan_id, lease_owner=None)

    def _resolve(self, plan_id: str, step: str, entry: dict, happened: bool, result: Any) -> dict:
        needs_output = entry["state"] == "compensatable" and "$output" in json.dumps(entry["compensator"] or {})
        if happened and result is None and needs_output:
            return {"status": "error",
                    "message": f"{step} is undone from the tool's result; pass it (e.g. --result "
                               "'{\"refund_id\": \"RF-00042\"}') so the plan can be rolled back"}
        if happened:
            self._jset(entry["seq"], "COMPLETED", result=result)
        else:
            self._jset(entry["seq"], "FAILED", error="a human confirmed it did not happen")
        return {"status": "resolved", "plan_id": plan_id, "step": step, "happened": happened,
                "next": "Run `planbound recover` to finish or roll back the plan."}

    def inspect(self, handle: str, path: str = "", where: dict | None = None, group_by: str | None = None) -> dict:
        if not handle.startswith("h://") or handle.count("/") != 3:
            return {"status": "error", "message": "handle must look like h://<plan_id>/<node_id>"}
        plan_id, node_id = handle[4:].split("/")
        h = self.store.get_handle(plan_id, node_id)
        if h is None:
            return {"status": "error", "message": f"unknown handle {handle}"}
        trusted = h["trust"] == "trusted"
        try:
            data = get_path(h["data"], split_path(path)) if path else h["data"]
            if where is not None:
                if not isinstance(data, list):
                    return {"status": "error", "message": "'where' needs the path to point at a list"}
                ctx = Ctx({}, self.caller)
                data = [i for i in data if evaluate(where, ctx.with_item(i))]
        except (EvalError, Pending, Skipped) as e:
            return {"status": "error", "message": str(e)}
        out: dict[str, Any] = {"handle": handle, "labels": h["labels"]}
        if group_by:
            if not isinstance(data, list):
                return {"status": "error", "message": "'group_by' needs a list"}
            groups: dict[str, list] = {}
            for item in data:
                key = item.get(group_by) if isinstance(item, dict) else None
                if not isinstance(key, (str, int, bool)) or not isinstance(summarize(key), (str, int, bool)):
                    return {"status": "error", "message": f"cannot group by {group_by!r}: values are free text"}
                groups.setdefault(str(key), []).append(item)
            out["groups"] = {k: summarize(v, trusted=trusted) for k, v in groups.items()}
        else:
            out["summary"] = summarize(data, trusted=trusted)
        return out

    # ================================================================ preview

    async def _preview(self, run: Run) -> dict:
        c = run.compiled
        try:
            await self._evaluate_read_side(run)
        except NodeFailure as e:
            return {"status": "read_failed", "plan_id": run.plan_id, "node_id": e.node_id, "message": e.message}
        if run.limit_errors:
            return {"status": "limit_error", "plan_id": run.plan_id, "errors": run.limit_errors}
        would_call: dict[str, int] = {}
        effects: dict[str, int] = {}
        for w in run.writes:
            would_call[w["tool"]] = would_call.get(w["tool"], 0) + 1
            effects[w["state"]] = effects.get(w["state"], 0) + 1
        cost = sum(w["cost"] for w in run.writes)
        for nid in run.pending:
            ni = c.info[nid]
            n = ni.node.raw.get("max_items", 1) if ni.node.op == "map" else 1
            if ni.tool:
                would_call[ni.tool] = would_call.get(ni.tool, 0) + n
                effects[ni.state] = effects.get(ni.state, 0) + n
                per = ni.effects.limits.get("max_per_call")
                if ni.effects.cost and per is not None:
                    cost += n * per
        write_nodes = [n for n in c.order if c.info[n].kind == "write" and n not in run.skipped]
        reader_driven = [n for n in write_nodes if READER_LABEL in c.info[n].labels]
        requires_human = any(c.info[n].effects.requires_approval for n in write_nodes) or (
            bool(reader_driven) and self.manifest.reader_driven_writes == "human")
        if not write_nodes:
            approval = "none"
        elif not requires_human and cost <= self.manifest.auto_approve_max_cost:
            approval = "auto"
        else:
            approval = "policy"
        return {
            "status": "preview",
            "plan_id": run.plan_id,
            "goal": c.plan.goal,
            "preview_hash": self._write_set_hash(run),
            "would_call": would_call,
            "cost": cost,
            "effects": effects,
            "pending": run.pending,
            "skipped": run.skipped,
            "approval": approval,
            "requires_human": requires_human,
            "reader_driven": reader_driven,
            "summaries": {nid: self._summary(run, nid) for nid in c.order if nid in c.read_frontier},
            "warnings": c.warnings,
        }

    async def _evaluate_read_side(self, run: Run) -> None:
        """Run the read frontier for real and simulate every write, collecting exact calls and cost."""
        c = run.compiled
        run.writes, run.pending, run.skipped, run.limit_errors = [], [], [], []
        reads = [n for n in c.order if n in c.read_frontier]
        for wave in _waves_of(reads, c.deps, set()):
            await asyncio.gather(*(self._run_node(run, nid) for nid in wave))
        cost_by_tool: dict[str, float] = {}
        for nid in c.order:
            if nid in c.read_frontier:
                continue
            ni = c.info[nid]
            ctx = Ctx(run.outputs, run.caller)
            try:
                if "when" in ni.node.raw and not evaluate(ni.node.raw["when"], ctx):
                    run.outputs[nid] = SKIPPED
                    if ni.kind == "write":
                        run.skipped.append(nid)
                    continue
                if ni.kind != "write":
                    run.outputs[nid] = PENDING
                    continue
                for idx, args in self._resolve_calls(ni, ctx):
                    self._check_call_limits(run, ni, args)
                    cost = self._cost(ni.effects, args)
                    cost_by_tool[ni.tool] = cost_by_tool.get(ni.tool, 0) + cost
                    run.writes.append({"node": nid, "i": idx, "tool": ni.tool, "state": ni.state, "args": args,
                                       "cost": cost})
                run.outputs[nid] = PENDING
            except Pending:
                run.outputs[nid] = PENDING
                if ni.kind == "write":
                    run.pending.append(nid)
            except Skipped:
                run.outputs[nid] = SKIPPED
                if ni.kind == "write":
                    run.skipped.append(nid)
            except EvalError as e:
                run.limit_errors.append({"code": "E_EVAL", "node_id": nid, "message": str(e)})
        for tool, total in cost_by_tool.items():
            eff = next(c.info[n].effects for n in c.order if c.info[n].tool == tool)
            cap = eff.limits.get("max_per_plan")
            if cap is not None and total > cap:
                run.limit_errors.append({"code": "E_COST_BOUND", "node_id": None,
                                         "message": f"{tool} would cost {total:g}, above its per-plan limit {cap:g}."})
        total = sum(cost_by_tool.values())
        if self.manifest.max_plan_cost is not None and total > self.manifest.max_plan_cost:
            run.limit_errors.append({"code": "E_COST_BOUND", "node_id": None,
                                     "message": f"Plan would cost {total:g}, above the plan limit "
                                                f"{self.manifest.max_plan_cost:g}."})

    def _resolve_calls(self, ni, ctx: Ctx) -> list[tuple[int, dict]]:
        node = ni.node
        if node.op == "call":
            return [(0, resolve(node.raw.get("args", {}), ctx))]
        items = resolve(node.raw["input"], ctx)
        if not isinstance(items, list):
            raise EvalError(f"map input must be a list, got {type(items).__name__}")
        bound = node.raw["max_items"]
        if len(items) > bound:
            raise EvalError(f"map over {len(items)} items exceeds max_items {bound}")
        template = node.raw["call"].get("args", {})
        return [(i, resolve(template, ctx.with_item(item))) for i, item in enumerate(items)]

    def _check_call_limits(self, run: Run, ni, args: dict) -> None:
        eff: ToolEffects = ni.effects
        per = eff.limits.get("max_per_call")
        cost = self._cost(eff, args)
        if per is not None and cost > per:
            run.limit_errors.append({"code": "E_COST_BOUND", "node_id": ni.node.id,
                                     "message": f"A {ni.tool} call would cost {cost:g}, above max_per_call {per:g}."})
        if eff.identity and eff.identity.get("unless_role") not in set(run.caller.get("roles", [])):
            want = run.caller.get(eff.identity.get("caller", eff.identity["arg"]))
            if args.get(eff.identity["arg"]) != want:
                run.limit_errors.append({"code": "E_IDENTITY", "node_id": ni.node.id,
                                         "message": f"{ni.tool} may only act on the caller's own record."})

    @staticmethod
    def _cost(eff: ToolEffects, args: dict) -> float:
        if not eff.cost:
            return 0.0
        _, segs = parse_io_path(eff.cost)
        value = get_path(args, segs)
        return float(value) if isinstance(value, (int, float)) else 0.0

    def _write_set_hash(self, run: Run) -> str:
        body = {
            "writes": [{k: w[k] for k in ("node", "i", "tool", "args")} for w in run.writes],
            "pending": sorted(run.pending),
            "skipped": sorted(run.skipped),
        }
        return "sha256:" + hashlib.sha256(canonical(body).encode()).hexdigest()

    # ================================================================ commit

    async def _commit(self, plan_id: str, compiled: CompiledPlan, approved_hash: str,
                      fresh_run: Run | None = None) -> dict:
        run = fresh_run or Run(plan_id, compiled, self.store.get_plan(plan_id)["caller"])
        if fresh_run is None:
            try:
                await self._evaluate_read_side(run)
            except NodeFailure as e:
                self.store.update_plan(plan_id, status="rejected")
                return {"status": "read_failed", "plan_id": plan_id, "node_id": e.node_id, "message": e.message}
        if run.limit_errors:
            self.store.update_plan(plan_id, status="rejected")
            return {"status": "limit_error", "plan_id": plan_id, "errors": run.limit_errors}
        if self._write_set_hash(run) != approved_hash:
            self.store.update_plan(plan_id, status="stale", result={"status": "stale"})
            return {"status": "stale", "plan_id": plan_id, "code": "E_PREVIEW_STALE",
                    "message": "The data changed since the preview, so the approved writes no longer match. "
                               "Nothing was written. Submit the plan again for a fresh preview."}
        unavailable = await self._preflight(compiled)
        if unavailable:
            # Nothing written and nothing claimed: the plan can simply be committed again later.
            return {"status": "deferred", "plan_id": plan_id, "unavailable": unavailable,
                    "message": "A service this plan would need to undo or confirm its steps is unavailable, "
                               "so it did not start. Nothing was written. Commit it again later."}
        if not self.store.claim(plan_id, self.instance_id, self.lease_ttl_s, ("awaiting_approval", "previewed"),
                                new_status="running"):
            rec = self.store.get_plan(plan_id)
            return {"status": rec["status"], "plan_id": plan_id,
                    "message": "Another commit of this plan got there first; it was not run twice."}
        remaining = [n for n in compiled.order if n not in compiled.read_frontier]
        for nid in remaining:
            run.outputs.pop(nid, None)
        try:
            return await self._execute(run, remaining)
        except LeaseLost:
            rec = self.store.get_plan(plan_id)
            return {"status": rec["status"], "plan_id": plan_id, "lease_lost": True,
                    "message": "This gateway stalled long enough for its lease to expire, and recovery took "
                               "the plan over. It stopped without touching anything more."}

    async def _execute(self, run: Run, nodes: list[str]) -> dict:
        c = run.compiled
        done = set(run.outputs)
        failure: NodeFailure | None = None
        for wave in _waves_of(nodes, c.sched_deps, done):
            results = await asyncio.gather(*(self._run_node(run, nid, real=True) for nid in wave),
                                           return_exceptions=True)
            crash = next((r for r in results if isinstance(r, (SimulatedCrash, LeaseLost))), None)
            if crash:
                raise crash
            errors = [r for r in results if isinstance(r, BaseException)]
            if errors:
                first = errors[0]
                failure = first if isinstance(first, NodeFailure) else NodeFailure("?", repr(first))
                break
        if failure is None:
            # Every step ran: the plan is past its point of no return. Confirm the reservations it holds.
            self.store.update_plan(run.plan_id, phase="confirm")
            conf = await self._confirm(run.plan_id, c)
            result = self._result(run, "completed") if conf["ok"] else self._result(
                run, "failed_partial", note="Every step ran, but some reservations are not confirmed yet. "
                                            "`planbound recover` keeps confirming them.")
            if conf["confirmed"] or conf["failed"]:
                result["confirmation"] = conf
        else:
            result = await self._settle(run.plan_id, run=run, failure=failure)
        self.store.update_plan(run.plan_id, status=result["status"], result=result)
        return result

    async def _confirm(self, plan_id: str, compiled: CompiledPlan) -> dict:
        """Turn held reservations into real effects. Retried; only ever moves forward."""
        done, failed = 0, []
        for e in self.store.journal(plan_id):
            eff = compiled.info[e["node_id"]].effects if e["node_id"] in compiled.info else None
            if e["status"] != "COMPLETED" or eff is None or not eff.confirm:
                continue
            fence = self._fence(plan_id)
            error: str | None = None
            for attempt in range(self.compensation_attempts):
                try:
                    tool, args = _io_call(eff.confirm, e["args"], e["result"])
                    await self.upstreams.call(tool, args, {**fence, IDEM_META: e["idem_key"] + ":confirm"})
                    error = None
                    break
                except (SimulatedCrash, LeaseLost):
                    raise
                except UpstreamError as ex:
                    _raise_if_fenced(ex, plan_id)
                    error = str(ex)
                except Exception as ex:  # noqa: BLE001 - e.g. no result to confirm from; recorded below
                    error = str(ex)
                    break
                if attempt + 1 < self.compensation_attempts:
                    await asyncio.sleep(self.retry_delay_s * 2 ** attempt)
            if error is None:
                self._jset(e["seq"], "CONFIRMED")
                done += 1
            else:
                failed.append({"step": _step(e), "error": error})
        return {"ok": not failed, "confirmed": done, "failed": failed}

    async def _settle(self, plan_id: str, *, run: Run | None = None, failure: NodeFailure | None = None) -> dict:
        """Decide what a plan that stopped early needs, from the journal alone, and do what is safe."""
        journal = self.store.journal(plan_id)
        unknown = [_step(e) for e in journal if e["status"] == "UNKNOWN"]
        # Past the point of no return once something that cannot be undone has happened: the pivot, or a
        # retry-safe step (those are not undone, so the plan can only go forward).
        rec = self.store.get_plan(plan_id)
        passed = rec["phase"] == "confirm" or any(
            (e["state"] in ("irreversible", "idempotent_write") and e["status"] == "COMPLETED")
            or e["status"] == "CONFIRMED" for e in journal)
        pivot_unknown = any(e["state"] == "irreversible" and e["status"] == "UNKNOWN" for e in journal)

        def out(status: str, **kw) -> dict:
            if run is not None:
                return self._result(run, status, failure=failure, **kw)
            return {"status": status, "plan_id": plan_id, **{k: v for k, v in kw.items() if v is not None}}

        if pivot_unknown:
            return out("needs_attention", unknown=unknown,
                       note="The point of no return may or may not have happened, so nothing was compensated. "
                            "Check each unknown step upstream, record it with `planbound resolve`, then run "
                            "`planbound recover`.")
        if not passed:
            comp = await self._compensate(plan_id)
            if unknown:
                return out("needs_attention", compensation=comp, unknown=unknown,
                           note="Undoable steps that are known to have happened were compensated. The unknown "
                                "steps may still have happened: check each upstream, record it with "
                                "`planbound resolve`, then run `planbound recover`.")
            if failure is not None and failure.code == "E_CANCELLED" and comp["ok"]:
                return out("cancelled", compensation=comp, note="Cancelled before the point of no return; "
                                                               "every step that ran was undone.")
            if run is None:
                return out("recovered_compensated" if comp["ok"] else "failed_partial", compensation=comp)
            return out("failed_compensated" if comp["ok"] else "failed_partial", compensation=comp,
                       note=None if comp["ok"] else "Some undo steps failed even after retries. "
                                                    "`planbound recover` retries them.")
        if unknown:
            return out("needs_attention", unknown=unknown,
                       note="Past the point of no return, with steps whose outcome is unknown. Check each "
                            "upstream, record it with `planbound resolve`, then run `planbound recover`.")
        return out("failed_partial",
                   note="The point of no return had passed. Remaining retry-safe steps can be finished "
                        "with `planbound recover`.")

    async def _run_node(self, run: Run, nid: str, real: bool = True) -> None:
        c = run.compiled
        ni = c.info[nid]
        node = ni.node
        ctx = Ctx(run.outputs, run.caller)
        try:
            if "when" in node.raw and not evaluate(node.raw["when"], ctx):
                raise Skipped()
            if node.op == "call":
                value = await self._call(run, ni, 0, resolve(node.raw.get("args", {}), ctx))
            elif node.op == "map":
                calls = self._resolve_calls(ni, ctx)
                sem = asyncio.Semaphore(int(node.raw.get("concurrency", 8)))

                async def one(i: int, a: dict):
                    async with sem:
                        return await self._call(run, ni, i, a)

                results = await asyncio.gather(*(one(i, a) for i, a in calls), return_exceptions=True)
                bad = [r for r in results if isinstance(r, BaseException)]
                if bad:
                    raise next((r for r in bad if isinstance(r, (SimulatedCrash, LeaseLost))), bad[0])
                value = results
            elif node.op in ("filter", "partition"):
                items = resolve(node.raw["input"], ctx)
                if not isinstance(items, list):
                    raise EvalError(f"'{node.op}' input must be a list")
                match = [i for i in items if evaluate(node.raw["where"], ctx.with_item(i))]
                if node.op == "filter":
                    value = match
                else:
                    ids = {id(i) for i in match}
                    value = {"match": match, "rest": [i for i in items if id(i) not in ids]}
            elif node.op == "quarantine_read":
                value = await self._quarantine(ni, resolve(node.raw["input"], ctx))
            elif node.op == "display":
                value = self._display(run, nid, resolve(node.raw["input"], ctx))
            elif node.op == "checkpoint":
                value = ni.checkpoint["data"]
            else:
                raise EvalError(f"op {node.op!r} cannot run")
        except Skipped:
            run.outputs[nid] = SKIPPED
            self.store.put_handle(run.plan_id, nid, SKIP_MARK, tool=ni.tool, labels=ni.labels, trust=ni.trust)
            return
        except Pending:
            run.outputs[nid] = PENDING
            return
        except PlanCancelled as e:
            raise NodeFailure(nid, str(e), code="E_CANCELLED") from e
        except WriteConflict as e:
            raise NodeFailure(nid, str(e), code="E_WRITE_CONFLICT") from e
        except OutcomeUnknown as e:
            raise NodeFailure(nid, str(e), code="E_OUTCOME_UNKNOWN") from e
        except (EvalError, UpstreamError) as e:
            raise NodeFailure(nid, str(e)) from e
        run.outputs[nid] = value
        self.store.put_handle(run.plan_id, nid, value, tool=ni.tool, labels=ni.labels, trust=ni.trust)
        if nid == c.pivot:
            run.pivot_done = True

    async def _call(self, run: Run, ni, idx: int, args: dict) -> Any:
        eff: ToolEffects = ni.effects
        self.store.record_usage(ni.tool)
        if self._crashed:
            raise SimulatedCrash("gateway already crashed")
        if ni.kind != "write":
            return await self.upstreams.call(ni.tool, args)
        if eff.state == "irreversible" and eff.hold_seconds:
            await self._hold(run.plan_id, eff.hold_seconds)
        if not run.pivot_done and self.store.cancel_requested(run.plan_id):
            raise PlanCancelled(f"{run.plan_id} was cancelled")
        fence = self._fence(run.plan_id)
        key = f"{run.plan_id}:{ni.node.id}:{idx}"
        prior = self.store.journal_find(key)
        if prior is not None:
            if prior["status"] == "COMPLETED":
                return prior["result"]  # rolling forward: this write already happened, never repeat it
            if prior["status"] != "FAILED":
                raise UpstreamError(f"{ni.tool} is {prior['status']} in the journal; refusing to repeat it")
            seq = prior["seq"]  # it definitely did not happen: try again on the same journal entry
            self._jset(seq, "IN_FLIGHT")
        else:
            seq = self.store.journal_begin(run.plan_id, ni.node.id, idx, ni.tool, eff.state, args, key,
                                           eff.compensator, owner=self.instance_id)
            if seq is None:
                raise LeaseLost(f"{run.plan_id} was taken over by another process")
        meta = {**fence, IDEM_META: key} if eff.idempotency_keys else fence
        # Re-sending is safe when the write is idempotent by nature, or the server deduplicates on the key.
        resend_safe = eff.state == "idempotent_write" or eff.idempotency_keys
        conflict = re.compile(eff.conditional.get("conflict_pattern", DEFAULT_CONFLICT_PATTERN)) \
            if eff.conditional else None
        last: UpstreamError | None = None
        result: Any = None
        ok = False
        for attempt in range(self.retry_attempts):
            try:
                result = await self.upstreams.call(ni.tool, args, meta)
                ok = True
                break
            except UpstreamError as e:
                _raise_if_fenced(e, run.plan_id)
                last = e
                if e.definite and conflict and conflict.search(str(e)):
                    break  # the data changed: retrying cannot help
                # A definite error is retried only for idempotent writes; a lost answer whenever re-sending
                # cannot repeat the effect.
                if not (eff.state == "idempotent_write" or (not e.definite and resend_safe)):
                    break
                if attempt + 1 < self.retry_attempts:
                    await asyncio.sleep(self.retry_delay_s * (attempt + 1))
        if not ok:
            assert last is not None
            if not last.definite:
                verdict, found = await self._check_outcome(eff, args)
                if verdict == "happened":
                    result, ok = found, True  # the upstream says it went through: carry on
                else:
                    self._jset(seq, "UNKNOWN", error=str(last))
                    raise OutcomeUnknown(f"{ni.tool} got no answer, so it may or may not have happened: {last}")
        if not ok:
            assert last is not None
            self._jset(seq, "FAILED", error=str(last))
            if conflict and conflict.search(str(last)):
                raise WriteConflict(f"{ni.tool} refused: the data changed after the commit re-read ({last})")
            raise UpstreamError(f"{ni.tool} failed: {last}")
        self._writes_done += 1
        if self._crashed or (self.crash_after_writes is not None and self._writes_done >= self.crash_after_writes):
            self._crashed = True  # the effect happened upstream, but the journal never hears about it
            raise SimulatedCrash(f"crash after {self._writes_done} writes")
        self._jset(seq, "COMPLETED", result=result)
        return result

    async def _preflight(self, compiled: CompiledPlan) -> list[dict]:
        """Before any write: are the services the undo and confirm steps need reachable right now?"""
        probes = {}
        for nid in compiled.order:
            eff = compiled.info[nid].effects
            if compiled.info[nid].kind == "write" and eff.undo_probe:
                probes[canonical(eff.undo_probe)] = eff.undo_probe
        failed = []
        for probe in probes.values():
            try:
                await self.upstreams.call(probe["tool"], probe.get("args", {}))
            except Exception as e:  # noqa: BLE001 - any failure means: do not start
                failed.append({"probe": probe["tool"], "error": str(e)})
        return failed

    async def _hold(self, plan_id: str, seconds: float) -> None:
        """The cancel window before a point of no return: wait, keep the lease, stop if cancelled."""
        deadline = time.time() + seconds
        while time.time() < deadline:
            if self.store.cancel_requested(plan_id):
                raise PlanCancelled(f"{plan_id} was cancelled during its hold")
            self._fence(plan_id)  # renew the lease while waiting
            await asyncio.sleep(min(0.05, max(0.0, deadline - time.time())))

    def cancel(self, plan_id: str) -> dict:
        """Stop a plan before its point of no return. Waiting plans are dropped; running ones roll back
        at their next step (and at once if they are in a hold window)."""
        rec = self.store.get_plan(plan_id)
        if rec is None:
            return {"status": "error", "message": f"unknown plan {plan_id}"}
        if rec["status"] in ("previewed", "awaiting_approval", "compiled"):
            self.store.update_plan(plan_id, status="cancelled", result={"status": "cancelled"})
            return {"status": "cancelled", "plan_id": plan_id, "message": "Nothing was written."}
        if rec["status"] != "running":
            return {"status": "error", "message": f"plan {plan_id} is {rec['status']}; nothing to cancel"}
        journal = self.store.journal(plan_id)
        if rec["phase"] == "confirm" or any(e["state"] == "irreversible" and e["status"] in ("COMPLETED", "IN_FLIGHT")
                                            for e in journal):
            return {"status": "error", "message": "too late: the point of no return has passed"}
        self.store.request_cancel(plan_id)
        return {"status": "cancel_requested", "plan_id": plan_id,
                "message": "The plan rolls back at its next step."}

    def _fence(self, plan_id: str) -> dict:
        """Renew our lease and return the fencing token to send with a write."""
        epoch = self.store.renew(plan_id, self.instance_id, self.lease_ttl_s)
        if epoch is None:
            raise LeaseLost(f"{plan_id} was taken over by another process")
        return {FENCE_META: {"plan": plan_id, "epoch": epoch}}

    def _jset(self, seq: int, status: str, **kw: Any) -> None:
        if not self.store.journal_set(seq, status, owner=self.instance_id, **kw):
            raise LeaseLost("lost the plan's lease; the journal belongs to its new holder")

    async def _check_outcome(self, eff: ToolEffects, args: dict, result: Any = None) -> tuple[str, Any]:
        """Ask the upstream whether a write happened: ("happened", result), ("absent", None) or ("unknown", None)."""
        check = eff.outcome_check
        if not check:
            return "unknown", None
        try:
            tool, call_args = _io_call(check, args, result)
            answer = await self.upstreams.call(tool, call_args)
            _, segs = parse_io_path(check["found"])
            found = get_path(answer, segs)
        except Exception:  # noqa: BLE001 - a failed lookup proves nothing either way
            return "unknown", None
        if found in (None, [], {}, ""):
            return "absent", None
        return "happened", found[0] if isinstance(found, list) else found

    async def _settle_unknown(self, plan_id: str, compiled: CompiledPlan) -> None:
        """Turn UNKNOWN writes into known ones where the upstream can say what happened."""
        for e in self.store.journal(plan_id):
            if e["status"] != "UNKNOWN":
                continue
            eff = compiled.info[e["node_id"]].effects
            verdict, found = await self._check_outcome(eff, e["args"])
            if verdict == "happened":
                self._jset(e["seq"], "COMPLETED", result=found, error=None)
            elif verdict == "absent" and time.time() - e["updated_at"] >= eff.outcome_check.get("settle_seconds", 30):
                # Absent for longer than a late request could still be in flight: it never happened.
                self._jset(e["seq"], "FAILED", error="outcome check: the upstream has no record of it")

    async def _compensate(self, plan_id: str) -> dict:
        """Undo completed compensatable writes, newest first. Each undo is retried; failures are recorded
        (COMPENSATION_FAILED) and retried again by the next `recover`."""
        done, failed = [], []
        entries = [e for e in self.store.journal(plan_id)
                   if e["state"] == "compensatable" and e["status"] in ("COMPLETED", "COMPENSATION_FAILED")]
        for e in reversed(entries):
            fence = self._fence(plan_id)
            error: str | None = None
            for attempt in range(self.compensation_attempts):
                try:
                    tool, args = _io_call(e["compensator"], e["args"], e["result"])
                except Exception as ex:  # noqa: BLE001 - e.g. no recorded result to undo from: not retryable
                    error = f"cannot build the undo call: {ex}"
                    break
                try:
                    await self.upstreams.call(tool, args, {**fence, IDEM_META: e["idem_key"] + ":compensate"})
                    error = None
                    break
                except (SimulatedCrash, LeaseLost):
                    raise
                except UpstreamError as ex:
                    _raise_if_fenced(ex, plan_id)
                    error = str(ex)
                    if attempt + 1 < self.compensation_attempts:
                        await asyncio.sleep(self.retry_delay_s * 2 ** attempt)
                except Exception as ex:  # noqa: BLE001 - every failure must be recorded, never raised mid-rollback
                    error = str(ex)
                    if attempt + 1 < self.compensation_attempts:
                        await asyncio.sleep(self.retry_delay_s * 2 ** attempt)
            if error is None:
                self._jset(e["seq"], "COMPENSATED")
                done.append(_step(e))
            else:
                self._jset(e["seq"], "COMPENSATION_FAILED", error=error)
                failed.append({"step": _step(e), "error": error})
        return {"ok": not failed, "compensated": len(done), "failed": failed}

    # ================================================================ recovery

    async def _recover_one(self, plan_id: str) -> dict:
        rec = self.store.get_plan(plan_id)
        compiled = self.compile(rec["raw"], rec["caller"], check_age=False)  # it already started: finish it
        for e in self.store.journal(plan_id):
            if e["status"] != "IN_FLIGHT":
                continue
            # A crash during the call: re-send to learn the outcome, but only where that cannot repeat it.
            eff = compiled.info[e["node_id"]].effects
            if e["state"] == "idempotent_write" or eff.idempotency_keys:
                fence = self._fence(plan_id)
                try:
                    result = await self.upstreams.call(e["tool"], e["args"],
                                                       {**fence, IDEM_META: e["idem_key"]} if eff.idempotency_keys
                                                       else fence)
                    self._jset(e["seq"], "COMPLETED", result=result)
                except UpstreamError as ex:
                    _raise_if_fenced(ex, plan_id)
                    self._jset(e["seq"], "FAILED" if ex.definite else "UNKNOWN", error=str(ex))
            else:
                self._jset(e["seq"], "UNKNOWN", error="crashed during the call; outcome unknown")
        await self._settle_unknown(plan_id, compiled)
        result = await self._settle(plan_id)
        if result["status"] == "failed_partial" and "compensation" not in result:
            # Past the point of no return with nothing unknown: roll forward.
            run = Run(plan_id, compiled, rec["caller"], pivot_done=True)
            for nid, data in self.store.handles_for(plan_id).items():
                run.outputs[nid] = SKIPPED if data == SKIP_MARK else data
            remaining = [n for n in compiled.order if n not in run.outputs]
            done = {e["node_id"] for e in self.store.journal(plan_id) if e["status"] == "COMPLETED"}
            blocked = [n for n in remaining if compiled.info[n].state == "irreversible" and n not in done]
            if blocked:
                result = {"status": "needs_attention", "plan_id": plan_id,
                          "note": f"Rolling forward would run irreversible step(s) {', '.join(blocked)} long after "
                                  "approval, so recover will not. Submit them as a new plan if still wanted, then "
                                  "close this one with `planbound abandon`."}
            else:
                result = await self._execute(run, remaining)
        result["recovered"] = True
        self.store.update_plan(plan_id, status=result["status"], result=result)
        return result

    async def _quarantine(self, ni, items: Any) -> list:
        node = ni.node
        if self.reader is None:
            raise EvalError("no quarantined reader configured")
        single = not isinstance(items, list)
        items = [items] if single else items
        if len(items) > node.raw["max_items"]:
            raise EvalError(f"quarantine_read over {len(items)} items exceeds max_items {node.raw['max_items']}")
        schema, fld = node.raw["schema"], node.raw["field"]
        out = []
        for item in items:
            text = get_path(item, split_path(fld))
            text = text if isinstance(text, str) else json.dumps(text)
            key = hashlib.sha256((canonical(schema) + text).encode()).hexdigest()
            if key not in self._reader_cache:
                self._reader_cache[key] = await self._agreed_reading(node, text, schema)
            out.append({**item, **self._reader_cache[key]} if isinstance(item, dict) else self._reader_cache[key])
        return out[0:1] if single else out

    async def _agreed_reading(self, node, text: str, schema: dict) -> dict:
        """Every reader classifies independently. A field they disagree on takes the node's `fallback`
        value, or fails the plan: one steered reader cannot decide alone."""
        answers = await asyncio.gather(*(r.extract(text, schema) for r in self.readers))
        valid = [_validate_extraction(a, schema) for a in answers]
        out = {}
        fallback = node.raw.get("fallback") or {}
        for name in schema:
            values = {json.dumps(v[name]) for v in valid}
            if len(values) == 1:
                out[name] = valid[0][name]
            elif name in fallback:
                out[name] = fallback[name]
            else:
                raise EvalError(f"quarantined readers disagree on {name!r}; add a 'fallback' value for it")
        return out

    def _display(self, run: Run, nid: str, data: Any) -> dict:
        self.display_dir.mkdir(parents=True, exist_ok=True)
        path = self.display_dir / f"{run.plan_id}_{nid}.json"
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
        return {"displayed_to": str(path), "items": len(data) if isinstance(data, list) else 1}

    def _summary(self, run: Run, nid: str) -> dict:
        ni = run.compiled.info[nid]
        value = run.outputs.get(nid)
        if value is SKIPPED:
            return {"handle": f"h://{run.plan_id}/{nid}", "skipped": True}
        if value is PENDING:
            return {"handle": f"h://{run.plan_id}/{nid}", "pending": True}
        out = {"handle": f"h://{run.plan_id}/{nid}", "summary": summarize(value, trusted=ni.trust == "trusted")}
        if ni.labels:
            out["labels"] = sorted(ni.labels)
        return out

    def _result(self, run: Run, status: str, *, failure: NodeFailure | None = None,
                compensation: dict | None = None, note: str | None = None,
                unknown: list[str] | None = None) -> dict:
        c = run.compiled
        wanted = c.plan.returns or [n for n in c.order if not any(n in c.deps[m] for m in c.order)]
        journal = self.store.journal(run.plan_id)
        writes: dict[str, dict[str, int]] = {}
        for e in journal:
            writes.setdefault(e["tool"], {}).setdefault(e["status"].lower(), 0)
            writes[e["tool"]][e["status"].lower()] += 1
        out: dict[str, Any] = {
            "status": status,
            "plan_id": run.plan_id,
            "writes": writes,
            "summaries": {n: self._summary(run, n) for n in wanted if n in run.outputs},
        }
        if failure:
            out["failure"] = {"node_id": failure.node_id, "message": failure.message}
            if failure.code:
                out["failure"]["code"] = failure.code
        if unknown:
            out["unknown"] = unknown
        if compensation:
            out["compensation"] = compensation
        if note:
            out["note"] = note
        if c.warnings:
            out["warnings"] = c.warnings
        return out


def _waves_of(nodes: list[str], deps: dict[str, set[str]], done: set[str]) -> list[list[str]]:
    """Group `nodes` into waves that only depend on `done` or earlier waves."""
    remaining = [n for n in nodes if n not in done]
    allowed = set(nodes) | done
    finished = set(done)
    waves = []
    while remaining:
        wave = [n for n in remaining if (deps[n] & allowed) <= finished]
        if not wave:
            raise RuntimeError(f"cannot schedule {remaining}")
        waves.append(wave)
        finished |= set(wave)
        remaining = [n for n in remaining if n not in finished]
    return waves


def _param_names(value: Any) -> set[str]:
    if isinstance(value, dict):
        if set(value) == {"$param"}:
            return {value["$param"]}
        return set().union(*(_param_names(v) for v in value.values())) if value else set()
    if isinstance(value, list):
        return set().union(*(_param_names(v) for v in value)) if value else set()
    return set()


def _has_params(value: Any) -> bool:
    return bool(_param_names(value))


def _bind(value: Any, params: dict) -> Any:
    """Replace every {"$param": name} with its value: the result is an ordinary plan."""
    if isinstance(value, dict):
        if set(value) == {"$param"}:
            return params[value["$param"]]
        return {k: _bind(v, params) for k, v in value.items()}
    if isinstance(value, list):
        return [_bind(v, params) for v in value]
    return value


def _raise_if_fenced(error: Exception, plan_id: str) -> None:
    if FENCING_REJECTED in str(error):
        raise LeaseLost(f"an upstream refused our fencing token: {plan_id} has a newer holder") from error


def _step(entry: dict) -> str:
    return f"{entry['node_id']}[{entry['item_index']}]"


def _io_call(spec: dict, inputs: dict, output: Any) -> tuple[str, dict]:
    """Build a follow-up call (undo, confirm, outcome check) from a write's input and output."""
    args = {}
    for name, value in spec.get("args", {}).items():
        if isinstance(value, str) and value.startswith("$"):
            source, segs = parse_io_path(value)
            args[name] = get_path(inputs if source == "input" else output, segs)
        else:
            args[name] = value
    return spec["tool"], args


def _validate_extraction(data: dict, schema: dict) -> dict:
    out = {}
    for name, spec in schema.items():
        if name not in data:
            raise EvalError(f"quarantined reader did not return {name!r}")
        v = data[name]
        if "enum" in spec:
            ok = v in spec["enum"]
        elif spec["type"] == "boolean":
            ok = isinstance(v, bool)
        else:
            ok = isinstance(v, (int, float)) and not isinstance(v, bool) and (
                spec["type"] != "integer" or float(v).is_integer())
            if ok and "minimum" in spec:
                ok = v >= spec["minimum"]
            if ok and "maximum" in spec:
                ok = v <= spec["maximum"]
        if not ok:
            raise EvalError(f"quarantined reader returned an invalid {name!r}: rejected")
        out[name] = v
    return out


def _public_preview(preview: dict) -> dict:
    return {k: preview[k] for k in ("would_call", "cost", "effects", "pending", "approval") if k in preview}
