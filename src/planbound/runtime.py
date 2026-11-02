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
from .compiler import CompiledPlan, CompileError, compile_plan
from .manifest import DEFAULT_CONFLICT_PATTERN, Manifest, ToolEffects, UpstreamTool, canonical, parse_io_path
from .plan import PENDING, SKIPPED, Ctx, EvalError, Pending, Skipped, evaluate, get_path, resolve, split_path
from .store import Store
from .summary import summarize

IDEM_META = "io.planbound/idempotency-key"
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
    ):
        self.store = store
        self.upstreams = upstreams
        self.manifest = manifest
        self.caller = caller or {}
        self.reader = reader
        self.display_dir = Path(display_dir) if display_dir else Path(".planbound/display")
        self.retry_attempts = retry_attempts
        self.retry_delay_s = retry_delay_s
        self.compensation_attempts = compensation_attempts
        self.crash_after_writes: int | None = None
        self._crashed = False
        self._writes_done = 0
        self._reader_cache: dict[str, dict] = {}

    # ================================================================ public API

    def compile(self, raw: Any, caller: dict | None = None) -> CompiledPlan:
        return compile_plan(raw, catalog=self.upstreams.catalog, manifest=self.manifest,
                            caller=caller if caller is not None else self.caller,
                            reader_configured=self.reader is not None)

    async def submit(self, raw: Any, mode: str = "execute", wait_seconds: float = 0) -> dict:
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

    async def commit(self, plan_id: str, wait_seconds: float = 0, _preview: dict | None = None) -> dict:
        rec = self.store.get_plan(plan_id)
        if rec is None:
            return {"status": "error", "message": f"unknown plan {plan_id}"}
        if rec["status"] not in ("awaiting_approval", "previewed"):
            return {"status": rec["status"], "plan_id": plan_id, "result": rec["result"]}
        preview = _preview or rec["preview"]
        compiled = self.compile(rec["raw"], rec["caller"])
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
        for plan_id in self.store.plans_with_status("running", "failed_partial", "needs_attention"):
            reports.append(await self._recover_one(plan_id))
        return reports

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
        needs_output = entry["state"] == "compensatable" and "$output" in json.dumps(entry["compensator"] or {})
        if happened and result is None and needs_output:
            return {"status": "error",
                    "message": f"{step} is undone from the tool's result; pass it (e.g. --result "
                               "'{\"refund_id\": \"RF-00042\"}') so the plan can be rolled back"}
        if happened:
            self.store.journal_set(entry["seq"], "COMPLETED", result=result)
        else:
            self.store.journal_set(entry["seq"], "FAILED", error="a human confirmed it did not happen")
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
        requires_human = any(c.info[n].effects.requires_approval for n in write_nodes)
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
        self.store.update_plan(plan_id, status="running")
        remaining = [n for n in compiled.order if n not in compiled.read_frontier]
        for nid in remaining:
            run.outputs.pop(nid, None)
        return await self._execute(run, remaining)

    async def _execute(self, run: Run, nodes: list[str]) -> dict:
        c = run.compiled
        done = set(run.outputs)
        failure: NodeFailure | None = None
        for wave in _waves_of(nodes, c.sched_deps, done):
            results = await asyncio.gather(*(self._run_node(run, nid, real=True) for nid in wave),
                                           return_exceptions=True)
            crash = next((r for r in results if isinstance(r, SimulatedCrash)), None)
            if crash:
                raise crash
            errors = [r for r in results if isinstance(r, BaseException)]
            if errors:
                first = errors[0]
                failure = first if isinstance(first, NodeFailure) else NodeFailure("?", repr(first))
                break
        if failure is None:
            result = self._result(run, "completed")
        else:
            result = await self._settle(run.plan_id, run=run, failure=failure)
        self.store.update_plan(run.plan_id, status=result["status"], result=result)
        return result

    async def _settle(self, plan_id: str, *, run: Run | None = None, failure: NodeFailure | None = None) -> dict:
        """Decide what a plan that stopped early needs, from the journal alone, and do what is safe."""
        journal = self.store.journal(plan_id)
        unknown = [_step(e) for e in journal if e["status"] == "UNKNOWN"]
        # Past the point of no return once something that cannot be undone has happened: the pivot, or a
        # retry-safe step (those are not undone, so the plan can only go forward).
        passed = any(e["state"] in ("irreversible", "idempotent_write") and e["status"] == "COMPLETED"
                     for e in journal)
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
                    raise bad[0]
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
            else:
                raise EvalError(f"op {node.op!r} cannot run")
        except Skipped:
            run.outputs[nid] = SKIPPED
            self.store.put_handle(run.plan_id, nid, SKIP_MARK, tool=ni.tool, labels=ni.labels, trust=ni.trust)
            return
        except Pending:
            run.outputs[nid] = PENDING
            return
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
        key = f"{run.plan_id}:{ni.node.id}:{idx}"
        prior = self.store.journal_find(key)
        if prior is not None:
            if prior["status"] == "COMPLETED":
                return prior["result"]  # rolling forward: this write already happened, never repeat it
            if prior["status"] != "FAILED":
                raise UpstreamError(f"{ni.tool} is {prior['status']} in the journal; refusing to repeat it")
            seq = prior["seq"]  # it definitely did not happen: try again on the same journal entry
            self.store.journal_set(seq, "IN_FLIGHT")
        else:
            seq = self.store.journal_begin(run.plan_id, ni.node.id, idx, ni.tool, eff.state, args, key,
                                           eff.compensator)
        meta = {IDEM_META: key} if eff.idempotency_keys else None
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
                self.store.journal_set(seq, "UNKNOWN", error=str(last))
                raise OutcomeUnknown(f"{ni.tool} got no answer, so it may or may not have happened: {last}")
            self.store.journal_set(seq, "FAILED", error=str(last))
            if conflict and conflict.search(str(last)):
                raise WriteConflict(f"{ni.tool} refused: the data changed after the commit re-read ({last})")
            raise UpstreamError(f"{ni.tool} failed: {last}")
        self._writes_done += 1
        if self._crashed or (self.crash_after_writes is not None and self._writes_done >= self.crash_after_writes):
            self._crashed = True  # the effect happened upstream, but the journal never hears about it
            raise SimulatedCrash(f"crash after {self._writes_done} writes")
        self.store.journal_set(seq, "COMPLETED", result=result)
        return result

    async def _compensate(self, plan_id: str) -> dict:
        """Undo completed compensatable writes, newest first. Each undo is retried; failures are recorded
        (COMPENSATION_FAILED) and retried again by the next `recover`."""
        done, failed = [], []
        entries = [e for e in self.store.journal(plan_id)
                   if e["state"] == "compensatable" and e["status"] in ("COMPLETED", "COMPENSATION_FAILED")]
        for e in reversed(entries):
            error: str | None = None
            for attempt in range(self.compensation_attempts):
                try:
                    tool, args = _compensator_call(e)
                except Exception as ex:  # noqa: BLE001 - e.g. no recorded result to undo from: not retryable
                    error = f"cannot build the undo call: {ex}"
                    break
                try:
                    await self.upstreams.call(tool, args, {IDEM_META: e["idem_key"] + ":compensate"})
                    error = None
                    break
                except SimulatedCrash:
                    raise
                except Exception as ex:  # noqa: BLE001 - every failure must be recorded, never raised mid-rollback
                    error = str(ex)
                    if attempt + 1 < self.compensation_attempts:
                        await asyncio.sleep(self.retry_delay_s * 2 ** attempt)
            if error is None:
                self.store.journal_set(e["seq"], "COMPENSATED")
                done.append(_step(e))
            else:
                self.store.journal_set(e["seq"], "COMPENSATION_FAILED", error=error)
                failed.append({"step": _step(e), "error": error})
        return {"ok": not failed, "compensated": len(done), "failed": failed}

    # ================================================================ recovery

    async def _recover_one(self, plan_id: str) -> dict:
        rec = self.store.get_plan(plan_id)
        compiled = self.compile(rec["raw"], rec["caller"])
        for e in self.store.journal(plan_id):
            if e["status"] != "IN_FLIGHT":
                continue
            # A crash during the call: re-send to learn the outcome, but only where that cannot repeat it.
            eff = compiled.info[e["node_id"]].effects
            if e["state"] == "idempotent_write" or eff.idempotency_keys:
                try:
                    result = await self.upstreams.call(e["tool"], e["args"],
                                                       {IDEM_META: e["idem_key"]} if eff.idempotency_keys else None)
                    self.store.journal_set(e["seq"], "COMPLETED", result=result)
                except UpstreamError as ex:
                    self.store.journal_set(e["seq"], "FAILED" if ex.definite else "UNKNOWN", error=str(ex))
            else:
                self.store.journal_set(e["seq"], "UNKNOWN", error="crashed during the call; outcome unknown")
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
                                  "approval. Submit them as a new plan instead."}
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
                self._reader_cache[key] = _validate_extraction(await self.reader.extract(text, schema), schema)
            out.append({**item, **self._reader_cache[key]} if isinstance(item, dict) else self._reader_cache[key])
        return out[0:1] if single else out

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
        return {
            "handle": f"h://{run.plan_id}/{nid}",
            "source_tool": ni.tool,
            "labels": sorted(ni.labels),
            "summary": summarize(value, trusted=ni.trust == "trusted"),
        }

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


def _step(entry: dict) -> str:
    return f"{entry['node_id']}[{entry['item_index']}]"


def _compensator_call(entry: dict) -> tuple[str, dict]:
    comp = entry["compensator"]
    args = {}
    for name, spec in comp.get("args", {}).items():
        if isinstance(spec, str) and spec.startswith("$"):
            source, segs = parse_io_path(spec)
            args[name] = get_path(entry["args"] if source == "input" else entry["result"], segs)
        else:
            args[name] = spec
    return comp["tool"], args


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
