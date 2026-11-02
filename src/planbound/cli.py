"""`planbound` command line: serve the gateway, run the signer, approve, recover, draft a manifest."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from .approval import BudgetPolicy, PolicySigner, describe_request, generate_keys
from .gateway import Gateway, GatewayConfig, build_server, make_reader, send_alerts
from .manifest import Manifest, infer_effects
from .runtime import Runtime
from .store import Store
from .upstream import MCPUpstreams


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="planbound", description=__doc__)
    p.add_argument("--config", default="planbound.config.json")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve", help="run the gateway as an MCP server on stdio")
    k = sub.add_parser("keys", help="create the policy and human signing keys")
    k.add_argument("--dir", default=".planbound/keys")
    s = sub.add_parser("signer", help="run the tier-2 policy signer (separate process, holds the private keys)")
    s.add_argument("--keys", default=".planbound/keys")
    s.add_argument("--budget", type=float, required=True, help="rolling budget, e.g. 500000")
    s.add_argument("--window-hours", type=float, default=24)
    s.add_argument("--max-plan-cost", type=float)
    s.add_argument("--once", action="store_true")
    for name in ("approve", "deny"):
        a = sub.add_parser(name, help=f"tier 3: {name} an escalated plan as a human")
        a.add_argument("plan_id")
        a.add_argument("--keys", default=".planbound/keys")
        a.add_argument("--yes", action="store_true")
    sub.add_parser("pending", help="list approval requests waiting for the signer or a human")
    st = sub.add_parser("stuck", help="list plans stuck in failed_partial or needs_attention")
    st.add_argument("--after", type=float, default=0, help="only plans stuck at least this many seconds")
    rc = sub.add_parser("recover", help="reconcile plans that did not end cleanly: retry failed undo steps, "
                                        "compensate or finish crashed plans")
    rc.add_argument("--watch", action="store_true", help="keep reconciling until stopped")
    rc.add_argument("--interval", type=float, default=30.0, help="seconds between passes with --watch")
    r = sub.add_parser("resolve", help="record what happened to a write whose outcome was unknown")
    r.add_argument("plan_id")
    r.add_argument("step", help="the step as reported, e.g. refunds[3]")
    g = r.add_mutually_exclusive_group(required=True)
    g.add_argument("--happened", action="store_true", help="the write did happen upstream")
    g.add_argument("--did-not-happen", action="store_true", help="the write did not happen upstream")
    r.add_argument("--result", help="the tool's result as JSON, so the step can be undone")
    sub.add_parser("draft", help="draft a manifest from the tools actually used in dev mode")
    vf = sub.add_parser("verify", help="test the guarantees a write tool claims (run against a sandbox)")
    vf.add_argument("tool")
    vf.add_argument("--args", required=True, help="sample arguments as JSON; probes are undone afterwards")
    vf.add_argument("--allow-irreversible", action="store_true")
    cn = sub.add_parser("cancel", help="stop a plan before its point of no return; what ran is undone")
    cn.add_argument("plan_id")
    ab = sub.add_parser("abandon", help="close a plan recovery cannot finish; nothing upstream is touched")
    ab.add_argument("plan_id")
    ab.add_argument("--reason", required=True, help="why, for the record (e.g. 'refund reversed by finance')")
    sh = sub.add_parser("show", help="print a handle's raw data for a human (never sent to the model)")
    sh.add_argument("handle")
    args = p.parse_args(argv)

    if args.cmd == "keys":
        pubs = generate_keys(args.dir)
        print(json.dumps({"approval": {"approvers": pubs}}, indent=2))
        print(f"Private keys written to {args.dir}. Put the public keys above in your manifest.", file=sys.stderr)
        return
    cfg = GatewayConfig.load(args.config)
    if args.cmd == "serve":
        build_server(Gateway(cfg)).run("stdio")
    elif args.cmd == "signer":
        _signer(cfg, args)
    elif args.cmd in ("approve", "deny"):
        store = Store(cfg.db_path)
        req = store.get_approval(args.plan_id)
        if req is None:
            sys.exit(f"no approval request for {args.plan_id}")
        print(describe_request(req))
        if not args.yes and input(f"{args.cmd} this plan? [y/N] ").strip().lower() != "y":
            sys.exit("cancelled")
        signer = PolicySigner(store, Path(cfg.data_dir) / "signer.db", args.keys, BudgetPolicy(budget=float("inf")))
        print(json.dumps(signer.human_decide(args.plan_id, args.cmd == "approve")))
    elif args.cmd == "stuck":
        for plan in _stuck(cfg, args.after):
            print(json.dumps({"plan_id": plan["plan_id"], "status": plan["status"],
                              "stuck_seconds": round(time.time() - plan["stuck_since"]),
                              "goal": plan["raw"].get("goal")}))
    elif args.cmd == "pending":
        store = Store(cfg.db_path)
        for req in store.approvals_with_status("pending", "escalated"):
            print(describe_request(req), end="\n\n")
    elif args.cmd == "recover":
        if not args.watch:
            print(json.dumps(asyncio.run(_recover(cfg)), indent=2))
            return
        print(f"Reconciling every {args.interval:g}s; Ctrl-C to stop.", file=sys.stderr)
        try:
            while True:
                for report in asyncio.run(_recover(cfg)):
                    if report["status"] != "skipped":
                        print(json.dumps(report), flush=True)
                asyncio.run(send_alerts(Store(cfg.db_path), cfg.alerts))
                time.sleep(args.interval)
        except KeyboardInterrupt:
            return
    elif args.cmd == "resolve":
        store = Store(cfg.db_path)
        rt = Runtime(store, _NoUpstreams(), Manifest.load(cfg.manifest_path), cfg.caller or {},
                     **(cfg.runtime or {}))
        out = rt.resolve(args.plan_id, args.step, args.happened, json.loads(args.result) if args.result else None)
        print(json.dumps(out, indent=2))
        if out["status"] == "error":
            sys.exit(1)
    elif args.cmd == "verify":
        out = asyncio.run(_verify(cfg, args.tool, json.loads(args.args), args.allow_irreversible))
        print(json.dumps(out, indent=2))
        print("Paste this into the manifest as the tool's \"verified\" entry." if out["passed"] else
              f"Verification FAILED for: {', '.join(out['failed'])}. Do not claim these in the manifest.",
              file=sys.stderr)
        if not out["passed"]:
            sys.exit(1)
    elif args.cmd == "cancel":
        rt = Runtime(Store(cfg.db_path), _NoUpstreams(), Manifest.load(cfg.manifest_path), cfg.caller or {},
                     **(cfg.runtime or {}))
        out = rt.cancel(args.plan_id)
        print(json.dumps(out, indent=2))
        if out["status"] == "error":
            sys.exit(1)
    elif args.cmd == "abandon":
        rt = Runtime(Store(cfg.db_path), _NoUpstreams(), Manifest.load(cfg.manifest_path), cfg.caller or {},
                     **(cfg.runtime or {}))
        out = rt.abandon(args.plan_id, args.reason)
        print(json.dumps(out, indent=2))
        if out["status"] == "error":
            sys.exit(1)
    elif args.cmd == "draft":
        out = asyncio.run(_draft(cfg))
        print(f"Draft written to {out}. Review every entry before using it in enforce mode.")
    elif args.cmd == "show":
        store = Store(cfg.db_path)
        plan_id, node_id = args.handle.removeprefix("h://").split("/")
        h = store.get_handle(plan_id, node_id)
        if h is None:
            sys.exit("unknown handle")
        print(json.dumps(h["data"], indent=2, ensure_ascii=False))


def _signer(cfg: GatewayConfig, args) -> None:
    policy = BudgetPolicy(budget=args.budget, window_s=args.window_hours * 3600, max_plan_cost=args.max_plan_cost)
    signer = PolicySigner(Store(cfg.db_path), Path(cfg.data_dir) / "signer.db", args.keys, policy)
    print(f"Policy signer running: budget {args.budget:g} per {args.window_hours:g}h", file=sys.stderr)
    while True:
        for d in signer.process():
            print(json.dumps(d), flush=True)
        if args.once:
            return
        time.sleep(0.5)


def _stuck(cfg: GatewayConfig, after: float) -> list[dict]:
    """All stuck plans, alerted or not."""
    store = Store(cfg.db_path)
    ids = store.plans_with_status("failed_partial", "needs_attention")
    plans = [store.get_plan(i) for i in ids]
    return [p for p in plans if p["stuck_since"] and time.time() - p["stuck_since"] >= after]


class _NoUpstreams:
    """`resolve` and `abandon` only edit the journal; they never call a tool."""

    def __init__(self) -> None:
        self.catalog: dict = {}

    async def call(self, tool, args, meta=None):
        raise RuntimeError("resolve does not call upstream tools")


async def _verify(cfg: GatewayConfig, tool: str, sample: dict, allow_irreversible: bool) -> dict:
    from .verify import verify_tool

    async with MCPUpstreams(cfg.servers) as up:
        return await verify_tool(up, Manifest.load(cfg.manifest_path), tool, sample,
                                 allow_irreversible=allow_irreversible)


async def _recover(cfg: GatewayConfig) -> list[dict]:
    async with MCPUpstreams(cfg.servers) as up:
        rt = Runtime(Store(cfg.db_path), up, Manifest.load(cfg.manifest_path), cfg.caller or {},
                     reader=make_reader(cfg.reader), **(cfg.runtime or {}))
        return await rt.recover()


async def _draft(cfg: GatewayConfig) -> Path:
    store = Store(cfg.db_path)
    async with MCPUpstreams(cfg.servers) as up:
        manifest = Manifest(mode="enforce", unlisted_tool_policy="reject")
        for name in store.used_tools():
            if name in up.catalog:
                eff = infer_effects(up.catalog[name], up.catalog)
                eff.inferred = False
                eff.description = up.catalog[name].description
                manifest.tools[name] = eff
                comp = eff.compensator and eff.compensator["tool"]
                if comp and comp not in manifest.tools:
                    ceff = infer_effects(up.catalog[comp], up.catalog)
                    ceff.inferred, ceff.state = False, "idempotent_write"
                    manifest.tools[comp] = ceff
    out = Path(cfg.manifest_path).with_suffix(".draft.json")
    out.write_text(json.dumps(manifest.to_json(), indent=2))
    return out


if __name__ == "__main__":
    main()
