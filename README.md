# Planbound

**Prepared statements and sagas for MCP agent actions.**

AI agents don't need a babysitter. They need a budget.

Planbound sits between your agent and your existing MCP servers. Instead of calling tools one
by one (or running a generated script), the agent sends **one bounded plan**, written as a short
script that Planbound parses (never runs), or runs a plan it prepared earlier by name. Planbound:

1. **checks it** before anything runs: bounds, data flow, who it may act on, a safe order;
2. **runs only the reads** and shows the exact impact: *"2 refunds, ₹4,500, 1 email"*;
3. **gets it approved by a budget policy**, and pages a human only when a plan is over the limit;
4. **runs undoable steps first**, the point of no return last, and **rolls back** if anything fails.

The model only sees summaries computed by code, never raw text from your tools, so a
"ignore your instructions" note hidden in a support ticket has no way in. Your MCP servers
don't change.

```
Agent ──plan──► Planbound: Check ─► Preview ─► Approve ─► Commit ──► CRM · Billing · Email (unchanged)
      ◄─summaries only─┘
```

## What it guarantees

| Problem | What Planbound does |
|---|---|
| Prompt injection through tool output | Untrusted text never reaches the model; it gets counts, sums, ids |
| Poisoned or changed tool descriptions | Tool contracts are hash-pinned; the model sees operator-reviewed descriptions |
| "How much will this touch?" | Every `map` declares `max_items`; per-call and per-plan cost limits, checked statically and on real data |
| Refund #17 fails after #1–16 ran | Automatic compensation in reverse order |
| Two irreversible actions half-done | Compile error: at most one point of no return, after every undoable step |
| Data changed while waiting for approval | The commit re-reads; a mismatch stops with `E_PREVIEW_STALE`, nothing written |
| Approval fatigue / the agent approving itself | Budget policy signs (Ed25519, separate process); humans only at the edge |
| Many small plans to dodge a limit | The signer keeps a rolling budget with authorization holds |
| Acting on the wrong employee | Identity binding: `employee_id` must be `{"$caller": "employee_id"}` |
| Gateway crash mid-plan | SQLite journal + idempotency keys; `planbound recover` compensates or rolls forward |
| An undo step fails during rollback | Retried with backoff; still failing → `failed_partial`, and `planbound recover` retries it later |
| A write gets no answer | Re-sent with the same key only if that can't repeat it; otherwise `needs_attention` and `planbound resolve`, never a guess |
| Data changes between the re-read and the write | `conditional` writes pass the version read earlier; the server refuses → `E_WRITE_CONFLICT`, rolled back |
| `recover` racing a live gateway, or two commits of one plan | Plan leases: one driver at a time; recover only takes plans whose lease expired |
| A stalled gateway waking up after recovery took over | Fencing tokens: the journal and fencing-aware upstreams refuse the old holder |
| An undo service down for hours | Background reconciliation (`reconcile_seconds`) finishes the rollback when it is back |
| A refund visible before it is rolled back | Reservations (`confirm`): held invisibly, confirmed only past the point of no return |
| An email sent too soon | `hold_seconds`: a cancel window before irreversible steps (`planbound cancel`) |
| An upstream that only claims to dedupe or check versions | `planbound verify` tests each claim; `require_verified` demands proof |
| A lost answer from a server without idempotency keys | `outcome_check`: ask the server whether it happened |
| Starting writes while the undo service is down | `undo_probe`: the plan does not start; alerts when a plan stays stuck |
| A quarantined reader steered by injected text | Several readers must agree, and reader-driven writes always need a human |
| Investigations that decide what to look at next | Multi-stage plans: each stage continues from earlier results with `checkpoint` |
| Money moving without upstream guarantees | `strict_writes`: sensitive tools must declare idempotency keys and a conditional write |

## Benchmark (real MCP calls, simulated model)

Same task every way: *refund C-42's overdue orders under ₹5,000 with no open dispute, email one receipt*.
Direct calling gets every advantage (parallel tool calls, server-side status filter). Full table: [bench/RESULTS.md](bench/RESULTS.md).

| | Direct MCP | Code mode | Planbound, JSON plan | Planbound, script | Planbound, prepared (repeat run) |
|---|---|---|---|---|---|
| LLM turns | 4 | 2 | 2 | 2 | **2** |
| Tokens the model reads | 8,213 | **1,497** | 2,127 | 1,873 | 1,766 |
| Tokens the model writes | 136 | 179 | 348 | 187 | **43** |
| Untrusted text in context (injected note) | 227 tok, **incl. injection** | 0 (this script didn't print it) | **0 (by construction)** | **0** | **0** |
| One refund fails mid-batch | **1 refund left done** | **1 refund left done** | **nothing changed** | **nothing changed** | **nothing changed** |
| Modeled latency | 5.4s | 4.6s | 7.4s | 4.6s | **2.2s** |

Where the cost went, and what removed it:
- **Output tokens drive latency** (60 tok/s in this model). A JSON plan is verbose, so plans can be
  written as a **script**: a small subset of Python syntax that is only parsed, never run, and
  translates one-to-one into the same JSON plan (`tests/test_script.py` checks they are identical).
  Same checks, about half the output; script plans now match code mode.
- **Repeated tasks** don't need a plan at all: `planbound_prepare` stores a plan with `param.*`
  placeholders once, then `planbound_run(name, params)` costs ~40 output tokens. Every run is still
  compiled, previewed, approved and journaled. (The first run costs what the script row costs.)
- **Input tokens**: the plan guide was sent twice (server instructions *and* the tool index) and is
  now sent once, in a shorter, script-first form; summaries no longer list every unique id (they
  stay one `planbound_inspect_handle` call away).

The model is simulated; `bench/live.py` measures the same with a real model.

## Quick start

```bash
uv sync --all-groups
uv run pytest -q                     # 127 tests, incl. fault injection
uv run --extra reader python -m bench.live   # real model + live reader check (needs API credentials)
uv run python -m bench.run           # writes bench/RESULTS.md
uv run python -m bench.demo_setup    # writes demo/: config, pinned manifest, signing keys
```

The demo config includes the optional `runtime` section (`lease_seconds`, `retry_attempts`,
`compensation_attempts`); see [SPEC.md](SPEC.md) §4.

Run the gateway (point any MCP client at it) and the policy signer in another terminal:

```bash
uv run planbound --config demo/planbound.config.json serve
```

```bash
uv run planbound --config demo/planbound.config.json signer --keys demo/keys --budget 50000
```

Other commands: `planbound pending`, `planbound approve <plan_id>`, `planbound deny <plan_id>`,
`planbound recover` (reconcile every unfinished plan), `planbound resolve <plan_id> <step> --happened|--did-not-happen`, `planbound abandon <plan_id> --reason ...`,
`planbound cancel <plan_id>`, `planbound verify <tool> --args JSON`, `planbound stuck`,
`planbound draft` (manifest from dev-mode usage), `planbound show h://plan/node`
(raw data for a human), `planbound keys`.

## A plan

```json
{"planbound": "0.1", "goal": "Refund eligible overdue orders, then email one receipt",
 "nodes": [
  {"id": "cust", "op": "call", "tool": "crm.get_customer", "args": {"customer_id": "C-42"}},
  {"id": "orders", "op": "call", "tool": "billing.list_orders",
   "args": {"customer_id": {"$ref": "cust.customer_id"}, "status": "overdue"}},
  {"id": "disputes", "op": "call", "tool": "billing.list_disputes", "args": {"customer_id": "C-42"}},
  {"id": "eligible", "op": "filter", "input": {"$ref": "orders.orders"},
   "where": {"and": [{"lt": [{"$item": "amount"}, 5000]},
                     {"not": {"in": [{"$item": "order_id"}, {"$ref": "disputes.order_ids"}]}}]}},
  {"id": "refunds", "op": "map", "input": {"$ref": "eligible"}, "max_items": 20,
   "call": {"tool": "billing.refund_order",
            "args": {"order_id": {"$item": "order_id"}, "amount": {"$item": "amount"}}}},
  {"id": "notify", "op": "call", "tool": "email.send_email", "depends_on": ["refunds"],
   "args": {"to": {"$ref": "cust.email"}, "template": "refund_processed",
            "vars": {"count": {"$agg": {"fn": "count", "of": "eligible"}},
                     "total": {"$agg": {"fn": "sum", "of": "eligible", "field": "amount"}}}}}
 ]}
```

With `"max_items": 50`, the compiler refuses: *worst case 500,000 is above the per-plan limit
200,000; lower max_items to at most 20.*

## What's built, and what isn't

Built and tested: compiler and diagnostics, effects manifest (state × egress, labels, trust,
identity, hash pinning, dev-mode inference and drafting), preview and stale check, three approval
tiers with Ed25519 and a budget ledger with holds, saga executor with compensation, retries and
crash recovery and reconciliation (fault-injection tests for failing undo steps, crashes during
rollback, lost answers and concurrent writers), conditional writes, plan leases and fencing tokens,
background reconciliation, multi-stage plans (`checkpoint`), handles and deterministic summaries,
display sink, MCP gateway and CLI.

Real-model benchmark: `bench/live.py` runs the task with a real Claude model, directly over MCP and
through Planbound, and checks the Claude-backed quarantined reader against the live API. Its loop is
tested offline; its numbers need your API credentials (results go to `bench/LIVE_RESULTS.md`).

Read [SPEC.md](SPEC.md) for the full design and its limits.

## Prior art

CaMeL ([arXiv 2503.18813](https://arxiv.org/abs/2503.18813)) and FIDES
([arXiv 2505.23643](https://arxiv.org/abs/2505.23643)) separate control from data and track
information flow; SPA ([arXiv 2608.27234](https://arxiv.org/abs/2608.27234)) checks a declarative
plan before running it. MCPTox ([arXiv 2508.14925](https://arxiv.org/abs/2508.14925)) measured
tool-poisoning attacks on real MCP servers. Undolith, Holdpoint, Countersign and glasshouse gate,
queue or undo individual agent actions. Planbound packages plan-level previews, budgets and saga
recovery as a drop-in MCP gateway.
