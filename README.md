# Planbound

**Prepared statements and sagas for MCP agent actions.**

AI agents don't need a babysitter. They need a budget.

Planbound sits between your agent and your existing MCP servers. Instead of calling tools one
by one (or running a generated script), the agent sends **one bounded plan**. Planbound:

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

## Benchmark (real MCP calls, simulated model)

Same task three ways: *refund C-42's overdue orders under ₹5,000 with no open dispute, email one receipt*.
Direct calling gets every advantage (parallel tool calls, server-side status filter). Full table: [bench/RESULTS.md](bench/RESULTS.md).

| | Direct MCP | Code mode | Planbound |
|---|---|---|---|
| LLM turns | 4 | 2 | **2** |
| Tokens the model reads | 7,927 | 1,442 | 3,195 |
| Tokens the model writes | 136 | 179 | 348 |
| Untrusted text in the model's context (injected note) | 227 tok, **incl. injection** | 0 (this script didn't print it) | **0 (by construction)** |
| One refund fails mid-batch | **1 refund left done** | **1 refund left done** | **nothing changed** |
| Modeled latency | 5.4s | **4.6s** | 7.5s |

Honest reading: Planbound matches code mode on turns, writes about twice the tokens of a short
script (a JSON plan is verbose), and is slower in this latency model, which charges 60 tok/s for
output. It wins on what code mode can't promise: the impact is known before anything is written,
untrusted text can't reach the model, and failures leave nothing half-done. The model is
simulated; run against a real model before quoting latency.

## Quick start

```bash
uv sync --all-groups
uv run pytest -q                     # 41 tests
uv run python -m bench.run           # writes bench/RESULTS.md
uv run python -m bench.demo_setup    # writes demo/: config, pinned manifest, signing keys
```

Run the gateway (point any MCP client at it) and the policy signer in another terminal:

```bash
uv run planbound --config demo/planbound.config.json serve
```

```bash
uv run planbound --config demo/planbound.config.json signer --keys demo/keys --budget 50000
```

Other commands: `planbound pending`, `planbound approve <plan_id>`, `planbound deny <plan_id>`,
`planbound recover`, `planbound draft` (manifest from dev-mode usage), `planbound show h://plan/node`
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
crash recovery, handles and deterministic summaries, display sink, MCP gateway and CLI.

Not yet verified: the Claude-backed quarantined reader (`planbound.reader.AnthropicReader`) has not
been run against the live API here; tests use a deterministic reader. `checkpoint` is reserved for v0.2.

Read [SPEC.md](SPEC.md) for the full design and its limits.

## Prior art

CaMeL ([arXiv 2503.18813](https://arxiv.org/abs/2503.18813)) and FIDES
([arXiv 2505.23643](https://arxiv.org/abs/2505.23643)) separate control from data and track
information flow; SPA ([arXiv 2608.27234](https://arxiv.org/abs/2608.27234)) checks a declarative
plan before running it. MCPTox ([arXiv 2508.14925](https://arxiv.org/abs/2508.14925)) measured
tool-poisoning attacks on real MCP servers. Undolith, Holdpoint, Countersign and glasshouse gate,
queue or undo individual agent actions. Planbound packages plan-level previews, budgets and saga
recovery as a drop-in MCP gateway.
