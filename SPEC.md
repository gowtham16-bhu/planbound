# Planbound v0.1: specification

> **Prepared statements and sagas for MCP agent actions.**
> The agent sends one bounded plan. Planbound checks it, runs the reads, shows the exact
> impact, gets it approved by a budget policy (a human only at the edge), and executes it
> in an undo-safe order, against ordinary MCP servers that need no changes.

This document describes what the code in `src/planbound/` implements.

```
 Agent (any MCP client)
   │  planbound_search_tools · planbound_submit_plan · planbound_commit · planbound_inspect_handle
   ▼
┌─────────────────────────── Planbound gateway (plain code) ────────────────────────────┐
│ Compiler ─► Preview (reads run, writes simulated) ─► Approval ─► Saga executor        │
│    ▲                         │                         │              │               │
│ Effects manifest       Handle store + summaries   Ed25519 check   SQLite journal      │
└────────────────────────────────────────────┬──────────────────────────────────────────┘
                                             │ plain MCP tools/call (+ idempotency key in _meta)
                                Existing MCP servers, unchanged
        Separate process: policy signer (private keys, rolling budget ledger) · humans via CLI
```

---

## 1. Effects manifest (`planbound.effects.json`)

Owned by the operator. Server annotations are hints; only this file is trusted (enforce mode).

```json
{
  "mode": "enforce",
  "defaults": {"unlisted_tool_policy": "reject", "max_plan_cost": 500000},
  "approval": {"auto_approve_max_cost": 2000, "ttl_seconds": 300,
               "approvers": {"policy": "<ed25519 public key>", "human": "<ed25519 public key>"}},
  "tools": {
    "billing.refund_order": {
      "state": "compensatable", "egress": "internal",
      "contract_sha256": "…",
      "description": "Refund one order. Undo with billing.cancel_refund.",
      "compensator": {"tool": "billing.cancel_refund", "args": {"refund_id": "$output.refund_id"}},
      "cost": "$input.amount",
      "limits": {"max_per_call": 10000, "max_per_plan": 200000},
      "output_labels": ["financial"]
    },
    "hris.update_address": {
      "state": "compensatable",
      "identity": {"arg": "employee_id", "caller": "employee_id", "unless_role": "hr_admin"},
      "compensator": {"tool": "hris.update_address",
                      "args": {"employee_id": "$input.employee_id", "address": "$output.previous_address"}}
    },
    "email.send_email": {"state": "irreversible", "allowed_input_labels": ["pii", "financial"]}
  }
}
```

| Field | Meaning |
|---|---|
| `state` | `read_only` · `idempotent_write` (retried until it succeeds) · `compensatable` (has a compensator) · `irreversible` |
| `egress` | `internal` · `external`. Labelled data never reaches an external tool unless `allowed_input_labels` lists it |
| `contract_sha256` | SHA-256 of `name + description + inputSchema + outputSchema`. Any change → `E_SCHEMA_CHANGED`. This covers poisoned *descriptions* too |
| `description` | Operator-reviewed text. In enforce mode the model sees this, never the server's own description |
| `output_labels` / `allowed_input_labels` | Data-flow labels (`pii`, `financial`, …), propagated through the plan |
| `output_trust` | `untrusted` (default) or `trusted`. Trusted output (for example HR policy docs) may reach the model as text |
| `cost`, `limits` | `$input.<field>` cost per call; `max_per_call`, `max_per_plan` |
| `identity` | The argument must be `{"$caller": "<attr>"}` (the signed-in user, set by the host, never by the model) unless the caller has `unless_role` |
| `requires_approval` | Always send plans using this tool to the policy signer |
| `idempotency_keys` | Whether the upstream honours `io.planbound/idempotency-key` (default true) |

**Modes.** `enforce`: unlisted tools are rejected. `dev`: effects are inferred conservatively
(read-like names or `readOnlyHint` → `read_only`; a `cancel_/delete_/undo_` partner → `compensatable`;
anything else → `irreversible`), results are marked unverified, and tool usage is recorded so
`planbound draft` can write a manifest for only the tools actually used.

---

## 2. Plan IR

```json
{"planbound": "0.1", "goal": "…", "nodes": [ … ], "return": ["node", …],
 "allow_partial_irreversible": false}
```

| `op` | Fields | Output |
|---|---|---|
| `call` | `tool`, `args`, `when?`, `depends_on?` | the tool's result |
| `filter` | `input` (list), `where` | matching items |
| `partition` | `input` (list), `where` | `{"match": [...], "rest": [...]}` |
| `map` | `input` (list), `max_items` (**required**), `call: {tool, args}`, `concurrency?` (default 8) | list of results |
| `quarantine_read` | `input`, `field`, `max_items`, `schema` of closed types | items plus the extracted fields |
| `display` | `input` | data is written for the human; the model sees only a receipt |
| `checkpoint` | reserved for v0.2 | — |

**Values:** literals, `{"$ref": "node.a.b"}`, `{"$item": "field"}` (map args, `where`),
`{"$agg": {"fn": "count|distinct_count|sum|min|max|mean", "of": "node.path", "field": "f", "default": d}}`,
`{"$caller": "attr"}`.
**Conditions:** `eq ne gt gte lt lte` (`[a, b]`), `in` (`[a, list]`), `and`, `or`, `not`, `exists`.
No loops, functions or string operations, so every plan terminates and can be analysed before it runs.

---

## 3. Compiler (before any upstream call)

All errors come back together with `code`, `node_id`, JSON `path`, `message` and `hint`:

`E_PLAN_SHAPE` `E_BAD_ID` `E_DUPLICATE_ID` `E_UNKNOWN_OP` `E_UNSUPPORTED_OP` `E_UNKNOWN_TOOL`
`E_UNLISTED_TOOL` `E_SCHEMA_CHANGED` `E_MISSING_ARG` `E_UNKNOWN_ARG` `E_ARG_TYPE` `E_UNKNOWN_REF`
`E_CYCLE` `E_UNKNOWN_FIELD` (lists available fields) `E_ITEM_OUTSIDE_MAP` `E_UNKNOWN_CALLER_ATTR`
`E_BAD_AGG` `E_MISSING_MAP_BOUND` `E_QUARANTINE_SCHEMA` `E_QUARANTINE_READER_UNCONFIGURED`
`E_IDENTITY` `E_LABEL_FLOW` `E_WHEN_ON_WRITE` `E_MULTIPLE_PIVOTS` `E_IRREVERSIBLE_NOT_TERMINAL`
`E_SAGA_ORDER` `E_COST_BOUND` (suggests a smaller `max_items`).

**Saga shape** (enforced here, not hoped for at runtime):
`compensatable steps → at most one irreversible pivot → idempotent_write steps`.
The compiler adds the ordering edges itself; an irreversible step with undoable steps after it
is rejected; a second irreversible step needs `allow_partial_irreversible`.

**Static worst case:** a `map` counts as `max_items` calls; cost uses `max_per_call`.

---

## 4. Lifecycle

1. **Preview.** The *read frontier* (read-only calls and pure nodes whose inputs are all reads)
   runs for real. Every write is resolved but not called, which gives exact call counts, exact
   cost, per-call and per-plan limit checks on real data, and a `preview_hash` over the write set.
2. **Approval tier.** No writes → none. Cost ≤ `auto_approve_max_cost` and no
   `requires_approval` → **Tier 1, auto**. Otherwise → **Tier 2**: an approval request goes to the
   policy signer.
3. **Signer (separate process).** It holds the private keys and a rolling-budget ledger. Within
   budget, it places a **hold** and signs `{plan_id, plan_hash, preview_hash, expires_at, signer}`
   with Ed25519. Over budget, over its per-plan cap, or `requires_approval` → **escalated** to a
   human, who signs with the human key (`planbound approve`). Holds are finalised when a plan
   completes and released when it rolls back, goes stale, is denied or expires.
4. **Commit.** The gateway verifies the signature with the public key (it cannot forge one),
   re-runs the reads, and recomputes the write set. A different hash → `E_PREVIEW_STALE`, nothing
   written. (With Tier 1, no time passes, so the preview's reads are reused.)
5. **Execute.** Waves in the saga order, map items in parallel. Before each write the journal
   records it (`IN_FLIGHT`, fsynced SQLite WAL), and the call carries the idempotency key
   `plan:node:item`. `idempotent_write` calls are retried.
6. **Failure.** Before the pivot: compensators run newest first → `failed_compensated`
   (or `failed_partial` if one fails). After the pivot: `failed_partial`; `planbound recover`
   retries the remaining retry-safe steps.
7. **Crash recovery** (`planbound recover`). `IN_FLIGHT` writes are re-sent with the same
   idempotency key to learn their outcome. Pivot not reached → compensate → `recovered_compensated`.
   Pivot done → roll forward. Pivot outcome unknown → `needs_attention` (never guessed).

---

## 5. What the model sees

Every result is stored as a handle `h://plan/node`. The model gets a summary computed by code:
numbers (value or min/max/sum/mean), booleans, strings that look like identifiers
(`^[A-Za-z0-9_\-:.@]{1,32}$`, but not email addresses) as values or histograms (≤ 20 distinct),
and for any other string only its length. `planbound_inspect_handle` adds filtered and grouped
views under the same rules. Trusted sources and `display` receipts are the only exceptions.

---

## 6. Honest limits

- **Saga semantics, not ACID.** Others can see a refund before it is compensated; an email cannot be unsent.
- **Classification can be steered.** A quarantined reader can only return closed types, so injected
  text cannot add actions, but a crafted text can still pick a wrong category. Bounds and budgets stay.
- **Upstream support matters.** Without idempotency keys or conditional writes, a small race window
  remains between the commit re-read and the write.
- **Approval channel.** The gateway only holds public keys, but the policy signer and the human
  must run where the agent cannot reach their private keys.
- **Identity comes from the host.** v0.1 reads the caller from config or `PLANBOUND_CALLER`.
- **Plans cost more output tokens than a code-mode script** (see `bench/RESULTS.md`).
