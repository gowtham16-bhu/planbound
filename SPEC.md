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
| `idempotency_keys` | The operator confirms the upstream honours `io.planbound/idempotency-key` (default **false**). Without it a write that gets no answer is never re-sent; it becomes `UNKNOWN` |
| `conditional` | `{"arg": "expected_version", "conflict_pattern"?: "<regex>"}`: the upstream refuses the write if `arg` no longer matches. The plan must fill `arg` from a read (`$ref`/`$item`), else `E_CONDITIONAL_REQUIRED`; a refusal matching the pattern fails the step with `E_WRITE_CONFLICT` and the plan rolls back |
| `compensator` | The undo call. Its tool must be listed and be `idempotent_write` or declare `idempotency_keys: true`, because rollback retries it; otherwise the manifest does not load |
| `confirm_read_only` | Needed to mark a tool `read_only` when its name says it writes (`refund_`, `send_`, `delete_`, …); otherwise the manifest does not load |

**`defaults.strict_writes`.** When true, every *sensitive* write (one with a `cost` or
`requires_approval`) must declare both `idempotency_keys: true` and `conditional`, or the plan fails
with `E_UNSAFE_WRITE`. Turn it on for anything that moves money.

**Hint conflicts.** A tool the manifest calls `read_only` but the server annotates `destructiveHint`
is refused with `E_EFFECTS_CONFLICT`.

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
`E_SAGA_ORDER` `E_COST_BOUND` (suggests a smaller `max_items`) `E_CONDITIONAL_REQUIRED`
`E_UNSAFE_WRITE` `E_EFFECTS_CONFLICT`.

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
   `plan:node:item`. Errors come in two kinds. A *definite* error (the server answered "no") means
   the write did not happen; only `idempotent_write` calls are retried on it. *No answer* (timeout,
   dropped connection) means it may have happened: the call is re-sent with the same key only if
   re-sending cannot repeat it (`idempotent_write`, or `idempotency_keys: true`); otherwise the step
   is journaled `UNKNOWN`. A conditional write refused because the data changed → `E_WRITE_CONFLICT`.
6. **Failure.** The *point of no return* is passed once the pivot or any `idempotent_write` step
   has completed (those are never undone). Before it: compensators run newest first, each retried
   with backoff (`compensation_attempts`, default 5) → `failed_compensated`; an undo that still fails
   is journaled `COMPENSATION_FAILED` → `failed_partial`, and `planbound recover` retries it. After
   it: `failed_partial`; `planbound recover` finishes the remaining retry-safe steps. Any `UNKNOWN`
   step → `needs_attention`, listing the steps (undoable steps known to have happened are still
   compensated when the pivot was not reached).
7. **Reconciliation** (`planbound recover`). Runs over every plan that is `running` (crash),
   `failed_partial` or `needs_attention`, and is safe to repeat. `IN_FLIGHT` writes are re-sent with
   the same key where that is safe, else marked `UNKNOWN`. Then: pivot outcome unknown →
   `needs_attention`, nothing compensated (never guessed). Pivot not reached → compensate →
   `recovered_compensated`. Past it → roll forward; a write already `COMPLETED` in the journal is
   replayed from the journal, never re-sent. Rolling forward never starts an irreversible step that
   had not run. `planbound resolve <plan> <node[i]> --happened|--did-not-happen [--result JSON]`
   records what a human found upstream for an `UNKNOWN` step; the next `recover` continues from it.
   The policy signer releases a finalized budget hold once a plan is fully compensated.
   `planbound abandon <plan> --reason "..."` closes a `failed_partial` or `needs_attention` plan that
   recovery cannot finish (for example a second irreversible step it will not start long after
   approval). Nothing upstream is touched; the result lists what was left as is, and `recover` no
   longer picks it up.
8. **Leases.** One process drives a plan at a time. Commit claims the plan atomically
   (`previewed`/`awaiting_approval` → `running`), so two commits of one plan run it once. The holder
   renews a lease (`runtime.lease_seconds` in the gateway config, default 60s) before every write and every undo step. `recover`
   only takes plans whose lease has expired, so it never compensates a plan a live gateway is still
   running; a gateway that finds its lease taken stops at once (`LeaseLost`).

**Gateway config `runtime` section** (optional; used by `serve`, `recover`, `resolve`, `abandon`):

```json
"runtime": {"lease_seconds": 60, "retry_attempts": 3, "compensation_attempts": 5}
```

| Option | Default | Meaning |
|---|---|---|
| `lease_seconds` | 60 (1–86400) | Lease length. Longer than your slowest upstream write; shorter than you will wait for `recover` to take over a dead gateway |
| `retry_attempts` | 3 (1–20) | Attempts per write: definite errors for `idempotent_write`, lost answers where re-sending is safe |
| `compensation_attempts` | 5 (1–50) | Attempts per undo step during one rollback, with exponential backoff |

Unknown options and out-of-range values stop the gateway from starting.

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
  remains between the commit re-read and the write, and a lost answer needs a human. Planbound can
  require them (`strict_writes`) but cannot make a server honour what it claims to.
- **Rollback can still be stuck.** Undo steps are retried and reconciled, but if a compensator keeps
  failing the plan stays `failed_partial` until the upstream recovers or a human steps in.
- **Leases are not fencing.** A gateway that stalls longer than `lease_seconds` *between* renewing and
  sending a write can still send it after recovery took over. Recovery then sees the write as in
  flight and handles it like a crash. Closing that gap needs upstreams that check a fencing token.
- **The manifest is trusted.** The load-time and hint checks catch obvious mislabels, not a
  write tool with a read-like name and no annotations.
- **Approval channel.** The gateway only holds public keys, but the policy signer and the human
  must run where the agent cannot reach their private keys.
- **Identity comes from the host.** v0.1 reads the caller from config or `PLANBOUND_CALLER`.
- **Plans cost more output tokens than a code-mode script** (see `bench/RESULTS.md`).
