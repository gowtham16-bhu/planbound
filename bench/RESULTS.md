# Planbound benchmark (simulated model, real MCP calls)

Latency model: 0.5s per turn + input/10000 tok/s + output/60 tok/s + measured tool time. Tokens ≈ characters / 4. The prepared row is a repeat run: the plan was prepared once before (that first run costs the same as the script row).

## refund_40_orders

| Strategy | LLM turns | Input tokens | Output tokens | Tool calls | Untrusted text in context | Modeled latency | End state |
|---|---|---|---|---|---|---|---|
| Direct MCP (parallel calls) | 4 | 8,213 | 136 | 6 | 172 tok | 5.38s | done |
| MCP code mode | 2 | 1,497 | 179 | 6 | 0 tok | 4.56s | done |
| Planbound (JSON plan) | 2 | 2,127 | 348 | 6 | 0 tok | 7.37s | done |
| Planbound (script) | 2 | 1,873 | 187 | 6 | 0 tok | 4.58s | done |
| Planbound (prepared, repeat run) | 2 | 1,766 | 43 | 6 | 0 tok | 2.17s | done |

## refund_400_orders

| Strategy | LLM turns | Input tokens | Output tokens | Tool calls | Untrusted text in context | Modeled latency | End state |
|---|---|---|---|---|---|---|---|
| Direct MCP (parallel calls) | 4 | 17,490 | 399 | 21 | 172 tok | 10.66s | done |
| MCP code mode | 2 | 1,497 | 179 | 21 | 0 tok | 4.57s | done |
| Planbound (JSON plan) | 2 | 2,130 | 348 | 21 | 0 tok | 7.54s | done |
| Planbound (script) | 2 | 1,875 | 187 | 21 | 0 tok | 4.76s | done |
| Planbound (prepared, repeat run) | 2 | 1,768 | 43 | 21 | 0 tok | 2.35s | done |

## refund_failure_mid_batch

| Strategy | LLM turns | Input tokens | Output tokens | Tool calls | Untrusted text in context | Modeled latency | End state |
|---|---|---|---|---|---|---|---|
| Direct MCP (parallel calls) | 3 | 5,882 | 101 | 5 | 172 tok | 3.95s | PARTIAL: 1 refunds done, no receipt |
| MCP code mode | 2 | 1,508 | 179 | 5 | 0 tok | 4.47s | PARTIAL: 1 refunds done, no receipt |
| Planbound (JSON plan) | 2 | 2,045 | 348 | 6 | 0 tok | 7.37s | clean (nothing changed) |
| Planbound (script) | 2 | 1,882 | 187 | 6 | 0 tok | 4.58s | clean (nothing changed) |
| Planbound (prepared, repeat run) | 2 | 1,776 | 43 | 6 | 0 tok | 2.17s | clean (nothing changed) |

- Direct MCP (parallel calls): a refund failed; the model stops and reports, the other refunds stay done
- MCP code mode: the script raised mid-batch; refunds that already ran stay done
- Planbound (JSON plan): plan ended failed_compensated: billing.refund_order failed: Error executing tool refund_order: payment processor declined refund for ORD-1009
- Planbound (script): plan ended failed_compensated: billing.refund_order failed: Error executing tool refund_order: payment processor declined refund for ORD-1009
- Planbound (prepared, repeat run): plan ended failed_compensated: billing.refund_order failed: Error executing tool refund_order: payment processor declined refund for ORD-1009

## injected_order_note

| Strategy | LLM turns | Input tokens | Output tokens | Tool calls | Untrusted text in context | Modeled latency | End state |
|---|---|---|---|---|---|---|---|
| Direct MCP (parallel calls) | 4 | 8,257 | 136 | 6 | 227 tok (incl. injection) | 5.35s | done |
| MCP code mode | 2 | 1,497 | 179 | 6 | 0 tok | 4.57s | done |
| Planbound (JSON plan) | 2 | 2,127 | 348 | 6 | 0 tok | 7.36s | done |
| Planbound (script) | 2 | 1,873 | 187 | 6 | 0 tok | 4.58s | done |
| Planbound (prepared, repeat run) | 2 | 1,766 | 43 | 6 | 0 tok | 2.18s | done |

