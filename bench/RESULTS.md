# Planbound benchmark (simulated model, real MCP calls)

Latency model: 0.5s per turn + input/10000 tok/s + output/60 tok/s + measured tool time. Tokens ≈ characters / 4.

## refund_40_orders

| Strategy | LLM turns | Input tokens | Output tokens | Tool calls | Untrusted text in context | Modeled latency | End state |
|---|---|---|---|---|---|---|---|
| Direct MCP (parallel calls) | 4 | 8,213 | 136 | 6 | 172 tok | 5.39s | done |
| MCP code mode | 2 | 1,497 | 179 | 6 | 0 tok | 4.56s | done |
| Planbound | 2 | 3,223 | 348 | 6 | 0 tok | 7.48s | done |

## refund_400_orders

| Strategy | LLM turns | Input tokens | Output tokens | Tool calls | Untrusted text in context | Modeled latency | End state |
|---|---|---|---|---|---|---|---|
| Direct MCP (parallel calls) | 4 | 17,490 | 399 | 21 | 172 tok | 10.67s | done |
| MCP code mode | 2 | 1,497 | 179 | 21 | 0 tok | 4.56s | done |
| Planbound | 2 | 3,510 | 348 | 21 | 0 tok | 7.7s | done |

## refund_failure_mid_batch

| Strategy | LLM turns | Input tokens | Output tokens | Tool calls | Untrusted text in context | Modeled latency | End state |
|---|---|---|---|---|---|---|---|
| Direct MCP (parallel calls) | 3 | 5,882 | 101 | 5 | 172 tok | 3.95s | PARTIAL: 1 refunds done, no receipt |
| MCP code mode | 2 | 1,508 | 179 | 5 | 0 tok | 4.48s | PARTIAL: 1 refunds done, no receipt |
| Planbound | 2 | 3,104 | 348 | 6 | 0 tok | 7.47s | clean (nothing changed) |

- Direct MCP (parallel calls): a refund failed; the model stops and reports, the other refunds stay done
- MCP code mode: the script raised mid-batch; refunds that already ran stay done
- Planbound: plan ended failed_compensated: billing.refund_order failed: Error executing tool refund_order: payment processor declined refund for ORD-1009

## injected_order_note

| Strategy | LLM turns | Input tokens | Output tokens | Tool calls | Untrusted text in context | Modeled latency | End state |
|---|---|---|---|---|---|---|---|
| Direct MCP (parallel calls) | 4 | 8,257 | 136 | 6 | 227 tok (incl. injection) | 5.35s | done |
| MCP code mode | 2 | 1,497 | 179 | 6 | 0 tok | 4.56s | done |
| Planbound | 2 | 3,223 | 348 | 6 | 0 tok | 7.49s | done |

