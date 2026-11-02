"""Effects manifests and plans for the demos and the benchmark."""

from __future__ import annotations

from planbound.manifest import Manifest, UpstreamTool


def pin(manifest: dict, catalog: dict[str, UpstreamTool]) -> Manifest:
    """The operator's review step: pin each listed tool's current contract hash."""
    for name, entry in manifest["tools"].items():
        entry.setdefault("contract_sha256", catalog[name].contract_sha256())
    return Manifest.from_json(manifest)


# ---------------------------------------------------------------- refunds


def refund_manifest(catalog: dict, approvers: dict | None = None, *, auto_max: float = 0) -> Manifest:
    return pin({
        "mode": "enforce",
        "defaults": {"unlisted_tool_policy": "reject", "max_plan_cost": 500000},
        "approval": {"auto_approve_max_cost": auto_max, "ttl_seconds": 300, "approvers": approvers or {}},
        "tools": {
            "crm.get_customer": {"state": "read_only", "output_labels": ["pii"],
                                 "description": "Customer profile: id, name, email, tier."},
            "billing.list_orders": {"state": "read_only", "output_labels": ["financial"],
                                    "description": "A customer's orders; filter by status."},
            "billing.list_disputes": {"state": "read_only",
                                      "description": "Order ids with an open dispute. Never refund these."},
            "billing.refund_order": {
                "state": "compensatable", "output_labels": ["financial"], "idempotency_keys": True,
                "description": "Refund one order. Undo with billing.cancel_refund.",
                "compensator": {"tool": "billing.cancel_refund", "args": {"refund_id": "$output.refund_id"}},
                "cost": "$input.amount",
                "limits": {"max_per_call": 10000, "max_per_plan": 200000},
            },
            "billing.cancel_refund": {"state": "idempotent_write", "idempotency_keys": True,
                                      "description": "Cancel a refund."},
            "email.send_email": {"state": "irreversible", "allowed_input_labels": ["pii", "financial"],
                                 "idempotency_keys": True,
                                 "description": "Send a templated email to a customer. Cannot be recalled."},
        },
    }, catalog)


def refund_plan(max_items: int = 20) -> dict:
    """Refund overdue orders under ₹5,000 that have no open dispute, then email the customer once."""
    return {
        "planbound": "0.1",
        "goal": "Refund C-42's overdue orders under 5000 without disputes, then email a receipt",
        "nodes": [
            {"id": "cust", "op": "call", "tool": "crm.get_customer", "args": {"customer_id": "C-42"}},
            {"id": "orders", "op": "call", "tool": "billing.list_orders",
             "args": {"customer_id": {"$ref": "cust.customer_id"}, "status": "overdue"}},
            {"id": "disputes", "op": "call", "tool": "billing.list_disputes",
             "args": {"customer_id": {"$ref": "cust.customer_id"}}},
            {"id": "eligible", "op": "filter", "input": {"$ref": "orders.orders"},
             "where": {"and": [
                 {"lt": [{"$item": "amount"}, 5000]},
                 {"not": {"in": [{"$item": "order_id"}, {"$ref": "disputes.order_ids"}]}},
             ]}},
            {"id": "refunds", "op": "map", "input": {"$ref": "eligible"}, "max_items": max_items,
             "concurrency": 8,
             "call": {"tool": "billing.refund_order",
                      "args": {"order_id": {"$item": "order_id"}, "amount": {"$item": "amount"}}}},
            {"id": "notify", "op": "call", "tool": "email.send_email", "depends_on": ["refunds"],
             "when": {"gt": [{"$agg": {"fn": "count", "of": "eligible"}}, 0]},
             "args": {"to": {"$ref": "cust.email"}, "template": "refund_processed",
                      "vars": {"count": {"$agg": {"fn": "count", "of": "eligible"}},
                               "total": {"$agg": {"fn": "sum", "of": "eligible", "field": "amount"}}}}},
        ],
        "return": ["eligible", "refunds", "notify"],
    }


# ---------------------------------------------------------------- HR self-service (identity binding)


def hr_manifest(catalog: dict, approvers: dict | None = None) -> Manifest:
    own = {"arg": "employee_id", "caller": "employee_id", "unless_role": "hr_admin"}
    return pin({
        "mode": "enforce",
        "defaults": {"unlisted_tool_policy": "reject"},
        "approval": {"auto_approve_max_cost": 0, "approvers": approvers or {}},
        "tools": {
            "hris.get_employee": {"state": "read_only", "output_labels": ["pii"], "identity": own,
                                  "description": "Your employee record."},
            "hris.update_address": {
                "state": "compensatable", "identity": own, "output_labels": ["pii"], "idempotency_keys": True,
                "description": "Change your home address.",
                "compensator": {"tool": "hris.update_address",
                                "args": {"employee_id": "$input.employee_id", "address": "$output.previous_address"}},
            },
            "payroll.update_tax_state": {
                "state": "compensatable", "identity": own, "idempotency_keys": True,
                "description": "Change your payroll tax state.",
                "compensator": {"tool": "payroll.update_tax_state",
                                "args": {"employee_id": "$input.employee_id", "state": "$output.previous_state"}},
            },
            "email.send_email": {"state": "irreversible", "allowed_input_labels": ["pii"], "idempotency_keys": True,
                                 "description": "Send a templated email. Cannot be recalled."},
            "iam.list_accounts": {"state": "read_only", "description": "An employee's system accounts."},
            "iam.revoke_access": {"state": "idempotent_write", "description": "Disable one account. Safe to repeat."},
        },
    }, catalog)


def address_plan(employee_id_value) -> dict:
    return {
        "planbound": "0.1",
        "goal": "Move my address to Bengaluru and switch payroll state",
        "nodes": [
            {"id": "me", "op": "call", "tool": "hris.get_employee", "args": {"employee_id": employee_id_value}},
            {"id": "addr", "op": "call", "tool": "hris.update_address",
             "args": {"employee_id": employee_id_value, "address": "48 Indiranagar 2nd Stage, Bengaluru"}},
            {"id": "tax", "op": "call", "tool": "payroll.update_tax_state",
             "args": {"employee_id": employee_id_value, "state": "KA"}},
            {"id": "confirm", "op": "call", "tool": "email.send_email",
             "args": {"to": {"$ref": "me.email"}, "template": "address_changed", "vars": {"state": "KA"}}},
        ],
    }


def offboarding_plan(employee_id: str = "E-2002") -> dict:
    return {
        "planbound": "0.1",
        "goal": f"Offboard {employee_id}: revoke every account, then tell the manager",
        "nodes": [
            {"id": "emp", "op": "call", "tool": "hris.get_employee", "args": {"employee_id": employee_id}},
            {"id": "accounts", "op": "call", "tool": "iam.list_accounts", "args": {"employee_id": employee_id}},
            {"id": "active", "op": "filter", "input": {"$ref": "accounts.accounts"},
             "where": {"eq": [{"$item": "active"}, True]}},
            {"id": "notify", "op": "call", "tool": "email.send_email",
             "args": {"to": {"$ref": "emp.manager_email"}, "template": "offboarding_started",
                      "vars": {"accounts": {"$agg": {"fn": "count", "of": "active"}}}}},
            {"id": "revoke", "op": "map", "input": {"$ref": "active"}, "max_items": 20,
             "call": {"tool": "iam.revoke_access",
                      "args": {"employee_id": employee_id, "system": {"$item": "system"}}}},
        ],
    }
