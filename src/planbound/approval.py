"""Approval: Ed25519 signatures over exactly what was previewed.

Tier 1 (auto): within the manifest's per-plan limits, the gateway needs no signature.
Tier 2 (policy): a separate signer process checks a rolling budget, places a hold
                 (reservation) and signs with the policy key.
Tier 3 (human):  plans the policy escalates are signed by a person with the human key.

The gateway only holds public keys, so it cannot forge an approval. Private keys live
with the signer process.
"""

from __future__ import annotations

import base64
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from .manifest import canonical
from .store import Store

SIGNERS = ("policy", "human")


# ---------------------------------------------------------------- keys


def generate_keys(directory: str | Path) -> dict[str, str]:
    """Create policy and human key pairs. Returns their public keys (base64)."""
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)
    out = {}
    for name in SIGNERS:
        key = Ed25519PrivateKey.generate()
        path = d / f"{name}.key"
        path.write_bytes(
            key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                              serialization.NoEncryption())
        )
        path.chmod(0o600)
        out[name] = public_b64(key)
    return out


def load_private(directory: str | Path, name: str) -> Ed25519PrivateKey:
    data = (Path(directory) / f"{name}.key").read_bytes()
    key = serialization.load_pem_private_key(data, password=None)
    assert isinstance(key, Ed25519PrivateKey)
    return key


def public_b64(key: Ed25519PrivateKey) -> str:
    raw = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return base64.b64encode(raw).decode()


def approval_message(plan_id: str, plan_hash: str, preview_hash: str, expires_at: float, signer: str) -> bytes:
    return canonical({
        "planbound_approval": "0.1",
        "plan_id": plan_id,
        "plan_hash": plan_hash,
        "preview_hash": preview_hash,
        "expires_at": round(expires_at, 3),
        "signer": signer,
    }).encode()


def sign(key: Ed25519PrivateKey, message: bytes) -> str:
    return base64.b64encode(key.sign(message)).decode()


def verify(public_key: str, message: bytes, signature: str) -> bool:
    try:
        Ed25519PublicKey.from_public_bytes(base64.b64decode(public_key)).verify(base64.b64decode(signature), message)
        return True
    except (InvalidSignature, ValueError):
        return False


def verify_approval(approval: dict, approvers: dict[str, str], *, now: float | None = None) -> str | None:
    """Return None if the approval is valid, else the reason it is not."""
    if approval.get("status") != "signed" or not approval.get("signature"):
        return f"approval is {approval.get('status')}"
    if (now or time.time()) > approval["expires_at"]:
        return "approval expired"
    signer = approval.get("signer")
    if signer not in approvers:
        return f"no public key configured for signer {signer!r}"
    msg = approval_message(approval["plan_id"], approval["plan_hash"], approval["preview_hash"],
                           approval["expires_at"], signer)
    if not verify(approvers[signer], msg, approval["signature"]):
        return "signature does not verify"
    return None


# ---------------------------------------------------------------- tier 2 policy signer


@dataclass
class BudgetPolicy:
    budget: float
    window_s: float = 24 * 3600
    max_plan_cost: float | None = None


class PolicySigner:
    """Runs in its own process with its own ledger. Reads approval requests from the gateway
    store, keeps a rolling budget with authorization holds, and signs or escalates."""

    def __init__(self, gateway: Store, ledger_path: str | Path, keys_dir: str | Path, policy: BudgetPolicy):
        self.gateway = gateway
        self.keys_dir = keys_dir
        self.policy = policy
        self.ledger = sqlite3.connect(str(ledger_path), isolation_level=None, check_same_thread=False)
        self.ledger.execute("PRAGMA journal_mode=WAL")
        self.ledger.execute(
            "CREATE TABLE IF NOT EXISTS holds (plan_id TEXT PRIMARY KEY, amount REAL NOT NULL, "
            "status TEXT NOT NULL, created_at REAL NOT NULL)"
        )

    def used(self, now: float | None = None) -> float:
        since = (now or time.time()) - self.policy.window_s
        row = self.ledger.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM holds WHERE status IN ('reserved', 'finalized') AND created_at >= ?",
            (since,),
        ).fetchone()
        return float(row[0])

    def sync_outcomes(self) -> None:
        """Finalize holds for plans that completed; release holds for plans that did not run.

        A partial plan's hold is finalized (the money may be gone), and released again if a later
        `planbound recover` rolls it back completely.
        """
        rows = self.ledger.execute("SELECT plan_id, status FROM holds WHERE status IN ('reserved', 'finalized')")
        for plan_id, hold in rows.fetchall():
            plan = self.gateway.get_plan(plan_id)
            approval = self.gateway.get_approval(plan_id)
            status = plan["status"] if plan else "missing"
            if hold == "finalized":
                if status in ("failed_compensated", "recovered_compensated", "cancelled"):
                    self._set_hold(plan_id, "released")
                continue
            if status in ("completed", "failed_partial", "needs_attention", "abandoned"):
                self._set_hold(plan_id, "finalized")
            elif status in ("failed_compensated", "recovered_compensated", "cancelled", "stale", "rejected",
                            "missing") or status in ("awaiting_approval", "previewed") and approval and approval["expires_at"] < time.time():
                self._set_hold(plan_id, "released")

    def _set_hold(self, plan_id: str, status: str) -> None:
        self.ledger.execute("UPDATE holds SET status = ? WHERE plan_id = ?", (status, plan_id))

    def process(self) -> list[dict]:
        """Handle every pending request once. Returns what was decided."""
        self.sync_outcomes()
        decisions = []
        for req in self.gateway.approvals_with_status("pending"):
            cost = float(req["preview"].get("cost") or 0)
            used = self.used()
            reasons = []
            if used + cost > self.policy.budget:
                reasons.append(f"rolling budget: {used:g} used + {cost:g} > {self.policy.budget:g}")
            if self.policy.max_plan_cost is not None and cost > self.policy.max_plan_cost:
                reasons.append(f"plan cost {cost:g} > policy max {self.policy.max_plan_cost:g}")
            if req["preview"].get("requires_human"):
                reasons.append("a tool in this plan always requires a human")
            if reasons:
                self.gateway.set_approval(req["plan_id"], status="escalated", tier="human")
                decisions.append({"plan_id": req["plan_id"], "decision": "escalated", "reasons": reasons})
                continue
            self.reserve(req["plan_id"], cost)
            self._sign(req, "policy")
            decisions.append({"plan_id": req["plan_id"], "decision": "signed", "cost": cost, "used_after": used + cost})
        return decisions

    def reserve(self, plan_id: str, amount: float) -> None:
        self.ledger.execute("INSERT OR REPLACE INTO holds VALUES (?,?,?,?)", (plan_id, amount, "reserved", time.time()))

    def _sign(self, req: dict, signer: str) -> None:
        key = load_private(self.keys_dir, signer)
        msg = approval_message(req["plan_id"], req["plan_hash"], req["preview_hash"], req["expires_at"], signer)
        self.gateway.set_approval(req["plan_id"], status="signed", tier=signer, signer=signer, signature=sign(key, msg))

    def human_decide(self, plan_id: str, approve: bool) -> dict:
        """Tier 3: called by a person (CLI), signs with the human key and still takes a budget hold."""
        req = self.gateway.get_approval(plan_id)
        if req is None or req["status"] not in ("pending", "escalated"):
            return {"plan_id": plan_id, "error": f"no open approval request ({req and req['status']})"}
        if not approve:
            self.gateway.set_approval(plan_id, status="denied", tier="human")
            return {"plan_id": plan_id, "decision": "denied"}
        self.reserve(plan_id, float(req["preview"].get("cost") or 0))
        self._sign(req, "human")
        return {"plan_id": plan_id, "decision": "signed", "signer": "human"}


def describe_request(req: dict) -> str:
    p = req["preview"]
    lines = [f"Plan {req['plan_id']}  ({req['status']})", f"  goal: {p.get('goal', '')}"]
    for tool, n in p.get("would_call", {}).items():
        lines.append(f"  {n} × {tool}")
    lines.append(f"  cost: {p.get('cost', 0):g}")
    if p.get("pending"):
        lines.append(f"  pending (depends on earlier writes): {', '.join(p['pending'])}")
    lines.append(f"  effects: {json.dumps(p.get('effects', {}))}")
    return "\n".join(lines)
