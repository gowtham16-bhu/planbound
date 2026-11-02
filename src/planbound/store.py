"""SQLite storage: plans, result handles, the saga journal and approval requests.

The journal is written (and committed) before every upstream write, so a crashed
gateway can compensate or resume on restart.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS plans (
    plan_id      TEXT PRIMARY KEY,
    plan_hash    TEXT NOT NULL,
    raw_json     TEXT NOT NULL,
    caller_json  TEXT NOT NULL,
    status       TEXT NOT NULL,
    preview_hash TEXT,
    preview_json TEXT,
    result_json  TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL,
    lease_owner  TEXT,
    lease_until  REAL,
    lease_epoch  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS handles (
    plan_id     TEXT NOT NULL,
    node_id     TEXT NOT NULL,
    source_tool TEXT,
    labels_json TEXT NOT NULL,
    trust       TEXT NOT NULL,
    data_json   TEXT NOT NULL,
    created_at  REAL,
    PRIMARY KEY (plan_id, node_id)
);
CREATE TABLE IF NOT EXISTS journal (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id         TEXT NOT NULL,
    node_id         TEXT NOT NULL,
    item_index      INTEGER NOT NULL,
    tool            TEXT NOT NULL,
    state           TEXT NOT NULL,
    args_json       TEXT NOT NULL,
    idem_key        TEXT NOT NULL,
    status          TEXT NOT NULL,
    result_json     TEXT,
    compensator_json TEXT,
    error           TEXT,
    updated_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS journal_plan ON journal(plan_id);
CREATE INDEX IF NOT EXISTS journal_key ON journal(idem_key);
CREATE TABLE IF NOT EXISTS approvals (
    plan_id      TEXT PRIMARY KEY,
    plan_hash    TEXT NOT NULL,
    preview_hash TEXT NOT NULL,
    preview_json TEXT NOT NULL,
    status       TEXT NOT NULL,
    tier         TEXT,
    signer       TEXT,
    signature    TEXT,
    expires_at   REAL NOT NULL,
    created_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS usage (
    tool TEXT PRIMARY KEY,
    calls INTEGER NOT NULL
);
"""


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(SCHEMA)
        cols = {r[1] for r in self.db.execute("PRAGMA table_info(plans)")}
        for col, kind in (("lease_owner", "TEXT"), ("lease_until", "REAL"),
                          ("lease_epoch", "INTEGER NOT NULL DEFAULT 0")):
            if col not in cols:  # databases created before leases existed
                self.db.execute(f"ALTER TABLE plans ADD COLUMN {col} {kind}")
        if "created_at" not in {r[1] for r in self.db.execute("PRAGMA table_info(handles)")}:
            self.db.execute("ALTER TABLE handles ADD COLUMN created_at REAL")

    def close(self) -> None:
        self.db.close()

    # ---------------------------------------------------------------- plans

    def create_plan(self, plan_id: str, plan_hash: str, raw: dict, caller: dict) -> None:
        now = time.time()
        self.db.execute(
            "INSERT INTO plans VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (plan_id, plan_hash, json.dumps(raw), json.dumps(caller), "compiled", None, None, None, now, now,
             None, None, 0),
        )

    def update_plan(self, plan_id: str, **fields: Any) -> None:
        cols = []
        vals: list[Any] = []
        for key, value in fields.items():
            if key in ("preview", "result"):
                key, value = f"{key}_json", json.dumps(value)
            cols.append(f"{key} = ?")
            vals.append(value)
        if "status" in fields and fields["status"] != "running" and "lease_owner" not in fields:
            cols.append("lease_owner = NULL")  # the plan stopped: whoever ran it lets go
        cols.append("updated_at = ?")
        vals.extend([time.time(), plan_id])
        self.db.execute(f"UPDATE plans SET {', '.join(cols)} WHERE plan_id = ?", vals)

    def get_plan(self, plan_id: str) -> dict | None:
        row = self.db.execute("SELECT * FROM plans WHERE plan_id = ?", (plan_id,)).fetchone()
        if row is None:
            return None
        out = dict(row)
        out["raw"] = json.loads(out.pop("raw_json"))
        out["caller"] = json.loads(out.pop("caller_json"))
        for key in ("preview", "result"):
            val = out.pop(f"{key}_json")
            out[key] = json.loads(val) if val else None
        return out

    # ---------------------------------------------------------------- leases
    #
    # Only one gateway (or `planbound recover`) may drive a plan at a time. The holder renews its lease
    # before every write; a lease that is not renewed expires, and then recovery may take the plan over.

    def claim(self, plan_id: str, owner: str, ttl_s: float, statuses: tuple[str, ...], *,
              new_status: str | None = None) -> bool:
        """Atomically take the plan if it is in one of `statuses` and nobody else holds a live lease.

        A new holder bumps the lease epoch: the fencing token every later write carries, so upstreams
        (and this journal) can refuse a former holder that wakes up late."""
        now = time.time()
        q = ",".join("?" for _ in statuses)
        sets = ("lease_epoch = lease_epoch + (CASE WHEN lease_owner = ? THEN 0 ELSE 1 END), "
                "lease_owner = ?, lease_until = ?, updated_at = ?" + (", status = ?" if new_status else ""))
        vals: list[Any] = [owner, owner, now + ttl_s, now] + ([new_status] if new_status else [])
        cur = self.db.execute(
            f"UPDATE plans SET {sets} WHERE plan_id = ? AND status IN ({q})"
            " AND (lease_owner IS NULL OR lease_until < ? OR lease_owner = ?)",
            [*vals, plan_id, *statuses, now, owner],
        )
        return cur.rowcount == 1

    def renew(self, plan_id: str, owner: str, ttl_s: float) -> int | None:
        """Extend our lease and return its epoch. None means someone else took the plan over: stop."""
        cur = self.db.execute("UPDATE plans SET lease_until = ? WHERE plan_id = ? AND lease_owner = ?",
                              (time.time() + ttl_s, plan_id, owner))
        if cur.rowcount != 1:
            return None
        return int(self.db.execute("SELECT lease_epoch FROM plans WHERE plan_id = ?", (plan_id,)).fetchone()[0])

    def plans_with_status(self, *statuses: str) -> list[str]:
        q = ",".join("?" for _ in statuses)
        return [r[0] for r in self.db.execute(f"SELECT plan_id FROM plans WHERE status IN ({q})", statuses)]

    # ---------------------------------------------------------------- handles

    def put_handle(self, plan_id: str, node_id: str, data: Any, *, tool: str | None, labels: set[str], trust: str) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO handles VALUES (?,?,?,?,?,?,?)",
            (plan_id, node_id, tool, json.dumps(sorted(labels)), trust, json.dumps(data), time.time()),
        )

    def get_handle(self, plan_id: str, node_id: str) -> dict | None:
        row = self.db.execute(
            "SELECT * FROM handles WHERE plan_id = ? AND node_id = ?", (plan_id, node_id)
        ).fetchone()
        if row is None:
            return None
        return {
            "source_tool": row["source_tool"],
            "labels": json.loads(row["labels_json"]),
            "trust": row["trust"],
            "data": json.loads(row["data_json"]),
            "created_at": row["created_at"] or 0.0,
        }

    def handles_for(self, plan_id: str) -> dict[str, Any]:
        rows = self.db.execute("SELECT node_id, data_json FROM handles WHERE plan_id = ?", (plan_id,))
        return {r["node_id"]: json.loads(r["data_json"]) for r in rows}

    # ---------------------------------------------------------------- journal

    # With `owner`, a journal write only lands while that owner holds the plan's lease: a process that
    # lost the plan can never overwrite what the new holder recorded.
    _HOLDS = "(? IS NULL OR EXISTS (SELECT 1 FROM plans p WHERE p.plan_id = {plan} AND p.lease_owner = ?))"

    def journal_begin(self, plan_id: str, node_id: str, item: int, tool: str, state: str, args: dict,
                      idem_key: str, compensator: dict | None, *, owner: str | None = None) -> int | None:
        cur = self.db.execute(
            "INSERT INTO journal (plan_id, node_id, item_index, tool, state, args_json, idem_key, status,"
            " compensator_json, updated_at) SELECT ?,?,?,?,?,?,?,?,?,? WHERE " + self._HOLDS.format(plan="?"),
            (plan_id, node_id, item, tool, state, json.dumps(args), idem_key, "IN_FLIGHT",
             json.dumps(compensator) if compensator else None, time.time(), owner, plan_id, owner),
        )
        return int(cur.lastrowid) if cur.rowcount == 1 else None

    def journal_set(self, seq: int, status: str, *, result: Any = None, error: str | None = None,
                    owner: str | None = None) -> bool:
        cur = self.db.execute(
            "UPDATE journal SET status = ?, result_json = COALESCE(?, result_json), error = ?, updated_at = ?"
            " WHERE seq = ? AND " + self._HOLDS.format(plan="journal.plan_id"),
            (status, json.dumps(result) if result is not None else None, error, time.time(), seq, owner, owner),
        )
        return cur.rowcount == 1

    def journal_find(self, idem_key: str) -> dict | None:
        """The journal entry for one write (plan:node:item), if it was ever attempted."""
        row = self.db.execute("SELECT * FROM journal WHERE idem_key = ? ORDER BY seq DESC LIMIT 1",
                              (idem_key,)).fetchone()
        return _journal_row(row) if row else None

    def journal(self, plan_id: str) -> list[dict]:
        rows = self.db.execute("SELECT * FROM journal WHERE plan_id = ? ORDER BY seq", (plan_id,))
        return [_journal_row(r) for r in rows]

    # ---------------------------------------------------------------- approvals

    def request_approval(self, plan_id: str, plan_hash: str, preview_hash: str, preview: dict, ttl_s: int) -> None:
        now = time.time()
        self.db.execute(
            "INSERT OR REPLACE INTO approvals VALUES (?,?,?,?,?,?,?,?,?,?)",
            (plan_id, plan_hash, preview_hash, json.dumps(preview), "pending", None, None, None, now + ttl_s, now),
        )

    def get_approval(self, plan_id: str) -> dict | None:
        row = self.db.execute("SELECT * FROM approvals WHERE plan_id = ?", (plan_id,)).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["preview"] = json.loads(d.pop("preview_json"))
        return d

    def approvals_with_status(self, *statuses: str) -> list[dict]:
        q = ",".join("?" for _ in statuses)
        ids = [r[0] for r in self.db.execute(f"SELECT plan_id FROM approvals WHERE status IN ({q})", statuses)]
        return [self.get_approval(i) for i in ids]

    def set_approval(self, plan_id: str, **fields: Any) -> None:
        cols = ", ".join(f"{k} = ?" for k in fields)
        self.db.execute(f"UPDATE approvals SET {cols} WHERE plan_id = ?", [*fields.values(), plan_id])

    # ---------------------------------------------------------------- usage (dev-mode manifest drafting)

    def record_usage(self, tool: str) -> None:
        self.db.execute(
            "INSERT INTO usage VALUES (?, 1) ON CONFLICT(tool) DO UPDATE SET calls = calls + 1", (tool,)
        )

    def used_tools(self) -> list[str]:
        return [r[0] for r in self.db.execute("SELECT tool FROM usage ORDER BY tool")]


def _journal_row(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["args"] = json.loads(d.pop("args_json"))
    res = d.pop("result_json")
    d["result"] = json.loads(res) if res else None
    comp = d.pop("compensator_json")
    d["compensator"] = json.loads(comp) if comp else None
    return d
