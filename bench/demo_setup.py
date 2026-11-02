"""Write a runnable demo into ./demo: gateway config, a reviewed (pinned) manifest and signing keys.

    uv run python -m bench.demo_setup
    uv run planbound --config demo/planbound.config.json serve        # point any MCP client here
    uv run planbound --config demo/planbound.config.json signer --keys demo/keys --budget 50000
"""

import asyncio
import json
import sys
from pathlib import Path

from bench.scenarios import refund_manifest
from planbound.approval import generate_keys
from planbound.upstream import MCPUpstreams

SERVERS = ("crm", "billing", "email")


async def main(out: Path) -> None:
    out.mkdir(exist_ok=True)
    root = Path.cwd().resolve()
    servers = {s: {"command": sys.executable, "args": ["-m", "bench.serve_mock", s],
                   "env": {"PYTHONPATH": str(root)}} for s in SERVERS}
    async with MCPUpstreams(servers) as up:
        pubs = generate_keys(out / "keys")
        manifest = refund_manifest(up.catalog, approvers=pubs, auto_max=2000)
    (out / "planbound.effects.json").write_text(json.dumps(manifest.to_json(), indent=2))
    (out / "planbound.config.json").write_text(json.dumps({
        "servers": servers, "manifest": "planbound.effects.json", "data_dir": ".planbound",
        "caller": {"user": "ops-demo", "roles": ["ops"]},
        "runtime": {"lease_seconds": 60, "retry_attempts": 3, "compensation_attempts": 5, "reconcile_seconds": 30},
    }, indent=2))
    print(f"Demo written to {out}/")


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1] if len(sys.argv) > 1 else "demo")))
