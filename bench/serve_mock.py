"""Run one mock server over stdio: `python -m bench.serve_mock billing`."""

import sys

from bench.servers import build_servers, make_world

if __name__ == "__main__":
    build_servers(make_world(latency_s=0.02, inject_note=True))[sys.argv[1]].run("stdio")
