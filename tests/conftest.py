import pytest

from bench.servers import build_servers, make_world
from planbound.runtime import Runtime
from planbound.store import Store
from planbound.upstream import MCPUpstreams


class Env:
    def __init__(self, world, upstreams, tmp_path):
        self.world = world
        self.up = upstreams
        self.tmp = tmp_path
        self.store = Store(tmp_path / "gateway.db")

    def runtime(self, manifest, caller=None, **kw) -> Runtime:
        kw.setdefault("display_dir", self.tmp / "display")
        kw.setdefault("retry_delay_s", 0.0)
        upstreams = kw.pop("upstreams", self.up)
        return Runtime(self.store, upstreams, manifest, caller or {"user": "ops"}, **kw)


@pytest.fixture
async def env(tmp_path):
    world = make_world(latency_s=0)
    async with MCPUpstreams(build_servers(world)) as up:
        yield Env(world, up, tmp_path)


@pytest.fixture
async def injected_env(tmp_path):
    world = make_world(latency_s=0, inject_note=True)
    async with MCPUpstreams(build_servers(world)) as up:
        yield Env(world, up, tmp_path)


@pytest.fixture
def anyio_backend():
    return "asyncio"


def pytest_collection_modifyitems(items):
    for item in items:
        if item.get_closest_marker("anyio") is None:
            item.add_marker(pytest.mark.anyio)
