"""Connections to the real (unchanged) MCP servers Planbound sits in front of."""

from __future__ import annotations

import json
from contextlib import AsyncExitStack
from typing import Any

from mcp import Client, StdioServerParameters

from .manifest import UpstreamTool
from .runtime import UpstreamError


class MCPUpstreams:
    """Holds one MCP client per upstream server and namespaces tools as `server.tool`."""

    def __init__(self, servers: dict[str, Any]):
        # A value is a URL (Streamable HTTP), a {"command", "args", "env"} dict (stdio),
        # or an in-process MCP server object (tests and demos).
        self.servers = servers
        self.catalog: dict[str, UpstreamTool] = {}
        self._clients: dict[str, Client] = {}
        self._stack = AsyncExitStack()

    async def __aenter__(self) -> MCPUpstreams:
        for name, target in self.servers.items():
            if isinstance(target, dict):
                target = StdioServerParameters(command=target["command"], args=target.get("args", []),
                                               env=target.get("env"))
            client = await self._stack.enter_async_context(Client(target))
            self._clients[name] = client
            for tool in (await client.list_tools()).tools:
                full = f"{name}.{tool.name}"
                self.catalog[full] = UpstreamTool(
                    name=full,
                    description=tool.description or "",
                    input_schema=tool.input_schema or {},
                    output_schema=tool.output_schema,
                    read_only_hint=bool(tool.annotations and tool.annotations.read_only_hint),
                )
        return self

    async def __aexit__(self, *exc) -> None:
        await self._stack.aclose()

    async def call(self, tool: str, args: dict, meta: dict | None = None) -> Any:
        server, _, name = tool.partition(".")
        client = self._clients.get(server)
        if client is None:
            raise UpstreamError(f"unknown server {server!r}")
        result = await client.call_tool(name, args, meta=meta)
        text = "".join(getattr(c, "text", "") for c in result.content or [])
        if result.is_error:
            raise UpstreamError(text or "tool returned an error")
        if result.structured_content is not None:
            return result.structured_content
        try:
            return json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return text
