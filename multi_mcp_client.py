"""Coordinator for multiple MCP server clients."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping

from config import MCPServerConfig
from mcp_client_hybrid import HybridMCPClient
from models import ToolDefinition
from telemetry import EventHub, Event

logger = logging.getLogger(__name__)


class MultiMCPClient:
    """Maintains per-server MCP clients and exposes a unified interface."""

    def __init__(
        self,
        servers: List[MCPServerConfig],
        timeout_seconds: float,
        event_hub: EventHub | None = None,
    ) -> None:
        if not servers:
            raise ValueError("At least one MCP server configuration is required")

        self._configs: Mapping[str, MCPServerConfig] = {cfg.name: cfg for cfg in servers}
        self._clients: Dict[str, HybridMCPClient] = {}
        self._events = event_hub

        for cfg in servers:
            headers = cfg.compile_headers()
            http_base = str(cfg.fallback_http_base or cfg.base_url)
            handshake_path = cfg.handshake_path
            self._clients[cfg.name] = HybridMCPClient(
                server_name=cfg.name,
                base_url=str(cfg.base_url),
                timeout_seconds=timeout_seconds,
                headers=headers,
                http_base_url=http_base,
                handshake_path=handshake_path,
                event_hub=event_hub,
            )
            logger.info(
                "Initialized MCP client for %s (base=%s, http_base=%s)",
                cfg.name,
                cfg.base_url,
                http_base,
            )

    def server_names(self) -> List[str]:
        return list(self._clients.keys())

    def config_for(self, server: str) -> MCPServerConfig:
        if server not in self._configs:
            raise KeyError(f"Unknown MCP server '{server}'")
        return self._configs[server]

    async def list_tools(
        self,
        server: str,
        *,
        session_id: str,
        correlation_id: str,
        force_refresh: bool = False,
    ) -> List[ToolDefinition]:
        client = self._clients.get(server)
        if not client:
            raise KeyError(f"Unknown MCP server '{server}'")
        return await client.list_tools(
            force_refresh=force_refresh,
            session_id=session_id,
            correlation_id=correlation_id,
        )

    async def call_tool(
        self,
        server: str,
        *,
        tool_name: str,
        arguments: Dict[str, Any],
        session_id: str,
        correlation_id: str,
    ) -> Dict[str, Any]:
        client = self._clients.get(server)
        if not client:
            raise KeyError(f"Unknown MCP server '{server}'")
        return await client.call_tool(
            name=tool_name,
            arguments=arguments,
            session_id=session_id,
            correlation_id=correlation_id,
        )

    async def emit(
        self,
        event: str,
        server: str,
        payload: Dict[str, Any],
        session_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Dict[str, Any]:
        if not self._events or not session_id or not correlation_id:
            return {}
        event_payload = {"server": server, **payload}
        await self._events.publish(
            Event(
                event=event,
                session_id=session_id,
                correlation_id=correlation_id,
                source="mcp_client",
                payload=event_payload,
            )
        )
        return event_payload

    async def close(self) -> None:
        for client in self._clients.values():
            try:
                await client.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Error closing MCP client: %s", exc)

