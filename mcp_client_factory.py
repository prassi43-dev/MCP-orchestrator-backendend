"""Factory for creating and managing tenant+server-aware MCP clients."""
from __future__ import annotations

import asyncio
import logging
from typing import Dict, Optional

from mcp_client_hybrid import HybridMCPClient
from repository_supabase import MCPServerRepository
from telemetry import EventHub

logger = logging.getLogger(__name__)

 
class MCPClientFactory:
    """Factory that creates and caches MCP clients per tenant+server."""

    def __init__(
        self,
        timeout_seconds: float = 10.0,
        event_hub: EventHub | None = None,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._event_hub = event_hub
        self._clients: Dict[str, HybridMCPClient] = {}  # Key: "{tenant_id}:{server_id}"
        self._lock = asyncio.Lock()
        self._server_repo: Optional[MCPServerRepository] = None

    def set_repository(self, repo: MCPServerRepository) -> None:
        """Set the server repository (injected dependency)."""
        self._server_repo = repo

    def _make_key(self, tenant_id: str, server_id: str) -> str:
        """Generate cache key for tenant+server."""
        return f"{tenant_id}:{server_id}"

    async def get_client(
        self,
        tenant_id: str,
        server_id: str,
        session_id: str,
        correlation_id: str,
    ) -> HybridMCPClient:
        """Get or create an MCP client for a tenant+server."""
        key = self._make_key(tenant_id, server_id)

        async with self._lock:
            if key in self._clients:
                return self._clients[key]

            # Load server config from repository
            if not self._server_repo:
                raise RuntimeError("MCPClientFactory: repository not set")

            server_config = self._server_repo.get_server(tenant_id, server_id)
            if not server_config:
                raise ValueError(f"MCP server {tenant_id}/{server_id} not found")

            # Build headers from auth config
            headers = dict(server_config.get("extra_headers_json") or {})
            auth = server_config.get("auth_json") or {}
            if auth.get("type") == "bearer" and auth.get("token"):
                headers.setdefault("Authorization", f"Bearer {auth['token']}")

            # Create hybrid client
            http_base = server_config.get("fallback_http_base") or server_config.get("base_url")
            client = HybridMCPClient(
                server_name=server_id,
                base_url=server_config.get("base_url"),
                timeout_seconds=self._timeout_seconds,
                headers=headers,
                http_base_url=http_base,
                handshake_path=server_config.get("handshake_path"),
                event_hub=self._event_hub,
                tenant_id=tenant_id,
                server_id=server_id,
            )

            self._clients[key] = client
            logger.info(
                "Created MCP client for tenant=%s server=%s (base_url=%s)",
                tenant_id,
                server_id,
                server_config.get("base_url"),
            )
            return client

    async def close_all(self) -> None:
        """Close all cached clients."""
        async with self._lock:
            for key, client in list(self._clients.items()):
                try:
                    await client.close()
                    logger.debug("Closed MCP client %s", key)
                except Exception as exc:
                    logger.warning("Error closing MCP client %s: %s", key, exc)
            self._clients.clear()

    async def close_client(self, tenant_id: str, server_id: str) -> None:
        """Close a specific client."""
        key = self._make_key(tenant_id, server_id)
        async with self._lock:
            client = self._clients.pop(key, None)
            if client:
                try:
                    await client.close()
                    logger.debug("Closed MCP client %s", key)
                except Exception as exc:
                    logger.warning("Error closing MCP client %s: %s", key, exc)

