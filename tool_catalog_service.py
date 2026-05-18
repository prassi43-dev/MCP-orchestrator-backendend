"""Tenant+server-aware tool catalog service."""
from __future__ import annotations

import logging
from typing import List

from mcp_client_factory import MCPClientFactory
from models import ToolDefinition
from repository_supabase import MCPServerRepository, ToolRepository
from telemetry import EventHub
from tool_catalog import build_tool_alias

logger = logging.getLogger(__name__)


class ToolCatalogService:
    """Service for managing tool catalogs per tenant+server."""

    def __init__(
        self,
        client_factory: MCPClientFactory,
        server_repo: MCPServerRepository,
        tool_repo: ToolRepository,
        event_hub: EventHub | None = None,
    ) -> None:
        self._client_factory = client_factory
        self._server_repo = server_repo
        self._tool_repo = tool_repo
        self._event_hub = event_hub

    async def get_tools(
        self,
        tenant_id: str,
        server_id: str,
        session_id: str,
        correlation_id: str,
        force_refresh: bool = False,
    ) -> List[ToolDefinition]:
        """Get tools for a tenant+server, refreshing from MCP if needed."""
        # Check persistence first
        if not force_refresh:
            cached_tools = self._tool_repo.load_tools(tenant_id, server_id)
            if cached_tools:
                logger.info(
                    "Loaded %d cached tool(s) for tenant=%s server=%s",
                    len(cached_tools),
                    tenant_id,
                    server_id,
                )
                return cached_tools

        # Refresh from MCP
        logger.info(
            "Refreshing tools from MCP for tenant=%s server=%s",
            tenant_id,
            server_id,
        )

        # Get MCP client
        client = await self._client_factory.get_client(
            tenant_id, server_id, session_id, correlation_id
        )

        # Fetch tools from MCP
        raw_tools = await client.list_tools(
            force_refresh=force_refresh,
            session_id=session_id,
            correlation_id=correlation_id,
        )

        # Build aliased tools
        aliased_tools = []
        for tool in raw_tools:
            alias = build_tool_alias(server_id, tool.server_tool_name)
            aliased_tool = ToolDefinition(
                name=alias,
                description=tool.description,
                parameters=tool.parameters,
                server=server_id,
                server_tool_name=tool.server_tool_name,
            )
            aliased_tools.append(aliased_tool)

        # Save to persistence
        self._tool_repo.save_tools(tenant_id, server_id, aliased_tools)

        logger.info(
            "Refreshed and saved %d tool(s) for tenant=%s server=%s",
            len(aliased_tools),
            tenant_id,
            server_id,
        )
        return aliased_tools

    def get_tool_count(self, tenant_id: str, server_id: str) -> int:
        """Get the number of tools for a tenant+server."""
        return self._tool_repo.get_tool_count(tenant_id, server_id)

