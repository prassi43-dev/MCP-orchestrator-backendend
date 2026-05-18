"""Hybrid MCP client: tries SDK first, falls back to direct HTTP on failure."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from mcp_client import MCPClient as SDKMCPClient
from mcp_client_http import MCPHTTPClient, ToolRegistry
from telemetry import Event, EventHub

from models import ToolDefinition

logger = logging.getLogger(__name__)


class HybridMCPClient:
    """
    Hybrid MCP client that uses SDK first, falls back to HTTP on failure.
    
    This approach:
    1. Tries the official MCP SDK first (works for most operations)
    2. Falls back to direct HTTP client if SDK fails (e.g., routing bug)
    3. Provides best of both worlds - SDK when it works, HTTP when it doesn't
    """

    def __init__(
        self,
        server_name: str,
        base_url: str,
        timeout_seconds: float = 10.0,
        headers: Optional[Dict[str, str]] = None,
        http_base_url: Optional[str] = None,
        handshake_path: Optional[str] = None,
        event_hub: Optional[EventHub] = None,
        tenant_id: Optional[str] = None,
        server_id: Optional[str] = None,
    ) -> None:
        self.server_name = server_name
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.headers = headers or {}
        self._events = event_hub
        self._tenant_id = tenant_id
        self._server_id = server_id
        
        # Initialize both clients with tenant/server context
        self._sdk_client = SDKMCPClient(
            server_name=server_name,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            headers=self.headers,
            handshake_path=handshake_path,
            event_hub=event_hub,
            tenant_id=tenant_id,
            server_id=server_id,
        )
        self._http_client = MCPHTTPClient(
            server_name=server_name,
            base_url=http_base_url or base_url,
            timeout_seconds=timeout_seconds,
            headers=self.headers,
            event_hub=event_hub,
            tenant_id=tenant_id,
            server_id=server_id,
        )
        
        # Track which client to use (starts with SDK)
        self._prefer_sdk = True
        self._tools_cache: Optional[List[ToolDefinition]] = None

    async def list_tools(
        self,
        *,
        session_id: str,
        correlation_id: str,
        force_refresh: bool = False,
    ) -> List[ToolDefinition]:
        """
        Fetch tools, trying SDK first, falling back to HTTP if SDK fails.
        """
        if not force_refresh and self._tools_cache is not None:
            return self._tools_cache
        
        if force_refresh:
            self._tools_cache = None
        
        # Try SDK first
        if self._prefer_sdk:
            try:
                tools = await asyncio.wait_for(
                    self._sdk_client.list_tools(
                        force_refresh=force_refresh,
                        session_id=session_id,
                        correlation_id=correlation_id,
                    ),
                    timeout=self.timeout_seconds * 3,
                )
                self._tools_cache = tools
                return tools
            except asyncio.TimeoutError:
                logger.warning("MCP SDK list_tools() timed out, falling back to HTTP client")
                self._prefer_sdk = False  # SDK has routing issues, prefer HTTP
            except Exception as exc:
                logger.warning("MCP SDK list_tools() failed: %s, falling back to HTTP client", exc)
                # Don't disable SDK completely - might be a transient error
                # Only disable if it's a routing/timeout issue
        
        # Fallback to HTTP client
        try:
            tools = await self._http_client.list_tools(
                force_refresh=force_refresh,
                session_id=session_id,
                correlation_id=correlation_id,
            )
            self._tools_cache = tools
            return tools
        except Exception as exc:
            logger.exception("Both SDK and HTTP client failed to fetch tools: %s", exc)
            raise

    async def call_tool(
        self,
        *,
        name: str,
        arguments: Dict[str, Any],
        session_id: str,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """
        Call a tool, trying SDK first, falling back to HTTP if SDK fails.
        
        The SDK has a known routing bug where SSE responses aren't delivered to waiting calls.
        If SDK times out, we automatically fall back to HTTP client.
        """
        # Try SDK first (if we haven't disabled it)
        if self._prefer_sdk:
            try:
                logger.debug("Attempting tool call via MCP SDK: %s", name)
                result = await asyncio.wait_for(
                    self._sdk_client.call_tool(
                        name=name,
                        arguments=arguments,
                        session_id=session_id,
                        correlation_id=correlation_id,
                    ),
                    timeout=self.timeout_seconds * 6,  # Extended timeout for SDK
                )
                
                # Check if result indicates an error (timeout from SDK)
                if isinstance(result, dict) and result.get("error"):
                    error_code = result.get("error", {}).get("code", "")
                    if error_code == "timeout":
                        logger.warning("MCP SDK call_tool() timed out for %s, falling back to HTTP", name)
                        self._prefer_sdk = False  # SDK routing bug confirmed, prefer HTTP
                    else:
                        # Other error from SDK - return it
                        logger.debug("MCP SDK returned error (non-timeout): %s", result)
                        return result
                else:
                    # Success from SDK
                    logger.debug("Tool call succeeded via MCP SDK: %s", name)
                    return result
                    
            except asyncio.TimeoutError:
                logger.warning("MCP SDK call_tool() timed out for %s, falling back to HTTP", name)
                self._prefer_sdk = False  # SDK routing bug - prefer HTTP going forward
            except Exception as exc:
                logger.warning("MCP SDK call_tool() failed for %s: %s, falling back to HTTP", name, exc)
                # Don't disable SDK completely - might be transient
        
        # Fallback to HTTP client
        try:
            logger.info("Calling tool via direct HTTP client: %s", name)
            result = await self._http_client.call_tool(
                name=name,
                arguments=arguments,
                session_id=session_id,
                correlation_id=correlation_id,
            )
            logger.debug("Tool call completed via HTTP client: %s", name)
            return result
        except Exception as exc:
            logger.exception("HTTP client also failed for tool %s: %s", name, exc)
            raise

    async def close(self) -> None:
        """Close both clients."""
        try:
            await self._sdk_client.close()
        except Exception as exc:
            logger.warning("Error closing SDK client: %s", exc)
        
        try:
            await self._http_client.close()
        except Exception as exc:
            logger.warning("Error closing HTTP client: %s", exc)


# ToolRegistry is already imported above

