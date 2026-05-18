"""MCP client and tool registry built on the official MCP SDK."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from models import ToolDefinition
from telemetry import Event, EventHub

logger = logging.getLogger(__name__)


class MCPClient:
    """Thin wrapper around the MCP HTTP transport using ClientSession."""

    def __init__(
        self,
        server_name: str,
        base_url: str,
        timeout_seconds: float = 10.0,
        headers: Optional[Dict[str, str]] = None,
        handshake_path: Optional[str] = None,
        event_hub: Optional[EventHub] = None,
        tenant_id: Optional[str] = None,
        server_id: Optional[str] = None,
    ) -> None:
        self.server_name = server_name
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.headers = headers or {}
        path = (handshake_path or "").lstrip("/")
        self.handshake_url = (
            self.base_url
            if not path
            else urljoin(f"{self.base_url}/", path)
        )
        self._session: Optional[ClientSession] = None
        self._client_ctx = None
        self._lock = asyncio.Lock()
        self._tools_cache: Optional[List[ToolDefinition]] = None
        self._events = event_hub
        self._session_context: Dict[str, tuple[str, str]] = {}
        self._tenant_id = tenant_id
        self._server_id = server_id

    async def _check_server_reachable(self) -> bool:
        """Check if the MCP server is reachable via HTTP."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(self.handshake_url, headers=self.headers)
                logger.debug(
                    "Server health check: %s responded with status %d",
                    self.handshake_url,
                    response.status_code,
                )
                return response.is_success
        except httpx.ConnectError:
            logger.error("Cannot connect to MCP server at %s - server may not be running", self.base_url)
            return False
        except Exception as exc:
            logger.warning("Server health check failed: %s", exc)
            return False

    async def _ensure_session(self) -> ClientSession:
        """Create (or return) a connected MCP session."""
        if self._session:
            return self._session
        async with self._lock:
            if self._session:
                return self._session
            await self._emit(
                "mcp.connect.attempt",
                payload={
                    "server": self.server_name,
                    "url": self.handshake_url,
                    "timeout_seconds": self.timeout_seconds,
                },
            )
            if not await self._check_server_reachable():
                raise ConnectionError(f"MCP server at {self.base_url} is not reachable")

            client_ctx = None
            try:
                client_ctx = streamablehttp_client(
                    self.handshake_url,
                    headers=self.headers,
                    timeout=self.timeout_seconds,
                )
                read_stream, write_stream, _ = await asyncio.wait_for(
                    client_ctx.__aenter__(), timeout=self.timeout_seconds
                )
                session = ClientSession(read_stream, write_stream)

                init_timeout = max(self.timeout_seconds * 2, 20.0)
                try:
                    await asyncio.wait_for(session.initialize(), timeout=init_timeout)
                except asyncio.TimeoutError as exc:
                    await self._emit(
                        "mcp.connect.fail",
                        payload={
                            "server": self.server_name,
                            "reason": "initialize_timeout",
                            "timeout_seconds": init_timeout,
                        },
                    )
                    raise exc
                await self._emit(
                    "mcp.connect.success",
                    payload={"server": self.server_name},
                )

                self._client_ctx = client_ctx
                self._session = session
                logger.info("Connected to MCP server at %s", self.base_url)
                return session
            except asyncio.TimeoutError as exc:
                logger.error("Connection to MCP server at %s timed out: %s", self.base_url, exc)
                await self._emit(
                    "mcp.connect.fail",
                    payload={
                        "server": self.server_name,
                        "reason": "connect_timeout",
                        "timeout_seconds": self.timeout_seconds,
                    },
                )
                # Clean up context if it was created
                if client_ctx:
                    try:
                        await asyncio.wait_for(client_ctx.__aexit__(None, None, None), timeout=2.0)
                    except Exception as cleanup_exc:
                        logger.warning("Error during cleanup: %s", cleanup_exc)
                raise
            except Exception as exc:
                logger.exception("Failed to connect to MCP server at %s: %s", self.base_url, exc)
                await self._emit(
                    "mcp.connect.fail",
                    payload={
                        "server": self.server_name,
                        "reason": "exception",
                        "message": str(exc),
                    },
                )
                if client_ctx:
                    try:
                        await asyncio.wait_for(client_ctx.__aexit__(None, None, None), timeout=2.0)
                    except Exception as cleanup_exc:
                        logger.warning("Error during cleanup: %s", cleanup_exc)
                raise

    async def list_tools(
        self,
        *,
        session_id: str,
        correlation_id: str,
        force_refresh: bool = False,
    ) -> List[ToolDefinition]:
        """Fetch and cache tool definitions from the MCP server."""
        if not force_refresh and self._tools_cache is not None:
            return self._tools_cache
        if force_refresh:
            self._tools_cache = None

        self._set_context(session_id, correlation_id)
        session = await self._ensure_session()
        await self._emit(
            "mcp.list_tools.request",
            payload={
                "server": self.server_name,
                "method": "sdk",
            },
            session_id=session_id,
            correlation_id=correlation_id,
        )
        try:
            response = await asyncio.wait_for(
                session.list_tools(), timeout=self.timeout_seconds
            )
        except asyncio.TimeoutError:
            await self._emit(
                "mcp.list_tools.response",
                payload={
                    "server": self.server_name,
                    "error": "timeout",
                    "timeout_seconds": self.timeout_seconds,
                },
                session_id=session_id,
                correlation_id=correlation_id,
            )
            raise
        except Exception as exc:
            await self._emit(
                "mcp.list_tools.response",
                payload={
                    "server": self.server_name,
                    "error": str(exc),
                },
                session_id=session_id,
                correlation_id=correlation_id,
            )
            raise

        tools = [
            ToolDefinition(
                name=tool.name,
                description=tool.description or tool.name,
                parameters=tool.input_schema
                or {"type": "object", "properties": {}, "required": []},
                server=self.server_name,
                server_tool_name=tool.name,
            )
            for tool in response.tools
        ]
        self._tools_cache = tools
        file_ref = None
        if self._events:
            file_ref = self._events.write_mcp_list_tools(
                self.server_name,
                {
                    "server": self.server_name,
                    "tools": [
                        {
                            "name": tool.name,
                            "description": tool.description,
                            "parameters": tool.parameters,
                        }
                        for tool in tools
                    ],
                },
            )

        await self._emit(
            "mcp.list_tools.response",
            payload={
                "server": self.server_name,
                "tool_count": len(tools),
                "tools": [
                    {"name": tool.name, "description": tool.description}
                    for tool in tools
                ],
                "file_ref": file_ref,
            },
            session_id=session_id,
            correlation_id=correlation_id,
        )
        return tools

    async def call_tool(
        self,
        *,
        session_id: str,
        correlation_id: str,
        name: str,
        arguments: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Invoke a tool via MCP and return a JSON-serializable payload."""
        self._set_context(session_id, correlation_id)
        session = await self._ensure_session()
        await self._emit(
            "tool.call.sdk_attempt",
            payload={
                "server": self.server_name,
                "tool_name": name,
                "timeout_seconds": self.timeout_seconds * 6,
            },
            session_id=session_id,
            correlation_id=correlation_id,
        )
        try:
            # Add timeout to prevent hanging (SDK routing bug can cause delays)
            extended_timeout = max(self.timeout_seconds * 6, 60.0)
            result = await asyncio.wait_for(
                session.call_tool(name, arguments=arguments),
                timeout=extended_timeout
            )
            serialized = serialize_tool_result(result)
            file_ref = None
            if self._events:
                file_ref = self._events.write_tool_payload(
                    self.server_name,
                    name,
                    payload=serialized,
                    transport="sdk",
                )
            await self._emit(
                "tool.call.sdk_response",
                payload={
                    "server": self.server_name,
                    "tool_name": name,
                    "preview": json.dumps(serialized, default=str)[:500],
                    "file_ref": file_ref,
                },
                session_id=session_id,
                correlation_id=correlation_id,
            )
            
            # Check for errors in the result
            if isinstance(serialized, dict):
                # Check for error in structuredContent
                if "structuredContent" in serialized:
                    structured = serialized["structuredContent"]
                    if isinstance(structured, dict) and "error" in structured:
                        error_msg = structured.get("error", "Unknown error")
                        logger.error("MCP tool %s returned error: %s", name, error_msg)
                        return {"error": {"code": "tool_error", "message": str(error_msg)}}
                
                # Check for error in content
                if "content" in serialized:
                    content = serialized["content"]
                    if isinstance(content, list) and len(content) > 0:
                        first_item = content[0]
                        if isinstance(first_item, dict) and "text" in first_item:
                            text = first_item["text"]
                            if isinstance(text, str) and '"error"' in text.lower():
                                logger.error("MCP tool %s returned error in content: %s", name, text)
                                return {"error": {"code": "tool_error", "message": text}}
            
            return serialized
        except asyncio.TimeoutError:
            await self._emit(
                "tool.call.sdk_error",
                payload={
                    "server": self.server_name,
                    "tool_name": name,
                    "error": "timeout",
                    "timeout_seconds": extended_timeout,
                },
                session_id=session_id,
                correlation_id=correlation_id,
            )
            return {"error": {"code": "timeout", "message": f"Tool call timed out after {extended_timeout}s"}}
        except Exception as exc:
            logger.exception("Unexpected error calling MCP tool %s", name)
            await self._emit(
                "tool.call.sdk_error",
                payload={
                    "server": self.server_name,
                    "tool_name": name,
                    "error": str(exc),
                },
                session_id=session_id,
                correlation_id=correlation_id,
            )
            return {"error": {"code": "unexpected_error", "message": str(exc)}}

    async def close(self) -> None:
        """Close the MCP session/transport."""
        # Note: ClientSession doesn't have a close() method when used outside context manager
        # We only need to close the transport context
        try:
            if self._client_ctx:
                await asyncio.wait_for(
                    self._client_ctx.__aexit__(None, None, None), timeout=2.0
                )
        except (asyncio.TimeoutError, Exception) as exc:
            logger.warning("Error closing MCP transport: %s", exc)
        finally:
            self._session = None
            self._client_ctx = None
            self._tools_cache = None
            self._session_context.clear()

    async def _emit(
        self,
        event: str,
        payload: Dict[str, Any],
        session_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        if not self._events:
            return
        if not session_id or not correlation_id:
            context = self._session_context.get("current")
            if context:
                session_id, correlation_id = context
            else:
                return
        await self._events.publish(
            Event(
                event=event,
                session_id=session_id,
                correlation_id=correlation_id,
                source="mcp_client",
                payload=payload,
                tenant_id=self._tenant_id,
                server_id=self._server_id,
            )
        )

    def _set_context(self, session_id: str, correlation_id: str) -> None:
        if session_id and correlation_id:
            self._session_context["current"] = (session_id, correlation_id)


def serialize_tool_result(result: Any) -> Dict[str, Any]:
    """Convert an MCP tool result into plain JSON, handling embedded errors."""
    if result is None:
        return {}
    
    # Check for structured error in the result
    if isinstance(result, dict):
        if "error" in result:
            return result
        if "structuredContent" in result and isinstance(result["structuredContent"], dict) and "error" in result["structuredContent"]:
            return {"error": result["structuredContent"]["error"]}
        if "content" in result and isinstance(result["content"], list):
            for item in result["content"]:
                if isinstance(item, dict) and item.get("type") == "text":
                    try:
                        content_json = json.loads(item.get("text", "{}"))
                        if "error" in content_json:
                            return {"error": content_json["error"]}
                    except json.JSONDecodeError:
                        pass  # Not a JSON error, continue
    
    if hasattr(result, "model_dump"):
        return result.model_dump()
    if isinstance(result, dict):
        return result
    if hasattr(result, "__dict__"):
        return {
            key: value
            for key, value in vars(result).items()
            if not key.startswith("_")
        }
    if isinstance(result, (list, tuple, set)):
        return {"content": list(result)}
    return {"result": str(result)}


class ToolRegistry:
    """In-memory registry of discovered MCP tools."""

    def __init__(self, tools: Optional[List[ToolDefinition]] = None) -> None:
        self._tools: Dict[str, ToolDefinition] = {}
        if tools:
            self.update_tools(tools)

    def update_tools(self, tools: List[ToolDefinition]) -> None:
        self._tools = {tool.name: tool for tool in tools}

    def to_openai_tools(self) -> List[Dict[str, Any]]:
        return [tool.to_openai_tool() for tool in self._tools.values()]

    def get_tool(self, name: str) -> ToolDefinition:
        if name not in self._tools:
            raise KeyError(f"Unknown tool requested: {name}")
        return self._tools[name]

    def is_empty(self) -> bool:
        return not self._tools

    def list_definitions(self) -> List[ToolDefinition]:
        return list(self._tools.values())

