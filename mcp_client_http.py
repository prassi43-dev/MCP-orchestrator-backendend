"""Direct HTTP client for MCP, bypassing the SDK to avoid routing bugs."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple

import httpx

from models import ToolDefinition
from telemetry import Event, EventHub

logger = logging.getLogger(__name__)


class MCPHTTPClient:
    """
    Direct HTTP client for MCP protocol, bypassing the SDK.
    
    Implements JSON-RPC 2.0 over HTTP with Server-Sent Events (SSE) for responses.
    This avoids the SDK routing bug where SSE responses aren't delivered to waiting calls.
    """

    def __init__(
        self,
        server_name: str,
        base_url: str,
        timeout_seconds: float = 10.0,
        headers: Optional[Dict[str, str]] = None,
        event_hub: Optional[EventHub] = None,
        tenant_id: Optional[str] = None,
        server_id: Optional[str] = None,
    ) -> None:
        self.server_name = server_name
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._default_headers = headers or {}
        self._events = event_hub
        self._http_client: Optional[httpx.AsyncClient] = None
        self._sse_url: Optional[str] = None
        self._session_id: Optional[str] = None
        self._lock = asyncio.Lock()
        self._tools_cache: Optional[List[ToolDefinition]] = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._sse_task: Optional[asyncio.Task] = None
        self._sse_connected = False
        self._session_context: Dict[str, Tuple[str, str]] = {}
        self._tenant_id = tenant_id
        self._server_id = server_id

    async def _ensure_connection(self) -> None:
        """Establish HTTP connection and SSE stream for receiving responses."""
        if self._http_client and self._sse_connected:
            return
        
        async with self._lock:
            if self._http_client and self._sse_connected:
                return
            
            logger.info(
                "Establishing direct HTTP connection to MCP server at %s",
                self.base_url,
            )
            await self._emit(
                "mcp.connect.attempt",
                payload={
                    "server": self.server_name,
                    "url": self.base_url,
                    "transport": "http",
                },
            )
            
            # Create HTTP client
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_seconds * 3, connect=self.timeout_seconds),
                follow_redirects=True,
                headers=self._default_headers,
            )
            
            # For MCP over HTTP, the base URL is used for both POST requests and SSE
            self._sse_url = self.base_url
            
            # Start SSE listener task
            if not self._sse_task or self._sse_task.done():
                self._sse_task = asyncio.create_task(self._listen_sse())
            
            # Wait a bit for SSE connection to establish
            await asyncio.sleep(0.5)
            
            # Send initialize request
            try:
                init_id = str(uuid.uuid4())
                init_future: asyncio.Future = asyncio.Future()
                self._pending_requests[init_id] = init_future
                
                init_request = {
                    "jsonrpc": "2.0",
                    "id": init_id,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {},
                        "clientInfo": {
                            "name": "mcp-orchestrator",
                            "version": "0.1.0",
                        },
                    },
                }
                
                logger.info("Sending initialize request to %s...", self.base_url)
                response = await self._http_client.post(
                    self.base_url,
                    json=init_request,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json, text/event-stream",
                        **self._default_headers,
                    },
                )
                logger.info("Initialize POST response status: %d", response.status_code)
                logger.info("Initialize response headers: %s", dict(response.headers))
                
                # Check for session ID in response headers
                session_id = response.headers.get("mcp-session-id")
                if session_id:
                    self._session_id = session_id
                    logger.info("Received MCP session ID: %s", session_id)
                
                # Log response body if error
                if response.status_code not in (200, 202):
                    try:
                        error_body = response.text
                        logger.error("Initialize error response (status %d): %s", response.status_code, error_body)
                        # Try to parse as JSON
                        try:
                            error_json = response.json()
                            logger.error("Initialize error JSON: %s", json.dumps(error_json, indent=2))
                        except Exception:
                            pass
                    except Exception:
                        pass
                else:
                    # Check if response body contains SSE events (some MCP servers send responses in POST body)
                    try:
                        response_body = response.text
                        logger.debug("Initialize response body: %s", response_body[:500])
                        await self._log_raw_sse(response_body)
                        
                        # Parse SSE events from POST response body
                        if response_body:
                            await self._parse_sse_from_text(response_body, init_id)
                    except Exception as exc:
                        logger.debug("Could not parse POST response body: %s", exc)
                
                # Wait for SSE response (either from POST body or GET stream)
                try:
                    await asyncio.wait_for(
                        init_future, timeout=self.timeout_seconds * 2
                    )
                    logger.info("Initialize completed successfully")
                    self._sse_connected = True
                    await self._emit(
                        "mcp.connect.success",
                        payload={
                            "server": self.server_name,
                            "transport": "http",
                            "session_id": self._session_id,
                        },
                    )
                except asyncio.TimeoutError:
                    self._pending_requests.pop(init_id, None)
                    logger.warning("Initialize timed out, but continuing anyway")
                    self._sse_connected = True  # Assume connected

            except Exception as exc:
                logger.exception("Failed to initialize MCP connection: %s", exc)
                # Don't fail completely - SSE might still work
                self._sse_connected = True
                await self._emit(
                    "mcp.connect.fail",
                    payload={
                        "server": self.server_name,
                        "transport": "http",
                        "reason": str(exc),
                    },
                )

    async def _listen_sse(self) -> None:
        """Listen to SSE stream and route responses to pending requests."""
        logger.info("Starting SSE listener for %s", self._sse_url)
        
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # MCP requires Accept: text/event-stream for SSE
                sse_headers = {
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache",
                    **self._default_headers,
                }
                if self._session_id:
                    sse_headers["mcp-session-id"] = self._session_id
                
                async with self._http_client.stream(
                    "GET", 
                    self._sse_url,
                    headers=sse_headers
                ) as response:
                    if response.status_code not in (200, 202):
                        logger.error("SSE stream returned status %d", response.status_code)
                        retry_count += 1
                        await asyncio.sleep(1)
                        continue
                    
                    logger.info("SSE stream connected, status: %d", response.status_code)
                    retry_count = 0  # Reset on success
                    
                    buffer = ""
                    async for chunk in response.aiter_bytes():
                        buffer += chunk.decode("utf-8", errors="ignore")
                        await self._log_raw_sse(chunk.decode("utf-8", errors="ignore"))
                        
                        # Process complete lines
                        while "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            line = line.strip()
                            
                            if not line:
                                continue
                            
                            # Parse SSE event
                            if line.startswith("data: "):
                                data_str = line[6:]  # Remove "data: " prefix
                                try:
                                    data = json.loads(data_str)
                                    await self._handle_sse_message(data)
                                except json.JSONDecodeError as exc:
                                    logger.debug("Failed to parse SSE data: %s (data: %s)", exc, data_str[:100])
                    
            except asyncio.CancelledError:
                logger.info("SSE listener cancelled")
                break
            except Exception as exc:
                logger.exception("Error in SSE listener: %s", exc)
                retry_count += 1
                if retry_count < max_retries:
                    await asyncio.sleep(1)
                else:
                    logger.error("SSE listener failed after %d retries", max_retries)
                    break

    async def _parse_sse_from_text(self, text: str, expected_id: Optional[str] = None) -> None:
        """Parse SSE events from text (e.g., POST response body) and route responses."""
        lines = text.split("\n")
        current_event = None
        current_data = None
        
        for line in lines:
            line = line.strip()
            if not line:
                # Empty line indicates end of event
                if current_data:
                    try:
                        data = json.loads(current_data)
                        await self._handle_sse_message(data)
                    except json.JSONDecodeError as exc:
                        logger.debug("Failed to parse SSE data from text: %s (data: %s)", exc, current_data[:100])
                current_event = None
                current_data = None
                continue
            
            if line.startswith("event: "):
                current_event = line[7:].strip()
            elif line.startswith("data: "):
                # Handle multi-line data (concatenate)
                data_line = line[6:].strip()
                if current_data:
                    current_data += "\n" + data_line
                else:
                    current_data = data_line
        
        # Handle last event if no trailing newline
        if current_data:
            try:
                data = json.loads(current_data)
                await self._handle_sse_message(data)
            except json.JSONDecodeError as exc:
                logger.debug("Failed to parse SSE data from text: %s (data: %s)", exc, current_data[:100])

    async def _handle_sse_message(self, data: Dict[str, Any]) -> None:
        """Handle incoming SSE message and route to waiting request."""
        request_id = data.get("id")
        if not request_id:
            logger.debug("SSE message without ID: %s", data.get("method", "unknown"))
            return
        
        request_id_str = str(request_id)
        future = self._pending_requests.get(request_id_str)  # Don't pop yet - might be in POST body
        
        if future:
            if "error" in data:
                error_info = data["error"]
                error_msg = error_info.get("message", "Unknown error") if isinstance(error_info, dict) else str(error_info)
                future.set_exception(Exception(f"MCP error: {error_msg}"))
            else:
                future.set_result(data.get("result"))
            # Only pop after successfully routing
            self._pending_requests.pop(request_id_str, None)
            logger.debug("Routed SSE response to request %s", request_id_str)
        else:
            logger.debug("Received SSE response for unknown/expired request ID: %s", request_id_str)

    async def _send_jsonrpc_request(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        session_id: str,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """Send a JSON-RPC request and wait for response via SSE."""
        await self._ensure_connection()
        
        request_id = str(uuid.uuid4())
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        }
        if params:
            request["params"] = params
        
        # Create future for response
        future: asyncio.Future = asyncio.Future()
        self._pending_requests[request_id] = future
        
        try:
            logger.debug("Sending JSON-RPC request: %s (id=%s)", method, request_id)
            
            # Send request via POST with session ID if available
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                **self._default_headers,
            }
            if self._session_id:
                headers["mcp-session-id"] = self._session_id
            
            await self._emit(
                "mcp.http.request",
                payload={
                    "server": self.server_name,
                    "method": method,
                    "request_id": request_id,
                },
                session_id=session_id,
                correlation_id=correlation_id,
            )

            response = await self._http_client.post(
                self.base_url,
                json=request,
                headers=headers,
            )
            
            # Update session ID if server sends a new one
            new_session_id = response.headers.get("mcp-session-id")
            if new_session_id and new_session_id != self._session_id:
                self._session_id = new_session_id
                logger.debug("Updated MCP session ID: %s", new_session_id)
            
            # Log error response body
            if response.status_code not in (200, 202):
                try:
                    error_body = response.text
                    logger.error("Request error response (status %d): %s", response.status_code, error_body[:500])
                except Exception:
                    pass
            
            await self._emit(
                "mcp.http.response",
                payload={
                    "server": self.server_name,
                    "method": method,
                    "request_id": request_id,
                    "status_code": response.status_code,
                },
                session_id=session_id,
                correlation_id=correlation_id,
            )
            
            # Check if response body contains SSE events (some MCP servers send responses in POST body)
            if response.status_code in (200, 202):
                try:
                    response_body = response.text
                    if response_body:
                        logger.debug("Checking POST response body for immediate response...")
                        await self._parse_sse_from_text(response_body, request_id)
                except Exception as exc:
                    logger.debug("Could not parse POST response body: %s", exc)
            
            # Wait for SSE response (either from POST body or GET stream, with timeout)
            try:
                result = await asyncio.wait_for(future, timeout=self.timeout_seconds * 3)
                logger.debug("Received JSON-RPC response for %s", method)
                return result
            except asyncio.TimeoutError:
                self._pending_requests.pop(request_id, None)
                raise TimeoutError(f"Request {method} timed out after {self.timeout_seconds * 3}s")
        
        except Exception as exc:
            self._pending_requests.pop(request_id, None)
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
        await self._emit(
            "mcp.list_tools.request",
            payload={"server": self.server_name, "method": "http"},
            session_id=session_id,
            correlation_id=correlation_id,
        )
        
        logger.info("Fetching tools from MCP server %s via direct HTTP...", self.server_name)
        try:
            result = await self._send_jsonrpc_request(
                "tools/list",
                session_id=session_id,
                correlation_id=correlation_id,
            )
            
            tools_data = result.get("tools", []) if isinstance(result, dict) else []
            tools = [
                ToolDefinition(
                    name=tool.get("name", ""),
                    description=tool.get("description", tool.get("name", "")),
                    parameters=tool.get("inputSchema", {}) or {"type": "object", "properties": {}, "required": []},
                    server=self.server_name,
                    server_tool_name=tool.get("name", ""),
                )
                for tool in tools_data
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
                    "file_ref": file_ref,
                },
            )
            return tools
        
        except Exception as exc:
            logger.exception("Failed to fetch tools via direct HTTP: %s", exc)
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

    async def call_tool(
        self,
        *,
        name: str,
        arguments: Dict[str, Any],
        session_id: str,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """Invoke a tool via MCP and return a JSON-serializable payload."""
        self._set_context(session_id, correlation_id)
        await self._emit(
            "tool.call.fallback_attempt",
            payload={
                "server": self.server_name,
                "tool_name": name,
                "transport": "http",
                "arguments_preview": str(arguments)[:200],
            },
            session_id=session_id,
            correlation_id=correlation_id,
        )
        
        try:
            result = await self._send_jsonrpc_request(
                "tools/call",
                params={"name": name, "arguments": arguments},
                session_id=session_id,
                correlation_id=correlation_id,
            )
            file_ref = None
            if self._events:
                file_ref = self._events.write_tool_payload(
                    self.server_name,
                    name,
                    payload=result,
                    transport="http",
                )
            await self._emit(
                "tool.call.fallback_response",
                payload={
                    "server": self.server_name,
                    "tool_name": name,
                    "result_preview": str(result)[:500],
                    "file_ref": file_ref,
                },
                session_id=session_id,
                correlation_id=correlation_id,
            )
            return serialize_tool_result(result)
        
        except TimeoutError as exc:
            await self._emit(
                "tool.call.error",
                payload={
                    "server": self.server_name,
                    "tool_name": name,
                    "transport": "http",
                    "code": "timeout",
                    "message": str(exc),
                },
                session_id=session_id,
                correlation_id=correlation_id,
            )
            return {"error": {"code": "timeout", "message": str(exc)}}
        except Exception as exc:
            await self._emit(
                "tool.call.error",
                payload={
                    "server": self.server_name,
                    "tool_name": name,
                    "transport": "http",
                    "code": "unexpected_error",
                    "message": str(exc),
                },
                session_id=session_id,
                correlation_id=correlation_id,
            )
            return {"error": {"code": "unexpected_error", "message": str(exc)}}

    async def close(self) -> None:
        """Close the HTTP connection and SSE stream."""
        if self._sse_task:
            self._sse_task.cancel()
            try:
                await self._sse_task
            except asyncio.CancelledError:
                pass
        
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
        
        self._sse_url = None
        self._sse_connected = False
        self._tools_cache = None
        self._pending_requests.clear()
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
        if session_id and correlation_id:
            self._session_context["current"] = (session_id, correlation_id)
        elif "current" in self._session_context:
            session_id, correlation_id = self._session_context["current"]
        else:
            return
        await self._events.publish(
            Event(
                event=event,
                session_id=session_id,
                correlation_id=correlation_id,
                source="mcp_http",
                payload=payload,
                tenant_id=self._tenant_id,
                server_id=self._server_id,
            )
        )
    
    def _set_context(
        self, session_id: str | None, correlation_id: str | None
    ) -> None:
        if session_id and correlation_id:
            self._session_context["current"] = (session_id, correlation_id)

    async def _log_raw_sse(self, raw: str) -> None:
        if not self._events or not raw or not self._events.stream_raw_sse():
            return
        if "current" not in self._session_context:
            return
        session_id, correlation_id = self._session_context["current"]
        file_ref = await self._events.write_mcp_sse(self.server_name, raw)
        await self._events.publish(
            Event(
                event="mcp.sse.message",
                session_id=session_id,
                correlation_id=correlation_id,
                source="mcp_http",
                payload={
                    "server": self.server_name,
                    "file_ref": file_ref,
                    "snippet": raw[:200],
                },
                tenant_id=self._tenant_id,
                server_id=self._server_id,
            )
        )


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
