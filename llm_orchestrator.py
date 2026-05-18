"""Core orchestration logic for OpenAI + MCP tools (tenant-aware)."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

from openai import AsyncOpenAI, OpenAI

from config import Settings
from mcp_client_factory import MCPClientFactory
from mcp_client_http import ToolRegistry
from models import OrchestrationResult, ToolCall, Turn
from telemetry import Event, EventHub

logger = logging.getLogger(__name__)


class LLMOrchestrator:
    """LLM orchestration pipeline that supports tool-calling (tenant-aware)."""

    def __init__(
        self,
        settings: Settings,
        client_factory: MCPClientFactory,
        event_hub: EventHub,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory
        self._client = OpenAI(api_key=settings.openai_api_key)
        self._async_client = AsyncOpenAI(api_key=settings.openai_api_key)
        self._events = event_hub

    async def run_chat(
        self,
        tenant_id: str,
        server_id: str,
        session_id: str,
        correlation_id: str,
        turns: List[Turn],
        tool_registry: ToolRegistry,
        stream_reply: bool = False,
    ) -> OrchestrationResult:
        """Execute an orchestration loop for the given turns (tenant-aware)."""
        messages = [self._turn_to_message(turn) for turn in turns]
        tools = tool_registry.to_openai_tools()
        tools_payload = tools or None

        if stream_reply:
            assistant_message, tool_calls, trace = await self._stream_completion(
                messages, tools_payload,
                tenant_id, server_id, session_id, correlation_id,
            )
        else:
            initial_response = await self._create_completion(messages, tools_payload)
            tool_calls = initial_response.choices[0].message.tool_calls or []
            assistant_message = initial_response.choices[0].message
            trace = {"initial_response": initial_response.model_dump(mode="python")}

        tool_calls = tool_calls or []

        if not tool_calls:
            final_turn = Turn(
                role="assistant",
                content=assistant_message.content or "",
                tool_calls=None,
            )
            # Emit events even when no tool calls are made
            await self._events.publish(
                Event(
                    event="assistant.final",
                    session_id=session_id,
                    correlation_id=correlation_id,
                    source="orchestrator",
                    payload={
                        "message": final_turn.content,
                        "tool_calls": None,
                    },
                    human_status="Assistant response ready",
                    tenant_id=tenant_id,
                    server_id=server_id,
                )
            )
            await self._events.publish(
                Event(
                    event="session.end",
                    session_id=session_id,
                    correlation_id=correlation_id,
                    source="orchestrator",
                    payload={"status": "completed"},
                    tenant_id=tenant_id,
                    server_id=server_id,
                )
            )
            return OrchestrationResult(
                assistant_turn=final_turn,
                tool_turns=[],
                tool_calls=[],
                raw_trace=trace,
            )

        openai_assistant_message = self._message_from_openai(assistant_message)
        
        # Save the initial assistant message with tool_calls to history
        initial_assistant_tool_calls = [
            {
                "id": tool_call.id,
                "type": tool_call.type,
                "function": {
                    "name": tool_call.function.name,
                    "arguments": tool_call.function.arguments,
                },
            }
            for tool_call in tool_calls
        ]
        initial_assistant_turn = Turn(
            role="assistant",
            content=assistant_message.content or "",
            tool_calls=initial_assistant_tool_calls,
        )
        
        tool_messages: List[Dict[str, Any]] = []
        executed_tool_turns: List[Turn] = []
        tool_call_records: List[ToolCall] = []

        # Get MCP client for this tenant+server
        mcp_client = await self._client_factory.get_client(
            tenant_id, server_id, session_id, correlation_id
        )

        for tool_call in tool_calls:
            args_dict = self._parse_tool_args(tool_call.function.arguments)
            try:
                definition = tool_registry.get_tool(tool_call.function.name)
            except KeyError as exc:
                logger.error(
                    "Model requested unknown tool %s (tenant=%s server=%s)",
                    tool_call.function.name,
                    tenant_id,
                    server_id,
                )
                raise ValueError(str(exc)) from exc

            await self._events.publish(
                Event(
                    event="tool.call.start",
                    session_id=session_id,
                    correlation_id=correlation_id,
                    source="orchestrator",
                    payload={
                        "tool_name": definition.name,
                        "server": definition.server,
                        "tool_call_id": tool_call.id,
                        "arguments": args_dict,
                    },
                    human_status=f"Invoking tool {definition.name} (tenant {tenant_id})",
                    tenant_id=tenant_id,
                    server_id=server_id,
                )
            )

            try:
                start_time = time.perf_counter()
                # Call tool via MCP client (using server_tool_name, not aliased name)
                tool_result = await mcp_client.call_tool(
                    name=definition.server_tool_name,
                    arguments=args_dict,
                    session_id=session_id,
                    correlation_id=correlation_id,
                )
                tool_payload = tool_result
                tool_error: str | None = None
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Tool execution failed for %s (tenant=%s server=%s)",
                    definition.name,
                    tenant_id,
                    server_id,
                )
                tool_payload = {
                    "error": {
                        "code": "tool_execution_failed",
                        "message": str(exc),
                    }
                }
                tool_error = str(exc)
            finally:
                elapsed = (
                    (time.perf_counter() - start_time) * 1000.0 if "start_time" in locals() else None
                )

                # Prepare a short preview string plus a richer JSON snippet for UI cards.
                preview_text = tool_payload
                if isinstance(preview_text, (dict, list)):
                    preview_text = json.dumps(preview_text)[:500]
                else:
                    preview_text = str(preview_text)[:500]

                # Trimmed JSON representation of the tool result for ChatGPT-style cards.
                try:
                    result_snippet = json.dumps(tool_payload, ensure_ascii=False)
                except TypeError:
                    # Fallback if the payload is not JSON-serializable as-is.
                    result_snippet = str(tool_payload)
                # Hard cap to avoid huge payloads in the stream.
                max_len = 4000
                if len(result_snippet) > max_len:
                    result_snippet = result_snippet[:max_len] + "…"

                await self._events.publish(
                    Event(
                        event="tool.call.complete",
                        session_id=session_id,
                        correlation_id=correlation_id,
                        source="orchestrator",
                        payload={
                            "tool_name": definition.name,
                            "server": definition.server,
                            "tool_call_id": tool_call.id,
                            "success": tool_error is None,
                            "latency_ms": elapsed,
                            "arguments": args_dict,
                            "preview": preview_text,
                            "result_snippet": result_snippet,
                        },
                        human_status=(
                            f"Tool {definition.name} completed (tenant {tenant_id})"
                            if not tool_error
                            else f"Tool {definition.name} failed (tenant {tenant_id})"
                        ),
                        tenant_id=tenant_id,
                        server_id=server_id,
                    )
                )

            tool_message_content = json.dumps(tool_payload)
            tool_messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": definition.name,
                    "content": tool_message_content,
                }
            )
            executed_tool_turns.append(
                Turn(
                    role="tool",
                    content=tool_message_content,
                    tool_name=definition.name,
                    tool_call_id=tool_call.id,
                )
            )
            tool_call_records.append(
                ToolCall(
                    tool_name=definition.name,
                    args=args_dict,
                    error=tool_error,
                    server=definition.server,
                )
            )

        follow_up_messages = messages + [openai_assistant_message] + tool_messages

        if stream_reply:
            final_message, _, _ = await self._stream_completion(
                follow_up_messages,
                None,
                tenant_id,
                server_id,
                session_id,
                correlation_id,
            )
            trace["follow_up_streamed"] = True
        else:
            follow_up_response = await self._create_completion(follow_up_messages)
            final_message = follow_up_response.choices[0].message
            trace["follow_up_response"] = follow_up_response.model_dump(mode="python")

        # Store tool_calls in the assistant turn if the final message has them
        final_tool_calls = None
        if final_message.tool_calls:
            final_tool_calls = [
                {
                    "id": tool_call.id,
                    "type": tool_call.type,
                    "function": {
                        "name": tool_call.function.name,
                        "arguments": tool_call.function.arguments,
                    },
                }
                for tool_call in final_message.tool_calls
            ]
        
        assistant_turn = Turn(
            role="assistant",
            content=final_message.content or "",
            tool_calls=final_tool_calls,
        )

        await self._events.publish(
            Event(
                event="assistant.final",
                session_id=session_id,
                correlation_id=correlation_id,
                source="orchestrator",
                payload={
                    "message": assistant_turn.content,
                    "tool_calls": final_tool_calls,
                },
                human_status="Assistant response ready",
                tenant_id=tenant_id,
                server_id=server_id,
            )
        )
        await self._events.publish(
            Event(
                event="session.end",
                session_id=session_id,
                correlation_id=correlation_id,
                source="orchestrator",
                payload={"status": "completed"},
                tenant_id=tenant_id,
                server_id=server_id,
            )
        )
        return OrchestrationResult(
            assistant_turn=assistant_turn,
            tool_turns=[initial_assistant_turn] + executed_tool_turns,
            tool_calls=tool_call_records,
            raw_trace=trace,
        )

    async def _create_completion(
        self, messages: List[Dict[str, Any]], tools: List[Dict[str, Any]] | None = None
    ) -> Any:
        """Call the OpenAI chat completions endpoint in a separate thread."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._create_completion_sync, messages, tools
        )

    def _create_completion_sync(
        self, messages: List[Dict[str, Any]], tools: List[Dict[str, Any]] | None
    ) -> Any:
        logger.debug("Dispatching %d messages to OpenAI", len(messages))
        return self._client.chat.completions.create(  # type: ignore[attr-defined]
            model=self._settings.openai_model,
            messages=messages,
            tools=tools,
            tool_choice="auto" if tools else None,
        )

    async def _stream_completion(
        self,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]] | None,
        tenant_id: str,
        server_id: str,
        session_id: str,
        correlation_id: str,
    ) -> Tuple[Any, List[Any], Dict[str, Any]]:
        """Stream completion, emit assistant.content for each delta; return (message, tool_calls, trace)."""
        content_parts: List[str] = []
        tool_calls_by_index: Dict[int, Dict[str, Any]] = {}
        trace: Dict[str, Any] = {"streamed": True}

        stream = await self._async_client.chat.completions.create(
            model=self._settings.openai_model,
            messages=messages,
            tools=tools,
            tool_choice="auto" if tools else None,
            stream=True,
        )

        async for chunk in stream:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta
            if delta is None:
                continue
            if getattr(delta, "content", None) and delta.content:
                content_parts.append(delta.content)
                await self._events.publish(
                    Event(
                        event="assistant.content",
                        session_id=session_id,
                        correlation_id=correlation_id,
                        source="orchestrator",
                        payload={"delta": delta.content},
                        human_status=delta.content,
                        tenant_id=tenant_id,
                        server_id=server_id,
                    )
                )
            if getattr(delta, "tool_calls", None) and delta.tool_calls:
                for tc in delta.tool_calls:
                    idx = getattr(tc, "index", None)
                    if idx is None:
                        continue
                    if idx not in tool_calls_by_index:
                        tool_calls_by_index[idx] = {
                            "id": "",
                            "type": "function",
                            "function": {"name": "", "arguments": ""},
                        }
                    acc = tool_calls_by_index[idx]
                    if getattr(tc, "id", None):
                        acc["id"] = tc.id
                    if getattr(tc, "type", None):
                        acc["type"] = tc.type
                    fn = getattr(tc, "function", None)
                    if fn:
                        if getattr(fn, "name", None):
                            acc["function"]["name"] = fn.name or ""
                        if getattr(fn, "arguments", None):
                            acc["function"]["arguments"] = (
                                acc["function"]["arguments"] + (fn.arguments or "")
                            )

        full_content = "".join(content_parts)
        tool_calls_list: List[Any] = []
        for idx in sorted(tool_calls_by_index.keys()):
            acc = tool_calls_by_index[idx]
            tool_calls_list.append(
                SimpleNamespace(
                    id=acc["id"],
                    type=acc["type"],
                    function=SimpleNamespace(
                        name=acc["function"]["name"],
                        arguments=acc["function"]["arguments"] or "{}",
                    ),
                )
            )

        # Build message-like object for the rest of run_chat
        message = SimpleNamespace(
            role="assistant",
            content=full_content,
            tool_calls=tool_calls_list if tool_calls_list else None,
        )
        return message, tool_calls_list, trace

    @staticmethod
    def _turn_to_message(turn: Turn) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"role": turn.role, "content": turn.content}
        if turn.role == "tool":
            if turn.tool_call_id:
                payload["tool_call_id"] = turn.tool_call_id
            if turn.tool_name:
                payload["name"] = turn.tool_name
        elif turn.role == "assistant" and turn.tool_calls:
            # Restore tool_calls for assistant messages
            payload["tool_calls"] = turn.tool_calls
        return payload

    @staticmethod
    def _parse_tool_args(arguments: str | None) -> Dict[str, Any]:
        if not arguments:
            return {}
        try:
            return json.loads(arguments)
        except json.JSONDecodeError as exc:
            logger.error("Failed to decode tool arguments: %s", exc)
            raise ValueError("Invalid tool arguments") from exc

    @staticmethod
    def _message_from_openai(message: Any) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "role": message.role,
            "content": message.content or "",
        }
        if message.tool_calls:
            payload["tool_calls"] = [
                {
                    "id": tool_call.id,
                    "type": tool_call.type,
                    "function": {
                        "name": tool_call.function.name,
                        "arguments": tool_call.function.arguments,
                    },
                }
                for tool_call in message.tool_calls
            ]
        return payload
