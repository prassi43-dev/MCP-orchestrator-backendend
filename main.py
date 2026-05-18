"""FastAPI entrypoint for the LLM Orchestrator service (tenant-aware)."""
from __future__ import annotations

import asyncio
import logging
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, AsyncGenerator, Dict, List
from uuid import uuid4

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

from config import Settings, get_settings
from llm_orchestrator import LLMOrchestrator
from mcp_client_factory import MCPClientFactory
from mcp_client_http import ToolRegistry
from auth import CurrentUser, get_current_user, require_jwt
from models import (
    ChatRequest,
    CreateTenantRequest,
    RegisterMCPServerRequest,
    TenantResponse,
    MCPServerResponse,
    ChatResponse,
    OrchestrationResult,
    ToolCall,
    Turn,
    OnboardRequest,
)
from persistence_supabase import init_database
from repository_supabase import (
    AuditEventRepository,
    ChatSessionRepository,
    MCPServerRepository,
    TenantRepository,
    ToolRepository,
    UserRepository,
    UserTenantRepository,
)
from telemetry import Event, EventHub, ensure_orchestrator_file_logger
from tool_catalog_service import ToolCatalogService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Uncomment below when debugging HTTP/MCP issues (noisy: every request/response and MCP frame)
# logging.getLogger("httpx").setLevel(logging.DEBUG)
# logging.getLogger("httpcore").setLevel(logging.DEBUG)
# logging.getLogger("mcp").setLevel(logging.DEBUG)
# logging.getLogger("mcp.client").setLevel(logging.DEBUG)
# logging.getLogger("mcp.client.streamable_http").setLevel(logging.DEBUG)

settings: Settings = get_settings()

# Initialize database (Supabase-native, no SQLAlchemy)
db = init_database(settings.supabase_url, settings.supabase_service_role_key)

# Initialize repositories
tenant_repo = TenantRepository(db)
server_repo = MCPServerRepository(db)
tool_repo = ToolRepository(db)
user_tenant_repo = UserTenantRepository(db)
user_repo = UserRepository(db)
audit_repo = AuditEventRepository(db)
chat_repo = ChatSessionRepository(db)

# Initialize services
event_hub = EventHub(settings.logs_dir, settings.stream_raw_sse)
ensure_orchestrator_file_logger(
    Path(settings.logs_dir),
    settings.orchestrator_log_max_bytes,
    settings.orchestrator_log_backup_count,
)

client_factory = MCPClientFactory(
    timeout_seconds=settings.mcp_timeout_seconds,
    event_hub=event_hub,
)
client_factory.set_repository(server_repo)

tool_catalog_service = ToolCatalogService(
    client_factory=client_factory,
    server_repo=server_repo,
    tool_repo=tool_repo,
    event_hub=event_hub,
)

orchestrator = LLMOrchestrator(
    settings=settings,
    client_factory=client_factory,
    event_hub=event_hub,
)

# Chat history: last N messages loaded from DB per request (no in-memory store)
CHAT_HISTORY_MESSAGE_LIMIT = 50

# System prompt for chat (kept in code only, not persisted)
SYSTEM_PROMPT = (
    "You are the MCP Orchestrator assistant for an analytics dashboard. "
    "You may call tools to fetch or analyze data, but all tool usage, retries, "
    "and low-level errors are an internal implementation detail. "
    "In your replies, do NOT narrate steps like 'trying again', 'calling the tool', "
    "or schema debugging. Instead, speak concisely about the final outcome in 1–3 "
    "short paragraphs or bullet points. If tools fail or data is unavailable, "
    "briefly explain the problem in plain language and suggest what the user can try next."
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifespan: startup and shutdown."""
    # Startup
    logger.info("Starting orchestrator service...")
    yield

    # Shutdown
    logger.info("Shutting down...")
    try:
        await asyncio.wait_for(client_factory.close_all(), timeout=3.0)
        logger.info("MCP clients closed")
    except asyncio.TimeoutError:
        logger.warning("MCP client close timed out, forcing cleanup")
    except Exception as exc:
        logger.warning("Error closing MCP clients: %s", exc)


app = FastAPI(title="LLM Orchestrator", version="0.2.0", lifespan=lifespan)

# Configure CORS middleware to handle preflight OPTIONS requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods including OPTIONS
    allow_headers=["*"],  # Allows all headers including X-Correlation-Id
)

# Serve the stream chat frontend at /app (same origin as API for easy integration)
_frontend_dir = Path(__file__).resolve().parent / "frontend"
if _frontend_dir.is_dir():
    app.mount("/app", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")


# ============================================================================
# Health (no auth)
# ============================================================================

@app.get("/health")
async def health() -> Dict[str, str]:
    """Public health check."""
    return {"status": "ok"}


# ============================================================================
# Auth & Onboarding (Supabase JWT)
# ============================================================================

@app.post("/auth/onboard", response_model=TenantResponse)
async def onboard(
    request: OnboardRequest,
    auth_user_id_and_email: Annotated[tuple[str, str | None], Depends(require_jwt)],
) -> TenantResponse:
    """Link the authenticated user to a new tenant. Call after Supabase signup."""
    auth_user_id, email = auth_user_id_and_email
    existing = user_tenant_repo.get_tenant_for_user(auth_user_id)
    if existing:
        raise HTTPException(
            status_code=400,
            detail="User already has a tenant. Use the existing tenant.",
        )
    try:
        tenant_name = (request.tenant_name or "").strip() or "My workspace"
        tenant = tenant_repo.create_tenant(name=tenant_name, metadata=None)
        user_tenant_repo.set_user_tenant(auth_user_id, str(tenant["tenant_id"]))
        user_repo.upsert(auth_user_id, email=email, full_name=request.full_name)
        audit_repo.insert(
            "tenant_created",
            auth_user_id=auth_user_id,
            tenant_id=str(tenant["tenant_id"]),
            payload={"tenant_name": tenant_name, "full_name": request.full_name},
        )
        return TenantResponse(
            tenant_id=tenant["tenant_id"],
            name=tenant["name"],
            metadata=tenant.get("metadata_json", {}),
            created_at=tenant.get("created_at"),
        )
    except Exception as exc:
        logger.exception("Failed to onboard user %s: %s", auth_user_id, exc)
        raise HTTPException(status_code=500, detail=f"Failed to create tenant: {exc}") from exc


# ============================================================================
# Tenant Management APIs (all require auth; tenant scoped to current user)
# ============================================================================

@app.get("/tenants/me")
async def get_my_tenant(
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
) -> Dict[str, Any]:
    """Get current user's tenant, user profile, and MCP servers."""
    tenant = tenant_repo.get_tenant(current_user.tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    servers = server_repo.list_servers(current_user.tenant_id)
    user_row = user_repo.get_by_auth_user_id(current_user.auth_user_id)
    user_info = (
        {"email": user_row.get("email"), "full_name": user_row.get("full_name")}
        if user_row
        else {"email": current_user.email, "full_name": None}
    )
    return {
        "tenant_id": tenant["tenant_id"],
        "name": tenant["name"],
        "metadata": tenant.get("metadata_json", {}),
        "created_at": tenant.get("created_at"),
        "user": user_info,
        "mcp_servers": [
            {
                "server_id": s["server_id"],
                "display_name": s["display_name"],
                "base_url": s["base_url"],
                "last_tools_refresh_at": s.get("last_tools_refresh_at"),
                "tool_count": tool_repo.get_tool_count(current_user.tenant_id, s["server_id"]),
            }
            for s in servers
        ],
    }


@app.get("/tenants/{tenant_id}")
async def get_tenant(
    tenant_id: str,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
) -> Dict[str, Any]:
    """Get tenant information and registered MCP servers (own tenant only)."""
    if tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")
    tenant = tenant_repo.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail=f"Tenant {tenant_id} not found")
    servers = server_repo.list_servers(tenant_id)
    return {
        "tenant_id": tenant["tenant_id"],
        "name": tenant["name"],
        "metadata": tenant.get("metadata_json", {}),
        "created_at": tenant.get("created_at"),
        "mcp_servers": [
            {
                "server_id": s["server_id"],
                "display_name": s["display_name"],
                "base_url": s["base_url"],
                "last_tools_refresh_at": s.get("last_tools_refresh_at"),
                "tool_count": tool_repo.get_tool_count(tenant_id, s["server_id"]),
            }
            for s in servers
        ],
    }


# ============================================================================
# MCP Server Management APIs
# ============================================================================

@app.post("/tenants/{tenant_id}/mcp-servers", response_model=MCPServerResponse)
async def register_mcp_server(
    tenant_id: str,
    request: RegisterMCPServerRequest,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
) -> MCPServerResponse:
    """Register a new MCP server for a tenant with auto-generated UUID."""
    if tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")
    try:
        server = server_repo.register_server(
            tenant_id=tenant_id,
            base_url=request.base_url,
            display_name=request.display_name,
            auth=request.auth,
            fallback_http_base=request.fallback_http_base,
            handshake_path=request.handshake_path,
            extra_headers=request.extra_headers,
        )
        audit_repo.insert(
            "mcp_server_registered",
            auth_user_id=current_user.auth_user_id,
            tenant_id=tenant_id,
            server_id=str(server.get("server_id")),
            payload={"display_name": request.display_name, "base_url": request.base_url},
        )
        return MCPServerResponse(
            id=server.get("id"),
            tenant_id=server.get("tenant_id"),
            server_id=server.get("server_id"),
            display_name=server.get("display_name"),
            base_url=server.get("base_url"),
            created_at=server.get("created_at"),
        )
    except ValueError as exc:
        # Tenant doesn't exist
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Failed to register MCP server for tenant %s: %s", tenant_id, exc)
        raise HTTPException(
            status_code=500, detail=f"Failed to register MCP server: {exc}"
        ) from exc


@app.get("/tenants/{tenant_id}/mcp-servers")
async def list_mcp_servers(
    tenant_id: str,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
) -> Dict[str, List[Dict[str, Any]]]:
    """List all MCP servers for a tenant."""
    if tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")
    servers = server_repo.list_servers(tenant_id)
    return {
            "servers": [
            {
                "server_id": s["server_id"],
                "display_name": s["display_name"],
                "base_url": s["base_url"],
                "status": "active",  # Could check connection status
                "last_tools_refresh_at": s.get("last_tools_refresh_at"),
                "tool_count": tool_repo.get_tool_count(tenant_id, s["server_id"]),
            }
            for s in servers
        ]
    }


@app.delete("/tenants/{tenant_id}/mcp-servers/{server_id}")
async def delete_mcp_server(
    tenant_id: str,
    server_id: str,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
) -> Dict[str, str]:
    """Delete an MCP server for a tenant (and its tools)."""
    if tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")
    server = server_repo.get_server(tenant_id, server_id)
    if not server:
        raise HTTPException(
            status_code=404,
            detail=f"MCP server {tenant_id}/{server_id} not found",
        )
    try:
        display_name = server.get("display_name") or server_id
        server_repo.delete_server(tenant_id, server_id)
        audit_repo.insert(
            "mcp_server_deleted",
            auth_user_id=current_user.auth_user_id,
            tenant_id=tenant_id,
            server_id=server_id,
            payload={"display_name": display_name},
        )
        return {"status": "deleted", "server_id": server_id}
    except Exception as exc:
        logger.exception("Failed to delete MCP server %s/%s: %s", tenant_id, server_id, exc)
        raise HTTPException(
            status_code=500, detail=f"Failed to delete MCP server: {exc}"
        ) from exc


@app.post("/tenants/{tenant_id}/mcp-servers/{server_id}/refresh-tools")
async def refresh_tools(
    tenant_id: str,
    server_id: str,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
) -> Dict[str, Any]:
    """Refresh the tool catalog for a tenant+server."""
    if tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")
    try:
        # Verify server exists
        server = server_repo.get_server(tenant_id, server_id)
        if not server:
            raise HTTPException(
                status_code=404,
                detail=f"MCP server {tenant_id}/{server_id} not found",
            )

        session_id = f"refresh_{tenant_id}_{server_id}"
        correlation_id = str(uuid4())

        # Register session for events
        await event_hub.register_session(
            session_id,
            correlation_id,
            message_preview=f"Tool refresh for {tenant_id}/{server_id}",
        )

        audit_repo.insert(
            "tool_refresh_started",
            auth_user_id=current_user.auth_user_id,
            tenant_id=tenant_id,
            server_id=server_id,
            payload={"display_name": server.get("display_name")},
        )

        try:
            # Refresh tools from MCP
            tools = await tool_catalog_service.get_tools(
                tenant_id=tenant_id,
                server_id=server_id,
                session_id=session_id,
                correlation_id=correlation_id,
                force_refresh=True,
            )

            tool_names = [t.name for t in tools[:5]]  # First 5 for debugging

            await event_hub.publish(
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

            audit_repo.insert(
                "tool_refresh_completed",
                auth_user_id=current_user.auth_user_id,
                tenant_id=tenant_id,
                server_id=server_id,
                payload={"tool_count": len(tools), "tool_names": tool_names},
            )

            return {
                "tenant_id": tenant_id,
                "server_id": server_id,
                "tool_count": len(tools),
                "tool_names": tool_names,
            }
        except Exception as exc:
            await event_hub.publish(
                Event(
                    event="session.end",
                    session_id=session_id,
                    correlation_id=correlation_id,
                    source="orchestrator",
                    payload={"status": "error", "error": str(exc)},
                    tenant_id=tenant_id,
                    server_id=server_id,
                )
            )
            audit_repo.insert(
                "tool_refresh_failed",
                auth_user_id=current_user.auth_user_id,
                tenant_id=tenant_id,
                server_id=server_id,
                payload={"error": str(exc)},
            )
            # Return user-friendly error for connection failures (e.g. invalid base_url host)
            if isinstance(exc, httpx.ConnectError):
                raise HTTPException(
                    status_code=502,
                    detail=f"MCP server unreachable. Check that the server URL is correct and the host is reachable: {exc!s}",
                ) from exc
            raise
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to refresh tools for %s/%s: %s", tenant_id, server_id, exc)
        raise HTTPException(
            status_code=500, detail=f"Failed to refresh tools: {exc}"
        ) from exc


# ============================================================================
# Chat API (tenant-aware)
# ============================================================================

@app.post("/chat", response_model=ChatResponse)
async def chat_endpoint(
    payload: ChatRequest,
    request: Request,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    stream: bool = Query(default=False, description="Stream events via SSE response"),
) -> ChatResponse | EventSourceResponse:
    """Process chat messages via the orchestrator (tenant-aware)."""
    session_id = payload.session_id
    tenant_id = payload.tenant_id or current_user.tenant_id
    if payload.tenant_id and payload.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")
    
    # Resolve server_id
    server_id = payload.server_id
    if not server_id:
        default_server = server_repo.get_default_server(tenant_id)
        if not default_server:
            raise HTTPException(
                status_code=400,
                detail=f"No MCP servers configured for tenant {tenant_id}. Please register a server first.",
            )
        if len(server_repo.list_servers(tenant_id)) > 1:
            raise HTTPException(
                status_code=400,
                detail=f"Multiple MCP servers found for tenant {tenant_id}. Please specify server_id.",
            )
        server_id = default_server.get("server_id")

    logger.info(
        "Incoming /chat request tenant=%s server=%s session=%s message=%s",
        tenant_id,
        server_id,
        session_id,
        (payload.message or "")[:120],
    )

    correlation_id = request.headers.get("X-Correlation-Id") or str(uuid4())

    # Get or create session (tenant + auth_user scoped); set title from first message when new
    title_preview = (payload.message or "").strip()[:80] or None
    session_row = chat_repo.get_or_create_session(
        tenant_id=tenant_id,
        auth_user_id=current_user.auth_user_id,
        session_id=session_id,
        server_id=server_id,
        title_for_new=title_preview,
    )
    chat_session_id = session_row["id"]

    # Persist user message so it is included when we load history
    chat_repo.append_messages(
        chat_session_id,
        [{"role": "user", "content": payload.message or ""}],
    )

    # Load recent messages from DB. For LLM context we use "less context": only user and
    # assistant (final reply) turns. Tool calls and tool results are not sent to the model;
    # the model sees only the conversation summary (Q&A). We still persist full detail to DB.
    loaded = chat_repo.get_recent_messages(chat_session_id, limit=CHAT_HISTORY_MESSAGE_LIMIT)
    turns_from_db = [
        Turn(role=r["role"], content=r["content"] or "")
        for r in loaded
        if r["role"] in ("user", "assistant")
    ]
    recent_history = [Turn(role="system", content=SYSTEM_PROMPT)] + turns_from_db

    def _persist_result(result: OrchestrationResult) -> None:
        tool_rows = [
            {
                "role": "tool",
                "content": t.content or "",
                "tool_name": t.tool_name,
                "tool_call_id": t.tool_call_id,
            }
            for t in result.tool_turns
        ]
        assistant_row = {"role": "assistant", "content": result.assistant_turn.content or ""}
        chat_repo.append_messages(chat_session_id, tool_rows + [assistant_row])

    async def _run_orchestration() -> OrchestrationResult:
        try:
            # Load tools for this tenant+server
            tools = await tool_catalog_service.get_tools(
                tenant_id=tenant_id,
                server_id=server_id,
                session_id=session_id,
                correlation_id=correlation_id,
                force_refresh=False,
            )

            # Build tool registry for this request
            tool_registry = ToolRegistry(tools)

            # Run orchestration (no in-memory history; we only persist to DB after)
            result = await orchestrator.run_chat(
                tenant_id=tenant_id,
                server_id=server_id,
                session_id=session_id,
                correlation_id=correlation_id,
                turns=recent_history,
                tool_registry=tool_registry,
                stream_reply=stream,
            )
            return result
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Failed to orchestrate tenant=%s server=%s session=%s: %s",
                tenant_id,
                server_id,
                session_id,
                exc,
            )
            await event_hub.publish(
                Event(
                    event="assistant.final",
                    session_id=session_id,
                    correlation_id=correlation_id,
                    source="orchestrator",
                    payload={"error": str(exc)},
                    human_status=f"Internal error while processing request (tenant {tenant_id})",
                    tenant_id=tenant_id,
                    server_id=server_id,
                )
            )
            await event_hub.publish(
                Event(
                    event="session.end",
                    session_id=session_id,
                    correlation_id=correlation_id,
                    source="orchestrator",
                    payload={"status": "error"},
                    tenant_id=tenant_id,
                    server_id=server_id,
                )
            )
            raise

    if stream:
        # Subscribe FIRST to ensure we don't miss session.start event
        subscription = await event_hub.subscribe(session_id)
        
        # Now register session (emits session.start) - subscriber will receive it
        await event_hub.register_session(
            session_id, correlation_id, message_preview=(payload.message or "")[:200]
        )
        
        # Start orchestration in background; persist tool + assistant turns when done
        async def _run_and_persist() -> None:
            try:
                result = await _run_orchestration()
                _persist_result(result)
            except Exception:
                pass  # Already logged and events emitted in _run_orchestration
        asyncio.create_task(_run_and_persist())

        async def event_generator():
            try:
                async for event in subscription.events():
                    # Yield each event immediately with proper SSE formatting
                    event_data = {
                        "event": event.event,
                        "data": json.dumps(event.to_dict(), ensure_ascii=False),
                    }
                    yield event_data
                    # Small delay to ensure events are flushed (helps with buffering)
                    await asyncio.sleep(0.001)
                    if event.event == "session.end":
                        break
            finally:
                subscription.close()

        return EventSourceResponse(event_generator())
    
    # Non-streaming path: register session normally
    await event_hub.register_session(
        session_id, correlation_id, message_preview=(payload.message or "")[:200]
    )

    result = await _run_orchestration()
    _persist_result(result)
    tool_calls_serialized: List[ToolCall] | None = result.tool_calls or None
    response = ChatResponse(
        session_id=session_id,
        message=result.assistant_turn.content,
        tool_calls=tool_calls_serialized,
        raw_model_trace=result.raw_trace if settings.orchestrator_debug else None,
    )
    return response


@app.get("/tenants/me/chat-sessions")
async def list_my_chat_sessions(
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    """List chat sessions for the current user (tenant + auth_user), most recent first."""
    sessions = chat_repo.list_sessions(
        tenant_id=current_user.tenant_id,
        auth_user_id=current_user.auth_user_id,
        limit=limit,
        offset=offset,
    )
    return {"sessions": sessions}


@app.get("/tenants/me/chat-sessions/{session_id}/messages")
async def get_chat_session_messages(
    session_id: str,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    limit: int = Query(default=50, ge=1, le=200),
) -> Dict[str, Any]:
    """Get messages for a chat session. Session must belong to the current user."""
    session_row = chat_repo.get_session(
        tenant_id=current_user.tenant_id,
        auth_user_id=current_user.auth_user_id,
        session_id=session_id,
    )
    if not session_row:
        raise HTTPException(status_code=404, detail="Chat session not found")
    messages = chat_repo.get_recent_messages(session_row["id"], limit=limit)
    return {"session_id": session_id, "messages": messages}


@app.get("/events")
async def events_endpoint(
    session_id: str,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
) -> EventSourceResponse:
    """Stream session events via SSE."""
    async def event_generator():
        subscription = await event_hub.subscribe(session_id)
        try:
            async for event in subscription.events():
                yield {
                    "event": event.event,
                    "data": json.dumps(event.to_dict(), ensure_ascii=False),
                }
                if event.event == "session.end":
                    break
        finally:
            subscription.close()

    return EventSourceResponse(event_generator())


@app.websocket("/ws")
async def websocket_events(
    websocket: WebSocket,
    session_id: str,
) -> None:
    """WebSocket alternative for streaming events (auth via query/header as needed)."""
    await websocket.accept()
    subscription = await event_hub.subscribe(session_id)
    try:
        async for event in subscription.events():
            await websocket.send_json(event.to_dict())
            if event.event == "session.end":
                break
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected for session %s", session_id)
    finally:
        subscription.close()
        await websocket.close()
