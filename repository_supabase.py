"""Repository layer using Supabase client directly (no SQLAlchemy)."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from models import ToolDefinition
from persistence_supabase import SupabaseDatabase, get_database
from tool_catalog import build_tool_alias

logger = logging.getLogger(__name__)


class TenantRepository:
    """Repository for tenant operations (Supabase-native)."""

    def __init__(self, db: SupabaseDatabase | None = None):
        self.db = db or get_database()

    def create_tenant(
        self,
        name: str,
        metadata: Dict[str, Any] | None = None,
        tenant_id: str | None = None,
    ) -> Dict[str, Any]:
        """Create a new tenant with auto-generated UUID."""
        try:
            # Generate UUID if not provided
            if tenant_id is None:
                tenant_id = str(uuid4())
            
            data = {
                "tenant_id": tenant_id,
                "name": name,
                "metadata_json": metadata or {},
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            
            result = self.db.client.table("tenants").insert(data).execute()
            logger.info("Created tenant %s (%s)", tenant_id, name)
            return result.data[0] if result.data else data
        except Exception as exc:
            logger.exception("Failed to create tenant: %s", exc)
            raise

    def create_or_update_tenant(self, tenant_id: str, name: str) -> Dict[str, Any]:
        """Ensure a tenant exists; create it if not (for default tenant at startup)."""
        existing = self.get_tenant(tenant_id)
        if existing:
            return existing
        return self.create_tenant(name=name, metadata=None, tenant_id=tenant_id)

    def get_tenant(self, tenant_id: str) -> Dict[str, Any] | None:
        """Get a tenant by ID."""
        try:
            result = (
                self.db.client.table("tenants")
                .select("*")
                .eq("tenant_id", tenant_id)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as exc:
            logger.exception("Failed to get tenant %s: %s", tenant_id, exc)
            return None

    def list_tenants(self) -> List[Dict[str, Any]]:
        """List all tenants."""
        try:
            result = self.db.client.table("tenants").select("*").execute()
            return result.data or []
        except Exception as exc:
            logger.exception("Failed to list tenants: %s", exc)
            return []


class UserTenantRepository:
    """Repository for linking Supabase Auth users to tenants."""

    def __init__(self, db: SupabaseDatabase | None = None):
        self.db = db or get_database()

    def get_tenant_for_user(self, auth_user_id: str) -> Dict[str, Any] | None:
        """Get the tenant linked to an auth user."""
        try:
            result = (
                self.db.client.table("user_tenants")
                .select("tenant_id")
                .eq("auth_user_id", auth_user_id)
                .execute()
            )
            if not result.data:
                return None
            tenant_id = result.data[0]["tenant_id"]
            tenant_repo = TenantRepository(self.db)
            return tenant_repo.get_tenant(str(tenant_id))
        except Exception as exc:
            logger.exception("Failed to get tenant for user %s: %s", auth_user_id, exc)
            return None

    def set_user_tenant(self, auth_user_id: str, tenant_id: str) -> None:
        """Link an auth user to a tenant (upsert)."""
        try:
            row = {
                "auth_user_id": auth_user_id,
                "tenant_id": tenant_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self.db.client.table("user_tenants").upsert(row, on_conflict="auth_user_id").execute()
            logger.info("Linked user %s to tenant %s", auth_user_id, tenant_id)
        except Exception as exc:
            logger.exception("Failed to set user tenant: %s", exc)
            raise


class UserRepository:
    """Repository for user profile (email, full_name) keyed by auth user id."""

    def __init__(self, db: SupabaseDatabase | None = None):
        self.db = db or get_database()

    def get_by_auth_user_id(self, auth_user_id: str) -> Dict[str, Any] | None:
        """Get user profile by Supabase auth user id."""
        try:
            result = (
                self.db.client.table("users")
                .select("*")
                .eq("auth_user_id", auth_user_id)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as exc:
            logger.exception("Failed to get user %s: %s", auth_user_id, exc)
            return None

    def upsert(self, auth_user_id: str, email: str | None = None, full_name: str | None = None) -> None:
        """Insert or update user profile."""
        try:
            now = datetime.now(timezone.utc).isoformat()
            row = {
                "auth_user_id": auth_user_id,
                "email": email,
                "full_name": full_name,
                "updated_at": now,
            }
            existing = self.get_by_auth_user_id(auth_user_id)
            if existing:
                self.db.client.table("users").update({
                    "email": email,
                    "full_name": full_name,
                    "updated_at": now,
                }).eq("auth_user_id", auth_user_id).execute()
            else:
                row["created_at"] = now
                self.db.client.table("users").upsert(row, on_conflict="auth_user_id").execute()
            logger.info("Upserted user %s", auth_user_id)
        except Exception as exc:
            logger.exception("Failed to upsert user: %s", exc)
            raise


class MCPServerRepository:
    """Repository for MCP server operations (Supabase-native)."""

    def __init__(self, db: SupabaseDatabase | None = None):
        self.db = db or get_database()

    def register_server(
        self,
        tenant_id: str,
        base_url: str,
        display_name: str,
        auth: Dict[str, Any] | None = None,
        fallback_http_base: str | None = None,
        handshake_path: str | None = None,
        extra_headers: Dict[str, str] | None = None,
        server_id: str | None = None,
    ) -> Dict[str, Any]:
        """Register a new MCP server for a tenant with auto-generated UUID."""
        try:
            # Ensure tenant exists
            tenant_repo = TenantRepository(self.db)
            tenant = tenant_repo.get_tenant(tenant_id)
            if not tenant:
                raise ValueError(f"Tenant {tenant_id} does not exist")

            # Generate UUID if not provided
            if server_id is None:
                server_id = str(uuid4())

            data = {
                "tenant_id": tenant_id,
                "server_id": server_id,
                "base_url": base_url,
                "display_name": display_name,
                "auth_json": auth or {},
                "fallback_http_base": fallback_http_base,
                "handshake_path": handshake_path,
                "extra_headers_json": extra_headers or {},
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            result = self.db.client.table("mcp_servers").insert(data).execute()
            logger.info("Registered MCP server %s/%s (%s)", tenant_id, server_id, display_name)
            return result.data[0] if result.data else data
        except Exception as exc:
            logger.exception("Failed to register MCP server for tenant %s: %s", tenant_id, exc)
            raise

    def get_server(self, tenant_id: str, server_id: str) -> Dict[str, Any] | None:
        """Get an MCP server configuration."""
        try:
            result = (
                self.db.client.table("mcp_servers")
                .select("*")
                .eq("tenant_id", tenant_id)
                .eq("server_id", server_id)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as exc:
            logger.exception("Failed to get server %s/%s: %s", tenant_id, server_id, exc)
            return None

    def list_servers(self, tenant_id: str) -> List[Dict[str, Any]]:
        """List all MCP servers for a tenant."""
        try:
            result = (
                self.db.client.table("mcp_servers")
                .select("*")
                .eq("tenant_id", tenant_id)
                .execute()
            )
            return result.data or []
        except Exception as exc:
            logger.exception("Failed to list servers for tenant %s: %s", tenant_id, exc)
            return []

    def delete_server(self, tenant_id: str, server_id: str) -> None:
        """Delete an MCP server and its tools for a tenant."""
        try:
            self.db.client.table("mcp_tools").delete().eq("tenant_id", tenant_id).eq(
                "server_id", server_id
            ).execute()
            self.db.client.table("mcp_servers").delete().eq("tenant_id", tenant_id).eq(
                "server_id", server_id
            ).execute()
            logger.info("Deleted MCP server %s/%s", tenant_id, server_id)
        except Exception as exc:
            logger.exception("Failed to delete server %s/%s: %s", tenant_id, server_id, exc)
            raise

    def get_default_server(self, tenant_id: str) -> Dict[str, Any] | None:
        """Get the default server for a tenant."""
        try:
            # Try 'default' first
            result = (
                self.db.client.table("mcp_servers")
                .select("*")
                .eq("tenant_id", tenant_id)
                .eq("server_id", "default")
                .limit(1)
                .execute()
            )
            if result.data:
                return result.data[0]
            
            # Otherwise return first server
            result = (
                self.db.client.table("mcp_servers")
                .select("*")
                .eq("tenant_id", tenant_id)
                .limit(1)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as exc:
            logger.exception("Failed to get default server for tenant %s: %s", tenant_id, exc)
            return None


class ToolRepository:
    """Repository for tool operations (Supabase-native)."""

    def __init__(self, db: SupabaseDatabase | None = None):
        self.db = db or get_database()

    def save_tools(
        self,
        tenant_id: str,
        server_id: str,
        tools: List[ToolDefinition],
    ) -> int:
        """Save or update tools for a tenant+server."""
        try:
            # Delete existing tools for this tenant+server
            self.db.client.table("mcp_tools").delete().eq("tenant_id", tenant_id).eq(
                "server_id", server_id
            ).execute()

            # Insert new tools
            tool_records = []
            now = datetime.now(timezone.utc).isoformat()
            for tool in tools:
                tool_records.append({
                    "tenant_id": tenant_id,
                    "server_id": server_id,
                    "tool_name": tool.name,
                    "server_tool_name": tool.server_tool_name,
                    "description": tool.description,
                    "parameters_json": tool.parameters,
                    "created_at": now,
                    "updated_at": now,
                })

            if tool_records:
                self.db.client.table("mcp_tools").insert(tool_records).execute()

            # Update server's last_tools_refresh_at
            self.db.client.table("mcp_servers").update({
                "last_tools_refresh_at": datetime.now(timezone.utc).isoformat()
            }).eq("tenant_id", tenant_id).eq("server_id", server_id).execute()

            count = len(tools)
            logger.info("Saved %d tool(s) for tenant=%s server=%s", count, tenant_id, server_id)
            return count
        except Exception as exc:
            logger.exception("Failed to save tools for %s/%s: %s", tenant_id, server_id, exc)
            raise

    def load_tools(self, tenant_id: str, server_id: str) -> List[ToolDefinition]:
        """Load tools for a tenant+server."""
        try:
            result = (
                self.db.client.table("mcp_tools")
                .select("*")
                .eq("tenant_id", tenant_id)
                .eq("server_id", server_id)
                .execute()
            )
            
            return [
                ToolDefinition(
                    name=tool["tool_name"],
                    description=tool["description"],
                    parameters=tool["parameters_json"],
                    server=server_id,
                    server_tool_name=tool["server_tool_name"],
                )
                for tool in result.data or []
            ]
        except Exception as exc:
            logger.exception("Failed to load tools for %s/%s: %s", tenant_id, server_id, exc)
            return []

    def get_tool_count(self, tenant_id: str, server_id: str) -> int:
        """Get the number of tools for a tenant+server."""
        try:
            result = (
                self.db.client.table("mcp_tools")
                .select("id", count="exact")
                .eq("tenant_id", tenant_id)
                .eq("server_id", server_id)
                .execute()
            )
            return result.count or 0
        except Exception as exc:
            logger.exception("Failed to get tool count for %s/%s: %s", tenant_id, server_id, exc)
            return 0


class AuditEventRepository:
    """Repository for audit/lifecycle events (single table, event_type + payload)."""

    def __init__(self, db: SupabaseDatabase | None = None):
        self.db = db or get_database()

    def insert(
        self,
        event_type: str,
        *,
        auth_user_id: str | None = None,
        tenant_id: str | None = None,
        server_id: str | None = None,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        """Append one audit event. Best-effort: log and skip on failure so requests are not broken."""
        try:
            row: Dict[str, Any] = {
                "event_type": event_type,
                "payload": payload or {},
            }
            if tenant_id is not None:
                row["tenant_id"] = tenant_id
            if auth_user_id is not None:
                row["auth_user_id"] = auth_user_id
            if server_id is not None:
                row["server_id"] = server_id
            self.db.client.table("audit_events").insert(row).execute()
        except Exception as exc:
            logger.warning("Audit event insert failed (event_type=%s): %s", event_type, exc)


class ChatSessionRepository:
    """Repository for chat sessions and messages (tenant + auth_user scoped)."""

    def __init__(self, db: SupabaseDatabase | None = None):
        self.db = db or get_database()

    def get_session(
        self,
        tenant_id: str,
        auth_user_id: str,
        session_id: str,
    ) -> Dict[str, Any] | None:
        """Get a chat session by tenant, user, and client session_id."""
        try:
            result = (
                self.db.client.table("chat_sessions")
                .select("*")
                .eq("tenant_id", tenant_id)
                .eq("auth_user_id", auth_user_id)
                .eq("session_id", session_id)
                .limit(1)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as exc:
            logger.exception(
                "Failed to get chat session %s for tenant=%s user=%s: %s",
                session_id,
                tenant_id,
                auth_user_id,
                exc,
            )
            return None

    def get_or_create_session(
        self,
        tenant_id: str,
        auth_user_id: str,
        session_id: str,
        *,
        server_id: str | None = None,
        title_for_new: str | None = None,
    ) -> Dict[str, Any]:
        """Get existing session or create one. Sets title from first message when creating."""
        existing = self.get_session(tenant_id, auth_user_id, session_id)
        if existing:
            now = datetime.now(timezone.utc).isoformat()
            self.db.client.table("chat_sessions").update({
                "updated_at": now,
                **({"server_id": server_id} if server_id is not None else {}),
            }).eq("id", existing["id"]).execute()
            return self.get_session(tenant_id, auth_user_id, session_id) or existing
        title = (title_for_new or "New conversation").strip()
        if len(title) > 512:
            title = title[:509] + "..."
        try:
            row = {
                "session_id": session_id,
                "tenant_id": tenant_id,
                "auth_user_id": auth_user_id,
                "server_id": server_id,
                "title": title or "New conversation",
            }
            result = self.db.client.table("chat_sessions").insert(row).execute()
            return result.data[0] if result.data else row
        except Exception as exc:
            logger.exception(
                "Failed to create chat session %s for tenant=%s user=%s: %s",
                session_id,
                tenant_id,
                auth_user_id,
                exc,
            )
            raise

    def list_sessions(
        self,
        tenant_id: str,
        auth_user_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List chat sessions for the user, most recently updated first."""
        try:
            result = (
                self.db.client.table("chat_sessions")
                .select("id, session_id, title, created_at, updated_at")
                .eq("tenant_id", tenant_id)
                .eq("auth_user_id", auth_user_id)
                .order("updated_at", desc=True)
                .range(offset, offset + limit - 1)
                .execute()
            )
            return result.data or []
        except Exception as exc:
            logger.exception(
                "Failed to list chat sessions for tenant=%s user=%s: %s",
                tenant_id,
                auth_user_id,
                exc,
            )
            return []

    def get_recent_messages(
        self,
        chat_session_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get the most recent messages for a session in chronological order."""
        try:
            # Fetch last N by sequence desc, then we return in asc order
            result = (
                self.db.client.table("chat_messages")
                .select("role, content, tool_name, tool_call_id, sequence")
                .eq("chat_session_id", chat_session_id)
                .order("sequence", desc=True)
                .limit(limit)
                .execute()
            )
            rows = result.data or []
            rows.reverse()
            return rows
        except Exception as exc:
            logger.exception(
                "Failed to get messages for session %s: %s",
                chat_session_id,
                exc,
            )
            return []

    def get_next_sequence(self, chat_session_id: str) -> int:
        """Return the next sequence number for appending a message."""
        try:
            result = (
                self.db.client.table("chat_messages")
                .select("sequence")
                .eq("chat_session_id", chat_session_id)
                .order("sequence", desc=True)
                .limit(1)
                .execute()
            )
            if not result.data:
                return 1
            return int(result.data[0]["sequence"]) + 1
        except Exception as exc:
            logger.exception("Failed to get next sequence for session %s: %s", chat_session_id, exc)
            return 1

    def append_messages(
        self,
        chat_session_id: str,
        messages: List[Dict[str, Any]],
    ) -> None:
        """Append messages with sequential ordering. Each dict: role, content, tool_name?, tool_call_id?."""
        if not messages:
            return
        try:
            next_seq = self.get_next_sequence(chat_session_id)
            now = datetime.now(timezone.utc).isoformat()
            rows = []
            for m in messages:
                rows.append({
                    "chat_session_id": chat_session_id,
                    "role": m["role"],
                    "content": (m.get("content") or "").strip() or "",
                    "tool_name": m.get("tool_name"),
                    "tool_call_id": m.get("tool_call_id"),
                    "sequence": next_seq,
                })
                next_seq += 1
            self.db.client.table("chat_messages").insert(rows).execute()
            # Bump session updated_at
            self.db.client.table("chat_sessions").update({
                "updated_at": now,
            }).eq("id", chat_session_id).execute()
        except Exception as exc:
            logger.exception(
                "Failed to append messages to session %s: %s",
                chat_session_id,
                exc,
            )
            raise
