"""Shared pydantic models."""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class Turn(BaseModel):
    """Represents an item in the conversation history."""

    role: Literal["system", "user", "assistant", "tool"]
    content: str
    tool_name: Optional[str] = None
    tool_call_id: Optional[str] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None  # Store tool_calls for assistant messages


class ToolCall(BaseModel):
    """Represents a tool invocation made by the model."""

    tool_name: str = Field(..., alias="tool_name")
    args: Dict[str, Any]
    error: Optional[str] = None
    server: Optional[str] = None

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True


class ChatRequest(BaseModel):
    """Request payload for /chat endpoint."""

    session_id: str
    message: str
    tenant_id: str | None = None  # Optional for backwards compatibility (defaults to "default")
    server_id: str | None = None  # Optional if tenant has exactly one server


class ChatResponse(BaseModel):
    """Response payload for /chat endpoint."""

    session_id: str
    message: str
    tool_calls: Optional[List[ToolCall]] = None
    raw_model_trace: Optional[Dict[str, Any]] = None


class ToolDefinition(BaseModel):
    """Normalized representation of an MCP tool for the LLM."""

    name: str
    description: str
    parameters: Dict[str, Any]
    server: str
    server_tool_name: str

    def to_openai_tool(self) -> Dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class OrchestrationResult(BaseModel):
    """Encapsulates the outcome of an orchestration cycle."""

    assistant_turn: Turn
    tool_turns: List[Turn]
    tool_calls: List[ToolCall]
    raw_trace: Optional[Dict[str, Any]] = None


# ============================================================================
# Tenant Management Models
# ============================================================================

class CreateTenantRequest(BaseModel):
    """Request to create a new tenant."""

    name: str
    metadata: Optional[Dict[str, Any]] = None


class TenantResponse(BaseModel):
    """Tenant information response."""

    tenant_id: str
    name: str
    metadata: Dict[str, Any]
    created_at: Optional[str] = None


class RegisterMCPServerRequest(BaseModel):
    """Request to register an MCP server."""

    base_url: str
    display_name: str
    auth: Optional[Dict[str, Any]] = None
    fallback_http_base: Optional[str] = None
    handshake_path: Optional[str] = None
    extra_headers: Optional[Dict[str, str]] = None


class MCPServerResponse(BaseModel):
    """MCP server information response."""

    id: str
    tenant_id: str
    server_id: str
    display_name: str
    base_url: str
    created_at: Optional[str] = None


class OnboardRequest(BaseModel):
    """Request to link a new user to a tenant (after Supabase signup)."""

    tenant_name: Optional[str] = None
    full_name: Optional[str] = None

