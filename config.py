"""Application configuration module."""
from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Dict, List, Optional

from pydantic import AnyHttpUrl, BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class MCPServerConfig(BaseModel):
    """Configuration for a single MCP server instance."""

    name: str
    base_url: AnyHttpUrl
    auth_token_env: Optional[str] = None
    fallback_http_base: Optional[AnyHttpUrl] = None
    handshake_path: Optional[str] = None
    extra_headers: Dict[str, str] = Field(default_factory=dict)

    def resolve_token(self) -> Optional[str]:
        if not self.auth_token_env:
            return None
        token = os.getenv(self.auth_token_env)
        return token or None

    def compile_headers(self) -> Dict[str, str]:
        headers = dict(self.extra_headers)
        token = self.resolve_token()
        if token:
            headers.setdefault("Authorization", f"Bearer {token}")
        return headers


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables."""

    openai_api_key: Optional[str] = Field(default=None, env="OPENAI_API_KEY")
    openai_model: str = "gpt-5"
    orchestrator_debug: bool = False

    mcp_timeout_seconds: float = 10.0
    mcp_base_url: Optional[AnyHttpUrl] = Field(
        default="http://127.0.0.1:8001",
        env="MCP_BASE_URL",
    )
    mcp_servers: List[MCPServerConfig] = Field(default_factory=list, env="MCP_SERVERS")

    # Supabase configuration
    supabase_url: Optional[str] = Field(
        default=None,
        env="SUPABASE_URL",
        description="Supabase project URL (e.g., https://[PROJECT-REF].supabase.co)",
    )
    supabase_service_role_key: Optional[str] = Field(
        default=None,
        env="SUPABASE_SERVICE_ROLE_KEY",
        description="Supabase service role key (for server-side operations)",
    )
    supabase_jwt_secret: Optional[str] = Field(
        default=None,
        env="SUPABASE_JWT_SECRET",
        description="Supabase JWT secret (Project Settings > API > JWT Secret) for verifying access tokens",
    )

    tool_catalog_path: str = "tool_catalog"
    logs_dir: str = Field(default="logs", env="LOGS_DIR")
    stream_raw_sse: bool = Field(default=True, env="STREAM_RAW_SSE")
    orchestrator_log_max_bytes: int = Field(default=5_000_000, env="ORCHESTRATOR_LOG_MAX_BYTES")
    orchestrator_log_backup_count: int = Field(default=5, env="ORCHESTRATOR_LOG_BACKUP_COUNT")

    @field_validator("mcp_base_url", mode="before")
    @classmethod
    def validate_mcp_url(cls, value: Optional[str]) -> str:
        """Handle empty strings from environment."""
        if value in ("", None):
            return "http://127.0.0.1:8001"
        return value

    @field_validator("mcp_servers", mode="before")
    @classmethod
    def parse_server_list(cls, value):
        if not value:
            return []
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError("MCP_SERVERS must be valid JSON") from exc
        return value

    @model_validator(mode="after")
    def ensure_default_server(self) -> "Settings":
        """Ensure at least one server configuration is present."""
        if not self.mcp_servers:
            self.mcp_servers = [
                MCPServerConfig(
                    name="default",
                    base_url=self.mcp_base_url,
                )
            ]
        return self

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8-sig"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()

