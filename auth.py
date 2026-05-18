"""Supabase JWT authentication for FastAPI."""
from __future__ import annotations

import logging
from typing import Annotated

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from repository_supabase import TenantRepository

logger = logging.getLogger(__name__)

security = HTTPBearer(auto_error=False)


class CurrentUser:
    """Authenticated user with resolved tenant."""

    def __init__(self, auth_user_id: str, email: str | None, tenant_id: str, tenant_name: str | None = None):
        self.auth_user_id = auth_user_id
        self.email = email
        self.tenant_id = tenant_id
        self.tenant_name = tenant_name


def _get_mock_tenant_id() -> str:
    return "00000000-0000-0000-0000-000000000000"


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)] = None,
) -> CurrentUser:
    """Mock auth: always return test user without checking token."""
    tenant_id = _get_mock_tenant_id()
    repo = TenantRepository()
    repo.create_or_update_tenant(tenant_id, "Default Workspace")
    return CurrentUser(
        auth_user_id="test-user-id",
        email="test@example.com",
        tenant_id=tenant_id,
        tenant_name="Default Workspace",
    )


async def require_jwt(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)] = None,
) -> tuple[str, str | None]:
    return ("test-user-id", "test@example.com")


async def get_current_user_optional(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)] = None,
) -> CurrentUser | None:
    return await get_current_user()
