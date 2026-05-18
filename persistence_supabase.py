"""Supabase-native persistence layer (no SQLAlchemy)."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from supabase import create_client, Client

logger = logging.getLogger(__name__)


class SupabaseDatabase:
    """Supabase database client (no SQLAlchemy)."""

    def __init__(self, supabase_url: str | None = None, supabase_key: str | None = None):
        """
        Initialize Supabase client.
        
        Args:
            supabase_url: Supabase project URL (e.g., https://[PROJECT-REF].supabase.co)
            supabase_key: Supabase service role key (for server-side operations)
        """
        if supabase_url is None:
            supabase_url = os.getenv("SUPABASE_URL")
        if supabase_key is None:
            supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        
        if not supabase_url or not supabase_key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables must be set. "
                "Get these from your Supabase project settings."
            )
        
        self.client: Client = create_client(supabase_url, supabase_key)
        self._initialized = False

    def initialize(self) -> None:
        """Verify connection and ensure tables exist."""
        if not self._initialized:
            try:
                # Test connection by querying tenants table
                self.client.table("tenants").select("tenant_id").limit(1).execute()
                logger.info("Supabase connection verified")
            except Exception as exc:
                error_msg = str(exc)
                if "PGRST205" in error_msg or "Could not find the table" in error_msg:
                    logger.error(
                        "Tables do not exist in Supabase. Please run the SQL script in create_tables.sql "
                        "in your Supabase SQL Editor to create the required tables."
                    )
                    logger.error("Error details: %s", exc)
                    # Don't raise - let the app start, but operations will fail until tables are created
                else:
                    logger.warning("Could not verify Supabase connection: %s", exc)
            self._initialized = True

    def close(self) -> None:
        """Close database connections (Supabase client is stateless)."""
        logger.info("Supabase client closed")


# Global database instance
_db: Optional[SupabaseDatabase] = None


def get_database() -> SupabaseDatabase:
    """Get the global database instance."""
    global _db
    if _db is None:
        raise RuntimeError("Database not initialized. Call init_database() first.")
    return _db


def init_database(
    supabase_url: str | None = None, supabase_key: str | None = None
) -> SupabaseDatabase:
    """Initialize the global database instance."""
    global _db
    _db = SupabaseDatabase(supabase_url, supabase_key)
    _db.initialize()
    return _db

