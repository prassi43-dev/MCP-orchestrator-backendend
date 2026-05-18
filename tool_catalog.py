"""Tool catalog caching utilities."""
from __future__ import annotations

import json
import logging
from datetime import datetime
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from models import ToolDefinition

logger = logging.getLogger(__name__)


class ToolCatalogCache:
    """Handles persistence of tool catalogs per MCP server."""

    def __init__(self, base_path: Path) -> None:
        self._base_path = base_path

    def _resolve_path(self, server: str) -> Path:
        """Return the file path for a server's cache."""
        if self._base_path.suffix == ".json":
            if server == "default":
                return self._base_path
            stem = self._base_path.stem
            return self._base_path.with_name(f"{stem}_{server}.json")
        return self._base_path.joinpath(f"{server}.json")

    def load(self, server: str) -> Optional[List[ToolDefinition]]:
        """Load cached tools for a server."""
        path = self._resolve_path(server)
        try:
            raw_text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            logger.info("Tool catalog cache not found for server %s at %s", server, path)
            return None

        try:
            payload = json.loads(raw_text)
            tools_blob = payload.get("tools")
            if isinstance(tools_blob, list):
                return [
                    tool_definition_from_cache_entry(server, entry)
                    for entry in tools_blob
                ]

            # Backwards compatibility: old format was a list of OpenAI tools
            if isinstance(payload, list):
                logger.info(
                    "Detected legacy tool catalog format for %s, converting on next save",
                    server,
                )
                return [
                    tool_definition_from_cache_entry(
                        server,
                        {
                            "type": entry.get("type"),
                            "function": entry.get("function"),
                            "server_tool_name": entry.get("function", {}).get("name"),
                        },
                    )
                    for entry in payload
                ]
            logger.warning("Tool catalog cache %s for %s missing 'tools' list", path, server)
            return None
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "Invalid tool catalog cache %s for %s: %s", path, server, exc
            )
            return None

    def save(
        self,
        server: str,
        tools: Iterable[ToolDefinition],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Persist a server's catalog to disk with metadata."""
        path = self._resolve_path(server)
        path.parent.mkdir(parents=True, exist_ok=True)

        tools_list = list(tools)
        record = {
            "server": server,
            "metadata": {
                "tool_count": len(tools_list),
                "saved_at": datetime.utcnow().isoformat(),
                **(metadata or {}),
            },
            "tools": [
                tool_definition_to_cache_entry(tool) for tool in tools_list
            ],
        }

        json_content = json.dumps(record, indent=2, ensure_ascii=False)
        temp_suffix = path.suffix + ".tmp" if path.suffix else ".tmp"
        temp_path = path.with_suffix(temp_suffix)
        temp_path.write_text(json_content, encoding="utf-8")
        temp_path.replace(path)
        logger.info(
            "Saved %d tool(s) for server %s to %s", len(tools_list), server, path
        )


def tool_definition_from_cache_entry(
    server: str, entry: Dict[str, Any]
) -> ToolDefinition:
    """Convert a cache entry back into ToolDefinition."""
    if entry.get("type") != "function":
        raise ValueError("Only function tools are supported")

    function_block = entry.get("function") or {}
    name = function_block.get("name")
    if not isinstance(name, str):
        raise ValueError("Tool entry missing string 'name'")

    description = (function_block.get("description") or name).strip()
    parameters = function_block.get("parameters") or {}
    if not isinstance(parameters, dict):
        raise ValueError(f"Tool {name} parameters must be a JSON object")

    server_tool_name = entry.get("server_tool_name") or name

    return ToolDefinition(
        name=name,
        description=description,
        parameters=parameters,
        server=server,
        server_tool_name=server_tool_name,
    )


def tool_definition_to_cache_entry(tool: ToolDefinition) -> Dict[str, Any]:
    """Convert ToolDefinition to cached representation with metadata."""
    base = tool.to_openai_tool()
    base["server_tool_name"] = tool.server_tool_name
    return base


_ALIAS_SANITIZE = re.compile(r"[^a-zA-Z0-9_-]")


def build_tool_alias(server: str, tool_name: str) -> str:
    """Generate a deterministic alias that meets OpenAI tool name constraints."""

    def sanitize(component: str) -> str:
        cleaned = _ALIAS_SANITIZE.sub("_", component.strip())
        cleaned = cleaned.strip("_") or "tool"
        return cleaned[:48]

    alias = f"{sanitize(server)}__{sanitize(tool_name)}"
    return alias[:64]

