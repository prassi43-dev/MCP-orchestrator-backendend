"""Streaming event bus + trace/log writers."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

logger = logging.getLogger(__name__)


def iso_ts() -> str:
    return datetime.utcnow().isoformat() + "Z"


def redact_headers(headers: Dict[str, str]) -> Dict[str, str]:
    redacted = {}
    for key, value in headers.items():
        norm = key.lower()
        if "authorization" in norm or "token" in norm or "secret" in norm:
            redacted[key] = "***REDACTED***"
        else:
            redacted[key] = value
    return redacted


@dataclass
class Event:
    """Canonical telemetry event."""

    event: str
    session_id: str
    correlation_id: str
    source: str
    timestamp: str = field(default_factory=iso_ts)
    payload: Dict[str, Any] = field(default_factory=dict)
    human_status: Optional[str] = None
    file_ref: Optional[str] = None
    tenant_id: Optional[str] = None  # Tenant context
    server_id: Optional[str] = None  # Server context

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        # drop None fields
        return {k: v for k, v in data.items() if v is not None}


class SessionTraceWriter:
    """Append-only writer for per-session trace files."""

    def __init__(self, session_id: str, logs_dir: Path) -> None:
        logs_dir.mkdir(parents=True, exist_ok=True)
        self._path = logs_dir / f"session_{session_id}_trace.json"
        self._file = self._path.open("a", encoding="utf-8")
        self._lock = asyncio.Lock()

    @property
    def path(self) -> Path:
        return self._path

    async def append(self, event: Event) -> None:
        line = json.dumps(event.to_dict(), ensure_ascii=False)
        async with self._lock:
            self._file.write(line + "\n")
            self._file.flush()

    async def close(self) -> None:
        async with self._lock:
            self._file.close()


class SessionState:
    """Holds runtime state for a session stream."""

    def __init__(self, session_id: str, logs_dir: Path) -> None:
        self.session_id = session_id
        self.trace_writer = SessionTraceWriter(session_id, logs_dir)
        self.subscribers: List["SessionSubscription"] = []
        self.correlation_id: str = ""


class SessionSubscription:
    """Represents a subscriber to session events (SSE/WS)."""

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=256)
        self._active = True

    async def publish(self, event: Event) -> None:
        if not self._active:
            return
        try:
            await self.queue.put(event)
        except asyncio.CancelledError:
            self._active = False
            raise

    async def events(self) -> AsyncIterator[Event]:
        try:
            while self._active:
                event = await self.queue.get()
                yield event
        except asyncio.CancelledError:
            self._active = False
            raise

    def close(self) -> None:
        self._active = False
        # best-effort put sentinel
        try:
            self.queue.put_nowait(
                Event(
                    event="session.end",
                    session_id=self.session_id,
                    correlation_id="",
                    source="telemetry",
                    payload={"reason": "subscription_closed"},
                )
            )
        except asyncio.QueueFull:
            pass


class EventHub:
    """Central event dispatcher + log writer."""

    def __init__(self, logs_dir: str, stream_raw_sse: bool = True) -> None:
        self._logs_dir = Path(logs_dir)
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        self._stream_raw_sse = stream_raw_sse
        self._sessions: Dict[str, SessionState] = {}
        self._lock = asyncio.Lock()

    def logs_dir(self) -> Path:
        return self._logs_dir
    
    def stream_raw_sse(self) -> bool:
        return self._stream_raw_sse

    async def _get_session(self, session_id: str) -> SessionState:
        async with self._lock:
            state = self._sessions.get(session_id)
            if state is None:
                state = SessionState(session_id, self._logs_dir)
                self._sessions[session_id] = state
            return state

    async def register_session(
        self,
        session_id: str,
        correlation_id: str,
        message_preview: str | None = None,
    ) -> None:
        state = await self._get_session(session_id)
        state.correlation_id = correlation_id
        await self.publish(
            Event(
                event="session.start",
                session_id=session_id,
                correlation_id=correlation_id,
                source="orchestrator",
                payload={
                    "message": "Session registered",
                    "preview": message_preview,
                },
            )
        )

    async def publish(self, event: Event) -> None:
        state = await self._get_session(event.session_id)
        await state.trace_writer.append(event)

        # fan-out to subscribers
        stale: List[SessionSubscription] = []
        for sub in state.subscribers:
            try:
                await sub.publish(event)
            except Exception:
                logger.debug(
                    "Subscriber publish failed for session %s", event.session_id
                )
                stale.append(sub)
        for sub in stale:
            try:
                state.subscribers.remove(sub)
            except ValueError:
                pass

        # auto-clean on session end
        if event.event == "session.end":
            await state.trace_writer.close()
            async with self._lock:
                self._sessions.pop(event.session_id, None)

    async def subscribe(self, session_id: str) -> SessionSubscription:
        state = await self._get_session(session_id)
        sub = SessionSubscription(session_id)
        state.subscribers.append(sub)
        return sub

    async def write_mcp_sse(self, server: str, payload: str) -> str:
        """Append raw SSE line for a server. Returns file reference."""
        path = self._logs_dir / f"mcp_{server}_sse.log"
        timestamp = iso_ts()
        with path.open("a", encoding="utf-8") as file:
            file.write(f"[{timestamp}] {payload}\n")
        return str(path.relative_to(self._logs_dir.parent))

    def write_mcp_list_tools(self, server: str, payload: Dict[str, Any]) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        path = self._logs_dir / f"mcp_{server}_list_tools_{timestamp}.json"
        with path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2)
        return str(path.relative_to(self._logs_dir.parent))

    def write_tool_payload(
        self,
        server: str,
        tool_name: str,
        payload: Any,
        transport: str,
    ) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        safe_server = _safe_filename(server)
        safe_tool = _safe_filename(tool_name)
        path = (
            self._logs_dir
            / f"tool_{safe_server}_{safe_tool}_{transport}_{timestamp}.json"
        )
        with path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2, ensure_ascii=False, default=str)
        return str(path.relative_to(self._logs_dir.parent))


def ensure_orchestrator_file_logger(
    logs_dir: Path, max_bytes: int, backup_count: int
) -> None:
    """Attach RotatingFileHandler for orchestrator debugging."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    target = logs_dir / "orchestrator.debug.log"
    handler_exists = any(
        isinstance(h, logging.Handler) and getattr(h, "baseFilename", "") == str(target)
        for h in logging.getLogger().handlers
    )
    if handler_exists:
        return
    from logging.handlers import RotatingFileHandler

    handler = RotatingFileHandler(
        target, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    logging.getLogger().addHandler(handler)


_FILENAME_SANITIZE = re.compile(r"[^a-zA-Z0-9_.-]")


def _safe_filename(value: str) -> str:
    if not value:
        return "item"
    safe = _FILENAME_SANITIZE.sub("_", value)
    return safe[:80] or "item"


