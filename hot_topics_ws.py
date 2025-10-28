"""WebSocket service for streaming Weibo hot topic snapshots to frontend clients."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import errno
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypedDict
from urllib.parse import parse_qs, urlparse

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError
from websockets.server import WebSocketServerProtocol

from spider.crawler_core import CHINA_TZ

HOT_TOPICS_ROOT = Path("data") / "hot_topics" / "hourly"
DEFAULT_REFRESH_SECONDS = 10.0
LOGGER = logging.getLogger(__name__)


class HotTopicItem(TypedDict, total=False):
    """Serialized topic structure shared with frontend clients."""

    rank: int
    title: str
    category: str
    description: str
    url: str
    hot: int
    ads: bool
    readCount: int
    discussCount: int
    origin: int


@dataclass(frozen=True)
class SnapshotReference:
    """Reference to a single hourly snapshot stored on disk."""

    date: str
    hour: int
    path: Path
    mtime_ns: int

    @property
    def version(self) -> Tuple[str, int]:
        return (str(self.path), self.mtime_ns)


@dataclass(frozen=True)
class HotTopicsSnapshot:
    """Loaded snapshot payload ready to be serialized for clients."""

    ref: SnapshotReference
    generated_at: datetime
    topics: List[HotTopicItem]

    def to_payload(self, *, limit: Optional[int] = None, message_type: str = "snapshot") -> Dict[str, Any]:
        items = self.topics[:limit] if limit is not None and limit >= 0 else self.topics
        return {
            "type": message_type,
            "date": self.ref.date,
            "hour": self.ref.hour,
            "generated_at": self.generated_at.isoformat(timespec="seconds"),
            "source_path": str(self.ref.path),
            "topics": items,
            "total": len(self.topics),
        }


class HotTopicsRepository:
    """Encapsulate snapshot discovery and loading logic."""

    def __init__(self, root: Path = HOT_TOPICS_ROOT) -> None:
        self.root = root

    def get_snapshot(self, *, date: Optional[str] = None, hour: Optional[int] = None) -> Optional[HotTopicsSnapshot]:
        ref = self._resolve_reference(date=date, hour=hour)
        if not ref:
            return None
        try:
            data = json.loads(ref.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            LOGGER.warning("Snapshot file disappeared during read: %s", ref.path)
            return None
        except json.JSONDecodeError as exc:
            LOGGER.error("Malformed JSON in snapshot %s: %s", ref.path, exc)
            return None

        topics: List[HotTopicItem] = []
        for index, item in enumerate(self._coerce_topic_list(data), start=1):
            entry: HotTopicItem = {"rank": index}
            entry.update(item)
            topics.append(entry)

        generated_at = datetime.fromtimestamp(ref.mtime_ns / 1_000_000_000, tz=CHINA_TZ)
        return HotTopicsSnapshot(ref=ref, generated_at=generated_at, topics=topics)

    def _resolve_reference(self, *, date: Optional[str], hour: Optional[int]) -> Optional[SnapshotReference]:
        if date:
            return self._resolve_for_date(date, hour)
        return self._resolve_latest()

    def _resolve_for_date(self, date: str, hour: Optional[int]) -> Optional[SnapshotReference]:
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            LOGGER.warning("Ignoring invalid date argument: %s", date)
            return None

        target_dir = self.root / date
        if not target_dir.exists():
            LOGGER.info("No snapshots for date %s", date)
            return None

        if hour is not None:
            if hour < 0 or hour > 23:
                LOGGER.warning("Ignoring invalid hour argument: %s", hour)
                return None
            path = target_dir / f"{hour:02d}.json"
            if not path.exists():
                LOGGER.info("Snapshot %s %02d missing", date, hour)
                return None
            return self._make_reference(date, hour, path)

        # hour unspecified: choose the latest existing file for that date.
        return self._latest_in_directory(date, target_dir)

    def _resolve_latest(self) -> Optional[SnapshotReference]:
        if not self.root.exists():
            LOGGER.debug("Hot topics root %s does not exist yet", self.root)
            return None

        candidates: List[SnapshotReference] = []
        for path in self.root.iterdir():
            if not path.is_dir():
                continue
            ref = self._latest_in_directory(path.name, path)
            if ref:
                candidates.append(ref)

        if not candidates:
            return None

        candidates.sort(key=lambda item: (item.date, item.hour), reverse=True)
        return candidates[0]

    def _latest_in_directory(self, date: str, directory: Path) -> Optional[SnapshotReference]:
        refs: List[SnapshotReference] = []
        for file in directory.iterdir():
            if not file.is_file() or file.suffix.lower() != ".json":
                continue
            try:
                hour = int(file.stem)
            except ValueError:
                continue
            refs.append(self._make_reference(date, hour, file))
        if not refs:
            return None
        refs.sort(key=lambda item: item.hour, reverse=True)
        return refs[0]

    def _make_reference(self, date: str, hour: int, path: Path) -> SnapshotReference:
        stat = path.stat()
        return SnapshotReference(date=date, hour=hour, path=path, mtime_ns=stat.st_mtime_ns)

    @staticmethod
    def _coerce_topic_list(data: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(data, list):
            return [dict(item) for item in data if isinstance(item, dict)]
        LOGGER.warning("Snapshot payload is not a list: %r", type(data))
        return []


@dataclass
class ClientSubscription:
    websocket: WebSocketServerProtocol
    limit: Optional[int]


class HotTopicsWebSocketService:
    """Manage client connections and broadcast updates when new snapshots arrive."""

    def __init__(
        self,
        repository: Optional[HotTopicsRepository] = None,
        *,
        refresh_interval: float = DEFAULT_REFRESH_SECONDS,
    ) -> None:
        self.repository = repository or HotTopicsRepository()
        self.refresh_interval = max(refresh_interval, 1.0)
        self._clients: Dict[WebSocketServerProtocol, ClientSubscription] = {}
        self._broadcast_task: Optional[asyncio.Task] = None
        self._last_version: Optional[Tuple[str, int]] = None
        self._shutdown_event = asyncio.Event()

    async def start(self, host: str = "0.0.0.0", port: int = 8765) -> None:
        LOGGER.info("Starting hot topics WebSocket server on %s:%s", host, port)
        serve_kwargs: Dict[str, Any] = {}
        if self._can_try_reuse_port():
            serve_kwargs["reuse_port"] = True
        try:
            await self._run_server(host, port, serve_kwargs)
        except OSError as exc:
            if exc.errno == errno.EADDRINUSE:
                LOGGER.error(
                    "WebSocket port %s is already in use. Stop the previous server or choose another port.",
                    port,
                )
            raise
        except ValueError as exc:
            if "reuse_port" in str(exc):
                LOGGER.warning("reuse_port not supported on this platform; retrying without it")
                serve_kwargs.pop("reuse_port", None)
                await self._run_server(host, port, serve_kwargs)
            else:
                raise

    async def _run_server(self, host: str, port: int, serve_kwargs: Dict[str, Any]) -> None:
        async with websockets.serve(self._client_handler, host, port, **serve_kwargs):
            self._broadcast_task = asyncio.create_task(self._broadcast_loop(), name="hot-topics-broadcast")
            await self._shutdown_event.wait()

    @staticmethod
    def _can_try_reuse_port() -> bool:
        # Windows prior to 10 build 17063 and some Python builds do not support reuse_port.
        return hasattr(asyncio, "start_server") and hasattr(asyncio, "get_running_loop")

    async def stop(self) -> None:
        LOGGER.info("Stopping hot topics WebSocket server")
        self._shutdown_event.set()
        if self._broadcast_task:
            self._broadcast_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._broadcast_task

    async def _client_handler(
        self,
        websocket: WebSocketServerProtocol,
        path: Optional[str] = None,
    ) -> None:
        actual_path = path if path is not None else getattr(websocket, "path", "/")
        subscription = self._register_client(websocket, actual_path)
        try:
            await self._send_initial_snapshot(subscription)
            async for message in websocket:
                await self._handle_client_message(subscription, message)
        except ConnectionClosed:
            LOGGER.info("Client disconnected: %s", websocket.remote_address)
        finally:
            self._clients.pop(websocket, None)

    def _register_client(self, websocket: WebSocketServerProtocol, path: Optional[str]) -> ClientSubscription:
        params = parse_qs(urlparse(path or "/").query)
        limit = self._parse_limit(params.get("limit", [None])[0])
        subscription = ClientSubscription(websocket=websocket, limit=limit)
        self._clients[websocket] = subscription
        LOGGER.info(
            "Client connected from %s with limit=%s", websocket.remote_address, limit if limit is not None else "default"
        )
        return subscription

    @staticmethod
    def _parse_limit(raw: Optional[str]) -> Optional[int]:
        if not raw:
            return None
        try:
            value = int(raw)
        except ValueError:
            LOGGER.warning("Ignoring invalid limit value: %s", raw)
            return None
        return value if value >= 0 else None

    async def _send_initial_snapshot(self, subscription: ClientSubscription) -> None:
        snapshot = await asyncio.to_thread(self.repository.get_snapshot)
        if not snapshot:
            await subscription.websocket.send(json.dumps({"type": "empty", "message": "No hot topics available"}))
            return
        payload = snapshot.to_payload(limit=subscription.limit, message_type="snapshot")
        await self._send(subscription.websocket, payload)

    async def _handle_client_message(self, subscription: ClientSubscription, message: str) -> None:
        message = message.strip()
        if not message:
            return
        if message.lower() == "ping":
            await self._send(subscription.websocket, {"type": "pong"})
            return
        try:
            request = json.loads(message)
        except json.JSONDecodeError:
            await self._send(subscription.websocket, {"type": "error", "message": "Invalid JSON message"})
            return
        action = request.get("action")
        if action == "set_limit":
            limit = self._parse_limit(str(request.get("value", "")))
            subscription.limit = limit
            await self._send(subscription.websocket, {"type": "ack", "message": f"limit set to {limit}"})
        elif action == "request_snapshot":
            await self._handle_snapshot_request(subscription, request)
        else:
            await self._send(subscription.websocket, {"type": "error", "message": "Unknown action"})

    async def _handle_snapshot_request(self, subscription: ClientSubscription, request: Dict[str, Any]) -> None:
        date = request.get("date")
        hour_value = request.get("hour")
        hour = None
        if hour_value is not None:
            try:
                hour = int(hour_value)
            except (TypeError, ValueError):
                await self._send(subscription.websocket, {"type": "error", "message": "Invalid hour parameter"})
                return
        snapshot = await asyncio.to_thread(self.repository.get_snapshot, date=date, hour=hour)
        if not snapshot:
            await self._send(subscription.websocket, {"type": "empty", "message": "Snapshot not found"})
            return
        payload = snapshot.to_payload(limit=subscription.limit, message_type="snapshot")
        await self._send(subscription.websocket, payload)

    async def _broadcast_loop(self) -> None:
        while not self._shutdown_event.is_set():
            snapshot = await asyncio.to_thread(self.repository.get_snapshot)
            if snapshot and snapshot.ref.version != self._last_version:
                self._last_version = snapshot.ref.version
                await self._broadcast(snapshot)
            await asyncio.sleep(self.refresh_interval)

    async def _broadcast(self, snapshot: HotTopicsSnapshot) -> None:
        if not self._clients:
            return
        payloads: Dict[WebSocketServerProtocol, str] = {}
        for websocket, subscription in list(self._clients.items()):
            payload_dict = snapshot.to_payload(limit=subscription.limit, message_type="update")
            payloads[websocket] = json.dumps(payload_dict, ensure_ascii=False)
        for websocket, payload in payloads.items():
            try:
                await websocket.send(payload)
            except ConnectionClosedError:
                LOGGER.info("Removing closed client %s", websocket.remote_address)
                self._clients.pop(websocket, None)

    @staticmethod
    async def _send(websocket: WebSocketServerProtocol, payload: Dict[str, Any]) -> None:
        await websocket.send(json.dumps(payload, ensure_ascii=False))


async def start_hot_topics_ws(
    host: str = "0.0.0.0", port: int = 8765, *, refresh_interval: float = DEFAULT_REFRESH_SECONDS
) -> None:
    """Entry point for embedding inside other scripts."""
    service = HotTopicsWebSocketService(refresh_interval=refresh_interval)
    await service.start(host=host, port=port)


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")


def _env_value(name: str, default: Any, caster):
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return caster(raw)
    except (TypeError, ValueError):
        LOGGER.warning("Invalid environment value for %s=%s, using default %s", name, raw, default)
        return default


def parse_args() -> argparse.Namespace:
    default_host = os.getenv("HOT_TOPICS_WS_HOST", "0.0.0.0")
    default_port = _env_value("HOT_TOPICS_WS_PORT", 8765, int)
    default_refresh = _env_value("HOT_TOPICS_WS_REFRESH", DEFAULT_REFRESH_SECONDS, float)
    default_log = os.getenv("HOT_TOPICS_WS_LOG", "INFO")
    parser = argparse.ArgumentParser(description="Weibo hot topics WebSocket service")
    parser.add_argument(
        "--host",
        default=default_host,
        help="Host/IP to bind (default: 0.0.0.0 or HOT_TOPICS_WS_HOST env)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=default_port,
        help="Port to bind (default: 8765 or HOT_TOPICS_WS_PORT env)",
    )
    parser.add_argument(
        "--refresh",
        type=float,
        default=default_refresh,
        help=f"Polling interval in seconds (default: {DEFAULT_REFRESH_SECONDS} or HOT_TOPICS_WS_REFRESH env)",
    )
    parser.add_argument("--log-level", default=default_log, help="Logging level")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(getattr(logging, args.log_level.upper(), logging.INFO))
    try:
        asyncio.run(start_hot_topics_ws(host=args.host, port=args.port, refresh_interval=args.refresh))
    except KeyboardInterrupt:
        LOGGER.info("Received shutdown signal, exiting")


if __name__ == "__main__":
    main()
