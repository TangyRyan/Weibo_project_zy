import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

import requests
from requests import Response, Session
from urllib.parse import quote_plus

CHINA_TZ = timezone.utc  # fallback; overwritten on module load

try:
    from spider.crawler_core import CHINA_TZ as _CHINA_TZ  # type: ignore
except Exception:  # pragma: no cover - fallback when core is unavailable
    _CHINA_TZ = timezone.utc

CHINA_TZ = _CHINA_TZ

logger = logging.getLogger(__name__)

AICARD_URL = "https://ai.s.weibo.com/api/wis/show.json"
DEFAULT_TIMEOUT = 10
MAX_RETRIES = 3

DEFAULT_HEADERS: Dict[str, str] = {
    "Pragma": "no-cache",
    "Priority": "u=1, i",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded",
}

DEFAULT_PAYLOAD: Dict[str, Any] = {
    "content_type": "loop",
    "request_time": 0,
    "search_source": "default_init",
    "sid": "pc_search",
    "vstyle": 1,
    "cot": 1,
    "loop_num": 1,
}


class AICardError(RuntimeError):
    """Raised when fetching the AI card payload fails."""


@dataclass(slots=True)
class AICardResult:
    query: str
    response: Dict[str, Any]
    status_code: int
    fetched_at: datetime

    def msg(self) -> str:
        return str(self.response.get("msg") or "")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "status_code": self.status_code,
            "fetched_at": self.fetched_at.isoformat(timespec="seconds"),
            "response": self.response,
        }


def _ensure_query(query: str) -> str:
    query = query.strip()
    if not query:
        raise ValueError("query must be non-empty")
    if "%" in query:
        return query
    return quote_plus(query)


def _build_headers(extra_headers: Optional[Mapping[str, str]]) -> Dict[str, str]:
    headers = dict(DEFAULT_HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    cookie = os.getenv("WEIBO_AICARD_COOKIE") or os.getenv("WEIBO_COOKIE")
    if cookie:
        headers.setdefault("Cookie", cookie)
    return headers


def _build_payload(
    query: str, extra_payload: Optional[Mapping[str, Any]], request_id: Optional[str]
) -> Dict[str, Any]:
    payload = dict(DEFAULT_PAYLOAD)
    payload["query"] = _ensure_query(query)
    payload["request_id"] = request_id or str(int(time.time()))
    if extra_payload:
        payload.update(extra_payload)
    return payload


def _send_request(
    session: Session,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int,
) -> Response:
    encoded_items = []
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, bool):
            value = int(value)
        key_str = quote_plus(str(key))
        if isinstance(value, (int, float)):
            value_str = str(value)
        else:
            value_str = str(value)
            if "%" not in value_str:
                value_str = quote_plus(value_str)
        encoded_items.append(f"{key_str}={value_str}")
    encoded_payload = "&".join(encoded_items)
    return session.post(
        AICARD_URL,
        headers=headers,
        data=encoded_payload,
        timeout=timeout,
    )


def fetch_ai_card(
    query: str,
    *,
    session: Optional[Session] = None,
    extra_headers: Optional[Mapping[str, str]] = None,
    extra_payload: Optional[Mapping[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = MAX_RETRIES,
    request_id: Optional[str] = None,
) -> AICardResult:
    """Fetch the AI Card payload for a given query."""
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    if retries < 0:
        raise ValueError("retries must be non-negative")

    headers = _build_headers(extra_headers)
    payload = _build_payload(query, extra_payload, request_id)
    owns_session = session is None
    sess = session or requests.Session()

    try:
        last_error: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                response = _send_request(sess, payload, headers, timeout)
                status = response.status_code
                if status >= 500:
                    logger.warning(
                        "AI Card request failed with %s on attempt %s/%s",
                        status,
                        attempt + 1,
                        retries + 1,
                    )
                    last_error = AICardError(f"server error {status}")
                    time.sleep(min(2 ** attempt, 5))
                    continue
                response.raise_for_status()
                break
            except requests.RequestException as exc:
                last_error = exc
                logger.warning(
                    "AI Card request exception on attempt %s/%s: %s",
                    attempt + 1,
                    retries + 1,
                    exc,
                )
                if attempt == retries:
                    raise
                time.sleep(min(2 ** attempt, 5))
        else:
            raise AICardError(str(last_error))
    finally:
        if owns_session:
            sess.close()

    try:
        payload_json = response.json()
    except json.JSONDecodeError as exc:
        raise AICardError(f"failed to decode JSON: {exc}") from exc

    fetched_at = datetime.now(tz=CHINA_TZ)
    return AICardResult(
        query=query,
        response=payload_json,
        status_code=response.status_code,
        fetched_at=fetched_at,
    )
