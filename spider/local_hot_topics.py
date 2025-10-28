"""Fetch and persist local Weibo hot topics without relying on Playwright."""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import requests
from bs4 import BeautifulSoup
from lxml import etree
from urllib.parse import quote

logger = logging.getLogger(__name__)

BASE_URL = "https://s.weibo.com"
SUMMARY_ENDPOINT = "/top/summary"
HOT_SEARCH_API = "https://weibo.com/ajax/side/hotSearch"
DETAIL_URL = "https://m.s.weibo.com/topic/detail?q={query}"
REQUEST_TIMEOUT = 10
DEFAULT_LIMIT = 50
LOCAL_ARCHIVE_ROOT = Path(__file__).resolve().parents[1] / "data" / "hot_topics" / "local"


def fetch_latest_topics_local(limit: int = DEFAULT_LIMIT, *, persist: bool = True) -> List[Dict[str, Any]]:
    """Fetch the current Weibo hot topics and optionally persist a snapshot."""
    limit = max(limit, 0)
    session = requests.Session()
    try:
        topics = _fetch_from_hot_search_api(limit, session)
        if not topics:
            logger.info("Hot search API yielded no data; falling back to HTML parse")
            topics = _fetch_from_html(limit, session)
            if not topics:
                logger.warning("Local crawler produced no topics from API or HTML fallback")
                return []

        _enrich_topics(topics, session)
    finally:
        session.close()

    if persist:
        try:
            snapshot_path = persist_topics_snapshot(topics)
            logger.info("Saved local snapshot to %s", snapshot_path)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Failed to persist local snapshot: %s", exc, exc_info=True)
    return topics


def _fetch_from_hot_search_api(limit: int, session: requests.Session) -> List[Dict[str, Any]]:
    headers = _build_headers()
    try:
        response = session.get(HOT_SEARCH_API, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.error("Hot search API request failed: %s", exc)
        return []
    except ValueError as exc:
        logger.error("Hot search API returned invalid JSON: %s", exc)
        return []

    data = payload.get("data", {})
    realtime = data.get("realtime") or []
    if not isinstance(realtime, list):
        logger.error("Unexpected hot search payload format: %s", type(realtime))
        return []

    normalized = [_normalize_api_topic(item) for item in realtime]
    return list(_take_unique(normalized, limit))


def _fetch_from_html(limit: int, session: requests.Session) -> List[Dict[str, Any]]:
    html = fetch_hot_topics_html(session)
    if not html:
        logger.warning("Local crawler received empty HTML payload")
        return []

    raw_topics = parse_hot_topics(html)
    if not raw_topics:
        logger.warning("Local crawler found no topics in parsed HTML")
        return []

    normalized = [_normalize_parsed_topic(topic) for topic in raw_topics]
    return list(_take_unique(normalized, limit))


def fetch_hot_topics_html(session: requests.Session) -> str:
    """Retrieve the hot topics HTML page."""
    headers = _build_headers()
    url = f"{BASE_URL}{SUMMARY_ENDPOINT}"
    try:
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Failed to fetch %s: %s", url, exc)
        return ""

    response.encoding = response.apparent_encoding or response.encoding
    return response.text or ""


def parse_hot_topics(html: str) -> List[Dict[str, Any]]:
    """Parse the hot topics table from the provided HTML string."""
    doc = etree.HTML(html)
    if doc is None:
        logger.error("Unable to parse HTML into DOM")
        return []

    topics: List[Dict[str, Any]] = []
    rows = doc.xpath('//tr[position()>1]')
    for row in rows:
        link = row.xpath('.//td[@class="td-02"]/a[not(contains(@href,"javascript:void(0);"))]')
        if not link:
            continue
        title = "".join(link[0].xpath(".//text()")).strip()
        href = link[0].get("href", "").strip()
        if not title or not href:
            continue

        hot_text = "".join(row.xpath('.//td[@class="td-02"]/span/text()')).strip()
        desc_text = "".join(row.xpath('.//td[@class="td-03"]//text()')).strip()
        hot_value, category = _split_hot_and_category(hot_text)

        topics.append(
            {
                "title": title,
                "url": _ensure_absolute_url(href),
                "hot": hot_value,
                "category": category,
                "description": desc_text or None,
            }
        )
    return topics


def persist_topics_snapshot(topics: List[Dict[str, Any]]) -> Path:
    """Persist the latest topics under data/hot_topics/local/<date>/<hour>.json."""
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    target_dir = LOCAL_ARCHIVE_ROOT / date_str
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{hour_str}.json"
    payload = json.dumps(topics, ensure_ascii=False, indent=2)
    path.write_text(payload, encoding="utf-8")
    return path


def _take_unique(topics: Iterable[Dict[str, Any]], limit: int) -> Iterable[Dict[str, Any]]:
    seen: set[str] = set()
    count = 0
    for topic in topics:
        title = topic.get("title", "").strip()
        if not title or title in seen:
            continue
        seen.add(title)
        yield topic
        count += 1
        if limit and count >= limit:
            break


def _normalize_api_topic(item: Dict[str, Any]) -> Dict[str, Any]:
    title = item.get("word") or ""
    if not isinstance(title, str):
        title = str(title)
    title = title.strip()
    word_scheme = item.get("word_scheme") or ""
    category = item.get("label_name") or item.get("icon_desc") or "综合"
    description = item.get("note") or ""
    hot_value = int(item.get("num") or 0)
    url = _build_search_url(word_scheme or title)
    record: Dict[str, Any] = {
        "title": title,
        "category": category,
        "url": url,
        "hot": hot_value,
        "ads": bool(item.get("icon_desc") == "荐" or item.get("flag") == 7),
        "readCount": 0,
        "discussCount": 0,
        "origin": 0,
    }
    if description and description != title:
        record["description"] = description
    return record


def _normalize_parsed_topic(topic: Dict[str, Any]) -> Dict[str, Any]:
    """Bring parsed topics to the schema expected by downstream tooling."""
    description = topic.get("description") or None
    record: Dict[str, Any] = {
        "title": topic.get("title", ""),
        "category": topic.get("category") or "综合",
        "url": topic.get("url", ""),
        "hot": int(topic.get("hot") or 0),
        "ads": False,
        "readCount": 0,
        "discussCount": 0,
        "origin": 0,
    }
    if description:
        record["description"] = description
    return record


def _split_hot_and_category(hot_text: str) -> Tuple[int, str]:
    """Extract numeric heat value and category label from mixed text."""
    hot_text = (hot_text or "").strip()
    if not hot_text:
        return 0, "综合"

    match = re.search(r"(\d+)$", hot_text)
    if not match:
        return 0, hot_text or "综合"

    number = int(match.group(1))
    prefix = hot_text[: match.start()].strip()
    category = prefix or "综合"
    return number, category


def _enrich_topics(topics: List[Dict[str, Any]], session: requests.Session) -> None:
    """Populate read/discuss statistics via the topic detail page."""
    for topic in topics:
        title = topic.get("title")
        if not title:
            continue
        detail = _fetch_topic_detail(title, session)
        if not detail:
            continue
        if detail.get("category"):
            topic["category"] = detail["category"]
        if detail.get("description"):
            topic["description"] = detail["description"]
        topic["readCount"] = detail.get("read_count", topic.get("readCount", 0))
        topic["discussCount"] = detail.get("discuss_count", topic.get("discussCount", 0))
        topic["origin"] = detail.get("origin", topic.get("origin", 0))


def _fetch_topic_detail(title: str, session: requests.Session) -> Dict[str, Any]:
    query = quote(title, safe="")
    url = DETAIL_URL.format(query=query)
    headers = _build_detail_headers()
    try:
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Detail request failed for %s: %s", title, exc)
        return {}

    soup = BeautifulSoup(response.text, "html.parser")
    category_elem = soup.select_one("#pl_topicband dl > dd")
    desc_elem = soup.select_one("#pl_topicband dl:nth-of-type(2) dd:not(.host-row)")
    stats = [_to_number(node.get_text(strip=True)) for node in soup.select("div.g-list-a.data ul li strong")]
    detail = {
        "category": category_elem.get_text(strip=True) if category_elem else "",
        "description": desc_elem.get_text(strip=True) if desc_elem else "",
        "read_count": stats[0] if len(stats) > 0 else 0,
        "discuss_count": stats[1] if len(stats) > 1 else 0,
        "origin": stats[2] if len(stats) > 2 else 0,
    }
    return detail


def _to_number(val: str) -> int:
    if not val:
        return 0
    val = val.strip()
    if "万" in val:
        try:
            return int(float(val.replace("万", "")) * 10000)
        except ValueError:
            return 0
    if "亿" in val:
        try:
            return int(float(val.replace("亿", "")) * 100000000)
        except ValueError:
            return 0
    digits = re.findall(r"\d+", val)
    if not digits:
        return 0
    try:
        return int(digits[0])
    except ValueError:
        return 0


def _ensure_absolute_url(url: str) -> str:
    if url.startswith("http"):
        return url
    return f"{BASE_URL}{url}"


def _build_search_url(keyword: str) -> str:
    keyword = keyword or ""
    if keyword.startswith("http"):
        return keyword
    return f"{BASE_URL}/weibo?q={requests.utils.quote(keyword)}"


def _build_headers() -> Dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": f"{BASE_URL}/top",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    cookie = os.getenv("WEIBO_COOKIE")
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _build_detail_headers() -> Dict[str, str]:
    headers = _build_headers().copy()
    headers["Referer"] = "https://m.s.weibo.com/"
    headers.setdefault(
        "Accept",
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    )
    return headers


__all__ = ["fetch_latest_topics_local", "persist_topics_snapshot", "parse_hot_topics"]
