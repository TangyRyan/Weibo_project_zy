"""Poll GitHub hourly trending snapshots, update archives, and refresh posts."""

import asyncio
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests

CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from spider.fetch_hot_topics import (
    CHINA_TZ,
    ensure_dirs,
    fetch_hour_topics,
    load_daily_archive,
    save_daily_archive,
    upsert_topic,
)
from spider.local_hot_topics import fetch_latest_topics_local
from spider.crawler_core import slugify_title
from spider.aicard_service import ensure_aicard_snapshot
from spider.update_posts import (
    MAX_TOPICS_PER_RUN,
    POST_DIR,
    ensure_topic_posts,
    refresh_posts_for_date,
)
from spider.daily_heat import update_daily_heat
POLL_INTERVAL_SECONDS = 600
RECENT_RETRY_SECONDS = 60
MAX_LOOKBACK_DAYS = 1
PROJECT_ROOT = Path(__file__).resolve().parents[1]
HOURLY_ARCHIVE_DIR = PROJECT_ROOT / "data" / "hot_topics" / "hourly"
LOG_LEVEL = logging.INFO
LOCAL_FALLBACK_THRESHOLD_MINUTES = 45
POST_EXPORT_DIR = PROJECT_ROOT / "data" / "hot_posts"
HOURLY_POST_LIMIT = 20
HOURLY_POST_CACHE_DIR = PROJECT_ROOT / "data" / "posts" / "hourly"
HOURLY_POST_CACHE_TTL_SECONDS = 3600


def ensure_hourly_dir() -> None:
    HOURLY_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


def hour_path(date_str: str, hour: int) -> Path:
    return HOURLY_ARCHIVE_DIR / date_str / f"{hour:02d}.json"


def collect_pending_hours(now: datetime) -> List[Tuple[str, int]]:
    pending: List[Tuple[str, int]] = []
    for offset in range(MAX_LOOKBACK_DAYS + 1):
        target = now - timedelta(days=offset)
        date_str = target.strftime("%Y-%m-%d")
        max_hour = target.hour if offset == 0 else 23
        for hour in range(max_hour + 1):
            if not hour_path(date_str, hour).exists():
                pending.append((date_str, hour))
    return pending


def update_hourly_archive(date_str: str, hour: int, topics: List[dict]) -> None:
    target_dir = HOURLY_ARCHIVE_DIR / date_str
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{hour:02d}.json"
    path.write_text(json.dumps(topics, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("Saved hourly snapshot %s", path)


def update_daily_archive(date_str: str, hour: int, topics: List[dict]) -> None:
    daily_data = load_daily_archive(date_str)
    new_titles = 0
    for topic in topics:
        title = (topic.get("title") or "").strip()
        if title and title not in daily_data:
            new_titles += 1
        record = upsert_topic(daily_data, topic, date_str, hour)
        if record:
            snapshot = ensure_aicard_snapshot(
                title,
                date_str,
                hour,
                slug=record.get("slug"),
                logger=logging.getLogger("aicard"),
            )
            if snapshot:
                record.setdefault("aicard", {})
                record["aicard"]["html"] = snapshot["html"]
                record["aicard"]["json"] = snapshot["json"]
                hours = record["aicard"].setdefault("hours", {})
                hours[f"{hour:02d}"] = {
                    "html": snapshot["html"],
                    "json": snapshot["json"],
                }
    save_daily_archive(date_str, daily_data)
    pending_refresh = sum(1 for item in daily_data.values() if item.get("needs_refresh"))
    try:
        summary = update_daily_heat(date_str, daily_data)
        logging.debug(
            "Daily heat summary updated during monitor sync %s: total_heat=%s topics=%s",
            summary.date,
            summary.total_heat,
            summary.topic_count,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("Failed to update daily heat summary for %s: %s", date_str, exc)
    logging.info(
        "Daily archive %s %02d synced: total=%s, new=%s, pending_refresh=%s",
        date_str,
        hour,
        len(daily_data),
        new_titles,
        pending_refresh,
    )


def process_hour(date_str: str, hour: int) -> bool:
    topics, local_used = fetch_topics_with_fallback(date_str, hour)
    if not topics:
        logging.warning("No topics available for %s %02d after fallback", date_str, hour)
        return False

    if local_used:
        logging.info("Using local crawler data for %s %02d", date_str, hour)
    update_hourly_archive(date_str, hour, topics)
    update_daily_archive(date_str, hour, topics)
    _refresh_posts_if_needed(date_str)
    payload_map = _collect_hourly_posts(date_str, hour, topics)
    _export_hourly_posts(date_str, hour, topics, payload_map)
    logging.debug("Completed processing for %s %02d (local_used=%s)", date_str, hour, local_used)
    return True


def process_latest_hour(force: bool = False) -> bool:
    now = datetime.now(tz=CHINA_TZ)
    date_str = now.strftime("%Y-%m-%d")
    hour = now.hour
    if not force and hour_path(date_str, hour).exists():
        logging.debug("Current hour %s %02d already exists, skip immediate fetch", date_str, hour)
        return False
    logging.info("Immediate fetch for %s %02d", date_str, hour)
    return process_hour(date_str, hour)


def process_pending_hours() -> bool:
    now = datetime.now(tz=CHINA_TZ)
    pending = collect_pending_hours(now)
    if not pending:
        logging.debug("No pending hourly snapshots")
        return False

    processed_any = False
    for date_str, hour in pending:
        if process_hour(date_str, hour):
            processed_any = True
    return processed_any


def fetch_topics_with_fallback(date_str: str, hour: int) -> Tuple[List[dict], bool]:
    try:
        topics = fetch_hour_topics(date_str, hour)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code == 404:
            logging.info(
                "Remote %s %02d data not ready (HTTP 404), attempting local fallback",
                date_str,
                hour,
            )
        else:
            logging.error(
                "HTTP %s when fetching %s %02d data; attempting local fallback",
                status_code,
                date_str,
                hour,
            )
        return _maybe_fetch_local(date_str, hour, "remote fetch error")
    except (requests.RequestException, ValueError) as exc:
        logging.error(
            "Remote fetch failed for %s %02d (%s), attempting local fallback",
            date_str,
            hour,
            exc,
        )
        return _maybe_fetch_local(date_str, hour, "remote failure")

    if topics:
        return topics, False

    logging.warning("Remote returned empty list for %s %02d, attempting local fallback", date_str, hour)
    return _maybe_fetch_local(date_str, hour, "remote empty payload")


def fetch_local_topics_with_logging(date_str: str, hour: int, reason: str) -> List[dict]:
    logging.info("Triggering local crawler for %s %02d (%s)", date_str, hour, reason)
    try:
        topics = fetch_latest_topics_local()
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("Local crawler failed for %s %02d: %s", date_str, hour, exc, exc_info=True)
        return []

    if not topics:
        logging.warning("Local crawler returned empty topics for %s %02d", date_str, hour)
        return []

    logging.info("Local crawler produced %s topics for %s %02d", len(topics), date_str, hour)
    return topics


def should_trigger_local(date_str: str, hour: int) -> bool:
    target = datetime.fromisoformat(f"{date_str}T{hour:02d}:00:00").replace(tzinfo=CHINA_TZ)
    now = datetime.now(tz=CHINA_TZ)
    if now < target:
        return False
    elapsed = now - target
    if elapsed >= timedelta(minutes=LOCAL_FALLBACK_THRESHOLD_MINUTES):
        return True
    if now.date() > target.date():
        return True
    if now.hour > hour:
        return True
    return False





def _maybe_fetch_local(date_str: str, hour: int, reason: str) -> Tuple[List[dict], bool]:
    if not should_trigger_local(date_str, hour):
        logging.info(
            "Local fallback for %s %02d postponed: only %s minutes since top of hour",
            date_str,
            hour,
            LOCAL_FALLBACK_THRESHOLD_MINUTES,
        )
        return [], False
    topics = fetch_local_topics_with_logging(date_str, hour, reason)
    return topics, bool(topics)


def _refresh_posts_if_needed(date_str: str) -> None:
    try:
        result = refresh_posts_for_date(date_str, MAX_TOPICS_PER_RUN)
    except FileNotFoundError:
        logging.warning("Daily archive %s missing; skip post refresh", date_str)
        return
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("Post refresh failed for %s: %s", date_str, exc)
        return

    refreshed = len(result.get("refreshed", []))
    skipped = len(result.get("skipped", []))
    failed = len(result.get("failed", []))
    logging.info(
        "Post refresh summary %s: refreshed=%s skipped=%s failed=%s",
        date_str,
        refreshed,
        skipped,
        failed,
    )


def _collect_hourly_posts(date_str: str, hour: int, topics: List[dict]) -> Dict[str, Dict[str, Any]]:
    hour_str = f"{hour:02d}"
    cache_dir = HOURLY_POST_CACHE_DIR / date_str / hour_str
    cache_dir.mkdir(parents=True, exist_ok=True)
    daily_data = load_daily_archive(date_str)
    payload_map: Dict[str, Dict[str, Any]] = {}
    now = datetime.now(tz=CHINA_TZ)
    changed = False
    for index, topic in enumerate(topics):
        if index >= HOURLY_POST_LIMIT:
            break
        title = (topic.get("title") or "").strip()
        if not title:
            continue
        record = daily_data.get(title)
        if not record:
            logging.debug("Skip hourly post collection for %s: missing archive record", title)
            continue
        slug = record.get("slug") or slugify_title(title)
        cache_path = cache_dir / f"{slug}.json"
        if cache_path.exists():
            mtime = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=CHINA_TZ)
            if (now - mtime) <= timedelta(seconds=HOURLY_POST_CACHE_TTL_SECONDS):
                try:
                    payload_map[title] = json.loads(cache_path.read_text(encoding="utf-8"))
                    continue
                except json.JSONDecodeError:
                    logging.warning("Corrupted hourly cache %s, refetching", cache_path)
        updated_record = ensure_topic_posts(title, record, date_str)
        daily_data[title] = updated_record
        payload = updated_record.get("latest_posts") or {}
        cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        payload_map[title] = payload
        changed = True
    if changed:
        save_daily_archive(date_str, daily_data)
    return payload_map


def _export_hourly_posts(date_str: str, hour: int, topics: List[dict], payload_map: Dict[str, Dict[str, Any]]) -> None:
    hour_str = f"{hour:02d}"
    export_dir = POST_EXPORT_DIR / date_str
    export_dir.mkdir(parents=True, exist_ok=True)
    daily_data = load_daily_archive(date_str)
    seen_titles: set[str] = set()
    all_payload: List[dict] = []
    new_payload: List[dict] = []
    for index, topic in enumerate(topics):
        if index >= HOURLY_POST_LIMIT:
            break
        title = (topic.get("title") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        record = daily_data.get(title)
        if not record:
            logging.warning("Daily archive missing entry for %s", title)
            continue
        slug = record.get("slug") or slugify_title(title)
        payload = payload_map.get(title)
        if not payload:
            cache_path = HOURLY_POST_CACHE_DIR / date_str / hour_str / f"{slug}.json"
            try:
                payload = json.loads(cache_path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                post_path = POST_DIR / date_str / f"{slug}.json"
                if not post_path.exists():
                    logging.warning("Post cache not found for %s", post_path)
                    continue
                payload = json.loads(post_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                logging.error("Failed to load hourly post cache %s: %s", cache_path, exc)
                continue
        all_payload.append(payload)

        first_seen = record.get("first_seen")
        if first_seen:
            try:
                first_dt = datetime.fromisoformat(first_seen)
            except ValueError:
                logging.debug("first_seen parse error for %s: %s", title, first_seen)
            else:
                if first_dt.tzinfo is None:
                    first_dt = first_dt.replace(tzinfo=CHINA_TZ)
                first_dt_local = first_dt.astimezone(CHINA_TZ)
                if first_dt_local.strftime("%H") == hour_str:
                    new_payload.append(payload)

    all_path = export_dir / f"{date_str}_{hour_str}_all.json"
    new_path = export_dir / f"{date_str}_{hour_str}_new.json"
    all_path.write_text(json.dumps(all_payload, ensure_ascii=False, indent=2), encoding='utf-8')
    new_path.write_text(json.dumps(new_payload, ensure_ascii=False, indent=2), encoding='utf-8')
    logging.info(
        "Exported hourly posts %s %s: all=%s new=%s",
        date_str,
        hour_str,
        len(all_payload),
        len(new_payload),
    )

async def run_loop() -> None:
    ensure_dirs()
    ensure_hourly_dir()
    await asyncio.to_thread(process_latest_hour, True)
    while True:
        processed = await asyncio.to_thread(process_pending_hours)
        sleep_for = RECENT_RETRY_SECONDS if processed else POLL_INTERVAL_SECONDS
        logging.info("Sleeping %s seconds before next check", sleep_for)
        await asyncio.sleep(sleep_for)


def configure_logging() -> None:
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main() -> None:
    configure_logging()
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        logging.info("Received stop signal, exiting monitor")


if __name__ == "__main__":
    main()
