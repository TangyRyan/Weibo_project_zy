import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, request

from spider.crawler_core import CHINA_TZ, slugify_title
from spider.daily_heat import ARCHIVE_DIR, SUMMARY_PATH, rebuild_summary
from spider.aicard_service import ensure_aicard_snapshot
from spider.update_posts import ensure_topic_posts, load_archive, save_archive

app = Flask(__name__)
LOG_LEVEL = logging.INFO
DEFAULT_LIMIT = 30
MAX_LIMIT = 60
MAX_HOURLY_LIMIT = 50
MAX_POST_LIMIT = 50

HOURLY_DIR = ARCHIVE_DIR / "hourly"
POSTS_DIR = ARCHIVE_DIR.parent / "posts"
REPO_ROOT = Path(__file__).resolve().parent


class DailyHeatStore:
    def __init__(self) -> None:
        self._cache: Dict[str, Any] = {"generated_at": None, "data": []}
        self._mtime: Optional[float] = None

    def invalidate(self) -> None:
        self._mtime = None

    def _load_from_disk(self) -> None:
        if not SUMMARY_PATH.exists():
            rebuild_summary()
            if not SUMMARY_PATH.exists():
                logging.warning("Daily heat summary file %s missing after rebuild", SUMMARY_PATH)
                self._cache = {"generated_at": None, "data": []}
                self._mtime = None
                return

        try:
            text = SUMMARY_PATH.read_text(encoding="utf-8")
            self._cache = json.loads(text)
            self._mtime = SUMMARY_PATH.stat().st_mtime
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logging.error("Failed to load summary file %s: %s", SUMMARY_PATH, exc)
            self._cache = {"generated_at": None, "data": []}
            self._mtime = None

    def get_payload(self) -> Dict[str, Any]:
        if not ARCHIVE_DIR.exists():
            ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        if not SUMMARY_PATH.exists() or self._mtime is None:
            self._load_from_disk()
            return self._cache

        try:
            current_mtime = SUMMARY_PATH.stat().st_mtime
        except FileNotFoundError:
            self._mtime = None
            self._load_from_disk()
            return self._cache

        if self._mtime != current_mtime:
            self._load_from_disk()
        return self._cache


store = DailyHeatStore()


def _resolve_limit(raw_value: Optional[str]) -> int:
    if raw_value is None:
        return DEFAULT_LIMIT
    try:
        limit = int(raw_value)
    except ValueError:
        return DEFAULT_LIMIT
    return max(1, min(limit, MAX_LIMIT))


@app.get("/api/hot_topics/daily_heat")
def daily_heat() -> Any:
    refresh_flag = request.args.get("refresh")
    if refresh_flag and refresh_flag.lower() in {"1", "true", "yes"}:
        rebuild_summary()
        store.invalidate()
    payload = store.get_payload()
    data = payload.get("data") or []
    if not isinstance(data, list):
        data = []
    limit = _resolve_limit(request.args.get("limit"))
    subset: List[Dict[str, Any]] = data[-limit:]
    response = {
        "generated_at": payload.get("generated_at"),
        "requested_limit": limit,
        "available_days": len(data),
        "data": subset,
    }
    return jsonify(response)


def _parse_hour(raw_hour: Optional[str]) -> Optional[int]:
    if raw_hour is None:
        return None
    try:
        hour = int(raw_hour)
    except ValueError:
        raise ValueError("hour must be an integer between 0 and 23") from None
    if hour < 0 or hour > 23:
        raise ValueError("hour must be an integer between 0 and 23")
    return hour


def _resolve_positive_limit(raw_value: Optional[str], *, maximum: int) -> Optional[int]:
    if raw_value is None:
        return None
    try:
        value = int(raw_value)
    except ValueError:
        return None
    value = max(1, value)
    if maximum is not None:
        value = min(value, maximum)
    return value


def _load_json(path: Path) -> Optional[Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        logging.error("Failed to read %s: %s", path, exc)
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        logging.error("Invalid JSON in %s: %s", path, exc)
        return None


def _coerce_topic_list(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [dict(item) for item in data if isinstance(item, dict)]
    return []


def _read_text_file(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        logging.error("Failed to read text file %s: %s", path, exc)
        return None


def _format_post_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    local_now = datetime.now(tz=CHINA_TZ)
    dt: Optional[datetime] = None

    if text in {"刚刚", "刚才"}:
        dt = local_now

    if dt is None:
        match = re.fullmatch(r"(\d+)\s*分钟前", text)
        if match:
            dt = local_now - timedelta(minutes=int(match.group(1)))

    if dt is None:
        match = re.fullmatch(r"(\d+)\s*小时前", text)
        if match:
            dt = local_now - timedelta(hours=int(match.group(1)))

    if dt is None:
        match = re.fullmatch(r"(\d+)\s*天前", text)
        if match:
            dt = local_now - timedelta(days=int(match.group(1)))

    if dt is None:
        match = re.fullmatch(r"今天\s*(\d{1,2}):(\d{2})", text)
        if match:
            hour, minute = int(match.group(1)), int(match.group(2))
            dt = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if dt is None:
        match = re.fullmatch(r"昨天\s*(\d{1,2}):(\d{2})", text)
        if match:
            hour, minute = int(match.group(1)), int(match.group(2))
            dt = (local_now - timedelta(days=1)).replace(hour=hour, minute=minute, second=0, microsecond=0)

    if dt is None:
        match = re.fullmatch(r"(\d{1,2})月(\d{1,2})日\s*(\d{1,2}):(\d{2})", text)
        if match:
            month = int(match.group(1))
            day = int(match.group(2))
            hour = int(match.group(3))
            minute = int(match.group(4))
            year = local_now.year
            candidate = datetime(year, month, day, hour, minute, tzinfo=CHINA_TZ)
            if candidate - local_now > timedelta(days=1):
                candidate = candidate.replace(year=year - 1)
            dt = candidate

    normalized = text.replace("Z", "+00:00")
    if dt is None:
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            dt = None
    if dt is None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
            try:
                dt = datetime.strptime(normalized, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=CHINA_TZ)
    else:
        dt = dt.astimezone(CHINA_TZ)
    return dt.strftime("%y-%m-%d %H:%M")


@dataclass
class SnapshotPayload:
    date: str
    hour: int
    topics: List[Dict[str, Any]]
    path: Path
    generated_at: datetime

    def sliced_topics(self, limit: Optional[int]) -> List[Dict[str, Any]]:
        if limit is None:
            return self.topics
        return self.topics[:limit]


class HourlySnapshotStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def get_snapshot(self, date: Optional[str], hour: Optional[int]) -> Optional[SnapshotPayload]:
        if date:
            return self._load_for_date(date, hour)
        return self._load_latest()

    def _load_for_date(self, date: str, hour: Optional[int]) -> Optional[SnapshotPayload]:
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            logging.warning("Invalid date format: %s", date)
            return None

        directory = self.root / date
        if not directory.exists():
            logging.info("Hourly directory missing for %s", date)
            return None

        if hour is None:
            candidates: List[Tuple[int, Path]] = []
            for path in directory.iterdir():
                if not path.is_file() or path.suffix.lower() != ".json":
                    continue
                try:
                    hour_value = int(path.stem)
                except ValueError:
                    continue
                candidates.append((hour_value, path))
            if not candidates:
                return None
            hour, path = max(candidates, key=lambda item: item[0])
        else:
            path = directory / f"{hour:02d}.json"
            if not path.exists():
                logging.info("Hourly snapshot %s %02d missing", date, hour)
                return None

        data = _load_json(path)
        if data is None:
            return None
        topics = _coerce_topic_list(data)
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=CHINA_TZ)
        return SnapshotPayload(date=date, hour=hour, topics=topics, path=path, generated_at=mtime)

    def _load_latest(self) -> Optional[SnapshotPayload]:
        if not self.root.exists():
            return None
        snapshots: List[SnapshotPayload] = []
        for directory in self.root.iterdir():
            if not directory.is_dir():
                continue
            snapshot = self._load_for_date(directory.name, None)
            if snapshot:
                snapshots.append(snapshot)
        if not snapshots:
            return None
        snapshots.sort(key=lambda item: (item.date, item.hour), reverse=True)
        return snapshots[0]


def _load_post_payload(date: str, slug: str) -> Tuple[Optional[Dict[str, Any]], Optional[Path]]:
    path = POSTS_DIR / date / f"{slug}.json"
    if not path.exists():
        return None, None
    data = _load_json(path)
    if data is None:
        return None, None
    return data, path


def _ensure_posts_exist(date: str, title: str, archive: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    record = archive.get(title)
    if not record:
        logging.info("Archive for %s missing topic %s", date, title)
        return None
    try:
        updated_record = ensure_topic_posts(title, record, date)
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("ensure_topic_posts failed for %s (%s): %s", title, date, exc)
        return None
    archive[title] = updated_record
    try:
        save_archive(date, archive)
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("Failed to save archive for %s: %s", date, exc)
    return updated_record


def _locate_archive_record_by_slug(
    archive: Dict[str, Dict[str, Any]], slug: str, fallback_title: Optional[str]
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    if fallback_title and fallback_title in archive:
        return fallback_title, archive[fallback_title]
    for title, record in archive.items():
        if record.get("slug") == slug:
            return title, record
    return None, None


def _derive_hour_from_record(record: Optional[Dict[str, Any]]) -> Optional[int]:
    if not record:
        return None
    hours = record.get("appeared_hours")
    if isinstance(hours, list):
        for raw in reversed(hours):
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue
    first_seen = record.get("first_seen")
    if isinstance(first_seen, str) and len(first_seen) >= 13:
        try:
            return int(first_seen[11:13])
        except ValueError:
            return None
    return None


HOURLY_STORE = HourlySnapshotStore(HOURLY_DIR)


@app.get("/api/hot_topics/hourly")
def hourly_topics() -> Any:
    date = request.args.get("date")
    raw_hour = request.args.get("hour")
    raw_limit = request.args.get("limit")

    try:
        hour = _parse_hour(raw_hour)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    limit = _resolve_positive_limit(raw_limit, maximum=MAX_HOURLY_LIMIT)
    snapshot = HOURLY_STORE.get_snapshot(date, hour)
    if not snapshot:
        return jsonify({"error": "Hourly snapshot not available"}), 404

    response = {
        "date": snapshot.date,
        "hour": snapshot.hour,
        "generated_at": snapshot.generated_at.isoformat(timespec="seconds"),
        "total": len(snapshot.topics),
        "topics": snapshot.sliced_topics(limit),
        "source_path": snapshot.path.as_posix(),
    }
    if limit is not None:
        response["requested_limit"] = limit
    return jsonify(response)


@app.get("/api/hot_topics/posts")
def topic_posts() -> Any:
    date = request.args.get("date")
    raw_hour = request.args.get("hour")
    slug = request.args.get("slug")
    title = request.args.get("title")
    raw_rank = request.args.get("rank")
    raw_limit = request.args.get("limit")

    try:
        hour = _parse_hour(raw_hour)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    limit = _resolve_positive_limit(raw_limit, maximum=MAX_POST_LIMIT)

    snapshot: Optional[SnapshotPayload] = None
    if date or hour is not None:
        snapshot = HOURLY_STORE.get_snapshot(date, hour)
    elif not slug and not title:
        snapshot = HOURLY_STORE.get_snapshot(None, None)

    if not date and snapshot:
        date = snapshot.date

    if not date:
        return jsonify({"error": "date is required when no hourly snapshot is available"}), 400

    if slug:
        slug = slug.strip()
    if title:
        title = title.strip()
        if not slug:
            slug = slugify_title(title)

    if raw_rank and not slug:
        try:
            rank = int(raw_rank)
        except ValueError:
            return jsonify({"error": "rank must be an integer"}), 400
        if rank <= 0:
            return jsonify({"error": "rank must be >= 1"}), 400
        if snapshot is None:
            snapshot = HOURLY_STORE.get_snapshot(date, hour)
        if snapshot is None:
            return jsonify({"error": "Unable to resolve snapshot for supplied rank"}), 404
        if rank > len(snapshot.topics):
            return jsonify({"error": "rank exceeds available topics"}), 400
        topic_entry = snapshot.topics[rank - 1]
        title = topic_entry.get("title") or title
        slug = slugify_title(title or "")

    if not slug:
        return jsonify({"error": "slug, title, or rank must be provided"}), 400

    payload, source_path = _load_post_payload(date, slug)
    archive: Optional[Dict[str, Dict[str, Any]]] = None

    if payload is None:
        try:
            archive = load_archive(date)
        except FileNotFoundError:
            archive = {}
        title, record = (_locate_archive_record_by_slug(archive, slug, title) if archive else (title, None))
        if title and archive:
            refreshed = _ensure_posts_exist(date, title, archive)
            if refreshed:
                payload, source_path = _load_post_payload(date, slug)
                if payload is None:
                    logging.warning("Post payload missing after refresh for %s (%s)", title, date)
        if payload is None:
            return jsonify({"error": "Topic posts not available"}), 404
    else:
        if not title:
            if archive is None:
                try:
                    archive = load_archive(date)
                except FileNotFoundError:
                    archive = {}
            if archive:
                title, _ = _locate_archive_record_by_slug(archive, slug, title)

    items_raw = payload.get("items")
    if not isinstance(items_raw, list):
        items_raw = []

    items: List[Dict[str, Any]] = []
    for entry in items_raw:
        if not isinstance(entry, dict):
            continue
        copy = dict(entry)
        original_ts = copy.get("created_at") or copy.get("timestamp")
        formatted_ts = _format_post_timestamp(original_ts)
        if formatted_ts:
            copy["created_at"] = formatted_ts
        items.append(copy)

    response = {
        "date": date,
        "slug": slug,
        "title": title or payload.get("topic"),
        "fetched_at": payload.get("fetched_at"),
        "total": payload.get("total", len(items)),
        "items": items[:limit] if limit is not None else items,
        "source_path": source_path.as_posix() if source_path else None,
    }
    if limit is not None:
        response["requested_limit"] = limit
    return jsonify(response)


@app.get("/api/hot_topics/aicard")
def topic_aicard() -> Any:
    date = request.args.get("date")
    raw_hour = request.args.get("hour")
    slug = request.args.get("slug")
    title = request.args.get("title")
    raw_rank = request.args.get("rank")

    try:
        hour = _parse_hour(raw_hour)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    snapshot: Optional[SnapshotPayload] = None
    if raw_rank or not slug or not title or date is None or hour is None:
        snapshot = HOURLY_STORE.get_snapshot(date, hour)
        if snapshot:
            if date is None:
                date = snapshot.date
            if hour is None:
                hour = snapshot.hour

    if not date:
        return jsonify({"error": "date is required or could not be inferred"}), 400

    if raw_rank:
        try:
            rank = int(raw_rank)
        except ValueError:
            return jsonify({"error": "rank must be an integer"}), 400
        if rank <= 0:
            return jsonify({"error": "rank must be >= 1"}), 400
        if snapshot is None:
            snapshot = HOURLY_STORE.get_snapshot(date, hour)
        if snapshot is None:
            return jsonify({"error": "Unable to resolve snapshot for supplied rank"}), 404
        if rank > len(snapshot.topics):
            return jsonify({"error": "rank exceeds available topics"}), 400
        topic_entry = snapshot.topics[rank - 1]
        title = topic_entry.get("title") or title
        slug = slugify_title(title or "")
        if hour is None:
            hour = snapshot.hour

    if title:
        title = title.strip()
        if not slug:
            slug = slugify_title(title)

    if slug:
        slug = slug.strip()

    if snapshot and not title and slug:
        for topic_entry in snapshot.topics:
            topic_title = topic_entry.get("title") or ""
            if slugify_title(topic_title) == slug:
                title = topic_title
                if hour is None:
                    hour = snapshot.hour
                break

    archive: Optional[Dict[str, Dict[str, Any]]] = None
    record: Optional[Dict[str, Any]] = None

    if (not title or hour is None) and date:
        try:
            archive = load_archive(date)
        except FileNotFoundError:
            archive = {}
        if slug and archive:
            title, record = _locate_archive_record_by_slug(archive, slug, title)
        if title and not record and archive:
            record = archive.get(title)
        if record and record.get("slug"):
            slug = record.get("slug") or slug
        if not title and archive:
            record = None
        if record and hour is None:
            hour = _derive_hour_from_record(record)

    if not title or not slug:
        return jsonify({"error": "Unable to resolve topic title or slug"}), 404

    if hour is None:
        return jsonify({"error": "hour must be provided or derivable from data"}), 400

    aicard_info = ensure_aicard_snapshot(title, date, hour, slug=slug)
    if not aicard_info:
        return jsonify({"error": "AI card not available"}), 404

    slug = aicard_info.get("slug", slug)
    html_rel = aicard_info.get("html")
    json_rel = aicard_info.get("json")

    html_abs = REPO_ROOT / Path(html_rel) if html_rel else None
    json_abs = REPO_ROOT / Path(json_rel) if json_rel else None

    html_content = _read_text_file(html_abs) if html_abs else None
    json_payload = _load_json(json_abs) if json_abs else None

    response: Dict[str, Any] = {
        "date": date,
        "hour": hour,
        "slug": slug,
        "title": title,
        "html_path": html_rel,
        "json_path": json_rel,
        "html": html_content,
        "meta": (json_payload or {}).get("meta"),
        "links": (json_payload or {}).get("links"),
        "media": (json_payload or {}).get("media"),
    }

    if record:
        response["first_seen"] = record.get("first_seen")
        response["last_seen"] = record.get("last_seen")

    return jsonify(response)


def main() -> None:
    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
    app.run(host="0.0.0.0", port=8766)


if __name__ == "__main__":
    main()
