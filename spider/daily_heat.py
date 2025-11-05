import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

from spider.crawler_core import slugify_title

CHINA_TZ = timezone(timedelta(hours=8))
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = PROJECT_ROOT / "data" / "hot_topics"
SUMMARY_PATH = ARCHIVE_DIR / "daily_heat.json"
MAX_DAYS = 120
REMOTE_SOURCE = (
    "https://raw.githubusercontent.com/lxw15337674/weibo-trending-hot-history/"
    "refs/heads/master/api/{date}/{hour}.json"
)


@dataclass
class DailyHeat:
    date: str
    total_heat: int
    topic_count: int

    def to_dict(self) -> Dict[str, int]:
        return {
            "date": self.date,
            "total_heat": self.total_heat,
            "topic_count": self.topic_count,
        }


def _coerce_int(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        digits = value.strip()
        if not digits:
            return None
        try:
            return int(float(digits))
        except ValueError:
            return None
    return None


def _iso_time(date_str: str, hour: int) -> str:
    dt = datetime.fromisoformat(f"{date_str}T{hour:02d}:00:00")
    return dt.replace(tzinfo=CHINA_TZ).isoformat(timespec="seconds")


def _upsert_topic(record_map: Dict[str, Dict], topic: Dict, date_str: str, hour: int) -> None:
    title = (topic.get("title") or "").strip()
    if not title:
        return
    hour_str = f"{hour:02d}"
    seen_time = _iso_time(date_str, hour)
    record = record_map.get(title)
    if not record:
        record = dict(topic)
        record["appeared_hours"] = [hour_str]
        record["first_seen"] = seen_time
        record["last_seen"] = seen_time
        record["last_post_refresh"] = None
        record["post_output"] = None
        record["known_ids"] = []
        record["needs_refresh"] = True
        record["slug"] = slugify_title(title)
        record_map[title] = record
        return

    record.update(topic)
    record.setdefault("appeared_hours", [])
    if hour_str not in record["appeared_hours"]:
        record["appeared_hours"].append(hour_str)
    record["last_seen"] = seen_time
    record.setdefault("first_seen", seen_time)
    record.setdefault("last_post_refresh", None)
    record.setdefault("known_ids", [])
    record["slug"] = slugify_title(title)
    if record.get("last_post_refresh") != date_str:
        record["needs_refresh"] = True
    return


def _fetch_remote_hour(date_str: str, hour: int) -> List[Dict]:
    url = REMOTE_SOURCE.format(date=date_str, hour=f"{hour:02d}")
    try:
        response = requests.get(url, timeout=10)
    except requests.RequestException as exc:
        logging.warning("fetch remote %s %02d failed: %s", date_str, hour, exc)
        return []

    if response.status_code == 404:
        logging.debug("remote hour missing %s %02d (404)", date_str, hour)
        return []
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        logging.warning("fetch remote %s %02d failed: %s", date_str, hour, exc)
        return []
    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        logging.warning("invalid json for %s %02d: %s", date_str, hour, exc)
        return []
    if not isinstance(data, list):
        logging.warning("remote %s %02d returned non-list payload", date_str, hour)
        return []
    return data


def summarize_archive(date_str: str, archive: Dict[str, Dict]) -> DailyHeat:
    total = 0
    topics = 0
    for record in archive.values():
        if record.get("ads"):
            continue
        hot_value = _coerce_int(record.get("hot"))
        if hot_value is None or hot_value < 0:
            continue
        total += hot_value
        topics += 1
    return DailyHeat(date=date_str, total_heat=total, topic_count=topics)


def load_daily_archive(date_str: str) -> Dict[str, Dict]:
    path = ARCHIVE_DIR / f"{date_str}.json"
    if not path.exists():
        raise FileNotFoundError(f"daily archive missing for date {date_str}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_summary() -> Dict[str, object]:
    if not SUMMARY_PATH.exists():
        return {"generated_at": None, "data": []}
    try:
        return json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        logging.warning("daily heat summary corrupted, recreating %s", SUMMARY_PATH)
        return {"generated_at": None, "data": []}


def _store_summary(entries: List[DailyHeat]) -> None:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(tz=CHINA_TZ).isoformat(timespec="seconds"),
        "data": [entry.to_dict() for entry in entries],
    }
    SUMMARY_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def update_daily_heat(date_str: str, archive: Optional[Dict[str, Dict]] = None) -> DailyHeat:
    if archive is None:
        archive = load_daily_archive(date_str)

    summary_entry = summarize_archive(date_str, archive)

    snapshot = _load_summary()
    raw_entries = snapshot.get("data") or []
    merged: Dict[str, DailyHeat] = {}
    for item in raw_entries:
        try:
            date_val = str(item.get("date"))
            total = _coerce_int(item.get("total_heat")) or 0
            topics = _coerce_int(item.get("topic_count")) or 0
        except Exception:  # pragma: no cover - defensive fallback
            continue
        if not date_val or len(date_val) != 10:
            continue
        merged[date_val] = DailyHeat(date=date_val, total_heat=total, topic_count=topics)

    merged[summary_entry.date] = summary_entry

    sorted_dates = sorted(merged.keys())
    if len(sorted_dates) > MAX_DAYS:
        sorted_dates = sorted_dates[-MAX_DAYS:]

    entries = [merged[item] for item in sorted_dates]
    _store_summary(entries)
    logging.debug(
        "Daily heat summary updated for %s: total_heat=%s topic_count=%s",
        summary_entry.date,
        summary_entry.total_heat,
        summary_entry.topic_count,
    )
    return summary_entry


def _spinner(iterable: Iterable[int]) -> Iterable[int]:
    for hour in iterable:
        yield hour


def fetch_remote_daily_archive(date_str: str) -> Optional[Dict[str, Dict]]:
    record_map: Dict[str, Dict] = {}
    for hour in _spinner(range(24)):
        topics = _fetch_remote_hour(date_str, hour)
        if not topics:
            continue
        for topic in topics:
            _upsert_topic(record_map, topic, date_str, hour)
    if not record_map:
        logging.warning("remote daily archive %s unavailable", date_str)
        return None
    path = ARCHIVE_DIR / f"{date_str}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record_map, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("synced remote daily archive %s to %s", date_str, path)
    return record_map


def rebuild_summary() -> None:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today()
    entries: List[DailyHeat] = []

    for offset in range(MAX_DAYS - 1, -1, -1):
        target = today - timedelta(days=offset)
        date_str = target.isoformat()
        archive_path = ARCHIVE_DIR / f"{date_str}.json"
        archive_data: Optional[Dict[str, Dict]] = None
        if archive_path.exists():
            try:
                archive_data = json.loads(archive_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logging.warning("skip %s due to invalid json: %s", archive_path, exc)
                archive_data = None

        if archive_data is None:
            archive_data = fetch_remote_daily_archive(date_str)

        if not archive_data:
            logging.debug("no archive data for %s", date_str)
            continue

        entries.append(summarize_archive(date_str, archive_data))

    if not entries:
        logging.info("no daily data collected; summary not created")
        return

    entries.sort(key=lambda item: item.date)
    if len(entries) > MAX_DAYS:
        entries = entries[-MAX_DAYS:]
    _store_summary(entries)
    logging.info("rebuilt daily heat summary for %s days", len(entries))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    rebuild_summary()
