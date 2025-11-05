import argparse
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

from spider.crawler_core import CHINA_TZ
from spider.update_posts import ensure_topic_posts, load_archive, save_archive

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = PROJECT_ROOT / "data" / "hot_topics"
POST_DIR = PROJECT_ROOT / "data" / "posts"
BUNDLE_DIR = PROJECT_ROOT / "data" / "daily_bundles"
LOG_LEVEL = logging.INFO


def _load_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        logging.error("JSON decode error for %s: %s", path, exc)
    except OSError as exc:
        logging.error("Failed to read %s: %s", path, exc)
    return None


def _ensure_posts(date_str: str, title: str, archive: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    record = archive.get(title)
    if not record:
        logging.warning("Record for topic %s missing in archive %s", title, date_str)
        return None
    try:
        updated = ensure_topic_posts(title, record, date_str)
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("ensure_topic_posts failed for %s (%s): %s", title, date_str, exc)
        return None
    archive[title] = updated
    try:
        save_archive(date_str, archive)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("Failed to save archive %s after refresh: %s", date_str, exc)
    return updated.get("latest_posts")


def build_bundle(date_str: str) -> Dict[str, Any]:
    archive_path = ARCHIVE_DIR / f"{date_str}.json"
    archive_data = _load_json(archive_path)
    if archive_data is None:
        raise FileNotFoundError(f"daily archive {archive_path} missing or invalid")
    if not isinstance(archive_data, dict):
        raise ValueError(f"expected dict in archive {archive_path}")

    bundle_topics: List[Dict[str, Any]] = []
    archive: Dict[str, Dict[str, Any]] = load_archive(date_str)

    for title, record in archive.items():
        if not isinstance(record, dict):
            continue
        slug = record.get("slug")
        posts_path = POST_DIR / date_str / f"{slug}.json" if slug else None
        posts_payload = _load_json(posts_path) if posts_path else None
        if posts_payload is None:
            posts_payload = _ensure_posts(date_str, title, archive)
        topic_entry = dict(record)
        topic_entry["latest_posts"] = posts_payload or {}
        bundle_topics.append(topic_entry)

    bundle_topics.sort(key=lambda item: item.get("first_seen") or item.get("slug") or "")
    generated_at = datetime.now(tz=CHINA_TZ).isoformat(timespec="seconds")
    return {
        "date": date_str,
        "generated_at": generated_at,
        "topics": bundle_topics,
        "source": {
            "archive": archive_path.as_posix(),
            "posts_dir": (POST_DIR / date_str).as_posix(),
        },
    }


def write_bundle(date_str: str) -> Path:
    bundle = build_bundle(date_str)
    target_dir = BUNDLE_DIR / date_str
    target_dir.mkdir(parents=True, exist_ok=True)

    topics_only_path = target_dir / "topics.json"
    archive_path = ARCHIVE_DIR / f"{date_str}.json"
    topics_only_path.write_text(archive_path.read_text(encoding="utf-8"), encoding="utf-8")

    bundle_path = target_dir / "topics_with_posts.json"
    bundle_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("Daily bundle stored at %s", bundle_path)
    return bundle_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export daily bundle with topics and posts.")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD). Defaults to today (GMT+8).")
    parser.add_argument("--log-level", help="Logging level (INFO/DEBUG/WARNING).", default="INFO")
    return parser.parse_args()


def resolve_date(value: Optional[str]) -> str:
    if value:
        return value
    now = datetime.now(tz=CHINA_TZ)
    return now.strftime("%Y-%m-%d")


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), LOG_LEVEL))
    date_str = resolve_date(args.date)
    try:
        write_bundle(date_str)
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("Failed to build bundle for %s: %s", date_str, exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
