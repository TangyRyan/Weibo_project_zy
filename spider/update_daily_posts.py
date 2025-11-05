import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from spider.crawler_core import CHINA_TZ, ensure_hashtag_format, slugify_title
from spider.fetch_hot_topics import load_daily_archive, save_daily_archive
from spider.update_posts import ensure_topic_posts

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HOT_TOPICS_DIR = PROJECT_ROOT / "data" / "hot_topics"
HOURLY_HOT_TOPICS_DIR = PROJECT_ROOT / "data" / "hot_topics" / "hourly"
POST_DIR = PROJECT_ROOT / "data" / "posts"
DAILY_OUTPUT_DIR = PROJECT_ROOT / "data" / "daily_posts"
HOURLY_EXPORT_LIMIT = 20


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate daily hot topic posts archive")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD), defaults to today", default=None)
    parser.add_argument("--force", action="store_true", help="Overwrite existing daily archive")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s [%(levelname)s] %(message)s")
    date_str = args.date or datetime.now(tz=CHINA_TZ).strftime("%Y-%m-%d")
    output_path = DAILY_OUTPUT_DIR / f"{date_str}.json"
    if output_path.exists() and not args.force:
        logging.info("Daily posts archive %s already exists; use --force to overwrite", output_path)
        return

    daily_data = load_daily_archive(date_str)
    if not daily_data:
        logging.warning("No daily archive data for %s; aborting", date_str)
        return

    DAILY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    daily_payload: Dict[str, Dict[str, Any]] = {}
    changed = False

    for title, record in daily_data.items():
        slug = record.get("slug") or slugify_title(title)
        record["slug"] = slug

        updated_record = ensure_topic_posts(title, record, date_str)
        if updated_record is not record:
            daily_data[title] = updated_record
            changed = True
        post_payload = _load_post_payload(date_str, slug, updated_record)
        daily_hot_max, daily_hot_sum = _calculate_daily_hot(title, date_str)
        daily_payload[title] = {
            "topic": post_payload.get("topic") or ensure_hashtag_format(title),
            "fetched_at": post_payload.get("fetched_at"),
            "total": post_payload.get("total", 0),
            "top_n": post_payload.get("top_n", HOURLY_EXPORT_LIMIT),
            "items": post_payload.get("items", []),
            "daily_hot_max": daily_hot_max,
            "daily_hot_sum": daily_hot_sum,
            "first_seen": record.get("first_seen"),
            "last_seen": record.get("last_seen"),
            "appeared_hours": record.get("appeared_hours", []),
        }

    output_path.write_text(json.dumps(daily_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("Daily posts archive written to %s (%s topics)", output_path, len(daily_payload))

    if changed:
        save_daily_archive(date_str, daily_data)


def _calculate_daily_hot(title: str, date_str: str) -> Tuple[int, int]:
    hour_dir = HOURLY_HOT_TOPICS_DIR / date_str
    if not hour_dir.exists():
        return 0, 0
    max_hot = 0
    sum_hot = 0
    for hour_path in sorted(hour_dir.glob("*.json")):
        try:
            hour_data = json.loads(hour_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        for item in hour_data:
            if (item.get("title") or "").strip() == title:
                hot_value = int(item.get("hot") or 0)
                max_hot = max(max_hot, hot_value)
                sum_hot += hot_value
    return max_hot, sum_hot


def _load_post_payload(date_str: str, slug: str, record: Dict[str, Any]) -> Dict[str, Any]:
    payload = record.get("latest_posts") or {}
    if payload.get("items"):
        return payload
    post_path = POST_DIR / date_str / f"{slug}.json"
    if post_path.exists():
        try:
            return json.loads(post_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            logging.error("Failed to read post cache %s: %s", post_path, exc)
    return {
        "topic": ensure_hashtag_format(record.get("title") or slug),
        "fetched_at": None,
        "total": 0,
        "top_n": HOURLY_EXPORT_LIMIT,
        "items": [],
    }


if __name__ == "__main__":
    main()
