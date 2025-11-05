import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

from spider.aicard_service import ensure_aicard_snapshot
from spider.crawler_core import slugify_title
from spider.daily_heat import update_daily_heat

CHINA_TZ = timezone(timedelta(hours=8))

# ------- 配置 -------
HOT_TOPIC_DATES = ["2025-10-25"]  # 支持同日多个小时
HOT_TOPIC_HOURS = [15]
HOT_TOPIC_SOURCE = (
    "https://raw.githubusercontent.com/lxw15337674/weibo-trending-hot-history/"
    "refs/heads/master/api/{date}/{hour}.json"
)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = PROJECT_ROOT / "data" / "hot_topics"
POST_DIR = PROJECT_ROOT / "data" / "posts"
LOG_LEVEL = logging.INFO


def ensure_dirs() -> None:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    POST_DIR.mkdir(parents=True, exist_ok=True)


def fetch_hour_topics(date_str: str, hour: int) -> List[Dict]:
    url = HOT_TOPIC_SOURCE.format(date=date_str, hour=f"{hour:02d}")
    logging.info("获取热榜：%s", url)
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise ValueError(f"{url} 返回非列表数据")
    return data


def load_daily_archive(date_str: str) -> Dict[str, Dict]:
    path = ARCHIVE_DIR / f"{date_str}.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_daily_archive(date_str: str, data: Dict[str, Dict]) -> None:
    path = ARCHIVE_DIR / f"{date_str}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("已更新归档：%s", path)


def iso_time(date_str: str, hour: int) -> str:
    dt = datetime.fromisoformat(f"{date_str}T{hour:02d}:00:00")
    return dt.replace(tzinfo=CHINA_TZ).isoformat(timespec="seconds")

def upsert_topic(record_map: Dict[str, Dict], topic: Dict, date_str: str, hour: int) -> Optional[Dict]:
    title = (topic.get("title") or "").strip()
    if not title:
        return None
    hour_str = f"{hour:02d}"
    seen_time = iso_time(date_str, hour)
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
        return record

    # 更新已有事件
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
    return record


def process_day(date_str: str, hours: List[int]) -> None:
    daily_data = load_daily_archive(date_str)
    new_titles: List[str] = []
    for hour in hours:
        try:
            topics = fetch_hour_topics(date_str, hour)
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("获取 %s %s 时段失败：%s", date_str, hour, exc)
            continue
        for topic in topics:
            title = (topic.get("title") or "").strip()
            if title and title not in daily_data:
                new_titles.append(title)
            record = upsert_topic(daily_data, topic, date_str, hour)
            if not record:
                continue
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
    try:
        summary = update_daily_heat(date_str, daily_data)
        logging.debug(
            "Daily heat summary refreshed for %s: total_heat=%s topics=%s",
            summary.date,
            summary.total_heat,
            summary.topic_count,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("更新 %s 日热度汇总失败：%s", date_str, exc)
    if daily_data:
        logging.info(
            "日期 %s：共 %s 个话题，新增 %s 个，待更新 %s 个",
            date_str,
            len(daily_data),
            len(set(new_titles)),
            sum(1 for item in daily_data.values() if item.get("needs_refresh")),
        )


def main() -> None:
    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
    ensure_dirs()
    for date_str in HOT_TOPIC_DATES:
        process_day(date_str, HOT_TOPIC_HOURS)


if __name__ == "__main__":
    main()
