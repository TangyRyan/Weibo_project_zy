import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from spider.crawler_core import CHINA_TZ, CrawlParams, crawl_topic, ensure_hashtag_format, slugify_title
from Weibo_zy.weibo_enhanced.topic_detail import WeiboPost, get_top_20_hot_posts

# ------- CONFIG -------
TARGET_DATE = "2025-10-25"
ARCHIVE_DIR = Path("../data/hot_topics")
POST_DIR = Path("../data/posts")
TOP_N = 30
MAX_PAGES = 5
MIN_SCORE = 0.0
SINCE = None  # e.g. "2025-10-24T00:00:00+08:00"
MAX_TOPICS_PER_RUN: Optional[int] = None
LOG_LEVEL = logging.INFO


def ensure_dirs() -> None:
    POST_DIR.mkdir(parents=True, exist_ok=True)


def load_archive(date_str: str) -> Dict[str, Dict]:
    path = ARCHIVE_DIR / f"{date_str}.json"
    if not path.exists():
        raise FileNotFoundError(f"archive {path} not found, please run fetch_hot_topics.py first")
    return json.loads(path.read_text(encoding="utf-8"))


def save_archive(date_str: str, data: Dict[str, Dict]) -> None:
    path = ARCHIVE_DIR / f"{date_str}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("Updated archive file %s", path)


def update_topic(title: str, record: Dict, date_str: str) -> Dict:
    slug = record.get("slug") or slugify_title(title)
    record["slug"] = slug
    skip_ids = record.get("known_ids") or []
    searches = [
        ("hashtag", ensure_hashtag_format(title)),
        ("keyword", title.strip()),
    ]
    result = None
    used_mode = None
    for mode, term in searches:
        if not term:
            continue
        params = CrawlParams(
            hashtag=term,
            top_n=TOP_N,
            max_pages=MAX_PAGES,
            min_score=MIN_SCORE,
            since=None,
            skip_ids=skip_ids,
        )
        candidate = crawl_topic(params)
        if result is None:
            result = candidate
            used_mode = mode
        if candidate.get("items"):
            result = candidate
            used_mode = mode
            break

    if result is None:
        result = {
            "topic": ensure_hashtag_format(title),
            "fetched_at": None,
            "total": 0,
            "top_n": TOP_N,
            "items": [],
        }
        used_mode = "hashtag"
    else:
        result["topic"] = ensure_hashtag_format(title)
        if used_mode == "keyword" and not result.get("items"):
            logging.info("Keyword fallback returned no posts for %s", title)
        elif used_mode == "keyword":
            logging.info("Keyword fallback captured posts for %s", title)

    if not result.get("items"):
        detail_items = _fetch_posts_via_topic_detail(title, TOP_N)
        if detail_items:
            result = {
                "topic": ensure_hashtag_format(title),
                "fetched_at": datetime.now(tz=CHINA_TZ).isoformat(timespec="seconds"),
                "total": len(detail_items),
                "top_n": TOP_N,
                "items": detail_items,
            }
            logging.info("Topic detail fallback captured posts for %s", title)
        else:
            logging.warning("No posts captured for %s after all fallbacks", title)

    post_path = POST_DIR / date_str / f"{slug}.json"
    post_path.parent.mkdir(parents=True, exist_ok=True)
    post_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    record["last_post_refresh"] = date_str
    record["post_output"] = str(post_path)
    record["known_ids"] = [item["id"] for item in result.get("items", []) if item.get("id")]
    record["latest_posts"] = result
    has_posts = bool(result.get("items"))
    record["needs_refresh"] = not has_posts
    record["last_post_total"] = result.get("total", 0)
    return record


def refresh_posts_for_date(
    date_str: str,
    max_topics: Optional[int] = None,
) -> Dict[str, List[str]]:
    ensure_dirs()
    archive = load_archive(date_str)
    refreshed: List[str] = []
    skipped: List[str] = []
    failed: List[str] = []
    for title, record in archive.items():
        if max_topics is not None and len(refreshed) >= max_topics:
            skipped.append(title)
            continue
        if not record.get("needs_refresh"):
            skipped.append(title)
            continue
        try:
            archive[title] = update_topic(title, record, date_str)
            refreshed.append(title)
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("Updating topic %s failed: %s", title, exc)
            failed.append(title)
    save_archive(date_str, archive)
    return {"refreshed": refreshed, "skipped": skipped, "failed": failed}


def main() -> None:
    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
    result = refresh_posts_for_date(TARGET_DATE, MAX_TOPICS_PER_RUN)
    logging.info(
        "Post refresh completed: refreshed=%s skipped=%s failed=%s",
        len(result.get("refreshed", [])),
        len(result.get("skipped", [])),
        len(result.get("failed", [])),
    )
    if result.get("refreshed"):
        logging.info("Refreshed topics: %s", ", ".join(result["refreshed"][:10]))


def _fetch_posts_via_topic_detail(title: str, limit: int) -> List[Dict]:
    async def runner() -> List[Dict]:
        try:
            posts = await get_top_20_hot_posts(title)
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("Topic detail crawler failed for %s: %s", title, exc)
            return []
        return _convert_detail_posts(posts, limit)

    return _run_async(runner)


def _convert_detail_posts(posts: Sequence[WeiboPost], limit: int) -> List[Dict]:
    items: List[Dict] = []
    max_items = limit if limit > 0 else len(posts)
    for index, post in enumerate(posts[:max_items]):
        detail_url = getattr(post, "detail_url", "") or ""
        item_id = _generate_detail_id(detail_url, index)
        forwards = getattr(post, "forwards_count", 0) or 0
        comments = getattr(post, "comments_count", 0) or 0
        likes = getattr(post, "likes_count", 0) or 0
        item = {
            "id": item_id,
            "bid": None,
            "url": detail_url or None,
            "created_at": _normalize_timestamp(getattr(post, "timestamp", "") or ""),
            "user_id": None,
            "user_name": getattr(post, "author", "") or None,
            "verified": None,
            "region": None,
            "source": getattr(post, "source", "") or "",
            "text": getattr(post, "content", "") or "",
            "text_raw": getattr(post, "content", "") or "",
            "reposts": forwards,
            "comments": comments,
            "likes": likes,
            "pics": list(getattr(post, "image_links", []) or []),
            "video": _build_video_payload(getattr(post, "video_link", "") or ""),
            "score": forwards * 0.6 + comments * 0.3 + likes * 0.1,
        }
        items.append(item)
    return items


def _build_video_payload(video_link: str) -> Optional[Dict[str, Any]]:
    if not video_link:
        return None
    return {
        "title": None,
        "cover": None,
        "duration": None,
        "streams": {"stream_url": video_link},
    }


def _normalize_timestamp(raw: str) -> Optional[str]:
    raw = raw.strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=CHINA_TZ).isoformat(timespec="seconds")
        except ValueError:
            continue
    return raw


def _generate_detail_id(detail_url: str, index: int) -> str:
    if detail_url:
        return f"detail-{abs(hash(detail_url))}"
    return f"detail-{index}"


def _run_async(func):
    try:
        return asyncio.run(func())
    except RuntimeError as exc:
        if "asyncio.run()" in str(exc):
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(func())
            finally:
                loop.close()
        raise


if __name__ == "__main__":
    main()
