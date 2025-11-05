import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional

from spider.aicard_client import AICardError, fetch_ai_card
from spider.aicard_parser import ParsedCard, render_aicard_markdown
from spider.crawler_core import ensure_hashtag_format, slugify_title

BASE_DIR = Path(__file__).resolve().parents[1] / "data" / "aicard"
HOURLY_DIR_NAME = "hourly"


def _relative_to_repo(path: Path) -> str:
    repo_root = Path(__file__).resolve().parents[1]
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return path.as_posix()


def _wrap_html(body: str, title: str) -> str:
    safe_title = title or "AI Card"
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{safe_title}</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      line-height: 1.6;
      color: #1f2933;
      background-color: #f7f9fb;
      margin: 0;
      padding: 32px 16px;
    }}
    .aicard-wrapper {{
      max-width: 840px;
      margin: 0 auto;
      background-color: #ffffff;
      border-radius: 12px;
      border: 1px solid #e5e9f0;
      padding: 32px 28px;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
    }}
    h1, h2, h3, h4 {{
      color: #0f172a;
    }}
    p {{
      margin: 12px 0;
    }}
    ol, ul {{
      padding-left: 22px;
      margin: 12px 0;
    }}
    li {{
      margin: 8px 0;
    }}
  </style>
</head>
<body>
  <div class="aicard-wrapper">
    {body}
  </div>
</body>
</html>
"""


def ensure_aicard_snapshot(
    title: str,
    date_str: str,
    hour: int,
    *,
    slug: Optional[str] = None,
    base_dir: Path = BASE_DIR,
    overwrite: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Optional[Dict[str, str]]:
    """确保为指定话题生成 AI Card 快照，返回相对路径信息。"""
    logger = logger or logging.getLogger(__name__)
    normalized_slug = slug or slugify_title(title)
    target_dir = base_dir / HOURLY_DIR_NAME / date_str / f"{hour:02d}"
    html_path = target_dir / f"{normalized_slug}.html"
    json_path = target_dir / f"{normalized_slug}.json"

    if not overwrite and html_path.exists() and json_path.exists():
        return {
            "slug": normalized_slug,
            "html": _relative_to_repo(html_path),
            "json": _relative_to_repo(json_path),
            "title": title,
        }

    query = ensure_hashtag_format(title)
    try:
        result = fetch_ai_card(query)
    except AICardError as exc:
        logger.warning("AI Card 获取失败：%s (%s)", title, exc)
        return None

    multimodal_data: List[Dict] = []
    card_multimodal = result.response.get("card_multimodal")
    if isinstance(card_multimodal, dict):
        data = card_multimodal.get("data")
        if isinstance(data, list):
            multimodal_data = data
        else:
            multimodal_data = [card_multimodal]
    elif isinstance(card_multimodal, list):
        multimodal_data = [item for item in card_multimodal if isinstance(item, dict)]

    share_multimodal = result.response.get("share_card_multimodal")
    if isinstance(share_multimodal, dict):
        multimodal_data.append(share_multimodal)
    elif isinstance(share_multimodal, list):
        multimodal_data.extend(item for item in share_multimodal if isinstance(item, dict))
    links = result.response.get("link_list")
    parsed = render_aicard_markdown(
        result.response.get("msg") or "",
        multimodal_data,
        links if isinstance(links, list) else None,
    )

    target_dir.mkdir(parents=True, exist_ok=True)
    html_doc = _wrap_html(parsed.html, query)
    html_path.write_text(html_doc, encoding="utf-8")
    payload = {
        "meta": result.to_dict(),
        "links": parsed.links,
        "media": [asdict(asset) for asset in parsed.media],
        "html_path": _relative_to_repo(html_path),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    index_path = target_dir / "index.json"
    try:
        index_payload = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
        index_payload = {}
    index_payload[normalized_slug] = {
        "title": title,
        "html": _relative_to_repo(html_path),
        "json": _relative_to_repo(json_path),
        "fetched_at": result.fetched_at.isoformat(timespec="seconds"),
    }
    index_path.write_text(json.dumps(index_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "slug": normalized_slug,
        "html": _relative_to_repo(html_path),
        "json": _relative_to_repo(json_path),
        "title": title,
    }


__all__ = ["ensure_aicard_snapshot", "BASE_DIR"]
