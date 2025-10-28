import argparse
import html
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping
from urllib.parse import unquote_plus

from spider.aicard_client import AICardError, AICardResult, fetch_ai_card
from spider.aicard_parser import MediaAsset, ParsedCard, render_aicard_markdown
from spider.crawler_core import slugify_title

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "aicard"


def _derive_topic_slug(result: AICardResult) -> str:
    raw_query = result.response.get("query") or result.query
    clean_query = unquote_plus(str(raw_query))
    return slugify_title(clean_query or "aicard-topic")


def _wrap_html(document_body: str, title: str) -> str:
    safe_title = html.escape(title or "AI Card", quote=True)
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
    h3 {{
      font-size: 1.15rem;
      margin-top: 32px;
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
    .aicard-media-gallery {{
      margin-top: 28px;
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
    }}
    .aicard-media-gallery.single {{
      justify-content: center;
    }}
    .aicard-media {{
      flex: 0 0 160px;
      max-width: 160px;
    }}
    .aicard-media-gallery.single .aicard-media {{
      max-width: 200px;
    }}
    .aicard-media img {{
      width: 100%;
      height: auto;
      border-radius: 8px;
      display: block;
      background-color: #f1f5f9;
    }}
    .aicard-media figcaption {{
      font-size: 0.85rem;
      color: #475569;
      margin-top: 6px;
    }}
  </style>
</head>
<body>
  <div class="aicard-wrapper">
    {document_body}
  </div>
</body>
</html>
"""


def _serialize_media(asset: MediaAsset) -> Dict[str, Any]:
    return {
        "original_url": asset.original_url,
        "secure_url": asset.secure_url,
        "alt": asset.alt,
        "width": asset.width,
        "height": asset.height,
        "media_type": asset.media_type,
        "mirrors": asset.mirrors,
        "user": asset.user,
    }


def _persist_outputs(
    result: AICardResult,
    parsed: ParsedCard,
    output_dir: Path,
) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    slug = _derive_topic_slug(result)
    title = unquote_plus(result.response.get("query") or result.query)
    html_doc = _wrap_html(parsed.html, title)

    html_path = output_dir / f"{slug}.html"
    json_path = output_dir / f"{slug}.json"

    html_path.write_text(html_doc, encoding="utf-8")
    payload = {
        "meta": result.to_dict(),
        "links": parsed.links,
        "media": [_serialize_media(item) for item in parsed.media],
        "html_path": str(html_path),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"html": html_path, "json": json_path}


def _collect_multimodal_entries(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []

    def _append_candidate(candidate: Any) -> None:
        if isinstance(candidate, dict) and candidate:
            entries.append(candidate)

    card_multimodal = payload.get("card_multimodal")
    if isinstance(card_multimodal, dict):
        data = card_multimodal.get("data")
        if isinstance(data, list):
            for item in data:
                _append_candidate(item)
        else:
            _append_candidate(card_multimodal)
    elif isinstance(card_multimodal, list):
        for item in card_multimodal:
            _append_candidate(item)

    share_multimodal = payload.get("share_card_multimodal")
    if isinstance(share_multimodal, dict):
        _append_candidate(share_multimodal)
    elif isinstance(share_multimodal, list):
        for item in share_multimodal:
            _append_candidate(item)

    return entries


def run(query: str, output_dir: Path) -> Dict[str, Path]:
    logging.info("Fetching AI card for %s", query)
    result = fetch_ai_card(query)
    logger_payload = result.response.get("status_stage")
    logging.debug("AI card status_stage=%s", logger_payload)
    raw_message = str(result.response.get("msg") or "")
    card_multimodal = _collect_multimodal_entries(result.response)

    links = result.response.get("link_list")
    if not isinstance(links, list):
        links = []
    parsed = render_aicard_markdown(raw_message, card_multimodal, links)
    return _persist_outputs(result, parsed, output_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and render Weibo AI Card content.")
    parser.add_argument("query", help="原始查询词（例如：#中方回应巴基斯坦向美国赠送稀土#）")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="结果输出目录，默认为项目 data/aicard/",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="日志级别 (DEBUG, INFO, WARNING, ...)，默认 INFO",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    try:
        outputs = run(args.query, args.output_dir)
    except AICardError as exc:
        logging.error("AI card 请求失败：%s", exc)
        raise SystemExit(1) from exc

    logging.info("HTML 输出：%s", outputs["html"])
    logging.info("JSON 输出：%s", outputs["json"])


if __name__ == "__main__":
    main()
