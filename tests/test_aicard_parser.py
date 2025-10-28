import json
from pathlib import Path

from spider.aicard_parser import render_aicard_markdown


def _load_fixture() -> dict:
    path = Path(__file__).resolve().parent / "fixtures" / "aicard_response.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_render_aicard_markdown_strips_internal_blocks():
    payload = _load_fixture()
    multimodal = payload.get("card_multimodal", {}).get("data", [])
    parsed = render_aicard_markdown(payload.get("msg", ""), multimodal, payload.get("link_list"))

    assert "<think>" not in parsed.html
    assert "media-block" not in parsed.html
    assert "wbCustomBlock" not in parsed.html
    assert "<h3>" in parsed.html
    assert "<strong>事件定性为谣言</strong>" in parsed.html
    assert any(asset.secure_url.startswith("https://") for asset in parsed.media)
    assert parsed.links == payload["link_list"]
    assert "aicard-media-gallery" in parsed.html
