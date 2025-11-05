from datetime import datetime, timezone
from pathlib import Path

from spider.aicard_api import list_hourly_snapshots, load_aicard_content
from spider.aicard_parser import MediaAsset, ParsedCard
from spider.aicard_service import ensure_aicard_snapshot


class DummyResult:
    def __init__(self, query: str):
        self.query = query
        self.response = {
            "msg": "content",
            "card_multimodal": {},
            "link_list": ["link"],
        }
        self.status_code = 200
        self.fetched_at = datetime.now(timezone.utc)

    def to_dict(self):
        return {
            "query": self.query,
            "status_code": self.status_code,
            "fetched_at": self.fetched_at.isoformat(),
        }


def test_ensure_aicard_snapshot_creates_files(monkeypatch, tmp_path):
    calls = {"fetch": 0}

    def fake_fetch(query: str):
        calls["fetch"] += 1
        dummy = DummyResult(query)
        dummy.response["card_multimodal"] = {"data": [{"img": "http://example.com/img.jpg"}]}
        return dummy

    def fake_render(msg: str, multimodal, links=None):
        return ParsedCard(
            html="<p>Rendered</p>",
            media=[MediaAsset("http://example.com/img.jpg", "https://example.com/img.jpg", "alt", None, None, "image", [], None)],
            links=list(links or []),
        )

    monkeypatch.setattr("spider.aicard_service.fetch_ai_card", fake_fetch)
    monkeypatch.setattr("spider.aicard_service.render_aicard_markdown", fake_render)

    snapshot = ensure_aicard_snapshot(
        "测试话题",
        "2025-10-28",
        14,
        slug="topic-slug",
        base_dir=tmp_path,
    )

    html_path = tmp_path / "hourly" / "2025-10-28" / "14" / "topic-slug.html"
    json_path = html_path.with_suffix(".json")
    assert html_path.exists()
    assert json_path.exists()
    assert snapshot["html"] == html_path.as_posix()
    assert snapshot["json"] == json_path.as_posix()
    assert snapshot["title"] == "测试话题"
    assert calls["fetch"] == 1

    index_path = html_path.parent / "index.json"
    assert index_path.exists()
    index_content = index_path.read_text(encoding="utf-8")
    assert "测试话题" in index_content

    # Second call should reuse cached files
    second = ensure_aicard_snapshot(
        "测试话题",
        "2025-10-28",
        14,
        slug="topic-slug",
        base_dir=tmp_path,
    )
    assert second == snapshot
    assert calls["fetch"] == 1

    index = list_hourly_snapshots("2025-10-28", 14, base_dir=tmp_path)
    assert "topic-slug" in index

    aicard_payload = load_aicard_content("2025-10-28", 14, "topic-slug", base_dir=tmp_path)
    assert isinstance(aicard_payload, dict)
    assert aicard_payload["html_path"] == snapshot["html"]
