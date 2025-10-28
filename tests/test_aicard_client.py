from unittest import mock

import pytest
import requests

from spider.aicard_client import AICardError, fetch_ai_card


class DummyResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")


def test_fetch_ai_card_success(monkeypatch):
    session = mock.Mock(spec=requests.Session)
    session.post.return_value = DummyResponse(
        status_code=200,
        payload={"msg": "ok", "query": "%23示例话题%23"},
    )

    result = fetch_ai_card("#示例话题#", session=session, timeout=5, retries=0, request_id="999")

    assert result.msg() == "ok"
    data = session.post.call_args.kwargs["data"]
    assert "query=%23" in data  # encoded hashtag
    assert session.post.call_args.kwargs["timeout"] == 5


def test_fetch_ai_card_server_error(monkeypatch):
    session = mock.Mock(spec=requests.Session)
    session.post.return_value = DummyResponse(status_code=500)
    monkeypatch.setattr("spider.aicard_client.time.sleep", lambda *_: None)

    with pytest.raises(AICardError):
        fetch_ai_card("#失败案例#", session=session, retries=0)

    # Ensure we attempted exactly once (since retries=0)
    assert session.post.call_count == 1

