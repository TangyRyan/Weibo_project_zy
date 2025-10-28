import json
import logging
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request

from spider.daily_heat import ARCHIVE_DIR, SUMMARY_PATH, rebuild_summary

app = Flask(__name__)
LOG_LEVEL = logging.INFO
DEFAULT_LIMIT = 30
MAX_LIMIT = 60


class DailyHeatStore:
    def __init__(self) -> None:
        self._cache: Dict[str, Any] = {"generated_at": None, "data": []}
        self._mtime: Optional[float] = None

    def invalidate(self) -> None:
        self._mtime = None

    def _load_from_disk(self) -> None:
        if not SUMMARY_PATH.exists():
            rebuild_summary()
            if not SUMMARY_PATH.exists():
                logging.warning("Daily heat summary file %s missing after rebuild", SUMMARY_PATH)
                self._cache = {"generated_at": None, "data": []}
                self._mtime = None
                return

        try:
            text = SUMMARY_PATH.read_text(encoding="utf-8")
            self._cache = json.loads(text)
            self._mtime = SUMMARY_PATH.stat().st_mtime
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logging.error("Failed to load summary file %s: %s", SUMMARY_PATH, exc)
            self._cache = {"generated_at": None, "data": []}
            self._mtime = None

    def get_payload(self) -> Dict[str, Any]:
        if not ARCHIVE_DIR.exists():
            ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        if not SUMMARY_PATH.exists() or self._mtime is None:
            self._load_from_disk()
            return self._cache

        try:
            current_mtime = SUMMARY_PATH.stat().st_mtime
        except FileNotFoundError:
            self._mtime = None
            self._load_from_disk()
            return self._cache

        if self._mtime != current_mtime:
            self._load_from_disk()
        return self._cache


store = DailyHeatStore()


def _resolve_limit(raw_value: Optional[str]) -> int:
    if raw_value is None:
        return DEFAULT_LIMIT
    try:
        limit = int(raw_value)
    except ValueError:
        return DEFAULT_LIMIT
    return max(1, min(limit, MAX_LIMIT))


@app.get("/api/hot_topics/daily_heat")
def daily_heat() -> Any:
    refresh_flag = request.args.get("refresh")
    if refresh_flag and refresh_flag.lower() in {"1", "true", "yes"}:
        rebuild_summary()
        store.invalidate()
    payload = store.get_payload()
    data = payload.get("data") or []
    if not isinstance(data, list):
        data = []
    limit = _resolve_limit(request.args.get("limit"))
    subset: List[Dict[str, Any]] = data[-limit:]
    response = {
        "generated_at": payload.get("generated_at"),
        "requested_limit": limit,
        "available_days": len(data),
        "data": subset,
    }
    return jsonify(response)


def main() -> None:
    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
    app.run(host="0.0.0.0", port=8766)


if __name__ == "__main__":
    main()
