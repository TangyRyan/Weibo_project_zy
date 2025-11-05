import json
from pathlib import Path
from typing import Dict, List, Optional

from spider.aicard_service import BASE_DIR, HOURLY_DIR_NAME


def _hour_dir(date_str: str, hour: int, base_dir: Path = BASE_DIR) -> Path:
    return base_dir / HOURLY_DIR_NAME / date_str / f"{hour:02d}"


def list_hourly_snapshots(date_str: str, hour: int, base_dir: Path = BASE_DIR) -> Dict[str, Dict[str, str]]:
    """返回指定日期、小时下所有话题的 AI Card 索引。"""
    index_path = _hour_dir(date_str, hour, base_dir) / "index.json"
    if not index_path.exists():
        return {}
    try:
        return json.loads(index_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def load_aicard_content(date_str: str, hour: int, slug: str, base_dir: Path = BASE_DIR) -> Optional[Dict]:
    """读取某个话题的 AI Card JSON 内容。"""
    json_path = _hour_dir(date_str, hour, base_dir) / f"{slug}.json"
    if not json_path.exists():
        return None
    try:
        return json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


__all__ = ["list_hourly_snapshots", "load_aicard_content"]
