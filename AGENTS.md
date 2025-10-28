# Repository Guidelines

## Project Structure & Module Organization
`crawler_core.py` holds the reusable crawling primitives (request throttling, slug/hashtag helpers, `CrawlParams`). Task-specific entry points live in `fetch_hot_topics.py` (pulls hourly archives into `data/hot_topics/`) and `update_posts.py` (refreshes topic posts into `data/posts/<date>/slug.json`). JSON snapshots such as `daily_hot_topics.json` illustrate the expected schema for downstream tooling. Keep transient notebooks or experiments inside `data/` or a new `sandbox/` so source modules stay focused and importable.

## Build, Test, and Development Commands
Use Python 3.10+ in a virtual environment. Typical setup:
```
python -m venv .venv && .venv\Scripts\activate
pip install requests beautifulsoup4 (BeautifulSoup is optional but supported)
```
Run the hourly ingest with `python fetch_hot_topics.py` (reads `HOT_TOPIC_*` constants and updates archives). Refresh posts with `python update_posts.py` after confirming `TARGET_DATE` and crawl limits. For ad-hoc checks, run `python -m crawler_core` with a temporary harness or REPL to exercise helpers without touching archives.
Start the remote GitHub monitor with `python monitor_remote_hot_topics.py` to poll the hourly JSON every 10 minutes, perform an immediate fetch on startup, and materialize snapshots under `data/hot_topics/hourly/`. 若在整点起 45 分钟内仍无法获取远程数据，监控脚本会触发 `Weibo_zy/weibo/main.py` 作为本地兜底，并将标准化结果写回本仓库的 `data/` 目录，确保数据结构与远程仓库一致。

## Coding Style & Naming Conventions
Follow PEP 8 with 4-space indentation, snake_case functions, and ALL_CAPS constants, mirroring the existing modules. Prefer dataclasses and TypedDict/List annotations when new payloads are introduced so `mypy` (or pyright) can be adopted later with minimal churn. Keep logging via the standard `logging` package; avoid bare prints. Configuration blocks at the top of scripts should stay uppercase and grouped, with comments in Chinese kept concise and updated as parameters evolve.

## Testing Guidelines
No automated tests ship yet, so add `tests/` with `test_<module>.py` files using `pytest`. Target pure functions (`slugify_title`, `ensure_hashtag_format`, retry logic) and mock network calls with `responses` or `pytest-httpx`. Aim for smoke coverage of ingest/update flows by faking small JSON fixtures under `tests/fixtures/`. Run `pytest` locally before publishing archives to ensure schema regressions are caught early.

## Commit & Pull Request Guidelines
The repository history is not tracked in Git yet; adopt a Conventional Commits style immediately (e.g., `feat: add hourly archive merger`, `fix: guard empty topic list`). Keep commit bodies short but include context for changed constants or data files. Pull requests should summarize the user impact, list new/changed commands, note any required reconfiguration (cookies, headers, schedules), and attach sample snippets from the produced JSON so reviewers can verify shape changes quickly.

## Hot Topics WebSocket Service
- Install dependency: `pip install websockets`.
- Launch the broadcaster: `python hot_topics_ws.py` (defaults to `ws://0.0.0.0:8765/?limit=30`). The module also exposes `start_hot_topics_ws()` for embedding in other scripts.
- Message types: initial `snapshot`, streaming `update`, optional `empty`/`error` responses. Clients may send `{"action": "request_snapshot", "date": "YYYY-MM-DD", "hour": 13}` for historical data or `{"action": "set_limit", "value": 20}` to adjust list length.
- Demo frontend: open `sandbox/hot_topics_client.html` in a browser, adjust the WebSocket地址字段指向后端所在主机即可查看实时刷新效果。
