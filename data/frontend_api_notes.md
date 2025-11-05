# 前端接口说明

本地 Flask 服务默认监听 `http://127.0.0.1:5000`。若启动时使用 `flask run --host=0.0.0.0 --port=8766`，请将以下示例中的端口替换为实际值。

---

## 热榜（小时级）
- **URL**：`GET /api/hot_topics/hourly`
- **用途**：返回指定日期/小时的热搜榜项，默认使用最新快照。
- **参数**
  - `date`（可选）`YYYY-MM-DD`：不传时自动选择最新日期。
  - `hour`（可选）`0-23`：配合 `date` 选择具体小时；不传时自动选择指定日期的最新小时。
  - `limit`（可选）`1-50`：限制返回条数，默认返回全部。
- **示例**
  ```
  GET http://127.0.0.1:5000/api/hot_topics/hourly?date=2025-11-05&hour=15&limit=20
  ```
- **返回示例**
  ```json
  {
    "date": "2025-11-05",
    "hour": 15,
    "generated_at": "2025-11-05T15:52:08+08:00",
    "total": 50,
    "topics": [
      {
        "rank": 1,
        "title": "家庭头等舱2026款夏19.68万元起",
        "hot": 123456,
        "category": "综合",
        "description": "话题摘要说明",
        "url": "https://s.weibo.com/weibo?q=...",
        "ads": false,
        "readCount": 3456789,
        "discussCount": 12345,
        "origin": 0
      }
    ],
    "source_path": "data/hot_topics/hourly/2025-11-05/15.json",
    "requested_limit": 20
  }
  ```

> 若需实时更新，可连接 `hot_topics_ws.py` 提供的 WebSocket 并监听 `snapshot`/`update` 消息。

---

## 话题帖子
- **URL**：`GET /api/hot_topics/posts`
- **用途**：返回指定话题的热门帖子列表。
- **必需参数**
  - `date`：`YYYY-MM-DD`（若未提供且系统能推断，会自动补全）。
- **话题定位方式**（至少提供其一）
  1. `slug`：文件名中的 slug。
  2. `title`：话题标题。
  3. `rank`：榜单排名，需配合 `date`/`hour` 或依赖最新榜单。
- **其他参数**
  - `hour`（可选）`0-23`：锁定某个小时的榜单。
  - `limit`（可选）`1-50`：限制返回帖子数量。
- **示例**
  ```
  GET http://127.0.0.1:5000/api/hot_topics/posts?date=2025-11-05&title=家庭头等舱2026款夏19.68万元起&limit=20
  ```
- **返回示例**
  ```json
  {
    "date": "2025-11-05",
    "slug": "jiating-toudengcang-2026kuan",
    "title": "家庭头等舱2026款夏19.68万元起",
    "fetched_at": "2025-11-05T15:47:38+08:00",
    "total": 20,
    "items": [
      {
        "id": "detail-123456",
        "url": "https://weibo.com/123456",
        "created_at": "25-11-05 15:22",
        "user_name": "微博用户A",
        "text": "...",
        "reposts": 120,
        "comments": 45,
        "likes": 560,
        "pics": [],
        "video": null,
        "score": 123.4
      }
    ],
    "source_path": "data/posts/2025-11-05/jiating-toudengcang-2026kuan.json",
    "requested_limit": 20
  }
  ```

> 返回的 `created_at` 字段统一格式为 `YY-MM-DD HH:MM`（示例：`25-11-05 15:22`），无论原始数据是“57分钟前”“今天 12:30”还是其他相对时间，都会转换为对应的北京时间。若文件尚未生成，接口会调用 `ensure_topic_posts()` 自动补抓；首次请求可能返回 404，稍后重试即可。

---

## AI Card
- **URL**：`GET /api/hot_topics/aicard`
- **用途**：返回指定话题的 AI Card 快照（HTML 内容 + 元数据）。
- **必需参数**
  - `date`：`YYYY-MM-DD`（若能推断，将自动补全）。
- **话题定位方式**（至少提供其一，与帖子接口一致）
  1. `slug`
  2. `title`
  3. `rank`
- **其他参数**
  - `hour`（可选）`0-23`：辅助定位小时榜单。
- **示例**
  ```
  GET http://127.0.0.1:5000/api/hot_topics/aicard?date=2025-11-05&title=家庭头等舱2026款夏19.68万元起
  ```
- **返回示例**
  ```json
  {
    "date": "2025-11-05",
    "hour": 15,
    "slug": "jiating-toudengcang-2026kuan",
    "title": "家庭头等舱2026款夏19.68万元起",
    "html_path": "data/aicard/hourly/2025-11-05/15/jiating-toudengcang-2026kuan.html",
    "json_path": "data/aicard/hourly/2025-11-05/15/jiating-toudengcang-2026kuan.json",
    "html": "<!DOCTYPE html>...完整 HTML 文本",
    "meta": { "...": "..." },
    "links": [],
    "media": [],
    "first_seen": "2025-11-05T15:00:00+08:00",
    "last_seen": "2025-11-05T15:00:00+08:00"
  }
  ```

> 若快照缺失，接口会调用 `ensure_aicard_snapshot()` 立即生成；首次请求可能返回 404，稍后重试即可。

---

## 30 天热度
- **URL**：`GET /api/hot_topics/daily_heat`
- **用途**：返回最近 N 天的热度汇总。
- **参数**
  - `limit`（可选）`1-60`：默认 30。
  - `refresh`（可选）布尔：`1/true/yes` 时强制重建摘要。
- **示例**
  ```
  GET http://127.0.0.1:5000/api/hot_topics/daily_heat?limit=30
  ```
- **返回示例**
  ```json
  {
    "generated_at": "2025-11-05T15:50:02+08:00",
    "requested_limit": 30,
    "available_days": 45,
    "data": [
      {
        "date": "2025-10-07",
        "total_heat": 1234567,
        "topic_count": 318
      }
    ]
  }
  ```

---

## 数据更新频率与目录
- **热榜数据**
  - `monitor_remote_hot_topics.py` 每 10 分钟轮询 GitHub 远程仓库，启动即刻拉取。
  - 若某小时在 45 分钟内仍缺失，会调用本地兜底爬虫。
  - 前端可通过 REST `/api/hot_topics/hourly` 或 WebSocket `snapshot/update` 获取。
- **帖子数据**
  - 每次写入小时榜后自动刷新 `needs_refresh` 话题；若接口发现缺失会即时补抓。
  - 依赖有效的 `WEIBO_COOKIE`。
- **AI Card**
  - 日归档流程会尝试生成；若缺失，`/api/hot_topics/aicard` 会调用 `ensure_aicard_snapshot()` 现场生成。
  - 依赖 `WEIBO_AICARD_COOKIE`（或 `WEIBO_COOKIE`）保持登录态。
- **30 天热度**
  - 小时榜更新或 `/api/hot_topics/daily_heat?refresh=1` 时重建。
  - 数据来源 `data/hot_topics/<date>.json`，输出 `data/hot_topics/daily_heat.json`。
- **数据目录**
  - `data/hot_topics/hourly/<date>/<hour>.json` —— 热榜快照（`/hourly` 数据源）。
  - `data/posts/<date>/<slug>.json` —— 帖子快照（`/posts` 数据源）。
  - `data/aicard/hourly/<date>/<hour>/<slug>.(html|json)` —— AI Card 快照（`/aicard` 数据源）。
  - `data/hot_topics/daily_heat.json` —— 30 天热度摘要（`/daily_heat` 数据源）。

出现 400/404 大多是参数缺失或数据尚未生成，按提示补齐或稍后重试即可。响应中的 `source_path` 字段可帮助定位原始文件以便排查。
