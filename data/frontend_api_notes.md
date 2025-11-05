# 前端与风险分析接口说明

接口基于本地 Flask 服务，默认监听 `http://127.0.0.1:5000`。如启动时指定 `flask run --host=0.0.0.0 --port=8766`，替换示例中的端口即可。

---

## 热榜（小时级）——前端
- **URL**：`GET /api/hot_topics/hourly`
- **作用**：返回指定日期/小时的热搜榜列表（默认使用最新快照）。
- **参数**
  - `date`（可选）`YYYY-MM-DD`
  - `hour`（可选）`0-23`
  - `limit`（可选）`1-50`
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
> 若需实时推送，可连接 `hot_topics_ws.py` 的 WebSocket，监听 `snapshot`/`update` 消息。

---

## 话题帖子——前端
- **URL**：`GET /api/hot_topics/posts`
- **作用**：返回单个话题的热门帖文列表。
- **必需参数**：`date`
- **话题定位方式（任选其一）**
  1. `slug`
  2. `title`
  3. `rank`
- **可选参数**
  - `hour`
  - `limit`（默认返回全部，最多 50）
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
> 返回的 `created_at` 字段统一格式为 `YY-MM-DD HH:MM`（北京时间），不再出现“xx分钟前”等相对时间；若首次请求返回 404，可稍后重试。

---

## AI Card——前端
- **URL**：`GET /api/hot_topics/aicard`
- **作用**：返回指定话题的 AI Card 快照（HTML + 元数据）。
- **参数**
  - `date`（如可推断会自动补全）
  - 至少提供 `slug`、`title`、`rank` 中的一种
  - `hour`（可选）
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
    "html": "<!DOCTYPE html>...",
    "meta": { "...": "..." },
    "links": [],
    "media": [],
    "first_seen": "2025-11-05T15:00:00+08:00",
    "last_seen": "2025-11-05T15:00:00+08:00"
  }
  ```
> 若快照缺失，接口会自动调用 `ensure_aicard_snapshot()` 现场生成。

---

## 每日整合——风险分析
- **URL**：`GET /api/hot_topics/daily_bundle`
- **作用**：风险分析模块一次性获取某日全部话题及对应帖文。
- **必需参数**：`date`
- **可选参数**：`include_posts`（默认 `true`），`true` 返回话题+帖文整合，`false` 仅返回纯话题归档。
- **示例**
  ```
  GET http://127.0.0.1:5000/api/hot_topics/daily_bundle?date=2025-11-05
  ```
- **返回示例**
  ```json
  {
    "date": "2025-11-05",
    "include_posts": true,
    "source_path": "data/daily_bundles/2025-11-05/topics_with_posts.json",
    "data": {
      "date": "2025-11-05",
      "generated_at": "2025-11-05T23:59:59+08:00",
      "topics": [
        {
          "title": "家庭头等舱2026款夏19.68万元起",
          "slug": "jiating-toudengcang-2026kuan",
          "...": "...",
          "latest_posts": {
            "topic": "#家庭头等舱2026款夏19.68万元起#",
            "fetched_at": "2025-11-05T15:47:38+08:00",
            "items": [...]
          }
        }
      ],
      "source": {
        "archive": "data/hot_topics/2025-11-05.json",
        "posts_dir": "data/posts/2025-11-05"
      }
    }
  }
  ```
> 若整合文件缺失，接口会尝试调 `spider/export_daily_bundle.write_bundle` 立即生成；需保证 `data/daily_bundles/` 可写。

---

## 30 天热度——前端/BI
- **URL**：`GET /api/hot_topics/daily_heat`
- **参数**
  - `limit`（可选）`1-60`，默认 30
  - `refresh`（可选）布尔
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
- **热榜**：`monitor_remote_hot_topics.py` 每 10 分钟轮询远程仓库，若 45 分钟内无数据则触发本地兜底。对应文件 `data/hot_topics/hourly/<date>/<hour>.json`。
- **帖子**：写入小时榜后自动刷新 `needs_refresh` 话题；若接口发现缺失会即时补抓。数据位于 `data/posts/<date>/<slug>.json`。
- **AI Card**：归档时抓取，接口缺失时现场生成。文件位于 `data/aicard/hourly/<date>/<hour>/<slug>.(html|json)`。
- **每日整合（风险分析）**：建议每日终端运行 `python spider/export_daily_bundle.py --date YYYY-MM-DD` 生成 `data/daily_bundles/<date>/topics_with_posts.json`。接口调用时若不存在会尝试自动生成。
- **30 天热度**：写入小时榜或调用 `/api/hot_topics/daily_heat?refresh=1` 时重建，文件 `data/hot_topics/daily_heat.json`。

接口返回 400/404 通常是参数缺失或数据尚未生成，可按提示补齐或稍后重试。响应中的 `source_path` 便于定位原始 JSON 进行排查。
