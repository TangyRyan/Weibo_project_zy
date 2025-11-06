"""兼容入口：导出 spider.hot_topics_api 中定义的 Flask 应用。"""

from spider.hot_topics_api import app, main

__all__ = ["app", "main"]


if __name__ == "__main__":
    main()
