# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import re
from dataclasses import dataclass, asdict
from typing import List, Optional
from urllib.parse import quote, urljoin
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# -------------------- 常量配置 --------------------
POSTS_SEARCH_URL = "https://s.weibo.com/weibo?q=%23{}%23&xsort=hot&suball=1&tw=hotweibo"
MAX_POSTS_TO_FETCH = 20          # 每个话题抓取的微博数量上限
MAX_SEARCH_PAGES = 2             # 搜索列表最多翻页数
SCROLL_COUNT = 2                 # 每页滚动次数，帮助加载更多
SCROLL_DELAY_MS = 2000           # 每次滚动等待(ms)

BASE_DIR = Path(__file__).parent
USER_DATA_DIR = BASE_DIR / "browser_data_detail"  # 持久化上下文目录（保存登录态）
COOKIES_PATH = BASE_DIR / "weibo_cookies.json"    # 兼容旧流程：可选地写出 cookies
AUTH_STATE_PATH = BASE_DIR / "auth_state.json"    # Playwright storage_state 文件

# “正常抓取”全程无UI；仅“登录流程”允许可见窗
HEADLESS = True
LOGIN_HEADLESS = False

# 登录页与成功判断
LOGIN_URL = (
    "https://passport.weibo.com/sso/signin?entry=miniblog&source=miniblog&disp=popup"
    "&url=https%3A%2F%2Fweibo.com%2Fnewlogin%3Ftabtype%3Dweibo%26gid%3D102803%26openLoginLayer%3D0"
    "%26url%3Dhttps%253A%252F%252Fweibo.com%252F"
)
LOGIN_SUCCESS_SELECTOR = "text=首页"
LOGIN_TIMEOUT = 120000  # 120s

# -------------------- 数据结构 --------------------
@dataclass
class WeiboPost:
    author: str
    content: str
    timestamp: str
    source: str
    forwards_count: int
    comments_count: int
    likes_count: int
    image_links: List[str]
    video_link: str
    detail_url: str

    def to_dict(self):
        d = asdict(self)
        # 对外不暴露 detail_url（如你不希望前端看到，可保留；若需要可删除下面一行）
        d.pop("detail_url", None)
        return d

# -------------------- 工具函数 --------------------
def _cn_number_to_int(text: str) -> int:
    """将“12万”“3.4亿”等中文计数转换为整数"""
    if not text:
        return 0
    text = str(text).strip()
    try:
        if "亿" in text:
            return int(float(text.replace("亿", "")) * 100000000)
        if "万" in text:
            return int(float(text.replace("万", "")) * 10000)
        # 去掉非数字
        digits = re.sub(r"[^\d.]", "", text)
        if digits == "":
            return 0
        return int(float(digits))
    except Exception:
        return 0

def _persist_login_state(storage_state: dict) -> List[dict]:
    """把 storage_state 和 cookies 写回到本地，便于兼容旧流程"""
    try:
        # 写 storage_state
        AUTH_STATE_PATH.write_text(json.dumps(storage_state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

    cookies = storage_state.get("cookies", [])
    try:
        COOKIES_PATH.write_text(json.dumps(cookies, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return cookies

def _load_cookies_from_file() -> Optional[List[dict]]:
    """（可选）从 weibo_cookies.json 预置一份 Cookie 到上下文"""
    if COOKIES_PATH.exists():
        try:
            data = json.loads(COOKIES_PATH.read_text(encoding="utf-8"))
            # 兼容两种结构：纯 list 或 {"cookies": [...]}
            if isinstance(data, dict) and "cookies" in data:
                return data["cookies"]
            if isinstance(data, list):
                return data
        except Exception:
            return None
    return None

# -------------------- 登录流程（仅此时弹窗） --------------------
async def _login_and_update_cookies(playwright) -> Optional[List[dict]]:
    """
    触发手动登录（允许弹窗）。成功后持久化到 USER_DATA_DIR，并写回 auth_state.json / weibo_cookies.json。
    """
    USER_DATA_DIR.mkdir(exist_ok=True)
    context = await playwright.chromium.launch_persistent_context(
        user_data_dir=str(USER_DATA_DIR),
        headless=LOGIN_HEADLESS,  # ★ 登录时才允许有头
        args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
    )
    page = await context.new_page()

    # 若已有 storage_state，先尝试直接进入首页（可避免无谓的登录步骤）
    if AUTH_STATE_PATH.exists():
        try:
            await page.goto("https://weibo.com", wait_until="load", timeout=60000)
            await page.wait_for_selector(LOGIN_SUCCESS_SELECTOR, timeout=15000)
            logging.info("已通过保存的登录状态自动登录。")
            storage = await context.storage_state()
            cookies = _persist_login_state(storage)
            await context.close()
            return cookies
        except Exception:
            logging.info("保存的登录状态无效，进入手动登录。")

    logging.info("请在弹出的浏览器中完成微博登录…")
    await page.goto(LOGIN_URL, wait_until="load", timeout=60000)
    try:
        await page.wait_for_selector(LOGIN_SUCCESS_SELECTOR, timeout=LOGIN_TIMEOUT)
        logging.info("登录成功，正在更新本地状态。")
    except PlaywrightTimeout:
        logging.error("登录超时，未能更新 Cookie。")
        await context.close()
        return None

    storage = await context.storage_state()
    cookies = _persist_login_state(storage)
    await context.close()
    return cookies

# -------------------- 详情页抓取 --------------------
async def _get_post_details(context, detail_url: str, base_data: dict) -> Optional[WeiboPost]:
    """
    进入单条微博详情页，补充图片/视频等信息。失败返回 None。
    """
    page = await context.new_page()
    try:
        await page.goto(detail_url, wait_until="load", timeout=60000)
        # 等待正文区域出现（尽量宽松一些）
        try:
            await page.wait_for_selector("article, div.Detail, div.WB_detail", timeout=10000)
        except PlaywrightTimeout:
            pass

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")

        # 图片
        image_links = []
        for img in soup.select("img"):
            src = img.get("src") or img.get("data-src") or ""
            if src and ("wx" in src or "sinaimg" in src or "mw" in src):
                if src.startswith("//"):
                    src = "https:" + src
                image_links.append(src)
        # 去重
        image_links = list(dict.fromkeys(image_links))

        # 视频（非常粗略的抓取方式，足够用）
        video_link = ""
        video_tag = soup.find("video")
        if video_tag and video_tag.get("src"):
            video_link = video_tag.get("src")
            if video_link.startswith("//"):
                video_link = "https:" + video_link

        return WeiboPost(
            author=base_data.get("author", ""),
            content=base_data.get("content", ""),
            timestamp=base_data.get("timestamp", ""),
            source=base_data.get("source", ""),
            forwards_count=base_data.get("forwards_count", 0),
            comments_count=base_data.get("comments_count", 0),
            likes_count=base_data.get("likes_count", 0),
            image_links=image_links,
            video_link=video_link,
            detail_url=detail_url
        )
    except Exception as e:
        logging.warning(f"详情页抓取失败：{detail_url}，原因：{e}")
        return None
    finally:
        try:
            await page.close()
        except Exception:
            pass

# -------------------- 列表页抓取（默认静默） --------------------
async def get_top_20_hot_posts(topic_title: str) -> List[WeiboPost]:
    """
    抓取一个话题的热门微博（最多 20 条）。
    常态：全程 headless 静默，不弹窗。
    仅当检测到登录态失效时，临时弹窗一次完成登录，然后回到静默抓取。
    """
    search_url = POSTS_SEARCH_URL.format(quote(topic_title.replace("#", "")))
    logging.info(f"开始抓取话题 '{topic_title}' 的热门微博...")

    seed_cookies = _load_cookies_from_file()  # 仅用于冷启动时的种子 Cookie

    async with async_playwright() as p:
        attempt = 0
        posts: List[WeiboPost] = []

        while attempt < 2:  # 最多两轮：第一轮失败 -> 执行登录 -> 再试一轮
            attempt += 1

            USER_DATA_DIR.mkdir(exist_ok=True)
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(USER_DATA_DIR),
                headless=HEADLESS,  # ★ 常态：静默
                args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox", "--window-position=-32000,-32000"]
            )

            # 冷启动可补种 cookie（不会覆盖 storage_state 里已有cookie）
            try:
                if seed_cookies:
                    await context.add_cookies(seed_cookies)
            except Exception:
                pass

            page = await context.new_page()

            try:
                await page.goto(search_url, wait_until="load", timeout=60000)
                await page.wait_for_selector("div.card-wrap", timeout=10000)
                # 如果能到这里，说明无需登录，直接解析列表
            except Exception:
                await context.close()

                if attempt >= 2:
                    logging.warning("连续两次访问失败，可能仍需登录。")
                    return []

                # ★ 仅此时触发登录流程（会弹窗），成功后进入下一轮静默抓取
                logging.info("疑似 Cookies/登录状态失效，开始登录刷新（将弹出浏览器窗口）...")
                seed_cookies = await _login_and_update_cookies(p)
                if not seed_cookies:
                    logging.error("登录失败，放弃本话题抓取。")
                    return []
                logging.info("登录成功，已刷新状态，准备重新尝试访问。")
                continue

            # ------- 解析列表页（含滚动 & 翻页） -------
            try:
                base_search_url = search_url
                page_urls = [base_search_url] + [
                    f"{base_search_url}&page={i}" for i in range(2, MAX_SEARCH_PAGES + 1)
                ]

                initial_posts: List[dict] = []
                seen_detail_urls = set()

                async def collect_from_page(page_obj, page_index: int) -> bool:
                    # 滚动加载更多
                    if SCROLL_COUNT > 0:
                        for _ in range(SCROLL_COUNT):
                            await page_obj.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await page_obj.wait_for_timeout(SCROLL_DELAY_MS)
                    html = await page_obj.content()
                    soup = BeautifulSoup(html, "html.parser")
                    cards = soup.select("div.card-wrap")
                    logging.info(f"Search page {page_index} returned {len(cards)} cards.")

                    for card in cards:
                        try:
                            # 文本
                            txt = card.select_one("p.txt")
                            if not txt:
                                continue
                            content = txt.get_text(strip=True) or ""

                            # 作者
                            name_node = card.select_one(".content .info .name")
                            author = name_node.get_text(strip=True) if name_node else ""

                            # 时间 与 详情链接
                            detail_url, timestamp = "", ""
                            from_node = card.select_one(".content .from")
                            if from_node:
                                a = from_node.select_one("a")
                                if a:
                                    href = a.get("href", "")
                                    if href.startswith("//"):
                                        detail_url = "https:" + href
                                    elif href.startswith("http"):
                                        detail_url = href
                                    else:
                                        detail_url = urljoin("https://weibo.com", href)
                                    timestamp = a.get_text(strip=True)

                            if not detail_url or detail_url in seen_detail_urls:
                                continue

                            # 转评赞
                            actions = card.select(".card-act ul li a")
                            forwards = _cn_number_to_int(actions[0].get_text(strip=True)) if len(actions) > 0 else 0
                            comments = _cn_number_to_int(actions[1].get_text(strip=True)) if len(actions) > 1 else 0
                            likes = _cn_number_to_int(actions[2].get_text(strip=True)) if len(actions) > 2 else 0

                            seen_detail_urls.add(detail_url)
                            initial_posts.append({
                                "author": author,
                                "content": content,
                                "timestamp": timestamp,
                                "source": "",
                                "forwards_count": forwards,
                                "comments_count": comments,
                                "likes_count": likes,
                                "image_links": [],
                                "video_link": "",
                                "detail_url": detail_url
                            })

                            if len(initial_posts) >= MAX_POSTS_TO_FETCH:
                                return True
                        except Exception:
                            continue
                    return False

                # 第1页
                await collect_from_page(page, 1)

                # 后续页
                page_idx = 2
                while len(initial_posts) < MAX_POSTS_TO_FETCH and page_idx <= MAX_SEARCH_PAGES:
                    page_url = page_urls[page_idx - 1]
                    logging.info(f"Fetching additional search page {page_idx}: {page_url}")
                    extra = await context.new_page()
                    page_completed = False
                    try:
                        await extra.goto(page_url, wait_until="load", timeout=60000)
                        await extra.wait_for_selector("div.card-wrap", timeout=10000)
                        page_completed = await collect_from_page(extra, page_idx)
                    except Exception as exc:
                        logging.warning(f"Failed to load search page {page_idx}: {exc}")
                    finally:
                        try:
                            await extra.close()
                        except Exception:
                            pass
                    if page_completed:
                        break
                    page_idx += 1

                # 关闭搜索页
                try:
                    await page.close()
                except Exception:
                    pass

                logging.info(f"列表页抓取完成：共 {len(initial_posts)} 条，开始抓取详情页...")

                # 详情页并发抓取（适度并发，避免过快）
                sem = asyncio.Semaphore(4)

                async def wrap_detail(d):
                    async with sem:
                        return await _get_post_details(context, d.pop("detail_url"), d)

                tasks = [wrap_detail(d) for d in initial_posts]
                results = await asyncio.gather(*tasks)
                posts = [r for r in results if r is not None]

            except Exception:
                logging.error(f"抓取话题 '{topic_title}' 时发生错误", exc_info=True)
                posts = []
            finally:
                try:
                    await context.close()
                except Exception:
                    pass

            break  # 第一轮成功就跳出 while

    logging.info(f"成功抓取到 {len(posts)} 条关于 '{topic_title}' 的微博详情")
    return posts



# ---------------- 入口 / 测试 ----------------
if __name__ == '__main__':
    async def main():
        test_topic = "卢浮宫劫案可能是粉红豹所为"
        print(f"正在测试：获取话题 '{test_topic}' 的前{MAX_POSTS_TO_FETCH} 条热门微博...")
        top_posts = await get_top_20_hot_posts(test_topic)

        if top_posts:
            print(f"\n成功获取 {len(top_posts)} 条微博：")
            posts_as_dicts = [post.to_dict() for post in top_posts]
            print(json.dumps(posts_as_dicts, ensure_ascii=False, indent=2))
        else:
            print("未能获取到任何微博内容。")


    asyncio.run(main())