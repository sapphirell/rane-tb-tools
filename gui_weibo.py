# -*- coding: utf-8 -*-
"""
微博采集 GUI 版

实现目标：
1. 复用本地 weiboSpider 的解析能力；
2. 只采集博主原创微博，不采集转发内容；
3. 写入 spider_log，并通过 origin_type='weibo' 与 xhs 采集区分。
"""

import hashlib
import json
import logging
import queue
import random
import re
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from tkinter import messagebox, scrolledtext, simpledialog
from typing import Callable, Dict, List, Optional, Tuple

import pymysql
import tkinter as tk
from tkinter import ttk


LOG_ROTATE_MAX_BYTES = 5 * 1024 * 1024
LOG_ROTATE_BACKUP_COUNT = 4
LOG_SIZE_REFRESH_INTERVAL_MS = 5000
HOT_RECENT_DAYS = 7
REVERSE_COOLDOWN_HOURS = 36
SUPPORTED_WEIBO_HOSTS = {"weibo.com", "m.weibo.cn", "weibo.cn"}


class CrawlMode:
    """微博采集模式。"""

    HOT = "hot"
    REVERSE = "reverse"
    FULL = "full"


CRAWL_MODE_OPTIONS: List[Tuple[str, str, str]] = [
    (CrawlMode.HOT, "采集热门", "优先采集近 7 天里已经频繁更新过的品牌。"),
    (CrawlMode.REVERSE, "倒序采集", "只采集 36 小时内没跑过微博采集的品牌。"),
    (CrawlMode.FULL, "全量采集", "按历史微博采集时间排序，完整跑一轮品牌列表。"),
]
CRAWL_MODE_LABEL_MAP = {mode: label for mode, label, _ in CRAWL_MODE_OPTIONS}
CRAWL_MODE_VALUE_BY_LABEL = {label: mode for mode, label, _ in CRAWL_MODE_OPTIONS}
CRAWL_MODE_HINT_MAP = {mode: hint for mode, _, hint in CRAWL_MODE_OPTIONS}


def get_runtime_base_dir() -> Path:
    """返回运行时目录。打包后使用 exe 同级目录，源码模式使用脚本目录。"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = get_runtime_base_dir()
TMP_DIR = RUNTIME_DIR / "tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = RUNTIME_DIR / "gui_weibo.log"
CONFIG_PATH = TMP_DIR / "gui_weibo_config.json"
WEIBO_SPIDER_DIR = BASE_DIR / "weiboSpider"

if str(WEIBO_SPIDER_DIR) not in sys.path:
    sys.path.insert(0, str(WEIBO_SPIDER_DIR))

IMPORT_ERROR: Optional[Exception] = None
IndexParser = None
PageParser = None
handle_html = None

try:
    from weibo_spider.parser.index_parser import IndexParser
    from weibo_spider.parser.page_parser import PageParser
    from weibo_spider.parser.util import handle_html
except Exception as exc:  # pragma: no cover - 仅在本地缺依赖时触发
    IMPORT_ERROR = exc


def build_logger() -> logging.Logger:
    """构建脚本日志。"""
    logger = logging.getLogger("gui_weibo")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = RotatingFileHandler(
        LOG_PATH,
        maxBytes=LOG_ROTATE_MAX_BYTES,
        backupCount=LOG_ROTATE_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


LOGGER = build_logger()


class CookieExpiredError(RuntimeError):
    """微博 Cookie 不可用。"""


class WeiboPictureSettingError(RuntimeError):
    """微博账号图片显示设置不正确。"""


def default_profile_settings() -> Dict[str, str]:
    """返回账号默认采集配置。"""
    return {
        "brand_keyword": "",
        "brand_limit": "0",
        "default_days": "365",
        "overlap_days": "1",
        "page_limit_per_brand": "0",
        "page_sleep_min": "0.8",
        "page_sleep_max": "1.8",
        "crawl_mode": CrawlMode.FULL,
    }


def normalize_profile_settings(raw: Optional[Dict]) -> Dict[str, str]:
    """归一化账号采集配置。"""
    settings = default_profile_settings()
    if isinstance(raw, dict):
        for key in settings.keys():
            value = raw.get(key)
            if value is not None:
                settings[key] = str(value).strip()

    crawl_mode = settings.get("crawl_mode") or CrawlMode.FULL
    if crawl_mode in CRAWL_MODE_VALUE_BY_LABEL:
        crawl_mode = CRAWL_MODE_VALUE_BY_LABEL[crawl_mode]
    settings["crawl_mode"] = crawl_mode if crawl_mode in CRAWL_MODE_LABEL_MAP else CrawlMode.FULL
    for key in settings.keys():
        if key == "crawl_mode":
            continue
        settings[key] = str(settings.get(key) or "").strip()
    return settings


def default_local_state() -> Dict:
    """返回本地配置默认值。"""
    return {
        "selected_profile": "",
        "draft_cookie": "",
        "draft_settings": default_profile_settings(),
        "profiles": [],
    }


def normalize_profile_entry(raw: Dict) -> Optional[Dict]:
    """归一化单个账号配置。"""
    if not isinstance(raw, dict):
        return None

    name = str(raw.get("name") or "").strip()
    if not name:
        return None

    cookie = str(raw.get("cookie") or "").strip()
    settings = normalize_profile_settings(raw.get("settings"))
    return {
        "name": name,
        "cookie": cookie,
        "settings": settings,
    }


def load_local_state() -> Dict:
    """读取 GUI 本地配置。兼容旧版平铺结构。"""
    state = default_local_state()
    if not CONFIG_PATH.exists():
        return state

    try:
        data = json.loads(CONFIG_PATH.read_text("utf-8"))
    except Exception as exc:
        LOGGER.warning("读取本地配置失败: %s", exc)
        return state

    if not isinstance(data, dict):
        return state

    legacy_settings = {
        key: data.get(key)
        for key in default_profile_settings().keys()
        if key in data
    }
    draft_settings = data.get("draft_settings")
    state["draft_settings"] = normalize_profile_settings(draft_settings or legacy_settings)
    state["draft_cookie"] = str(data.get("draft_cookie") or data.get("cookie") or "").strip()

    seen_names = set()
    for raw_profile in data.get("profiles") or []:
        profile = normalize_profile_entry(raw_profile)
        if not profile:
            continue
        if profile["name"] in seen_names:
            continue
        seen_names.add(profile["name"])
        state["profiles"].append(profile)

    selected_profile = str(data.get("selected_profile") or "").strip()
    if selected_profile in seen_names:
        state["selected_profile"] = selected_profile
    return state


def save_local_state(state: Dict) -> None:
    """保存 GUI 本地配置。"""
    try:
        CONFIG_PATH.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        LOGGER.warning("保存本地配置失败: %s", exc)


def normalize_text(text: str) -> str:
    """清洗正文中的多余空白。"""
    value = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    value = re.sub(r"[ \t]+\n", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    value = re.sub(r"[ \t]{2,}", " ", value)
    return value.strip()


def build_title(content: str) -> str:
    """根据微博正文生成 spider_log 的标题。"""
    text = normalize_text(content)
    if not text:
        return "微博动态"
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), text)
    if len(first_line) > 60:
        return first_line[:60] + "..."
    return first_line


def parse_publish_time_to_unix(value: str) -> int:
    """将微博发布时间转换为 Unix 秒。"""
    text = str(value or "").strip()
    if not text:
        return 0

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return int(datetime.strptime(text, fmt).timestamp())
        except ValueError:
            continue
    return 0


def normalize_weibo_url(raw_url: str) -> str:
    """规范化微博主页链接，仅用于展示与日志。"""
    text = str(raw_url or "").strip().replace("&amp;", "&")
    if not text:
        return ""
    if not text.startswith(("http://", "https://")):
        text = "https://" + text.lstrip("/")

    try:
        parsed = urllib.parse.urlsplit(text)
    except Exception:
        return text

    scheme = "https"
    host = (parsed.netloc or "").strip().lower()
    host = host.removeprefix("www.")
    path = re.sub(r"/{2,}", "/", parsed.path or "").rstrip("/")
    return urllib.parse.urlunsplit((scheme, host, path, "", ""))


def extract_weibo_host(raw_url: str) -> str:
    """提取并返回微博链接的 host。"""
    text = str(raw_url or "").strip().replace("&amp;", "&")
    if not text:
        return ""
    if not text.startswith(("http://", "https://")):
        text = "https://" + text.lstrip("/")

    try:
        parsed = urllib.parse.urlsplit(text)
    except Exception:
        return ""

    return (parsed.netloc or "").strip().lower().removeprefix("www.")


def is_supported_weibo_url(raw_url: str) -> bool:
    """判断链接是否属于支持的微博域名。"""
    host = extract_weibo_host(raw_url)
    return host in SUPPORTED_WEIBO_HOSTS


def build_weibo_user_uri_candidates(raw_url: str) -> List[str]:
    """从品牌微博链接中提取 weiboSpider 可接受的 user_uri 候选值。"""
    text = str(raw_url or "").strip().replace("&amp;", "&")
    if not text:
        return []
    if not text.startswith(("http://", "https://")):
        text = "https://" + text.lstrip("/")

    candidates: List[str] = []

    def add(candidate: str) -> None:
        value = str(candidate or "").strip().strip("/")
        if value and value not in candidates:
            candidates.append(value)

    try:
        parsed = urllib.parse.urlsplit(text)
    except Exception:
        add(text)
        return candidates

    host = (parsed.netloc or "").strip().lower().removeprefix("www.")
    if host and host not in SUPPORTED_WEIBO_HOSTS:
        return []

    query = urllib.parse.parse_qs(parsed.query or "")
    uid_from_query = (query.get("uid") or [""])[0].strip()
    if uid_from_query:
        add(f"u/{uid_from_query}")
        add(uid_from_query)

    parts = [urllib.parse.unquote(part) for part in (parsed.path or "").split("/") if part]
    if not parts:
        return candidates

    first = parts[0]
    second = parts[1] if len(parts) > 1 else ""

    if first == "u" and second:
        add(f"u/{second}")
        add(second)
        return candidates

    if first == "n" and second:
        # /n/<昵称> 常见于 @提及链接，优先尝试昵称主页本身，再回退 /n/ 形式。
        add(second)
        add(f"n/{second}")
        return candidates

    if first == "p" and second:
        add(f"p/{second}")
        if second.startswith("100505") and len(second) > 6:
            real_uid = second[6:]
            add(f"u/{real_uid}")
            add(real_uid)
        return candidates

    if first == "profile" and second:
        add(f"u/{second}")
        add(second)
        return candidates

    if first not in {"comment", "status", "detail"}:
        if first.isdigit():
            add(f"u/{first}")
        add(first)

    return candidates


def build_weibo_entry_url(user_uri: str) -> str:
    """将 user_uri 转为可直接打开的 weibo.cn 主页入口。"""
    safe_uri = urllib.parse.quote(str(user_uri or "").strip().strip("/"), safe="/")
    return f"https://weibo.cn/{safe_uri}"


def should_try_entry_resolution(user_uri: str) -> bool:
    """判断该 user_uri 是否适合回退到主页入口解析。"""
    value = str(user_uri or "").strip().strip("/")
    if not value:
        return False
    return not value.startswith("u/")


def extract_selector_title(selector) -> str:
    """从页面 selector 中提取 title，便于定位主页不可访问原因。"""
    if selector is None:
        return ""
    try:
        titles = selector.xpath("//title/text()")
    except Exception:
        return ""
    if not titles:
        return ""
    return str(titles[0] or "").strip()


def split_images(raw_images: str) -> List[str]:
    """将逗号分隔的图片串转为列表。"""
    out: List[str] = []
    for item in str(raw_images or "").split(","):
        url = item.strip()
        if url and url != "无" and url not in out:
            out.append(url)
    return out


def compute_log_total_size() -> int:
    """计算主日志与轮转日志总占用。"""
    total = 0
    for path in sorted(LOG_PATH.parent.glob(f"{LOG_PATH.name}*")):
        if path.is_file():
            try:
                total += path.stat().st_size
            except Exception:
                pass
    return total


def clear_log_files() -> None:
    """清空主日志和轮转日志。"""
    for handler in LOGGER.handlers:
        if isinstance(handler, RotatingFileHandler):
            handler.acquire()
            try:
                stream = getattr(handler, "stream", None)
                if stream:
                    stream.seek(0)
                    stream.truncate()
                    stream.flush()
            finally:
                handler.release()

    for path in LOG_PATH.parent.glob(f"{LOG_PATH.name}.*"):
        if not path.is_file():
            continue
        try:
            path.unlink()
        except Exception:
            pass


class DatabaseManager:
    """微博采集所需的数据库能力。"""

    def __init__(self) -> None:
        self._conn_args = dict(
            host="111.229.182.88",
            port=3306,
            user="root",
            password="s*xNvd%v@",
            database="sukitime",
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
        )
        self.connection = pymysql.connect(**self._conn_args)
        self._has_likes_col_spider: Optional[bool] = None

    def close(self) -> None:
        """关闭数据库连接。"""
        try:
            if self.connection:
                self.connection.close()
        except Exception:
            pass

    def _ensure_conn(self) -> None:
        """确保数据库连接可用。"""
        try:
            self.connection.ping(reconnect=True)
        except Exception:
            self.connection = pymysql.connect(**self._conn_args)

    def _exec(self, sql: str, params=None):
        """执行 SQL。"""
        self._ensure_conn()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params or ())
                return cursor, cursor.rowcount
        except Exception:
            self._ensure_conn()
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params or ())
                return cursor, cursor.rowcount

    @staticmethod
    def _normalize_url_for_md5(url: str) -> str:
        """统一 URL 以便做去重哈希。"""
        text = str(url or "").strip()
        if not text:
            return ""
        try:
            parsed = urllib.parse.urlsplit(text)
            scheme = (parsed.scheme or "").lower()
            netloc = (parsed.netloc or "").lower()
            path = parsed.path or ""
            if path != "/" and path.endswith("/"):
                path = path.rstrip("/")
            normalized = urllib.parse.urlunsplit((scheme, netloc, path, "", ""))
            return normalized or text.split("?", 1)[0].split("#", 1)[0].strip()
        except Exception:
            return text.split("?", 1)[0].split("#", 1)[0].strip()

    def _calc_url_md5(self, url: str) -> str:
        """计算标准化 URL 的 md5。"""
        normalized = self._normalize_url_for_md5(url)
        if not normalized:
            return ""
        return hashlib.md5(normalized.encode("utf-8")).hexdigest()

    def _insert_with_url_md5_dedup(
        self,
        *,
        table_name: str,
        lock_name: str,
        cols: List[str],
        vals: List,
        url_md5: str,
    ) -> bool:
        """使用 url_md5 去重写入，尽量避免重复采集。"""
        lock_acquired = False
        try:
            cur, _ = self._exec("SELECT GET_LOCK(%s, 8) AS locked", (lock_name,))
            row = cur.fetchone() or {}
            lock_acquired = int(row.get("locked") or 0) == 1
        except Exception:
            lock_acquired = False

        try:
            cur, _ = self._exec(
                f"SELECT 1 FROM {table_name} WHERE url_md5 = %s LIMIT 1",
                (url_md5,),
            )
            if cur.fetchone():
                return False
            sql = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))})"
            _, affected = self._exec(sql, tuple(vals))
            return int(affected or 0) > 0
        finally:
            if lock_acquired:
                try:
                    self._exec("SELECT RELEASE_LOCK(%s)", (lock_name,))
                except Exception:
                    pass

    def _check_likes_col_spider(self) -> bool:
        """检测 spider_log 是否已包含 likes 字段。"""
        if self._has_likes_col_spider is not None:
            return self._has_likes_col_spider

        try:
            cur, _ = self._exec("SHOW COLUMNS FROM spider_log LIKE 'likes'")
            self._has_likes_col_spider = bool(cur.fetchone())
        except Exception:
            self._has_likes_col_spider = False
        return self._has_likes_col_spider

    def fetch_brand_targets(self, crawl_mode: str, keyword: str = "", limit: int = 0) -> List[Dict]:
        """按指定模式读取微博采集品牌列表。"""
        recent_threshold = int(time.time()) - HOT_RECENT_DAYS * 24 * 3600
        reverse_threshold = int(time.time()) - REVERSE_COOLDOWN_HOURS * 3600

        sql = """
            SELECT
                b.id,
                b.brand_name,
                b.weibo_url,
                COALESCE(w.recent_post_count, 0) AS recent_post_count,
                COALESCE(w.last_weibo_crawl_at, 0) AS last_weibo_crawl_at
            FROM brand b
            LEFT JOIN (
                SELECT
                    brand_id,
                    MAX(created_at) AS last_weibo_crawl_at,
                    SUM(CASE WHEN auth_time >= %s THEN 1 ELSE 0 END) AS recent_post_count
                FROM spider_log
                WHERE origin_type = 'weibo'
                GROUP BY brand_id
            ) w ON w.brand_id = b.id
            WHERE b.is_delete = 0
              AND b.is_brand = 1
              AND TRIM(COALESCE(b.weibo_url, '')) != ''
        """
        params: List[object] = [recent_threshold]

        keyword = str(keyword or "").strip()
        if keyword:
            sql += " AND b.brand_name LIKE %s"
            params.append(f"%{keyword}%")

        if crawl_mode == CrawlMode.HOT:
            sql += """
                AND COALESCE(w.recent_post_count, 0) >= 2
                ORDER BY COALESCE(w.recent_post_count, 0) DESC, COALESCE(w.last_weibo_crawl_at, 0) ASC, b.id DESC
            """
        elif crawl_mode == CrawlMode.REVERSE:
            sql += """
                AND (w.last_weibo_crawl_at IS NULL OR w.last_weibo_crawl_at < %s)
                ORDER BY COALESCE(w.last_weibo_crawl_at, 0) DESC, b.id DESC
            """
            params.append(reverse_threshold)
        else:
            sql += " ORDER BY COALESCE(w.last_weibo_crawl_at, 0) DESC, b.id DESC"

        if limit > 0:
            sql += " LIMIT %s"
            params.append(limit)

        cur, _ = self._exec(sql, tuple(params))
        return cur.fetchall() or []

    def fetch_latest_weibo_auth_time(self, brand_id: int) -> int:
        """获取品牌已采集微博中的最新发布时间。"""
        cur, _ = self._exec(
            """
            SELECT MAX(auth_time) AS auth_time
            FROM spider_log
            WHERE brand_id = %s
              AND origin_type = 'weibo'
            """,
            (brand_id,),
        )
        row = cur.fetchone() or {}
        return int(row.get("auth_time") or 0)

    def insert_brand_log(self, data: Dict) -> bool:
        """写入 spider_log。"""
        raw_url = str(data.get("url") or "").strip()
        url = self._normalize_url_for_md5(raw_url)
        if not url:
            return False

        url_md5 = self._calc_url_md5(url)
        title = build_title(str(data.get("title") or data.get("content") or ""))[:255]
        content = normalize_text(str(data.get("content") or ""))[:2000]
        images = ",".join(data.get("images") or [])[:2000]
        like_count = int(data.get("like_count") or 0)
        now = int(time.time())

        cols = [
            "msg_type",
            "status",
            "origin_type",
            "title",
            "content",
            "url",
            "images",
            "brand_id",
            "brand_name",
            "auth_time",
            "created_at",
            "updated_at",
            "url_md5",
        ]
        vals = [
            0,
            0,
            "weibo",
            title,
            content,
            url,
            images,
            int(data.get("brand_id") or 0),
            str(data.get("brand_name") or ""),
            int(data.get("auth_time") or 0),
            now,
            now,
            url_md5,
        ]

        if self._check_likes_col_spider():
            cols.append("likes")
            vals.append(like_count)

        inserted = self._insert_with_url_md5_dedup(
            table_name="spider_log",
            lock_name="weibo_spider_log_url_md5_lock",
            cols=cols,
            vals=vals,
            url_md5=url_md5,
        )
        if not inserted:
            LOGGER.info("微博 spider_log 重复跳过（url_md5=%s, url=%s）", url_md5, url)
        return inserted


class WeiboSpiderRunner:
    """微博采集执行器。"""

    def __init__(
        self,
        *,
        db: DatabaseManager,
        cookie: str,
        crawl_mode: str,
        default_days: int,
        overlap_days: int,
        brand_keyword: str,
        brand_limit: int,
        page_limit_per_brand: int,
        page_sleep_min: float,
        page_sleep_max: float,
        stop_event: threading.Event,
        log_func: Callable[[str, str], None],
        status_func: Callable[[str], None],
        progress_func: Callable[[int, int, str], None],
        sleep_func: Callable[[str, float, float], None],
    ) -> None:
        self.db = db
        self.cookie = cookie.strip()
        self.crawl_mode = crawl_mode if crawl_mode in CRAWL_MODE_LABEL_MAP else CrawlMode.FULL
        self.default_days = max(1, int(default_days or 365))
        self.overlap_days = max(0, int(overlap_days or 1))
        self.brand_keyword = brand_keyword.strip()
        self.brand_limit = max(0, int(brand_limit or 0))
        self.page_limit_per_brand = max(0, int(page_limit_per_brand or 0))
        self.page_sleep_min = max(0.0, float(page_sleep_min or 0.0))
        self.page_sleep_max = max(self.page_sleep_min, float(page_sleep_max or 0.0))
        self.stop_event = stop_event
        self.log = log_func
        self.set_status = status_func
        self.update_progress = progress_func
        self.on_sleep = sleep_func

    def emit_log(self, message: str, level: str = "INFO") -> None:
        """输出运行日志。"""
        self.log(str(message or "").strip(), level)

    def check_stop(self) -> None:
        """检查是否收到停止信号。"""
        if self.stop_event.is_set():
            raise KeyboardInterrupt("采集已停止")

    def resolve_since_date(self, latest_auth_time: int) -> str:
        """根据历史采集时间生成本次 since_date。"""
        if latest_auth_time > 0:
            since_dt = datetime.fromtimestamp(latest_auth_time) - timedelta(days=self.overlap_days)
        else:
            since_dt = datetime.now() - timedelta(days=self.default_days)
        return since_dt.strftime("%Y-%m-%d")

    def resolve_profile(self, weibo_url: str) -> Tuple[str, object, int]:
        """根据品牌微博主页链接解析 user_uri、用户信息与总页数。"""
        if not is_supported_weibo_url(weibo_url):
            raise ValueError(f"微博主页链接无效，请填写微博主页链接：{weibo_url}")

        last_error: Optional[Exception] = None
        attempt_errors: List[str] = []
        candidates = build_weibo_user_uri_candidates(weibo_url)
        if not candidates:
            raise ValueError(f"无法从微博链接中提取主页信息：{weibo_url}")

        for candidate in candidates:
            self.check_stop()
            try:
                parser = IndexParser(self.cookie, candidate)
                user = parser.get_user()
                if not user or not getattr(user, "id", ""):
                    page_title = extract_selector_title(getattr(parser, "selector", None))
                    if page_title:
                        attempt_errors.append(f"{candidate} => 页面标题：{page_title}")
                    else:
                        attempt_errors.append(f"{candidate} => 未解析出用户")
                    continue
                page_num = int(parser.get_page_num() or 1)
                return str(user.id), user, max(1, page_num)
            except SystemExit as exc:
                raise CookieExpiredError("微博 Cookie 无效或已过期，请重新从 weibo.cn 复制 Cookie。") from exc
            except Exception as exc:
                last_error = exc
                attempt_errors.append(f"{candidate} => {type(exc).__name__}: {exc}")

            if not should_try_entry_resolution(candidate):
                continue

            self.check_stop()
            try:
                entry_selector = handle_html(self.cookie, build_weibo_entry_url(candidate))
                if entry_selector is None:
                    attempt_errors.append(f"{candidate} => 入口页为空")
                    continue
                parser = IndexParser(self.cookie, candidate, selector=entry_selector)
                user = parser.get_user()
                if not user or not getattr(user, "id", ""):
                    page_title = extract_selector_title(entry_selector)
                    if page_title:
                        attempt_errors.append(f"{candidate} => 入口标题：{page_title}")
                    else:
                        attempt_errors.append(f"{candidate} => 入口页未解析出用户")
                    continue
                page_num = int(parser.get_page_num() or 1)
                return str(user.id), user, max(1, page_num)
            except SystemExit as exc:
                raise CookieExpiredError("微博 Cookie 无效或已过期，请重新从 weibo.cn 复制 Cookie。") from exc
            except Exception as exc:
                last_error = exc
                attempt_errors.append(f"{candidate} => 入口回退失败 {type(exc).__name__}: {exc}")

        if last_error:
            details = "；".join(attempt_errors[-3:]) if attempt_errors else f"{type(last_error).__name__}: {last_error}"
            raise ValueError(f"无法访问微博主页：{weibo_url}（{details}）") from last_error

        if attempt_errors:
            raise ValueError(f"无法访问微博主页：{weibo_url}（{'；'.join(attempt_errors[-3:])}）")
        raise ValueError(f"无法访问微博主页：{weibo_url}")

    def build_log_payload(self, brand_row: Dict, weibo) -> Dict:
        """将 weiboSpider 的微博对象转换为 spider_log 入库格式。"""
        content = normalize_text(getattr(weibo, "content", "") or "")
        images = list(getattr(weibo, "original_pictures_list", []) or [])
        if not images:
            images = split_images(getattr(weibo, "original_pictures", ""))
        url = f"https://weibo.cn/comment/{getattr(weibo, 'id', '')}"

        return {
            "brand_id": int(brand_row.get("id") or 0),
            "brand_name": str(brand_row.get("brand_name") or ""),
            "title": build_title(content),
            "content": content,
            "url": url,
            "images": images,
            "auth_time": parse_publish_time_to_unix(getattr(weibo, "publish_time", "")),
            "like_count": int(getattr(weibo, "up_num", 0) or 0),
        }

    def sleep_with_progress(self, phase: str, total: float) -> None:
        """页间等待并同步更新等待读条。"""
        total = max(0.0, float(total))
        self.on_sleep(phase, 0.0, total)
        if total <= 0:
            return

        elapsed = 0.0
        step = 0.1
        while elapsed < total:
            self.check_stop()
            time.sleep(min(step, total - elapsed))
            elapsed = min(total, elapsed + step)
            self.on_sleep(phase, elapsed, total)

    def crawl_one_brand(self, row: Dict) -> Dict[str, int]:
        """采集单个品牌微博。"""
        brand_id = int(row.get("id") or 0)
        brand_name = str(row.get("brand_name") or "").strip() or f"品牌{brand_id}"
        weibo_url = normalize_weibo_url(str(row.get("weibo_url") or ""))
        if not weibo_url:
            raise ValueError("微博主页地址为空")

        latest_auth_time = self.db.fetch_latest_weibo_auth_time(brand_id)
        since_date = self.resolve_since_date(latest_auth_time)
        user_uri, user, total_page_num = self.resolve_profile(weibo_url)
        effective_page_num = total_page_num
        if self.page_limit_per_brand > 0:
            effective_page_num = min(total_page_num, self.page_limit_per_brand)

        recent_post_count = int(row.get("recent_post_count") or 0)
        last_weibo_crawl_at = int(row.get("last_weibo_crawl_at") or 0)
        last_crawl_text = (
            datetime.fromtimestamp(last_weibo_crawl_at).strftime("%Y-%m-%d %H:%M")
            if last_weibo_crawl_at > 0
            else "从未"
        )
        self.emit_log(
            f"[{brand_name}] 微博主页：{weibo_url} | 用户：{getattr(user, 'nickname', '')}({getattr(user, 'id', '')})"
            f" | 从 {since_date} 开始采集 | 近7天已采微博 {recent_post_count} 条 | 上次采集 {last_crawl_text}"
        )
        if effective_page_num < total_page_num:
            self.emit_log(
                f"[{brand_name}] 本次已限制最多采集 {effective_page_num} 页，微博主页实际共有 {total_page_num} 页"
            )

        stats = {
            "seen": 0,
            "inserted": 0,
            "duplicates": 0,
            "pages": 0,
        }
        user_config = {
            "user_uri": user_uri,
            "since_date": since_date,
            "end_date": "now",
        }
        weibo_id_list: List[str] = []

        for page in range(1, effective_page_num + 1):
            self.check_stop()
            stats["pages"] += 1
            self.set_status(f"运行中：{brand_name}（第 {page}/{effective_page_num} 页）")

            try:
                parser = PageParser(self.cookie, user_config, page, 1)
                page_result = parser.get_one_page(weibo_id_list)
            except SystemExit as exc:
                raise WeiboPictureSettingError(
                    "当前微博账号设置为不显示图片，请在 https://weibo.cn/account/customize/pic 中改成“显示”。"
                ) from exc

            if not page_result:
                self.emit_log(f"[{brand_name}] 第 {page} 页未返回有效内容，结束当前品牌采集")
                break

            weibos, weibo_id_list, to_continue = page_result
            page_inserted = 0
            page_duplicates = 0

            for weibo in weibos or []:
                self.check_stop()
                stats["seen"] += 1
                payload = self.build_log_payload(row, weibo)
                inserted = self.db.insert_brand_log(payload)
                if inserted:
                    stats["inserted"] += 1
                    page_inserted += 1
                else:
                    stats["duplicates"] += 1
                    page_duplicates += 1

            self.emit_log(
                f"[{brand_name}] 第 {page}/{effective_page_num} 页完成：发现 {len(weibos or [])} 条，新增 {page_inserted} 条，重复 {page_duplicates} 条"
            )

            if not to_continue:
                break

            if page < effective_page_num:
                sleep_seconds = self.page_sleep_min
                if self.page_sleep_max > self.page_sleep_min:
                    sleep_seconds = random.uniform(self.page_sleep_min, self.page_sleep_max)
                self.sleep_with_progress("page", sleep_seconds)

        self.emit_log(
            f"[{brand_name}] 采集完成：共浏览 {stats['pages']} 页，发现 {stats['seen']} 条，新增 {stats['inserted']} 条"
        )
        return stats

    def run(self) -> Dict[str, int]:
        """执行全部品牌采集。"""
        self.emit_log(f"开始读取品牌列表，采集模式：{CRAWL_MODE_LABEL_MAP.get(self.crawl_mode, self.crawl_mode)}")
        rows = self.db.fetch_brand_targets(self.crawl_mode, self.brand_keyword, self.brand_limit)
        self.update_progress(0, len(rows), "")
        if not rows:
            self.set_status("没有可采集的微博品牌")
            self.emit_log("没有找到可采集的微博品牌。")
            return {
                "brands_total": 0,
                "brands_success": 0,
                "brands_failed": 0,
                "seen": 0,
                "inserted": 0,
                "duplicates": 0,
            }

        self.emit_log(f"本次待采集品牌：{len(rows)} 个")
        summary = {
            "brands_total": len(rows),
            "brands_success": 0,
            "brands_failed": 0,
            "seen": 0,
            "inserted": 0,
            "duplicates": 0,
        }

        for index, row in enumerate(rows, start=1):
            self.check_stop()
            brand_name = str(row.get("brand_name") or "").strip() or f"品牌{row.get('id')}"
            self.emit_log(f"---------- {index}/{len(rows)} 开始采集：{brand_name} ----------")
            try:
                stats = self.crawl_one_brand(row)
                summary["brands_success"] += 1
                summary["seen"] += stats["seen"]
                summary["inserted"] += stats["inserted"]
                summary["duplicates"] += stats["duplicates"]
            except KeyboardInterrupt:
                self.emit_log("已收到停止指令，结束本次采集。")
                raise
            except (CookieExpiredError, WeiboPictureSettingError):
                raise
            except Exception as exc:
                summary["brands_failed"] += 1
                self.emit_log(f"[{brand_name}] 采集失败：{exc}", level="ERROR")
            finally:
                self.update_progress(index, len(rows), brand_name)

        self.emit_log(
            "全部任务结束："
            f"品牌成功 {summary['brands_success']} 个，"
            f"失败 {summary['brands_failed']} 个，"
            f"发现微博 {summary['seen']} 条，"
            f"新增 {summary['inserted']} 条，"
            f"重复 {summary['duplicates']} 条"
        )
        return summary


class App:
    """微博采集桌面界面。"""

    def __init__(self, master: tk.Tk) -> None:
        self.master = master
        self.master.title("微博采集")
        self.master.geometry("1040x860")
        self.master.minsize(980, 760)

        self.logger = LOGGER
        self.event_queue: "queue.Queue[Tuple[str, object]]" = queue.Queue()
        self.worker_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self._queue_after_id: Optional[str] = None
        self._tick_after_id: Optional[str] = None
        self._log_size_after_id: Optional[str] = None
        self.start_ts: Optional[float] = None
        self.total_rows = 0
        self.done_rows = 0

        self.local_state = load_local_state()
        self.current_profile_name = ""

        self.var_status = tk.StringVar(value="准备就绪")
        self.var_duration = tk.StringVar(value="已用时：00:00:00")
        self.var_progress_text = tk.StringVar(value="进度：0 / 0 (0.0%)")
        self.var_current_brand = tk.StringVar(value="当前对象：-")
        self.var_sleep_text = tk.StringVar(value="当前无等待")
        self.var_log_size = tk.StringVar(value="日志占用：0 B")
        self.var_mode_hint = tk.StringVar(value="")

        self.var_profile_name = tk.StringVar(value="")
        self.var_brand_keyword = tk.StringVar(value="")
        self.var_brand_limit = tk.StringVar(value="0")
        self.var_default_days = tk.StringVar(value="365")
        self.var_overlap_days = tk.StringVar(value="1")
        self.var_page_limit_per_brand = tk.StringVar(value="0")
        self.var_page_sleep_min = tk.StringVar(value="0.8")
        self.var_page_sleep_max = tk.StringVar(value="1.8")
        self.var_crawl_mode_label = tk.StringVar(value=CRAWL_MODE_LABEL_MAP[CrawlMode.FULL])

        self._build_ui()
        self._load_initial_form()
        self._schedule_event_pump()
        self._schedule_tick()
        self._schedule_log_size_refresh()
        self.master.protocol("WM_DELETE_WINDOW", self.on_close)

        if IMPORT_ERROR is not None:
            self.append_log(
                f"依赖加载失败：{IMPORT_ERROR}。请先执行 `pip install -r requirements.txt`。",
                level="ERROR",
            )
            self.var_status.set("缺少运行依赖")
            self.btn_start.config(state="disabled")

    def _build_ui(self) -> None:
        """构建界面。"""
        container = ttk.Frame(self.master, padding=16)
        container.pack(fill="both", expand=True)

        tip_label = ttk.Label(
            container,
            text="只采集品牌微博主页里的原创帖子，不采集转发内容；写入 spider_log 时会自动标记 origin_type=weibo。",
            foreground="#2f6f7f",
            wraplength=980,
        )
        tip_label.pack(anchor="w", pady=(0, 10))

        profile_frame = ttk.LabelFrame(container, text="账号管理")
        profile_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(profile_frame, text="选择账号").grid(row=0, column=0, padx=(10, 6), pady=8, sticky="w")
        self.cb_profile = ttk.Combobox(
            profile_frame,
            textvariable=self.var_profile_name,
            state="readonly",
            width=30,
        )
        self.cb_profile.grid(row=0, column=1, padx=(0, 8), pady=8, sticky="w")
        self.cb_profile.bind("<<ComboboxSelected>>", self.on_profile_selected)

        self.btn_save_profile = ttk.Button(profile_frame, text="保存/更新当前账号", command=self.save_or_update_profile)
        self.btn_save_profile.grid(row=0, column=2, padx=(0, 8), pady=8, sticky="w")
        self.btn_remove_profile = ttk.Button(profile_frame, text="移除账号", command=self.remove_profile)
        self.btn_remove_profile.grid(row=0, column=3, padx=(0, 8), pady=8, sticky="w")
        self.btn_clear_profile = ttk.Button(profile_frame, text="取消账号选择", command=self.clear_profile_selection)
        self.btn_clear_profile.grid(row=0, column=4, padx=(0, 10), pady=8, sticky="w")

        cookie_frame = ttk.LabelFrame(container, text="登录信息")
        cookie_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(cookie_frame, text="微博 Cookie").grid(row=0, column=0, sticky="nw", padx=(10, 6), pady=(10, 6))
        self.txt_cookie = scrolledtext.ScrolledText(cookie_frame, height=6, wrap="word")
        self.txt_cookie.grid(row=0, column=1, sticky="nsew", padx=(0, 10), pady=(10, 6))
        ttk.Label(
            cookie_frame,
            text="请从 weibo.cn 登录后的请求头中复制完整 Cookie。若账号设置成“不显示图片”，请先到 weibo.cn 开启图片显示。",
            foreground="#666666",
            wraplength=860,
        ).grid(row=1, column=1, sticky="w", padx=(0, 10), pady=(0, 10))
        cookie_frame.columnconfigure(1, weight=1)

        config_frame = ttk.LabelFrame(container, text="采集配置")
        config_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(config_frame, text="采集模式").grid(row=0, column=0, sticky="e", padx=(10, 6), pady=8)
        self.cb_mode = ttk.Combobox(
            config_frame,
            textvariable=self.var_crawl_mode_label,
            values=[label for _, label, _ in CRAWL_MODE_OPTIONS],
            state="readonly",
            width=16,
        )
        self.cb_mode.grid(row=0, column=1, sticky="w", pady=8)
        self.cb_mode.bind("<<ComboboxSelected>>", self.on_mode_changed)
        ttk.Label(config_frame, textvariable=self.var_mode_hint, foreground="#666666").grid(
            row=0,
            column=2,
            columnspan=6,
            sticky="w",
            padx=(10, 0),
            pady=8,
        )

        ttk.Label(config_frame, text="品牌筛选").grid(row=1, column=0, sticky="e", padx=(10, 6), pady=8)
        ttk.Entry(config_frame, textvariable=self.var_brand_keyword, width=20).grid(row=1, column=1, sticky="w", pady=8)
        ttk.Label(config_frame, text="本次最多采集").grid(row=1, column=2, sticky="e", padx=(10, 6), pady=8)
        ttk.Entry(config_frame, textvariable=self.var_brand_limit, width=10).grid(row=1, column=3, sticky="w", pady=8)
        ttk.Label(config_frame, text="首次补抓天数").grid(row=1, column=4, sticky="e", padx=(10, 6), pady=8)
        ttk.Entry(config_frame, textvariable=self.var_default_days, width=10).grid(row=1, column=5, sticky="w", pady=8)
        ttk.Label(config_frame, text="重叠回看天数").grid(row=1, column=6, sticky="e", padx=(10, 6), pady=8)
        ttk.Entry(config_frame, textvariable=self.var_overlap_days, width=10).grid(row=1, column=7, sticky="w", pady=8)

        ttk.Label(config_frame, text="每个品牌最多页数").grid(row=2, column=0, sticky="e", padx=(10, 6), pady=(0, 10))
        ttk.Entry(config_frame, textvariable=self.var_page_limit_per_brand, width=10).grid(row=2, column=1, sticky="w", pady=(0, 10))
        ttk.Label(config_frame, text="页间等待最短(秒)").grid(row=2, column=2, sticky="e", padx=(10, 6), pady=(0, 10))
        ttk.Entry(config_frame, textvariable=self.var_page_sleep_min, width=10).grid(row=2, column=3, sticky="w", pady=(0, 10))
        ttk.Label(config_frame, text="页间等待最长(秒)").grid(row=2, column=4, sticky="e", padx=(10, 6), pady=(0, 10))
        ttk.Entry(config_frame, textvariable=self.var_page_sleep_max, width=10).grid(row=2, column=5, sticky="w", pady=(0, 10))
        ttk.Label(
            config_frame,
            text="页数填 0 表示不限制；品牌数填 0 表示不限制。",
            foreground="#666666",
        ).grid(row=2, column=6, columnspan=2, sticky="w", padx=(10, 0), pady=(0, 10))

        control_frame = ttk.Frame(container)
        control_frame.pack(fill="x", pady=(0, 10))
        self.btn_start = ttk.Button(control_frame, text="开始采集", command=self.start_crawl)
        self.btn_start.pack(side="left")
        self.btn_stop = ttk.Button(control_frame, text="停止采集", command=self.stop_crawl, state="disabled")
        self.btn_stop.pack(side="left", padx=(10, 0))
        ttk.Button(control_frame, text="清空界面日志", command=self.clear_log_text).pack(side="left", padx=(10, 0))
        ttk.Button(control_frame, text="清空日志文件", command=self.clear_log_files_with_confirm).pack(side="left", padx=(10, 0))

        info_frame = ttk.Frame(container)
        info_frame.pack(fill="x")
        ttk.Label(info_frame, textvariable=self.var_status).pack(side="left")
        ttk.Label(info_frame, textvariable=self.var_duration).pack(side="left", padx=(18, 0))
        ttk.Label(info_frame, textvariable=self.var_progress_text).pack(side="left", padx=(18, 0))
        ttk.Label(info_frame, textvariable=self.var_current_brand).pack(side="left", padx=(18, 0))

        self.progress_bar = ttk.Progressbar(container, mode="determinate", maximum=100)
        self.progress_bar.pack(fill="x", pady=(6, 10))

        sleep_frame = ttk.LabelFrame(container, text="等待进度")
        sleep_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(sleep_frame, textvariable=self.var_sleep_text).pack(anchor="w", padx=10, pady=(8, 4))
        self.sleep_bar = ttk.Progressbar(sleep_frame, mode="determinate", maximum=100)
        self.sleep_bar.pack(fill="x", padx=10, pady=(0, 10))

        log_frame = ttk.LabelFrame(container, text="日志")
        log_frame.pack(fill="both", expand=True)
        log_header = ttk.Frame(log_frame)
        log_header.pack(fill="x", padx=10, pady=(8, 6))
        ttk.Label(log_header, textvariable=self.var_log_size).pack(side="left")
        self.txt_log = scrolledtext.ScrolledText(log_frame, wrap="word", state="disabled")
        self.txt_log.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    def _schedule_event_pump(self) -> None:
        """轮询事件队列。"""
        self.flush_event_queue()
        self._queue_after_id = self.master.after(120, self._schedule_event_pump)

    def flush_event_queue(self) -> None:
        """处理后台线程发回的事件。"""
        try:
            while True:
                kind, payload = self.event_queue.get_nowait()
                if kind == "log":
                    self.append_log(str(payload.get("message") or ""), level=str(payload.get("level") or "INFO"))
                elif kind == "status":
                    self.var_status.set(str(payload or ""))
                elif kind == "progress":
                    self._apply_progress(
                        int(payload.get("done") or 0),
                        int(payload.get("total") or 0),
                        str(payload.get("brand") or ""),
                    )
                elif kind == "sleep":
                    self._apply_sleep_progress(
                        str(payload.get("phase") or ""),
                        float(payload.get("elapsed") or 0.0),
                        float(payload.get("total") or 0.0),
                    )
                elif kind == "done":
                    self.on_crawl_done(payload, None)
                elif kind == "error":
                    self.on_crawl_done(None, str(payload or ""))
        except queue.Empty:
            return

    def _schedule_tick(self) -> None:
        """刷新运行时长。"""
        if self.start_ts is not None:
            elapsed = int(time.time() - self.start_ts)
            hh = elapsed // 3600
            mm = (elapsed % 3600) // 60
            ss = elapsed % 60
            self.var_duration.set(f"已用时：{hh:02d}:{mm:02d}:{ss:02d}")
        else:
            self.var_duration.set("已用时：00:00:00")
        self._tick_after_id = self.master.after(1000, self._schedule_tick)

    def _schedule_log_size_refresh(self) -> None:
        """刷新日志占用大小。"""
        self.var_log_size.set(f"日志占用：{self._format_size(compute_log_total_size())}")
        self._log_size_after_id = self.master.after(LOG_SIZE_REFRESH_INTERVAL_MS, self._schedule_log_size_refresh)

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """格式化文件大小。"""
        units = ["B", "KB", "MB", "GB", "TB"]
        value = float(max(0, size_bytes))
        for unit in units:
            if value < 1024 or unit == units[-1]:
                if unit == "B":
                    return f"{int(value)} {unit}"
                return f"{value:.1f} {unit}"
            value /= 1024
        return "0 B"

    def enqueue_event(self, kind: str, payload) -> None:
        """线程安全推送事件。"""
        self.event_queue.put((kind, payload))

    def enqueue_log(self, message: str, level: str = "INFO") -> None:
        """线程安全推送日志。"""
        self.enqueue_event("log", {"message": message, "level": level})

    def append_log(self, message: str, level: str = "INFO") -> None:
        """在界面和文件中写日志。"""
        text = str(message or "").strip()
        if not text:
            return

        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(text)

        prefix = datetime.now().strftime("%H:%M:%S")
        self.txt_log.config(state="normal")
        self.txt_log.insert("end", f"[{prefix}] {text}\n")
        self.txt_log.see("end")
        self.txt_log.config(state="disabled")

    def clear_log_text(self) -> None:
        """清空界面日志。"""
        self.txt_log.config(state="normal")
        self.txt_log.delete("1.0", "end")
        self.txt_log.config(state="disabled")

    def clear_log_files_with_confirm(self) -> None:
        """清空日志文件并保留界面。"""
        if not messagebox.askyesno("确认清空", "这会清空微博采集日志文件和历史轮转文件，确定继续吗？"):
            return
        clear_log_files()
        self.var_log_size.set(f"日志占用：{self._format_size(compute_log_total_size())}")
        messagebox.showinfo("已清空", "日志文件已经清空。")

    def _load_initial_form(self) -> None:
        """加载初始界面内容。"""
        self.refresh_profile_values()
        selected = str(self.local_state.get("selected_profile") or "").strip()
        if selected and self.get_profile(selected):
            self.load_profile_to_form(selected)
            return

        self.current_profile_name = ""
        self.var_profile_name.set("")
        self.txt_cookie.delete("1.0", "end")
        self.txt_cookie.insert("1.0", str(self.local_state.get("draft_cookie") or ""))
        self.apply_settings_to_form(self.local_state.get("draft_settings"))

    def get_profile_names(self) -> List[str]:
        """返回当前账号名列表。"""
        return [profile["name"] for profile in self.local_state.get("profiles") or []]

    def get_profile(self, name: str) -> Optional[Dict]:
        """按名称读取账号。"""
        target = str(name or "").strip()
        for profile in self.local_state.get("profiles") or []:
            if str(profile.get("name") or "").strip() == target:
                return profile
        return None

    def refresh_profile_values(self) -> None:
        """刷新账号下拉框。"""
        self.cb_profile["values"] = self.get_profile_names()

    def collect_settings_from_form(self) -> Dict[str, str]:
        """收集表单里的采集配置。"""
        crawl_mode = CRAWL_MODE_VALUE_BY_LABEL.get(
            str(self.var_crawl_mode_label.get() or "").strip(),
            CrawlMode.FULL,
        )
        return normalize_profile_settings(
            {
                "brand_keyword": self.var_brand_keyword.get().strip(),
                "brand_limit": self.var_brand_limit.get().strip() or "0",
                "default_days": self.var_default_days.get().strip() or "365",
                "overlap_days": self.var_overlap_days.get().strip() or "1",
                "page_limit_per_brand": self.var_page_limit_per_brand.get().strip() or "0",
                "page_sleep_min": self.var_page_sleep_min.get().strip() or "0.8",
                "page_sleep_max": self.var_page_sleep_max.get().strip() or "1.8",
                "crawl_mode": crawl_mode,
            }
        )

    def apply_settings_to_form(self, settings: Optional[Dict]) -> None:
        """将配置填回表单。"""
        normalized = normalize_profile_settings(settings)
        self.var_brand_keyword.set(normalized["brand_keyword"])
        self.var_brand_limit.set(normalized["brand_limit"])
        self.var_default_days.set(normalized["default_days"])
        self.var_overlap_days.set(normalized["overlap_days"])
        self.var_page_limit_per_brand.set(normalized["page_limit_per_brand"])
        self.var_page_sleep_min.set(normalized["page_sleep_min"])
        self.var_page_sleep_max.set(normalized["page_sleep_max"])
        self.var_crawl_mode_label.set(CRAWL_MODE_LABEL_MAP.get(normalized["crawl_mode"], CRAWL_MODE_LABEL_MAP[CrawlMode.FULL]))
        self.on_mode_changed()

    def collect_form_data(self) -> Dict[str, object]:
        """收集完整表单。"""
        return {
            "cookie": self.txt_cookie.get("1.0", "end").strip(),
            "settings": self.collect_settings_from_form(),
        }

    def persist_current_form(self) -> None:
        """将当前表单内容写回本地状态。"""
        payload = self.collect_form_data()
        cookie = str(payload["cookie"] or "").strip()
        settings = payload["settings"]

        if self.current_profile_name and self.get_profile(self.current_profile_name):
            profile = self.get_profile(self.current_profile_name)
            profile["cookie"] = cookie
            profile["settings"] = settings
            self.local_state["selected_profile"] = self.current_profile_name
        else:
            self.local_state["selected_profile"] = ""
            self.local_state["draft_cookie"] = cookie
            self.local_state["draft_settings"] = settings

        save_local_state(self.local_state)

    def load_profile_to_form(self, name: str) -> None:
        """按账号名称加载 Cookie 和采集配置。"""
        profile = self.get_profile(name)
        if not profile:
            return

        self.current_profile_name = profile["name"]
        self.local_state["selected_profile"] = profile["name"]
        self.var_profile_name.set(profile["name"])
        self.txt_cookie.delete("1.0", "end")
        self.txt_cookie.insert("1.0", str(profile.get("cookie") or ""))
        self.apply_settings_to_form(profile.get("settings"))
        save_local_state(self.local_state)

    def on_profile_selected(self, _event=None) -> None:
        """切换当前账号。"""
        selected = str(self.var_profile_name.get() or "").strip()
        self.persist_current_form()
        if selected and self.get_profile(selected):
            self.load_profile_to_form(selected)

    def clear_profile_selection(self) -> None:
        """取消账号选择，回到草稿态。"""
        self.persist_current_form()
        self.current_profile_name = ""
        self.local_state["selected_profile"] = ""
        self.var_profile_name.set("")
        self.txt_cookie.delete("1.0", "end")
        self.txt_cookie.insert("1.0", str(self.local_state.get("draft_cookie") or ""))
        self.apply_settings_to_form(self.local_state.get("draft_settings"))
        save_local_state(self.local_state)

    def save_or_update_profile(self) -> None:
        """保存或更新当前账号。"""
        payload = self.collect_form_data()
        cookie = str(payload["cookie"] or "").strip()
        settings = payload["settings"]
        target_name = self.current_profile_name

        if not cookie:
            messagebox.showwarning("请补充信息", "请先填写微博 Cookie，再保存账号。")
            return

        if not target_name:
            target_name = simpledialog.askstring("保存账号", "请输入账号备注名称：", parent=self.master)
            if target_name is None:
                return
            target_name = target_name.strip()
            if not target_name:
                messagebox.showwarning("提示", "账号名称不能为空。")
                return

        profile = self.get_profile(target_name)
        if profile:
            profile["cookie"] = cookie
            profile["settings"] = settings
        else:
            self.local_state.setdefault("profiles", []).append(
                {
                    "name": target_name,
                    "cookie": cookie,
                    "settings": settings,
                }
            )

        self.current_profile_name = target_name
        self.local_state["selected_profile"] = target_name
        self.refresh_profile_values()
        self.var_profile_name.set(target_name)
        save_local_state(self.local_state)
        self.append_log(f"账号 [{target_name}] 已保存。")

    def remove_profile(self) -> None:
        """移除当前选中的账号。"""
        target_name = str(self.var_profile_name.get() or "").strip()
        if not target_name or not self.get_profile(target_name):
            messagebox.showwarning("提示", "请先选择一个已保存的账号。")
            return

        if not messagebox.askyesno("确认移除", f"确认移除账号 [{target_name}]？"):
            return

        self.local_state["profiles"] = [
            profile
            for profile in self.local_state.get("profiles") or []
            if str(profile.get("name") or "").strip() != target_name
        ]
        if self.current_profile_name == target_name:
            self.current_profile_name = ""
        if str(self.local_state.get("selected_profile") or "").strip() == target_name:
            self.local_state["selected_profile"] = ""
        self.refresh_profile_values()
        self.var_profile_name.set("")
        save_local_state(self.local_state)
        self.txt_cookie.delete("1.0", "end")
        self.txt_cookie.insert("1.0", str(self.local_state.get("draft_cookie") or ""))
        self.apply_settings_to_form(self.local_state.get("draft_settings"))
        self.append_log(f"账号 [{target_name}] 已移除。")

    def on_mode_changed(self, _event=None) -> None:
        """刷新采集模式说明。"""
        mode = CRAWL_MODE_VALUE_BY_LABEL.get(str(self.var_crawl_mode_label.get() or "").strip(), CrawlMode.FULL)
        self.var_mode_hint.set(CRAWL_MODE_HINT_MAP.get(mode, ""))

    def validate_form(self) -> Dict[str, object]:
        """校验并返回运行参数。"""
        payload = self.collect_form_data()
        cookie = str(payload["cookie"] or "").strip()
        settings = payload["settings"]
        if not cookie:
            raise ValueError("请先填写微博 Cookie。")

        try:
            brand_limit = int(settings["brand_limit"] or "0")
            default_days = int(settings["default_days"] or "0")
            overlap_days = int(settings["overlap_days"] or "0")
            page_limit_per_brand = int(settings["page_limit_per_brand"] or "0")
            page_sleep_min = float(settings["page_sleep_min"] or "0")
            page_sleep_max = float(settings["page_sleep_max"] or "0")
        except ValueError as exc:
            raise ValueError("品牌数量、补抓天数、页数、等待时间都需要填写数字。") from exc

        if brand_limit < 0:
            raise ValueError("本次最多采集不能小于 0。")
        if default_days <= 0:
            raise ValueError("首次补抓天数需要大于 0。")
        if overlap_days < 0:
            raise ValueError("重叠回看天数不能小于 0。")
        if page_limit_per_brand < 0:
            raise ValueError("每个品牌最多页数不能小于 0。")
        if page_sleep_min < 0 or page_sleep_max < 0:
            raise ValueError("页间等待不能小于 0。")
        if page_sleep_max < page_sleep_min:
            raise ValueError("页间等待最长值不能小于最短值。")

        return {
            "cookie": cookie,
            "crawl_mode": settings["crawl_mode"],
            "brand_keyword": settings["brand_keyword"],
            "brand_limit": brand_limit,
            "default_days": default_days,
            "overlap_days": overlap_days,
            "page_limit_per_brand": page_limit_per_brand,
            "page_sleep_min": page_sleep_min,
            "page_sleep_max": page_sleep_max,
            "settings": settings,
        }

    def set_running_ui(self, running: bool) -> None:
        """切换运行态按钮。"""
        self.btn_start.config(state="disabled" if running else "normal")
        self.btn_stop.config(state="normal" if running else "disabled")
        self.btn_save_profile.config(state="disabled" if running else "normal")
        self.btn_remove_profile.config(state="disabled" if running else "normal")
        self.btn_clear_profile.config(state="disabled" if running else "normal")
        self.cb_profile.config(state="disabled" if running else "readonly")
        self.cb_mode.config(state="disabled" if running else "readonly")

    def _apply_progress(self, done: int, total: int, brand_name: str) -> None:
        """更新总进度。"""
        self.done_rows = max(0, done)
        self.total_rows = max(0, total)
        percent = (self.done_rows / self.total_rows * 100.0) if self.total_rows else 0.0
        self.var_progress_text.set(f"进度：{self.done_rows} / {self.total_rows} ({percent:.1f}%)")
        self.progress_bar["value"] = percent
        self.var_current_brand.set(f"当前对象：{brand_name or '-'}")

    def _apply_sleep_progress(self, phase: str, elapsed: float, total: float) -> None:
        """更新等待读条。"""
        total = max(0.0, float(total))
        elapsed = min(max(0.0, float(elapsed)), total)
        if total <= 0:
            self.var_sleep_text.set("当前无等待")
            self.sleep_bar["value"] = 0
            return

        phase_cn = "页间等待" if phase == "page" else "等待中"
        percent = (elapsed / total * 100.0) if total else 0.0
        self.var_sleep_text.set(f"{phase_cn}：{elapsed:.1f}s / {total:.1f}s")
        self.sleep_bar["value"] = percent
        if abs(elapsed - total) < 1e-6:
            self.master.after(600, lambda: self._apply_sleep_progress("", 0.0, 0.0))

    def start_crawl(self) -> None:
        """启动采集线程。"""
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "微博采集已在运行中。")
            return

        if IMPORT_ERROR is not None:
            messagebox.showerror("无法启动", "缺少运行依赖，请先执行 `pip install -r requirements.txt`。")
            return

        if not WEIBO_SPIDER_DIR.exists():
            messagebox.showerror("无法启动", "未找到 weiboSpider 目录，请先确认仓库已克隆到 spiders/weiboSpider。")
            return

        try:
            config = self.validate_form()
        except ValueError as exc:
            messagebox.showwarning("输入有误", str(exc))
            return

        self.persist_current_form()
        self.stop_event.clear()
        self.set_running_ui(True)
        self.start_ts = time.time()
        self._apply_progress(0, 0, "")
        self._apply_sleep_progress("", 0.0, 0.0)
        self.var_status.set("采集中")
        self.append_log("微博采集已启动。")

        def worker() -> None:
            db = None
            try:
                db = DatabaseManager()
                runner = WeiboSpiderRunner(
                    db=db,
                    cookie=config["cookie"],
                    crawl_mode=str(config["crawl_mode"]),
                    default_days=int(config["default_days"]),
                    overlap_days=int(config["overlap_days"]),
                    brand_keyword=str(config["brand_keyword"]),
                    brand_limit=int(config["brand_limit"]),
                    page_limit_per_brand=int(config["page_limit_per_brand"]),
                    page_sleep_min=float(config["page_sleep_min"]),
                    page_sleep_max=float(config["page_sleep_max"]),
                    stop_event=self.stop_event,
                    log_func=self.enqueue_log,
                    status_func=lambda text: self.enqueue_event("status", text),
                    progress_func=lambda done, total, brand: self.enqueue_event(
                        "progress",
                        {"done": done, "total": total, "brand": brand},
                    ),
                    sleep_func=lambda phase, elapsed, total: self.enqueue_event(
                        "sleep",
                        {"phase": phase, "elapsed": elapsed, "total": total},
                    ),
                )
                summary = runner.run()
                self.enqueue_event("done", summary)
            except KeyboardInterrupt:
                self.enqueue_event("error", "采集已停止")
            except CookieExpiredError as exc:
                self.enqueue_event("error", str(exc))
            except WeiboPictureSettingError as exc:
                self.enqueue_event("error", str(exc))
            except Exception as exc:
                self.enqueue_event("error", f"运行失败：{exc}")
            finally:
                try:
                    if db:
                        db.close()
                except Exception:
                    pass

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()

    def stop_crawl(self) -> None:
        """请求停止采集。"""
        if self.worker_thread and self.worker_thread.is_alive():
            self.stop_event.set()
            self.var_status.set("正在停止…")
            self.append_log("已发送停止指令，当前页处理结束后会退出。")
        else:
            messagebox.showinfo("提示", "当前没有运行中的微博采集任务。")

    def on_crawl_done(self, summary: Optional[Dict[str, int]], error: Optional[str]) -> None:
        """采集结束后的界面回调。"""
        self.set_running_ui(False)
        self.worker_thread = None
        self.start_ts = None
        self._apply_sleep_progress("", 0.0, 0.0)

        if error:
            self.var_status.set("已结束")
            self.append_log(error, level="ERROR")
            return

        if not summary:
            self.var_status.set("已结束")
            return

        self.var_status.set("已完成")
        self.append_log(
            "任务完成："
            f"品牌成功 {summary['brands_success']} 个，"
            f"失败 {summary['brands_failed']} 个，"
            f"发现微博 {summary['seen']} 条，"
            f"新增 {summary['inserted']} 条，"
            f"重复 {summary['duplicates']} 条"
        )

    def on_close(self) -> None:
        """关闭窗口。"""
        self.persist_current_form()
        self.stop_event.set()
        for after_id in (self._queue_after_id, self._tick_after_id, self._log_size_after_id):
            if after_id is None:
                continue
            try:
                self.master.after_cancel(after_id)
            except Exception:
                pass
        self.master.destroy()


def main_gui() -> None:
    """启动 GUI。"""
    root = tk.Tk()
    try:
        style = ttk.Style()
        if "clam" in style.theme_names():
            style.theme_use("clam")
    except Exception:
        pass
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main_gui()
