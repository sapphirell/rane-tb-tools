# -*- coding: utf-8 -*-
"""
小红书采集 GUI 版（集成：品牌采集 + 妆师/毛娘采集）
功能点：
- 验证码触发把等待改为999秒
- “跳过当前等待”按钮（立即打断本轮sleep）
- 采集对象切换：品牌(娃店)->spider_log / 艺术家(妆师/毛娘)->artist_spider_log
- DB 自动重连、likes列自适应（品牌端）
"""

import os
import pickle
import time
import urllib
import urllib.parse
import logging
import threading
from typing import Dict, Optional, Callable, List

# ====== 第三方 ======
import pymysql
from selenium.webdriver import Chrome
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ====== Tkinter GUI ======
import tkinter as tk
from tkinter import ttk, messagebox

# ===================== 工具函数 =====================

def convert_xhs_url(original_url):
    parsed_url = urllib.parse.urlparse(original_url)
    new_query = parsed_url.query.replace('&amp;', '&')

    path_parts = parsed_url.path.split('/')
    if len(path_parts) >= 5 and path_parts[1] == 'user' and path_parts[2] == 'profile':
        note_id = path_parts[4]
        new_path = f'/explore/{note_id}'
    else:
        new_path = parsed_url.path

    new_parsed = parsed_url._replace(
        path=new_path,
        query=new_query
    )
    return urllib.parse.urlunparse(new_parsed)


def parse_xhs_time(time_str: str) -> int:
    """解析小红书时间格式（含 天前/小时前/分钟前/昨天/今天 及常见日期格式）"""
    from datetime import datetime, timedelta
    import re

    clean_str = time_str.replace('编辑于 ', '').strip()
    parts = re.split(r'\s+(?=[\u4e00-\u9fa5]{2,5}$)', clean_str)
    time_part = parts[0]

    now = datetime.now()
    current_year = now.year

    try:
        if '天前' in time_part:
            days = int(re.search(r'\d+', time_part).group())
            return int((now - timedelta(days=days)).timestamp())

        if '昨天' in time_part:
            date = now - timedelta(days=1)
            time_str2 = re.sub(r'昨天', date.strftime('%Y-%m-%d'), time_part)
        elif '今天' in time_part:
            time_str2 = re.sub(r'今天', now.strftime('%Y-%m-%d'), time_part)
        elif '分钟前' in time_part:
            minutes = int(re.search(r'\d+', time_part).group())
            return int((now - timedelta(minutes=minutes)).timestamp())
        elif '小时前' in time_part:
            hours = int(re.search(r'\d+', time_part).group())
            return int((now - timedelta(hours=hours)).timestamp())
        else:
            time_str2 = time_part

        time_formats = [
            (r'(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{2})', "%m-%d %H:%M"),
            (r'(\d{4})-(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{2})', "%Y-%m-%d %H:%M"),
            (r'(\d{1,2})-(\d{1,2})', "%m-%d"),
        ]

        for pattern, time_format in time_formats:
            if re.match(pattern, time_str2):
                dt = datetime.strptime(time_str2, time_format)
                if dt.year == 1900:
                    dt = dt.replace(year=current_year)
                    if dt > now + timedelta(days=60):
                        dt = dt.replace(year=current_year - 1)
                return int(dt.timestamp())

        return 0
    except Exception as e:
        logging.warning(f"时间解析失败: {clean_str} ({str(e)})")
        return 0


# ===================== 常量与枚举 =====================

class TargetType:
    BRAND = 'brand'     # 写 spider_log
    ARTIST = 'artist'   # 写 artist_spider_log

# ===================== 数据库（自动重连 + 重试 + likes列自适应） =====================

class DatabaseManager:
    def __init__(self):
        self._conn_args = dict(
            host='111.229.182.88',
            port=3306,
            user='root',
            password='s*xNvd%v@',
            database='sukitime',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
        )
        self.connection = pymysql.connect(**self._conn_args)
        self._has_likes_col_spider = None  # spider_log.likes
        self._has_likes_col_artist = None  # artist_spider_log.likes

    def _ensure_conn(self):
        try:
            self.connection.ping(reconnect=True)
        except Exception:
            self.connection = pymysql.connect(**self._conn_args)

    def _exec(self, sql, params=None, many=False):
        self._ensure_conn()
        try:
            with self.connection.cursor() as cursor:
                if many:
                    cursor.executemany(sql, params or [])
                else:
                    cursor.execute(sql, params or ())
                return cursor, cursor.rowcount
        except Exception:
            self._ensure_conn()
            with self.connection.cursor() as cursor:
                if many:
                    cursor.executemany(sql, params or [])
                else:
                    cursor.execute(sql, params or ())
                return cursor, cursor.rowcount

    # ---- likes 列探测 ----
    def _check_likes_col_spider(self) -> bool:
        if self._has_likes_col_spider is not None:
            return self._has_likes_col_spider
        try:
            cur, _ = self._exec("SHOW COLUMNS FROM spider_log LIKE 'likes'")
            self._has_likes_col_spider = bool(cur.fetchone())
        except Exception:
            self._has_likes_col_spider = False
        return self._has_likes_col_spider

    def _check_likes_col_artist(self) -> bool:
        if self._has_likes_col_artist is not None:
            return self._has_likes_col_artist
        try:
            cur, _ = self._exec("SHOW COLUMNS FROM artist_spider_log LIKE 'likes'")
            self._has_likes_col_artist = bool(cur.fetchone())
        except Exception:
            self._has_likes_col_artist = False
        return self._has_likes_col_artist

    # ---- 品牌（娃店）数据集 ----
    def fetch_brand_urls(self) -> list:
        sql = """
            SELECT id, brand_name, rednote_url, rednote_spd_setting 
            FROM brand 
            WHERE rednote_url != '' AND is_delete = 0 AND is_brand = 1
            ORDER BY spider_index DESC, last_gather_time ASC
        """
        cur, _ = self._exec(sql)
        return cur.fetchall()

    def is_url_exists_brand(self, url: str) -> bool:
        cur, _ = self._exec("SELECT 1 FROM spider_log WHERE url = %s LIMIT 1", (url,))
        return bool(cur.fetchone())

    def get_collected_count_by_brand(self, brand_id: int) -> int:
        cur, _ = self._exec("SELECT COUNT(*) AS cnt FROM spider_log WHERE brand_id = %s", (brand_id,))
        row = cur.fetchone()
        return int(row['cnt'] if row and row['cnt'] is not None else 0)

    def insert_brand_log(self, data: Dict):
        title = (data.get('title') or '')[:255]
        content = (data.get('content') or '')[:2000]
        images = ','.join(data.get('images') or [])[:2000]
        like_count = int(data.get('like_count') or 0)
        has_likes = self._check_likes_col_spider()
        now = int(time.time())
        if has_likes:
            sql = """
                INSERT INTO spider_log (
                    msg_type, status, origin_type, title, 
                    content, url, images, brand_id, 
                    brand_name, auth_time, created_at, updated_at, likes
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            params = (0, 0, 'xhs', title, content, data['url'], images,
                      data['brand_id'], data['brand_name'], int(data.get('auth_time', 0)),
                      now, now, like_count)
        else:
            sql = """
                INSERT INTO spider_log (
                    msg_type, status, origin_type, title, 
                    content, url, images, brand_id, 
                    brand_name, auth_time, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            params = (0, 0, 'xhs', title, content, data['url'], images,
                      data['brand_id'], data['brand_name'], int(data.get('auth_time', 0)),
                      now, now)
        self._exec(sql, params)

    # ---- 艺术家（妆师/毛娘）数据集 ----
    def fetch_artists(self) -> list:
        sql = """
            SELECT id, brand_name, rednote_url, rednote_spd_setting_for_artist 
            FROM brand 
            WHERE is_delete = 0 
              AND is_bjd_artist = 1 
              AND rednote_url != '' 
              AND rednote_spd_setting_for_artist != 3
            ORDER BY spider_index DESC, last_gather_time ASC
        """
        cur, _ = self._exec(sql)
        return cur.fetchall()

    def is_url_exists_artist(self, url: str) -> bool:
        cur, _ = self._exec("SELECT 1 FROM artist_spider_log WHERE url = %s LIMIT 1", (url,))
        return bool(cur.fetchone())

    def insert_artist_log(self, data: Dict):
        """插入艺术家作品数据（artist_spider_log）"""
        title = (data.get('title') or '')[:600]
        content = (data.get('content') or '')[:2000]
        images = ','.join(data.get('images') or [])[:2000]
        now = int(time.time())
        has_likes = self._check_likes_col_artist()
        likes = int(data.get('like_count') or 0)
        sql = """
            INSERT INTO artist_spider_log (
                msg_type, status, origin_type, title, 
                content, url, images, brand_id, 
                brand_name, created_at, updated_at, 
                full_get, auth_time, likes
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """
        params = (
            0, 0, 'xhs', title, content, data.get('url', ''),
            images,
            data.get('artist_id', data.get('brand_id', 0)),
            data.get('artist_name', data.get('brand_name', '')),
            now, now,
            int(data.get('full_get', 0)),
            int(data.get('auth_time', 0)),
            likes if has_likes else 0
        )
        self._exec(sql, params)

    # ---- 公共：更新时间 ----
    def update_last_gather_time(self, brand_id: int):
        self._exec("UPDATE brand SET last_gather_time = NOW() WHERE id = %s", (brand_id,))


# ===================== 爬虫核心 =====================

class XHSCrawler:
    def __init__(
        self,
        target_type: str,
        url_checker: Optional[Callable] = None,
        insert_callback: Optional[Callable] = None,
        *,
        get_scroll_sleep: Optional[Callable[[], float]] = None,
        get_detail_sleep: Optional[Callable[[], float]] = None,
        on_sleep: Optional[Callable[[str, float, float], None]] = None,
        on_captcha_detected: Optional[Callable[[str], None]] = None,
        skip_event: Optional[threading.Event] = None,
        max_scroll_default: int = 20,
        headless: bool = False,
        logger: Optional[logging.Logger] = None
    ):
        self.target_type = target_type  # BRAND / ARTIST
        self.url_checker = url_checker
        self.insert_callback = insert_callback
        self.get_scroll_sleep = get_scroll_sleep or (lambda: 10.5)
        self.get_detail_sleep = get_detail_sleep or (lambda: 5.0)
        self.on_sleep = on_sleep
        self.on_captcha_detected = on_captcha_detected
        self.skip_event = skip_event or threading.Event()
        self.max_scroll_default = int(max_scroll_default)
        self.logger = logger or logging.getLogger(__name__)

        self.stop_requested = False

        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_experimental_option("excludeSwitches", ['enable-automation'])
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        self.logger.info("准备初始化浏览器")
        self.driver: Chrome = webdriver.Chrome(options=options)

        # 注入 stealth（如有）
        stealth_path = './stealth.min.js'
        if os.path.exists(stealth_path):
            try:
                with open(stealth_path, 'r', encoding='utf-8') as f:
                    stealth_script = f.read()
                self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': stealth_script})
                self.logger.info("已注入 stealth 脚本")
            except Exception as e:
                self.logger.warning(f"stealth 注入失败：{e}")
        else:
            self.logger.warning("未找到 stealth.min.js，跳过注入")

        self.logger.info("浏览器运行成功")

        self.seen_links = set()
        self.notes_data = []
        self.main_window = None
        self.all_links = set()
        self.collected_quick_data = []  # 快速模式数据缓存

    # ---------- 停止控制 ----------
    def request_stop(self):
        self.stop_requested = True

    def check_stop(self):
        if self.stop_requested:
            raise KeyboardInterrupt("收到停止信号")

    # ---------- 验证码检测 ----------
    def _detect_and_handle_captcha(self, where: str):
        """where: 'explore' / 'scroll' / 'detail'"""
        try:
            els = self.driver.find_elements(By.CSS_SELECTOR, "#red-captcha, div#red-captcha")
            visible = any(e.is_displayed() for e in els) if els else False
            if visible:
                self.logger.warning(f"检测到验证码 #red-captcha（{where}），已将等待修改为 999 秒，直到你手动改回。")
                if self.on_captcha_detected:
                    try:
                        self.on_captcha_detected(where)
                    except Exception:
                        pass
                return True
        except Exception:
            pass
        return False

    # ---------- 登录 ----------
    def login(self):
        self.driver.get('https://www.xiaohongshu.com/explore')
        self.main_window = self.driver.current_window_handle

        # 主页验证码也检测一次
        self._detect_and_handle_captcha("explore")

        cookie_file = "xhs_cookie.pkl" if self.target_type == TargetType.BRAND else "xhs_artist_cookie.pkl"

        if os.path.exists(cookie_file):
            try:
                cookies = pickle.load(open(cookie_file, "rb"))
                for cookie in cookies:
                    self.driver.add_cookie(cookie)
                self.driver.refresh()
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'user.side-bar-component'))
                )
                self.logger.info("Cookie 登录成功")
                return
            except Exception as e:
                self.logger.warning(f"Cookie加载失败: {e}")

        self.logger.info("等待手动登录（120s 超时）")
        WebDriverWait(self.driver, 120).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'user.side-bar-component'))
        )
        pickle.dump(self.driver.get_cookies(), open(cookie_file, "wb"))
        self.logger.info('登录成功并已保存 Cookie')

    # ---------- 列表页提取 ----------
    def extract_current_links(self):
        current_links = set()
        try:
            items = self.driver.find_elements(By.CSS_SELECTOR, '.note-item')
            for item in items:
                try:
                    link_element = item.find_element(
                        By.CSS_SELECTOR, 'a.cover.mask.ld[href^="/user/profile/"]'
                    )
                    raw_url = link_element.get_attribute('href')
                    clean_url = raw_url.replace('&amp;', '&')
                    current_links.add(clean_url)
                except Exception:
                    continue
        except Exception as e:
            self.logger.warning(f"提取链接异常: {e}")
        return current_links

    # ---------- 智能滚动 ----------
    def smart_scroll(self, spd_setting=1, max_scroll=None):
        total_scroll = 0
        no_new_count = 0
        max_no_new = 6
        last_height = 0
        max_scroll = self.max_scroll_default if max_scroll is None else int(max_scroll)

        self.logger.info(f"智能滚动设置: 最大滚动次数={max_scroll}（滚动等待将实时读取 GUI 配置）")

        while no_new_count < max_no_new and total_scroll < max_scroll:
            self.check_stop()

            # 滚动前也检测一次验证码
            self._detect_and_handle_captcha("scroll")

            current_links = self.extract_current_links()
            converted_new_links = {
                convert_xhs_url(link).split('?')[0]
                for link in (current_links - self.all_links)
            }
            new_links = converted_new_links - self.all_links

            if spd_setting == 2 and new_links:
                self.process_quick_data(new_links)

            self.all_links.update(current_links)
            self.logger.info(f"当前总链接数：{len(self.all_links)} 新增：{len(new_links)}")

            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # —— 滚动等待（可被“跳过本次等待”打断）——
            scroll_sleep = max(0.0, float(self.get_scroll_sleep()))
            self._sleep_with_progress("scroll", scroll_sleep)

            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                no_new_count += 1
            else:
                no_new_count = 0
                last_height = new_height

            total_scroll += 1

    # ---------- 处理单个笔记 ----------
    def process_single_note(self, origin_note_url: str) -> Optional[Dict]:
        note_url = convert_xhs_url(origin_note_url)
        self.logger.info(f"打开URL: {origin_note_url} -> {note_url}")
        img_urls = []

        try:
            self.driver.switch_to.window(self.main_window)
            self.driver.execute_script(f"window.open('{note_url}');")
            new_window = [w for w in self.driver.window_handles if w != self.main_window][0]
            self.driver.switch_to.window(new_window)

            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".note-container"))
            )

            # 详情页也检查验证码
            self._detect_and_handle_captcha("detail")

            # —— 详情等待（可被“跳过本次等待”打断）——
            detail_sleep = max(0.0, float(self.get_detail_sleep()))
            self._sleep_with_progress("detail", detail_sleep)

            # 发布时间
            try:
                time_element = self.driver.find_element(By.CSS_SELECTOR, '.bottom-container .date')
                raw_time = time_element.text.strip()
                auth_time = parse_xhs_time(raw_time)
            except Exception as te:
                self.logger.warning(f"时间提取失败: {te}")
                auth_time = 0

            # 点赞
            try:
                like_count = 0
                like_element = self.driver.find_element(
                    By.CSS_SELECTOR,
                    '.interact-container .like-active .count'
                )
                like_text = like_element.text.strip()
                if '万' in like_text:
                    like_count = int(float(like_text.replace('万', '')) * 10000)
                elif 'k' in like_text.lower():
                    like_count = int(float(like_text.lower().replace('k', '')) * 1000)
                else:
                    like_count = int(like_text) if like_text.isdigit() else 0
            except Exception as le:
                self.logger.warning(f"点赞数提取失败: {le}")
                like_count = 0

            # 视频封面 or 图文图片
            try:
                video_element = self.driver.find_element(By.CSS_SELECTOR, '.player-container')
                if video_element:
                    try:
                        poster = self.driver.find_element(By.CSS_SELECTOR, 'xg-poster.xgplayer-poster')
                        style = poster.get_attribute('style')
                        cover_url = style.split('url("')[1].split('")')[0].replace('&quot;', '')
                        img_urls = [cover_url]
                    except Exception as ve2:
                        self.logger.warning(f"视频封面提取失败: {ve2}")
            except Exception:
                try:
                    swiper = self.driver.find_element(By.CLASS_NAME, 'swiper-wrapper')
                    for img in swiper.find_elements(By.TAG_NAME, 'img'):
                        src = img.get_attribute('src')
                        if src and src.startswith('http') and src not in img_urls:
                            img_urls.append(src)
                except Exception as ie:
                    self.logger.warning(f"图片提取失败: {ie}")

            # 内容
            content = ''
            try:
                text_element = self.driver.find_element(By.CSS_SELECTOR, '.note-content .desc')
                content = text_element.text.replace('\n', ' ').strip()[:2000]
            except Exception as te:
                self.logger.warning(f"内容提取失败: {te}")

            # 标题
            title = ''
            try:
                title_element = self.driver.find_element(By.ID, 'detail-title')
                title = title_element.text.strip()
            except Exception as titile_e:
                self.logger.warning(f"标题提取失败: {titile_e}")

            baseUrl = note_url.split('?')[0]
            return {
                'images': img_urls,
                'content': content,
                'url': baseUrl,
                'title': title,
                'auth_time': auth_time,
                'like_count': like_count,
            }

        except Exception as e:
            self.logger.error(f"笔记处理失败 {note_url}: {e}", exc_info=True)
            return None
        finally:
            try:
                self.driver.close()
                self.driver.switch_to.window(self.main_window)
            except Exception as close_e:
                self.logger.warning(f"窗口关闭异常: {close_e}")

    # ---------- 快速模式（列表页） ----------
    def process_quick_data(self, new_links: set):
        current_items = self.driver.find_elements(By.CSS_SELECTOR, '.note-item')
        for item in current_items:
            try:
                link = item.find_element(By.CSS_SELECTOR, 'a.cover.mask.ld').get_attribute('href')
                clean_url = convert_xhs_url(link).split('?')[0]

                if self.url_checker and self.url_checker(clean_url):
                    self.logger.info(f"已存在，跳过快速采集: {clean_url}")
                    continue

                if clean_url not in new_links:
                    continue

                # 首图
                try:
                    img = item.find_element(By.CSS_SELECTOR, 'img[src*="xhscdn.com"]')
                    cover_url = img.get_attribute('src').split('?')[0]
                except Exception as e:
                    self.logger.warning(f"首图提取失败: {e}")
                    cover_url = ""

                # 标题
                try:
                    title = item.find_element(By.CSS_SELECTOR, '.title > span').text[:600]
                except Exception as e:
                    title = "无标题"
                    self.logger.warning(f"标题提取失败: {e}")

                self.collected_quick_data.append({
                    'url': clean_url,
                    'images': [cover_url] if cover_url else [],
                    'title': title,
                    'content': ''
                })
            except Exception as e:
                self.logger.error(f"快速采集异常: {e}")

    # ---------- 已采集数 ----------
    def get_collected_count_for_brand(self, brand_id: int) -> int:
        try:
            if self.url_checker and hasattr(self.url_checker, '__self__'):
                db = self.url_checker.__self__
                if hasattr(db, 'get_collected_count_by_brand'):
                    return db.get_collected_count_by_brand(brand_id)
        except Exception:
            self.logger.exception("查询已采集数量失败")
        return 0

    # ---------- 总调度：采集一个“对象”（品牌或艺术家） ----------
    def crawl_target(self, row: Dict):
        """
        row: 来自 brand 表的一行
        品牌：使用 rednote_spd_setting
        艺术家：使用 rednote_spd_setting_for_artist
        """
        try:
            self.check_stop()

            if self.target_type == TargetType.BRAND:
                spd_setting = row.get('rednote_spd_setting', 1)
                name_key = 'brand_name'
                id_key = 'id'
                self.logger.info(f"品牌[{row[name_key]}] 采集配置: {spd_setting}")
                if spd_setting == 3:
                    self.logger.info(f"品牌[{row[name_key]}] 配置不采集，跳过")
                    return True
                collected_count = self.get_collected_count_for_brand(row[id_key])
                self.logger.info(f"品牌[{row[name_key]}] 已采集数量: {collected_count}")
                max_scroll = 1 if (spd_setting == 1 and collected_count > 20) else self.max_scroll_default

            else:
                spd_setting = row.get('rednote_spd_setting_for_artist', 3)
                name_key = 'brand_name'
                id_key = 'id'
                self.logger.info(f"艺术家[{row[name_key]}] 采集配置: {spd_setting}")
                if spd_setting == 3:
                    self.logger.info(f"艺术家[{row[name_key]}] 配置不采集，跳过")
                    return True
                max_scroll = 1 if spd_setting == 2 else self.max_scroll_default  # 艺术家快速模式时可适当减少滚动

            if not row.get('rednote_url'):
                self.logger.info(f"{'品牌' if self.target_type==TargetType.BRAND else '艺术家'}[{row[name_key]}] 未设置小红书地址，跳过")
                return True

            self.driver.get(row['rednote_url'])
            self._detect_and_handle_captcha("scroll")
            self.smart_scroll(spd_setting, max_scroll)

            if spd_setting == 1:
                # 全量详情逐条
                for note_url in list(self.all_links):
                    self.check_stop()
                    base_url = convert_xhs_url(note_url).split('?')[0]
                    if self.url_checker and self.url_checker(base_url):
                        self.logger.info(f"已处理过，跳过: {base_url}")
                        continue

                    detail = self.process_single_note(note_url)

                    # 详情页之间的等待（可被“跳过本次等待”打断）
                    detail_sleep = max(0.0, float(self.get_detail_sleep()))
                    self._sleep_with_progress("detail", detail_sleep)

                    if detail:
                        if self.target_type == TargetType.BRAND:
                            detail.update({
                                'brand_id': row[id_key],
                                'brand_name': row[name_key]
                            })
                        else:
                            detail.update({
                                'artist_id': row[id_key],
                                'artist_name': row[name_key],
                                'full_get': 0
                            })
                        if self.insert_callback:
                            try:
                                self.insert_callback(detail)
                            except Exception:
                                self.logger.exception("数据库插入失败")

            elif spd_setting == 2:
                # 快速模式：只写链接+首图+标题
                for quick_data in self.collected_quick_data:
                    self.check_stop()
                    if self.target_type == TargetType.BRAND:
                        quick_data.update({
                            'brand_id': row[id_key],
                            'brand_name': row[name_key],
                            'auth_time': 0
                        })
                    else:
                        quick_data.update({
                            'artist_id': row[id_key],
                            'artist_name': row[name_key],
                            'auth_time': 0,
                            'full_get': 0
                        })
                    if self.insert_callback:
                        try:
                            self.insert_callback(quick_data)
                        except Exception:
                            self.logger.exception("数据库插入失败")
                self.logger.info(f"快速采集数据入库成功: {len(self.collected_quick_data)} 条")

            self.all_links.clear()
            self.collected_quick_data.clear()
            return True
        except KeyboardInterrupt:
            self.logger.info("收到停止信号，已终止当前对象采集")
            return False
        except Exception as e:
            self.logger.error(f"对象采集失败 {row.get('rednote_url')}: {e}")
            return False

    # ---------- 内部：带进度、可被“跳过本次等待”打断的 sleep ----------
    def _sleep_with_progress(self, phase: str, total: float):
        """phase: 'scroll' / 'detail'"""
        total = max(0.0, float(total))
        if self.on_sleep:
            try:
                self.on_sleep(phase, 0.0, total)
            except Exception:
                pass
        if total == 0:
            if self.on_sleep:
                try:
                    self.on_sleep(phase, total, total)
                except Exception:
                    pass
            return

        elapsed = 0.0
        step = 0.2
        self.skip_event.clear()

        while elapsed < total:
            self.check_stop()
            if self.skip_event.is_set():
                self.logger.info("收到“跳过本次等待”指令，立即继续")
                self.skip_event.clear()
                break

            time.sleep(min(step, total - elapsed))
            elapsed = min(total, elapsed + step)
            if self.on_sleep:
                try:
                    self.on_sleep(phase, elapsed, total)
                except Exception:
                    pass

        if self.on_sleep:
            try:
                self.on_sleep(phase, total, total)
            except Exception:
                pass


# ===================== GUI & 主流程 =====================

class TextHandler(logging.Handler):
    """将日志输出到 Tkinter Text"""
    def __init__(self, text_widget: tk.Text):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        try:
            self.text_widget.after(0, self._append, msg)
        except RuntimeError:
            pass

    def _append(self, msg):
        self.text_widget.configure(state='normal')
        self.text_widget.insert('end', msg + '\n')
        self.text_widget.see('end')
        self.text_widget.configure(state='disabled')


class App:
    def __init__(self, master: tk.Tk):
        self.master = master
        self.master.title("小红书爬虫 · 采集控制台（品牌 / 妆师）")
        self.master.geometry("1000x860")

        # ===== 参数区 =====
        frm = ttk.LabelFrame(master, text="采集参数（可运行中随时修改）")
        frm.pack(fill="x", padx=10, pady=10)

        ttk.Label(frm, text="滚动等待(秒)：").grid(row=0, column=0, padx=6, pady=6, sticky='e')
        self.var_scroll_sleep = tk.StringVar(value="10.5")
        ttk.Entry(frm, textvariable=self.var_scroll_sleep, width=10).grid(row=0, column=1, padx=6, pady=6, sticky='w')

        ttk.Label(frm, text="详情等待(秒)：").grid(row=0, column=2, padx=6, pady=6, sticky='e')
        self.var_detail_sleep = tk.StringVar(value="5.0")
        ttk.Entry(frm, textvariable=self.var_detail_sleep, width=10).grid(row=0, column=3, padx=6, pady=6, sticky='w')

        ttk.Label(frm, text="最大滚动次数：").grid(row=0, column=4, padx=6, pady=6, sticky='e')
        self.var_max_scroll = tk.StringVar(value="20")
        ttk.Entry(frm, textvariable=self.var_max_scroll, width=10).grid(row=0, column=5, padx=6, pady=6, sticky='w')

        self.var_headless = tk.BooleanVar(value=False)
        ttk.Checkbutton(frm, text="无头模式(Headless)", variable=self.var_headless).grid(row=0, column=6, padx=6, pady=6)

        # ===== 采集对象切换（新增） =====
        type_frame = ttk.LabelFrame(master, text="采集对象")
        type_frame.pack(fill='x', padx=10, pady=(0,10))
        self.var_target_type = tk.StringVar(value=TargetType.BRAND)
        ttk.Radiobutton(type_frame, text="品牌（娃店 → spider_log）", value=TargetType.BRAND, variable=self.var_target_type).pack(side='left', padx=10, pady=6)
        ttk.Radiobutton(type_frame, text="艺术家（妆师/毛娘 → artist_spider_log）", value=TargetType.ARTIST, variable=self.var_target_type).pack(side='left', padx=10, pady=6)

        # ===== 控制/状态区 =====
        ctrl = ttk.Frame(master)
        ctrl.pack(fill='x', padx=10)

        self.btn_start = ttk.Button(ctrl, text="开始采集", command=self.start)
        self.btn_stop = ttk.Button(ctrl, text="停止采集", command=self.stop, state='disabled')
        self.btn_skip = ttk.Button(ctrl, text="跳过当前等待", command=self.skip_current_wait, state='disabled')
        self.btn_start.pack(side='left', padx=6, pady=4)
        self.btn_stop.pack(side='left', padx=6, pady=4)
        self.btn_skip.pack(side='left', padx=6, pady=4)

        self.var_status = tk.StringVar(value="就绪")
        ttk.Label(ctrl, textvariable=self.var_status).pack(side='left', padx=12)

        # 用时 + 进度
        info = ttk.Frame(master)
        info.pack(fill='x', padx=10, pady=(6, 8))
        self.var_duration = tk.StringVar(value="已用时：00:00:00")
        self.var_progress_text = tk.StringVar(value="进度：0 / 0 (0.0%)")
        ttk.Label(info, textvariable=self.var_duration).pack(side='left', padx=6)
        ttk.Label(info, textvariable=self.var_progress_text).pack(side='left', padx=18)

        self.progress = ttk.Progressbar(master, length=940, mode='determinate', maximum=100)
        self.progress.pack(fill='x', padx=10, pady=(0, 8))

        # ===== Wait 读条区 =====
        sleep_frame = ttk.LabelFrame(master, text="等待进度（Sleep 读条）")
        sleep_frame.pack(fill='x', padx=10, pady=(0, 10))
        self.var_sleep_text = tk.StringVar(value="当前无等待")
        ttk.Label(sleep_frame, textvariable=self.var_sleep_text).pack(anchor='w', padx=8, pady=(6, 2))
        self.sleep_bar = ttk.Progressbar(sleep_frame, length=940, mode='determinate', maximum=100)
        self.sleep_bar.pack(fill='x', padx=8, pady=(0, 8))

        # ===== 日志区 =====
        log_frame = ttk.LabelFrame(master, text="运行日志")
        log_frame.pack(fill='both', expand=True, padx=10, pady=10)
        self.txt_log = tk.Text(log_frame, height=18, state='disabled')
        self.txt_log.pack(fill='both', expand=True, side='left')
        scroll = ttk.Scrollbar(log_frame, command=self.txt_log.yview)
        scroll.pack(side='right', fill='y')
        self.txt_log['yscrollcommand'] = scroll.set

        # Logger
        self.logger = logging.getLogger("XHS")
        self.logger.setLevel(logging.INFO)
        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.gui_handler = TextHandler(self.txt_log)
        self.gui_handler.setFormatter(fmt)
        self.logger.addHandler(self.gui_handler)
        file_handler = logging.FileHandler('xhs_crawler.log', encoding='utf-8')
        file_handler.setFormatter(fmt)
        self.logger.addHandler(file_handler)

        # 运行线程与对象
        self.running_thread: Optional[threading.Thread] = None
        self.crawler: Optional[XHSCrawler] = None
        self.db: Optional[DatabaseManager] = None

        # 控制定时
        self.start_ts = None
        self._tick_after_id = None
        self.total_rows = 0
        self.done_rows = 0

        # 跳过等待事件（传给 crawler）
        self.skip_event = threading.Event()

        self.master.protocol("WM_DELETE_WINDOW", self.on_close)

    # ====== 给爬虫的“动态读取”函数 ======
    def get_scroll_sleep(self) -> float:
        try:
            return max(0.0, float(self.var_scroll_sleep.get()))
        except Exception:
            return 10.5

    def get_detail_sleep(self) -> float:
        try:
            return max(0.0, float(self.var_detail_sleep.get()))
        except Exception:
            return 5.0

    # ====== 给爬虫的 sleep 回调（线程安全） ======
    def on_sleep(self, phase: str, elapsed: float, total: float):
        def _update():
            total_ = max(0.0, total)
            elapsed_ = min(max(0.0, elapsed), total_)
            percent = 0 if total_ == 0 else (elapsed_ / total_) * 100.0
            phase_cn = "滚动等待" if phase == "scroll" else "详情等待"
            self.var_sleep_text.set(f"{phase_cn}：{elapsed_:.1f}s / {total_:.1f}s")
            self.sleep_bar['value'] = percent
            if total_ > 0 and abs(elapsed_ - total_) < 1e-6:
                self.master.after(1000, self._clear_sleep_bar)
        try:
            self.master.after(0, _update)
        except Exception:
            pass

    def _clear_sleep_bar(self):
        self.var_sleep_text.set("当前无等待")
        self.sleep_bar['value'] = 0

    # ====== 验证码回调（把等待改为999） ======
    def on_captcha_detected(self, where: str):
        def _apply():
            self.var_scroll_sleep.set("999")
            self.var_detail_sleep.set("999")
        try:
            self.master.after(0, _apply)
        except Exception:
            pass

    # ====== 计时器 ======
    def _tick(self):
        if self.start_ts is not None:
            elapsed = int(time.time() - self.start_ts)
            hh = elapsed // 3600
            mm = (elapsed % 3600) // 60
            ss = elapsed % 60
            self.var_duration.set(f"已用时：{hh:02d}:{mm:02d}:{ss:02d}")
        self._tick_after_id = self.master.after(1000, self._tick)

    # ====== 进度刷新 ======
    def _update_progress(self):
        percent = (self.done_rows / self.total_rows * 100.0) if self.total_rows else 0.0
        self.var_progress_text.set(f"进度：{self.done_rows} / {self.total_rows} ({percent:.1f}%)")
        self.progress['value'] = percent

    # ====== 控制逻辑 ======
    def start(self):
        if self.running_thread and self.running_thread.is_alive():
            messagebox.showinfo("提示", "采集已在进行中")
            return

        try:
            max_scroll = int(self.var_max_scroll.get())
        except ValueError:
            messagebox.showerror("错误", "最大滚动次数需为整数")
            return

        self.btn_start.config(state='disabled')
        self.btn_stop.config(state='normal')
        self.btn_skip.config(state='normal')
        self.var_status.set("启动中…")

        self.done_rows = 0
        self.total_rows = 0
        self._update_progress()
        self.start_ts = time.time()
        if self._tick_after_id is None:
            self._tick()

        target_type = self.var_target_type.get()

        def run():
            try:
                self.logger.info("初始化DB")
                self.db = DatabaseManager()

                self.logger.info("初始化爬虫（速度参数将实时读取 GUI 输入框）")
                # 根据采集对象设置不同的回调
                if target_type == TargetType.BRAND:
                    url_checker = self.db.is_url_exists_brand
                    insert_cb = self.db.insert_brand_log
                else:
                    url_checker = self.db.is_url_exists_artist
                    insert_cb = self.db.insert_artist_log

                self.crawler = XHSCrawler(
                    target_type=target_type,
                    url_checker=url_checker,
                    insert_callback=insert_cb,
                    get_scroll_sleep=self.get_scroll_sleep,
                    get_detail_sleep=self.get_detail_sleep,
                    on_sleep=self.on_sleep,
                    on_captcha_detected=self.on_captcha_detected,
                    skip_event=self.skip_event,
                    max_scroll_default=max_scroll,
                    headless=self.var_headless.get(),
                    logger=self.logger
                )

                self.logger.info("准备登录")
                self.crawler.login()

                rows = self.db.fetch_brand_urls() if target_type == TargetType.BRAND else self.db.fetch_artists()
                self.total_rows = len(rows)
                self._update_progress()
                self.logger.info(f"待处理数量：{self.total_rows}")

                for idx, row in enumerate(rows, start=1):
                    if self.crawler.stop_requested:
                        break
                    try:
                        name = row.get('brand_name', f"id={row.get('id')}")
                        self.var_status.set(f"运行中：{name} ({idx}/{self.total_rows})")
                        self.logger.info(f"处理：{name}")

                        # 允许运行中修改最大滚动：下一对象生效
                        try:
                            self.crawler.max_scroll_default = int(self.var_max_scroll.get())
                        except Exception:
                            pass

                        ok = self.crawler.crawl_target(row)
                        if ok:
                            self.db.update_last_gather_time(row['id'])
                            self.logger.info(f"已更新采集时间: {name}")

                        self.crawler.notes_data.clear()
                        self.crawler.all_links.clear()
                    except Exception as e:
                        self.logger.error(f"处理异常 {row.get('brand_name')}: {e}")
                    finally:
                        self.done_rows = idx
                        self.master.after(0, self._update_progress)

                if self.crawler and self.crawler.stop_requested:
                    self.logger.info("任务被用户停止")
                    self.var_status.set("已停止")
                else:
                    self.logger.info("任务结束")
                    self.var_status.set("已完成")
            except KeyboardInterrupt:
                self.logger.info("用户停止")
                self.var_status.set("已停止")
            except Exception as e:
                self.logger.exception(f"运行异常：{e}")
                self.var_status.set("异常")
            finally:
                try:
                    if self.crawler:
                        self.crawler.driver.quit()
                except Exception:
                    pass
                try:
                    if self.db:
                        self.db.connection.close()
                except Exception:
                    pass

                self.btn_start.config(state='normal')
                self.btn_stop.config(state='disabled')
                self.btn_skip.config(state='disabled')
                try:
                    self.master.after(0, self._clear_sleep_bar)
                except Exception:
                    pass

        self.running_thread = threading.Thread(target=run, daemon=True)
        self.running_thread.start()
        self.var_status.set("运行中…")

    def stop(self):
        if self.crawler:
            self.crawler.request_stop()
            self.var_status.set("停止中…")
            self.logger.info("已请求停止，请等待当前步骤完成")
        else:
            self.var_status.set("就绪")

    def skip_current_wait(self):
        """一键跳过当前 sleep（本轮立刻结束）"""
        self.skip_event.set()
        self.logger.info("已触发“跳过本次等待”，即将继续下一步")

    def on_close(self):
        try:
            if self.crawler:
                self.crawler.request_stop()
        except Exception:
            pass
        if self._tick_after_id is not None:
            try:
                self.master.after_cancel(self._tick_after_id)
            except Exception:
                pass
            self._tick_after_id = None
        self.master.after(200, self.master.destroy)


def main_gui():
    root = tk.Tk()
    try:
        style = ttk.Style()
        if 'clam' in style.theme_names():
            style.theme_use('clam')
    except Exception:
        pass
    App(root)
    root.mainloop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main_gui()
