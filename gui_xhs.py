# -*- coding: utf-8 -*-
"""
小红书采集 GUI 版（集成：品牌采集 + 妆师/毛娘采集 + 多账号数据库管理）

功能更新：
- Cookie 存储于数据库表 `xhs_cookies`。
- GUI 支持选择账号登录，支持新增/录入新账号。
- 新增：数据维护功能（批量更新品牌状态与日志状态，移除二次确认，移除运行状态限制）。
- 新增：检测 captcha-modal-content 验证码弹窗并弹出系统提示框。
"""

import os
import re
import sys
import time
import glob
import json  # 新增：用于序列化Cookie
import base64
import hashlib
import hmac
import random
import shutil
import urllib.parse
import logging
import threading
import queue
import subprocess
from time import sleep
from typing import Dict, Optional, Callable, List, Tuple
from pathlib import Path

import pymysql
import requests

from selenium.common.exceptions import SessionNotCreatedException, WebDriverException
from selenium.webdriver.chrome import webdriver as chrome_webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.edge import webdriver as edge_webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog


def get_resource_base_dir() -> str:
    """返回打包资源目录；源码模式下回落到当前脚本目录。"""
    if getattr(sys, "frozen", False):
        return getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(sys.executable)))
    return os.path.dirname(os.path.abspath(__file__))


def get_runtime_base_dir() -> str:
    """返回运行时可写目录；打包后使用 exe 同级目录。"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def convert_xhs_url(original_url: str) -> str:
    parsed_url = urllib.parse.urlparse(original_url)
    new_query = parsed_url.query.replace('&amp;', '&')

    path_parts = parsed_url.path.split('/')
    if len(path_parts) >= 5 and path_parts[1] == 'user' and path_parts[2] == 'profile':
        note_id = path_parts[4]
        new_path = f'/explore/{note_id}'
    else:
        new_path = parsed_url.path

    new_parsed = parsed_url._replace(path=new_path, query=new_query)
    return urllib.parse.urlunparse(new_parsed)


def get_rednote_urls(row: Dict) -> List[str]:
    urls: List[str] = []
    for key in ("rednote_url", "rednote_url2"):
        url = str((row or {}).get(key) or '').strip()
        if url and url not in urls:
            urls.append(url)
    return urls


def resolve_rednote_url(row: Dict) -> str:
    urls = get_rednote_urls(row)
    return urls[0] if urls else ""


def parse_xhs_time(time_str: str) -> int:
    """解析小红书时间格式"""
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


def _safe_profile_key(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return "default"
    # Chrome profile 路径统一使用 ASCII，避免中文路径在不同版本驱动上的兼容问题
    ascii_key = re.sub(r"[^A-Za-z0-9_.-]", "_", text)
    ascii_key = re.sub(r"_+", "_", ascii_key).strip("._-")
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]
    return f"{(ascii_key or 'acc')}_{digest}"


def _legacy_profile_key(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return "default"
    text = re.sub(r"[^\w\-.]", "_", text)
    text = text.strip("._-")
    return text or "default"


class TargetType:
    BRAND = 'brand'  # 写 spider_log
    ARTIST = 'artist'  # 写 artist_spider_log


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
        self._has_likes_col_spider = None
        self._has_likes_col_artist = None

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

    # --- Cookie 管理 ---
    def fetch_cookies(self) -> List[Dict]:
        """获取所有可用账号"""
        sql = """
            SELECT
                id,
                account_name,
                cookie_data,
                last_used_at
            FROM xhs_cookies
            ORDER BY last_used_at DESC, id DESC
        """
        cur, _ = self._exec(sql)
        return cur.fetchall()

    def upsert_xhs_cookie(self, name: str, cookie_data: list):
        """插入或更新账号Cookie"""
        json_str = json.dumps(cookie_data)
        now = int(time.time())
        sql = """
            INSERT INTO xhs_cookies (account_name, cookie_data, last_used_at, created_at)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                cookie_data = VALUES(cookie_data),
                last_used_at = VALUES(last_used_at)
        """
        self._exec(sql, (name, json_str, now, now))

    def update_cookie_usage(self, account_name: str):
        """更新最后使用时间"""
        now = int(time.time())
        self._exec("UPDATE xhs_cookies SET last_used_at = %s WHERE account_name = %s", (now, account_name))

    def delete_xhs_cookie(self, account_name: str) -> int:
        """删除指定账号Cookie，返回影响行数"""
        _, affected = self._exec("DELETE FROM xhs_cookies WHERE account_name = %s", (account_name,))
        return int(affected or 0)

    # --- 字段检测 ---
    def fetch_account_settings(self, account_name: str) -> Optional[Dict]:
        cur, _ = self._exec(
            """
            SELECT
                account_name,
                crawl_settings_json
            FROM xhs_cookies
            WHERE account_name = %s
            LIMIT 1
            """,
            (account_name,)
        )
        row = cur.fetchone()
        if not row:
            return None

        raw_json = row.get("crawl_settings_json")
        if not raw_json:
            return None

        try:
            payload = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        except Exception:
            return None

        if not isinstance(payload, dict):
            return None

        params = payload.get("params")
        if isinstance(params, dict):
            return params

        # 兼容早期/异常数据：如果直接存的是参数平铺对象，也尽量兜底读取
        known_keys = {
            "scroll_sleep", "detail_sleep", "max_scroll", "browser", "headless", "target_type",
            "shutdown_after_finish", "shutdown_timer_minutes"
        }
        if any(key in payload for key in known_keys):
            return payload
        return None

    def upsert_account_settings(self, account_name: str, settings: Dict):
        now = int(time.time())
        payload = {
            "purpose": "gui_xhs_crawl_settings",
            "version": 1,
            "updated_at": now,
            "params": settings,
        }
        self._exec(
            """
            UPDATE xhs_cookies
            SET
                crawl_settings_json = %s,
                last_used_at = %s
            WHERE account_name = %s
            """,
            (
                json.dumps(payload, ensure_ascii=False),
                now,
                account_name,
            )
        )

    def delete_account_settings(self, account_name: str) -> int:
        _, affected = self._exec(
            """
            UPDATE xhs_cookies
            SET
                crawl_settings_json = NULL
            WHERE account_name = %s
            """,
            (account_name,)
        )
        return int(affected or 0)

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

    # --- 品牌 ---
    def fetch_brand_urls(self) -> list:
        sql = """
            SELECT id, brand_name, rednote_url, rednote_url2, rednote_spd_setting 
            FROM brand 
            WHERE (rednote_url != '' OR rednote_url2 != '')
              AND is_delete = 0
              AND is_brand = 1
            ORDER BY spider_index DESC, last_gather_time ASC
        """
        cur, _ = self._exec(sql)
        return cur.fetchall()

    def is_url_exists_brand(self, url: str) -> bool:
        cur, _ = self._exec("SELECT 1 FROM spider_log WHERE url = %s LIMIT 1", (url,))
        return bool(cur.fetchone())

    def insert_brand_log(self, data: Dict):
        title = (data.get('title') or '')[:255]
        content = (data.get('content') or '')[:2000]
        images = ','.join(data.get('images') or [])[:2000]
        like_count = int(data.get('like_count') or 0)
        has_likes = self._check_likes_col_spider()
        now = int(time.time())

        # 构建 SQL
        cols = ["msg_type", "status", "origin_type", "title", "content", "url", "images", "brand_id", "brand_name",
                "auth_time", "created_at", "updated_at"]
        vals = [0, 0, 'xhs', title, content, data['url'], images, data['brand_id'], data['brand_name'],
                int(data.get('auth_time', 0)), now, now]

        if has_likes:
            cols.append("likes")
            vals.append(like_count)

        sql = f"INSERT INTO spider_log ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))})"
        self._exec(sql, tuple(vals))

    # --- 艺术家 ---
    def fetch_artists(self) -> list:
        sql = """
            SELECT id, brand_name, rednote_url, rednote_url2, rednote_spd_setting_for_artist 
            FROM brand 
            WHERE is_delete = 0 
              AND (is_bjd_artist = 1 or is_bjd_hairstylist = 1)
              AND (rednote_url != '' OR rednote_url2 != '')
              AND rednote_spd_setting_for_artist != 3
            ORDER BY spider_index DESC, last_gather_time ASC
        """
        cur, _ = self._exec(sql)
        return cur.fetchall()

    def is_url_exists_artist(self, url: str) -> bool:
        cur, _ = self._exec("SELECT 1 FROM artist_spider_log WHERE url = %s LIMIT 1", (url,))
        return bool(cur.fetchone())

    def insert_artist_log(self, data: Dict):
        title = (data.get('title') or '')[:600]
        content = (data.get('content') or '')[:2000]
        images = ','.join(data.get('images') or [])[:2000]
        now = int(time.time())
        has_likes = self._check_likes_col_artist()
        likes = int(data.get('like_count') or 0)

        cols = ["msg_type", "status", "origin_type", "title", "content", "url", "images", "brand_id", "brand_name",
                "created_at", "updated_at", "full_get", "auth_time", "likes"]
        vals = [
            0, 0, 'xhs', title, content, data.get('url', ''),
            images,
            data.get('artist_id', data.get('brand_id', 0)),
            data.get('artist_name', data.get('brand_name', '')),
            now, now,
            int(data.get('full_get', 0)),
            int(data.get('auth_time', 0)),
            likes if has_likes else 0
        ]

        sql = f"INSERT INTO artist_spider_log ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))})"
        self._exec(sql, tuple(vals))

    def update_last_gather_time(self, brand_id: int):
        self._exec("UPDATE brand SET last_gather_time = NOW() WHERE id = %s", (brand_id,))

    # --- 传图失败记录处理 ---
    def mark_spider_logs_deleted(self, ids: List[int]) -> int:
        """按 spider_log 现有删除语义处理：status=2, msg_type=4"""
        clean_ids = [int(i) for i in ids if int(i) > 0]
        if not clean_ids:
            return 0
        now = int(time.time())
        placeholders = ",".join(["%s"] * len(clean_ids))
        sql = f"UPDATE spider_log SET msg_type = 4, `status` = 2, updated_at = %s WHERE id IN ({placeholders})"
        _, count = self._exec(sql, tuple([now] + clean_ids))
        return count

    def soft_delete_artist_logs(self, ids: List[int]) -> int:
        """按 artist_spider_log 现有删除语义处理：is_delete=1"""
        clean_ids = [int(i) for i in ids if int(i) > 0]
        if not clean_ids:
            return 0
        now = int(time.time())
        placeholders = ",".join(["%s"] * len(clean_ids))
        sql = f"UPDATE artist_spider_log SET is_delete = 1, updated_at = %s WHERE id IN ({placeholders})"
        _, count = self._exec(sql, tuple([now] + clean_ids))
        return count


class SpiderImageUploader:
    """Python 版 spider_image.go：下载原图 -> 上传七牛 -> 写 images_log -> 回写 full_get"""

    QINIU_AK = "HTTyjWkdHISJbKTD0n3OZ_2UPt-AvBKdPRZs2wxQ"
    QINIU_SK = "9Xp6-AlBO9mqP9iyPKsYgxVadj93sIEcfdGxxnG9"
    QINIU_BUCKET = "hobby-box"
    # 对齐 Go 的 ZoneHuadongZheJiang2（七牛会返回 up-cn-east-2.qiniup.com）
    QINIU_UPLOAD_HOST = "https://up-cn-east-2.qiniup.com"
    QINIU_DOMAIN = "https://images1.fantuanpu.com/"

    def __init__(self, db: DatabaseManager, logger: logging.Logger, stop_event: Optional[threading.Event] = None):
        self.db = db
        self.logger = logger
        self.stop_event = stop_event or threading.Event()
        self.tmp_dir = os.path.abspath("./tmp/red_book")
        os.makedirs(self.tmp_dir, exist_ok=True)

    def _is_stopped(self) -> bool:
        return self.stop_event.is_set()

    def run(self) -> Dict[str, object]:
        spider_logs = self._fetch_logs("spider_log")
        artist_logs = self._fetch_logs("artist_spider_log")
        summary = {
            "spider_total": len(spider_logs),
            "spider_updated": 0,
            "spider_image_success": 0,
            "spider_image_failed": 0,
            "spider_failed_log_count": 0,
            "spider_failed_log_ids": [],
            "artist_total": len(artist_logs),
            "artist_updated": 0,
            "artist_image_success": 0,
            "artist_image_failed": 0,
            "artist_failed_log_count": 0,
            "artist_failed_log_ids": [],
            "errors": 0,
            "stopped": 0,
        }

        self.logger.info("开始执行图片上传：spider_log=%d, artist_spider_log=%d", len(spider_logs), len(artist_logs))

        for row in spider_logs:
            if self._is_stopped():
                summary["stopped"] = 1
                self.logger.warning("收到停止请求，终止 spider_log 上传流程")
                break
            try:
                result = self._process_spider_log(row)
                summary["spider_image_success"] += result["image_success"]
                summary["spider_image_failed"] += result["image_failed"]
                if result["failed"]:
                    summary["spider_failed_log_count"] += 1
                    summary["spider_failed_log_ids"].append(int(row.get("id") or 0))
                if result["updated"]:
                    summary["spider_updated"] += 1
            except Exception as exc:
                summary["errors"] += 1
                self.logger.error("处理 spider_log 失败 (id=%s): %s", row.get("id"), exc)

        if not self._is_stopped():
            for row in artist_logs:
                if self._is_stopped():
                    summary["stopped"] = 1
                    self.logger.warning("收到停止请求，终止 artist_spider_log 上传流程")
                    break
                try:
                    result = self._process_artist_log(row)
                    summary["artist_image_success"] += result["image_success"]
                    summary["artist_image_failed"] += result["image_failed"]
                    if result["failed"]:
                        summary["artist_failed_log_count"] += 1
                        summary["artist_failed_log_ids"].append(int(row.get("id") or 0))
                    if result["updated"]:
                        summary["artist_updated"] += 1
                except Exception as exc:
                    summary["errors"] += 1
                    self.logger.error("处理 artist_spider_log 失败 (id=%s): %s", row.get("id"), exc)
        else:
            summary["stopped"] = 1

        self.logger.info(
            "图片上传完成：spider=%d/%d(成功图=%d, 失败图=%d, 失败记录=%d), artist=%d/%d(成功图=%d, 失败图=%d, 失败记录=%d), errors=%d, stopped=%d",
            summary["spider_updated"], summary["spider_total"],
            summary["spider_image_success"], summary["spider_image_failed"], summary["spider_failed_log_count"],
            summary["artist_updated"], summary["artist_total"],
            summary["artist_image_success"], summary["artist_image_failed"], summary["artist_failed_log_count"],
            summary["errors"], summary["stopped"],
        )
        return summary

    def _fetch_logs(self, table_name: str) -> List[Dict]:
        if table_name == "artist_spider_log":
            sql = "SELECT id, images FROM artist_spider_log WHERE full_get = 0 AND status = 0 AND is_delete = 0"
        else:
            sql = "SELECT id, images FROM spider_log WHERE full_get = 0 AND status = 0"
        cur, _ = self.db._exec(sql)
        return cur.fetchall()

    @staticmethod
    def _split_images(images_text: str) -> List[str]:
        return [item.strip() for item in (images_text or "").split(",") if item and item.strip()]

    @staticmethod
    def _urlsafe_b64(data: bytes) -> str:
        # 与 Go SDK 的 base64.URLEncoding.EncodeToString 保持一致（保留 '=' padding）
        return base64.urlsafe_b64encode(data).decode("utf-8")

    def _make_qiniu_upload_token(self) -> str:
        # 对齐 Go SDK PutPolicy：默认 1 小时过期，字段名为 deadline（unix 秒）
        put_policy = {
            "scope": self.QINIU_BUCKET,
            "deadline": int(time.time()) + 3600,
        }
        encoded_policy = self._urlsafe_b64(
            json.dumps(put_policy, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        )
        sign = hmac.new(self.QINIU_SK.encode("utf-8"), encoded_policy.encode("utf-8"), hashlib.sha1).digest()
        encoded_sign = self._urlsafe_b64(sign)
        return f"{self.QINIU_AK}:{encoded_sign}:{encoded_policy}"

    def _download_image(self, download_url: str) -> Tuple[str, str]:
        save_name = f"{hashlib.md5(f'{download_url}{random.randint(0, 10**9)}'.encode('utf-8')).hexdigest()}.jpg"
        store_path = os.path.join(self.tmp_dir, save_name)

        resp = requests.get(download_url, timeout=5)
        resp.raise_for_status()
        with open(store_path, "wb") as f:
            f.write(resp.content)
        return store_path, save_name

    @staticmethod
    def _extract_region_host(error_body: str) -> str:
        """从七牛报错里提取建议上传域名：please use xxx.qiniup.com"""
        message = error_body or ""
        try:
            payload = json.loads(error_body)
            if isinstance(payload, dict):
                message = str(payload.get("error") or message)
        except Exception:
            pass

        marker = "please use "
        idx = message.find(marker)
        if idx < 0:
            return ""

        host = message[idx + len(marker):].split(",")[0].strip()
        if not host:
            return ""
        if host.startswith("http://") or host.startswith("https://"):
            return host
        return f"https://{host}"

    def _upload_to_qiniu(self, local_path: str, save_key: str) -> str:
        token = self._make_qiniu_upload_token()
        data = {"token": token, "key": save_key}
        upload_hosts = [self.QINIU_UPLOAD_HOST]
        tried_hosts = set()
        last_error = ""

        while upload_hosts:
            upload_host = upload_hosts.pop(0)
            if upload_host in tried_hosts:
                continue
            tried_hosts.add(upload_host)

            with open(local_path, "rb") as f:
                files = {"file": (os.path.basename(local_path), f)}
                resp = requests.post(upload_host, data=data, files=files, timeout=20)

            if resp.status_code < 400:
                body = resp.json()
                if "key" not in body:
                    raise RuntimeError(f"七牛返回异常: {body}")
                return body["key"]

            last_error = f"status={resp.status_code}, body={resp.text}"
            suggested_host = self._extract_region_host(resp.text)
            if suggested_host and suggested_host not in tried_hosts:
                self.logger.warning("七牛提示区域不匹配，自动切换上传域名: %s", suggested_host)
                upload_hosts.append(suggested_host)

        raise RuntimeError(f"七牛上传失败: {last_error}")

    def _add_images_log(self, full_url: str, relation_id: int, where_type: str):
        now = int(time.time())
        sql = "INSERT INTO images_log (url, create_time, uid, relation_id, `where`) VALUES (%s, %s, %s, %s, %s)"
        self.db._exec(sql, (full_url, now, 0, relation_id, where_type))

    def _process_spider_log(self, row: Dict) -> Dict[str, int]:
        log_id = int(row.get("id") or 0)
        images = self._split_images(row.get("images") or "")
        result = {"updated": 0, "image_success": 0, "image_failed": 0, "failed": 0}
        if not images:
            self.logger.info("[spider_log:%d] 没有可处理图片，跳过", log_id)
            return result

        new_images = []

        for download_url in images:
            if self._is_stopped():
                self.logger.warning("[spider_log:%d] 收到停止请求，当前记录不回写数据库", log_id)
                return result
            local_path = ""
            try:
                self.logger.info("[spider_log:%d] 下载图片: %s", log_id, download_url)
                local_path, save_name = self._download_image(download_url)
                save_key = f"spd/log/{time.strftime('%Y_%m_%d')}/{save_name}"
                key = self._upload_to_qiniu(local_path, save_key)
                full_url = self.QINIU_DOMAIN + key
                self._add_images_log(full_url, log_id, "spider-log")
                new_images.append(full_url)
                result["image_success"] += 1
            except Exception as exc:
                result["image_failed"] += 1
                self.logger.warning("[spider_log:%d] 处理图片失败: %s", log_id, exc)
            finally:
                if local_path and os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                    except Exception:
                        self.logger.warning("[spider_log:%d] 删除临时文件失败: %s", log_id, local_path)

        if self._is_stopped():
            self.logger.warning("[spider_log:%d] 收到停止请求，当前记录不回写数据库", log_id)
            return result
        if not new_images:
            self.logger.warning("[spider_log:%d] 无上传成功图片，不回写数据库", log_id)
            if result["image_failed"] > 0:
                result["failed"] = 1
            return result

        update_sql = "UPDATE spider_log SET images = %s, full_get = 1, updated_at = %s WHERE id = %s"
        self.db._exec(update_sql, (",".join(new_images), int(time.time()), log_id))
        self.logger.info("[spider_log:%d] 更新完成, 图片数: %d", log_id, len(new_images))
        result["updated"] = 1
        return result

    def _process_artist_log(self, row: Dict) -> Dict[str, int]:
        log_id = int(row.get("id") or 0)
        images = self._split_images(row.get("images") or "")
        result = {"updated": 0, "image_success": 0, "image_failed": 0, "failed": 0}
        if not images:
            self.logger.warning("[artist_spider_log:%d] 没有可处理图片，跳过", log_id)
            return result

        new_images = []
        for download_url in images:
            if self._is_stopped():
                self.logger.warning("[artist_spider_log:%d] 收到停止请求，当前记录不回写数据库", log_id)
                return result
            local_path = ""
            try:
                self.logger.info("[artist_spider_log:%d] 下载图片: %s", log_id, download_url)
                local_path, save_name = self._download_image(download_url)
                save_key = f"artist/log/{time.strftime('%Y_%m_%d')}/{save_name}"
                key = self._upload_to_qiniu(local_path, save_key)
                full_url = self.QINIU_DOMAIN + key
                self._add_images_log(full_url, log_id, "artist-spider-log")
                new_images.append(full_url)
                result["image_success"] += 1
            except Exception as exc:
                result["image_failed"] += 1
                self.logger.warning("[artist_spider_log:%d] 处理图片失败: %s", log_id, exc)
            finally:
                if local_path and os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                    except Exception:
                        self.logger.warning("[artist_spider_log:%d] 删除临时文件失败: %s", log_id, local_path)

        if self._is_stopped():
            self.logger.warning("[artist_spider_log:%d] 收到停止请求，当前记录不回写数据库", log_id)
            return result
        if new_images:
            update_sql = "UPDATE artist_spider_log SET images = %s, full_get = 1 WHERE id = %s"
            self.db._exec(update_sql, (",".join(new_images), log_id))
            self.logger.info("[artist_spider_log:%d] 更新完成, 图片数: %d", log_id, len(new_images))
            result["updated"] = 1
            return result

        self.logger.warning("[artist_spider_log:%d] 无上传成功图片，不回写数据库", log_id)
        if result["image_failed"] > 0:
            result["failed"] = 1
        return result


class XHSCrawler:
    def __init__(
            self,
            target_type: str,
            account_name: str = "",
            url_checker: Optional[Callable] = None,
            insert_callback: Optional[Callable] = None,
            *,
            get_scroll_sleep: Optional[Callable[[], float]] = None,
            get_detail_sleep: Optional[Callable[[], float]] = None,
            get_login_qr_offset: Optional[Callable[[], Tuple[int, int]]] = None,
            on_sleep: Optional[Callable[[str, float, float], None]] = None,
            on_captcha_detected: Optional[Callable[..., None]] = None,
            on_login_qr_detected: Optional[Callable[[Dict], None]] = None,
            skip_event: Optional[threading.Event] = None,
            pause_event: Optional[threading.Event] = None,
            max_scroll_default: int = 20,
            headless: bool = False,
            run_in_background: bool = False,
            browser: str = "chrome",
            logger: Optional[logging.Logger] = None
    ):
        self.target_type = target_type
        self.account_name = str(account_name or "").strip()
        self.url_checker = url_checker
        self.insert_callback = insert_callback
        self.get_scroll_sleep = get_scroll_sleep or (lambda: 10.5)
        self.get_detail_sleep = get_detail_sleep or (lambda: 5.0)
        self.get_login_qr_offset = get_login_qr_offset or (lambda: (0, 0))
        self.on_sleep = on_sleep
        self.on_captcha_detected = on_captcha_detected
        self.on_login_qr_detected = on_login_qr_detected
        self.skip_event = skip_event or threading.Event()
        self.pause_event = pause_event or threading.Event()
        self.pause_reason = ''
        self._pause_logged = False
        self._last_login_qr_fingerprint = ""
        self._last_login_qr_emit_ts = 0.0
        self.max_scroll_default = int(max_scroll_default)
        self.logger = logger or logging.getLogger(__name__)
        self.stop_requested = False
        self.run_in_background = bool(run_in_background) and (not bool(headless))
        self.browser_name = self._normalize_browser_name(browser)
        self.accept_language = "zh-CN,zh;q=0.9,en;q=0.8"
        self.locale = "zh-CN"
        # 采集计数器：
        # - opened_urls: 实际打开详情页 URL 的数量（主要用于精采模式）
        # - inserted_urls: 成功写入数据库的数量
        # - skipped_existing_urls: 因数据库已存在而跳过的数量
        self.total_crawl_stats = self._new_crawl_stats()
        self.last_target_stats = self._new_crawl_stats()

        self.profile_key = _safe_profile_key(self.account_name or f"{self.target_type}_default")
        profiles_root = os.path.join(get_runtime_base_dir(), "xhs_browser_profiles")
        os.makedirs(profiles_root, exist_ok=True)

        # 账号基础目录（保留历史数据），运行目录改放到独立 _runtime 下，彻底与旧损坏 profile 隔离
        self.account_profile_base_dir = os.path.join(profiles_root, self.profile_key)
        runtime_root = os.path.join(profiles_root, "_runtime")
        os.makedirs(runtime_root, exist_ok=True)
        self.account_profile_dir = os.path.join(runtime_root, self.profile_key)
        os.makedirs(self.account_profile_dir, exist_ok=True)

        # 兼容旧版本的中文目录，仅迁移到 base 目录，不直接拿旧目录当 user-data-dir
        legacy_key = _legacy_profile_key(self.account_name or f"{self.target_type}_default")
        legacy_profile_dir = os.path.join(profiles_root, legacy_key)
        if (
                legacy_profile_dir != self.account_profile_base_dir
                and os.path.isdir(legacy_profile_dir)
                and not os.path.exists(self.account_profile_base_dir)
        ):
            try:
                shutil.move(legacy_profile_dir, self.account_profile_base_dir)
                os.makedirs(self.account_profile_dir, exist_ok=True)
                self.logger.info(
                    f"已迁移历史 profile 基目录：{legacy_profile_dir} -> {self.account_profile_base_dir}"
                )
            except Exception as migrate_err:
                self.logger.warning(f"迁移历史 profile 基目录失败，将继续使用新目录：{migrate_err}")

        # 兼容上一版把 runtime_profile 建在旧 profile 目录里的情况；发现旧嵌套 runtime 时仅提示，不复用。
        legacy_runtime_dir = os.path.join(self.account_profile_base_dir, "runtime_profile")
        if os.path.isdir(legacy_runtime_dir):
            self.logger.warning(
                f"检测到旧版嵌套 runtime_profile，已停用该目录以避免个人资料损坏提示：{legacy_runtime_dir}"
            )

        self._clean_profile_runtime_locks(self.account_profile_dir)
        self.logger.info(
            f"准备初始化浏览器（账号: {self.account_name or '未命名'}，"
            f"browser: {self.browser_name}，profile: {self.account_profile_dir}）"
        )

        try:
            self.driver = self._start_browser_with_profile(self.account_profile_dir, headless=headless)
        except SessionNotCreatedException as e:
            self.logger.error(f"启动浏览器失败（持久 profile）：{e}")
            fallback_dir = os.path.join(
                profiles_root,
                "_tmp",
                f"{self.profile_key}_{int(time.time())}"
            )
            os.makedirs(fallback_dir, exist_ok=True)
            self._clean_profile_runtime_locks(fallback_dir)
            self.logger.warning(f"改用临时 profile 重试启动：{fallback_dir}")
            try:
                self.driver = self._start_browser_with_profile(fallback_dir, headless=headless)
                self.account_profile_dir = fallback_dir
            except Exception as retry_err:
                raise RuntimeError(
                    f"{self.browser_name} 启动失败；请关闭所有占用该 profile 的浏览器进程后重试。"
                    f"driver 日志见 spiders/tmp/*driver_*.log；原始错误：{retry_err}"
                ) from retry_err
        except WebDriverException as e:
            raise RuntimeError(
                f"{self.browser_name} driver 启动异常：{e}。请检查浏览器是否可正常启动，"
                f"并查看 spiders/tmp/*driver_*.log。"
            ) from e

        stealth_path = os.path.join(get_resource_base_dir(), 'stealth.min.js')
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

        # 再补一层运行期反检测脚本，重点清理 cdc_* 与 webdriver 暴露。
        self._inject_runtime_stealth_overrides()
        self._apply_cdp_anti_detection(headless=headless)
        if self.run_in_background:
            if self._minimize_browser_window():
                self.logger.info("后台模式已启用：浏览器已最小化")
            else:
                self.logger.warning("后台模式已启用，但最小化浏览器失败，请手动最小化窗口")

        self.all_links = set()
        self.collected_quick_data = []

    @staticmethod
    def _new_crawl_stats() -> Dict[str, int]:
        """创建一份采集统计字典。"""
        return {
            "opened_urls": 0,
            "inserted_urls": 0,
            "skipped_existing_urls": 0,
        }

    @staticmethod
    def _inc_stat(stats: Dict[str, int], key: str, step: int = 1):
        """对采集统计中的指定字段做自增。"""
        stats[key] = int(stats.get(key, 0)) + int(step)

    def _inject_runtime_stealth_overrides(self):
        """注入补充反检测脚本，尽量降低 webdriver/cdc_* 暴露特征。"""
        script = r"""
(() => {
  const hiddenKey = (k) => /^cdc_/i.test(k) || /webdriver/i.test(k);
  const win = window;
  const nav = win.navigator;

  const defineGetter = (obj, key, getter) => {
    try {
      Object.defineProperty(obj, key, {
        get: getter,
        configurable: true
      });
    } catch (_) {}
  };

  // 优先删除 webdriver 属性，尽量让 `'webdriver' in navigator` 返回 false。
  // 仅当浏览器不允许删除且值仍为 true 时，再退回 getter 覆盖。
  try {
    delete Navigator.prototype.webdriver;
  } catch (_) {}
  try {
    const navProto = Object.getPrototypeOf(nav);
    if (navProto) {
      delete navProto.webdriver;
    }
  } catch (_) {}
  try {
    if (nav.webdriver === true) {
      defineGetter(Navigator.prototype, 'webdriver', () => undefined);
      defineGetter(nav, 'webdriver', () => undefined);
    }
  } catch (_) {}

  try {
    if (!win.chrome) {
      Object.defineProperty(win, 'chrome', {
        value: {
          runtime: {},
          app: {},
          csi: function () { return {}; },
          loadTimes: function () { return {}; }
        },
        configurable: false,
        enumerable: true,
        writable: true
      });
    } else if (!win.chrome.runtime) {
      win.chrome.runtime = {};
    }
  } catch (_) {}

  // 注意：这里不再强行伪造 plugins，避免被检测为“不是 PluginArray”。

  try {
    const langs = ['zh-CN', 'zh', 'en-US', 'en'];
    defineGetter(Navigator.prototype, 'languages', () => langs.slice());
    defineGetter(nav, 'languages', () => langs.slice());
    defineGetter(nav, 'language', () => 'zh-CN');
  } catch (_) {}

  try {
    if (nav.permissions && typeof nav.permissions.query === 'function') {
      const originQuery = nav.permissions.query.bind(nav.permissions);
      nav.permissions.query = (parameters) => {
        const name = parameters && parameters.name ? parameters.name : '';
        if (name === 'notifications') {
          return Promise.resolve({ state: Notification.permission });
        }
        return originQuery(parameters);
      };
    }
  } catch (_) {}

  const filterWindowKeys = (keys) => {
    if (!Array.isArray(keys)) return keys;
    return keys.filter((k) => !hiddenKey(String(k || '')));
  };

  const scrubWindow = () => {
    try {
      for (const key of Object.getOwnPropertyNames(win)) {
        if (!hiddenKey(key)) continue;
        try {
          delete win[key];
        } catch (_) {
          try {
            Object.defineProperty(win, key, {
              value: undefined,
              configurable: true
            });
          } catch (_) {}
        }
      }
    } catch (_) {}
  };

  scrubWindow();

  try {
    const origKeys = Object.keys;
    Object.keys = new Proxy(origKeys, {
      apply(target, thisArg, args) {
        const result = Reflect.apply(target, thisArg, args);
        if (args && args[0] === win) {
          return filterWindowKeys(result);
        }
        return result;
      }
    });
  } catch (_) {}

  try {
    const origGetOwnPropertyNames = Object.getOwnPropertyNames;
    Object.getOwnPropertyNames = new Proxy(origGetOwnPropertyNames, {
      apply(target, thisArg, args) {
        const result = Reflect.apply(target, thisArg, args);
        if (args && args[0] === win) {
          return filterWindowKeys(result);
        }
        return result;
      }
    });
  } catch (_) {}

  try {
    const origOwnKeys = Reflect.ownKeys;
    Reflect.ownKeys = new Proxy(origOwnKeys, {
      apply(target, thisArg, args) {
        const result = Reflect.apply(target, thisArg, args);
        if (args && args[0] === win && Array.isArray(result)) {
          return result.filter((k) => !hiddenKey(String(k || '')));
        }
        return result;
      }
    });
  } catch (_) {}

  try {
    const origGetOwnPropertyDescriptors = Object.getOwnPropertyDescriptors;
    Object.getOwnPropertyDescriptors = new Proxy(origGetOwnPropertyDescriptors, {
      apply(target, thisArg, args) {
        const result = Reflect.apply(target, thisArg, args);
        if (args && args[0] === win && result && typeof result === 'object') {
          for (const key of Object.keys(result)) {
            if (hiddenKey(key)) {
              delete result[key];
            }
          }
        }
        return result;
      }
    });
  } catch (_) {}

  try {
    const originalContentWindow = Object.getOwnPropertyDescriptor(HTMLIFrameElement.prototype, 'contentWindow');
    if (originalContentWindow && originalContentWindow.get) {
      Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
        get: function () {
          return originalContentWindow.get.call(this);
        },
        configurable: true
      });
    }
  } catch (_) {}
})();
"""
        try:
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': script})
            try:
                self.driver.execute_script(script)
            except Exception:
                pass
            self.logger.info("已注入补充反检测脚本（webdriver/cdc/plugins/languages）")
        except Exception as e:
            self.logger.warning(f"补充反检测注入失败：{e}")

    def _apply_cdp_anti_detection(self, *, headless: bool):
        """通过 CDP 进一步覆盖 UA/语言等基础指纹，降低默认自动化特征。"""
        try:
            self.driver.execute_cdp_cmd("Network.enable", {})
        except Exception as e:
            self.logger.warning(f"CDP Network.enable 失败，跳过指纹覆盖：{e}")
            return

        user_agent = ""
        platform = "MacIntel"
        try:
            current_ua = self.driver.execute_script("return navigator.userAgent || '';")
            if isinstance(current_ua, str):
                user_agent = current_ua.strip()
            current_platform = self.driver.execute_script("return navigator.platform || '';")
            if isinstance(current_platform, str) and current_platform.strip():
                platform = current_platform.strip()
        except Exception:
            pass

        if not user_agent:
            user_agent = (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
            )
        if "HeadlessChrome/" in user_agent:
            user_agent = user_agent.replace("HeadlessChrome/", "Chrome/")

        try:
            self.driver.execute_cdp_cmd(
                "Network.setUserAgentOverride",
                {
                    "userAgent": user_agent,
                    "acceptLanguage": self.accept_language,
                    "platform": platform,
                },
            )
            self.logger.info("已应用 CDP 指纹覆盖（UA/语言/平台）")
        except Exception as e:
            self.logger.warning(f"CDP 设置 UA 覆盖失败：{e}")

        try:
            self.driver.execute_cdp_cmd("Emulation.setLocaleOverride", {"locale": self.locale})
        except Exception:
            pass

        if headless:
            try:
                self.driver.execute_cdp_cmd(
                    "Emulation.setDeviceMetricsOverride",
                    {
                        "width": 1366,
                        "height": 864,
                        "deviceScaleFactor": 1,
                        "mobile": False,
                    },
                )
            except Exception:
                pass

    def _build_chrome_options(self, profile_dir: str, *, headless: bool) -> ChromeOptions:
        options = ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        elif self.run_in_background:
            options.add_argument("--start-minimized")
        else:
            options.add_argument("--start-maximized")
        options.add_experimental_option("excludeSwitches", ['enable-automation'])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_experimental_option("prefs", {
            "profile.exit_type": "Normal",
            "profile.exited_cleanly": True,
            "intl.accept_languages": "zh-CN,zh,en-US,en",
        })
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-session-crashed-bubble")
        options.add_argument("--disable-infobars")
        options.add_argument("--lang=zh-CN")
        options.add_argument("--remote-debugging-pipe")
        options.add_argument(f"--user-data-dir={profile_dir}")
        options.add_argument("--profile-directory=Default")
        return options

    def _build_edge_options(self, profile_dir: str, *, headless: bool) -> EdgeOptions:
        options = EdgeOptions()
        if headless:
            options.add_argument("--headless=new")
        elif self.run_in_background:
            options.add_argument("--start-minimized")
        else:
            options.add_argument("--start-maximized")
        options.add_experimental_option("excludeSwitches", ['enable-automation'])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_experimental_option("prefs", {
            "profile.exit_type": "Normal",
            "profile.exited_cleanly": True,
            "intl.accept_languages": "zh-CN,zh,en-US,en",
        })
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-session-crashed-bubble")
        options.add_argument("--disable-infobars")
        options.add_argument("--lang=zh-CN")
        options.add_argument("--remote-debugging-pipe")
        options.add_argument(f"--user-data-dir={profile_dir}")
        options.add_argument("--profile-directory=Default")
        return options

    @staticmethod
    def _normalize_browser_name(browser: str) -> str:
        text = str(browser or "").strip().lower()
        if text in {"edge", "msedge", "microsoft-edge"}:
            return "edge"
        return "chrome"

    def _driver_log_path(self, suffix: str = "") -> str:
        log_dir = os.path.join(get_runtime_base_dir(), "tmp")
        os.makedirs(log_dir, exist_ok=True)
        ts = int(time.time())
        tail = f"_{suffix}" if suffix else ""
        return os.path.join(log_dir, f"{self.browser_name}driver_{self.profile_key}_{ts}{tail}.log")

    def _resolve_driver_executable(self, browser_name: str) -> str:
        base_dir = get_resource_base_dir()
        bin_dir = os.path.join(base_dir, "bin")
        tmp_dir = os.path.join(base_dir, "tmp")
        is_windows = (os.name == "nt")
        home_dir = os.path.expanduser("~")

        candidates: List[str] = []
        if browser_name == "edge":
            if is_windows:
                candidates.extend([
                    os.path.join(bin_dir, "msedgedriver.exe"),
                    os.path.join(bin_dir, "edgedriver.exe"),
                ])
                candidates.extend(glob.glob(os.path.join(tmp_dir, "**", "msedgedriver.exe"), recursive=True))
                candidates.extend(glob.glob(os.path.join(tmp_dir, "**", "edgedriver.exe"), recursive=True))
            else:
                candidates.extend([
                    os.path.join(bin_dir, "msedgedriver"),
                    os.path.join(bin_dir, "edgedriver"),
                ])
                candidates.extend(glob.glob(os.path.join(tmp_dir, "**", "msedgedriver"), recursive=True))
                candidates.extend(glob.glob(os.path.join(tmp_dir, "**", "edgedriver"), recursive=True))
                cache_candidates = []
                cache_candidates.extend(
                    glob.glob(os.path.join(home_dir, ".cache", "selenium", "msedgedriver", "**", "msedgedriver"),
                              recursive=True)
                )
                cache_candidates.extend(
                    glob.glob(os.path.join(home_dir, ".cache", "selenium", "msedgedriver", "**", "edgedriver"),
                              recursive=True)
                )
                cache_candidates.extend(
                    glob.glob(os.path.join(home_dir, "Library", "Caches", "selenium", "msedgedriver", "**",
                                           "msedgedriver"), recursive=True)
                )
                cache_candidates.extend(
                    glob.glob(os.path.join(home_dir, "Library", "Caches", "selenium", "msedgedriver", "**",
                                           "edgedriver"), recursive=True)
                )
                cache_candidates = sorted(
                    set(cache_candidates),
                    key=lambda p: os.path.getmtime(p) if os.path.exists(p) else 0,
                    reverse=True
                )
                candidates.extend(cache_candidates)
                system_edge_driver = shutil.which("msedgedriver") or shutil.which("edgedriver")
                if system_edge_driver:
                    candidates.append(system_edge_driver)
        else:
            if is_windows:
                candidates.extend([
                    os.path.join(bin_dir, "chromedriver.exe"),
                ])
                candidates.extend(glob.glob(os.path.join(tmp_dir, "**", "chromedriver.exe"), recursive=True))
            else:
                candidates.extend([
                    os.path.join(bin_dir, "chromedriver"),
                ])
                candidates.extend(glob.glob(os.path.join(tmp_dir, "**", "chromedriver"), recursive=True))
                cache_candidates = []
                cache_candidates.extend(
                    glob.glob(os.path.join(home_dir, ".cache", "selenium", "chromedriver", "**", "chromedriver"),
                              recursive=True)
                )
                cache_candidates.extend(
                    glob.glob(os.path.join(home_dir, "Library", "Caches", "selenium", "chromedriver", "**",
                                           "chromedriver"), recursive=True)
                )
                cache_candidates = sorted(
                    set(cache_candidates),
                    key=lambda p: os.path.getmtime(p) if os.path.exists(p) else 0,
                    reverse=True
                )
                candidates.extend(cache_candidates)
                system_chrome_driver = shutil.which("chromedriver")
                if system_chrome_driver:
                    candidates.append(system_chrome_driver)

        for path in candidates:
            if path and os.path.isfile(path):
                return path
        return ""

    def _ensure_driver_executable(self, driver_path: str):
        """确保非 Windows 平台的 driver 文件具备可执行权限。"""
        if not driver_path or os.name == "nt":
            return
        try:
            mode = os.stat(driver_path).st_mode
            if mode & 0o111:
                return
            os.chmod(driver_path, mode | 0o755)
            self.logger.info(f"已补充 driver 执行权限: {driver_path}")
        except Exception as chmod_err:
            self.logger.warning(f"补充 driver 执行权限失败: {driver_path}, err={chmod_err}")

    def _start_chrome_with_profile(self, profile_dir: str, *, headless: bool):
        log_path = self._driver_log_path("init")
        driver_path = self._resolve_driver_executable("chrome")
        if driver_path:
            self._ensure_driver_executable(driver_path)
            service = ChromeService(executable_path=driver_path, log_output=log_path, service_args=["--verbose"])
            self.logger.info(f"使用本地 ChromeDriver: {driver_path}")
        else:
            service = ChromeService(log_output=log_path, service_args=["--verbose"])
            self.logger.warning("未找到可复用的本地 ChromeDriver（bin/缓存/PATH），改用 Selenium 自动解析 driver（首次或版本变更会较慢）")
        options = self._build_chrome_options(profile_dir, headless=headless)
        self.logger.info(f"ChromeDriver 日志路径: {log_path}")
        try:
            return chrome_webdriver.WebDriver(service=service, options=options)
        except WebDriverException as local_err:
            if driver_path:
                self.logger.warning(
                    "本地 ChromeDriver 启动失败，自动回退 Selenium 解析 driver: %s",
                    local_err
                )
                fallback_service = ChromeService(log_output=log_path, service_args=["--verbose"])
                return chrome_webdriver.WebDriver(service=fallback_service, options=options)
            raise

    def _start_edge_with_profile(self, profile_dir: str, *, headless: bool):
        log_path = self._driver_log_path("init")
        driver_path = self._resolve_driver_executable("edge")
        if driver_path:
            self._ensure_driver_executable(driver_path)
            service = EdgeService(executable_path=driver_path, log_output=log_path, service_args=["--verbose"])
            self.logger.info(f"使用本地 EdgeDriver: {driver_path}")
        else:
            service = EdgeService(log_output=log_path, service_args=["--verbose"])
            self.logger.warning("未找到可复用的本地 EdgeDriver（bin/缓存/PATH），改用 Selenium 自动解析 driver（首次或版本变更会较慢）")
        options = self._build_edge_options(profile_dir, headless=headless)
        self.logger.info(f"EdgeDriver 日志路径: {log_path}")
        try:
            return edge_webdriver.WebDriver(service=service, options=options)
        except WebDriverException as local_err:
            if driver_path:
                self.logger.warning(
                    "本地 EdgeDriver 启动失败，自动回退 Selenium 解析 driver: %s",
                    local_err
                )
                fallback_service = EdgeService(log_output=log_path, service_args=["--verbose"])
                return edge_webdriver.WebDriver(service=fallback_service, options=options)
            raise

    def _start_browser_with_profile(self, profile_dir: str, *, headless: bool):
        if self.browser_name == "edge":
            return self._start_edge_with_profile(profile_dir, headless=headless)
        return self._start_chrome_with_profile(profile_dir, headless=headless)

    def _set_window_state_via_cdp(self, state: str) -> bool:
        if state not in {"normal", "minimized", "maximized", "fullscreen"}:
            return False
        try:
            info = self.driver.execute_cdp_cmd("Browser.getWindowForTarget", {})
            window_id = info.get("windowId")
            if window_id is None:
                return False
            self.driver.execute_cdp_cmd(
                "Browser.setWindowBounds",
                {"windowId": window_id, "bounds": {"windowState": state}}
            )
            return True
        except Exception:
            return False

    def _minimize_browser_window(self) -> bool:
        try:
            if self._set_window_state_via_cdp("minimized"):
                return True
            self.driver.minimize_window()
            return True
        except Exception:
            return False

    def _restore_browser_window_for_verify(self) -> bool:
        try:
            restored = self._set_window_state_via_cdp("normal")
            if not restored:
                try:
                    self.driver.maximize_window()
                    restored = True
                except Exception:
                    restored = False
            if restored:
                try:
                    self.driver.switch_to.window(self.driver.current_window_handle)
                except Exception:
                    pass
            return restored
        except Exception:
            return False

    def _clean_profile_runtime_locks(self, profile_dir: str):
        targets = [
            os.path.join(profile_dir, "SingletonLock"),
            os.path.join(profile_dir, "SingletonSocket"),
            os.path.join(profile_dir, "SingletonCookie"),
            os.path.join(profile_dir, "DevToolsActivePort"),
            os.path.join(profile_dir, "Default", "SingletonLock"),
            os.path.join(profile_dir, "Default", "SingletonSocket"),
            os.path.join(profile_dir, "Default", "SingletonCookie"),
            os.path.join(profile_dir, "Default", "DevToolsActivePort"),
        ]
        lock_file_names = {"LOCK", "SingletonLock", "SingletonSocket", "SingletonCookie", "DevToolsActivePort"}
        for root, _, files in os.walk(profile_dir):
            for fname in files:
                if fname in lock_file_names:
                    targets.append(os.path.join(root, fname))

        removed = []
        for path in set(targets):
            if not os.path.lexists(path):
                continue
            try:
                if os.path.isdir(path) and not os.path.islink(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                removed.append(path)
            except Exception as e:
                self.logger.warning(f"清理 Chrome 运行锁文件失败: {path} ({e})")
        if removed:
            self.logger.info(f"已清理 profile 锁文件: {len(removed)} 个")

    # ---- stop / pause ----
    def request_stop(self):
        self.stop_requested = True

    def check_stop(self):
        if self.stop_requested:
            raise KeyboardInterrupt("收到停止信号")
        if self.pause_event.is_set():
            self._wait_until_resumed()

    def request_pause(self, reason: str = ""):
        self.pause_reason = reason or self.pause_reason
        self._pause_logged = False
        self.pause_event.set()

    def resume(self):
        self.pause_reason = ""
        self._pause_logged = False
        self.pause_event.clear()

    def _wait_until_resumed(self):
        if not self._pause_logged:
            r = f"（原因：{self.pause_reason}）" if self.pause_reason else ""
            self.logger.warning(f"采集已暂停{r}，请在处理完验证码后点击 GUI 中的“恢复运行”。")
            self._pause_logged = True

        while self.pause_event.is_set():
            if self.stop_requested:
                raise KeyboardInterrupt("收到停止信号")
            time.sleep(0.2)

    # ---- captcha detect ----
    def _detect_and_handle_captcha(self, where: str):
        try:
            can_pause = self.on_captcha_detected is not None

            def _handle_detected(captcha_type: str, reason: str):
                self.logger.warning(reason)
                if self.run_in_background:
                    if self._restore_browser_window_for_verify():
                        self.logger.warning("检测到验证码：已恢复浏览器窗口，等待人工处理")
                    else:
                        self.logger.warning("检测到验证码：尝试恢复浏览器窗口失败，请手动切换浏览器处理")
                try:
                    captured = self._try_capture_login_qr(
                        stage=f"{where}-captcha",
                        reason=f"{captcha_type}:{where}",
                        force_center=True,
                        container_selectors=["#captcha-div", "#red-captcha", ".captcha-modal-content"],
                        override_offset=(0, 0)
                    )
                    if captured:
                        self.logger.info("已自动捕获风控二维码并回显到 GUI（captcha_type=%s, where=%s）", captcha_type, where)
                    else:
                        self.logger.info("未捕获到风控二维码（captcha_type=%s, where=%s），请在浏览器手动处理", captcha_type, where)
                except Exception as cap_err:
                    self.logger.warning("风控二维码捕获异常: %s", cap_err)
                if can_pause:
                    try:
                        self.on_captcha_detected(where, captcha_type)
                    except TypeError:
                        self.on_captcha_detected(where)
                    self.request_pause(f"{captcha_type}@{where}")
                    self._wait_until_resumed()
                    try:
                        self.driver.refresh()
                    except Exception:
                        pass
                    if self.run_in_background:
                        if self._minimize_browser_window():
                            self.logger.info("恢复运行后已重新最小化窗口")
                else:
                    self.logger.warning("当前无暂停回调，需在浏览器中手动完成验证后继续。")
                    time.sleep(2.0)
                return True

            # 1. 检查 .captcha-modal-content (新增)
            modal_els = self.driver.find_elements(By.CLASS_NAME, "captcha-modal-content")
            modal_visible = any(e.is_displayed() for e in modal_els) if modal_els else False
            if modal_visible:
                return _handle_detected(
                    "captcha-modal-content",
                    f"检测到弹窗验证码 .captcha-modal-content（{where}），将暂停采集并等待你处理验证码。"
                )

            # 2. 检查 #captcha-div (严格风控)
            strict_els = self.driver.find_elements(By.CSS_SELECTOR, "#captcha-div")
            strict_visible = any(e.is_displayed() for e in strict_els) if strict_els else False
            if strict_visible:
                return _handle_detected(
                    "captcha-div",
                    f"检测到扫码/风控验证码页 #captcha-div（{where}），将暂停采集并等待你处理验证码。"
                )

            # 3. 检查 #red-captcha
            els = self.driver.find_elements(By.CSS_SELECTOR, "#red-captcha, div#red-captcha")
            visible = any(e.is_displayed() for e in els) if els else False
            if visible:
                return _handle_detected(
                    "red-captcha",
                    f"检测到验证码 #red-captcha（{where}），将暂停采集并等待你处理验证码。"
                )
        except Exception:
            pass
        return False

    def _sanitize_cookie_for_injection(self, cookie: Dict) -> Optional[Dict]:
        if not isinstance(cookie, dict):
            return None
        name = str(cookie.get("name") or "").strip()
        value = str(cookie.get("value") or "").strip()
        if not name or value == "":
            return None

        out = {
            "name": name,
            "value": value,
            "path": str(cookie.get("path") or "/"),
            "secure": bool(cookie.get("secure", False)),
            "httpOnly": bool(cookie.get("httpOnly", False)),
        }

        domain = str(cookie.get("domain") or "").strip()
        if domain:
            out["domain"] = domain

        expiry = cookie.get("expiry")
        if expiry is not None:
            try:
                out["expiry"] = int(float(expiry))
            except Exception:
                pass

        same_site = cookie.get("sameSite")
        if same_site in ("Lax", "Strict", "None"):
            out["sameSite"] = same_site

        return out

    def _has_login_marker(self, *, allow_cookie_fallback: bool = True) -> bool:
        # 若页面明确出现“登录”入口，优先判定为未登录，避免误把公共页面元素当作登录态
        logged_out_reason = self._detect_logged_out_reason()
        if logged_out_reason:
            return False

        # 优先使用强登录态标记：侧边栏/头部里的个人中心链接（公共 feed 区域的 profile 链接不算）
        try:
            profile_links = self.driver.find_elements(
                By.CSS_SELECTOR,
                ".side-bar-component a[href*='/user/profile/'], header a[href*='/user/profile/']"
            )
            for link in profile_links:
                try:
                    if not link.is_displayed():
                        continue
                    href = str(link.get_attribute("href") or "")
                    if "/user/profile/" in href:
                        return True
                except Exception:
                    continue
        except Exception:
            pass

        selectors = [
            ".side-bar-component a[href*='/user/profile/']",
            "header a[href*='/user/profile/']",
        ]
        for selector in selectors:
            try:
                els = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for e in els:
                    if not e.is_displayed():
                        continue
                    href = str(e.get_attribute("href") or "")
                    if "/user/profile/" not in href:
                        continue
                    txt = str(e.text or "").strip()
                    if "登录" in txt:
                        continue
                    return True
            except Exception:
                continue

        if not allow_cookie_fallback:
            return False

        # Cookie 登录时允许弱标记兜底；手动扫码模式不使用该兜底，避免误判访客态
        try:
            ws = self.driver.get_cookie("web_session")
            if ws and str(ws.get("value") or "").strip():
                return True
        except Exception:
            pass
        return False

    def _detect_logged_out_reason(self) -> str:
        checks = [
            ("login-btn-css", By.CSS_SELECTOR, "button[class*='login'], a[class*='login'], div[class*='login']"),
            ("login-text-btn", By.XPATH, "//button[contains(normalize-space(.), '登录')]"),
            ("login-text-link", By.XPATH, "//a[contains(normalize-space(.), '登录')]"),
            ("login-text-div", By.XPATH, "//div[contains(normalize-space(.), '登录') and @role='button']"),
        ]
        for label, by, selector in checks:
            try:
                els = self.driver.find_elements(by, selector)
                for e in els:
                    try:
                        if not e.is_displayed():
                            continue
                        txt = str(e.text or "").strip()
                        if "登录" in txt or "扫码" in txt:
                            return f"{label}:{txt[:30]}"
                    except Exception:
                        continue
            except Exception:
                continue
        return ""

    def _emit_login_qr_event(self, payload: Dict):
        cb = self.on_login_qr_detected
        if not cb:
            return
        try:
            cb(payload)
        except Exception:
            pass

    def _notify_login_qr_cleared(self, stage: str, reason: str = ""):
        self._last_login_qr_fingerprint = ""
        self._last_login_qr_emit_ts = 0.0
        self._emit_login_qr_event({
            "cleared": True,
            "stage": stage,
            "reason": reason,
            "ts": int(time.time())
        })

    def _safe_element_rect(self, elem) -> Optional[Dict[str, float]]:
        try:
            if not elem or not elem.is_displayed():
                return None
            r = elem.rect or {}
            x = float(r.get("x") or 0.0)
            y = float(r.get("y") or 0.0)
            w = float(r.get("width") or 0.0)
            h = float(r.get("height") or 0.0)
            if w <= 0 or h <= 0:
                return None
            return {"x": x, "y": y, "width": w, "height": h}
        except Exception:
            return None

    @staticmethod
    def _rect_contains(outer: Dict[str, float], inner: Dict[str, float]) -> bool:
        try:
            ox, oy = float(outer["x"]), float(outer["y"])
            ow, oh = float(outer["width"]), float(outer["height"])
            ix, iy = float(inner["x"]), float(inner["y"])
            iw, ih = float(inner["width"]), float(inner["height"])
            return (
                ix >= ox and iy >= oy and
                ix + iw <= ox + ow and
                iy + ih <= oy + oh
            )
        except Exception:
            return False

    def _find_login_context_rects(self) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
        anchor_rects: List[Dict[str, float]] = []
        modal_rects: List[Dict[str, float]] = []

        anchor_xpath = (
            "//*[contains(normalize-space(.), '扫码登录') "
            "or contains(normalize-space(.), '请先登录') "
            "or contains(normalize-space(.), '登录后查看更多') "
            "or contains(normalize-space(.), '登录以继续') "
            "or contains(normalize-space(.), '二维码登录')]"
        )
        try:
            anchors = self.driver.find_elements(By.XPATH, anchor_xpath)
        except Exception:
            anchors = []

        for a in anchors:
            r = self._safe_element_rect(a)
            if r:
                anchor_rects.append(r)
            try:
                host = self.driver.execute_script(
                    "return arguments[0] && arguments[0].closest("
                    "'[class*=login],[class*=modal],[class*=dialog],[class*=popup]'"
                    ");",
                    a
                )
            except Exception:
                host = None
            hr = self._safe_element_rect(host) if host is not None else None
            if hr:
                modal_rects.append(hr)

        # 去重（保留大致不同区域）
        uniq_modal: List[Dict[str, float]] = []
        for r in modal_rects:
            duplicated = False
            for u in uniq_modal:
                if (
                    abs(r["x"] - u["x"]) < 8 and
                    abs(r["y"] - u["y"]) < 8 and
                    abs(r["width"] - u["width"]) < 8 and
                    abs(r["height"] - u["height"]) < 8
                ):
                    duplicated = True
                    break
            if not duplicated:
                uniq_modal.append(r)

        return anchor_rects, uniq_modal

    def _find_visible_rects_by_selectors(self, selectors: List[str]) -> List[Dict[str, float]]:
        rects: List[Dict[str, float]] = []
        for selector in selectors:
            try:
                elems = self.driver.find_elements(By.CSS_SELECTOR, selector)
            except Exception:
                elems = []
            for elem in elems:
                r = self._safe_element_rect(elem)
                if r:
                    rects.append(r)
        uniq: List[Dict[str, float]] = []
        for r in rects:
            duplicated = False
            for u in uniq:
                if (
                    abs(r["x"] - u["x"]) < 8 and
                    abs(r["y"] - u["y"]) < 8 and
                    abs(r["width"] - u["width"]) < 8 and
                    abs(r["height"] - u["height"]) < 8
                ):
                    duplicated = True
                    break
            if not duplicated:
                uniq.append(r)
        return uniq

    def _try_capture_login_qr(
            self,
            *,
            stage: str,
            reason: str,
            force_center: bool = False,
            container_selectors: Optional[List[str]] = None,
            override_offset: Optional[Tuple[int, int]] = None
    ) -> bool:
        selectors = []
        if container_selectors:
            for csel in container_selectors:
                selectors.append((f"container-img:{csel}", f"{csel} img"))
                selectors.append((f"container-canvas:{csel}", f"{csel} canvas"))
        selectors.extend([
            ("login-qrcode-img", "[class*='login'] [class*='qrcode'] img, [class*='qrcode'] img"),
            ("login-qrcode-canvas", "[class*='login'] [class*='qrcode'] canvas, [class*='qrcode'] canvas"),
            ("qrcode-img-src", "img[src*='qrcode'], img[src*='qr']"),
            ("login-img", "[class*='login'] img"),
            ("dialog-img", "[class*='modal'] img, [class*='dialog'] img, [class*='popup'] img"),
            ("captcha-img", "[class*='captcha'] img"),
            ("captcha-canvas", "[class*='captcha'] canvas"),
        ])

        now = time.time()
        if self._last_login_qr_emit_ts > 0 and now - self._last_login_qr_emit_ts < 1.2:
            return False

        anchor_rects, modal_rects = self._find_login_context_rects()
        container_rects = self._find_visible_rects_by_selectors(container_selectors or []) if container_selectors else []
        try:
            if override_offset is not None:
                offset_x, offset_y = int(override_offset[0]), int(override_offset[1])
            else:
                offset_x, offset_y = self.get_login_qr_offset()
                offset_x = int(offset_x)
                offset_y = int(offset_y)
        except Exception:
            offset_x, offset_y = 0, 0

        try:
            viewport = self.driver.execute_script(
                "return {w: window.innerWidth || 0, h: window.innerHeight || 0};"
            ) or {}
            vw = float(viewport.get("w") or 0.0)
            vh = float(viewport.get("h") or 0.0)
        except Exception:
            vw, vh = 0.0, 0.0

        candidates: List[Tuple[float, str, str, object, Dict[str, float]]] = []
        for source, selector in selectors:
            try:
                elems = self.driver.find_elements(By.CSS_SELECTOR, selector)
            except Exception:
                continue
            for elem in elems:
                try:
                    rect = self._safe_element_rect(elem)
                    if not rect:
                        continue
                    w = rect["width"]
                    h = rect["height"]
                    if w < 90 or h < 90 or w > 640 or h > 640:
                        continue
                    ratio = w / h if h > 0 else 0
                    if ratio < 0.65 or ratio > 1.45:
                        continue

                    cx = rect["x"] + w / 2.0
                    cy = rect["y"] + h / 2.0

                    score = 0.0
                    source_weight = {
                        "captcha-img": 130.0,
                        "captcha-canvas": 130.0,
                        "login-qrcode-img": 120.0,
                        "login-qrcode-canvas": 110.0,
                        "qrcode-img-src": 80.0,
                        "login-img": 35.0,
                        "dialog-img": 20.0,
                    }.get(source, 0.0)
                    if source.startswith("container-img"):
                        source_weight = 145.0
                    elif source.startswith("container-canvas"):
                        source_weight = 145.0
                    score += source_weight

                    # 尺寸和比例越像二维码，分越高
                    score += max(0.0, 60.0 - abs(min(w, h) - 220.0) * 0.35)
                    score += max(0.0, 35.0 - abs(1.0 - ratio) * 120.0)

                    # 倾向登录弹窗内元素
                    inside_modal = any(self._rect_contains(mr, rect) for mr in modal_rects) if modal_rects else False
                    if inside_modal:
                        score += 95.0
                    elif modal_rects:
                        score -= 55.0

                    inside_container = any(self._rect_contains(cr, rect) for cr in container_rects) if container_rects else False
                    if inside_container:
                        score += 120.0
                    elif container_rects:
                        score -= 85.0

                    if force_center:
                        if container_rects:
                            # 风控码常在弹层正中，优先靠近容器中心
                            for cr in container_rects:
                                tx = cr["x"] + cr["width"] / 2.0
                                ty = cr["y"] + cr["height"] / 2.0
                                score += max(0.0, 55.0 - abs(cx - tx) * 0.14)
                                score += max(0.0, 55.0 - abs(cy - ty) * 0.14)
                        elif vw > 0 and vh > 0:
                            # 无容器时退化到视口中心
                            score += max(0.0, 50.0 - abs(cx - vw / 2.0) * 0.12)
                            score += max(0.0, 50.0 - abs(cy - vh / 2.0) * 0.12)
                    else:
                        # 倾向登录文案左侧元素（Cookie 失效登录态）
                        for ar in anchor_rects:
                            ax = ar["x"] + ar["width"] / 2.0
                            ay = ar["y"] + ar["height"] / 2.0
                            target_x = ax + float(offset_x)
                            target_y = ay + float(offset_y)
                            score += max(0.0, 44.0 - abs(cx - target_x) * 0.12)
                            score += max(0.0, 28.0 - abs(cy - target_y) * 0.10)
                            if cx < ax:
                                score += 10.0

                    # 略偏向视口中间区域，避免命中左侧列表缩略图
                    if vw > 0 and vh > 0:
                        dx = abs(cx - vw / 2.0) / max(vw / 2.0, 1.0)
                        dy = abs(cy - vh / 2.0) / max(vh / 2.0, 1.0)
                        score += max(-25.0, 20.0 - (dx + dy) * 40.0)

                    candidates.append((score, source, selector, elem, rect))
                except Exception:
                    continue

        if not candidates:
            return False

        candidates.sort(key=lambda x: x[0], reverse=True)
        top_score, top_source, top_selector, top_elem, top_rect = candidates[0]

        try:
            self.logger.info(
                "二维码候选已选中: score=%.1f, source=%s, rect=(x=%.1f,y=%.1f,w=%.1f,h=%.1f), offset=(%d,%d), reason=%s",
                top_score, top_source,
                top_rect["x"], top_rect["y"], top_rect["width"], top_rect["height"],
                offset_x, offset_y, reason
            )
        except Exception:
            pass

        try:
            png_bytes = top_elem.screenshot_as_png
            if not png_bytes or len(png_bytes) < 300:
                return False
        except Exception:
            return False

        fp = hashlib.sha1(png_bytes).hexdigest()
        if fp == self._last_login_qr_fingerprint and (now - self._last_login_qr_emit_ts) < 8.0:
            return False

        self._last_login_qr_fingerprint = fp
        self._last_login_qr_emit_ts = now

        payload = {
            "cleared": False,
            "stage": stage,
            "reason": reason,
            "source": top_source,
            "selector": top_selector,
            "url": str(self.driver.current_url or ""),
            "x": int(round(top_rect["x"])),
            "y": int(round(top_rect["y"])),
            "width": int(round(top_rect["width"])),
            "height": int(round(top_rect["height"])),
            "offset_x": int(offset_x),
            "offset_y": int(offset_y),
            "png_b64": base64.b64encode(png_bytes).decode("ascii"),
            "ts": int(now),
        }
        self._emit_login_qr_event(payload)
        return True

    def _detect_captcha_or_risk_reason(self) -> str:
        checks = [
            ("captcha-modal-content", By.CLASS_NAME, "captcha-modal-content"),
            ("captcha-div", By.CSS_SELECTOR, "#captcha-div"),
            ("red-captcha", By.CSS_SELECTOR, "#red-captcha, div#red-captcha"),
        ]
        for label, by, selector in checks:
            try:
                els = self.driver.find_elements(by, selector)
                if els and any(e.is_displayed() for e in els):
                    return label
            except Exception:
                continue

        try:
            current_url = str(self.driver.current_url or "").lower()
            if "captcha" in current_url or "verify" in current_url:
                return f"url={current_url}"
        except Exception:
            pass

        try:
            body_text = self.driver.execute_script(
                "return (document.body && document.body.innerText ? document.body.innerText : '').slice(0, 6000);"
            ) or ""
            text = str(body_text)
            risk_keywords = [
                "请完成验证",
                "请通过验证",
                "扫码验证",
                "异常流量",
                "访问过于频繁",
                "请先登录",
                "登录后查看更多",
                "登录以继续",
            ]
            for kw in risk_keywords:
                if kw in text:
                    return kw
        except Exception:
            pass
        return ""

    def _validate_cross_page_session(self, *, allow_cookie_fallback: bool = True) -> Tuple[bool, str]:
        check_pages = [
            ("explore", "https://www.xiaohongshu.com/explore"),
            ("search", "https://www.xiaohongshu.com/search_result?keyword=BJD"),
        ]
        for where, url in check_pages:
            self.driver.get(url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            risk_reason = self._detect_captcha_or_risk_reason()
            if risk_reason:
                return False, f"{where} 页触发风控/验证: {risk_reason}"
            logged_out_reason = self._detect_logged_out_reason()
            if logged_out_reason:
                return False, f"{where} 页检测到未登录信号: {logged_out_reason}"

        if not self._has_login_marker(allow_cookie_fallback=allow_cookie_fallback):
            return False, "跨页后未检测到登录态标记"
        return True, ""

    def _wait_for_session_ready(
            self,
            timeout_sec: int,
            stage: str,
            *,
            allow_cookie_fallback: bool = True
    ) -> Tuple[bool, str]:
        deadline = time.time() + max(1, int(timeout_sec))
        last_reason = "会话尚未就绪"

        while time.time() < deadline:
            if self.stop_requested:
                raise KeyboardInterrupt("收到停止信号")

            try:
                if not self._has_login_marker(allow_cookie_fallback=allow_cookie_fallback):
                    last_reason = "未检测到登录态标记"
                    self._try_capture_login_qr(stage=stage, reason=last_reason)
                    time.sleep(1.0)
                    continue

                ok, reason = self._validate_cross_page_session(allow_cookie_fallback=allow_cookie_fallback)
                if ok:
                    self._notify_login_qr_cleared(stage=stage, reason="会话已稳定")
                    return True, ""
                last_reason = reason or "跨页验证未通过"
                if ("登录" in last_reason) or ("未检测到登录态" in last_reason):
                    self._try_capture_login_qr(stage=stage, reason=last_reason)
                self.logger.info(f"{stage} 会话未稳定：{last_reason}，等待重试...")
            except Exception as exc:
                last_reason = str(exc)
                self.logger.info(f"{stage} 会话校验异常：{last_reason}，等待重试...")

            time.sleep(2.0)

        return False, last_reason

    def login(self, cookie_list: Optional[List[Dict]] = None) -> Optional[List[Dict]]:
        """
        登录逻辑
        :param cookie_list: 如果提供了 cookie_list，则注入 Cookie；否则等待扫码
        :return: 登录成功后返回最新 Cookie 列表
        """
        self.driver.get('https://www.xiaohongshu.com/')
        self._detect_and_handle_captcha("explore")

        if cookie_list:
            self.logger.info("正在注入选定的 Cookie...")
            try:
                try:
                    self.driver.delete_all_cookies()
                except Exception:
                    pass

                injected = 0
                for cookie in cookie_list:
                    c = self._sanitize_cookie_for_injection(cookie)
                    if not c:
                        continue
                    self.driver.add_cookie(c)
                    injected += 1

                self.logger.info(f"Cookie 注入完成：{injected} 条")
                self.driver.refresh()
                ok, reason = self._wait_for_session_ready(
                    timeout_sec=30,
                    stage="Cookie登录",
                    allow_cookie_fallback=False
                )
                if ok:
                    self.logger.info("Cookie 登录成功（已通过跨页会话校验）")
                    self._notify_login_qr_cleared(stage="Cookie登录", reason="Cookie 登录成功")
                    sleep(1)
                    return self.driver.get_cookies()
                self.logger.warning(f"Cookie 登录会话不稳定：{reason}，将转为手动登录")
            except Exception as e:
                self.logger.warning(f"Cookie 登录失败或失效: {e}，将转为手动登录")

        # 手动登录流程
        self.logger.info("等待手动扫码登录（180s 超时）...")
        ok, reason = self._wait_for_session_ready(
            timeout_sec=180,
            stage="扫码登录",
            allow_cookie_fallback=False
        )
        if not ok:
            raise RuntimeError(f"扫码后会话仍未稳定：{reason}")
        self.logger.info('扫码登录成功（会话稳定）')
        self._notify_login_qr_cleared(stage="扫码登录", reason="扫码登录成功")
        return self.driver.get_cookies()

    def extract_current_links(self):
        current_links = set()
        try:
            items = self.driver.find_elements(By.CSS_SELECTOR, '.note-item')
            for item in items:
                try:
                    link_element = item.find_element(By.CSS_SELECTOR, 'a.cover.mask.ld[href^="/user/profile/"]')
                    raw_url = link_element.get_attribute('href')
                    clean_url = raw_url.replace('&amp;', '&')
                    current_links.add(clean_url)
                except Exception:
                    continue
        except Exception as e:
            self.logger.warning(f"提取链接异常: {e}")
        return current_links

    def smart_scroll(self, spd_setting=1, max_scroll=None, stats: Optional[Dict[str, int]] = None):
        total_scroll = 0
        no_new_count = 0
        max_no_new = 6
        last_height = 0
        max_scroll = self.max_scroll_default if max_scroll is None else int(max_scroll)

        self.logger.info(f"智能滚动设置: 最大滚动次数={max_scroll}（滚动等待将实时读取 GUI 配置）")

        self.check_stop()
        self._detect_and_handle_captcha("scroll")
        current_links = self.extract_current_links()
        converted_new_links = {convert_xhs_url(link).split('?')[0] for link in (current_links - self.all_links)}
        new_links = converted_new_links - {convert_xhs_url(x).split('?')[0] for x in self.all_links}

        if spd_setting == 2 and new_links:
            self.process_quick_data(new_links, stats=stats)

        self.all_links.update(current_links)
        self.logger.info(f"[首屏] 当前总链接数：{len(self.all_links)} 新增：{len(new_links)}")

        if max_scroll <= 0:
            return

        while no_new_count < max_no_new and total_scroll < max_scroll:
            self.check_stop()
            self._detect_and_handle_captcha("scroll")

            current_links = self.extract_current_links()
            converted_new_links = {convert_xhs_url(link).split('?')[0] for link in (current_links - self.all_links)}
            new_links = converted_new_links - {convert_xhs_url(x).split('?')[0] for x in self.all_links}

            if spd_setting == 2 and new_links:
                self.process_quick_data(new_links, stats=stats)

            self.all_links.update(current_links)
            self.logger.info(f"当前总链接数：{len(self.all_links)} 新增：{len(new_links)}")

            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            scroll_sleep = max(0.0, float(self.get_scroll_sleep()))
            self._sleep_with_progress("scroll", scroll_sleep)

            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                no_new_count += 1
            else:
                no_new_count = 0
                last_height = new_height

            total_scroll += 1

    def process_single_note(self, origin_note_url: str) -> Optional[Dict]:
        note_url = convert_xhs_url(origin_note_url)
        self.logger.info(f"打开URL(单窗口复用): {origin_note_url} -> {note_url}")
        img_urls = []

        try:
            self.driver.get(note_url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".note-container"))
            )

            self._detect_and_handle_captcha("detail")

            detail_sleep = max(0.0, float(self.get_detail_sleep()))
            self._sleep_with_progress("detail", detail_sleep)

            try:
                time_element = self.driver.find_element(By.CSS_SELECTOR, '.bottom-container .date')
                raw_time = time_element.text.strip()
                auth_time = parse_xhs_time(raw_time)
            except Exception as te:
                self.logger.warning(f"时间提取失败: {te}")
                auth_time = 0

            try:
                like_count = 0
                like_element = self.driver.find_element(By.CSS_SELECTOR, '.interact-container .like-active .count')
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

            content = ''
            try:
                text_element = self.driver.find_element(By.CSS_SELECTOR, '.note-content .desc')
                content = text_element.text.replace('\n', ' ').strip()[:2000]
            except Exception as te:
                self.logger.warning(f"内容提取失败: {te}")

            title = ''
            try:
                title_element = self.driver.find_element(By.ID, 'detail-title')
                title = title_element.text.strip()
            except Exception as title_e:
                self.logger.warning(f"标题提取失败: {title_e}")

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

    def process_quick_data(self, new_links: set, stats: Optional[Dict[str, int]] = None):
        current_items = self.driver.find_elements(By.CSS_SELECTOR, '.note-item')
        for item in current_items:
            try:
                link = item.find_element(By.CSS_SELECTOR, 'a.cover.mask.ld').get_attribute('href')
                clean_url = convert_xhs_url(link).split('?')[0]

                if self.url_checker and self.url_checker(clean_url):
                    self.logger.info(f"已存在，跳过快速采集: {clean_url}")
                    if stats is not None:
                        self._inc_stat(stats, "skipped_existing_urls")
                    continue
                if clean_url not in new_links:
                    continue

                try:
                    img = item.find_element(By.CSS_SELECTOR, 'img[src*="xhscdn.com"]')
                    cover_url = img.get_attribute('src').split('?')[0]
                except Exception as e:
                    self.logger.warning(f"首图提取失败: {e}")
                    cover_url = ""

                try:
                    title = item.find_element(By.CSS_SELECTOR, '.title > span').text[:600]
                except Exception:
                    title = "无标题"

                self.collected_quick_data.append({
                    'url': clean_url,
                    'images': [cover_url] if cover_url else [],
                    'title': title,
                    'content': ''
                })
            except Exception as e:
                self.logger.error(f"快速采集异常: {e}")

    def _sleep_with_progress(self, phase: str, total: float):
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

    def crawl_target(self, row: Dict):
        current_stats = self._new_crawl_stats()
        self.last_target_stats = current_stats
        try:
            self.check_stop()

            if self.target_type == TargetType.BRAND:
                spd_setting = row.get('rednote_spd_setting', 1)
                if spd_setting == 3:
                    self.logger.info(f"品牌[{row.get('brand_name', '')}] 配置不采集，跳过")
                    return True
                max_scroll = self.max_scroll_default
            else:
                spd_setting = row.get('rednote_spd_setting_for_artist', 3)
                if spd_setting == 3:
                    self.logger.info(f"艺术家[{row.get('brand_name', '')}] 配置不采集，跳过")
                    return True
                max_scroll = self.max_scroll_default

            target_urls = get_rednote_urls(row)
            if not target_urls:
                return True

            for target_url in target_urls:
                self.check_stop()
                self.driver.get(target_url)
                self._detect_and_handle_captcha("scroll")
                self.smart_scroll(spd_setting, max_scroll, stats=current_stats)

                if spd_setting == 1:
                    for note_url in list(self.all_links):
                        self.check_stop()
                        base_url = convert_xhs_url(note_url).split('?')[0]
                        if self.url_checker and self.url_checker(base_url):
                            self._inc_stat(current_stats, "skipped_existing_urls")
                            continue
                        self._inc_stat(current_stats, "opened_urls")
                        detail = self.process_single_note(note_url)
                        detail_sleep = max(0.0, float(self.get_detail_sleep()))
                        self._sleep_with_progress("detail", detail_sleep)

                        if detail:
                            if self.target_type == TargetType.BRAND:
                                detail.update({'brand_id': row['id'], 'brand_name': row.get('brand_name', '')})
                            else:
                                detail.update(
                                    {'artist_id': row['id'], 'artist_name': row.get('brand_name', ''), 'full_get': 0})
                            if self.insert_callback:
                                self.insert_callback(detail)
                                self._inc_stat(current_stats, "inserted_urls")

                elif spd_setting == 2:
                    for quick_data in self.collected_quick_data:
                        self.check_stop()
                        if self.target_type == TargetType.BRAND:
                            quick_data.update(
                                {'brand_id': row['id'], 'brand_name': row.get('brand_name', ''), 'auth_time': 0})
                        else:
                            quick_data.update(
                                {'artist_id': row['id'], 'artist_name': row.get('brand_name', ''), 'auth_time': 0,
                                 'full_get': 0})
                        if self.insert_callback:
                            self.insert_callback(quick_data)
                            self._inc_stat(current_stats, "inserted_urls")
                    self.logger.info(f"快速采集数据入库成功: {len(self.collected_quick_data)} 条")

                self.all_links.clear()
                self.collected_quick_data.clear()
            for k in current_stats.keys():
                self.total_crawl_stats[k] = int(self.total_crawl_stats.get(k, 0)) + int(current_stats.get(k, 0))
            return True
        except KeyboardInterrupt:
            self.logger.info("收到停止信号，已终止当前对象采集")
            return False
        except Exception as e:
            self.logger.error(f"对象采集失败 {resolve_rednote_url(row)}: {e}")
            return False


class TextHandler(logging.Handler):
    def __init__(self, text_widget: tk.Text, ui_dispatch: Optional[Callable[[Callable], None]] = None):
        super().__init__()
        self.text_widget = text_widget
        self.ui_dispatch = ui_dispatch

    def emit(self, record):
        msg = self.format(record)
        try:
            if self.ui_dispatch:
                self.ui_dispatch(lambda: self._append(msg))
            else:
                self._append(msg)
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
        self._input_focus_classes = {"Entry", "TEntry", "Text", "Spinbox", "TCombobox"}
        self._ui_queue: "queue.Queue[Callable]" = queue.Queue()
        self._ui_pump_after_id = None
        self._captcha_prompt_active = False
        self.master.title("小红书爬虫 · 采集控制台（数据库Cookie管理版）")
        self.master.geometry("920x820")
        compact_style = ttk.Style(self.master)
        compact_style.configure("Compact.TButton", padding=(4, 1))
        compact_style.configure("Compact.TCheckbutton", padding=(0, 0))
        compact_style.configure("Compact.TRadiobutton", padding=(0, 0))

        # --- 账号管理区域 ---
        acc_frame = ttk.LabelFrame(master, text="账号管理")
        acc_frame.pack(fill="x", padx=8, pady=(8, 6))

        ttk.Label(acc_frame, text="选择登录账号：").pack(side='left', padx=4, pady=4)
        self.cb_account = ttk.Combobox(acc_frame, state='readonly', width=30)
        self.cb_account.pack(side='left', padx=4, pady=4)
        self.cb_account.bind("<<ComboboxSelected>>", self.on_account_selected)

        self.btn_add_acc = ttk.Button(
            acc_frame, text="+ 新增/更新账号", command=self.add_new_account, style="Compact.TButton"
        )
        self.btn_add_acc.pack(side='left', padx=(8, 4), pady=4)

        self.btn_remove_acc = ttk.Button(
            acc_frame, text="- 移除账号", command=self.remove_account, style="Compact.TButton"
        )
        self.btn_remove_acc.pack(side='left', padx=4, pady=4)

        ttk.Button(
            acc_frame, text="刷新列表", command=self.load_accounts, style="Compact.TButton"
        ).pack(side='left', padx=4, pady=4)

        # --- 采集参数 ---
        frm = ttk.LabelFrame(master, text="采集参数（可运行中随时修改）")
        frm.pack(fill="x", padx=8, pady=(0, 6))

        ttk.Label(frm, text="滚动等待(秒)：").grid(row=0, column=0, padx=4, pady=4, sticky='e')
        self.var_scroll_sleep = tk.StringVar(value="10.5")
        ttk.Entry(frm, textvariable=self.var_scroll_sleep, width=9).grid(row=0, column=1, padx=4, pady=4, sticky='w')

        ttk.Label(frm, text="详情等待(秒)：").grid(row=0, column=2, padx=4, pady=4, sticky='e')
        self.var_detail_sleep = tk.StringVar(value="5.0")
        ttk.Entry(frm, textvariable=self.var_detail_sleep, width=9).grid(row=0, column=3, padx=4, pady=4, sticky='w')

        ttk.Label(frm, text="最大滚动次数：").grid(row=0, column=4, padx=4, pady=4, sticky='e')
        self.var_max_scroll = tk.StringVar(value="20")
        ttk.Entry(frm, textvariable=self.var_max_scroll, width=9).grid(row=0, column=5, padx=4, pady=4, sticky='w')

        ttk.Label(frm, text="浏览器：").grid(row=0, column=6, padx=4, pady=4, sticky='e')
        self.var_browser = tk.StringVar(value="chrome")
        self.cb_browser = ttk.Combobox(
            frm,
            state='readonly',
            width=9,
            textvariable=self.var_browser,
            values=("chrome", "edge")
        )
        self.cb_browser.grid(row=0, column=7, padx=4, pady=4, sticky='w')

        self.var_headless = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            frm, text="无头模式(Headless)", variable=self.var_headless, style="Compact.TCheckbutton"
        ).grid(row=0, column=8, padx=4, pady=4)
        self.var_background_mode = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            frm, text="后台最小化运行", variable=self.var_background_mode, style="Compact.TCheckbutton"
        ).grid(
            row=0, column=9, padx=4, pady=4, sticky='w'
        )

        type_frame = ttk.LabelFrame(master, text="采集对象")
        type_frame.pack(fill='x', padx=8, pady=(0, 6))
        self.var_target_type = tk.StringVar(value=TargetType.BRAND)
        ttk.Radiobutton(
            type_frame, text="品牌（娃店 → spider_log）",
            value=TargetType.BRAND, variable=self.var_target_type,
            command=lambda: self.on_target_type_change(TargetType.BRAND),
            style="Compact.TRadiobutton"
        ).pack(side='left', padx=8, pady=4)
        ttk.Radiobutton(
            type_frame, text="艺术家（妆师/毛娘 → artist_spider_log）",
            value=TargetType.ARTIST, variable=self.var_target_type,
            command=lambda: self.on_target_type_change(TargetType.ARTIST),
            style="Compact.TRadiobutton"
        ).pack(side='left', padx=8, pady=4)

        shutdown_frame = ttk.LabelFrame(master, text="自动关机")
        shutdown_frame.pack(fill='x', padx=8, pady=(0, 6))

        ttk.Label(shutdown_frame, text="定时关机(分钟)：").grid(row=0, column=0, padx=4, pady=4, sticky='e')
        self.var_shutdown_timer_minutes = tk.StringVar(value="")
        ttk.Entry(shutdown_frame, textvariable=self.var_shutdown_timer_minutes, width=10).grid(
            row=0, column=1, padx=4, pady=4, sticky='w'
        )

        self.btn_schedule_shutdown = ttk.Button(
            shutdown_frame, text="设置/重设定时关机", command=self.schedule_timed_shutdown, style="Compact.TButton"
        )
        self.btn_schedule_shutdown.grid(row=0, column=2, padx=4, pady=4, sticky='w')

        self.btn_cancel_shutdown = ttk.Button(
            shutdown_frame, text="取消关机计划", command=self.cancel_scheduled_shutdown, state='disabled',
            style="Compact.TButton"
        )
        self.btn_cancel_shutdown.grid(row=0, column=3, padx=4, pady=4, sticky='w')

        self.var_shutdown_after_finish = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            shutdown_frame,
            text="抓取完毕后自动关机（提前1分钟提醒）",
            variable=self.var_shutdown_after_finish,
            style="Compact.TCheckbutton"
        ).grid(row=0, column=4, padx=4, pady=4, sticky='w')

        self.var_shutdown_status = tk.StringVar(value="关机计划：未设置")
        ttk.Label(shutdown_frame, textvariable=self.var_shutdown_status).grid(
            row=1, column=0, columnspan=5, padx=4, pady=(0, 4), sticky='w'
        )

        ctrl = ttk.Frame(master)
        ctrl.pack(fill='x', padx=8)

        self.btn_start = ttk.Button(ctrl, text="开始采集", command=self.start, style="Compact.TButton")
        self.btn_stop = ttk.Button(ctrl, text="停止采集", command=self.stop, state='disabled', style="Compact.TButton")
        self.btn_resume = ttk.Button(ctrl, text="恢复运行", command=self.resume, state='disabled', style="Compact.TButton")
        self.btn_skip = ttk.Button(
            ctrl, text="跳过当前等待", command=self.skip_current_wait, state='disabled', style="Compact.TButton"
        )
        # 新增的维护按钮
        self.btn_maintenance = ttk.Button(ctrl, text="数据维护", command=self.run_data_maintenance, style="Compact.TButton")
        self.btn_upload_images = ttk.Button(ctrl, text="上传图片", command=self.run_spider_image_upload, style="Compact.TButton")
        self.btn_stop_upload = ttk.Button(
            ctrl, text="停止上传", command=self.stop_spider_image_upload, state='disabled', style="Compact.TButton"
        )
        self.btn_open_detection_page = ttk.Button(
            ctrl, text="访问检测页", command=self.open_detection_page, style="Compact.TButton"
        )
        self.btn_open_sannysoft = ttk.Button(
            ctrl, text="访问 SannySoft", command=self.open_sannysoft_detection_page, style="Compact.TButton"
        )
        self.btn_delete_failed_upload = ttk.Button(
            ctrl,
            text="删除传图失败记录",
            command=self.delete_failed_upload_records,
            style="Compact.TButton"
        )
        self.btn_auto_process = ttk.Button(
            ctrl, text="自动处理(每1分钟): 关", command=self.toggle_auto_process, style="Compact.TButton"
        )

        self.btn_start.pack(side='left', padx=3, pady=2)
        self.btn_stop.pack(side='left', padx=3, pady=2)
        self.btn_resume.pack(side='left', padx=3, pady=2)
        self.btn_skip.pack(side='left', padx=3, pady=2)
        self.btn_maintenance.pack(side='left', padx=3, pady=2)
        self.btn_upload_images.pack(side='left', padx=3, pady=2)
        self.btn_stop_upload.pack(side='left', padx=3, pady=2)
        self.btn_open_detection_page.pack(side='left', padx=3, pady=2)
        self.btn_open_sannysoft.pack(side='left', padx=3, pady=2)
        self.btn_delete_failed_upload.pack(side='left', padx=3, pady=2)
        self.btn_auto_process.pack(side='left', padx=3, pady=2)

        self.var_status = tk.StringVar(value="就绪")
        ttk.Label(ctrl, textvariable=self.var_status).pack(side='left', padx=8)

        info = ttk.Frame(master)
        info.pack(fill='x', padx=8, pady=(4, 6))
        self.var_duration = tk.StringVar(value="已用时：00:00:00")
        self.var_progress_text = tk.StringVar(value="进度：0 / 0 (0.0%)")
        ttk.Label(info, textvariable=self.var_duration).pack(side='left', padx=4)
        ttk.Label(info, textvariable=self.var_progress_text).pack(side='left', padx=12)

        self.progress = ttk.Progressbar(master, length=860, mode='determinate', maximum=100)
        self.progress.pack(fill='x', padx=8, pady=(0, 6))

        sleep_frame = ttk.LabelFrame(master, text="等待进度（Sleep 读条）")
        sleep_frame.pack(fill='x', padx=8, pady=(0, 6))
        self.var_sleep_text = tk.StringVar(value="当前无等待")
        ttk.Label(sleep_frame, textvariable=self.var_sleep_text).pack(anchor='w', padx=6, pady=(4, 2))
        self.sleep_bar = ttk.Progressbar(sleep_frame, length=860, mode='determinate', maximum=100)
        self.sleep_bar.pack(fill='x', padx=6, pady=(0, 6))

        qr_frame = ttk.LabelFrame(master, text="登录二维码回显（Cookie 失效时）")
        qr_frame.pack(fill='x', padx=8, pady=(0, 6))
        self.var_login_qr_status = tk.StringVar(value="状态：暂未捕获二维码")
        self.var_login_qr_offset_x = tk.StringVar(value="-80")
        self.var_login_qr_offset_y = tk.StringVar(value="0")
        self.lbl_login_qr = ttk.Label(qr_frame, text="暂无二维码")
        self.lbl_login_qr.pack(side='left', padx=8, pady=6)
        qr_right = ttk.Frame(qr_frame)
        qr_right.pack(side='left', fill='x', expand=True, padx=4, pady=4)
        ttk.Label(qr_right, textvariable=self.var_login_qr_status).pack(anchor='w')
        qr_offset_row = ttk.Frame(qr_right)
        qr_offset_row.pack(anchor='w', pady=(4, 0))
        ttk.Label(qr_offset_row, text="捕获偏移 X：").pack(side='left')
        ttk.Entry(qr_offset_row, textvariable=self.var_login_qr_offset_x, width=7).pack(side='left', padx=(2, 8))
        ttk.Label(qr_offset_row, text="Y：").pack(side='left')
        ttk.Entry(qr_offset_row, textvariable=self.var_login_qr_offset_y, width=7).pack(side='left', padx=(2, 8))
        ttk.Button(
            qr_offset_row, text="应用偏移", command=self.apply_login_qr_offset, style="Compact.TButton"
        ).pack(side='left')
        ttk.Button(
            qr_offset_row, text="清空二维码", command=self.clear_login_qr_preview, style="Compact.TButton"
        ).pack(side='left', padx=(8, 0))
        self._login_qr_photo_ref = None

        logs_notebook = ttk.Notebook(master)
        logs_notebook.pack(fill='both', expand=True, padx=8, pady=(0, 8))
        crawl_log_tab = ttk.Frame(logs_notebook)
        upload_log_tab = ttk.Frame(logs_notebook)
        logs_notebook.add(crawl_log_tab, text="采集日志")
        logs_notebook.add(upload_log_tab, text="图片上传日志")

        self.txt_log = tk.Text(crawl_log_tab, height=12, state='disabled', font=("Consolas", 9))
        self.txt_log.pack(fill='both', expand=True, side='left')
        scroll = ttk.Scrollbar(crawl_log_tab, command=self.txt_log.yview)
        scroll.pack(side='right', fill='y')
        self.txt_log['yscrollcommand'] = scroll.set

        self.txt_upload_log = tk.Text(upload_log_tab, height=9, state='disabled', font=("Consolas", 9))
        self.txt_upload_log.pack(fill='both', expand=True, side='left')
        upload_scroll = ttk.Scrollbar(upload_log_tab, command=self.txt_upload_log.yview)
        upload_scroll.pack(side='right', fill='y')
        self.txt_upload_log['yscrollcommand'] = upload_scroll.set

        self.logger = logging.getLogger("XHS")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        if self.logger.handlers:
            self.logger.handlers.clear()
        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.gui_handler = TextHandler(self.txt_log, self.ui)
        self.gui_handler.setFormatter(fmt)
        self.logger.addHandler(self.gui_handler)
        file_handler = logging.FileHandler(os.path.join(get_runtime_base_dir(), 'xhs_crawler.log'), encoding='utf-8')
        file_handler.setFormatter(fmt)
        self.logger.addHandler(file_handler)

        self.upload_logger = logging.getLogger("XHS_UPLOAD")
        self.upload_logger.setLevel(logging.INFO)
        self.upload_logger.propagate = False
        if self.upload_logger.handlers:
            self.upload_logger.handlers.clear()
        self.upload_gui_handler = TextHandler(self.txt_upload_log, self.ui)
        self.upload_gui_handler.setFormatter(fmt)
        self.upload_logger.addHandler(self.upload_gui_handler)
        upload_file_handler = logging.FileHandler(os.path.join(get_runtime_base_dir(), 'xhs_upload.log'), encoding='utf-8')
        upload_file_handler.setFormatter(fmt)
        self.upload_logger.addHandler(upload_file_handler)

        self.running_thread: Optional[threading.Thread] = None
        self.maintenance_thread: Optional[threading.Thread] = None
        self.upload_thread: Optional[threading.Thread] = None
        self.crawler: Optional[XHSCrawler] = None
        self.detector_crawler: Optional[XHSCrawler] = None
        self.db: Optional[DatabaseManager] = None
        self.failed_upload_spider_ids: List[int] = []
        self.failed_upload_artist_ids: List[int] = []

        # 缓存账号列表 {name: cookie_json_list}
        self.account_map = {}
        self.current_account_name: Optional[str] = None

        self.start_ts = None
        self._tick_after_id = None
        self.total_rows = 0
        self.done_rows = 0

        self.skip_event = threading.Event()
        self.upload_stop_event = threading.Event()
        self.auto_process_enabled = False
        self.auto_process_after_id = None
        self.shutdown_warning_after_id = None
        self.shutdown_status_after_id = None
        self.shutdown_warning_at: Optional[float] = None
        self.shutdown_execute_at: Optional[float] = None
        self.shutdown_reason: Optional[str] = None
        self.shutdown_trigger: Optional[str] = None
        self.shutdown_os_armed = False

        self.master.bind_all("<Button-1>", self._blur_input_on_outside_click, add="+")
        self.master.protocol("WM_DELETE_WINDOW", self.on_close)
        self._schedule_ui_pump()

        # 初始化加载账号
        self.load_accounts()

    # ---------- UI 线程安全 ----------
    def ui(self, fn):
        try:
            if threading.current_thread() is threading.main_thread():
                fn()
            else:
                self._ui_queue.put(fn)
        except Exception:
            pass

    def _schedule_ui_pump(self):
        try:
            self._ui_pump_after_id = self.master.after(20, self._drain_ui_queue)
        except Exception:
            self._ui_pump_after_id = None

    def _drain_ui_queue(self):
        try:
            for _ in range(200):
                try:
                    fn = self._ui_queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    fn()
                except Exception:
                    pass
        finally:
            try:
                if self.master.winfo_exists():
                    self._schedule_ui_pump()
            except Exception:
                self._ui_pump_after_id = None

    def ui_set_status(self, text: str):
        self.ui(lambda: self.var_status.set(text))

    def get_login_qr_offset(self) -> Tuple[int, int]:
        try:
            ox = int(float(self.var_login_qr_offset_x.get()))
        except Exception:
            ox = -80
        try:
            oy = int(float(self.var_login_qr_offset_y.get()))
        except Exception:
            oy = 0
        ox = max(-800, min(800, ox))
        oy = max(-800, min(800, oy))
        return ox, oy

    def apply_login_qr_offset(self):
        ox, oy = self.get_login_qr_offset()
        self.var_login_qr_offset_x.set(str(ox))
        self.var_login_qr_offset_y.set(str(oy))
        self.logger.info("二维码捕获偏移已更新：offset=(%d,%d)", ox, oy)

    def clear_login_qr_preview(self):
        def _apply():
            self._login_qr_photo_ref = None
            try:
                self.lbl_login_qr.configure(image='', text="暂无二维码")
            except Exception:
                pass
            try:
                self.var_login_qr_status.set("状态：暂未捕获二维码")
            except Exception:
                pass
        self.ui(_apply)

    def on_login_qr_detected(self, payload: Dict):
        def _apply():
            if not isinstance(payload, dict):
                return
            if payload.get("cleared"):
                self._login_qr_photo_ref = None
                self.lbl_login_qr.configure(image='', text="暂无二维码")
                reason = str(payload.get("reason") or "").strip()
                if reason:
                    self.var_login_qr_status.set(f"状态：二维码已清空（{reason}）")
                else:
                    self.var_login_qr_status.set("状态：二维码已清空")
                return

            b64 = payload.get("png_b64")
            if not b64:
                return

            try:
                img = tk.PhotoImage(data=b64)
                max_w = 260
                max_h = 260
                sw = max(1, (img.width() + max_w - 1) // max_w)
                sh = max(1, (img.height() + max_h - 1) // max_h)
                scale = max(sw, sh)
                if scale > 1:
                    img = img.subsample(scale, scale)
                self._login_qr_photo_ref = img
                self.lbl_login_qr.configure(image=img, text="")
            except Exception as e:
                self.lbl_login_qr.configure(image='', text="二维码渲染失败")
                self.var_login_qr_status.set(f"状态：二维码渲染失败（{e}）")
                return

            reason = str(payload.get("reason") or "").strip()
            source = str(payload.get("source") or "").strip()
            stage = str(payload.get("stage") or "").strip()
            url = str(payload.get("url") or "").strip()
            x = payload.get("x")
            y = payload.get("y")
            w = payload.get("width")
            h = payload.get("height")
            ox = payload.get("offset_x")
            oy = payload.get("offset_y")
            rect_info = "-"
            if all(v is not None for v in (x, y, w, h)):
                rect_info = f"{x},{y},{w}x{h}"
            offset_info = "-"
            if ox is not None and oy is not None:
                offset_info = f"{ox},{oy}"
            self.var_login_qr_status.set(
                f"状态：已捕获二维码 | 阶段={stage or '-'} | 来源={source or '-'} | 偏移={offset_info} | 区域={rect_info} | 原因={reason or '-'} | 页面={url or '-'}"
            )
            self.logger.warning("已捕获登录二维码并回显到 GUI（stage=%s, source=%s, reason=%s）", stage, source, reason)

        self.ui(_apply)

    def _is_input_widget(self, widget) -> bool:
        current = widget
        while current is not None:
            try:
                if current.winfo_class() in self._input_focus_classes:
                    return True
                current = current.master
            except Exception:
                return False
        return False

    def _belongs_to_main_window(self, widget) -> bool:
        """判断控件是否属于主窗口，避免全局点击处理影响弹窗。"""
        try:
            if not widget or not widget.winfo_exists():
                return False
            return widget.winfo_toplevel() == self.master
        except Exception:
            return False

    def _blur_input_on_outside_click(self, event):
        if not self._belongs_to_main_window(getattr(event, "widget", None)):
            return
        try:
            focus_widget = self.master.focus_get()
        except (KeyError, tk.TclError):
            # ttk.Combobox 弹出层关闭瞬间可能返回已失效的 popdown，直接忽略本次点击事件
            return
        except Exception:
            return
        try:
            if focus_widget and not focus_widget.winfo_exists():
                return
        except Exception:
            return
        if focus_widget and not self._belongs_to_main_window(focus_widget):
            return
        if not focus_widget or not self._is_input_widget(focus_widget):
            return
        if self._is_input_widget(event.widget):
            return
        self.master.after_idle(self.master.focus_set)

    def ui_set_buttons(self, *, start=None, stop=None, resume=None, skip=None, add_acc=None, remove_acc=None):
        def _apply():
            try:
                if start is not None: self.btn_start.config(state=start)
                if stop is not None: self.btn_stop.config(state=stop)
                if resume is not None: self.btn_resume.config(state=resume)
                if skip is not None: self.btn_skip.config(state=skip)
                if add_acc is not None: self.btn_add_acc.config(state=add_acc)
                if remove_acc is not None: self.btn_remove_acc.config(state=remove_acc)
            except Exception:
                pass

        self.ui(_apply)

    def ui_set_upload_buttons(self, *, start_upload=None, stop_upload=None):
        def _apply():
            try:
                if start_upload is not None: self.btn_upload_images.config(state=start_upload)
                if stop_upload is not None: self.btn_stop_upload.config(state=stop_upload)
            except Exception:
                pass

        self.ui(_apply)

    def _get_detection_page_url(self) -> str:
        detection_file = Path(get_resource_base_dir()) / "scripts" / "selenium_detection_landing.html"
        if not detection_file.exists():
            raise FileNotFoundError(f"未找到检测页文件: {detection_file}")
        return detection_file.resolve().as_uri()

    def _quit_detector_browser(self):
        crawler = self.detector_crawler
        self.detector_crawler = None
        if not crawler:
            return
        try:
            if crawler.driver:
                crawler.driver.quit()
        except Exception:
            pass

    def _open_detection_target(self, *, target_url: str, page_name: str, button_attr: str):
        """使用检测浏览器打开指定检测页面。"""
        if self.running_thread and self.running_thread.is_alive():
            messagebox.showinfo("提示", "采集运行中，请先停止采集后再访问检测页，避免打断当前任务。")
            return

        button = getattr(self, button_attr, None)
        if button is not None:
            self.ui(lambda: button.config(state='disabled'))
        self.ui_set_status(f"正在打开{page_name}...")

        def _run():
            try:
                # 优先复用已存在的检测浏览器，避免重复启动 driver。
                if self.detector_crawler and self.detector_crawler.driver:
                    try:
                        self.detector_crawler.driver.get(target_url)
                        self.logger.info("已在检测浏览器中打开%s：%s", page_name, target_url)
                        self.ui_set_status(f"{page_name}已打开")
                        return
                    except Exception:
                        self._quit_detector_browser()

                account_name = (self.cb_account.get() or "检测页").strip() or "检测页"
                detector_account = f"{account_name}_detect"
                self.detector_crawler = XHSCrawler(
                    target_type="detect",
                    account_name=detector_account,
                    headless=False,
                    browser=self.var_browser.get(),
                    logger=self.logger,
                    get_login_qr_offset=self.get_login_qr_offset,
                    on_login_qr_detected=self.on_login_qr_detected
                )
                self.detector_crawler.driver.get(target_url)
                self.logger.info("已打开%s：%s", page_name, target_url)
                self.ui_set_status(f"{page_name}已打开")
            except Exception as e:
                self.logger.error(f"打开{page_name}失败: {e}")
                self.ui(lambda: messagebox.showerror("错误", f"打开{page_name}失败: {e}"))
                self.ui_set_status(f"打开{page_name}失败")
            finally:
                if button is not None:
                    self.ui(lambda: button.config(state='normal'))

        threading.Thread(target=_run, daemon=True).start()

    def open_detection_page(self):
        """打开本地 Selenium 检测页，快速查看当前浏览器的自动化特征。"""
        try:
            url = self._get_detection_page_url()
        except Exception as e:
            self.logger.error(f"检测页文件不可用: {e}")
            messagebox.showerror("错误", f"检测页文件不可用: {e}")
            return
        self._open_detection_target(
            target_url=url,
            page_name="检测页",
            button_attr="btn_open_detection_page"
        )

    def open_sannysoft_detection_page(self):
        """打开 SannySoft 浏览器指纹检测页。"""
        self._open_detection_target(
            target_url="https://bot.sannysoft.com/",
            page_name="SannySoft",
            button_attr="btn_open_sannysoft"
        )

    def _set_auto_process_button_text(self):
        text = "自动处理(每1分钟): 开" if self.auto_process_enabled else "自动处理(每1分钟): 关"
        self.ui(lambda: self.btn_auto_process.config(text=text))

    def _schedule_auto_process(self, delay_ms: int = 60_000):
        if not self.auto_process_enabled:
            return
        if self.auto_process_after_id is not None:
            try:
                self.master.after_cancel(self.auto_process_after_id)
            except Exception:
                pass
            self.auto_process_after_id = None
        self.auto_process_after_id = self.master.after(delay_ms, self._auto_process_tick)

    def toggle_auto_process(self):
        self.auto_process_enabled = not self.auto_process_enabled
        self._set_auto_process_button_text()
        if self.auto_process_enabled:
            self.upload_logger.info("自动处理已开启：每60秒执行一次数据维护 + 上传图片")
            self._schedule_auto_process(1000)
        else:
            if self.auto_process_after_id is not None:
                try:
                    self.master.after_cancel(self.auto_process_after_id)
                except Exception:
                    pass
                self.auto_process_after_id = None
            self.upload_logger.info("自动处理已关闭")

    def _auto_process_tick(self):
        self.auto_process_after_id = None
        if not self.auto_process_enabled:
            return

        if (self.upload_thread and self.upload_thread.is_alive()) or (
                self.maintenance_thread and self.maintenance_thread.is_alive()):
            self.upload_logger.info("自动处理跳过：已有任务运行中（上传或维护）")
            self._schedule_auto_process(60_000)
            return

        self.upload_logger.info("自动处理触发：执行数据维护，完成后执行上传图片")
        self.run_data_maintenance(show_dialog=False, source="auto", on_complete=self._auto_after_maintenance)
        self._schedule_auto_process(60_000)

    def _auto_after_maintenance(self, success: bool):
        if not self.auto_process_enabled:
            return
        if not success:
            self.upload_logger.warning("自动处理：数据维护失败，本轮不执行上传")
            return
        if self.upload_thread and self.upload_thread.is_alive():
            self.upload_logger.info("自动处理：上传任务仍在进行，跳过本轮上传")
            return
        self.run_spider_image_upload(show_dialog=False, source="auto")

    def ui_update_progress(self):
        def _apply():
            percent = (self.done_rows / self.total_rows * 100.0) if self.total_rows else 0.0
            self.var_progress_text.set(f"进度：{self.done_rows} / {self.total_rows} ({percent:.1f}%)")
            self.progress['value'] = percent

        self.ui(_apply)

    # ---------- 账号管理 ----------
    def _is_valid_account_name(self, account_name: str) -> bool:
        return bool(account_name and account_name in self.account_map)

    def _default_account_settings(self) -> Dict:
        return {
            "scroll_sleep": "10.5",
            "detail_sleep": "5.0",
            "max_scroll": "20",
            "browser": "chrome",
            "headless": False,
            "background_mode": True,
            "target_type": TargetType.BRAND,
            "shutdown_after_finish": False,
            "shutdown_timer_minutes": "",
        }

    def _normalize_account_settings(self, settings: Optional[Dict]) -> Dict:
        normalized = self._default_account_settings()
        if isinstance(settings, dict):
            for key in normalized.keys():
                if key in settings and settings.get(key) is not None:
                    normalized[key] = settings.get(key)

        normalized["scroll_sleep"] = str(normalized.get("scroll_sleep") or "10.5")
        normalized["detail_sleep"] = str(normalized.get("detail_sleep") or "5.0")
        normalized["max_scroll"] = str(normalized.get("max_scroll") or "20")

        browser = str(normalized.get("browser") or "chrome").strip().lower()
        normalized["browser"] = browser if browser in ("chrome", "edge") else "chrome"

        target_type = str(normalized.get("target_type") or TargetType.BRAND).strip()
        normalized["target_type"] = target_type if target_type in (TargetType.BRAND, TargetType.ARTIST) else TargetType.BRAND

        headless = normalized.get("headless")
        if isinstance(headless, str):
            normalized["headless"] = headless.strip().lower() in ("1", "true", "yes", "on")
        else:
            normalized["headless"] = bool(headless)

        background_mode = normalized.get("background_mode")
        if isinstance(background_mode, str):
            normalized["background_mode"] = background_mode.strip().lower() in ("1", "true", "yes", "on")
        else:
            normalized["background_mode"] = bool(background_mode)

        shutdown_after_finish = normalized.get("shutdown_after_finish")
        if isinstance(shutdown_after_finish, str):
            normalized["shutdown_after_finish"] = shutdown_after_finish.strip().lower() in ("1", "true", "yes", "on")
        else:
            normalized["shutdown_after_finish"] = bool(shutdown_after_finish)

        normalized["shutdown_timer_minutes"] = str(normalized.get("shutdown_timer_minutes") or "").strip()
        return normalized

    def _collect_current_account_settings(self) -> Dict:
        return self._normalize_account_settings({
            "scroll_sleep": (self.var_scroll_sleep.get() or "").strip() or "10.5",
            "detail_sleep": (self.var_detail_sleep.get() or "").strip() or "5.0",
            "max_scroll": (self.var_max_scroll.get() or "").strip() or "20",
            "browser": (self.var_browser.get() or "").strip() or "chrome",
            "headless": bool(self.var_headless.get()),
            "background_mode": bool(self.var_background_mode.get()),
            "target_type": (self.var_target_type.get() or "").strip() or TargetType.BRAND,
            "shutdown_after_finish": bool(self.var_shutdown_after_finish.get()),
            "shutdown_timer_minutes": (self.var_shutdown_timer_minutes.get() or "").strip(),
        })

    def _apply_account_settings(self, settings: Optional[Dict]):
        settings = self._normalize_account_settings(settings)
        self.var_scroll_sleep.set(str(settings.get("scroll_sleep") or "10.5"))
        self.var_detail_sleep.set(str(settings.get("detail_sleep") or "5.0"))
        self.var_max_scroll.set(str(settings.get("max_scroll") or "20"))
        self.var_browser.set(str(settings.get("browser") or "chrome"))
        self.var_headless.set(bool(settings.get("headless")))
        self.var_background_mode.set(bool(settings.get("background_mode")))
        self.var_shutdown_after_finish.set(bool(settings.get("shutdown_after_finish")))
        self.var_shutdown_timer_minutes.set(str(settings.get("shutdown_timer_minutes") or "").strip())
        target_type = str(settings.get("target_type") or TargetType.BRAND)
        if target_type not in (TargetType.BRAND, TargetType.ARTIST):
            target_type = TargetType.BRAND
        self.var_target_type.set(target_type)
        self.on_target_type_change(target_type)

    def _run_shutdown_command(self, *args: str) -> Tuple[bool, str]:
        if not sys.platform.startswith("win"):
            return False, "当前仅支持 Windows 系统自动关机"
        try:
            kwargs = {
                "capture_output": True,
                "text": True,
                "errors": "ignore",
                "check": False,
            }
            if hasattr(subprocess, "CREATE_NO_WINDOW"):
                kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
            completed = subprocess.run(["shutdown", *args], **kwargs)
            output = (completed.stdout or completed.stderr or "").strip()
            return completed.returncode == 0, output
        except Exception as e:
            return False, str(e)

    def _set_shutdown_status(self, text: str, *, cancel_enabled: bool):
        def _apply():
            self.var_shutdown_status.set(text)
            try:
                self.btn_cancel_shutdown.config(state='normal' if cancel_enabled else 'disabled')
            except Exception:
                pass

        self.ui(_apply)

    def _stop_shutdown_status_timer(self):
        if self.shutdown_status_after_id is not None:
            try:
                self.master.after_cancel(self.shutdown_status_after_id)
            except Exception:
                pass
            self.shutdown_status_after_id = None

    def _refresh_shutdown_status(self):
        self.shutdown_status_after_id = None
        if self.shutdown_execute_at is None:
            self._set_shutdown_status("关机计划：未设置", cancel_enabled=False)
            return

        remaining = max(0, int(round(self.shutdown_execute_at - time.time())))
        if not self.shutdown_os_armed and remaining > 60:
            reminder_after = remaining - 60
            text = (
                f"关机计划：{reminder_after // 60}分{reminder_after % 60}秒后提醒，"
                f"{remaining // 60}分{remaining % 60}秒后关机"
            )
        else:
            text = f"关机计划：{remaining // 60}分{remaining % 60}秒后自动关机"
        self._set_shutdown_status(text, cancel_enabled=True)

        if remaining > 0:
            self.shutdown_status_after_id = self.master.after(1000, self._refresh_shutdown_status)

    def _clear_shutdown_plan(self, *, cancel_os_shutdown: bool):
        if self.shutdown_warning_after_id is not None:
            try:
                self.master.after_cancel(self.shutdown_warning_after_id)
            except Exception:
                pass
            self.shutdown_warning_after_id = None

        self._stop_shutdown_status_timer()

        if cancel_os_shutdown and self.shutdown_os_armed:
            ok, output = self._run_shutdown_command("/a")
            if not ok and output:
                self.logger.warning(f"取消系统关机失败: {output}")

        self.shutdown_warning_at = None
        self.shutdown_execute_at = None
        self.shutdown_reason = None
        self.shutdown_trigger = None
        self.shutdown_os_armed = False
        self._set_shutdown_status("关机计划：未设置", cancel_enabled=False)

    def _arm_system_shutdown(self, reason: str) -> Tuple[bool, str]:
        if self.shutdown_os_armed:
            self._run_shutdown_command("/a")
        ok, output = self._run_shutdown_command("/s", "/t", "60", "/c", reason)
        if ok:
            self.shutdown_os_armed = True
        return ok, output

    def _trigger_shutdown_warning(self):
        self.shutdown_warning_after_id = None
        if self.shutdown_execute_at is None:
            return

        trigger_name = self.shutdown_trigger or "自动关机"
        reason = self.shutdown_reason or f"{trigger_name}，系统将在 1 分钟后自动关机。"
        ok, output = self._arm_system_shutdown(reason)
        if not ok:
            self.logger.error(f"设置自动关机失败: {output}")
            self._clear_shutdown_plan(cancel_os_shutdown=False)
            self.ui(lambda: messagebox.showerror("错误", f"设置自动关机失败: {output or '未知错误'}"))
            return

        notice = f"{trigger_name}，系统将在 1 分钟后自动关机，请及时保存其它工作。"
        self.logger.warning(notice)
        self._refresh_shutdown_status()
        self.ui(lambda: messagebox.showwarning("自动关机提醒", notice))

    def _schedule_shutdown_plan(self, delay_seconds: int, trigger_name: str, *, replace_only_if_earlier: bool = False) -> bool:
        delay_seconds = max(60, int(delay_seconds))
        new_execute_at = time.time() + delay_seconds

        if replace_only_if_earlier and self.shutdown_execute_at is not None and self.shutdown_execute_at <= new_execute_at:
            remaining = max(0, int(round(self.shutdown_execute_at - time.time())))
            self.logger.info(
                "已有更早的关机计划，跳过 [%s]（剩余 %d 秒）",
                trigger_name, remaining
            )
            return False

        self._clear_shutdown_plan(cancel_os_shutdown=True)

        self.shutdown_trigger = trigger_name
        self.shutdown_reason = f"{trigger_name}，系统将在 1 分钟后自动关机。"
        self.shutdown_execute_at = new_execute_at
        self.shutdown_warning_at = new_execute_at - 60

        wait_ms = max(0, int(round((self.shutdown_warning_at - time.time()) * 1000)))
        self.shutdown_warning_after_id = self.master.after(wait_ms, self._trigger_shutdown_warning)
        self._refresh_shutdown_status()

        self.logger.info(
            "已设置自动关机计划 [%s]，将在 %d 分 %d 秒后关机",
            trigger_name, delay_seconds // 60, delay_seconds % 60
        )
        return True

    def schedule_timed_shutdown(self):
        raw_minutes = (self.var_shutdown_timer_minutes.get() or "").strip()
        if not raw_minutes:
            messagebox.showwarning("提示", "请输入定时关机的分钟数（至少 1 分钟）")
            return

        try:
            minutes = int(raw_minutes)
        except ValueError:
            messagebox.showerror("错误", "定时关机只支持整数分钟")
            return

        if minutes < 1:
            messagebox.showwarning("提示", "定时关机至少需要 1 分钟")
            return

        if not self._schedule_shutdown_plan(minutes * 60, f"定时关机（{minutes} 分钟）"):
            return

        self.save_account_settings(log_errors=False)
        if minutes > 1:
            messagebox.showinfo(
                "成功",
                f"已设置定时关机：{minutes} 分钟后关机。\n系统会在关机前 1 分钟弹出提醒。"
            )

    def cancel_scheduled_shutdown(self, show_dialog: bool = True):
        has_plan = self.shutdown_execute_at is not None or self.shutdown_os_armed
        if not has_plan:
            if show_dialog:
                messagebox.showinfo("提示", "当前没有关机计划")
            return

        self._clear_shutdown_plan(cancel_os_shutdown=True)
        self.save_account_settings(log_errors=False)
        self.logger.info("已取消自动关机计划")
        if show_dialog:
            messagebox.showinfo("成功", "已取消自动关机计划")

    def _schedule_shutdown_after_completion(self):
        if not bool(self.var_shutdown_after_finish.get()):
            return

        scheduled = self._schedule_shutdown_plan(60, "抓取完毕后自动关机", replace_only_if_earlier=True)
        if scheduled:
            self.logger.info("抓取已完成，已进入 1 分钟关机倒计时...")

    def save_account_settings(self, account_name: Optional[str] = None, *, log_errors: bool = True):
        target_account = (account_name or self.current_account_name or self.cb_account.get() or "").strip()
        if not self._is_valid_account_name(target_account):
            return
        db = None
        try:
            db = DatabaseManager()
            db.upsert_account_settings(target_account, self._collect_current_account_settings())
        except Exception as e:
            if log_errors:
                self.logger.warning(f"保存账号参数失败 [{target_account}]: {e}")
        finally:
            try:
                if db and db.connection:
                    db.connection.close()
            except Exception:
                pass

    def load_account_settings(self, account_name: str):
        target_account = (account_name or "").strip()
        if not self._is_valid_account_name(target_account):
            return
        db = None
        try:
            db = DatabaseManager()
            settings = db.fetch_account_settings(target_account)
        except Exception as e:
            self.logger.warning(f"加载账号参数失败 [{target_account}]: {e}")
            settings = None
        finally:
            try:
                if db and db.connection:
                    db.connection.close()
            except Exception:
                pass

        self._apply_account_settings(settings)

    def on_account_selected(self, _event=None):
        selected_account = (self.cb_account.get() or "").strip()
        previous_account = (self.current_account_name or "").strip()
        if previous_account and previous_account != selected_account:
            self.save_account_settings(previous_account)
        self.current_account_name = selected_account if self._is_valid_account_name(selected_account) else None
        if self.current_account_name:
            self.load_account_settings(self.current_account_name)

    def load_accounts(self):
        """加载数据库中的账号列表"""

        def _load():
            try:
                db = DatabaseManager()
                rows = db.fetch_cookies()
                db.connection.close()

                self.account_map.clear()
                cb_values = []
                for row in rows:
                    name = row['account_name']
                    try:
                        data = json.loads(row['cookie_data'])
                        self.account_map[name] = data
                        cb_values.append(name)
                    except Exception:
                        pass

                def _ui_update():
                    self.cb_account['values'] = cb_values
                    if cb_values:
                        selected_account = (
                            self.current_account_name
                            if self.current_account_name in cb_values
                            else cb_values[0]
                        )
                        self.cb_account.set(selected_account)
                        self.current_account_name = selected_account
                        self.load_account_settings(selected_account)
                        self.btn_remove_acc.config(state='normal')
                    else:
                        self.cb_account.set('暂无账号，请新增')
                        self.current_account_name = None
                        self.btn_remove_acc.config(state='disabled')

                self.ui(_ui_update)
            except Exception as e:
                self.logger.error(f"加载账号列表失败: {e}")

        threading.Thread(target=_load, daemon=True).start()

    def _purge_account_profile_dirs(self, account_name: str) -> int:
        """清理账号本地浏览器会话目录，确保新增账号时进入扫码流程。"""
        profile_key = _safe_profile_key(account_name)
        legacy_key = _legacy_profile_key(account_name)
        profiles_root = os.path.join(get_runtime_base_dir(), "xhs_browser_profiles")

        removed = 0
        targets = [
            os.path.join(profiles_root, "_runtime", profile_key),
            os.path.join(profiles_root, profile_key),
            os.path.join(profiles_root, legacy_key),
            os.path.join(profiles_root, profile_key, "runtime_profile"),
        ]
        for p in targets:
            if not os.path.isdir(p):
                continue
            try:
                shutil.rmtree(p)
                removed += 1
            except Exception as e:
                self.logger.warning(f"清理账号目录失败: {p} ({e})")

        tmp_root = os.path.join(profiles_root, "_tmp")
        if os.path.isdir(tmp_root):
            prefix = f"{profile_key}_"
            try:
                for name in os.listdir(tmp_root):
                    if not (name == profile_key or name.startswith(prefix)):
                        continue
                    p = os.path.join(tmp_root, name)
                    if not os.path.isdir(p):
                        continue
                    try:
                        shutil.rmtree(p)
                        removed += 1
                    except Exception as e:
                        self.logger.warning(f"清理账号临时目录失败: {p} ({e})")
            except Exception as e:
                self.logger.warning(f"扫描账号临时目录失败: {e}")
        return removed

    def add_new_account(self):
        """新增账号流程：打开浏览器 -> 扫码 -> 输入名称 -> 保存"""
        if self.running_thread and self.running_thread.is_alive():
            messagebox.showinfo("提示", "采集进行中，请先停止")
            return

        account_name = simpledialog.askstring(
            "新增账号",
            "请输入该账号的备注名称（用于保存独立浏览器会话）:",
            parent=self.master
        )
        if account_name is None:
            return
        account_name = account_name.strip()
        if not account_name:
            messagebox.showwarning("提示", "账号名称不能为空")
            return
        if account_name in self.account_map:
            overwrite = messagebox.askyesno("提示", f"账号 [{account_name}] 已存在，是否覆盖其登录信息？")
            if not overwrite:
                return

        self.ui_set_buttons(start='disabled', add_acc='disabled', remove_acc='disabled')
        self.ui_set_status("正在启动浏览器录入账号...")
        self.clear_login_qr_preview()
        self.logger.info("开始新增账号流程：%s", account_name)

        def _run_add():
            temp_crawler = None
            try:
                purged = self._purge_account_profile_dirs(account_name)
                if purged > 0:
                    self.logger.info(f"新增账号前已清理历史会话目录: {purged} 个")

                temp_crawler = XHSCrawler(
                    target_type="temp",
                    account_name=account_name,
                    logger=self.logger,
                    headless=False,
                    browser=self.var_browser.get(),
                    get_login_qr_offset=self.get_login_qr_offset,
                    on_login_qr_detected=self.on_login_qr_detected
                )
                self.logger.info(f"浏览器已启动（账号: {account_name}），请在弹出的浏览器中扫码登录...")

                # 调用 login 不传 cookie，触发扫码逻辑
                new_cookies = temp_crawler.login(cookie_list=None)

                if new_cookies:
                    db = None
                    try:
                        db = DatabaseManager()
                        db.upsert_xhs_cookie(account_name, new_cookies)
                        self.logger.info(f"账号 [{account_name}] 登录信息已保存（cookies={len(new_cookies)}）")

                        def _on_saved():
                            messagebox.showinfo("成功", f"账号 [{account_name}] 已保存！")
                            self.load_accounts()
                            self.ui_set_buttons(start='normal', add_acc='normal', remove_acc='normal')
                            self.ui_set_status("就绪")

                        self.ui(_on_saved)
                    except Exception as db_e:
                        self.logger.error(f"数据库保存失败: {db_e}")
                        self.ui(lambda: messagebox.showerror("错误", f"数据库保存失败: {db_e}"))
                        self.ui_set_buttons(start='normal', add_acc='normal', remove_acc='normal')
                        self.ui_set_status("录入失败")
                    finally:
                        try:
                            if db and db.connection:
                                db.connection.close()
                        except Exception:
                            pass
                else:
                    self.logger.warning(f"账号 [{account_name}] 未获取到有效登录信息，未保存。")
                    self.ui_set_buttons(start='normal', add_acc='normal', remove_acc='normal')
                    self.ui_set_status("录入失败")

            except Exception as e:
                self.logger.error(f"录入账号异常: {e}")
                self.ui_set_buttons(start='normal', add_acc='normal', remove_acc='normal')
                self.ui_set_status("录入失败")
            finally:
                if temp_crawler and temp_crawler.driver:
                    temp_crawler.driver.quit()

        threading.Thread(target=_run_add, daemon=True).start()

    def remove_account(self):
        """移除当前选中账号：删除数据库 Cookie，并清理本地会话目录"""
        if self.running_thread and self.running_thread.is_alive():
            messagebox.showinfo("提示", "采集进行中，请先停止")
            return

        selected_acc_name = (self.cb_account.get() or "").strip()
        if not selected_acc_name or selected_acc_name not in self.account_map:
            messagebox.showwarning("提示", "请先选择一个有效账号")
            return

        confirm = messagebox.askyesno(
            "确认移除账号",
            f"确认移除账号 [{selected_acc_name}]？\n\n"
            "将删除数据库中的登录信息，并清理本地浏览器会话目录。"
        )
        if not confirm:
            return

        self.ui_set_buttons(add_acc='disabled', remove_acc='disabled')
        self.ui_set_status("正在移除账号...")

        def _run_remove():
            db = None
            try:
                db = DatabaseManager()
                affected = db.delete_xhs_cookie(selected_acc_name)
                db.delete_account_settings(selected_acc_name)

                profile_key = _safe_profile_key(selected_acc_name)
                legacy_key = _legacy_profile_key(selected_acc_name)
                profiles_root = os.path.join(get_runtime_base_dir(), "xhs_browser_profiles")
                candidate_dirs = [
                    os.path.join(profiles_root, profile_key),
                    os.path.join(profiles_root, legacy_key),
                    os.path.join(profiles_root, "_runtime", profile_key),
                ]

                removed_dirs = []
                for p in candidate_dirs:
                    if os.path.isdir(p):
                        try:
                            shutil.rmtree(p)
                            removed_dirs.append(p)
                        except Exception as clean_err:
                            self.logger.warning(f"清理账号目录失败: {p} ({clean_err})")

                tmp_root = os.path.join(profiles_root, "_tmp")
                if os.path.isdir(tmp_root):
                    prefix = f"{profile_key}_"
                    try:
                        for name in os.listdir(tmp_root):
                            if not (name == profile_key or name.startswith(prefix)):
                                continue
                            p = os.path.join(tmp_root, name)
                            if not os.path.isdir(p):
                                continue
                            try:
                                shutil.rmtree(p)
                                removed_dirs.append(p)
                            except Exception as clean_err:
                                self.logger.warning(f"清理账号临时目录失败: {p} ({clean_err})")
                    except Exception as list_err:
                        self.logger.warning(f"扫描账号临时目录失败: {list_err}")

                self.logger.info(
                    f"账号 [{selected_acc_name}] 已移除（db={affected} 行, profile={len(removed_dirs)} 个目录）")

                def _on_success():
                    messagebox.showinfo(
                        "成功",
                        f"账号 [{selected_acc_name}] 已移除。\n"
                        f"数据库删除: {affected} 行\n"
                        f"本地目录清理: {len(removed_dirs)} 个"
                    )
                    self.load_accounts()
                    self.ui_set_status("就绪")

                self.ui(_on_success)
            except Exception as e:
                self.logger.error(f"移除账号失败: {e}")
                self.ui(lambda: messagebox.showerror("错误", f"移除账号失败: {e}"))
                self.ui_set_status("移除失败")
            finally:
                try:
                    if db and db.connection:
                        db.connection.close()
                except Exception:
                    pass
                if self.running_thread and self.running_thread.is_alive():
                    self.ui_set_buttons(add_acc='disabled', remove_acc='disabled')
                else:
                    self.ui_set_buttons(add_acc='normal', remove_acc='normal')

        threading.Thread(target=_run_remove, daemon=True).start()

    # ---------- 新增：数据维护功能 ----------
    def run_data_maintenance(self, *, show_dialog: bool = True, source: str = "manual",
                             on_complete: Optional[Callable[[bool], None]] = None):
        """执行 SQL 数据维护（更新品牌状态和处理旧日志）"""
        if self.maintenance_thread and self.maintenance_thread.is_alive():
            if show_dialog:
                messagebox.showinfo("提示", "数据维护任务已在运行中")
            self.logger.info("数据维护跳过：已有任务运行中（source=%s）", source)
            if on_complete:
                on_complete(False)
            return

        def _run():
            success = False
            self.ui_set_status("正在执行数据维护...")
            self.ui(lambda: self.btn_maintenance.config(state='disabled'))
            db = None
            try:
                db = DatabaseManager()
                self.logger.info("开始执行数据维护 SQL... (source=%s)", source)

                sql_update_brand = """
                    UPDATE brand 
                    SET rednote_spd_setting = 1 
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT b.id
                            FROM brand b
                            WHERE b.is_delete = 0
                              AND (SELECT COUNT(*) FROM spider_log WHERE brand_id = b.id) >= 10
                        ) AS temp
                    );
                """
                _, count_brand = db._exec(sql_update_brand)
                self.logger.info(f"已重置品牌 rednote_spd_setting=1，影响行数：{count_brand}")

                sql_update_log = """
                    UPDATE spider_log 
                    SET msg_type = 4, `status` = 2 
                    WHERE msg_type = 0 
                      AND `status` = 0 
                      AND (auth_time = 0 OR auth_time <= UNIX_TIMESTAMP(NOW() - INTERVAL 3 DAY));
                """
                _, count_log = db._exec(sql_update_log)
                self.logger.info(f"已清理旧 spider_log (msg_type=4, status=2)，影响行数：{count_log}")

                success = True
                result_msg = f"维护完成！\n\n重置品牌数: {count_brand}\n清理日志数: {count_log}"
                if show_dialog:
                    self.ui(lambda: messagebox.showinfo("成功", result_msg))
                else:
                    self.upload_logger.info("自动维护完成：重置品牌=%d, 清理日志=%d", count_brand, count_log)

            except Exception as e:
                self.logger.error(f"维护执行失败: {e}")
                if show_dialog:
                    self.ui(lambda: messagebox.showerror("错误", f"维护失败: {e}"))
                else:
                    self.upload_logger.error("自动维护失败: %s", e)
            finally:
                try:
                    if db:
                        db.connection.close()
                except Exception:
                    pass
                self.ui(lambda: self.btn_maintenance.config(state='normal'))
                if self.running_thread and self.running_thread.is_alive():
                    self.ui_set_status("运行中...")
                else:
                    self.ui_set_status("就绪")
                if on_complete:
                    try:
                        on_complete(success)
                    except Exception:
                        pass

        self.maintenance_thread = threading.Thread(target=_run, daemon=True)
        self.maintenance_thread.start()

    # ---------- 新增：上传图片功能（Python 版 spider_image.go） ----------
    def run_spider_image_upload(self, *, show_dialog: bool = True, source: str = "manual"):
        if self.upload_thread and self.upload_thread.is_alive():
            if show_dialog:
                messagebox.showinfo("提示", "图片上传任务已在运行中")
            else:
                self.upload_logger.info("上传跳过：图片上传任务已在运行中（source=%s）", source)
            return

        self.upload_stop_event.clear()

        def _run():
            self.ui_set_status("正在执行图片上传...")
            self.ui_set_upload_buttons(start_upload='disabled', stop_upload='normal')
            db = None
            try:
                db = DatabaseManager()
                uploader = SpiderImageUploader(db=db, logger=self.upload_logger, stop_event=self.upload_stop_event)
                summary = uploader.run()
                self.failed_upload_spider_ids = list(summary.get("spider_failed_log_ids", []))
                self.failed_upload_artist_ids = list(summary.get("artist_failed_log_ids", []))

                stopped_text = "（任务已手动停止）\n\n" if summary.get("stopped") else ""
                result_msg = (
                    f"图片上传执行完成！\n\n{stopped_text}"
                    f"spider_log: {summary['spider_updated']} / {summary['spider_total']}"
                    f"（成功图 {summary['spider_image_success']} / 失败图 {summary['spider_image_failed']}）\n"
                    f"artist_spider_log: {summary['artist_updated']} / {summary['artist_total']}"
                    f"（成功图 {summary['artist_image_success']} / 失败图 {summary['artist_image_failed']}）\n"
                    f"传图失败记录: spider {summary['spider_failed_log_count']} 条，artist {summary['artist_failed_log_count']} 条\n"
                    f"运行异常数: {summary['errors']}"
                )
                if show_dialog:
                    self.ui(lambda: messagebox.showinfo("完成", result_msg))
                else:
                    self.upload_logger.info(
                        "自动上传完成：spider=%d/%d(成功图=%d,失败图=%d), artist=%d/%d(成功图=%d,失败图=%d), 失败记录 spider=%d artist=%d, errors=%d",
                        summary['spider_updated'], summary['spider_total'],
                        summary['spider_image_success'], summary['spider_image_failed'],
                        summary['artist_updated'], summary['artist_total'],
                        summary['artist_image_success'], summary['artist_image_failed'],
                        summary['spider_failed_log_count'], summary['artist_failed_log_count'],
                        summary['errors']
                    )
            except Exception as e:
                self.upload_logger.exception(f"图片上传执行失败: {e}")
                if show_dialog:
                    self.ui(lambda: messagebox.showerror("错误", f"图片上传失败: {e}"))
            finally:
                try:
                    if db:
                        db.connection.close()
                except Exception:
                    pass
                self.ui_set_upload_buttons(start_upload='normal', stop_upload='disabled')
                if self.running_thread and self.running_thread.is_alive():
                    self.ui_set_status("运行中...")
                else:
                    self.ui_set_status("就绪")

        self.upload_thread = threading.Thread(target=_run, daemon=True)
        self.upload_thread.start()

    def stop_spider_image_upload(self):
        if self.upload_thread and self.upload_thread.is_alive():
            self.upload_stop_event.set()
            self.upload_logger.warning("已请求停止图片上传任务，当前处理中的图片完成后会终止。")
            self.ui_set_status("上传停止中...")
            self.ui_set_upload_buttons(stop_upload='disabled')
        else:
            messagebox.showinfo("提示", "当前没有运行中的图片上传任务")

    def delete_failed_upload_records(self):
        if self.upload_thread and self.upload_thread.is_alive():
            messagebox.showwarning("提示", "请先停止上传任务后再删除失败记录")
            return

        spider_ids = list(self.failed_upload_spider_ids)
        artist_ids = list(self.failed_upload_artist_ids)
        if not spider_ids and not artist_ids:
            messagebox.showinfo("提示", "当前没有传图失败记录可删除")
            return

        if not messagebox.askyesno(
                "确认删除",
                f"确认删除传图失败记录？\n\nspider_log: {len(spider_ids)} 条\nartist_spider_log: {len(artist_ids)} 条"
        ):
            return

        def _run():
            db = None
            try:
                db = DatabaseManager()
                spider_affected = db.mark_spider_logs_deleted(spider_ids)
                artist_affected = db.soft_delete_artist_logs(artist_ids)
                self.upload_logger.info(
                    "删除传图失败记录完成：spider_log=%d/%d, artist_spider_log=%d/%d",
                    spider_affected, len(spider_ids), artist_affected, len(artist_ids)
                )
                self.failed_upload_spider_ids = []
                self.failed_upload_artist_ids = []
                self.ui(lambda: messagebox.showinfo(
                    "完成",
                    f"删除完成！\n\nspider_log: {spider_affected} 条\nartist_spider_log: {artist_affected} 条"
                ))
            except Exception as e:
                self.upload_logger.exception(f"删除传图失败记录失败: {e}")
                self.ui(lambda: messagebox.showerror("错误", f"删除失败: {e}"))
            finally:
                try:
                    if db:
                        db.connection.close()
                except Exception:
                    pass

        threading.Thread(target=_run, daemon=True).start()

    # ---------- 采集对象切换 ----------
    def on_target_type_change(self, target: str):
        def _apply():
            if target == TargetType.ARTIST:
                self.var_max_scroll.set("0")
                self.logger.info("切换到【艺术家】采集：默认最大滚动次数已设置为 0（仅采集首屏）")
            else:
                if self.var_max_scroll.get().strip() == "0":
                    self.var_max_scroll.set("20")
                self.logger.info("切换到【品牌】采集：最大滚动次数保持/恢复为常用默认（20）")
            if self.current_account_name and self._is_valid_account_name(self.current_account_name):
                self.save_account_settings(self.current_account_name)

        self.ui(_apply)

    # ---------- 动态读取 ----------
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

    # ---------- sleep 回调 ----------
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

        self.ui(_update)

    def _clear_sleep_bar(self):
        self.var_sleep_text.set("当前无等待")
        self.sleep_bar['value'] = 0

    # ---------- 验证码回调 ----------
    def on_captcha_detected(self, where: str, captcha_type: str = "unknown"):
        """当爬虫线程检测到验证码时调用此方法"""
        def _apply():
            # 暂停完全由 pause_event 控制，不再改写用户输入的等待参数，
            # 避免恢复后界面残留 99999999 或被错误保存到账号配置中。
            self.btn_resume.config(state='normal')
            self.var_status.set("已暂停：等待验证码处理后恢复运行")
            self.var_sleep_text.set("已暂停：等待扫码/验证码处理")
            self.sleep_bar['value'] = 0
            if self._captcha_prompt_active:
                return
            self._captcha_prompt_active = True

            log_msg = (
                f"检测到验证码页面({captcha_type}, {where})，采集已暂停。\n"
                f"请在浏览器中完成扫码/风控验证后，点击“恢复运行”。"
            )
            self.logger.warning(log_msg)

            # 新增：弹出模态对话框提醒用户 (兼容 Mac/Windows)
            messagebox.showwarning(
                "验证码提醒",
                f"检测到小红书验证码拦截！\n\n位置: {where}\n类型: {captcha_type}\n\n请前往浏览器手动完成验证，\n完成后点击 GUI 上的【恢复运行】按钮。"
            )

        self.ui(_apply)

    # ---------- 计时 ----------
    def _tick(self):
        if self.start_ts is not None:
            elapsed = int(time.time() - self.start_ts)
            hh = elapsed // 3600
            mm = (elapsed % 3600) // 60
            ss = elapsed % 60
            self.var_duration.set(f"已用时：{hh:02d}:{mm:02d}:{ss:02d}")
        self._tick_after_id = self.master.after(1000, self._tick)

    # ---------- 控制 ----------
    def start(self):
        if self.running_thread and self.running_thread.is_alive():
            messagebox.showinfo("提示", "采集已在进行中")
            return

        selected_acc_name = self.cb_account.get()
        if not selected_acc_name or selected_acc_name not in self.account_map:
            messagebox.showwarning("提示", "请先选择一个有效的登录账号（或点击新增录入）")
            return

        self.current_account_name = selected_acc_name
        selected_cookies = self.account_map[selected_acc_name]
        self.clear_login_qr_preview()

        try:
            max_scroll = int(self.var_max_scroll.get())
        except ValueError:
            messagebox.showerror("错误", "最大滚动次数需为整数")
            return

        self.save_account_settings(selected_acc_name)

        self.btn_start.config(state='disabled')
        self.btn_add_acc.config(state='disabled')  # 运行时不可新增
        self.btn_remove_acc.config(state='disabled')
        self.btn_stop.config(state='normal')
        self.btn_resume.config(state='disabled')
        self.btn_skip.config(state='normal')
        self.ui_set_status("启动中…")

        self.done_rows = 0
        self.total_rows = 0
        self.ui_update_progress()

        self.start_ts = time.time()
        if self._tick_after_id is None:
            self._tick()

        target_type = self.var_target_type.get()

        def run():
            completed_normally = False
            try:
                self.logger.info("初始化DB")
                self.db = DatabaseManager()

                # 更新账号使用时间
                self.db.update_cookie_usage(selected_acc_name)

                self.logger.info("初始化爬虫（速度参数将实时读取 GUI 输入框）")
                if target_type == TargetType.BRAND:
                    url_checker = self.db.is_url_exists_brand
                    insert_cb = self.db.insert_brand_log
                else:
                    url_checker = self.db.is_url_exists_artist
                    insert_cb = self.db.insert_artist_log
                background_mode = bool(self.var_background_mode.get())
                if background_mode and bool(self.var_headless.get()):
                    self.logger.info("已启用无头模式，后台最小化设置将被自动忽略。")

                self.crawler = XHSCrawler(
                    target_type=target_type,
                    account_name=selected_acc_name,
                    url_checker=url_checker,
                    insert_callback=insert_cb,
                    get_scroll_sleep=self.get_scroll_sleep,
                    get_detail_sleep=self.get_detail_sleep,
                    get_login_qr_offset=self.get_login_qr_offset,
                    on_sleep=self.on_sleep,
                    on_captcha_detected=self.on_captcha_detected,
                    skip_event=self.skip_event,
                    max_scroll_default=max_scroll,
                    headless=self.var_headless.get(),
                    run_in_background=background_mode,
                    browser=self.var_browser.get(),
                    logger=self.logger,
                    on_login_qr_detected=self.on_login_qr_detected
                )

                self.logger.info(f"使用账号 [{selected_acc_name}] 登录...")
                # 注入 Cookie 登录
                latest_cookies = self.crawler.login(cookie_list=selected_cookies)
                try:
                    snapshot = latest_cookies if latest_cookies else self.crawler.driver.get_cookies()
                    if snapshot:
                        self.db.upsert_xhs_cookie(selected_acc_name, snapshot)
                        self.logger.info(f"账号 [{selected_acc_name}] Cookie 已刷新回写（{len(snapshot)}条）")
                except Exception as refresh_err:
                    self.logger.warning(f"回写最新 Cookie 失败: {refresh_err}")

                rows = self.db.fetch_brand_urls() if target_type == TargetType.BRAND else self.db.fetch_artists()
                self.total_rows = len(rows)
                self.ui_update_progress()
                self.logger.info(f"待处理数量：{self.total_rows}")

                for idx, row in enumerate(rows, start=1):
                    if self.crawler.stop_requested:
                        break
                    try:
                        name = row.get('brand_name', f"id={row.get('id')}")
                        self.ui_set_status(f"运行中：{name} ({idx}/{self.total_rows})")
                        self.logger.info(f"处理：{name}")

                        try:
                            self.crawler.max_scroll_default = int(self.var_max_scroll.get())
                        except Exception:
                            pass

                        ok = self.crawler.crawl_target(row)
                        if ok:
                            self.db.update_last_gather_time(row['id'])
                            self.logger.info(f"已更新采集时间: {name}")
                        target_stats = dict(self.crawler.last_target_stats or {})
                        total_stats = dict(self.crawler.total_crawl_stats or {})
                        self.logger.info(
                            "处理完成[%s]：打开URL=%d，入库=%d，DB已存在跳过=%d；累计 打开URL=%d，入库=%d，DB已存在跳过=%d",
                            name,
                            int(target_stats.get("opened_urls", 0)),
                            int(target_stats.get("inserted_urls", 0)),
                            int(target_stats.get("skipped_existing_urls", 0)),
                            int(total_stats.get("opened_urls", 0)),
                            int(total_stats.get("inserted_urls", 0)),
                            int(total_stats.get("skipped_existing_urls", 0)),
                        )

                        self.crawler.all_links.clear()
                    except Exception as e:
                        self.logger.error(f"处理异常 {row.get('brand_name')}: {e}")
                    finally:
                        self.done_rows = idx
                        self.ui_update_progress()

                if self.crawler and self.crawler.stop_requested:
                    self.logger.info("任务被用户停止")
                    self.ui_set_status("已停止")
                else:
                    self.logger.info("任务结束")
                    self.ui_set_status("已完成")
                    completed_normally = True
            except KeyboardInterrupt:
                self.logger.info("用户停止或被验证码拦截")
                self.ui_set_status("已停止")
            except Exception as e:
                self.logger.exception(f"运行异常：{e}")
                self.ui_set_status("异常")
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

                self.ui_set_buttons(start='normal', stop='disabled', resume='disabled', skip='disabled',
                                    add_acc='normal', remove_acc='normal')
                self.ui(lambda: self._clear_sleep_bar())
                if completed_normally:
                    self.ui(self._schedule_shutdown_after_completion)

        self.running_thread = threading.Thread(target=run, daemon=True)
        self.running_thread.start()
        self.ui_set_status("运行中…")

    def stop(self):
        if self.crawler:
            self.crawler.request_stop()
            self.ui_set_status("停止中…")
            self.logger.info("已请求停止，请等待当前步骤完成")
        else:
            self.ui_set_status("就绪")

    def resume(self):
        """扫码/风控完成后，手动恢复采集"""
        if not self.crawler:
            return

        def _apply_restore():
            try:
                self.btn_resume.config(state='disabled')
            except Exception:
                pass
            self._captcha_prompt_active = False

            self.ui_set_status("运行中…")
            self._clear_sleep_bar()
            self.logger.info("已点击“恢复运行”，采集将继续执行")

        self.ui(_apply_restore)

        try:
            self.crawler.resume()
        except Exception:
            pass

    def skip_current_wait(self):
        self.skip_event.set()
        self.logger.info("已触发“跳过本次等待”，即将继续下一步")

    def on_close(self):
        self._captcha_prompt_active = False
        self.auto_process_enabled = False
        self.save_account_settings(log_errors=False)
        self._clear_shutdown_plan(cancel_os_shutdown=True)
        if self.auto_process_after_id is not None:
            try:
                self.master.after_cancel(self.auto_process_after_id)
            except Exception:
                pass
            self.auto_process_after_id = None
        try:
            if self.crawler:
                self.crawler.request_stop()
        except Exception:
            pass
        self._quit_detector_browser()
        try:
            self.upload_stop_event.set()
        except Exception:
            pass
        if self._tick_after_id is not None:
            try:
                self.master.after_cancel(self._tick_after_id)
            except Exception:
                pass
            self._tick_after_id = None
        if self._ui_pump_after_id is not None:
            try:
                self.master.after_cancel(self._ui_pump_after_id)
            except Exception:
                pass
            self._ui_pump_after_id = None
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
