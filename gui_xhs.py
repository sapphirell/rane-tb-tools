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
import time
import json  # 新增：用于序列化Cookie
import base64
import hashlib
import hmac
import random
import urllib.parse
import logging
import threading
from time import sleep
from typing import Dict, Optional, Callable, List, Tuple

import pymysql
import requests
from selenium.webdriver import Chrome
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog


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
        sql = "SELECT id, account_name, cookie_data, last_used_at FROM xhs_cookies ORDER BY last_used_at DESC, id DESC"
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

    # --- 字段检测 ---
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
            url_checker: Optional[Callable] = None,
            insert_callback: Optional[Callable] = None,
            *,
            get_scroll_sleep: Optional[Callable[[], float]] = None,
            get_detail_sleep: Optional[Callable[[], float]] = None,
            on_sleep: Optional[Callable[[str, float, float], None]] = None,
            on_captcha_detected: Optional[Callable[..., None]] = None,
            skip_event: Optional[threading.Event] = None,
            pause_event: Optional[threading.Event] = None,
            max_scroll_default: int = 20,
            headless: bool = False,
            logger: Optional[logging.Logger] = None
    ):
        self.target_type = target_type
        self.url_checker = url_checker
        self.insert_callback = insert_callback
        self.get_scroll_sleep = get_scroll_sleep or (lambda: 10.5)
        self.get_detail_sleep = get_detail_sleep or (lambda: 5.0)
        self.on_sleep = on_sleep
        self.on_captcha_detected = on_captcha_detected
        self.skip_event = skip_event or threading.Event()
        self.pause_event = pause_event or threading.Event()
        self.pause_reason = ''
        self._pause_logged = False
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

        self.all_links = set()
        self.collected_quick_data = []

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
            # 1. 检查 .captcha-modal-content (新增)
            modal_els = self.driver.find_elements(By.CLASS_NAME, "captcha-modal-content")
            modal_visible = any(e.is_displayed() for e in modal_els) if modal_els else False
            if modal_visible:
                self.logger.warning(f"检测到弹窗验证码 .captcha-modal-content（{where}），将暂停采集并等待你处理验证码。")
                if self.on_captcha_detected:
                    try:
                        self.on_captcha_detected(where, "captcha-modal-content")
                    except TypeError:
                        self.on_captcha_detected(where)
                self.request_pause(f"captcha-modal-content@{where}")
                self._wait_until_resumed()
                try:
                    self.driver.refresh()
                except Exception:
                    pass
                return True

            # 2. 检查 #captcha-div (严格风控)
            strict_els = self.driver.find_elements(By.CSS_SELECTOR, "#captcha-div")
            strict_visible = any(e.is_displayed() for e in strict_els) if strict_els else False
            if strict_visible:
                self.logger.warning(f"检测到扫码/风控验证码页 #captcha-div（{where}），将暂停采集并等待你处理验证码。")
                if self.on_captcha_detected:
                    try:
                        self.on_captcha_detected(where, "captcha-div")
                    except TypeError:
                        self.on_captcha_detected(where)
                self.request_pause(f"captcha-div@{where}")
                self._wait_until_resumed()
                try:
                    self.driver.refresh()
                except Exception:
                    pass
                return True

            # 3. 检查 #red-captcha
            els = self.driver.find_elements(By.CSS_SELECTOR, "#red-captcha, div#red-captcha")
            visible = any(e.is_displayed() for e in els) if els else False
            if visible:
                self.logger.warning(f"检测到验证码 #red-captcha（{where}），将暂停采集并等待你处理验证码。")
                if self.on_captcha_detected:
                    try:
                        self.on_captcha_detected(where, "red-captcha")
                    except TypeError:
                        self.on_captcha_detected(where)
                self.request_pause(f"red-captcha@{where}")
                self._wait_until_resumed()
                try:
                    self.driver.refresh()
                except Exception:
                    pass
                return True
        except Exception:
            pass
        return False

    def login(self, cookie_list: Optional[List[Dict]] = None) -> Optional[List[Dict]]:
        """
        登录逻辑
        :param cookie_list: 如果提供了 cookie_list，则注入 Cookie；否则等待扫码
        :return: 如果是扫码登录，返回新的 Cookie 列表；否则返回 None
        """
        self.driver.get('https://www.xiaohongshu.com/explore')
        self._detect_and_handle_captcha("explore")

        if cookie_list:
            self.logger.info("正在注入选定的 Cookie...")
            try:
                for cookie in cookie_list:
                    # Selenium 对 cookie 字段有些要求，过滤掉不必要的
                    c = {k: v for k, v in cookie.items() if
                         k in ['name', 'value', 'domain', 'path', 'expiry', 'secure', 'httpOnly']}
                    self.driver.add_cookie(c)

                self.driver.refresh()
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'user.side-bar-component'))
                )
                self.logger.info("Cookie 登录成功")
                sleep(3)
                return None
            except Exception as e:
                self.logger.warning(f"Cookie 登录失败或失效: {e}，将转为手动登录")

        # 手动登录流程
        self.logger.info("等待手动扫码登录（120s 超时）...")
        WebDriverWait(self.driver, 120).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'user.side-bar-component'))
        )
        self.logger.info('扫码登录成功')
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

    def smart_scroll(self, spd_setting=1, max_scroll=None):
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
            self.process_quick_data(new_links)

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
                self.process_quick_data(new_links)

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
                self.smart_scroll(spd_setting, max_scroll)

                if spd_setting == 1:
                    for note_url in list(self.all_links):
                        self.check_stop()
                        base_url = convert_xhs_url(note_url).split('?')[0]
                        if self.url_checker and self.url_checker(base_url):
                            continue
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
                    self.logger.info(f"快速采集数据入库成功: {len(self.collected_quick_data)} 条")

                self.all_links.clear()
                self.collected_quick_data.clear()
            return True
        except KeyboardInterrupt:
            self.logger.info("收到停止信号，已终止当前对象采集")
            return False
        except Exception as e:
            self.logger.error(f"对象采集失败 {resolve_rednote_url(row)}: {e}")
            return False


class TextHandler(logging.Handler):
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
        self.master.title("小红书爬虫 · 采集控制台（数据库Cookie管理版）")
        self.master.geometry("1000x980")

        # --- 账号管理区域 ---
        acc_frame = ttk.LabelFrame(master, text="账号管理")
        acc_frame.pack(fill="x", padx=10, pady=10)

        ttk.Label(acc_frame, text="选择登录账号：").pack(side='left', padx=5, pady=5)
        self.cb_account = ttk.Combobox(acc_frame, state='readonly', width=30)
        self.cb_account.pack(side='left', padx=5, pady=5)

        self.btn_add_acc = ttk.Button(acc_frame, text="+ 新增/更新账号", command=self.add_new_account)
        self.btn_add_acc.pack(side='left', padx=10, pady=5)

        ttk.Button(acc_frame, text="刷新列表", command=self.load_accounts).pack(side='left', padx=5)

        # --- 采集参数 ---
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
        ttk.Checkbutton(frm, text="无头模式(Headless)", variable=self.var_headless).grid(row=0, column=6, padx=6,
                                                                                         pady=6)

        type_frame = ttk.LabelFrame(master, text="采集对象")
        type_frame.pack(fill='x', padx=10, pady=(0, 10))
        self.var_target_type = tk.StringVar(value=TargetType.BRAND)
        ttk.Radiobutton(
            type_frame, text="品牌（娃店 → spider_log）",
            value=TargetType.BRAND, variable=self.var_target_type,
            command=lambda: self.on_target_type_change(TargetType.BRAND)
        ).pack(side='left', padx=10, pady=6)
        ttk.Radiobutton(
            type_frame, text="艺术家（妆师/毛娘 → artist_spider_log）",
            value=TargetType.ARTIST, variable=self.var_target_type,
            command=lambda: self.on_target_type_change(TargetType.ARTIST)
        ).pack(side='left', padx=10, pady=6)

        ctrl = ttk.Frame(master)
        ctrl.pack(fill='x', padx=10)

        self.btn_start = ttk.Button(ctrl, text="开始采集", command=self.start)
        self.btn_stop = ttk.Button(ctrl, text="停止采集", command=self.stop, state='disabled')
        self.btn_resume = ttk.Button(ctrl, text="恢复运行", command=self.resume, state='disabled')
        self.btn_skip = ttk.Button(ctrl, text="跳过当前等待", command=self.skip_current_wait, state='disabled')
        # 新增的维护按钮
        self.btn_maintenance = ttk.Button(ctrl, text="数据维护", command=self.run_data_maintenance)
        self.btn_upload_images = ttk.Button(ctrl, text="上传图片", command=self.run_spider_image_upload)
        self.btn_stop_upload = ttk.Button(ctrl, text="停止上传", command=self.stop_spider_image_upload, state='disabled')
        self.btn_delete_failed_upload = ttk.Button(
            ctrl,
            text="删除传图失败记录",
            command=self.delete_failed_upload_records
        )
        self.btn_auto_process = ttk.Button(ctrl, text="自动处理(每1分钟): 关", command=self.toggle_auto_process)

        self.btn_start.pack(side='left', padx=6, pady=4)
        self.btn_stop.pack(side='left', padx=6, pady=4)
        self.btn_resume.pack(side='left', padx=6, pady=4)
        self.btn_skip.pack(side='left', padx=6, pady=4)
        self.btn_maintenance.pack(side='left', padx=6, pady=4)
        self.btn_upload_images.pack(side='left', padx=6, pady=4)
        self.btn_stop_upload.pack(side='left', padx=6, pady=4)
        self.btn_delete_failed_upload.pack(side='left', padx=6, pady=4)
        self.btn_auto_process.pack(side='left', padx=6, pady=4)

        self.var_status = tk.StringVar(value="就绪")
        ttk.Label(ctrl, textvariable=self.var_status).pack(side='left', padx=12)

        info = ttk.Frame(master)
        info.pack(fill='x', padx=10, pady=(6, 8))
        self.var_duration = tk.StringVar(value="已用时：00:00:00")
        self.var_progress_text = tk.StringVar(value="进度：0 / 0 (0.0%)")
        ttk.Label(info, textvariable=self.var_duration).pack(side='left', padx=6)
        ttk.Label(info, textvariable=self.var_progress_text).pack(side='left', padx=18)

        self.progress = ttk.Progressbar(master, length=940, mode='determinate', maximum=100)
        self.progress.pack(fill='x', padx=10, pady=(0, 8))

        sleep_frame = ttk.LabelFrame(master, text="等待进度（Sleep 读条）")
        sleep_frame.pack(fill='x', padx=10, pady=(0, 10))
        self.var_sleep_text = tk.StringVar(value="当前无等待")
        ttk.Label(sleep_frame, textvariable=self.var_sleep_text).pack(anchor='w', padx=8, pady=(6, 2))
        self.sleep_bar = ttk.Progressbar(sleep_frame, length=940, mode='determinate', maximum=100)
        self.sleep_bar.pack(fill='x', padx=8, pady=(0, 8))

        log_frame = ttk.LabelFrame(master, text="采集日志")
        log_frame.pack(fill='both', expand=True, padx=10, pady=10)
        self.txt_log = tk.Text(log_frame, height=15, state='disabled')
        self.txt_log.pack(fill='both', expand=True, side='left')
        scroll = ttk.Scrollbar(log_frame, command=self.txt_log.yview)
        scroll.pack(side='right', fill='y')
        self.txt_log['yscrollcommand'] = scroll.set

        upload_log_frame = ttk.LabelFrame(master, text="图片上传日志")
        upload_log_frame.pack(fill='both', expand=True, padx=10, pady=(0, 10))
        self.txt_upload_log = tk.Text(upload_log_frame, height=10, state='disabled')
        self.txt_upload_log.pack(fill='both', expand=True, side='left')
        upload_scroll = ttk.Scrollbar(upload_log_frame, command=self.txt_upload_log.yview)
        upload_scroll.pack(side='right', fill='y')
        self.txt_upload_log['yscrollcommand'] = upload_scroll.set

        self.logger = logging.getLogger("XHS")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        if self.logger.handlers:
            self.logger.handlers.clear()
        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.gui_handler = TextHandler(self.txt_log)
        self.gui_handler.setFormatter(fmt)
        self.logger.addHandler(self.gui_handler)
        file_handler = logging.FileHandler('xhs_crawler.log', encoding='utf-8')
        file_handler.setFormatter(fmt)
        self.logger.addHandler(file_handler)

        self.upload_logger = logging.getLogger("XHS_UPLOAD")
        self.upload_logger.setLevel(logging.INFO)
        self.upload_logger.propagate = False
        if self.upload_logger.handlers:
            self.upload_logger.handlers.clear()
        self.upload_gui_handler = TextHandler(self.txt_upload_log)
        self.upload_gui_handler.setFormatter(fmt)
        self.upload_logger.addHandler(self.upload_gui_handler)
        upload_file_handler = logging.FileHandler('xhs_upload.log', encoding='utf-8')
        upload_file_handler.setFormatter(fmt)
        self.upload_logger.addHandler(upload_file_handler)

        self.running_thread: Optional[threading.Thread] = None
        self.maintenance_thread: Optional[threading.Thread] = None
        self.upload_thread: Optional[threading.Thread] = None
        self.crawler: Optional[XHSCrawler] = None
        self.db: Optional[DatabaseManager] = None
        self.failed_upload_spider_ids: List[int] = []
        self.failed_upload_artist_ids: List[int] = []

        # 缓存账号列表 {name: cookie_json_list}
        self.account_map = {}

        self.start_ts = None
        self._tick_after_id = None
        self.total_rows = 0
        self.done_rows = 0

        self.skip_event = threading.Event()
        self.upload_stop_event = threading.Event()
        self.auto_process_enabled = False
        self.auto_process_after_id = None

        self._backup_scroll_sleep: Optional[str] = None
        self._backup_detail_sleep: Optional[str] = None

        self.master.protocol("WM_DELETE_WINDOW", self.on_close)

        # 初始化加载账号
        self.load_accounts()

    # ---------- UI 线程安全 ----------
    def ui(self, fn):
        try:
            self.master.after(0, fn)
        except Exception:
            pass

    def ui_set_status(self, text: str):
        self.ui(lambda: self.var_status.set(text))

    def ui_set_buttons(self, *, start=None, stop=None, resume=None, skip=None, add_acc=None):
        def _apply():
            try:
                if start is not None: self.btn_start.config(state=start)
                if stop is not None: self.btn_stop.config(state=stop)
                if resume is not None: self.btn_resume.config(state=resume)
                if skip is not None: self.btn_skip.config(state=skip)
                if add_acc is not None: self.btn_add_acc.config(state=add_acc)
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
                        self.cb_account.current(0)
                    else:
                        self.cb_account.set('暂无账号，请新增')

                self.ui(_ui_update)
            except Exception as e:
                self.logger.error(f"加载账号列表失败: {e}")

        threading.Thread(target=_load, daemon=True).start()

    def add_new_account(self):
        """新增账号流程：打开浏览器 -> 扫码 -> 输入名称 -> 保存"""
        if self.running_thread and self.running_thread.is_alive():
            messagebox.showinfo("提示", "采集进行中，请先停止")
            return

        self.ui_set_buttons(start='disabled', add_acc='disabled')
        self.var_status.set("正在启动浏览器录入账号...")

        def _run_add():
            temp_crawler = None
            try:
                temp_crawler = XHSCrawler(target_type="temp", logger=self.logger, headless=False)
                self.logger.info("浏览器已启动，请在弹出的浏览器中扫码登录...")

                # 调用 login 不传 cookie，触发扫码逻辑
                new_cookies = temp_crawler.login(cookie_list=None)

                if new_cookies:
                    # 获取用户输入需要在主线程
                    user_input_name = [None]

                    def _ask_name():
                        name = simpledialog.askstring("保存账号", "登录成功！请输入该账号的备注名称：")
                        user_input_name[0] = name

                    # 阻塞等待用户输入，或者使用 event，这里简单用 after+delay 模拟同步是不行的
                    # 所以我们在 thread 里调用 ui 方法，但需要等待结果
                    # 实际上 simpledialog 会阻塞主循环，如果在 callback 里调用会卡住
                    # 这里直接通过 sync 方式调用有点麻烦。
                    # 简化处理：不阻塞，只是在保存前确认。

                    def _save_step():
                        name = simpledialog.askstring("保存账号", "登录成功！请输入该账号的备注名称：")
                        if name:
                            try:
                                db = DatabaseManager()
                                db.upsert_xhs_cookie(name, new_cookies)
                                db.connection.close()
                                messagebox.showinfo("成功", f"账号 [{name}] 已保存！")
                                self.load_accounts()
                            except Exception as db_e:
                                messagebox.showerror("错误", f"数据库保存失败: {db_e}")
                        else:
                            self.logger.info("用户取消保存账号")

                        self.ui_set_buttons(start='normal', add_acc='normal')
                        self.var_status.set("就绪")

                    self.ui(_save_step)

            except Exception as e:
                self.logger.error(f"录入账号异常: {e}")
                self.ui_set_buttons(start='normal', add_acc='normal')
                self.var_status.set("录入失败")
            finally:
                if temp_crawler and temp_crawler.driver:
                    temp_crawler.driver.quit()

        threading.Thread(target=_run_add, daemon=True).start()

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
            self.var_status.set("正在执行数据维护...")
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
            # 保存当前设置的速度
            cur_scroll = (self.var_scroll_sleep.get() or "").strip()
            cur_detail = (self.var_detail_sleep.get() or "").strip()
            if self._backup_scroll_sleep is None and cur_scroll and cur_scroll != "99999999":
                self._backup_scroll_sleep = cur_scroll
            if self._backup_detail_sleep is None and cur_detail and cur_detail != "99999999":
                self._backup_detail_sleep = cur_detail

            # 暂停计时器（设为极长时间）
            self.var_scroll_sleep.set("99999999")
            self.var_detail_sleep.set("99999999")

            # 更新 UI 状态
            self.btn_resume.config(state='normal')
            self.var_status.set("已暂停：等待验证码处理后恢复运行")

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

        selected_cookies = self.account_map[selected_acc_name]

        try:
            max_scroll = int(self.var_max_scroll.get())
        except ValueError:
            messagebox.showerror("错误", "最大滚动次数需为整数")
            return

        self.btn_start.config(state='disabled')
        self.btn_add_acc.config(state='disabled')  # 运行时不可新增
        self.btn_stop.config(state='normal')
        self.btn_resume.config(state='disabled')
        self.btn_skip.config(state='normal')
        self.var_status.set("启动中…")

        self.done_rows = 0
        self.total_rows = 0
        self.ui_update_progress()

        self.start_ts = time.time()
        if self._tick_after_id is None:
            self._tick()

        target_type = self.var_target_type.get()

        def run():
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

                self.logger.info(f"使用账号 [{selected_acc_name}] 登录...")
                # 注入 Cookie 登录
                self.crawler.login(cookie_list=selected_cookies)

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
                                    add_acc='normal')
                self.ui(lambda: self._clear_sleep_bar())

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

    def resume(self):
        """扫码/风控完成后，手动恢复采集"""
        if not self.crawler:
            return

        def _apply_restore():
            if (self.var_scroll_sleep.get() or '').strip() == "99999999" and self._backup_scroll_sleep:
                self.var_scroll_sleep.set(self._backup_scroll_sleep)
            if (self.var_detail_sleep.get() or '').strip() == "99999999" and self._backup_detail_sleep:
                self.var_detail_sleep.set(self._backup_detail_sleep)

            self._backup_scroll_sleep = None
            self._backup_detail_sleep = None

            try:
                self.btn_resume.config(state='disabled')
            except Exception:
                pass

            self.var_status.set("运行中…")
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
        self.auto_process_enabled = False
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
