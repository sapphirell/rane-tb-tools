#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 From Switch 采集来源修复历史商品的尺寸父子关系。

该脚本只读取 spider_log、images_log、goods 和 goods_size，使用图片 URL 将商品
关联回唯一的 From Switch 来源记录，然后把历史上误写进 goods_size 的细分类
转换为“父分类 + 细分类”。实际写入统一调用现有商品尺寸 PATCH 接口，不直接
修改商品表，也不重新采集、删除或重新创建商品。

默认是只读预览；只有显式传入 --apply 才会写入。来源匹配不唯一、图片交集过少、
尺寸值无法安全保留或来源没有足够证据的记录会跳过并写入报告。
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple
from urllib.parse import urlsplit, urlunsplit

import pymysql
import requests

from brand_spider_145 import (
    FROM_SWITCH_SIZE_DETAILS_BY_CATEGORY,
    FROM_SWITCH_SIZE_DETAIL_TO_CATEGORY,
    clean_inline_text,
    is_from_switch_empty_option,
    normalize_from_switch_size_pair,
)


DEFAULT_BRAND_ID = 145  # From Switch 在系统中的品牌 ID。
DEFAULT_ORIGIN_TYPE = "from_switch"  # 来源记录的固定 origin_type。
DEFAULT_API_BASE_URL = "http://localhost:8080"  # 本地管理后台 API 地址。
DEFAULT_MIN_SHARED_IMAGES = 2  # 至少共享两张图片才认为商品与来源关联可靠。
DEFAULT_API_TIMEOUT = 30.0  # 尺寸 PATCH 接口的请求读取超时时间。

# 这些值来自非尺寸选项或无选项占位，历史脏数据中出现时可以确定移除。
NON_SIZE_VALUES = frozenset(
    {
        "no option",
        "no options",
        "none",
        "hands",
        "hands+foot",
        "hands+feet",
        "feet",
        "body painting",
        "chest parts",
    }
)

# 目前 From Switch 脏数据的细分类证据主要是身体型号；仅用于校验已有值，
# 不根据头围或模糊图片自行新增尺寸，避免修复脚本改变原有分类意图。
SOURCE_DETAIL_EVIDENCE_PATTERNS: Dict[str, Tuple[str, ...]] = {
    "65成男": (r"\b(?:hd\s*)?65\s*boy\b", r"\b65boy\b"),
    "70+普成男": (r"\b(?:hd\s*)?70\s*boy\b", r"\b70boy\b"),
    "大女": (r"\b(?:hd\s*)?64\s*(?:girl|labyrinth)\s*body\b", r"\bgirl\s*body\b"),
}


def normalized_size_key(value: Any) -> str:
    """生成尺寸匹配键，只折叠空白和大小写，不改动用户看到的文字。"""
    return re.sub(r"\s+", "", clean_inline_text(value)).casefold()


def normalized_image_match_url(value: Any) -> str:
    """生成来源与商品图片的匹配键，兼容协议、fragment 和末尾斜杠差异。"""
    raw_value = clean_inline_text(value)
    if not raw_value:
        return ""
    if raw_value.startswith("//"):
        raw_value = f"https:{raw_value}"
    parsed = urlsplit(raw_value)
    if not parsed.netloc:
        return raw_value
    return urlunsplit(
        (
            "https",
            parsed.netloc.casefold(),
            parsed.path.rstrip("/") or "/",
            parsed.query,
            "",
        )
    )


KNOWN_DETAIL_BY_KEY: Dict[str, str] = {
    normalized_size_key(detail): detail for detail in FROM_SWITCH_SIZE_DETAIL_TO_CATEGORY
}
KNOWN_PARENT_BY_KEY: Dict[str, str] = {
    normalized_size_key(category): category for category in FROM_SWITCH_SIZE_DETAILS_BY_CATEGORY
}
NON_SIZE_VALUE_KEYS = {normalized_size_key(value) for value in NON_SIZE_VALUES}


@dataclass(frozen=True)
class SizeRow:
    """数据库中 goods_size 的一行尺寸记录。"""

    row_id: int  # goods_size 主键；修复接口不直接使用，只用于审计。
    goods_id: int  # 所属商品 ID。
    category: str  # 当前 goods_size 字段。
    detail: str  # 当前 size_detail 字段。
    sort_order: int  # 当前尺寸显示顺序。


@dataclass(frozen=True)
class SourceMatch:
    """商品与一条 spider_log 来源记录的图片匹配结果。"""

    goods_id: int  # 商品主键。
    goods_name: str  # 商品当前名称。
    primary_category: str  # 商品主记录当前 size。
    primary_detail: str  # 商品主记录当前 size_detail。
    source_id: int  # 唯一来源 spider_log 主键。
    source_title: str  # 来源标题。
    source_url: str  # 来源详情地址。
    source_content: str  # 来源采集正文和规格文本。
    shared_images: int  # 商品图片与来源图片的 URL 交集数量。
    candidate_count: int = 1  # 同一商品命中的来源记录数量。
    size_rows: Tuple[SizeRow, ...] = field(default_factory=tuple)  # 商品当前全部尺寸行。


@dataclass(frozen=True)
class RepairPlan:
    """一条商品尺寸修复计划。"""

    match: SourceMatch  # 经过来源匹配的商品。
    old_sizes: Tuple[Dict[str, str], ...]  # 当前发送前的尺寸快照。
    new_sizes: Tuple[Dict[str, str], ...]  # 计划提交的完整尺寸列表。
    changed: bool  # 是否确实需要写入。
    reason: str  # 允许修复的原因，写入日志和报告。
    skip_reason: str = ""  # 非空表示只报告、不写入。


@dataclass
class RepairStats:
    """一次修复运行的统计。"""

    matched_goods: int = 0  # 通过来源图片关联到至少一条来源的商品数。
    unique_goods: int = 0  # 只关联到唯一来源的商品数。
    planned: int = 0  # 生成有效变更计划的数量。
    unchanged: int = 0  # 来源可靠但无需修改的数量。
    skipped: int = 0  # 因安全门槛或数据不完整而跳过的数量。
    applied: int = 0  # PATCH 接口返回成功的数量。
    verified: int = 0  # 写入后重新从数据库核对成功的数量。
    failures: List[Dict[str, str]] = field(default_factory=list)  # 失败或跳过明细。


def source_text_supports_detail(detail: str, source_text: str) -> bool:
    """校验已有细分类是否能在来源正文或标题中找到明确身体型号证据。"""
    patterns = SOURCE_DETAIL_EVIDENCE_PATTERNS.get(detail)
    if not patterns:
        # 其它细分类暂不从来源文字猜测；其父子转换仍由系统尺寸白名单保证。
        return True
    normalized_source = clean_inline_text(source_text).casefold()
    return any(re.search(pattern, normalized_source, re.IGNORECASE) for pattern in patterns)


def is_non_size_value(value: Any) -> bool:
    """判断历史尺寸行是否实际来自 Body painting 等非尺寸选项。"""
    return normalized_size_key(value) in NON_SIZE_VALUE_KEYS


def normalize_repair_pair(category: Any, detail: Any) -> Tuple[str, str, str]:
    """归一化一行尺寸并返回 `(父分类, 细分类, 处理说明)`。"""
    raw_category = clean_inline_text(category)
    raw_detail = clean_inline_text(detail)
    if not raw_category or is_from_switch_empty_option(raw_category) or is_non_size_value(raw_category):
        return "", "", "移除无选项或非尺寸值"
    if is_from_switch_empty_option(raw_detail) or is_non_size_value(raw_detail):
        raw_detail = ""

    category, detail = normalize_from_switch_size_pair(raw_category, raw_detail)
    category = clean_inline_text(category)
    detail = clean_inline_text(detail)
    if not category:
        return "", "", "移除空尺寸"
    if normalized_size_key(category) in KNOWN_DETAIL_BY_KEY:
        # 这里理论上不会发生：共享归一化函数应已把细分类放入父分类。
        parent = FROM_SWITCH_SIZE_DETAIL_TO_CATEGORY[KNOWN_DETAIL_BY_KEY[normalized_size_key(category)]]
        detail = detail or KNOWN_DETAIL_BY_KEY[normalized_size_key(category)]
        return parent, detail, "细分类移入父分类"
    if normalized_size_key(category) not in KNOWN_PARENT_BY_KEY:
        return category, detail, "保留来源中的自定义尺寸"
    return category, detail, "保留规范父子尺寸"


def current_rows_for_match(match: SourceMatch) -> List[SizeRow]:
    """取得商品的尺寸行；历史表缺少子行时用主商品尺寸构造审计行。"""
    rows = list(match.size_rows)
    if rows:
        primary = (clean_inline_text(match.primary_category), clean_inline_text(match.primary_detail))
        if primary[0] and not any((row.category, row.detail) == primary for row in rows):
            rows.insert(
                0,
                SizeRow(
                    row_id=0,
                    goods_id=match.goods_id,
                    category=primary[0],
                    detail=primary[1],
                    sort_order=-1,
                ),
            )
        return rows
    if not clean_inline_text(match.primary_category):
        return []
    return [
        SizeRow(
            row_id=0,
            goods_id=match.goods_id,
            category=match.primary_category,
            detail=match.primary_detail,
            sort_order=0,
        )
    ]


def build_repair_plan(match: SourceMatch, min_shared_images: int) -> RepairPlan:
    """根据唯一来源和当前尺寸行生成安全的完整替换计划。"""
    if match.candidate_count != 1:
        return RepairPlan(match, tuple(), tuple(), False, "", "同一商品命中多个采集来源，拒绝自动修复")
    if match.shared_images < min_shared_images:
        return RepairPlan(
            match,
            tuple(),
            tuple(),
            False,
            "",
            f"商品与来源只共享 {match.shared_images} 张图片，低于安全阈值 {min_shared_images}",
        )
    source_text = f"{match.source_title}\n{match.source_url}\n{match.source_content}"
    old_rows = current_rows_for_match(match)
    if not old_rows:
        return RepairPlan(match, tuple(), tuple(), False, "", "商品没有可读取的尺寸记录")

    old_sizes: List[Dict[str, str]] = []
    normalized_sizes: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    removed_non_size = 0
    converted_detail = 0
    for row in old_rows:
        raw_category = clean_inline_text(row.category)
        raw_detail = clean_inline_text(row.detail)
        old_sizes.append({"goods_size": raw_category, "size_detail": raw_detail})
        category, detail, _row_reason = normalize_repair_pair(raw_category, raw_detail)
        if not category:
            removed_non_size += 1
            continue
        if normalized_size_key(raw_category) in KNOWN_DETAIL_BY_KEY and not raw_detail:
            converted_detail += 1
        if detail and not source_text_supports_detail(detail, source_text):
            return RepairPlan(
                match,
                tuple(old_sizes),
                tuple(),
                False,
                "",
                f"来源正文未找到细分类“{detail}”的明确型号证据",
            )
        identity = (category, detail)
        if identity in seen:
            continue
        seen.add(identity)
        normalized_sizes.append({"goods_size": category, "size_detail": detail})

    if not normalized_sizes:
        return RepairPlan(match, tuple(old_sizes), tuple(), False, "", "清洗后没有可保存的尺寸，拒绝清空商品尺寸")

    # 同一父分类已经有细分类时，父分类空详情行是旧 AI 的重复概括，应删除。
    detailed_categories = {
        item["goods_size"] for item in normalized_sizes if item["goods_size"] and item["size_detail"]
    }
    normalized_sizes = [
        item
        for item in normalized_sizes
        if not (item["goods_size"] in detailed_categories and not item["size_detail"])
    ]
    if not normalized_sizes:
        return RepairPlan(match, tuple(old_sizes), tuple(), False, "", "去除重复父分类后没有可保存尺寸")

    changed = old_sizes != normalized_sizes
    if not changed:
        return RepairPlan(match, tuple(old_sizes), tuple(normalized_sizes), False, "来源匹配可靠但尺寸无需修改")

    reasons: List[str] = []
    if converted_detail:
        reasons.append(f"转换 {converted_detail} 条细分类父子关系")
    if removed_non_size:
        reasons.append(f"移除 {removed_non_size} 条非尺寸选项")
    if any(
        item["goods_size"] not in {row.category for row in old_rows}
        or item["size_detail"] != next(
            (row.detail for row in old_rows if row.category == item["goods_size"]), ""
        )
        for item in normalized_sizes
    ):
        reasons.append("清理重复父分类或规范尺寸顺序")
    return RepairPlan(match, tuple(old_sizes), tuple(normalized_sizes), True, "；".join(reasons) or "规范尺寸父子关系")


class RepairDatabase:
    """读取商品、尺寸、图片和采集来源，并提供写入后的只读核对。"""

    def __init__(self, host: str, port: int, user: str, password: str, database: str, logger: logging.Logger) -> None:
        """建立只用于查询的 MySQL 连接；本脚本不执行任何 SQL 写入。"""
        if not password:
            raise RuntimeError("缺少数据库密码，请设置 SPIDER_DB_PASSWORD")
        self.logger = logger  # 记录匹配和核对过程。
        self.connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            # 该连接只做盘点和 PATCH 后核对；开启自动提交，避免外部 API 写入后
            # 长事务继续读取旧快照，导致把已经成功的修复误报为失败。
            autocommit=True,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
        )

    def _ensure_connection(self) -> None:
        """在读取前确认数据库连接仍然可用。"""
        self.connection.ping(reconnect=True)

    def list_source_matches(
        self,
        brand_id: int,
        origin_type: str,
        goods_ids: Sequence[int],
    ) -> List[SourceMatch]:
        """通过商品图片与 spider_log.images 的 URL 交集读取来源候选。

        images_log.url 没有适合大范围连接的索引，直接在数据库中把两类图片按
        URL JOIN 会触发全表扫描并在真实库上超时。因此这里先按 relation_id 读取
        脏商品图片，再读取品牌来源记录的 images 字段，在脚本内完成小集合匹配。
        """
        details = tuple(FROM_SWITCH_SIZE_DETAIL_TO_CATEGORY.keys())
        detail_placeholders = ", ".join(["%s"] * len(details))
        id_clause = ""
        id_params: List[Any] = []
        if goods_ids:
            id_clause = f"AND g.id IN ({', '.join(['%s'] * len(goods_ids))})"
            id_params.extend(goods_ids)
        dirty_query = f"""
            SELECT
                g.id AS goods_id,
                g.name AS goods_name,
                g.size AS primary_category,
                COALESCE(g.size_detail, '') AS primary_detail
            FROM goods g
            WHERE g.brand_id = %s
              AND g.is_delete = 0
              AND (
                    (g.size IN ({detail_placeholders})
                     AND COALESCE(TRIM(g.size_detail), '') = '')
                    OR EXISTS (
                        SELECT 1
                        FROM goods_size dirty_size
                        WHERE dirty_size.goods_id = g.id
                          AND dirty_size.goods_size IN ({detail_placeholders})
                          AND COALESCE(TRIM(dirty_size.size_detail), '') = ''
                    )
                  )
              {id_clause}
            ORDER BY g.id ASC
        """
        params: List[Any] = [brand_id]
        params.extend(details)
        params.extend(details)
        params.extend(id_params)
        self._ensure_connection()
        with self.connection.cursor() as cursor:
            cursor.execute(dirty_query, tuple(params))
            dirty_rows = cursor.fetchall()
        if not dirty_rows:
            return []

        dirty_goods_ids = [int(row.get("goods_id") or 0) for row in dirty_rows]
        image_placeholders = ", ".join(["%s"] * len(dirty_goods_ids))
        image_query = f"""
            SELECT relation_id, url
            FROM images_log
            WHERE `where` = %s
              AND relation_id IN ({image_placeholders})
        """
        image_params: List[Any] = ["goods-images"]
        image_params.extend(dirty_goods_ids)
        self._ensure_connection()
        with self.connection.cursor() as cursor:
            cursor.execute(image_query, tuple(image_params))
            image_rows = cursor.fetchall()
            source_query = """
                SELECT id, title, url, content, images
                FROM spider_log
                WHERE brand_id = %s
                  AND origin_type = %s
                  AND status = 2
                ORDER BY id ASC
            """
            cursor.execute(source_query, (brand_id, origin_type))
            source_rows = cursor.fetchall()

        source_ids = [int(row.get("id") or 0) for row in source_rows if int(row.get("id") or 0) > 0]
        source_image_rows: List[Dict[str, Any]] = []
        if source_ids:
            source_image_placeholders = ", ".join(["%s"] * len(source_ids))
            source_image_query = f"""
                SELECT relation_id, url
                FROM images_log
                WHERE `where` = %s
                  AND relation_id IN ({source_image_placeholders})
            """
            source_image_params: List[Any] = ["spider-log"]
            source_image_params.extend(source_ids)
            self._ensure_connection()
            with self.connection.cursor() as cursor:
                cursor.execute(source_image_query, tuple(source_image_params))
                source_image_rows = cursor.fetchall()

        goods_images: Dict[int, Set[str]] = defaultdict(set)
        for row in image_rows or []:
            goods_id = int(row.get("relation_id") or 0)
            image_url = normalized_image_match_url(row.get("url"))
            if goods_id and image_url:
                goods_images[goods_id].add(image_url)

        source_images_by_id: Dict[int, Set[str]] = defaultdict(set)
        for row in source_image_rows:
            source_id = int(row.get("relation_id") or 0)
            image_url = normalized_image_match_url(row.get("url"))
            if source_id and image_url:
                source_images_by_id[source_id].add(image_url)

        source_images: List[Tuple[Dict[str, Any], Set[str]]] = []
        for row in source_rows or []:
            source_id = int(row.get("id") or 0)
            urls = set(source_images_by_id.get(source_id, set()))
            if not urls:
                # 少数旧记录可能没有 images_log 子行，才退回 spider_log.images。
                urls = {
                    normalized_image_match_url(image)
                    for image in str(row.get("images") or "").split(",")
                }
            urls.discard("")
            if urls:
                source_images.append((row, urls))

        matches: List[SourceMatch] = []
        for goods_row in dirty_rows:
            goods_id = int(goods_row.get("goods_id") or 0)
            current_images = goods_images.get(goods_id, set())
            if not current_images:
                continue
            for source_row, source_urls in source_images:
                shared_images = len(current_images & source_urls)
                if shared_images <= 0:
                    continue
                matches.append(
                    SourceMatch(
                        goods_id=goods_id,
                        goods_name=clean_inline_text(goods_row.get("goods_name")),
                        primary_category=clean_inline_text(goods_row.get("primary_category")),
                        primary_detail=clean_inline_text(goods_row.get("primary_detail")),
                        source_id=int(source_row.get("id") or 0),
                        source_title=clean_inline_text(source_row.get("title")),
                        source_url=clean_inline_text(source_row.get("url")),
                        source_content=str(source_row.get("content") or ""),
                        shared_images=shared_images,
                    )
                )
        return matches

    def load_size_rows(self, goods_ids: Sequence[int]) -> Dict[int, Tuple[SizeRow, ...]]:
        """读取指定商品当前全部子尺寸行。"""
        if not goods_ids:
            return {}
        placeholders = ", ".join(["%s"] * len(goods_ids))
        query = f"""
            SELECT id, goods_id, goods_size, COALESCE(size_detail, '') AS size_detail, sort_order
            FROM goods_size
            WHERE goods_id IN ({placeholders})
            ORDER BY goods_id ASC, sort_order ASC, id ASC
        """
        self._ensure_connection()
        with self.connection.cursor() as cursor:
            cursor.execute(query, tuple(goods_ids))
            rows = cursor.fetchall()
        grouped: Dict[int, List[SizeRow]] = defaultdict(list)
        for row in rows or []:
            goods_id = int(row.get("goods_id") or 0)
            grouped[goods_id].append(
                SizeRow(
                    row_id=int(row.get("id") or 0),
                    goods_id=goods_id,
                    category=clean_inline_text(row.get("goods_size")),
                    detail=clean_inline_text(row.get("size_detail")),
                    sort_order=int(row.get("sort_order") or 0),
                )
            )
        return {goods_id: tuple(rows) for goods_id, rows in grouped.items()}

    def load_size_state(self, goods_ids: Sequence[int]) -> Dict[int, Tuple[Dict[str, str], ...]]:
        """重新读取主商品尺寸与子尺寸，供 PATCH 后核对。"""
        if not goods_ids:
            return {}
        placeholders = ", ".join(["%s"] * len(goods_ids))
        query = f"""
            SELECT g.id AS goods_id, g.size AS primary_category,
                   COALESCE(g.size_detail, '') AS primary_detail,
                   gs.sort_order, gs.id AS size_id,
                   gs.goods_size, COALESCE(gs.size_detail, '') AS size_detail
            FROM goods g
            LEFT JOIN goods_size gs ON gs.goods_id = g.id
            WHERE g.id IN ({placeholders})
            ORDER BY g.id ASC, gs.sort_order ASC, gs.id ASC
        """
        self._ensure_connection()
        with self.connection.cursor() as cursor:
            cursor.execute(query, tuple(goods_ids))
            rows = cursor.fetchall()
        grouped: Dict[int, List[Dict[str, str]]] = defaultdict(list)
        primary_by_goods: Dict[int, Tuple[str, str]] = {}
        for row in rows or []:
            goods_id = int(row.get("goods_id") or 0)
            primary_by_goods[goods_id] = (
                clean_inline_text(row.get("primary_category")),
                clean_inline_text(row.get("primary_detail")),
            )
            if row.get("size_id") is not None:
                grouped[goods_id].append(
                    {
                        "goods_size": clean_inline_text(row.get("goods_size")),
                        "size_detail": clean_inline_text(row.get("size_detail")),
                    }
                )
        result: Dict[int, Tuple[Dict[str, str], ...]] = {}
        for goods_id, primary in primary_by_goods.items():
            result[goods_id] = tuple(grouped.get(goods_id) or [{"goods_size": primary[0], "size_detail": primary[1]}])
        return result

    def close(self) -> None:
        """关闭数据库连接。"""
        try:
            self.connection.close()
        except pymysql.MySQLError:
            pass


class AdminGoodsSizeClient:
    """调用现有管理员商品尺寸 PATCH 接口。"""

    def __init__(self, base_url: str, token: str, timeout: float, logger: logging.Logger) -> None:
        """初始化 HTTP 客户端，不记录管理员令牌。"""
        if not token:
            raise RuntimeError("缺少管理员令牌，请设置 DOGDOGDOLL_ADMIN_TOKEN")
        if not math.isfinite(timeout) or timeout <= 0:
            raise RuntimeError("API 超时时间必须大于 0")
        self.base_url = base_url.rstrip("/")  # 管理后台 API 根地址。
        self.token = token  # 只放入请求头，不写入日志或报告。
        self.timeout = timeout  # PATCH 请求超时。
        self.logger = logger  # 记录接口结果。
        self.session = requests.Session()  # 复用本次修复的 HTTP 连接。

    def patch_sizes(self, goods_id: int, sizes: Sequence[Mapping[str, str]]) -> Dict[str, Any]:
        """提交完整尺寸列表，只更新商品尺寸字段。"""
        url = f"{self.base_url}/admin/v2/goods/{goods_id}/sizes"
        headers = {
            "Content-Type": "application/json",
            "X-Token": self.token,
            "Authorization": self.token,
        }
        try:
            response = self.session.patch(
                url,
                headers=headers,
                json={"sizes": list(sizes)},
                timeout=(10.0, self.timeout),
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"PATCH 请求失败：{exc}") from exc
        if response.status_code >= 400:
            raise RuntimeError(f"PATCH 返回 HTTP {response.status_code}：{self.response_message(response)}")
        try:
            body = response.json()
        except (TypeError, ValueError) as exc:
            raise RuntimeError("PATCH 返回不是 JSON") from exc
        if not isinstance(body, dict):
            raise RuntimeError("PATCH 返回格式不是对象")
        status = body.get("status")
        if status not in (None, "success", 0, 200):
            raise RuntimeError(f"PATCH 业务失败：{clean_inline_text(body.get('msg') or body.get('message'))[:300]}")
        data = body.get("data")
        if isinstance(data, dict):
            returned_sizes = data.get("sizes")
            if isinstance(returned_sizes, list):
                returned_pairs = tuple(
                    {
                        "goods_size": clean_inline_text(item.get("goods_size")),
                        "size_detail": clean_inline_text(item.get("size_detail")),
                    }
                    for item in returned_sizes
                    if isinstance(item, dict)
                )
                requested_pairs = tuple(
                    {
                        "goods_size": clean_inline_text(item.get("goods_size")),
                        "size_detail": clean_inline_text(item.get("size_detail")),
                    }
                    for item in sizes
                )
                self.logger.info(
                    "尺寸接口回执：goods_id=%d，返回尺寸=%s",
                    goods_id,
                    list(returned_pairs),
                )
                if returned_pairs != requested_pairs:
                    raise RuntimeError(
                        f"接口回执尺寸与请求不一致：返回={list(returned_pairs)}，请求={list(requested_pairs)}"
                    )
        return body

    @staticmethod
    def response_message(response: requests.Response) -> str:
        """提取短错误消息，避免把完整响应写入日志。"""
        try:
            body = response.json()
        except (TypeError, ValueError):
            body = None
        if isinstance(body, dict):
            return clean_inline_text(body.get("msg") or body.get("message") or body.get("error"))[:300]
        return clean_inline_text(response.text)[:300]

    def close(self) -> None:
        """关闭 HTTP 会话。"""
        self.session.close()


def group_matches(matches: Sequence[SourceMatch]) -> List[SourceMatch]:
    """标记每个商品的来源候选数量，供唯一性门槛使用。"""
    grouped: Dict[int, List[SourceMatch]] = defaultdict(list)
    for match in matches:
        grouped[match.goods_id].append(match)
    result: List[SourceMatch] = []
    for candidates in grouped.values():
        count = len({candidate.source_id for candidate in candidates})
        result.extend(
            SourceMatch(**{**candidate.__dict__, "candidate_count": count})
            for candidate in candidates
        )
    return result


def load_db_config(args: argparse.Namespace) -> Dict[str, Any]:
    """从命令行和环境变量读取数据库连接参数。"""
    return {
        "host": args.db_host,
        "port": args.db_port,
        "user": args.db_user,
        "password": os.environ.get("SPIDER_DB_PASSWORD", ""),
        "database": args.db_name,
    }


def parse_goods_ids(raw_values: Sequence[str]) -> List[int]:
    """解析可重复传入的商品 ID 参数，拒绝非正整数。"""
    result: List[int] = []
    for raw_value in raw_values:
        for item in str(raw_value).split(","):
            value = item.strip()
            if not value:
                continue
            if not value.isdigit() or int(value) <= 0:
                raise ValueError(f"商品 ID 无效：{value}")
            result.append(int(value))
    return sorted(set(result))


def write_report(path: str, plans: Sequence[RepairPlan], stats: RepairStats) -> None:
    """写入不包含令牌的 JSON 修复报告。"""
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "stats": {
            "matched_goods": stats.matched_goods,
            "unique_goods": stats.unique_goods,
            "planned": stats.planned,
            "unchanged": stats.unchanged,
            "skipped": stats.skipped,
            "applied": stats.applied,
            "verified": stats.verified,
        },
        "items": [
            {
                "goods_id": plan.match.goods_id,
                "goods_name": plan.match.goods_name,
                "source_id": plan.match.source_id,
                "source_title": plan.match.source_title,
                "source_url": plan.match.source_url,
                "shared_images": plan.match.shared_images,
                "candidate_count": plan.match.candidate_count,
                "old_sizes": list(plan.old_sizes),
                "new_sizes": list(plan.new_sizes),
                "changed": plan.changed,
                "reason": plan.reason,
                "skip_reason": plan.skip_reason,
            }
            for plan in plans
        ],
    }
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    """构造尺寸修复命令行参数。"""
    parser = argparse.ArgumentParser(description="从 From Switch 采集来源修复历史商品尺寸")
    parser.add_argument("--apply", action="store_true", help="确认后调用商品尺寸接口写入；默认只读预览")
    parser.add_argument("--goods-id", action="append", default=[], help="只修复指定商品 ID，可重复或用逗号分隔")
    parser.add_argument("--brand-id", type=int, default=DEFAULT_BRAND_ID, help=f"品牌 ID，默认 {DEFAULT_BRAND_ID}")
    parser.add_argument("--min-shared-images", type=int, default=DEFAULT_MIN_SHARED_IMAGES, help="来源匹配所需最少共享图片数")
    parser.add_argument("--api-base-url", default=os.environ.get("DOGDOGDOLL_API_BASE_URL", DEFAULT_API_BASE_URL))
    parser.add_argument("--api-timeout", type=float, default=float(os.environ.get("DOGDOGDOLL_API_TIMEOUT", DEFAULT_API_TIMEOUT)))
    parser.add_argument("--report-file", default="", help="JSON 报告路径，缺省写入 /tmp")
    parser.add_argument("--db-host", default=os.environ.get("SPIDER_DB_HOST", "222.186.135.83"))
    parser.add_argument("--db-port", type=int, default=int(os.environ.get("SPIDER_DB_PORT", "3306")))
    parser.add_argument("--db-user", default=os.environ.get("SPIDER_DB_USER", "sukitime_remote"))
    parser.add_argument("--db-name", default=os.environ.get("SPIDER_DB_NAME", "sukitime"))
    parser.add_argument("--log-file", default="", help="日志文件路径，缺省写入 /tmp")
    return parser


def configure_logging(path: str) -> logging.Logger:
    """配置控制台和文件日志。"""
    logger = logging.getLogger("repair_brand_spider_145_sizes")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)
    log_path = path or f"/tmp/repair_brand_spider_145_sizes_{datetime.now():%Y%m%d_%H%M%S}.log"
    try:
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
    except OSError as exc:
        logger.warning("日志文件不可写，仅保留控制台日志：%s", exc)
    else:
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


def main(argv: Optional[Sequence[str]] = None) -> int:
    """执行只读预览或显式确认的商品尺寸修复。"""
    args = build_parser().parse_args(argv)
    logger = configure_logging(args.log_file)
    if args.min_shared_images < 1:
        logger.error("--min-shared-images 必须大于 0")
        return 1
    if args.api_timeout <= 0 or not math.isfinite(args.api_timeout):
        logger.error("--api-timeout 必须是正数")
        return 1
    try:
        goods_ids = parse_goods_ids(args.goods_id)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    report_file = args.report_file or f"/tmp/repair_brand_spider_145_sizes_{datetime.now():%Y%m%d_%H%M%S}.json"
    database: Optional[RepairDatabase] = None
    api_client: Optional[AdminGoodsSizeClient] = None
    stats = RepairStats()
    plans: List[RepairPlan] = []
    try:
        database = RepairDatabase(logger=logger, **load_db_config(args))
        matches = database.list_source_matches(args.brand_id, DEFAULT_ORIGIN_TYPE, goods_ids)
        stats.matched_goods = len({match.goods_id for match in matches})
        matches = group_matches(matches)
        stats.unique_goods = len({match.goods_id for match in matches if match.candidate_count == 1})
        size_rows = database.load_size_rows(sorted({match.goods_id for match in matches}))
        hydrated_matches = [
            SourceMatch(**{**match.__dict__, "size_rows": size_rows.get(match.goods_id, tuple())})
            for match in matches
        ]
        logger.info(
            "发现 From Switch 脏尺寸商品：goods=%d，唯一来源商品=%d，来源候选=%d",
            stats.matched_goods,
            stats.unique_goods,
            len(matches),
        )

        for match in hydrated_matches:
            plan = build_repair_plan(match, args.min_shared_images)
            plans.append(plan)
            if plan.skip_reason:
                stats.skipped += 1
                stats.failures.append({"goods_id": str(match.goods_id), "reason": plan.skip_reason})
                logger.warning(
                    "跳过：goods_id=%d，source_id=%d，name=%s，reason=%s",
                    match.goods_id,
                    match.source_id,
                    match.goods_name,
                    plan.skip_reason,
                )
            elif plan.changed:
                stats.planned += 1
                logger.info(
                    "%s：goods_id=%d，source_id=%d，共享图片=%d，旧=%s，新=%s",
                    plan.reason,
                    match.goods_id,
                    match.source_id,
                    match.shared_images,
                    list(plan.old_sizes),
                    list(plan.new_sizes),
                )
            else:
                stats.unchanged += 1
                logger.info(
                    "无需修改：goods_id=%d，source_id=%d，尺寸=%s",
                    match.goods_id,
                    match.source_id,
                    list(plan.new_sizes),
                )

        if args.apply:
            api_client = AdminGoodsSizeClient(args.api_base_url, os.environ.get("DOGDOGDOLL_ADMIN_TOKEN", "").strip(), args.api_timeout, logger)
            for plan in plans:
                if not plan.changed:
                    continue
                try:
                    api_client.patch_sizes(plan.match.goods_id, plan.new_sizes)
                    stats.applied += 1
                    logger.info(
                        "已修复商品尺寸：goods_id=%d，source_id=%d，尺寸=%s",
                        plan.match.goods_id,
                        plan.match.source_id,
                        list(plan.new_sizes),
                    )
                except (RuntimeError, requests.RequestException) as exc:
                    stats.failures.append({"goods_id": str(plan.match.goods_id), "reason": str(exc)})
                    logger.error("商品尺寸修复失败：goods_id=%d，reason=%s", plan.match.goods_id, exc)

            state = database.load_size_state([plan.match.goods_id for plan in plans if plan.changed and not any(
                failure.get("goods_id") == str(plan.match.goods_id) for failure in stats.failures
            )])
            for plan in plans:
                if not plan.changed:
                    continue
                if state.get(plan.match.goods_id) == plan.new_sizes:
                    stats.verified += 1
                else:
                    stats.failures.append({"goods_id": str(plan.match.goods_id), "reason": "PATCH 返回成功但数据库核对不一致"})
                    logger.error("数据库核对不一致：goods_id=%d，期望=%s，实际=%s", plan.match.goods_id, list(plan.new_sizes), list(state.get(plan.match.goods_id, ())))
            logger.info("修复写入完成：applied=%d，verified=%d", stats.applied, stats.verified)
        else:
            logger.info("当前为只读预览，未调用 PATCH；如确认报告无误，请加 --apply")

        write_report(report_file, plans, stats)
        logger.info(
            "修复任务完成：matched=%d，unique=%d，planned=%d，unchanged=%d，skipped=%d，applied=%d，verified=%d，failures=%d，report=%s",
            stats.matched_goods,
            stats.unique_goods,
            stats.planned,
            stats.unchanged,
            stats.skipped,
            stats.applied,
            stats.verified,
            len(stats.failures),
            report_file,
        )
        return 2 if stats.failures else 0
    except (OSError, pymysql.MySQLError, RuntimeError, requests.RequestException) as exc:
        logger.error("修复任务终止：%s", exc)
        return 1
    finally:
        if api_client is not None:
            api_client.close()
        if database is not None:
            database.close()


if __name__ == "__main__":
    raise SystemExit(main())
