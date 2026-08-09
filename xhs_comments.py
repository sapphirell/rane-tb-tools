#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""通过浏览器页面采集小红书笔记评论。

这个入口只读取浏览器已经渲染出的公开内容，不调用未公开接口，也不读取
Cookie、密码或验证码。评论区需要登录时，用户应在打开的浏览器中手动完成
登录后再继续采集。

运行示例::

    python xhs_comments.py --url "https://www.xiaohongshu.com/explore/<note_id>" \
        --output ./output/xhs_comments_<note_id>.json

输出中的 ``comments`` 是扁平列表，每条记录都保留父评论关系，便于导入
数据库或后续按楼层重建；``floors`` 是同一批数据的楼层嵌套表示。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import time
import urllib.parse
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


LOGGER = logging.getLogger("xhs_comments")

# 评论列表在不同版本页面中使用过这些容器名称，选择器按稳定的语义类名排列。
COMMENT_CONTAINER_SELECTOR = ".comments-container, [class*='comments-container']"

# 页面请求/滚动之间必须留出间隔，避免形成高频请求。
DEFAULT_PAGE_WAIT_SECONDS = 1.0
DEFAULT_MAX_ROUNDS = 240
DEFAULT_IDLE_ROUNDS = 8


def normalize_note_url(original_url: str) -> str:
    """将笔记地址统一为 explore 地址并保留必要的 xsec 参数。"""
    parsed = urllib.parse.urlparse(str(original_url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("小红书地址无效，必须包含 https://www.xiaohongshu.com/")
    host = parsed.netloc.split(":", 1)[0].lower()
    if host != "xiaohongshu.com" and not host.endswith(".xiaohongshu.com"):
        raise ValueError("只支持小红书笔记地址")
    path_parts = [part for part in parsed.path.split("/") if part]
    note_id = ""
    if len(path_parts) >= 4 and path_parts[0:2] == ["user", "profile"]:
        note_id = path_parts[3]
        parsed = parsed._replace(path=f"/explore/{note_id}")
    elif path_parts and path_parts[0] == "explore":
        note_id = path_parts[1] if len(path_parts) > 1 else ""
        parsed = parsed._replace(path=f"/explore/{note_id}")
    parsed = parsed._replace(query=parsed.query.replace("&amp;", "&"), fragment="")
    if not note_id:
        raise ValueError("小红书地址中缺少笔记 ID")
    return urllib.parse.urlunparse(parsed)


def extract_note_id(note_url: str) -> str:
    """从 explore 地址中提取笔记 ID。"""
    path_parts = [part for part in urllib.parse.urlparse(note_url).path.split("/") if part]
    if len(path_parts) >= 2 and path_parts[0] == "explore":
        return path_parts[1]
    return ""


def parse_count(raw_value: Any) -> int:
    """将页面上的 10+、1.2万、3.4k 等展示值转换为整数。"""
    text = str(raw_value or "").strip().replace(",", "").replace("，", "")
    if not text or text in {"赞", "点赞"}:
        return 0
    match = re.search(r"(\d+(?:\.\d+)?)\s*([万wWkK]?)", text)
    if not match:
        return 0
    value = float(match.group(1))
    unit = match.group(2).lower()
    if unit == "万":
        value *= 10000
    elif unit == "w":
        value *= 10000
    elif unit == "k":
        value *= 1000
    return int(value)


def split_time_location(raw_value: Any) -> Tuple[str, str]:
    """拆分小红书评论中的时间和地区，例如 ``5小时前 江苏``。"""
    text = re.sub(r"\s+", " ", str(raw_value or "")).strip()
    if not text:
        return "", ""
    match = re.match(r"^(.*?)\s+([\u4e00-\u9fff]{2,8})$", text)
    if not match:
        return text, ""
    return match.group(1).strip(), match.group(2).strip()


def stable_comment_key(comment: Dict[str, Any]) -> str:
    """为没有暴露 comment_id 的 DOM 评论生成去重键。"""
    source_id = str(comment.get("comment_id") or "").strip()
    if source_id:
        return f"id:{source_id}"
    parts = (
        str(comment.get("parent_comment_id") or ""),
        str(comment.get("parent_comment_key") or ""),
        str(comment.get("user_id") or ""),
        str(comment.get("user_name") or ""),
        str(comment.get("create_time") or ""),
        str(comment.get("content") or ""),
    )
    digest = hashlib.sha1("\x1f".join(parts).encode("utf-8")).hexdigest()
    return f"dom:{digest}"


def normalize_comment_record(raw: Dict[str, Any], note_id: str, *, is_reply: bool,
                             parent_comment_id: str = "", parent_comment_key: str = "") -> Optional[Dict[str, Any]]:
    """清洗浏览器脚本返回的一条评论，并补全统一字段。"""
    if not isinstance(raw, dict):
        return None
    user_name = str(raw.get("user_name") or "").strip()
    content = str(raw.get("content") or "").strip()
    if not user_name or not content:
        return None
    create_time, location = split_time_location(raw.get("raw_time") or raw.get("create_time"))
    record: Dict[str, Any] = {
        "note_id": note_id,
        "comment_id": str(raw.get("comment_id") or "").strip(),
        "parent_comment_id": str(parent_comment_id or raw.get("parent_comment_id") or "").strip(),
        "parent_comment_key": str(parent_comment_key or raw.get("parent_comment_key") or "").strip(),
        "user_id": str(raw.get("user_id") or "").strip(),
        "user_name": user_name[:120],
        "content": content[:3000],
        "create_time": create_time[:80],
        "location": str(raw.get("location") or location).strip()[:80],
        "like_count": parse_count(raw.get("like_count")),
        "like_count_display": str(raw.get("like_count_display") or "").strip()[:40],
        "reply_count": parse_count(raw.get("reply_count")),
        "reply_to_user_id": str(raw.get("reply_to_user_id") or "").strip(),
        "reply_to_user_name": str(raw.get("reply_to_user_name") or "").strip()[:120],
        "is_reply": bool(is_reply),
        "is_author": bool(raw.get("is_author")),
    }
    record["comment_key"] = stable_comment_key(record)
    return record


def flatten_comment_floors(floors: Iterable[Dict[str, Any]], note_id: str) -> List[Dict[str, Any]]:
    """将脚本返回的楼层结构展平，同时保留父评论关系并去重。"""
    result: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for floor in floors or []:
        if not isinstance(floor, dict):
            continue
        parent = normalize_comment_record(floor.get("comment") or {}, note_id, is_reply=False)
        if not parent:
            continue
        parent_key = stable_comment_key(parent)
        if parent_key not in seen:
            seen.add(parent_key)
            result.append(parent)
        parent_id = parent.get("comment_id") or ""
        for raw_reply in floor.get("replies") or []:
            reply = normalize_comment_record(
                raw_reply or {},
                note_id,
                is_reply=True,
                parent_comment_id=parent_id,
                parent_comment_key=parent_key,
            )
            if not reply:
                continue
            reply_key = stable_comment_key(reply)
            if reply_key in seen:
                continue
            seen.add(reply_key)
            result.append(reply)
    return result


def group_comment_floors(comments: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """将扁平评论按真实评论 ID 或采集键重新组织为楼层结构。"""
    floors: List[Dict[str, Any]] = []
    by_parent_key: Dict[str, Dict[str, Any]] = {}
    for comment in comments:
        if comment.get("is_reply"):
            parent_ref = str(comment.get("parent_comment_id") or comment.get("parent_comment_key") or "")
            parent = by_parent_key.get(parent_ref)
            if parent is not None:
                parent.setdefault("replies", []).append(comment)
                continue
        floor = {"comment": comment, "replies": []}
        floors.append(floor)
        comment_id = str(comment.get("comment_id") or "")
        if comment_id:
            by_parent_key[comment_id] = floor
        comment_key = str(comment.get("comment_key") or "")
        if comment_key:
            by_parent_key[comment_key] = floor
    return floors


def merge_comment_snapshots(snapshots: Iterable[Sequence[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """合并展开前后的 DOM 快照，避免展开按钮消失后丢失回复数。"""
    result: List[Dict[str, Any]] = []
    positions: Dict[str, int] = {}
    for snapshot in snapshots:
        for raw_comment in snapshot or []:
            key = stable_comment_key(raw_comment)
            position = positions.get(key)
            if position is None:
                positions[key] = len(result)
                result.append(dict(raw_comment))
                continue
            current = result[position]
            for field in (
                    "comment_id", "parent_comment_id", "parent_comment_key", "user_id",
                    "reply_to_user_id", "reply_to_user_name"
            ):
                if not current.get(field) and raw_comment.get(field):
                    current[field] = raw_comment[field]
            for field in ("like_count", "reply_count"):
                current[field] = max(parse_count(current.get(field)), parse_count(raw_comment.get(field)))
            if raw_comment.get("like_count_display"):
                current["like_count_display"] = raw_comment["like_count_display"]
    return result


EXTRACT_COMMENTS_SCRIPT = r"""
const compact = (value) => String(value || '').replace(/\s+/g, ' ').trim();
const first = (root, selectors) => {
  for (const selector of selectors) {
    const node = root && root.querySelector(selector);
    if (node) return node;
  }
  return null;
};
const textOf = (node) => compact(node ? (node.innerText || node.textContent) : '');
const attr = (node, names) => {
  if (!node) return '';
  for (const name of names) {
    const value = node.getAttribute(name);
    if (value) return compact(value);
  }
  return '';
};
const userIdFromHref = (href) => {
  const match = String(href || '').match(/\/user\/profile\/([^/?#]+)/);
  return match ? match[1] : '';
};
const parseReplyTarget = (item, userName) => {
  const target = first(item, [
    '.reply-to a', '.reply-user a', '.reply-target a',
    '[class*="reply-to"] a', '[class*="reply-user"] a', '[class*="target"] a'
  ]);
  if (target) return {name: textOf(target), id: userIdFromHref(target.getAttribute('href'))};
  const content = first(item, ['.content', '[class*="content"]']);
  const links = Array.from(item.querySelectorAll('a[href*="/user/profile/"]'));
  for (const link of links) {
    const name = textOf(link);
    if (name && name !== userName) return {name, id: userIdFromHref(link.getAttribute('href'))};
  }
  const plain = textOf(content);
  const match = plain.match(/^回复\s+@?([^:：\s]+)\s*[:：]/);
  return match ? {name: match[1], id: ''} : {name: '', id: ''};
};
const readComment = (item) => {
  if (!item) return null;
  const nameNode = first(item, ['.author .name', '.author [class*="name"]', '[class*="author"] [class*="name"]']);
  const contentNode = first(item, ['.content .note-text', '.content [class*="note-text"]', '.content', '[class*="content"]']);
  const userLink = first(item, ['.author a[href*="/user/profile/"]', 'a[href*="/user/profile/"]']);
  const dateNode = first(item, ['.info .date', '.info [class*="date"]', '[class*="date"]']);
  const name = textOf(nameNode);
  const content = textOf(contentNode);
  if (!name || !content) return null;
  const rawTime = textOf(dateNode);
  const likeNode = first(item, [
    '.like-active .count', '.like .count', '[class*="like"] .count',
    '[class*="like-count"]', '[aria-label*="赞"]'
  ]);
  const expandNode = Array.from(item.querySelectorAll('.show-more, button, [role="button"]'))
    .find((node) => /展开|更多回复|条回复/.test(textOf(node)));
  const replyTarget = parseReplyTarget(item, name);
  const authorTag = Array.from(item.querySelectorAll('.author .tag, .author [class*="tag"]'))
    .some((node) => textOf(node) === '作者');
  return {
    comment_id: attr(item, ['data-comment-id', 'data-commentid', 'data-id', 'comment-id']) ||
      attr(item.parentElement, ['data-comment-id', 'data-commentid', 'data-id', 'comment-id']),
    user_id: userIdFromHref(userLink ? userLink.getAttribute('href') : ''),
    user_name: name,
    content,
    raw_time: rawTime,
    like_count: textOf(likeNode),
    like_count_display: textOf(likeNode),
    reply_count: expandNode ? textOf(expandNode) : '',
    reply_to_user_id: replyTarget.id,
    reply_to_user_name: replyTarget.name,
    is_author: authorTag
  };
};
const container = document.querySelector(".comments-container, [class*='comments-container']");
if (!container) return {floors: [], total_comments: 0};
const floors = [];
const parentNodes = Array.from(container.querySelectorAll('.parent-comment, [class*="parent-comment"]'));
for (const parentNode of parentNodes) {
  const parentItem = parentNode.querySelector(':scope > .comment-item') || parentNode.querySelector('.comment-item');
  const comment = readComment(parentItem);
  if (!comment) continue;
  const replyCountNode = Array.from(parentNode.querySelectorAll('.show-more, button, [role="button"]'))
    .find((node) => /展开|更多回复|条回复/.test(textOf(node)));
  if (replyCountNode) comment.reply_count = textOf(replyCountNode);
  const replies = [];
  const replyNodes = Array.from(parentNode.querySelectorAll('.reply-container .comment-item-sub, .reply-container [class*="comment-item-sub"]'));
  for (const replyNode of replyNodes) {
    const reply = readComment(replyNode);
    if (reply) replies.push(reply);
  }
  floors.push({comment, replies});
}
const containerText = textOf(container);
const totalMatch = containerText.match(/(?:共\s*)?[\d,.万wWkK]+\s*条评论/);
const headerCount = first(container, ['.comments-header .count', '.comment-header .count', '[class*="comment-header"] [class*="count"]']);
const totalText = totalMatch ? totalMatch[0] : textOf(headerCount);
return {floors, total_comments: totalText};
"""


SCROLL_COMMENTS_SCRIPT = r"""
const container = document.querySelector(".comments-container, [class*='comments-container']");
const candidates = container ? [container, ...container.querySelectorAll('*')] : [];
const scrollables = candidates.filter((node) => node.scrollHeight > node.clientHeight + 8);
const commentScrollable = scrollables.find((node) => /comment/i.test(String(node.className || '')));
const scrollable = commentScrollable || scrollables[0];
if (!scrollable) {
  return {before: 0, after: 0, height: 0, viewport: 0, at_bottom: true};
}
const target = scrollable;
const before = target.scrollTop;
target.scrollTop = target.scrollHeight;
return {
  before,
  after: target.scrollTop,
  height: target.scrollHeight,
  viewport: target.clientHeight,
  at_bottom: target.scrollTop + target.clientHeight >= target.scrollHeight - 8
};
"""


EXPAND_REPLIES_SCRIPT = r"""
const container = document.querySelector(".comments-container, [class*='comments-container']");
if (!container) return {clicked: 0, remaining: 0};
let clicked = 0;
const nodes = Array.from(container.querySelectorAll('.show-more, button, [role="button"]'));
for (const node of nodes) {
  const text = String(node.innerText || node.textContent || '').replace(/\s+/g, ' ').trim();
  if (!/展开|更多回复|条回复/.test(text) || /^回复$/.test(text)) continue;
  const style = window.getComputedStyle(node);
  if (style.display === 'none' || style.visibility === 'hidden') continue;
  try {
    node.click();
    clicked += 1;
  } catch (_) {}
  if (clicked >= 40) break;
}
const remaining = nodes.filter((node) => /展开|更多回复|条回复/.test(String(node.innerText || node.textContent || ''))).length;
return {clicked, remaining};
"""


class XHSCommentCrawler:
    """使用一个可见 Selenium 浏览器采集公开评论。"""

    def __init__(self, driver: Optional[Any] = None, *, page_wait_seconds: float = DEFAULT_PAGE_WAIT_SECONDS,
                 navigate: Optional[Callable[[str], None]] = None,
                 check_stop: Optional[Callable[[], None]] = None,
                 logger: Optional[logging.Logger] = None):
        self._owns_driver = driver is None
        self.driver = driver or self._create_driver()
        self.page_wait_seconds = max(0.3, float(page_wait_seconds))
        self._navigate = navigate or self.driver.get
        self._check_stop = check_stop or (lambda: None)
        self.logger = logger or LOGGER

    @staticmethod
    def _create_driver() -> Any:
        """创建可供用户手动登录的浏览器，不启用无头模式。"""
        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument("--disable-blink-features=AutomationControlled")
        return webdriver.Chrome(options=options)

    def close(self) -> None:
        """关闭由本类创建的浏览器。传入外部 driver 时由调用方负责关闭。"""
        if self._owns_driver:
            try:
                self.driver.quit()
            except WebDriverException:
                LOGGER.debug("关闭浏览器时页面已经退出", exc_info=True)

    def _wait_for_page(self) -> None:
        """等待页面基础结构出现；登录弹窗由用户在浏览器中处理。"""
        WebDriverWait(self.driver, 30).until(
            lambda current: current.find_elements(By.TAG_NAME, "body")
        )
        try:
            WebDriverWait(self.driver, 30).until(
                lambda current: current.find_elements(By.CSS_SELECTOR, COMMENT_CONTAINER_SELECTOR)
            )
        except TimeoutException as exc:
            raise RuntimeError("评论区没有加载出来，请确认页面可访问且已在浏览器中完成登录") from exc

    def _expand_visible_replies(self) -> int:
        """点击当前已渲染评论中的楼中楼展开控件。"""
        self._check_stop()
        result = self.driver.execute_script(EXPAND_REPLIES_SCRIPT) or {}
        return int(result.get("clicked") or 0)

    def _scroll_comments(self) -> Dict[str, Any]:
        """滚动评论容器到底部，触发下一批评论加载。"""
        self._check_stop()
        return self.driver.execute_script(SCROLL_COMMENTS_SCRIPT) or {}

    def _extract_current(self, note_id: str) -> Tuple[List[Dict[str, Any]], int]:
        """读取当前 DOM 中已经渲染的评论和页面宣称的总评论数。"""
        payload = self.driver.execute_script(EXTRACT_COMMENTS_SCRIPT) or {}
        floors = payload.get("floors") if isinstance(payload, dict) else []
        comments = flatten_comment_floors(floors or [], note_id)
        return comments, parse_count(payload.get("total_comments") if isinstance(payload, dict) else 0)

    def crawl(self, note_url: str, *, max_comments: int = 0, max_rounds: int = DEFAULT_MAX_ROUNDS,
              idle_rounds: int = DEFAULT_IDLE_ROUNDS) -> Dict[str, Any]:
        """采集单篇笔记评论，返回元数据、扁平评论和楼层结构。"""
        normalized_url = normalize_note_url(note_url)
        note_id = extract_note_id(normalized_url)
        self._check_stop()
        self._navigate(normalized_url)
        self._wait_for_page()

        merged: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        total_comments = 0
        idle = 0
        rounds = max(1, min(int(max_rounds), 1000))
        for round_index in range(rounds):
            self._check_stop()
            before_expand, before_total = self._extract_current(note_id)
            expanded = self._expand_visible_replies()
            after_expand, page_total = self._extract_current(note_id)
            current = merge_comment_snapshots((before_expand, after_expand))
            total_comments = max(total_comments, before_total, page_total)
            added = 0
            for comment in current:
                key = stable_comment_key(comment)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(comment)
                added += 1
                if max_comments > 0 and len(merged) >= max_comments:
                    break

            if max_comments > 0 and len(merged) >= max_comments:
                break
            if total_comments > 0 and len(merged) >= total_comments:
                break

            scroll_state = self._scroll_comments()
            changed = added > 0 or expanded > 0
            at_bottom = bool(scroll_state.get("at_bottom"))
            idle = 0 if changed else idle + 1
            self.logger.info(
                "评论采集第 %d 轮：新增 %d，已展开 %d，累计 %d%s",
                round_index + 1,
                added,
                expanded,
                len(merged),
                f"/{total_comments}" if total_comments else "",
            )
            if idle >= max(1, int(idle_rounds)) and at_bottom:
                break
            time.sleep(self.page_wait_seconds)

        return {
            "note_id": note_id,
            "note_url": normalized_url,
            "total_comments": total_comments,
            "collected_comments": len(merged),
            "comments": merged,
            "floors": group_comment_floors(merged),
        }


def write_result(result: Dict[str, Any], output_path: Path) -> None:
    """以 UTF-8 JSON 写入采集结果。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数。"""
    parser = argparse.ArgumentParser(description="采集小红书笔记公开评论及楼中楼")
    parser.add_argument("--url", required=True, help="小红书笔记地址")
    parser.add_argument("--output", type=Path, help="JSON 输出路径，默认写入 spiders/output")
    parser.add_argument("--max-comments", type=int, default=0, help="最多采集多少条，0 表示不限制")
    parser.add_argument("--max-rounds", type=int, default=DEFAULT_MAX_ROUNDS, help="最多滚动轮数")
    parser.add_argument("--wait", type=float, default=DEFAULT_PAGE_WAIT_SECONDS, help="每轮滚动间隔秒数")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """命令行入口。"""
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")
    normalized_url = normalize_note_url(args.url)
    note_id = extract_note_id(normalized_url)
    output_path = args.output or Path("output") / f"xhs_comments_{note_id}.json"
    crawler = XHSCommentCrawler(page_wait_seconds=args.wait)
    try:
        result = crawler.crawl(
            normalized_url,
            max_comments=max(0, args.max_comments),
            max_rounds=max(1, args.max_rounds),
        )
        write_result(result, output_path)
        LOGGER.info("评论采集完成：%s（%d/%d）", output_path, result["collected_comments"], result["total_comments"])
        return 0
    finally:
        crawler.close()


if __name__ == "__main__":
    raise SystemExit(main())
