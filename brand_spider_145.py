#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""分类已有 From Switch DOLL 采集记录，并可接入现有流程创建商品。

脚本默认从 spider_log 读取 From Switch 的未处理记录，直接调用管理后台现有 AI 分类
SSE 流程，并将每条记录按商品调用现有商品创建接口，不创建贩售记录或新闻。只有显式
传入 --collect-new 时，才会访问 From Switch 分类页并采集新详情。
脚本不执行数据库结构变更，也不依赖 GUI 的初始化逻辑。采集模式下如果站点返回验证码
或其它非商品页面，脚本会记录明确错误，不会用不完整数据填充记录。
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import html as std_html
import json
import logging
import math
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit, urlunsplit

import pymysql
import requests
from lxml import html as lxml_html


# 目标站点的固定信息，商品采集器不从页面输入改变品牌范围。
BASE_URL = "https://from-switch.com"
DEFAULT_CATEGORY_URL = f"{BASE_URL}/product/list.html?cate_no=25"
DEFAULT_CATEGORY_NO = 25
DEFAULT_BRAND_ID = 145
DEFAULT_ORIGIN_TYPE = "from_switch"
DEFAULT_MSG_TYPE = 0  # 新采集内容类型，沿用 spider_log 的原始记录语义。
DEFAULT_STATUS = 1  # 商品页没有发布时间，使用“未处理”避免被旧日志维护误清理。
DEFAULT_AI_PROVIDER = "chatgpt"  # 自动分类固定使用管理后台的 ChatGPT-Luna 来源。
DEFAULT_AI_MODEL = "gpt-5.6-luna"  # 自动分类固定使用用户指定的 GPT-5.6 Luna 模型。
DEFAULT_API_BASE_URL = "http://localhost:8080"  # 管理后台开发环境的后端 API 地址。
# 给图片准备和模型首个响应留出足够时间；后端 AI 流超时也按 5 分钟对齐。
DEFAULT_API_TIMEOUT = 360.0

# From Switch 每条详情都会带上的编辑器废图；只按域名和路径精确排除，不影响其它站点同名文件。
FROM_SWITCH_WASTE_IMAGE_URL = (
    "https://from-switch.com/web/upload/category/editor/2020/03/02/"
    "4ec60c80c2204fa0e8a3f2f2d357c396.gif"
)

# From Switch 用于表示“没有选项”的英文占位值；它不能作为尺寸、颜色或其它商品字段写入。
FROM_SWITCH_EMPTY_OPTION_LABELS = frozenset({"no option", "no options", "none"})
# 只有明确的尺寸选项组才允许作为商品尺寸来源，避免把 Body painting、Chest parts 等组误当成尺寸。
FROM_SWITCH_SIZE_OPTION_GROUP_PATTERN = (
    r"^(?:size|sizes|doll\s+size|body\s+size|fit|dimension(?:s)?|"
    r"尺寸|体型|身体尺寸|适配体型|适配身体)$"
)

# AI 返回的尺寸可能只有细分类名称；自动创建商品前必须还原为系统的父级/细分类组合。
# 该表与后端 config.Sizes 保持一致，避免把“65成男”等细分类直接写入 goods_size。
FROM_SWITCH_SIZE_DETAILS_BY_CATEGORY: Dict[str, Tuple[str, ...]] = {
    "一分": ("普通一分",),
    "二分": ("普通二分",),
    "三分": ("普通三分", "DD", "SD10", "SD13", "SD16", "SD17", "SDGR", "62成男", "62成女"),
    "四分": ("普通四分", "胖体四分", "巨婴", "大四分", "特四", "特体四分", "MSD", "MDD"),
    "五分": ("普通五分",),
    "六分": (
        "普通六分",
        "胖体六分",
        "特体六分",
        "小六分",
        "棍六",
        "高六",
        "YoSD",
        "AZone-M",
        "AZone-S",
        "AZone-XS",
        "OB21",
        "OB22",
        "OB23",
        "OB24",
        "OB25",
        "OB26",
        "OB27",
    ),
    "八分": ("普通八分", "胖体八分"),
    "十二分": ("普通十二分", "胖体十二分", "OB11"),
    "其它大尺寸": (
        "大女",
        "65成男",
        "68成男",
        "68成女",
        "70+普成男",
        "70+壮成男",
        "70+成女",
        "72普成男",
        "72壮成男",
        "72成女",
        "73普成男",
        "73壮成男",
        "73成女",
        "75普成男",
        "75壮成男",
        "76普成男",
        "76壮成男",
        "76成女",
        "75成女",
        "77普成男",
        "77壮成男",
        "78普成男",
        "78壮成男",
        "80普成男",
        "80壮成男",
        "83普成男",
        "83壮成男",
    ),
}
FROM_SWITCH_SIZE_DETAIL_TO_CATEGORY: Dict[str, str] = {
    detail: category
    for category, details in FROM_SWITCH_SIZE_DETAILS_BY_CATEGORY.items()
    for detail in details
}
FROM_SWITCH_SIZE_CATEGORY_ALIASES: Dict[str, Tuple[str, str]] = {
    "大尺寸": ("其它大尺寸", ""),
    "75成男": ("其它大尺寸", ""),
    "叔体": ("其它大尺寸", ""),
    "普六": ("六分", "普通六分"),
    "普6": ("六分", "普通六分"),
    "小六": ("六分", "小六分"),
    "小6": ("六分", "小六分"),
    "胖六": ("六分", "胖体六分"),
    "胖6": ("六分", "胖体六分"),
    "特六": ("六分", "特体六分"),
    "特6": ("六分", "特体六分"),
    "特六分": ("六分", "特体六分"),
    "特6分": ("六分", "特体六分"),
    "特体六": ("六分", "特体六分"),
}

# 玩家对照表中能够从截图确认的英文官名与中文名。截图中被遮挡的行不猜测、不写入白名单。
FROM_SWITCH_CHINESE_NAME_MAPPING: Tuple[Tuple[str, str], ...] = (
    ("CHOYO robin", "瞳谣R（开嘴）"),
    ("EUNJO Velvet", "银照v（开嘴）"),
    ("JIUN", "至云"),
    ("GARAM", "佳蓝"),
    ("DONGGYEONG", "冬镜"),
    ("YUNSEOL somnia", "尹雪 半眠"),
    ("Cedric", "塞德里克"),
    ("EUNJO", "银照"),
    ("LIS", "LIS"),
    ("WUJIN", "优珍"),
    ("CHAGYEONG", "借景"),
    ("YUNSEOL: Tokki", "尹雪 兔子"),
    ("MUHYUL", "雾灿"),
    ("YIDO masquerade", "理度m（开嘴）"),
    ("YUNSEOL", "尹雪"),
    ("KAIEN", "卡宴"),
    ("HWAHUI: Hiver", "华熙 hiver"),
    ("SHATI", "SHATI"),
    ("CHACHA", "CHACHA"),
    ("LILI", "LILI"),
    ("TAEO", "鹰鳥"),
    ("CHOYO somnia", "瞳谣 半眠"),
    ("SANHONG", "山红"),
    ("DAMHYEON Crescent", "大炫 c"),
    ("TAERIN Eclipse", "血大湊"),
    ("SEHAN", "势澜"),
    ("YIWON", "李愿"),
    ("RYUZO Persona", "琉照 半眠"),
    ("SEOLROK: Hiver", "樰绿 Hiver"),
    ("RUAN", "樱岸"),
    ("CHOYO", "瞳谣"),
    ("YOZO", "遥潮"),
    ("TAERIN bijou", "大湊 闭嘴"),
    ("RION", "璃温"),
    ("SOSEO diamante", "小景 眠"),
    ("HWAYEON", "画缘"),
    ("GYOHA mobius", "皎河m（搞m）"),
    ("TAERIN", "大湊"),
    ("YIDO inferno", "理度 半眠"),
    ("SEOLROK somnia", "眠绿"),
    ("DAMHYEON", "大炫"),
    ("YIDO", "理度"),
)

# 英文官名与中文名的一对一关系；运行时不仅校验中文名在白名单中，还会校验配对及来源文本证据。
FROM_SWITCH_CHINESE_NAME_BY_OFFICIAL_NAME = dict(FROM_SWITCH_CHINESE_NAME_MAPPING)


def build_from_switch_name_mapping_prompt() -> str:
    """把已确认的 From Switch 英文官名与中文名关系渲染为临时提示词表格。"""
    return "\n".join(f"- {english} => {chinese}" for english, chinese in FROM_SWITCH_CHINESE_NAME_MAPPING)


# 该提示词由采集脚本随请求传给现有 spider-kimi-analyze-sse 流程，不复制 AI 分类逻辑。
FROM_SWITCH_CLASSIFICATION_PROMPT = f"""
【From Switch DOLL 临时任务规则】
本次输入来自 From Switch 英文站 DOLL 分类页。该分类页中的每一条详情都是娃娃、身体、假发、眼珠、服装或其它可售商品，
因此本次来源规则覆盖上层分类结果：必须按商品处理，并输出 category=3。即使标题或正文出现 sale、pre-order、release、available、
限量、发售、预售等销售时间或状态文字，也不能把它当作贩售通知。

本脚本对 From Switch 采集到的每条详情都必须继续创建商品；category 不是商品创建的跳过条件。
不要创建贩售记录，不要导入新闻，sale 必须返回 null。若 AI 返回了 1、2、4 或 9，仍须按照商品字段补全 details，脚本会继续调用商品创建接口。

请在 details 中额外返回字段：
- official_name：只能返回下面白名单左侧的一条完整英文官名；若无法明确匹配，必须返回空字符串 ""。
- chinese_name：只能从下面的“英文官名→中文名白名单”中选择一个中文名；不能翻译、联想、补全或自造。若标题、正文和图片无法确认是表中对应的英文官名，必须返回空字符串 ""。
- product_name：只返回来源商品的英文商品名或型号，不要在这里添加中文括号；创建商品时脚本以来源页面标题为基础，再追加通过校验的中文名。
- supported_sizes 只能填写真实的娃娃尺寸分类或明确尺寸详情；`No option`、`No options`、`None` 等“无选项”占位词必须返回空数组 []，不能作为尺寸写入。
- `Body painting`、`Chest parts`、`Skin Color` 等不是尺寸的选项组，其值也不能放入 supported_sizes；例如 `Hands`、`Hands+Foot` 是身体绘制选项，不是尺寸。
- 尺寸明确时，若标题、正文、规格组或图片能对应到支持尺寸列表中的规范小分类，必须把该小分类写入 `supported_sizes`，不能因为已识别父级就只返回父级；例如明确出现“大四分”时返回“大四分”。无法确认小分类时才返回父级，不要根据相似词或模糊图片编造细项。

official_name 与 chinese_name 必须来自白名单中的同一行；不能把一个官名和另一行的中文名拼在一起。无法同时确认这两个字段时，两者都返回空字符串。

对照时必须使用英文官名和型号的精确对应关系：
- 先匹配完整官名和变体，例如 `YIDO inferno` 只能对应“理度 半眠”，不能因为包含 YIDO 就改用其它理度变体。
- `YUNSEOL somnia`、`YUNSEOL: Tokki`、`YUNSEOL` 是三个不同的明确对应；`CHOYO robin`、`CHOYO somnia`、`CHOYO` 也分别对应不同记录。
- 冒号、大小写和多余空格可以视为排版差异；不能仅凭所属系列、图片风格或相似读音匹配。
- 官名后面出现页面固定的 `MAKE UP` 等销售展示后缀时，不改变前面的官名判断；其它无法确认的额外文字不要强行匹配。
- 表内同一官名（例如 JIUN）出现重复记录时，只返回同一个白名单中文名，不重复追加。
- 只有明确对应时才返回中文名；不相关时必须返回空字符串，宁可不加括号，也不能编造。

【英文官名→中文名白名单】
{build_from_switch_name_mapping_prompt()}

截图中有部分行被遮挡或无法清晰确认，本次不纳入白名单；不要根据缺失行自行补充新关系。
""".strip()

# AI 结果经过脚本白名单校验后，只有这些值可以出现在商品名末尾。
FROM_SWITCH_CHINESE_NAME_WHITELIST = frozenset(
    chinese for _, chinese in FROM_SWITCH_CHINESE_NAME_MAPPING
)

# HTTP 和详情页节流参数，避免短时间内重复请求目标站点。
DEFAULT_TIMEOUT = (10.0, 30.0)
DEFAULT_MAX_RETRIES = 3
DEFAULT_DETAIL_DELAY = 0.8
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# 详情正文中的操作节点不属于商品描述文本。
TEXT_SKIP_TAGS = {"script", "style", "noscript", "template"}
TEXT_BLOCK_TAGS = {
    "address",
    "article",
    "aside",
    "blockquote",
    "center",
    "dd",
    "div",
    "dl",
    "dt",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
}

# 商品价格和选项加价的展示文本解析规则。
PRICE_RE = re.compile(
    r"(?P<currency>[A-Z]{3})\s*(?P<amount>[0-9][0-9,]*(?:\.[0-9]+)?)",
    re.IGNORECASE,
)
SYMBOL_PRICE_RE = re.compile(
    r"(?P<symbol>[$€£¥])\s*(?P<amount>[0-9][0-9,]*(?:\.[0-9]+)?)",
    re.IGNORECASE,
)
OPTION_ADJUSTMENT_RE = re.compile(
    r"\(\s*\+\s*(?:(?P<currency>[A-Z]{3})\s*)?(?P<amount>[0-9][0-9,]*(?:\.[0-9]+)?)\s*\)",
    re.IGNORECASE,
)
PRODUCT_NO_FROM_CARD_RE = re.compile(r"anchorBoxId_(\d+)", re.IGNORECASE)
JS_ASSIGNMENT_RE_TEMPLATE = r"\bvar\s+{name}\s*=\s*(['\"])(.*?)\1\s*;"


class SpiderError(RuntimeError):
    """采集器可以识别并记录的业务错误。"""


class PageFetchError(SpiderError):
    """目标页面无法在有限次数内成功读取。"""


class PageParseError(SpiderError):
    """目标页面已读取，但缺少采集流程需要的页面结构。"""


class DatabaseError(SpiderError):
    """数据库连接或写入失败。"""


class APIError(SpiderError):
    """管理后台 API 返回失败或协议不完整。"""


class GoodsPayloadError(SpiderError):
    """AI 结果缺少创建商品所需的明确字段。"""


@dataclass
class ProductCard:
    """分类页上的商品卡片。"""

    product_no: str  # 商品编号，用于页内去重和详情页校验。
    title: str  # 分类页展示的商品名称，用作详情解析失败时的日志上下文。
    url: str  # 分类页给出的商品详情地址。
    cover_image: str  # 分类页封面地址，详情主图缺失时作为图片兜底。
    page_no: int  # 商品卡片来自分类页的页码。


@dataclass
class ProductOptionValue:
    """一个商品选项值。"""

    value: str  # Cafe24 option 的原始 value。
    label: str  # 去除末尾加价标记后的展示名称。
    raw_label: str  # 页面原始文本，保留站点原始写法。
    disabled: bool  # 页面是否将该选项标记为 disabled。
    price_adjustment: str = ""  # 从选项文本中解析出的加价，例如 USD 40.00。
    stock_data: Dict[str, Any] = field(default_factory=dict)  # 与 option value 对应的库存脚本数据。


@dataclass
class ProductOptionGroup:
    """一个商品选项组。"""

    name: str  # 选项组名称，例如 Skin、Size 或 Color。
    sort_no: int  # 页面 option_sort_no，用于保持站点顺序。
    required: bool  # 页面是否要求选择该选项。
    option_type: str  # Cafe24 option_type，常见值为 E 或 T。
    option_code: str  # Cafe24 选项组编号。
    values: List[ProductOptionValue] = field(default_factory=list)  # 组内实际可见选项。


@dataclass
class ProductSkuItem:
    """从 option_stock_data 中解析出的 SKU 或 SKU 组合。"""

    sku_code: str  # Cafe24 SKU/选项编码。
    option_values: List[str] = field(default_factory=list)  # 组合对应的原始选项值。
    option_names: List[str] = field(default_factory=list)  # 组合对应的原始选项组名称。
    option_price: str = ""  # 选项或组合价格/加价的原始值。
    stock_number: str = ""  # 页面脚本中的库存数量，可能为空。
    is_selling: str = ""  # 页面脚本中的销售状态。
    use_soldout: str = ""  # 页面脚本中的售罄控制状态。


@dataclass
class ProductDetail:
    """商品详情页提取结果。"""

    product_no: str  # 商品编号。
    title: str  # 商品名称。
    url: str  # 保存到 spider_log 的详情地址。
    canonical_url: str  # 页面 canonical 地址，没有时为空。
    price_display: str  # 页面原始价格展示文本。
    currency: str  # 解析出的币种代码或符号。
    price_value: str  # 解析出的价格数值文本，不做汇率换算。
    body_text: str  # #prdDetail 中清洗后的商品正文。
    images: List[str] = field(default_factory=list)  # 主图、附加图和正文图的去重地址。
    option_groups: List[ProductOptionGroup] = field(default_factory=list)  # 全部规格选项组。
    sku_items: List[ProductSkuItem] = field(default_factory=list)  # 组合 SKU 明细。


@dataclass
class CrawlStats:
    """一次运行的统计与失败清单。"""

    pages_visited: int = 0  # 实际处理的分类页数。
    last_page: int = 0  # 从第 1 页分页器解析出的最后页。
    products_discovered: int = 0  # 去重后发现的商品数量。
    details_success: int = 0  # 成功解析详情页的商品数量。
    inserted: int = 0  # 成功新增 spider_log 的数量。
    duplicates: int = 0  # 已存在并跳过的数量。
    images_collected: int = 0  # 收集到的图片地址总数。
    option_groups_collected: int = 0  # 收集到的选项组总数。
    sku_items_collected: int = 0  # 收集到的 SKU 明细总数。
    classifications_succeeded: int = 0  # 成功从现有分类流程拿到 AI 结果的数量。
    goods_created: int = 0  # From Switch 详情成功调用商品接口并创建的数量。
    existing_skipped: int = 0  # 已命中数据库且本次无需读取详情的数量。
    existing_records_loaded: int = 0  # 从数据库读取并准备分类的已有采集记录数量。
    failures: List[Dict[str, str]] = field(default_factory=list)  # 商品级失败，包含页码、编号、地址和原因。

    def add_failure(self, page_no: int, product_no: str, url: str, reason: str) -> None:
        """追加一条可复核的商品失败记录。"""
        self.failures.append(
            {
                "page": str(page_no),
                "product_no": product_no,
                "url": url,
                "reason": reason,
            }
        )


@dataclass
class SpiderConfig:
    """命令行解析后的采集配置。"""

    category_url: str = DEFAULT_CATEGORY_URL  # 分类页入口。
    category_no: int = DEFAULT_CATEGORY_NO  # Cafe24 分类编号。
    brand_id: int = DEFAULT_BRAND_ID  # 写入 spider_log 的品牌 ID。
    start_page: int = 1  # 起始分类页。
    end_page: Optional[int] = None  # 结束分类页，空值表示使用站点分页结果。
    limit_products: int = 0  # 试跑商品数，0 表示不限。
    detail_delay: float = DEFAULT_DETAIL_DELAY  # 详情页之间的基础等待秒数。
    max_retries: int = DEFAULT_MAX_RETRIES  # 单个 URL 的最大请求次数。
    dry_run: bool = False  # 是否只解析、不写数据库。
    log_file: Optional[str] = None  # 日志文件路径。
    db_host: str = "222.186.135.83"  # 数据库地址，可由 SPIDER_DB_HOST 覆盖。
    db_port: int = 3306  # 数据库端口，可由 SPIDER_DB_PORT 覆盖。
    db_user: str = "sukitime_remote"  # 数据库用户，可由 SPIDER_DB_USER 覆盖。
    db_password: str = ""  # 数据库密码，只从 SPIDER_DB_PASSWORD 读取。
    db_name: str = "sukitime"  # 数据库名称，可由 SPIDER_DB_NAME 覆盖。
    auto_create_goods: bool = False  # 是否调用现有 AI 分类并自动创建 From Switch 商品。
    retry_pending: bool = False  # 采集新商品模式下是否重试已有但仍未处理完成的 spider_log。
    existing_only: bool = True  # 是否只读取已有采集记录；默认不访问 From Switch 站点。
    api_base_url: str = DEFAULT_API_BASE_URL  # 管理后台 API 地址，可由 DOGDOGDOLL_API_BASE_URL 覆盖。
    api_token: str = ""  # 管理员 JWT，只从 DOGDOGDOLL_ADMIN_TOKEN 读取。
    api_timeout: float = DEFAULT_API_TIMEOUT  # 管理后台请求和 AI 流式响应的读取超时。


@dataclass(frozen=True)
class SpiderLogRecord:
    """用于判断重复采集记录是否可以继续自动处理的最小字段集合。"""

    spider_log_id: int  # spider_log 主键。
    status: int  # 采集记录处理状态，2 表示已处理。
    msg_type: int  # 采集记录类型，保留用于日志上下文。
    full_get: int  # 图片是否已经过后台上传流程。
    images: str  # 数据库中当前保存的图片地址串。
    title: str = ""  # 数据库中保存的采集标题。
    content: str = ""  # 数据库中保存的采集正文和规格文本。
    url: str = ""  # 数据库中保存的来源地址。
    created_at: int = 0  # 采集记录创建时间，用于保留原始上下文。


@dataclass(frozen=True)
class SpiderLogWriteResult:
    """表示本次写入是新增还是命中已有采集记录。"""

    spider_log_id: int  # 可传给现有 AI 分类和商品创建接口的采集记录 ID。
    inserted: bool  # True 表示本次新增，False 表示按 url_md5 命中旧记录。
    status: int  # 命中记录的处理状态；新增记录使用 DEFAULT_STATUS。
    full_get: int  # 命中记录的图片上传状态；新增记录为 0。


def clean_inline_text(value: Any) -> str:
    """清洗单行 HTML 文本，保留内容并压缩无意义空白。"""
    text = std_html.unescape(str(value or "")).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def is_from_switch_empty_option(value: Any) -> bool:
    """判断 From Switch 的“无选项”占位文本，避免把英文占位值当成商品资料。"""
    normalized = clean_inline_text(value).casefold()
    return normalized in FROM_SWITCH_EMPTY_OPTION_LABELS


def is_from_switch_size_option_group(value: Any) -> bool:
    """判断详情页选项组是否明确表示尺寸，排除 Body painting 等非尺寸选项组。"""
    normalized = re.sub(r"[\s_-]+", " ", clean_inline_text(value)).strip()
    return bool(re.fullmatch(FROM_SWITCH_SIZE_OPTION_GROUP_PATTERN, normalized, re.IGNORECASE))


def normalize_from_switch_chinese_name(raw_name: Any) -> str:
    """只接受对照表白名单中的中文名，兼容 AI 误加的一层外部括号。"""
    value = clean_inline_text(raw_name)
    if len(value) >= 2 and value[0] in "（(" and value[-1] in "）)":
        value = value[1:-1].strip()
    return value if value in FROM_SWITCH_CHINESE_NAME_WHITELIST else ""


def normalize_from_switch_official_name(raw_name: Any) -> str:
    """把 AI 返回的英文官名归一到白名单原文，只兼容大小写、冒号和空格差异。"""
    value = clean_inline_text(raw_name).casefold()
    value = re.sub(r"\s*[:：]\s*", ":", value)
    for official_name in FROM_SWITCH_CHINESE_NAME_BY_OFFICIAL_NAME:
        candidate = re.sub(r"\s*[:：]\s*", ":", official_name.casefold())
        if value == candidate:
            return official_name
    return ""


def find_from_switch_official_name(raw_text: Any) -> str:
    """从来源文本中找出最长的白名单英文官名，避免把明确变体误判为基础官名。"""
    source_tokens = re.findall(r"[a-z0-9]+", clean_inline_text(raw_text).casefold())
    if not source_tokens:
        return ""
    matches: List[Tuple[int, str]] = []
    for official_name in FROM_SWITCH_CHINESE_NAME_BY_OFFICIAL_NAME:
        official_tokens = re.findall(r"[a-z0-9]+", official_name.casefold())
        if not official_tokens or len(official_tokens) > len(source_tokens):
            continue
        width = len(official_tokens)
        if any(
            source_tokens[index:index + width] == official_tokens
            for index in range(len(source_tokens) - width + 1)
        ):
            matches.append((width, official_name))
    if not matches:
        return ""
    best_width = max(width for width, _ in matches)
    best_names = {official_name for width, official_name in matches if width == best_width}
    return next(iter(best_names)) if len(best_names) == 1 else ""


def resolve_from_switch_chinese_name(
    raw_official_name: Any,
    raw_chinese_name: Any,
    title: Any,
    body_text: Any,
) -> str:
    """校验 AI 返回的官名、中文名和来源证据，任何一项不一致都不追加中文名。"""
    official_name = normalize_from_switch_official_name(raw_official_name)
    chinese_name = normalize_from_switch_chinese_name(raw_chinese_name)
    if not official_name or not chinese_name:
        return ""
    if FROM_SWITCH_CHINESE_NAME_BY_OFFICIAL_NAME.get(official_name) != chinese_name:
        return ""

    title_match = find_from_switch_official_name(title)
    if title_match:
        return chinese_name if title_match == official_name else ""
    body_match = find_from_switch_official_name(body_text)
    return chinese_name if body_match == official_name else ""


def compose_from_switch_product_name(product_name: Any, chinese_name: Any) -> str:
    """将已经通过配对和来源校验的中文名追加到商品名末尾，避免重复括号。"""
    base_name = clean_inline_text(product_name)
    normalized_name = normalize_from_switch_chinese_name(chinese_name)
    if not base_name or not normalized_name:
        return base_name
    if base_name.endswith(f"（{normalized_name}）") or base_name.endswith(f"({normalized_name})"):
        return base_name
    return f"{base_name}（{normalized_name}）"


def normalize_api_string_list(raw_values: Any) -> List[str]:
    """把 AI 返回的字符串数组或分隔文本清洗为去重后的字符串列表。"""
    if isinstance(raw_values, (list, tuple)):
        values = [clean_inline_text(item) for item in raw_values]
    else:
        values = [
            clean_inline_text(item)
            for item in re.split(r"[、,，/|\\\s]+", clean_inline_text(raw_values))
        ]
    result: List[str] = []
    seen: Set[str] = set()
    for value in values:
        if not value or is_from_switch_empty_option(value) or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def normalize_multiline_text(value: Any) -> str:
    """清洗多行正文，保留段落顺序并压缩连续空行。"""
    text = std_html.unescape(str(value or "")).replace("\xa0", " ")
    normalized_lines: List[str] = []
    for raw_line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = re.sub(r"[ \t\f\v]+", " ", raw_line).strip()
        if line:
            normalized_lines.append(line)
            continue
        if normalized_lines and normalized_lines[-1] != "":
            normalized_lines.append("")
    while normalized_lines and normalized_lines[-1] == "":
        normalized_lines.pop()
    return "\n".join(normalized_lines)


def extract_node_text_with_breaks(node: Any) -> str:
    """递归提取 HTML 节点文本，将 br 和块级标签转换为换行。"""
    parts: List[str] = []

    def visit(element: Any) -> None:
        """遍历一个 HTML 元素并累积正文文本。"""
        tag = str(getattr(element, "tag", "") or "").lower()
        if tag in TEXT_SKIP_TAGS:
            return
        if tag == "br":
            parts.append("\n")
            return
        if tag in TEXT_BLOCK_TAGS:
            parts.append("\n")
        if element.text:
            parts.append(element.text)
        for child in element:
            visit(child)
            if child.tail:
                parts.append(child.tail)
        if tag in TEXT_BLOCK_TAGS:
            parts.append("\n")

    visit(node)
    return normalize_multiline_text("".join(parts))


def absolute_url(raw_url: Any, base_url: str) -> str:
    """把站点相对地址转换成绝对 HTTP(S) 地址，并移除 fragment。"""
    value = std_html.unescape(str(raw_url or "")).strip()
    if not value or value.lower().startswith("data:"):
        return ""
    resolved = urljoin(base_url, value)
    parsed = urlsplit(resolved)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return ""
    return urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc,
            parsed.path,
            parsed.query,
            "",
        )
    )


def is_from_switch_waste_image_url(raw_url: Any) -> bool:
    """判断图片是否为 From Switch 固定编辑器废图，忽略协议、查询参数和 fragment 差异。"""
    normalized = absolute_url(raw_url, BASE_URL)
    if not normalized:
        return False
    expected = urlsplit(FROM_SWITCH_WASTE_IMAGE_URL)
    actual = urlsplit(normalized)
    return (
        actual.netloc.lower() == expected.netloc.lower()
        and actual.path.rstrip("/").lower() == expected.path.rstrip("/").lower()
    )


def normalize_identity_url(raw_url: str) -> str:
    """生成用于 url_md5 去重的规范地址，不改变实际保存的可访问 URL。"""
    value = absolute_url(raw_url, BASE_URL)
    if not value:
        return ""
    parsed = urlsplit(value)
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))


def calculate_url_md5(raw_url: str) -> str:
    """计算规范详情地址的 MD5，写入 spider_log.url_md5。"""
    normalized = normalize_identity_url(raw_url)
    if not normalized:
        return ""
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def build_category_page_url(category_url: str, category_no: int, page_no: int) -> str:
    """构造保留 cate_no 的分类页地址，避免站点相对分页链接丢失分类。"""
    parsed = urlsplit(category_url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query["cate_no"] = [str(category_no)]
    query["page"] = [str(page_no)]
    return urlunsplit(
        (
            parsed.scheme or "https",
            parsed.netloc or urlsplit(BASE_URL).netloc,
            parsed.path or "/product/list.html",
            urlencode(query, doseq=True),
            "",
        )
    )


def parse_document(raw_html: Any) -> Any:
    """把响应内容解析为 lxml HTML 文档。"""
    if isinstance(raw_html, bytes):
        # 目标站点响应头和页面声明均为 UTF-8；先解码再交给 lxml，避免
        # lxml 在缺少可识别 meta 时按单字节编码解析中文商品名。
        payload = raw_html.decode("utf-8", errors="replace")
    else:
        payload = str(raw_html or "")
    if not payload.strip():
        raise PageParseError("页面 HTML 为空")
    try:
        return lxml_html.fromstring(payload)
    except (TypeError, ValueError) as exc:
        raise PageParseError(f"页面 HTML 解析失败: {exc}") from exc


def extract_page_number(raw_url: str, base_url: str = BASE_URL) -> Optional[int]:
    """从分页链接中提取 page 参数。"""
    resolved = absolute_url(raw_url, base_url)
    if not resolved:
        return None
    values = parse_qs(urlsplit(resolved).query).get("page", [])
    if not values:
        return None
    try:
        page_no = int(values[0])
    except (TypeError, ValueError):
        return None
    return page_no if page_no > 0 else None


def extract_last_page(document: Any, current_page: int) -> int:
    """读取 Cafe24 分页器的最后页，兼容只展示部分页码的分页器。"""
    candidates: List[int] = []
    paging_nodes = document.xpath(
        '//*[contains(concat(" ", normalize-space(@class), " "), " xans-product-normalpaging ")]'
    )
    for paging in paging_nodes:
        last_links = paging.xpath(
            './/a[img[translate(@alt, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz") = "last"]]/@href'
        )
        for href in last_links:
            page_no = extract_page_number(href, BASE_URL)
            if page_no:
                candidates.append(page_no)
        for href in paging.xpath('.//a[contains(@href, "page=")]/@href'):
            page_no = extract_page_number(href, BASE_URL)
            if page_no:
                candidates.append(page_no)
    return max(candidates) if candidates else current_page


def extract_product_no_from_url(raw_url: str) -> str:
    """从 product_no 查询参数或详情路径提取商品编号。"""
    value = absolute_url(raw_url, BASE_URL)
    if not value:
        return ""
    parsed = urlsplit(value)
    query_values = parse_qs(parsed.query).get("product_no", [])
    if query_values and str(query_values[0]).strip():
        return str(query_values[0]).strip()
    matches = re.findall(r"/(\d+)(?:/|$)", parsed.path)
    return matches[-1] if matches else ""


def extract_card_title(card: Any) -> str:
    """从分类卡片中提取商品名称，排除隐藏的 Product Name 标签。"""
    anchors = card.xpath('.//p[contains(concat(" ", normalize-space(@class), " "), " name ")]//a[1]')
    if not anchors:
        return ""
    anchor = anchors[0]
    visible_text = anchor.xpath('.//span[not(ancestor::strong[contains(@class, "displaynone")])]//text()')
    title = clean_inline_text(" ".join(visible_text))
    if title:
        return title
    fallback = clean_inline_text(anchor.text_content())
    return re.sub(r"^Product Name\s*:\s*", "", fallback, flags=re.IGNORECASE).strip()


def parse_category_page(raw_html: Any, page_no: int, page_url: str) -> Tuple[List[ProductCard], int]:
    """解析一个分类页，返回商品卡片和站点报告的最后页。"""
    document = parse_document(raw_html)
    list_nodes = document.xpath(
        '//*[contains(concat(" ", normalize-space(@class), " "), " xans-product-listnormal ")]'
    )
    if not list_nodes:
        raise PageParseError(f"分类页 {page_no} 未找到商品列表容器: {page_url}")

    cards: List[ProductCard] = []
    card_nodes = list_nodes[0].xpath('.//ul[contains(concat(" ", normalize-space(@class), " "), " prdList ")]/li[starts-with(@id, "anchorBoxId_")]')
    for card_node in card_nodes:
        card_id = str(card_node.get("id") or "")
        product_no_match = PRODUCT_NO_FROM_CARD_RE.search(card_id)
        product_no = product_no_match.group(1) if product_no_match else ""
        hrefs = card_node.xpath(
            './/p[contains(concat(" ", normalize-space(@class), " "), " name ")]//a[1]/@href'
        )
        if not hrefs:
            hrefs = card_node.xpath('.//a[contains(@href, "product_no=")]/@href')
        detail_url = absolute_url(hrefs[0], page_url) if hrefs else ""
        if not product_no:
            product_no = extract_product_no_from_url(detail_url)
        if not product_no or not detail_url:
            continue
        cover_nodes = card_node.xpath(
            './/img[contains(concat(" ", normalize-space(@class), " "), " thumb ")]'
        )
        cover_image = ""
        if cover_nodes:
            cover_image = absolute_url(
                cover_nodes[0].get("ec-data-src")
                or cover_nodes[0].get("data-src")
                or cover_nodes[0].get("src"),
                page_url,
            )
        cards.append(
            ProductCard(
                product_no=product_no,
                title=extract_card_title(card_node),
                url=detail_url,
                cover_image=cover_image,
                page_no=page_no,
            )
        )

    return cards, extract_last_page(document, page_no)


def extract_js_assignment(script_text: str, variable_name: str) -> str:
    """读取 var name = '...' 形式的 JavaScript 字符串变量。"""
    pattern = re.compile(
        JS_ASSIGNMENT_RE_TEMPLATE.format(name=re.escape(variable_name)),
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(script_text or "")
    if not match:
        return ""
    quote = match.group(1)
    raw_value = match.group(2)
    try:
        decoded = ast.literal_eval(f"{quote}{raw_value}{quote}")
        return str(decoded)
    except (SyntaxError, ValueError):
        return raw_value.replace(r"\'", "'").replace(r'\"', '"')


def parse_option_stock_data(script_text: str) -> Dict[str, Any]:
    """解析详情页中的 option_stock_data JSON 字符串。"""
    raw_value = extract_js_assignment(script_text, "option_stock_data")
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_price_display(raw_price: str, script_text: str) -> Tuple[str, str, str]:
    """解析价格展示值、币种和金额，必要时使用页面脚本中的基础价格。"""
    display = clean_inline_text(raw_price)
    if not display:
        script_price = extract_js_assignment(script_text, "product_price")
        currency_match = re.search(r'"currency_code"\s*:\s*"([A-Z]{3})"', script_text or "")
        currency = currency_match.group(1) if currency_match else ""
        if script_price:
            display = f"{currency} {script_price}".strip()

    match = PRICE_RE.search(display)
    if match:
        return display, match.group("currency").upper(), match.group("amount").replace(",", "")
    symbol_match = SYMBOL_PRICE_RE.search(display)
    if symbol_match:
        return display, symbol_match.group("symbol"), symbol_match.group("amount").replace(",", "")
    numeric_match = re.search(r"[0-9][0-9,]*(?:\.[0-9]+)?", display)
    numeric = numeric_match.group(0).replace(",", "") if numeric_match else ""
    return display, "", numeric


def parse_option_adjustment(raw_label: str) -> Tuple[str, str]:
    """从选项文本中拆出干净名称和原始加价展示。"""
    label = clean_inline_text(raw_label)
    match = OPTION_ADJUSTMENT_RE.search(label)
    if not match:
        return label, ""
    currency = (match.group("currency") or "").upper()
    amount = match.group("amount").replace(",", "")
    adjustment = f"{currency} {amount}".strip()
    clean_label = OPTION_ADJUSTMENT_RE.sub("", label).strip()
    return clean_label, adjustment


def value_as_text(value: Any) -> str:
    """把脚本中的标量值转换成稳定的展示文本。"""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def value_as_list(value: Any) -> List[str]:
    """将脚本中的字符串或数组统一成字符串列表。"""
    if isinstance(value, (list, tuple)):
        return [value_as_text(item) for item in value if value_as_text(item)]
    text = value_as_text(value)
    return [text] if text else []


def parse_option_groups(document: Any, stock_data: Dict[str, Any]) -> List[ProductOptionGroup]:
    """读取 Product Option 表中的全部选项组和值。"""
    tables = document.xpath('//table[caption[normalize-space(.) = "Product Option"]]')
    groups: List[ProductOptionGroup] = []
    for table in tables:
        for select in table.xpath(".//select"):
            rows = select.xpath("./ancestor::tr[1]")
            row = rows[0] if rows else None
            name = clean_inline_text(select.get("option_title"))
            if not name and row is not None:
                name = clean_inline_text(" ".join(row.xpath("./th//text()")))
            try:
                sort_no = int(select.get("option_sort_no") or len(groups) + 1)
            except (TypeError, ValueError):
                sort_no = len(groups) + 1
            required_value = str(select.get("required") or "").lower()
            required = required_value in {"1", "true", "t", "yes"}
            option_type = clean_inline_text(select.get("option_type"))
            option_code = clean_inline_text(select.get("option_code"))
            values: List[ProductOptionValue] = []
            for option in select.xpath("./option"):
                value = clean_inline_text(option.get("value"))
                raw_label = clean_inline_text(option.text_content())
                if value in {"", "*", "**"} or not raw_label or set(raw_label) <= {"-", "—", "_", " "}:
                    continue
                label, adjustment = parse_option_adjustment(raw_label)
                stock_entry = stock_data.get(value)
                values.append(
                    ProductOptionValue(
                        value=value,
                        label=label,
                        raw_label=raw_label,
                        disabled=option.get("disabled") is not None,
                        price_adjustment=adjustment,
                        stock_data=stock_entry if isinstance(stock_entry, dict) else {},
                    )
                )
            groups.append(
                ProductOptionGroup(
                    name=name or f"Option {sort_no}",
                    sort_no=sort_no,
                    required=required,
                    option_type=option_type,
                    option_code=option_code,
                    values=values,
                )
            )
    groups.sort(key=lambda group: group.sort_no)
    return groups


def parse_sku_items(stock_data: Dict[str, Any], option_groups: Sequence[ProductOptionGroup]) -> List[ProductSkuItem]:
    """根据 E/T 两类 option_stock_data 解析选项 SKU 和组合 SKU。"""
    known_option_values: Set[str] = {
        option.value
        for group in option_groups
        for option in group.values
        if option.value
    }
    items: List[ProductSkuItem] = []
    for sku_code, raw_payload in stock_data.items():
        if not isinstance(raw_payload, dict):
            continue
        original_values = raw_payload.get("option_value_original")
        if original_values is None:
            original_values = raw_payload.get("option_value_orginal")
        option_values = value_as_list(original_values)
        if not option_values:
            option_values = value_as_list(raw_payload.get("option_value"))
        option_names = value_as_list(raw_payload.get("option_name_original"))
        is_combination = bool(option_values or raw_payload.get("option_value"))
        if not is_combination and str(sku_code) not in known_option_values:
            continue
        items.append(
            ProductSkuItem(
                sku_code=str(sku_code),
                option_values=option_values,
                option_names=option_names,
                option_price=value_as_text(
                    raw_payload.get("option_price", raw_payload.get("origin_option_added_price"))
                ),
                stock_number=value_as_text(raw_payload.get("stock_number")),
                is_selling=value_as_text(raw_payload.get("is_selling")),
                use_soldout=value_as_text(raw_payload.get("use_soldout")),
            )
        )
    return items


def collect_image_urls(document: Any, body_node: Optional[Any], detail_url: str, fallback_cover: str = "") -> List[str]:
    """按主图、附加图、正文图顺序收集去重后的绝对图片地址。"""
    images: List[str] = []
    seen: Set[str] = set()

    def append_image(raw_url: Any) -> None:
        """规范化并追加一张尚未出现的图片。"""
        normalized = absolute_url(raw_url, detail_url)
        if not normalized or is_from_switch_waste_image_url(normalized) or normalized in seen:
            return
        seen.add(normalized)
        images.append(normalized)

    big_images = document.xpath(
        '//*[contains(concat(" ", normalize-space(@class), " "), " imgArea ")]//img[contains(concat(" ", normalize-space(@class), " "), " BigImage ")]'
    )
    for image in big_images:
        append_image(image.get("ec-data-src") or image.get("data-src") or image.get("src"))
    thumbnails = document.xpath(
        '//*[contains(concat(" ", normalize-space(@class), " "), " listImg ")]//img[contains(concat(" ", normalize-space(@class), " "), " ThumbImage ")]'
    )
    for image in thumbnails:
        append_image(image.get("ec-data-src") or image.get("data-src") or image.get("src"))
    if body_node is not None:
        for image in body_node.xpath(".//img"):
            append_image(image.get("ec-data-src") or image.get("data-src") or image.get("src"))
    if not images:
        append_image(fallback_cover)
    return images


def extract_product_title(document: Any) -> str:
    """按详情页主信息、Basic Information、JSON-LD 顺序提取商品名称。"""
    info_nodes = document.xpath(
        '//*[contains(concat(" ", normalize-space(@class), " "), " infoArea ")]'
    )
    if info_nodes:
        direct_spans = info_nodes[0].xpath("./span[1]//text()")
        title = clean_inline_text(" ".join(direct_spans))
        if title:
            return title
    basic_names = document.xpath(
        '//table[caption[normalize-space(.) = "Basic Information"]]//tr[th[contains(normalize-space(.), "Product Name")]]/td//text()'
    )
    title = clean_inline_text(" ".join(basic_names))
    if title:
        return title
    for raw_json in document.xpath('//script[@type="application/ld+json"]/text()'):
        try:
            payload = json.loads(raw_json)
        except (TypeError, ValueError):
            continue
        if isinstance(payload, dict) and clean_inline_text(payload.get("name")):
            return clean_inline_text(payload.get("name"))
    return ""


def extract_product_number(document: Any, detail_url: str) -> str:
    """从页面脚本、option_product_no 或 URL 提取商品编号。"""
    scripts = "\n".join(document.xpath("//script/text()"))
    match = re.search(r"\bvar\s+iProductNo\s*=\s*['\"]?(\d+)", scripts, re.IGNORECASE)
    if match:
        return match.group(1)
    option_product_numbers = document.xpath("//select[@option_product_no]/@option_product_no")
    if option_product_numbers and clean_inline_text(option_product_numbers[0]):
        return clean_inline_text(option_product_numbers[0])
    return extract_product_no_from_url(detail_url)


def parse_product_detail(raw_html: Any, detail_url: str, fallback_cover: str = "") -> ProductDetail:
    """解析详情页商品名称、正文、图片、价格、规格组和 SKU 明细。"""
    document = parse_document(raw_html)
    detail_nodes = document.xpath(
        '//*[contains(concat(" ", normalize-space(@class), " "), " xans-product-detail ")]'
    )
    body_nodes = document.xpath('//*[@id="prdDetail"]')
    if not detail_nodes or not body_nodes:
        raise PageParseError(f"详情页缺少商品详情节点: {detail_url}")

    canonical_nodes = document.xpath('//link[@rel="canonical"]/@href')
    canonical_url = absolute_url(canonical_nodes[0], detail_url) if canonical_nodes else ""
    saved_url = canonical_url or absolute_url(detail_url, BASE_URL)
    if not saved_url:
        raise PageParseError(f"详情页地址无效: {detail_url}")

    title = extract_product_title(document)
    product_no = extract_product_number(document, saved_url)
    if not product_no or not title:
        raise PageParseError(f"详情页缺少商品编号或名称: {detail_url}")

    price_nodes = document.xpath('//*[@id="span_product_price_text"]')
    raw_price = price_nodes[0].text_content() if price_nodes else ""
    scripts = "\n".join(document.xpath("//script/text()"))
    price_display, currency, price_value = parse_price_display(raw_price, scripts)
    stock_data = parse_option_stock_data(scripts)
    option_groups = parse_option_groups(document, stock_data)
    sku_items = parse_sku_items(stock_data, option_groups)
    body_node = body_nodes[0]
    body_text = extract_node_text_with_breaks(body_node)
    images = collect_image_urls(document, body_node, saved_url, fallback_cover=fallback_cover)

    return ProductDetail(
        product_no=product_no,
        title=title,
        url=saved_url,
        canonical_url=canonical_url,
        price_display=price_display,
        currency=currency,
        price_value=price_value,
        body_text=body_text,
        images=images,
        option_groups=option_groups,
        sku_items=sku_items,
    )


def format_sku_status(item: ProductSkuItem) -> str:
    """把 SKU 脚本状态转成采集正文中的简短状态。"""
    if item.use_soldout.upper() == "T":
        return "售罄控制"
    if item.is_selling.upper() == "F":
        return "停止销售"
    if item.stock_number == "0":
        return "库存为 0"
    return "页面可售"


def build_spider_content(detail: ProductDetail) -> str:
    """按照固定格式合并价格、SKU/规格和商品正文，写入 spider_log.content。"""
    lines: List[str] = [
        f"商品编号：{detail.product_no}",
        f"商品价格：{detail.price_display or '未提供'}",
        "",
        "SKU/规格选项：",
    ]
    if detail.option_groups:
        for group in detail.option_groups:
            required_label = "必选" if group.required else "可选"
            if not group.values:
                lines.append(f"- {group.name}（{required_label}）：页面未提供实际值")
                continue
            for option in group.values:
                details = [f"value={option.value}"]
                if option.price_adjustment:
                    details.append(f"加价={option.price_adjustment}")
                details.append("页面禁用" if option.disabled else required_label)
                lines.append(f"- {group.name}：{option.label}（" + "；".join(details) + "）")
    else:
        lines.append("- 页面未提供")

    lines.extend(["", "SKU组合："])
    if detail.sku_items:
        for item in detail.sku_items:
            details = [f"sku_code={item.sku_code}"]
            if item.option_values:
                details.append("选项=" + " / ".join(item.option_values))
            if item.option_price:
                details.append(f"价格={item.option_price}")
            if item.stock_number:
                details.append(f"库存={item.stock_number}")
            details.append(f"状态={format_sku_status(item)}")
            lines.append("- " + "；".join(details))
    else:
        lines.append("- 页面未提供可解析的组合 SKU")

    lines.extend(["", "商品正文：", detail.body_text or "页面未提供正文"])
    return normalize_multiline_text("\n".join(lines))


class HttpClient:
    """带有限重试和来源请求头的 HTTP 客户端。"""

    def __init__(self, session: requests.Session, logger: logging.Logger, max_retries: int) -> None:
        """初始化 HTTP 客户端。"""
        self.session = session  # 复用连接和 Cookie 的 requests 会话。
        self.logger = logger  # 统一记录请求重试和失败原因。
        self.max_retries = max(1, int(max_retries))  # 防止传入 0 导致没有请求机会。

    def get(self, url: str, referer: str = "") -> requests.Response:
        """读取 URL；仅对超时、429 和 5xx 做有限重试。"""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        if referer:
            headers["Referer"] = referer

        last_error = "未知请求错误"
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=DEFAULT_TIMEOUT,
                    allow_redirects=True,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt >= self.max_retries:
                    break
                self.logger.warning("请求失败，准备重试 %d/%d：%s；url=%s", attempt, self.max_retries, exc, url)
                time.sleep(min(8.0, 1.5 * attempt))
                continue

            if response.status_code in RETRY_STATUS_CODES:
                last_error = f"HTTP {response.status_code}"
                if attempt >= self.max_retries:
                    break
                self.logger.warning(
                    "目标站点返回 %s，准备重试 %d/%d；url=%s",
                    response.status_code,
                    attempt,
                    self.max_retries,
                    url,
                )
                time.sleep(min(8.0, 1.5 * attempt))
                continue
            if response.status_code >= 400:
                raise PageFetchError(f"HTTP {response.status_code}: {url}")
            if not response.content.strip():
                raise PageFetchError(f"响应内容为空: {url}")
            return response

        raise PageFetchError(f"请求失败（重试 {self.max_retries} 次）: {url}; {last_error}")


class SpiderLogDatabase:
    """spider_log 的最小显式写入接口。"""

    def __init__(self, config: SpiderConfig, logger: logging.Logger) -> None:
        """连接数据库，不执行任何建表或修改表结构的 SQL。"""
        if not config.db_password:
            raise DatabaseError("缺少数据库密码，请设置 SPIDER_DB_PASSWORD")
        self.config = config  # 保存连接参数，断线重连时复用。
        self.logger = logger  # 记录去重和写入信息。
        self.connection = self._connect()  # 当前数据库连接。

    def _connect(self) -> Any:
        """建立一条关闭自动提交的 MySQL 连接。"""
        try:
            return pymysql.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                user=self.config.db_user,
                password=self.config.db_password,
                database=self.config.db_name,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=False,
                connect_timeout=10,
                read_timeout=30,
                write_timeout=30,
            )
        except pymysql.MySQLError as exc:
            raise DatabaseError(f"数据库连接失败: {exc}") from exc

    def _ensure_connection(self) -> None:
        """在数据库连接断开时重连。"""
        try:
            self.connection.ping(reconnect=True)
        except pymysql.MySQLError:
            try:
                self.connection.close()
            except pymysql.MySQLError:
                pass
            self.connection = self._connect()

    def fetch_brand_name(self, brand_id: int) -> str:
        """校验品牌有效并读取品牌名称。"""
        self._ensure_connection()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, brand_name, is_delete, is_brand
                    FROM brand
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (brand_id,),
                )
                row = cursor.fetchone()
        except pymysql.MySQLError as exc:
            raise DatabaseError(f"读取品牌 {brand_id} 失败: {exc}") from exc
        if not row or int(row.get("is_delete") or 0) != 0 or int(row.get("is_brand") or 0) != 1:
            raise DatabaseError(f"品牌 {brand_id} 不存在或不是有效品牌")
        brand_name = clean_inline_text(row.get("brand_name"))
        if not brand_name:
            raise DatabaseError(f"品牌 {brand_id} 名称为空")
        return brand_name

    def list_unprocessed_records(
        self,
        brand_id: int,
        origin_type: str,
        limit: int = 0,
    ) -> List[SpiderLogRecord]:
        """读取指定来源仍未处理的采集记录，供已有数据分类模式直接使用。"""
        self._ensure_connection()
        query = """
            SELECT id, status, msg_type, full_get, images, title, content, url, created_at
            FROM spider_log
            WHERE brand_id = %s
              AND origin_type = %s
              AND status IN (0, 1)
            ORDER BY id ASC
        """
        params: List[Any] = [brand_id, origin_type]
        if limit > 0:
            query += " LIMIT %s"
            params.append(limit)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, tuple(params))
                rows = cursor.fetchall()
        except pymysql.MySQLError as exc:
            raise DatabaseError(f"读取已有未处理采集记录失败: {exc}") from exc

        records: List[SpiderLogRecord] = []
        for row in rows or []:
            records.append(
                SpiderLogRecord(
                    spider_log_id=int(row.get("id") or 0),
                    status=int(row.get("status") or 0),
                    msg_type=int(row.get("msg_type") or 0),
                    full_get=int(row.get("full_get") or 0),
                    images=str(row.get("images") or ""),
                    title=str(row.get("title") or ""),
                    content=str(row.get("content") or ""),
                    url=str(row.get("url") or ""),
                    created_at=int(row.get("created_at") or 0),
                )
            )
        return records

    def find_by_url_md5(self, url_md5: str) -> Optional[SpiderLogRecord]:
        """按 url_md5 读取采集记录最小状态，用于去重和安全复用未处理记录。"""
        if not url_md5:
            raise DatabaseError("商品详情地址无法生成 url_md5")
        self._ensure_connection()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, status, msg_type, full_get, images
                    FROM spider_log
                    WHERE url_md5 = %s
                    LIMIT 1
                    """,
                    (url_md5,),
                )
                row = cursor.fetchone()
        except pymysql.MySQLError as exc:
            raise DatabaseError(f"查询采集记录去重失败: {exc}") from exc
        if not row:
            return None
        return SpiderLogRecord(
            spider_log_id=int(row.get("id") or 0),
            status=int(row.get("status") or 0),
            msg_type=int(row.get("msg_type") or 0),
            full_get=int(row.get("full_get") or 0),
            images=str(row.get("images") or ""),
        )

    def find_by_source_product_no(
        self,
        product_no: str,
        brand_id: int,
        origin_type: str,
    ) -> Optional[SpiderLogRecord]:
        """按来源、品牌和站点商品编号预查已有记录，避免重复读取已处理详情页。"""
        normalized_product_no = clean_inline_text(product_no)
        if not re.fullmatch(r"\d+", normalized_product_no):
            return None
        self._ensure_connection()
        url_patterns = (
            f"%/{normalized_product_no}/%",
            f"%product_no={normalized_product_no}&%",
            f"%product_no={normalized_product_no}",
        )
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, status, msg_type, full_get, images
                    FROM spider_log
                    WHERE brand_id = %s
                      AND origin_type = %s
                      AND (url LIKE %s OR url LIKE %s OR url LIKE %s)
                    ORDER BY status DESC, id DESC
                    LIMIT 1
                    """,
                    (brand_id, origin_type, *url_patterns),
                )
                row = cursor.fetchone()
        except pymysql.MySQLError as exc:
            raise DatabaseError(f"预查商品 {normalized_product_no} 的采集记录失败: {exc}") from exc
        if not row:
            return None
        return SpiderLogRecord(
            spider_log_id=int(row.get("id") or 0),
            status=int(row.get("status") or 0),
            msg_type=int(row.get("msg_type") or 0),
            full_get=int(row.get("full_get") or 0),
            images=str(row.get("images") or ""),
        )

    def exists_by_url_md5(self, url_md5: str) -> bool:
        """按现有 url_md5 检查采集记录是否已存在。"""
        return self.find_by_url_md5(url_md5) is not None

    def insert_product(self, detail: ProductDetail, brand_id: int, brand_name: str) -> SpiderLogWriteResult:
        """显式新增一条 spider_log；重复记录返回其 ID，不覆盖已处理记录。"""
        url_md5 = calculate_url_md5(detail.url)
        if not url_md5:
            raise DatabaseError(f"商品 {detail.product_no} 无法生成详情地址哈希")
        existing = self.find_by_url_md5(url_md5)
        if existing is not None:
            return SpiderLogWriteResult(
                spider_log_id=existing.spider_log_id,
                inserted=False,
                status=existing.status,
                full_get=existing.full_get,
            )

        now = int(time.time())
        content = build_spider_content(detail)
        images = ",".join(detail.images)
        sql = """
            INSERT INTO spider_log (
                msg_type,
                status,
                origin_type,
                title,
                content,
                url,
                images,
                brand_id,
                brand_name,
                created_at,
                updated_at,
                full_get,
                auth_time,
                likes,
                ai_think_type,
                ai_summary,
                `text`,
                url_md5
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        params = (
            DEFAULT_MSG_TYPE,  # 新采集记录。
            DEFAULT_STATUS,  # 商品内容待后台处理，且避免 auth_time=0 被旧日志维护清理。
            DEFAULT_ORIGIN_TYPE,  # From Switch 来源标识。
            detail.title[:600],  # 对应 spider_log.title。
            content,  # 价格、SKU 和商品正文的固定格式文本。
            detail.url,  # canonical 或列表详情地址。
            images,  # 去重后的来源图片地址，等待既有上传流程处理。
            brand_id,  # 固定的 Switch 品牌 ID。
            brand_name[:255],  # 从 brand 表读取的品牌名称。
            now,  # 抓取时间。
            now,  # 初始处理时间。
            0,  # 来源图片尚未上传到七牛。
            0,  # 商品页没有社交发布时间。
            0,  # 商品页没有点赞数量。
            0,  # 等待既有采集分类流程处理。
            "",  # 尚无 AI 摘要。
            "",  # text 专用于评论 JSON，本采集不写入。
            url_md5,  # 规范详情地址 MD5。
        )
        self._ensure_connection()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                spider_log_id = int(cursor.lastrowid or 0)
            self.connection.commit()
            if spider_log_id <= 0:
                raise DatabaseError(f"商品 {detail.product_no} 写入 spider_log 后未返回记录 ID")
            return SpiderLogWriteResult(
                spider_log_id=spider_log_id,
                inserted=True,
                status=DEFAULT_STATUS,
                full_get=0,
            )
        except DatabaseError:
            self.connection.rollback()
            raise
        except pymysql.MySQLError as exc:
            self.connection.rollback()
            raise DatabaseError(f"商品 {detail.product_no} 写入 spider_log 失败: {exc}") from exc

    def replace_unprocessed_images(self, spider_log_id: int, images: Sequence[str]) -> None:
        """为未处理记录写回当前过滤后的来源图片，并重置图片上传状态。"""
        if spider_log_id <= 0:
            raise DatabaseError("采集记录 ID 无效")
        image_text = ",".join(images)
        self._ensure_connection()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE spider_log
                    SET images = %s, full_get = 0, updated_at = %s
                    WHERE id = %s AND status <> 2
                    """,
                    (image_text, int(time.time()), spider_log_id),
                )
            self.connection.commit()
        except pymysql.MySQLError as exc:
            self.connection.rollback()
            raise DatabaseError(f"更新采集记录 {spider_log_id} 图片失败: {exc}") from exc

    def close(self) -> None:
        """关闭数据库连接。"""
        try:
            self.connection.close()
        except pymysql.MySQLError:
            pass


def normalize_goods_currency(raw_currency: Any) -> str:
    """将来源或 AI 币种转换为商品接口支持的币种编码。"""
    aliases = {
        "CNY": "CNY",
        "RMB": "CNY",
        "CNH": "CNY",
        "USD": "USD",
        "JPY": "JPY",
        "KRW": "KRW",
        "$": "USD",
        "¥": "CNY",
        "￥": "CNY",
    }
    value = clean_inline_text(raw_currency).upper()
    return aliases.get(value, "")


def parse_goods_integer_amount(raw_amount: Any) -> int:
    """解析商品接口的整数金额，拒绝静默截断小数价格。"""
    value = clean_inline_text(raw_amount).replace(",", "")
    if not value:
        return 0
    try:
        amount = Decimal(value)
    except (InvalidOperation, ValueError):
        raise GoodsPayloadError(f"商品价格无法解析：{value}")
    if not amount.is_finite():
        raise GoodsPayloadError(f"商品价格不是有限数值：{value}")
    if amount < 0:
        raise GoodsPayloadError(f"商品价格不能为负数：{value}")
    if amount != amount.to_integral_value():
        raise GoodsPayloadError(f"商品价格含小数，但现有商品接口只接受整数：{value}")
    return int(amount)


def normalize_spider_log_images(raw_images: Any) -> List[str]:
    """读取已有 spider_log 图片地址，去重并排除 From Switch 固定废图。"""
    source = raw_images if isinstance(raw_images, (list, tuple)) else str(raw_images or "").split(",")
    result: List[str] = []
    seen: Set[str] = set()
    for raw_image in source:
        image_url = absolute_url(raw_image, BASE_URL)
        if not image_url or is_from_switch_waste_image_url(image_url) or image_url in seen:
            continue
        seen.add(image_url)
        result.append(image_url)
    return result


def product_detail_from_spider_log(record: SpiderLogRecord) -> ProductDetail:
    """把数据库已有采集记录转换为商品创建所需的最小详情对象，不访问来源站点。"""
    title = clean_inline_text(record.title)
    content = normalize_multiline_text(record.content)
    source_url = absolute_url(record.url, BASE_URL)
    product_no = extract_product_no_from_url(source_url)
    if not product_no:
        product_match = re.search(r"(?im)^\s*商品编号\s*[：:]\s*(\d+)\s*$", content)
        product_no = product_match.group(1) if product_match else ""
    if not product_no:
        raise PageParseError(f"已有采集记录缺少来源商品编号：spider_id={record.spider_log_id}")
    if not title:
        raise PageParseError(f"已有采集记录缺少商品标题：spider_id={record.spider_log_id}")

    price_display = ""
    price_match = re.search(r"(?im)^\s*(?:商品)?价格\s*[：:]\s*(.+?)\s*$", content)
    if price_match:
        price_display = clean_inline_text(price_match.group(1))
    price_display, currency, price_value = parse_price_display(price_display, "")
    return ProductDetail(
        product_no=product_no,
        title=title,
        url=source_url,
        canonical_url=source_url,
        price_display=price_display,
        currency=currency,
        price_value=price_value,
        body_text=content,
        images=normalize_spider_log_images(record.images),
    )


def normalize_from_switch_size_pair(category: Any, detail: Any) -> Tuple[str, str]:
    """把 From Switch 尺寸值归一为系统要求的父级分类和细分类。"""
    normalized_category = clean_inline_text(category)
    normalized_detail = clean_inline_text(detail)
    detail_to_category = {
        re.sub(r"\s+", "", key).casefold(): value
        for key, value in FROM_SWITCH_SIZE_DETAIL_TO_CATEGORY.items()
    }
    category_aliases = {
        re.sub(r"\s+", "", key).casefold(): value
        for key, value in FROM_SWITCH_SIZE_CATEGORY_ALIASES.items()
    }

    category_key = re.sub(r"\s+", "", normalized_category).casefold()
    detail_key = re.sub(r"\s+", "", normalized_detail).casefold()
    if not normalized_detail and category_key in category_aliases:
        return category_aliases[category_key]
    if category_key in detail_to_category:
        parent = detail_to_category[category_key]
        normalized_category = parent
        if not normalized_detail:
            normalized_detail = clean_inline_text(category)
    if detail_key in detail_to_category:
        parent = detail_to_category[detail_key]
        if not normalized_category or normalized_category.casefold() in {
            parent.casefold(),
            clean_inline_text(category).casefold(),
        }:
            normalized_category = parent
            normalized_detail = clean_inline_text(detail)
    return normalized_category, normalized_detail


def normalize_goods_size_items(raw_sizes: Any) -> List[Dict[str, str]]:
    """把 AI 返回的尺寸分类转换为商品创建接口需要的 goods_size/size_detail 对象。"""
    source = raw_sizes if isinstance(raw_sizes, (list, tuple)) else [raw_sizes]
    result: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for item in source:
        category = ""
        detail = ""
        if isinstance(item, dict):
            category = clean_inline_text(
                item.get("goods_size") or item.get("size") or item.get("parent")
            )
            detail = clean_inline_text(
                item.get("size_detail") or item.get("detail") or item.get("child")
            )
        else:
            text = clean_inline_text(item)
            parts = [clean_inline_text(part) for part in re.split(r"\s*[／/]\s*", text, maxsplit=1)]
            if len(parts) == 2 and parts[0] and parts[1] and not re.fullmatch(r"\d+(?:\.\d+)?/\d+(?:\.\d+)?", text):
                category, detail = parts[0], parts[1]
            else:
                category = text
        category, detail = normalize_from_switch_size_pair(category, detail)
        if is_from_switch_empty_option(category):
            category = ""
        if is_from_switch_empty_option(detail):
            detail = ""
        if not category:
            continue
        identity = (category, detail)
        if identity in seen:
            continue
        seen.add(identity)
        result.append({"goods_size": category, "size_detail": detail})
    # 同一父级已经有明确细分类时，父级空详情行只是 AI 的重复概括；保留它会让
    # 商品接口把父级行排在第一行，导致主尺寸展示成大类而不是实际命中的细分类。
    detailed_categories = {
        item["goods_size"]
        for item in result
        if item["goods_size"] and item["size_detail"]
    }
    return [
        item
        for item in result
        if item["goods_size"] not in detailed_categories or item["size_detail"]
    ]


def extract_source_option_values(detail: ProductDetail, name_pattern: str) -> List[str]:
    """从详情页已解析的规格组中提取指定名称的原始选项值。"""
    pattern = re.compile(name_pattern, re.IGNORECASE)
    result: List[str] = []
    seen: Set[str] = set()
    for group in detail.option_groups:
        if not pattern.search(group.name):
            continue
        for option in group.values:
            label = clean_inline_text(option.label)
            if not label or is_from_switch_empty_option(label) or label in seen:
                continue
            seen.add(label)
            result.append(label)
    return result


def filter_from_switch_size_items(
    detail: ProductDetail,
    sizes: Sequence[Dict[str, str]],
) -> List[Dict[str, str]]:
    """过滤 AI 把非尺寸选项组值误识别为尺寸的结果，保留来源可解释的尺寸字段。"""
    non_size_option_values = {
        clean_inline_text(option.label).casefold()
        for group in detail.option_groups
        if not is_from_switch_size_option_group(group.name)
        for option in group.values
        if clean_inline_text(option.label)
    }
    filtered: List[Dict[str, str]] = []
    for item in sizes:
        category = clean_inline_text(item.get("goods_size"))
        size_detail = clean_inline_text(item.get("size_detail"))
        if is_from_switch_empty_option(category) or is_from_switch_empty_option(size_detail):
            continue
        if category.casefold() in non_size_option_values or size_detail.casefold() in non_size_option_values:
            continue
        filtered.append({"goods_size": category, "size_detail": size_detail})
    return filtered


def build_from_switch_goods_payload(detail: ProductDetail, result: Dict[str, Any]) -> Dict[str, Any]:
    """将 From Switch 的 AI 结果转换为商品请求，不生成贩售记录或新闻。"""
    if not isinstance(result, dict):
        raise GoodsPayloadError("AI 返回结果不是对象")

    raw_details = result.get("details")
    if not isinstance(raw_details, dict):
        raise GoodsPayloadError("AI 返回结果缺少 details")

    chinese_name = resolve_from_switch_chinese_name(
        raw_details.get("official_name"),
        raw_details.get("chinese_name"),
        detail.title,
        detail.body_text,
    )
    product_name = compose_from_switch_product_name(detail.title, chinese_name)
    if not product_name:
        raise GoodsPayloadError("来源页面没有明确商品名称")

    product_type = clean_inline_text(
        raw_details.get("product_type")
        or raw_details.get("goods_type")
        or raw_details.get("type")
    )
    if not product_type:
        raise GoodsPayloadError(f"商品 {detail.product_no} 缺少明确商品类型")

    colors = normalize_api_string_list(raw_details.get("colors"))
    if not colors:
        colors = extract_source_option_values(detail, r"skin|color|colour|肤色|颜色")
    if not colors:
        raise GoodsPayloadError(f"商品 {detail.product_no} 缺少明确颜色/肤色，不能填入虚构值")

    sizes = filter_from_switch_size_items(
        detail,
        normalize_goods_size_items(raw_details.get("supported_sizes")),
    )
    if not sizes:
        sizes = normalize_goods_size_items(
            extract_source_option_values(detail, FROM_SWITCH_SIZE_OPTION_GROUP_PATTERN)
        )

    currency = normalize_goods_currency(detail.currency or raw_details.get("currency"))
    if not currency:
        raise GoodsPayloadError(f"商品 {detail.product_no} 缺少明确币种")
    total_amount = parse_goods_integer_amount(detail.price_value)
    if total_amount <= 0:
        total_amount = parse_goods_integer_amount(raw_details.get("lowest_set_price"))

    head_circumference = clean_inline_text(
        raw_details.get("head_circumference_cm") or raw_details.get("head_circumference")
    )
    neck_circumference = clean_inline_text(
        raw_details.get("neck_circumference_cm") or raw_details.get("neck_circumference")
    )
    socket_sizes = normalize_api_string_list(
        raw_details.get("socket_sizes") or raw_details.get("socket_size")
    )
    eye_recommendations = normalize_api_string_list(
        raw_details.get("eye_recommendations") or raw_details.get("eye_recommendation")
    )

    doll_material = ""
    source_text = f"{detail.title}\n{detail.body_text}"
    if re.search(r"盲盒|blind\s*box", source_text, re.IGNORECASE):
        doll_material = "PVC"
    elif product_type == "娃衣":
        doll_material = "布制"

    primary_size = sizes[0] if sizes else {"goods_size": "", "size_detail": ""}
    return {
        "spider_id": 0,  # 调用方在发送请求前填入当前 spider_log ID。
        "name": product_name,
        "size": primary_size["goods_size"],
        "size_detail": primary_size["size_detail"],
        "sizes": sizes,
        "type": product_type,
        "skin": " ".join(colors),
        "total_amount": total_amount,
        "currency": currency,
        "head_circumference": head_circumference,
        "neck_circumference": neck_circumference,
        "socket_sizes": " ".join(socket_sizes),
        "eye_recommendations": " ".join(eye_recommendations),
        "doll_material": doll_material,
        "waiting_sale": 1,
        # 商品接口会通过 images_log.origin_url 映射到七牛地址；显式传入当前过滤后的
        # 来源列表，避免旧 images_log 中的历史图片或固定废图重新混入商品。
        "selected_image_list": list(detail.images),
    }


def parse_sse_frame(lines: Sequence[str]) -> Optional[Dict[str, Any]]:
    """解析管理后台 SSE 的一个事件帧，返回 JSON 数据对象。"""
    data_lines: List[str] = []
    for line in lines:
        value = str(line or "").rstrip("\r")
        if value.startswith("data:"):
            data_lines.append(value[5:].strip())
    if not data_lines:
        return None
    try:
        payload = json.loads("\n".join(data_lines))
    except (TypeError, ValueError) as exc:
        raise APIError(f"AI 分类 SSE 返回了无法解析的 JSON：{exc}") from exc
    return payload if isinstance(payload, dict) else None


class DogdogdollAPIClient:
    """调用现有管理后台的 AI 分类和商品创建接口。"""

    def __init__(
        self,
        session: requests.Session,
        logger: logging.Logger,
        base_url: str,
        token: str,
        timeout: float,
    ) -> None:
        """初始化管理后台 API 客户端，不在脚本中保存或读取 AI 服务商密钥。"""
        normalized_base_url = str(base_url or "").strip().rstrip("/")
        if not normalized_base_url:
            raise APIError("缺少管理后台 API 地址")
        if not token:
            raise APIError("缺少管理员 JWT，请设置 DOGDOGDOLL_ADMIN_TOKEN")
        normalized_timeout = float(timeout)
        if not math.isfinite(normalized_timeout) or normalized_timeout <= 0:
            raise APIError("管理后台 API 超时时间必须大于 0")
        self.session = session  # 复用采集会话，减少连接创建。
        self.logger = logger  # 记录阶段进度，不记录管理员令牌。
        self.base_url = normalized_base_url  # 管理后台 API 根地址。
        self.token = token  # 当前管理员 JWT，仅放在请求头中。
        self.timeout = normalized_timeout  # AI 流式读取超时。

    def _headers(self) -> Dict[str, str]:
        """生成与管理后台页面一致的鉴权请求头。"""
        return {
            "Content-Type": "application/json",
            "X-Token": self.token,
            "Authorization": self.token,
        }

    def _url(self, path: str) -> str:
        """拼接管理后台接口地址。"""
        return f"{self.base_url}/{path.lstrip('/')}"

    @staticmethod
    def _response_message(response: requests.Response) -> str:
        """提取后台错误短消息，避免把完整响应或令牌写入日志。"""
        try:
            payload = response.json()
        except (TypeError, ValueError):
            payload = None
        if isinstance(payload, dict):
            message = payload.get("msg") or payload.get("message") or payload.get("error")
            if message:
                return clean_inline_text(message)[:500]
        return clean_inline_text(response.text)[:500] or f"HTTP {response.status_code}"

    def _log_sse_frame(
        self,
        spider_log_id: int,
        frame: Dict[str, Any],
        started_at: float,
        log_state: Dict[str, Any],
    ) -> None:
        """记录一条 AI SSE 阶段信息，帮助区分图片准备、模型等待和最终返回。"""
        stage = clean_inline_text(frame.get("stage")) or "unknown"
        message = clean_inline_text(frame.get("message"))
        elapsed = time.monotonic() - started_at
        details: List[str] = []
        if stage == "preparing_images":
            total = frame.get("total")
            done = frame.get("done")
            success = frame.get("success")
            failed = frame.get("failed")
            details.append(f"图片={done}/{total}，成功={success}，失败={failed}")
        elif stage == "generating":
            char_count = int(frame.get("char_count") or 0)
            last_logged_at = float(log_state.get("generating_at") or 0.0)
            last_logged_chars = int(log_state.get("generating_chars") or -1)
            # 生成阶段可能每个 token 都发送一次 SSE；按字符数或时间节流，保留进度但避免刷屏。
            if (
                last_logged_chars >= 0
                and char_count - last_logged_chars < 100
                and elapsed - last_logged_at < 2.0
            ):
                return
            log_state["generating_at"] = elapsed
            log_state["generating_chars"] = char_count
            details.append(f"字符数={char_count}")
        elif stage == "requesting_ai":
            details.append(f"图片数={frame.get('image_count', 0)}")
        elif stage == "final_result":
            result = frame.get("result")
            if isinstance(result, dict):
                raw_details = result.get("details")
                result_details = raw_details if isinstance(raw_details, dict) else {}
                details.extend(
                    [
                        f"分类={result.get('category', '')}",
                        f"商品名={clean_inline_text(result_details.get('product_name')) or '空'}",
                        f"中文名={clean_inline_text(result_details.get('chinese_name')) or '空'}",
                    ]
                )
        if message:
            details.append(message)
        suffix = "，".join(details) if details else "无附加信息"
        self.logger.info(
            "AI 流信息：spider_id=%d，stage=%s，耗时=%.1fs，%s",
            spider_log_id,
            stage,
            elapsed,
            suffix,
        )

    def _handle_sse_frame(
        self,
        spider_log_id: int,
        frame: Optional[Dict[str, Any]],
        started_at: float,
        log_state: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """记录并处理一个 AI SSE 帧，返回最终分类结果或空值。"""
        if not frame:
            return None
        self._log_sse_frame(spider_log_id, frame, started_at, log_state)
        stage = clean_inline_text(frame.get("stage"))
        if stage == "error":
            raise APIError(
                clean_inline_text(frame.get("error") or frame.get("message"))
                or "AI 分类失败"
            )
        if stage == "final_result" and isinstance(frame.get("result"), dict):
            return frame["result"]
        return None

    def classify_goods(self, spider_log_id: int) -> Dict[str, Any]:
        """调用现有 SSE 自动分类流程，并返回最终结构化结果。"""
        payload = {
            "provider": DEFAULT_AI_PROVIDER,
            "model": DEFAULT_AI_MODEL,
            "spider_id": spider_log_id,
            "mode": "goods",
            "use_current_images_only": True,
            "prompt_suffix": FROM_SWITCH_CLASSIFICATION_PROMPT,
        }
        started_at = time.monotonic()
        log_state: Dict[str, Any] = {}
        self.logger.info(
            "AI 流开始：spider_id=%d，provider=%s，model=%s，读取超时=%.0fs",
            spider_log_id,
            DEFAULT_AI_PROVIDER,
            DEFAULT_AI_MODEL,
            self.timeout,
        )
        try:
            response = self.session.post(
                self._url("/admin/spider-kimi-analyze-sse"),
                headers=self._headers(),
                json=payload,
                stream=True,
                timeout=(10.0, self.timeout),
            )
        except requests.RequestException as exc:
            raise APIError(f"请求 AI 分类接口失败：{exc}") from exc

        frame_lines: List[str] = []
        try:
            if response.status_code >= 400:
                raise APIError(f"AI 分类接口失败：{self._response_message(response)}")
            for raw_line in response.iter_lines(decode_unicode=True):
                line = str(raw_line or "")
                if not line.strip():
                    if frame_lines:
                        frame = parse_sse_frame(frame_lines)
                        frame_lines = []
                        result = self._handle_sse_frame(spider_log_id, frame, started_at, log_state)
                        if result is not None:
                            return result
                    continue
                frame_lines.append(line)
            if frame_lines:
                frame = parse_sse_frame(frame_lines)
                result = self._handle_sse_frame(spider_log_id, frame, started_at, log_state)
                if result is not None:
                    return result
        except requests.RequestException as exc:
            self.logger.error(
                "AI 流读取失败：spider_id=%d，耗时=%.1fs，reason=%s",
                spider_log_id,
                time.monotonic() - started_at,
                exc,
            )
            raise APIError(f"读取 AI 分类流失败：{exc}") from exc
        finally:
            response.close()
        self.logger.error(
            "AI 流结束但没有 final_result：spider_id=%d，耗时=%.1fs",
            spider_log_id,
            time.monotonic() - started_at,
        )
        raise APIError("AI 分类接口未返回 final_result")

    def create_goods(self, payload: Dict[str, Any]) -> Any:
        """调用现有采集创建商品接口，不发送 sale_record 或新闻导入请求。"""
        try:
            response = self.session.post(
                self._url("/admin/merchant-create-goods-by-spd"),
                headers=self._headers(),
                json=payload,
                timeout=(10.0, min(60.0, self.timeout)),
            )
        except requests.RequestException as exc:
            raise APIError(f"创建商品接口请求失败：{exc}") from exc
        if response.status_code >= 400:
            raise APIError(f"创建商品接口失败：{self._response_message(response)}")
        try:
            body = response.json()
        except (TypeError, ValueError) as exc:
            raise APIError(f"创建商品接口返回不是 JSON：{exc}") from exc
        if not isinstance(body, dict) or body.get("status") != "success":
            message = body.get("msg") if isinstance(body, dict) else ""
            raise APIError(f"创建商品失败：{clean_inline_text(message) or '后台未返回成功状态'}")
        return body.get("data")


class BrandSpider145:
    """From Switch DOLL 分类的一次性采集执行器。"""

    def __init__(
        self,
        config: SpiderConfig,
        http_client: HttpClient,
        database: Optional[SpiderLogDatabase],
        api_client: Optional[DogdogdollAPIClient],
        logger: logging.Logger,
    ) -> None:
        """初始化采集配置、HTTP 客户端和可选数据库。"""
        self.config = config  # 当前运行参数。
        self.http_client = http_client  # 负责分类页和详情页请求。
        self.database = database  # dry-run 时为空，其它模式必须存在。
        self.api_client = api_client  # 自动商品模式使用的管理后台 API 客户端。
        self.logger = logger  # 统一输出运行进度。
        self.seen_product_nos: Set[str] = set()  # 本次运行内商品编号去重集合。
        self.seen_page_signatures: Set[Tuple[str, ...]] = set()  # 防止分页响应重复导致死循环。

    def _sleep_before_detail(self) -> None:
        """在打开下一个详情页前等待一个小幅随机间隔。"""
        base_delay = max(0.0, float(self.config.detail_delay))
        if base_delay <= 0:
            return
        delay = random.uniform(base_delay * 0.8, base_delay * 1.2)
        time.sleep(delay)

    def _fetch_category_page(self, page_no: int) -> Tuple[List[ProductCard], int]:
        """读取并解析指定分类页。"""
        page_url = build_category_page_url(
            self.config.category_url,
            self.config.category_no,
            page_no,
        )
        response = self.http_client.get(page_url)
        cards, last_page = parse_category_page(response.content, page_no, page_url)
        self.logger.info("分类页 %d：发现 %d 个商品，站点最后页=%d", page_no, len(cards), last_page)
        return cards, last_page

    def _classify_and_create_goods(
        self,
        detail: ProductDetail,
        spider_log_id: int,
        stats: CrawlStats,
    ) -> None:
        """调用既有 AI 分类流程，并将 From Switch 每条详情按商品创建。"""
        if self.api_client is None:
            raise APIError("自动商品模式缺少管理后台 API 客户端")
        result = self.api_client.classify_goods(spider_log_id)
        stats.classifications_succeeded += 1
        if not isinstance(result, dict):
            raise GoodsPayloadError("AI 返回结果不是对象")
        raw_details = result.get("details") if isinstance(result.get("details"), dict) else {}
        try:
            category = int(result.get("category") or 0)
        except (TypeError, ValueError):
            category = 0
        if category != 3:
            self.logger.info(
                "From Switch 来源固定按商品处理：AI 分类为 %d，继续创建商品：product_no=%s，spider_id=%d",
                category,
                detail.product_no,
                spider_log_id,
            )

        goods_payload = build_from_switch_goods_payload(detail, result)
        goods_payload["spider_id"] = spider_log_id
        self.logger.info(
            "尺寸归一化：spider_id=%d，AI原始=%s，商品载荷=%s",
            spider_log_id,
            raw_details.get("supported_sizes", []),
            goods_payload.get("sizes", []),
        )
        self.api_client.create_goods(goods_payload)
        stats.goods_created += 1
        self.logger.info(
            "已创建商品：product_no=%s，spider_id=%d，name=%s",
            detail.product_no,
            spider_log_id,
            goods_payload["name"],
        )

    def _process_existing_record(self, record: SpiderLogRecord, stats: CrawlStats) -> None:
        """读取一条已有采集记录并调用现有 AI 分类，不重新请求来源详情页。"""
        product_no = extract_product_no_from_url(record.url) or f"spider_id={record.spider_log_id}"
        try:
            detail = product_detail_from_spider_log(record)
            stats.images_collected += len(detail.images)
            if not self.config.auto_create_goods:
                self.logger.info(
                    "已有记录已读取但未启用自动商品模式：product_no=%s，spider_id=%d",
                    detail.product_no,
                    record.spider_log_id,
                )
                return
            self._classify_and_create_goods(detail, record.spider_log_id, stats)
        except (PageFetchError, PageParseError, SpiderError, requests.RequestException) as exc:
            reason = str(exc)
            stats.add_failure(0, product_no, record.url, reason)
            self.logger.error(
                "已有记录分类失败：spider_id=%d，product_no=%s，reason=%s",
                record.spider_log_id,
                product_no,
                reason,
            )

    def _log_stats(self, stats: CrawlStats, message: str) -> None:
        """输出一次分类或采集任务的统一统计信息。"""
        self.logger.info(
            "%s：pages=%d/%d，records=%d，discovered=%d，details=%d，inserted=%d，duplicates=%d，existing_skipped=%d，classified=%d，goods_created=%d，failures=%d，images=%d，option_groups=%d，sku_items=%d",
            message,
            stats.pages_visited,
            stats.last_page,
            stats.existing_records_loaded,
            stats.products_discovered,
            stats.details_success,
            stats.inserted,
            stats.duplicates,
            stats.existing_skipped,
            stats.classifications_succeeded,
            stats.goods_created,
            len(stats.failures),
            stats.images_collected,
            stats.option_groups_collected,
            stats.sku_items_collected,
        )
        if stats.failures:
            self.logger.error("失败商品清单（共 %d 条）：", len(stats.failures))
            for failure in stats.failures:
                self.logger.error(
                    "  page=%s product_no=%s url=%s reason=%s",
                    failure["page"],
                    failure["product_no"],
                    failure["url"],
                    failure["reason"],
                )

    def _run_existing_records(self, stats: CrawlStats) -> CrawlStats:
        """从数据库读取 From Switch 未处理记录并逐条执行 AI 分类。"""
        if self.database is None:
            raise DatabaseError("已有数据模式缺少数据库连接")
        records = self.database.list_unprocessed_records(
            self.config.brand_id,
            DEFAULT_ORIGIN_TYPE,
            self.config.limit_products,
        )
        stats.products_discovered = len(records)
        stats.existing_records_loaded = len(records)
        self.logger.info(
            "已有数据模式：读取到 %d 条未处理 From Switch 采集记录，不访问分类页和商品详情页",
            len(records),
        )
        for record in records:
            self._process_existing_record(record, stats)
        self._log_stats(stats, "已有数据分类完成")
        return stats

    def _process_card(self, card: ProductCard, stats: CrawlStats, brand_name: str) -> None:
        """读取、解析并按需写入一个商品详情。"""
        if not self.config.dry_run and self.database is not None:
            existing = self.database.find_by_source_product_no(
                card.product_no,
                self.config.brand_id,
                DEFAULT_ORIGIN_TYPE,
            )
            if existing is not None and existing.status == 2:
                stats.duplicates += 1
                stats.existing_skipped += 1
                self.logger.info(
                    "分类页已命中已处理记录，跳过详情请求：product_no=%s，spider_id=%d",
                    card.product_no,
                    existing.spider_log_id,
                )
                return
            if existing is not None and not self.config.auto_create_goods:
                stats.duplicates += 1
                stats.existing_skipped += 1
                self.logger.info(
                    "分类页已命中未处理记录，当前模式跳过详情请求：product_no=%s，spider_id=%d",
                    card.product_no,
                    existing.spider_log_id,
                )
                return
            if existing is not None and not self.config.retry_pending:
                stats.duplicates += 1
                stats.existing_skipped += 1
                self.logger.info(
                    "分类页已命中未处理记录，默认跳过详情请求：product_no=%s，spider_id=%d；需要重试请加 --retry-pending",
                    card.product_no,
                    existing.spider_log_id,
                )
                return

        self._sleep_before_detail()
        try:
            response = self.http_client.get(card.url, referer=build_category_page_url(
                self.config.category_url,
                self.config.category_no,
                card.page_no,
            ))
            detail = parse_product_detail(response.content, card.url, fallback_cover=card.cover_image)
            if detail.product_no != card.product_no:
                raise PageParseError(
                    f"详情商品编号与分类页不一致: list={card.product_no}, detail={detail.product_no}"
                )
            stats.details_success += 1
            stats.images_collected += len(detail.images)
            stats.option_groups_collected += len(detail.option_groups)
            stats.sku_items_collected += len(detail.sku_items)

            if self.config.dry_run:
                self.logger.info(
                    "dry-run 商品 %s：标题=%s，价格=%s，图片=%d，选项组=%d，SKU=%d",
                    detail.product_no,
                    detail.title,
                    detail.price_display or "未提供",
                    len(detail.images),
                    len(detail.option_groups),
                    len(detail.sku_items),
                )
                return
            if self.database is None:
                raise DatabaseError("非 dry-run 模式缺少数据库连接")
            write_result = self.database.insert_product(detail, self.config.brand_id, brand_name)
            if write_result.inserted:
                stats.inserted += 1
                self.logger.info(
                    "已写入 spider_log：product_no=%s，spider_id=%d，title=%s",
                    detail.product_no,
                    write_result.spider_log_id,
                    detail.title,
                )
            else:
                stats.duplicates += 1
                if write_result.status == 2:
                    self.logger.info(
                        "已存在且已处理，跳过：product_no=%s，spider_id=%d，url=%s",
                        detail.product_no,
                        write_result.spider_log_id,
                        detail.url,
                    )
                    return
                if not self.config.auto_create_goods:
                    self.logger.info(
                        "已存在但未处理，当前模式跳过：product_no=%s，spider_id=%d",
                        detail.product_no,
                        write_result.spider_log_id,
                    )
                    return
                if not self.config.retry_pending:
                    self.logger.info(
                        "已存在但未处理，默认跳过：product_no=%s，spider_id=%d；需要重试请加 --retry-pending",
                        detail.product_no,
                        write_result.spider_log_id,
                    )
                    return
                self.logger.info(
                    "已存在但未处理，开始重试：product_no=%s，spider_id=%d",
                    detail.product_no,
                    write_result.spider_log_id,
                )
                # 旧的未处理记录可能已经包含废图或被后台标为 full_get=1；
                # 重新写入当前详情中的过滤后图片，让既有上传流程从干净来源开始。
                self.database.replace_unprocessed_images(write_result.spider_log_id, detail.images)
                self.logger.info(
                    "已清理未处理旧记录图片并等待重新上传：spider_id=%d",
                    write_result.spider_log_id,
                )

            if self.config.auto_create_goods:
                self._classify_and_create_goods(detail, write_result.spider_log_id, stats)
        except (PageFetchError, PageParseError, SpiderError, requests.RequestException) as exc:
            reason = str(exc)
            stats.add_failure(card.page_no, card.product_no, card.url, reason)
            self.logger.error(
                "商品处理失败：page=%d，product_no=%s，url=%s，reason=%s",
                card.page_no,
                card.product_no,
                card.url,
                reason,
            )

    def run(self) -> CrawlStats:
        """默认分类已有采集记录；仅在显式采集模式下读取来源分类页和详情页。"""
        if self.config.start_page <= 0:
            raise SpiderError("start_page 必须大于 0")
        if self.config.end_page is not None and self.config.end_page < self.config.start_page:
            raise SpiderError("end_page 不能小于 start_page")
        if self.config.limit_products < 0:
            raise SpiderError("limit_products 不能小于 0")

        stats = CrawlStats()
        brand_name = ""
        if not self.config.dry_run:
            if self.database is None:
                raise DatabaseError("非 dry-run 模式缺少数据库连接")
            brand_name = self.database.fetch_brand_name(self.config.brand_id)
            self.logger.info("已校验品牌：id=%d，name=%s", self.config.brand_id, brand_name)

        if self.config.existing_only:
            if self.config.dry_run:
                raise SpiderError("已有数据模式不能与 --dry-run 同时使用；如需试采集请加 --collect-new")
            return self._run_existing_records(stats)

        first_page_cards, discovered_last_page = self._fetch_category_page(1)
        if not first_page_cards:
            raise PageParseError("分类第 1 页没有商品卡片，无法开始采集")
        stats.last_page = discovered_last_page
        end_page = self.config.end_page or discovered_last_page
        end_page = min(end_page, discovered_last_page)
        first_page_cache = first_page_cards

        for page_no in range(self.config.start_page, end_page + 1):
            cards = first_page_cache if page_no == 1 else self._fetch_category_page(page_no)[0]
            stats.pages_visited += 1
            if not cards:
                self.logger.warning("分类页 %d 没有商品，提前结束分页", page_no)
                break
            signature = tuple(card.product_no for card in cards)
            if signature in self.seen_page_signatures:
                self.logger.warning("分类页 %d 与之前页面商品编号完全重复，停止分页", page_no)
                break
            self.seen_page_signatures.add(signature)

            for card in cards:
                if self.config.limit_products > 0 and stats.products_discovered >= self.config.limit_products:
                    break
                if card.product_no in self.seen_product_nos:
                    continue
                self.seen_product_nos.add(card.product_no)
                stats.products_discovered += 1
                self._process_card(card, stats, brand_name)
            if self.config.limit_products > 0 and stats.products_discovered >= self.config.limit_products:
                self.logger.info("已达到 --limit-products=%d，停止继续翻页", self.config.limit_products)
                break

        self._log_stats(stats, "新商品采集完成")
        return stats


def env_int(name: str, default: int) -> int:
    """读取整数环境变量，空值或非法值时使用默认值。"""
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    """读取浮点环境变量，空值、非法值或非有限值时使用默认值。"""
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return default
    try:
        value = float(raw_value)
    except ValueError:
        return default
    return value if math.isfinite(value) else default


def build_argument_parser() -> argparse.ArgumentParser:
    """构造命令行参数解析器。"""
    parser = argparse.ArgumentParser(description="分类已有 From Switch DOLL 采集记录并创建商品")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--existing-only",
        dest="existing_only",
        action="store_true",
        default=True,
        help="只读取数据库已有未处理记录，默认开启",
    )
    mode_group.add_argument(
        "--collect-new",
        dest="existing_only",
        action="store_false",
        help="访问 From Switch 分类页并采集新商品，需显式指定",
    )
    parser.add_argument("--category-url", default=DEFAULT_CATEGORY_URL, help="分类页入口")
    parser.add_argument("--start-page", type=int, default=1, help="起始页，默认 1")
    parser.add_argument("--end-page", type=int, default=None, help="结束页，缺省使用站点最后页")
    parser.add_argument("--limit-products", type=int, default=0, help="试跑商品数，0 表示不限")
    parser.add_argument(
        "--detail-delay",
        type=float,
        default=DEFAULT_DETAIL_DELAY,
        help=f"详情页之间的基础等待秒数，默认 {DEFAULT_DETAIL_DELAY}",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"单个 URL 最大请求次数，默认 {DEFAULT_MAX_RETRIES}",
    )
    parser.add_argument("--dry-run", action="store_true", help="只解析和打印摘要，不写数据库")
    parser.add_argument(
        "--auto-create-goods",
        action="store_true",
        help="调用现有 AI 分类流程；From Switch 每条详情都自动创建商品，不创建贩售记录和新闻",
    )
    parser.add_argument(
        "--retry-pending",
        action="store_true",
        help="采集新商品模式下重试已有未处理记录；已有数据模式会自动处理未处理记录",
    )
    parser.add_argument(
        "--api-base-url",
        default=os.environ.get("DOGDOGDOLL_API_BASE_URL", DEFAULT_API_BASE_URL),
        help=f"管理后台 API 地址，默认 {DEFAULT_API_BASE_URL}",
    )
    parser.add_argument(
        "--api-timeout",
        type=float,
        default=env_float("DOGDOGDOLL_API_TIMEOUT", DEFAULT_API_TIMEOUT),
        help=f"AI 流式响应读取超时秒数，默认 {DEFAULT_API_TIMEOUT}",
    )
    parser.add_argument("--log-file", default=None, help="日志文件路径，缺省写入脚本同目录")
    parser.add_argument("--db-host", default=os.environ.get("SPIDER_DB_HOST", "222.186.135.83"))
    parser.add_argument("--db-port", type=int, default=env_int("SPIDER_DB_PORT", 3306))
    parser.add_argument("--db-user", default=os.environ.get("SPIDER_DB_USER", "sukitime_remote"))
    parser.add_argument("--db-name", default=os.environ.get("SPIDER_DB_NAME", "sukitime"))
    return parser


def configure_logging(log_file: Optional[str]) -> logging.Logger:
    """配置控制台和文件日志，并返回采集器专用 logger。"""
    logger = logging.getLogger("brand_spider_145")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    resolved_log_file = log_file or str(Path(__file__).resolve().with_name("brand_spider_145.log"))
    try:
        file_handler = logging.FileHandler(resolved_log_file, encoding="utf-8")
    except OSError as exc:
        logger.warning("日志文件不可写，仅保留控制台日志：%s", exc)
    else:
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


def config_from_args(args: argparse.Namespace) -> SpiderConfig:
    """把命令行参数和环境变量转换成 SpiderConfig。"""
    return SpiderConfig(
        category_url=args.category_url,
        start_page=args.start_page,
        end_page=args.end_page,
        limit_products=args.limit_products,
        detail_delay=args.detail_delay,
        max_retries=args.max_retries,
        dry_run=args.dry_run,
        log_file=args.log_file,
        db_host=args.db_host,
        db_port=args.db_port,
        db_user=args.db_user,
        db_password=os.environ.get("SPIDER_DB_PASSWORD", ""),
        db_name=args.db_name,
        auto_create_goods=args.auto_create_goods,
        retry_pending=args.retry_pending,
        existing_only=args.existing_only,
        api_base_url=args.api_base_url,
        api_token=os.environ.get("DOGDOGDOLL_ADMIN_TOKEN", "").strip(),
        api_timeout=args.api_timeout,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """执行命令行采集任务并返回适合 shell 使用的退出码。"""
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    logger = configure_logging(args.log_file)
    config = config_from_args(args)
    if config.auto_create_goods:
        if config.existing_only:
            logger.info("运行模式：只分类数据库已有未处理记录，不访问 From Switch 分类页和详情页")
        else:
            logger.info(
                "新商品采集模式：%s",
                "已有未处理记录也会重试（--retry-pending）" if config.retry_pending else "已有未处理记录默认跳过（需要时加 --retry-pending）",
            )
    database: Optional[SpiderLogDatabase] = None
    session: Optional[requests.Session] = None
    try:
        if config.auto_create_goods and config.dry_run:
            raise SpiderError("--auto-create-goods 不能与 --dry-run 同时使用")
        if not config.dry_run:
            database = SpiderLogDatabase(config, logger)
        session = requests.Session()
        http_client = HttpClient(session, logger, config.max_retries)
        api_client = (
            DogdogdollAPIClient(
                session,
                logger,
                config.api_base_url,
                config.api_token,
                config.api_timeout,
            )
            if config.auto_create_goods
            else None
        )
        spider = BrandSpider145(config, http_client, database, api_client, logger)
        stats = spider.run()
        return 2 if stats.failures else 0
    except KeyboardInterrupt:
        logger.warning("收到中断信号，任务停止")
        return 130
    except (SpiderError, requests.RequestException) as exc:
        logger.error("任务终止：%s", exc)
        return 1
    finally:
        if session is not None:
            session.close()
        if database is not None:
            database.close()


if __name__ == "__main__":
    raise SystemExit(main())
