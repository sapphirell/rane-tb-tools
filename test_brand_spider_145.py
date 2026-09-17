# -*- coding: utf-8 -*-
"""brand_spider_145.py 的离线解析测试。"""

import logging
import unittest

from brand_spider_145 import (
    BrandSpider145,
    CrawlStats,
    FROM_SWITCH_CLASSIFICATION_PROMPT,
    FROM_SWITCH_WASTE_IMAGE_URL,
    DogdogdollAPIClient,
    GoodsPayloadError,
    build_category_page_url,
    build_argument_parser,
    build_from_switch_goods_payload,
    build_spider_content,
    calculate_url_md5,
    compose_from_switch_product_name,
    find_from_switch_official_name,
    filter_from_switch_size_items,
    is_from_switch_empty_option,
    is_from_switch_size_option_group,
    is_from_switch_waste_image_url,
    normalize_api_string_list,
    normalize_from_switch_size_pair,
    normalize_goods_size_items,
    parse_sse_frame,
    parse_category_page,
    parse_product_detail,
    product_detail_from_spider_log,
    ProductCard,
    resolve_from_switch_chinese_name,
    SpiderConfig,
    SpiderLogRecord,
    SpiderLogWriteResult,
)


CATEGORY_HTML = """
<html>
  <body>
    <div class="xans-element- xans-product xans-product-listnormal">
      <ul class="prdList column4">
        <li id="anchorBoxId_670" class="item xans-record-">
          <div class="box">
            <a href="/product/detail.html?product_no=670&amp;cate_no=25&amp;display_group=1" name="anchorBoxName_670">
              <img src="//from-switch.com/web/product/medium/202412/cover.jpg" class="thumb" />
            </a>
            <p class="name">
              <a href="/product/detail.html?product_no=670&amp;cate_no=25&amp;display_group=1">
                <strong class="title displaynone">Product Name :</strong>
                <span>LIS</span><br />
              </a>
            </p>
          </div>
        </li>
      </ul>
    </div>
    <div class="xans-element- xans-product xans-product-normalpaging">
      <ol><li><a href="?page=1">1</a></li></ol>
      <p><a href="?page=17"><img alt="Last" src="last.gif" /></a></p>
    </div>
  </body>
</html>
"""


DETAIL_E_HTML = r"""
<html>
  <head><link rel="canonical" href="https://from-switch.com/product/lis/670/" /></head>
  <body>
    <div class="xans-element- xans-product xans-product-detail"><div class="detailArea">
      <div class="xans-product-image imgArea">
        <div class="keyImg"><img class="BigImage" src="//from-switch.com/web/product/big/main.jpg" /></div>
        <div class="listImg"><img class="ThumbImage" ec-data-src="/web/product/small/extra.jpg" /></div>
      </div>
      <div class="infoArea"><span>LIS</span></div>
      <table><caption>Basic Information</caption>
        <tr><th>Price</th><td><strong id="span_product_price_text">USD 150.00</strong></td></tr>
      </table>
      <table><caption>Product Option</caption>
        <tr class="xans-product-option"><th>Skin</th><td>
          <select option_product_no="670" option_sort_no="1" option_type="E" option_title="Skin" required="true">
            <option value="*" selected="selected">- [Required] Please select options. -</option>
            <option value="**" disabled="disabled">-------------------</option>
            <option value="4171">Rosy White</option>
            <option value="4173">Milktea Rose (+USD 40.00)</option>
          </select>
        </td></tr>
      </table>
    </div></div>
    <div id="prdDetail"><div class="cont"><p>Line one<br />Line two</p>
      <img ec-data-src="/web/upload/detail/body.jpg" />
      <img src="https://from-switch.com/web/upload/category/editor/2020/03/02/4ec60c80c2204fa0e8a3f2f2d357c396.gif" />
    </div></div>
    <script>
      var iProductNo = 670;
      var product_price = '150';
      var option_stock_data = '{"4171":{"stock_number":3,"option_price":"0.00","use_soldout":"F","is_selling":"T"},"4173":{"stock_number":0,"option_price":"40.00","use_soldout":"T","is_selling":"T"},"P00000ZU":{"stock_number":0}}';
    </script>
  </body>
</html>
"""


DETAIL_T_HTML = r"""
<html>
  <head><link rel="canonical" href="/product/cosmos/414/" /></head>
  <body>
    <div class="xans-product xans-product-detail"><div class="detailArea">
      <div class="infoArea"><span>Cosmos</span></div>
      <table><caption>Basic Information</caption>
        <tr><th>Price</th><td><strong id="span_product_price_text">USD 23.00</strong></td></tr>
      </table>
      <table><caption>Product Option</caption>
        <tr><th>Size</th><td><select option_product_no="414" option_sort_no="1" option_type="T" option_title="Size" required="true">
          <option value="*">- select -</option><option value="M size(8.5 inch)">M size(8.5 inch)</option>
        </select></td></tr>
        <tr><th>Color</th><td><select option_product_no="414" option_sort_no="2" option_type="T" option_title="Color" required="true">
          <option value="*">- select -</option><option value="Milktea Gray" disabled="disabled">Milktea Gray</option>
        </select></td></tr>
      </table>
    </div></div>
    <div id="prdDetail"><div class="cont"><p>Wig description.</p></div></div>
    <script>
      var iProductNo = 414;
      var option_stock_data = '{"P0000001":{"stock_number":4,"option_price":23,"option_value":"M size(8.5 inch)-Milktea Gray","option_value_orginal":["M size(8.5 inch)","Milktea Gray"],"option_name_original":["Size","Color"],"is_selling":"T","use_soldout":"F"}}';
    </script>
  </body>
</html>
"""


class BrandSpider145ParserTest(unittest.TestCase):
    """验证页面解析和数据库正文格式的稳定契约。"""

    def test_category_page_keeps_category_when_building_page_url(self):
        """分页 URL 必须同时保留 cate_no 和 page。"""
        url = build_category_page_url(
            "https://from-switch.com/product/list.html?cate_no=25",
            25,
            2,
        )
        self.assertEqual(url, "https://from-switch.com/product/list.html?cate_no=25&page=2")

    def test_category_page_extracts_card_and_last_page(self):
        """分类页应提取商品编号、标题、详情地址、封面和最后页。"""
        cards, last_page = parse_category_page(
            CATEGORY_HTML,
            1,
            "https://from-switch.com/product/list.html?cate_no=25",
        )
        self.assertEqual(last_page, 17)
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0].product_no, "670")
        self.assertEqual(cards[0].title, "LIS")
        self.assertEqual(cards[0].url, "https://from-switch.com/product/detail.html?product_no=670&cate_no=25&display_group=1")
        self.assertEqual(cards[0].cover_image, "https://from-switch.com/web/product/medium/202412/cover.jpg")

    def test_detail_e_extracts_lazy_images_price_options_and_skus(self):
        """E 类型详情页应保留延迟图片、选项加价和选项编号。"""
        detail = parse_product_detail(
            DETAIL_E_HTML,
            "https://from-switch.com/product/detail.html?product_no=670&cate_no=25",
        )
        self.assertEqual(detail.product_no, "670")
        self.assertEqual(detail.title, "LIS")
        self.assertEqual(detail.url, "https://from-switch.com/product/lis/670/")
        self.assertEqual(detail.price_display, "USD 150.00")
        self.assertEqual(detail.currency, "USD")
        self.assertEqual(detail.price_value, "150.00")
        self.assertEqual(
            detail.images,
            [
                "https://from-switch.com/web/product/big/main.jpg",
                "https://from-switch.com/web/product/small/extra.jpg",
                "https://from-switch.com/web/upload/detail/body.jpg",
            ],
        )
        self.assertEqual(detail.body_text, "Line one\nLine two")
        self.assertEqual(len(detail.option_groups), 1)
        self.assertEqual(detail.option_groups[0].name, "Skin")
        self.assertEqual(detail.option_groups[0].values[1].label, "Milktea Rose")
        self.assertEqual(detail.option_groups[0].values[1].price_adjustment, "USD 40.00")
        self.assertEqual(len(detail.sku_items), 2)
        self.assertEqual(detail.sku_items[1].sku_code, "4173")
        self.assertEqual(detail.sku_items[1].use_soldout, "T")

    def test_detail_t_extracts_all_groups_and_combination_sku(self):
        """T 类型详情页应保留全部选项组和组合 SKU。"""
        detail = parse_product_detail(
            DETAIL_T_HTML,
            "https://from-switch.com/product/detail.html?product_no=414&cate_no=25",
        )
        self.assertEqual([group.name for group in detail.option_groups], ["Size", "Color"])
        self.assertEqual(len(detail.sku_items), 1)
        self.assertEqual(detail.sku_items[0].sku_code, "P0000001")
        self.assertEqual(detail.sku_items[0].option_values, ["M size(8.5 inch)", "Milktea Gray"])
        self.assertEqual(detail.sku_items[0].option_names, ["Size", "Color"])
        self.assertEqual(detail.sku_items[0].option_price, "23")

    def test_spider_content_contains_price_options_and_body(self):
        """写入 content 的固定格式应同时包含价格、规格和正文。"""
        detail = parse_product_detail(
            DETAIL_E_HTML,
            "https://from-switch.com/product/detail.html?product_no=670&cate_no=25",
        )
        content = build_spider_content(detail)
        self.assertIn("商品价格：USD 150.00", content)
        self.assertIn("Skin：Milktea Rose（value=4173；加价=USD 40.00；必选）", content)
        self.assertIn("sku_code=4173", content)
        self.assertIn("商品正文：\nLine one\nLine two", content)

    def test_url_md5_ignores_query_and_trailing_slash(self):
        """同一个商品的 canonical 和带 query 详情地址应产生同一哈希。"""
        self.assertEqual(
            calculate_url_md5("https://from-switch.com/product/lis/670/"),
            calculate_url_md5("https://from-switch.com/product/lis/670/?cate_no=25&display_group=1"),
        )

    def test_utf8_bytes_keep_non_ascii_product_title(self):
        """响应是 UTF-8 字节时，中文商品名称不能被解析成乱码。"""
        html_text = DETAIL_E_HTML.replace("<span>LIS</span>", "<span>徐夏</span>")
        detail = parse_product_detail(
            html_text.encode("utf-8"),
            "https://from-switch.com/product/detail.html?product_no=670&cate_no=25",
        )
        self.assertEqual(detail.title, "徐夏")

    def test_fixed_editor_waste_image_is_excluded(self):
        """固定编辑器废图应在进入 spider_log 前被排除，含 query/http 变体也不能漏回。"""
        self.assertTrue(is_from_switch_waste_image_url(FROM_SWITCH_WASTE_IMAGE_URL + "?v=1"))
        self.assertTrue(is_from_switch_waste_image_url(FROM_SWITCH_WASTE_IMAGE_URL.replace("https://", "http://")))
        self.assertFalse(is_from_switch_waste_image_url(FROM_SWITCH_WASTE_IMAGE_URL.replace("from-switch.com", "example.com")))
        detail = parse_product_detail(
            DETAIL_E_HTML,
            "https://from-switch.com/product/detail.html?product_no=670&cate_no=25",
        )
        self.assertNotIn(FROM_SWITCH_WASTE_IMAGE_URL, detail.images)

    def test_from_switch_prompt_contains_mapping_and_no_guess_rule(self):
        """临时提示词应包含中文对照、精确变体匹配和不相关时留空的要求。"""
        for expected in [
            "CHOYO robin => 瞳谣R（开嘴）",
            "SANHONG => 山红",
            "YIDO inferno => 理度 半眠",
            "YUNSEOL: Tokki => 尹雪 兔子",
            "SEOLROK: Hiver => 樰绿 Hiver",
            "TAERIN bijou => 大湊 闭嘴",
            "GYOHA mobius => 皎河m（搞m）",
            "TAERIN => 大湊",
            "不相关时必须返回空字符串",
            "只能从下面的“英文官名→中文名白名单”中选择",
            "official_name 与 chinese_name 必须来自白名单中的同一行",
            "每条详情都必须继续创建商品",
            "sale 必须返回 null",
            "No option",
            "Body painting",
            "尺寸明确时",
            "不能因为已识别父级就只返回父级",
            "大四分",
        ]:
            self.assertIn(expected, FROM_SWITCH_CLASSIFICATION_PROMPT)

    def test_no_option_is_treated_as_empty_value(self):
        """站点的无选项占位文本不能进入颜色、尺寸或商品字段。"""
        self.assertTrue(is_from_switch_empty_option(" No option "))
        self.assertTrue(is_from_switch_empty_option("None"))
        self.assertTrue(is_from_switch_size_option_group(" Size "))
        self.assertFalse(is_from_switch_size_option_group("Body painting"))
        self.assertFalse(is_from_switch_size_option_group("Chest parts"))
        self.assertEqual(normalize_api_string_list(["No option", "Powder Beige"]), ["Powder Beige"])
        self.assertEqual(
            normalize_goods_size_items(["No option", "六分/普通六分"]),
            [{"goods_size": "六分", "size_detail": "普通六分"}],
        )
        self.assertEqual(
            normalize_goods_size_items(["65成男", "70+普成男"]),
            [
                {"goods_size": "其它大尺寸", "size_detail": "65成男"},
                {"goods_size": "其它大尺寸", "size_detail": "70+普成男"},
            ],
        )
        self.assertEqual(
            normalize_goods_size_items(["其它大尺寸", "65成男"]),
            [{"goods_size": "其它大尺寸", "size_detail": "65成男"}],
        )
        self.assertEqual(
            normalize_from_switch_size_pair("65成男", ""),
            ("其它大尺寸", "65成男"),
        )

    def test_existing_record_is_reused_without_source_request(self):
        """已有 spider_log 应能直接转换为商品详情，不依赖来源站点响应。"""
        record = SpiderLogRecord(
            7001,
            1,
            0,
            1,
            "https://from-switch.com/web/product/big/main.jpg,"
            + FROM_SWITCH_WASTE_IMAGE_URL,
            title="SANHONG MAKE UP",
            content="商品编号：414\n商品价格：USD 23.00\n商品正文：\nMilktea Gray",
            url="https://from-switch.com/product/cosmos/414/",
        )
        detail = product_detail_from_spider_log(record)
        self.assertEqual(detail.product_no, "414")
        self.assertEqual(detail.title, "SANHONG MAKE UP")
        self.assertEqual(detail.currency, "USD")
        self.assertEqual(detail.price_value, "23.00")
        self.assertEqual(detail.images, ["https://from-switch.com/web/product/big/main.jpg"])

    def test_existing_only_is_default_and_collect_new_is_explicit(self):
        """命令行默认只处理已有记录，采集新商品必须显式指定。"""
        parser = build_argument_parser()
        self.assertTrue(parser.parse_args([]).existing_only)
        self.assertFalse(parser.parse_args(["--collect-new"]).existing_only)

    def test_product_name_only_accepts_whitelisted_chinese_name(self):
        """格式化阶段仍应拒绝未知中文名，并避免重复追加。"""
        self.assertEqual(compose_from_switch_product_name("SANHONG", "山红"), "SANHONG（山红）")
        self.assertEqual(compose_from_switch_product_name("SANHONG（山红）", "山红"), "SANHONG（山红）")
        self.assertEqual(compose_from_switch_product_name("SANHONG", "不存在的名字"), "SANHONG")

    def test_chinese_name_requires_matching_pair_and_source_evidence(self):
        """中文名必须与 AI 返回的官名成对，并能在来源文字中找到最具体的官名。"""
        self.assertEqual(find_from_switch_official_name("YIDO inferno MAKE UP"), "YIDO inferno")
        self.assertEqual(find_from_switch_official_name("SANHONG / YIDO"), "")
        self.assertEqual(
            resolve_from_switch_chinese_name("sanhong", "（山红）", "SANHONG MAKE UP", ""),
            "山红",
        )
        self.assertEqual(
            resolve_from_switch_chinese_name("SANHONG", "理度", "SANHONG MAKE UP", ""),
            "",
        )
        self.assertEqual(
            resolve_from_switch_chinese_name("YIDO", "理度", "YIDO inferno MAKE UP", ""),
            "",
        )
        self.assertEqual(
            resolve_from_switch_chinese_name("SANHONG", "山红", "Cosmos", "Wig description"),
            "",
        )

    def test_goods_payload_has_no_category_gate_and_omits_sale(self):
        """From Switch 商品载荷不以 AI 分类结果为创建门槛，且不携带贩售记录或新闻字段。"""
        detail = parse_product_detail(
            DETAIL_T_HTML,
            "https://from-switch.com/product/detail.html?product_no=414&cate_no=25",
        )
        detail.title = "SANHONG MAKE UP"
        details = {
            "product_name": "AI 不应覆盖来源商品名",
            "official_name": "SANHONG",
            "chinese_name": "山红",
            "product_type": "假发",
            "colors": [],
            "supported_sizes": [],
            "currency": "USD",
        }
        payload = build_from_switch_goods_payload(
            detail,
            {
                "category": 3,
                "details": details,
            },
        )
        self.assertEqual(payload["name"], "SANHONG MAKE UP（山红）")
        self.assertEqual(payload["type"], "假发")
        self.assertEqual(payload["skin"], "Milktea Gray")
        self.assertEqual(payload["sizes"], [{"goods_size": "M size(8.5 inch)", "size_detail": ""}])
        self.assertEqual(payload["waiting_sale"], 1)
        self.assertEqual(payload["selected_image_list"], detail.images)
        self.assertNotIn("sale_record", payload)
        category_one_payload = build_from_switch_goods_payload(
            detail,
            {"category": 1, "details": details},
        )
        self.assertEqual(category_one_payload["name"], "SANHONG MAKE UP（山红）")
        self.assertEqual(category_one_payload["type"], "假发")

    def test_specific_size_details_are_written_under_parent_category(self):
        """明确的小分类必须在商品请求中携带正确的父分类，不能把小分类当成 goods_size。"""
        detail = parse_product_detail(
            DETAIL_T_HTML,
            "https://from-switch.com/product/detail.html?product_no=414&cate_no=25",
        )
        detail.title = "YIDO"
        payload = build_from_switch_goods_payload(
            detail,
            {
                "category": 3,
                "details": {
                    "official_name": "YIDO",
                    "chinese_name": "理度",
                    "product_type": "单头",
                    "colors": ["Powder Beige"],
                    "supported_sizes": ["65成男", "70+普成男"],
                    "currency": "USD",
                },
            },
        )
        self.assertEqual(payload["size"], "其它大尺寸")
        self.assertEqual(payload["size_detail"], "65成男")
        self.assertEqual(
            payload["sizes"],
            [
                {"goods_size": "其它大尺寸", "size_detail": "65成男"},
                {"goods_size": "其它大尺寸", "size_detail": "70+普成男"},
            ],
        )

    def test_non_size_option_group_does_not_become_size(self):
        """Body painting 等选项组的 No option、Hands 不应伪装成尺寸分类。"""
        detail = parse_product_detail(
            DETAIL_T_HTML,
            "https://from-switch.com/product/detail.html?product_no=414&cate_no=25",
        )
        detail.option_groups[0].name = "Body painting"
        raw_sizes = normalize_goods_size_items(["No option", "M size(8.5 inch)"])
        self.assertEqual(filter_from_switch_size_items(detail, raw_sizes), [])

        detail.option_groups = []
        detail.title = "HD64 Labyrinth body"
        payload = build_from_switch_goods_payload(
            detail,
            {
                "category": 1,
                "details": {
                    "product_type": "单体",
                    "colors": ["Powder Beige"],
                    "supported_sizes": ["No option"],
                    "currency": "USD",
                },
            },
        )
        self.assertEqual(payload["size"], "")
        self.assertEqual(payload["size_detail"], "")
        self.assertEqual(payload["sizes"], [])

        detail.currency = ""
        with self.assertRaises(GoodsPayloadError):
            build_from_switch_goods_payload(
                detail,
                {"category": 3, "details": {"currency": "CNY", "product_type": "假发"}},
            )

    def test_classification_category_one_still_calls_goods_creation(self):
        """From Switch 即使 AI 返回非 3 分类，也必须继续调用商品接口。"""
        detail = parse_product_detail(
            DETAIL_T_HTML,
            "https://from-switch.com/product/detail.html?product_no=414&cate_no=25",
        )
        detail.title = "SANHONG MAKE UP"

        class FakeGoodsAPI:
            """记录分类和商品创建调用。"""

            def __init__(self):
                """初始化调用记录。"""
                self.created_payloads = []

            def classify_goods(self, spider_log_id):
                """返回一个非 3 分类但字段完整的商品结果。"""
                del spider_log_id
                return {
                    "category": 1,
                    "details": {
                        "official_name": "SANHONG",
                        "chinese_name": "山红",
                        "product_type": "假发",
                        "colors": [],
                        "supported_sizes": [],
                        "currency": "USD",
                    },
                }

            def create_goods(self, payload):
                """记录商品请求。"""
                self.created_payloads.append(payload)

        api = FakeGoodsAPI()
        spider = BrandSpider145(
            SpiderConfig(auto_create_goods=True),
            None,
            None,
            api,
            logging.getLogger("brand_spider_145_test_goods"),
        )
        stats = CrawlStats()
        spider._classify_and_create_goods(detail, 414, stats)

        self.assertEqual(stats.classifications_succeeded, 1)
        self.assertEqual(stats.goods_created, 1)
        self.assertEqual(len(api.created_payloads), 1)
        self.assertEqual(api.created_payloads[0]["name"], "SANHONG MAKE UP（山红）")

    def test_existing_pending_record_is_skipped_without_retry_flag(self):
        """已有未处理记录默认不重跑，避免每次完整扫描都重复调用 AI。"""
        class FakeResponse:
            """提供详情页响应。"""

            content = DETAIL_T_HTML

        class FakeHTTP:
            """返回固定详情页。"""

            def get(self, url, **kwargs):
                """返回详情页内容。"""
                del url, kwargs
                return FakeResponse()

        class FakeDB:
            """返回一条已有未处理记录。"""

            def __init__(self):
                """初始化图片替换调用记录。"""
                self.replaced = False

            def find_by_source_product_no(self, product_no, brand_id, origin_type):
                """模拟分类页预查命中一条未处理记录。"""
                del product_no, brand_id, origin_type
                return SpiderLogRecord(999, 1, 0, 1, "")

            def insert_product(self, detail, brand_id, brand_name):
                """模拟 URL 去重命中。"""
                del detail, brand_id, brand_name
                return SpiderLogWriteResult(999, False, 1, 1)

            def replace_unprocessed_images(self, spider_log_id, images):
                """记录是否进入了重试分支。"""
                del spider_log_id, images
                self.replaced = True

        class FakeAPI:
            """记录 AI 是否被调用。"""

            def __init__(self):
                """初始化调用次数。"""
                self.calls = 0

            def classify_goods(self, spider_log_id):
                """记录一次 AI 调用。"""
                del spider_log_id
                self.calls += 1
                return {}

        database = FakeDB()
        api = FakeAPI()
        spider = BrandSpider145(
            SpiderConfig(auto_create_goods=True, detail_delay=0),
            FakeHTTP(),
            database,
            api,
            logging.getLogger("brand_spider_145_test_pending"),
        )
        spider._process_card(
            ProductCard("414", "Cosmos", "https://from-switch.com/product/detail.html?product_no=414", "", 1),
            CrawlStats(),
            "Switch",
        )

        self.assertFalse(database.replaced)
        self.assertEqual(api.calls, 0)

    def test_existing_only_run_does_not_request_source_pages(self):
        """默认已有数据模式只读数据库并分类，不访问来源分类页或详情页。"""
        class FakeHTTP:
            """检测是否错误访问了来源站点。"""

            def get(self, url, **kwargs):
                """已有数据模式不应调用来源 HTTP。"""
                del url, kwargs
                raise AssertionError("已有数据模式不应请求来源站点")

        class FakeDB:
            """提供一条已有未处理采集记录。"""

            def fetch_brand_name(self, brand_id):
                """返回固定品牌名称。"""
                self.assert_brand_id = brand_id
                return "Switch"

            def list_unprocessed_records(self, brand_id, origin_type, limit):
                """返回数据库中的未处理记录。"""
                del brand_id, origin_type, limit
                return [
                    SpiderLogRecord(
                        888,
                        1,
                        0,
                        1,
                        "https://from-switch.com/web/product/big/main.jpg",
                        title="SANHONG",
                        content="商品编号：414\n商品价格：USD 23.00",
                        url="https://from-switch.com/product/cosmos/414/",
                    )
                ]

        class FakeAPI:
            """返回完整分类结果并记录商品创建。"""

            def __init__(self):
                """初始化调用记录。"""
                self.classified = []
                self.created = []

            def classify_goods(self, spider_log_id):
                """记录已有采集记录的 AI 分类调用。"""
                self.classified.append(spider_log_id)
                return {
                    "category": 3,
                    "details": {
                        "official_name": "SANHONG",
                        "chinese_name": "山红",
                        "product_type": "假发",
                        "colors": ["Milktea Gray"],
                        "supported_sizes": [],
                        "currency": "USD",
                    },
                }

            def create_goods(self, payload):
                """记录商品创建载荷。"""
                self.created.append(payload)

        api = FakeAPI()
        spider = BrandSpider145(
            SpiderConfig(auto_create_goods=True),
            FakeHTTP(),
            FakeDB(),
            api,
            logging.getLogger("brand_spider_145_test_existing_only"),
        )
        stats = spider.run()

        self.assertEqual(stats.existing_records_loaded, 1)
        self.assertEqual(stats.classifications_succeeded, 1)
        self.assertEqual(stats.goods_created, 1)
        self.assertEqual(api.classified, [888])
        self.assertEqual(api.created[0]["name"], "SANHONG（山红）")

    def test_parse_sse_frame_reads_data_payload(self):
        """后台 SSE 的 data 行应解析为 JSON 对象。"""
        self.assertEqual(
            parse_sse_frame(["event: progress", 'data: {"stage":"final_result","result":{}}']),
            {"stage": "final_result", "result": {}},
        )

    def test_ai_request_uses_luna_prompt_and_current_images_only(self):
        """自动分类请求应固定使用 Luna、临时提示词和当前图片隔离开关。"""
        class FakeResponse:
            """提供最小 SSE 响应协议。"""

            status_code = 200
            text = ""

            def iter_lines(self, decode_unicode=False):
                """返回一个完成事件。"""
                del decode_unicode
                return iter([
                    'event: progress',
                    'data: {"stage":"requesting_ai","message":"正在请求问题","image_count":2}',
                    "",
                    'data: {"stage":"final_result","result":{"category":3,"details":{}}}',
                    "",
                ])

            def close(self):
                """兼容 requests.Response.close。"""

        class FakeSession:
            """记录管理后台请求参数。"""

            def __init__(self):
                """初始化最近一次请求。"""
                self.last_request = None

            def post(self, url, **kwargs):
                """保存请求并返回固定 SSE 响应。"""
                self.last_request = {"url": url, **kwargs}
                return FakeResponse()

        session = FakeSession()
        client = DogdogdollAPIClient(
            session,
            logging.getLogger("brand_spider_145_test"),
            "http://localhost:8080",
            "test-token",
            30,
        )
        with self.assertLogs("brand_spider_145_test", level="INFO") as captured:
            result = client.classify_goods(123)
        self.assertEqual(result["category"], 3)
        self.assertEqual(session.last_request["json"]["provider"], "chatgpt")
        self.assertEqual(session.last_request["json"]["model"], "gpt-5.6-luna")
        self.assertTrue(session.last_request["json"]["use_current_images_only"])
        self.assertIn("SANHONG => 山红", session.last_request["json"]["prompt_suffix"])
        self.assertTrue(any("AI 流开始" in message for message in captured.output))
        self.assertTrue(any("AI 流信息" in message and "stage=requesting_ai" in message for message in captured.output))
        self.assertTrue(any("stage=final_result" in message and "分类=3" in message for message in captured.output))


if __name__ == "__main__":
    unittest.main()
