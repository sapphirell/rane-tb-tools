# -*- coding: utf-8 -*-
"""历史商品尺寸修复脚本的离线测试。"""

import unittest

from repair_brand_spider_145_sizes import (
    SizeRow,
    SourceMatch,
    build_repair_plan,
    parse_goods_ids,
    source_text_supports_detail,
)


def make_match(rows, source_content="Compatible body type HD 65boy body / HD 70boy body"):
    """构造不连接数据库的来源匹配样本。"""
    return SourceMatch(
        goods_id=17454,
        goods_name="YIDO（理度）",
        primary_category="65成男",
        primary_detail="",
        source_id=234462,
        source_title="YIDO",
        source_url="https://from-switch.com/product/yido/268/",
        source_content=source_content,
        shared_images=14,
        size_rows=tuple(rows),
    )


class RepairPlanTest(unittest.TestCase):
    """验证尺寸父子关系修复的安全边界。"""

    def test_detail_values_move_under_large_size_parent(self):
        """65/70 细分类不能继续作为 goods_size 父分类。"""
        plan = build_repair_plan(
            make_match(
                [
                    SizeRow(45412, 17454, "65成男", "", 1),
                    SizeRow(45413, 17454, "70+普成男", "", 2),
                ]
            ),
            min_shared_images=2,
        )
        self.assertTrue(plan.changed)
        self.assertEqual(
            list(plan.new_sizes),
            [
                {"goods_size": "其它大尺寸", "size_detail": "65成男"},
                {"goods_size": "其它大尺寸", "size_detail": "70+普成男"},
            ],
        )

    def test_redundant_parent_row_is_removed(self):
        """同一父分类已有明确细分类时，不保留空详情父分类行。"""
        plan = build_repair_plan(
            make_match(
                [
                    SizeRow(1, 17454, "65成男", "", 1),
                    SizeRow(2, 17454, "其它大尺寸", "", 2),
                ]
            ),
            min_shared_images=2,
        )
        self.assertEqual(list(plan.new_sizes), [{"goods_size": "其它大尺寸", "size_detail": "65成男"}])

    def test_non_size_option_is_removed(self):
        """历史上混入尺寸表的 Body painting 选项不能继续提交。"""
        plan = build_repair_plan(
            make_match(
                [
                    SizeRow(1, 17454, "65成男", "", 1),
                    SizeRow(2, 17454, "Hands", "", 2),
                ]
            ),
            min_shared_images=2,
        )
        self.assertEqual(list(plan.new_sizes), [{"goods_size": "其它大尺寸", "size_detail": "65成男"}])

    def test_detail_without_source_evidence_is_skipped(self):
        """来源未出现对应身体型号时不能仅凭脏数据强行修复。"""
        plan = build_repair_plan(
            make_match(
                [SizeRow(1, 17454, "70+普成男", "", 1)],
                source_content="Head Size 21cm Compatible body type HD 65boy body",
            ),
            min_shared_images=2,
        )
        self.assertFalse(plan.changed)
        self.assertIn("未找到细分类", plan.skip_reason)

    def test_source_evidence_and_id_parser(self):
        """来源身体型号和商品 ID 参数应按明确文本解析。"""
        self.assertTrue(source_text_supports_detail("65成男", "HD 65boy Attractive body"))
        self.assertFalse(source_text_supports_detail("70+普成男", "HD 65boy body"))
        self.assertEqual(parse_goods_ids(["17454, 17455", "17454"]), [17454, 17455])


if __name__ == "__main__":
    unittest.main()
