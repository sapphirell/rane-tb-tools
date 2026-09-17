import unittest
from unittest.mock import Mock

from gui_xhs import (
    ARTIST_WORK_COUNT_THRESHOLD,
    ArtistCrawlMode,
    DatabaseManager,
)


class ArtistCrawlStrategyTest(unittest.TestCase):
    """验证艺术家补采策略只筛选对应身份作品不足阈值的记录。"""

    def setUp(self):
        """构造不连接数据库的查询对象。"""
        self.db = DatabaseManager.__new__(DatabaseManager)
        self.cursor = Mock()
        self.cursor.fetchall.return_value = []
        self.db._exec = Mock(return_value=(self.cursor, 0))

    def test_low_work_count_strategy_uses_identity_specific_filters(self):
        """妆师按妆图统计，毛娘按毛图统计，同一人任一身份不足即可进入列表。"""
        self.db.fetch_artists(ArtistCrawlMode.LOW_WORK_COUNT)

        sql, params = self.db._exec.call_args.args
        normalized_sql = " ".join(sql.lower().split())
        self.assertEqual(params, (ARTIST_WORK_COUNT_THRESHOLD, ARTIST_WORK_COUNT_THRESHOLD))
        self.assertIn("is_bjd_artist = 1", normalized_sql)
        self.assertIn("from bjd_faceup faceup", normalized_sql)
        self.assertIn("is_bjd_hairstylist = 1", normalized_sql)
        self.assertIn("from custom_wig wig", normalized_sql)
        self.assertIn("wig.is_delete = 0", normalized_sql)
        self.assertEqual(normalized_sql.count("< %s"), 2)

    def test_other_artist_strategies_do_not_add_work_count_filter(self):
        """原有两种艺术家排序策略不改变筛选范围。"""
        self.db.fetch_artists(ArtistCrawlMode.DEFAULT)
        sql, params = self.db._exec.call_args.args
        normalized_sql = " ".join(sql.lower().split())
        self.assertEqual(params, ())
        self.assertNotIn("from bjd_faceup", normalized_sql)
        self.assertNotIn("from custom_wig", normalized_sql)


if __name__ == "__main__":
    unittest.main()
