import unittest

from gui_weibo import (
    build_weibo_entry_url,
    build_weibo_user_uri_candidates,
    is_supported_weibo_url,
    should_try_entry_resolution,
)


class GuiWeiboUrlHelperTest(unittest.TestCase):
    """覆盖微博主页链接解析的关键规则。"""

    def test_build_candidates_prefers_screen_name_for_n_links(self) -> None:
        candidates = build_weibo_user_uri_candidates("https://weibo.com/n/%E5%8F%AF%E5%B0%A4%E7%89%B9CUTE")
        self.assertEqual(candidates[:2], ["可尤特CUTE", "n/可尤特CUTE"])

    def test_build_candidates_prefers_u_prefix_for_numeric_profile(self) -> None:
        candidates = build_weibo_user_uri_candidates("https://weibo.com/6298770499")
        self.assertEqual(candidates[:2], ["u/6298770499", "6298770499"])

    def test_non_weibo_url_returns_no_candidates(self) -> None:
        self.assertFalse(is_supported_weibo_url("https://bing.com/ck/a"))
        self.assertEqual(build_weibo_user_uri_candidates("https://bing.com/ck/a"), [])

    def test_build_entry_url_preserves_encoded_user_uri(self) -> None:
        self.assertEqual(
            build_weibo_entry_url("n/可尤特CUTE"),
            "https://weibo.cn/n/%E5%8F%AF%E5%B0%A4%E7%89%B9CUTE",
        )

    def test_only_non_u_uri_needs_entry_resolution(self) -> None:
        self.assertTrue(should_try_entry_resolution("可尤特CUTE"))
        self.assertTrue(should_try_entry_resolution("n/可尤特CUTE"))
        self.assertFalse(should_try_entry_resolution("u/7582779964"))


if __name__ == "__main__":
    unittest.main()
