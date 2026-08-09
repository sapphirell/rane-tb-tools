import unittest

from xhs_comments import (
    EXPAND_REPLIES_SCRIPT,
    SCROLL_COMMENTS_SCRIPT,
    extract_note_id,
    flatten_comment_floors,
    group_comment_floors,
    merge_comment_snapshots,
    normalize_note_url,
    parse_count,
    split_time_location,
)


class XHSCommentHelpersTest(unittest.TestCase):
    def test_parse_count(self):
        self.assertEqual(parse_count("10+"), 10)
        self.assertEqual(parse_count("1.2万"), 12000)
        self.assertEqual(parse_count("3.4k"), 3400)
        self.assertEqual(parse_count("赞"), 0)

    def test_note_url(self):
        url = normalize_note_url(
            "https://www.xiaohongshu.com/explore/69986954000000000a02e645?xsec_token=a&amp;xsec_source=pc"
        )
        self.assertEqual(extract_note_id(url), "69986954000000000a02e645")
        with self.assertRaises(ValueError):
            normalize_note_url("https://www.xiaohongshu.com/explore/")
        with self.assertRaises(ValueError):
            normalize_note_url("https://example.com/explore/note-1")

    def test_time_location(self):
        self.assertEqual(split_time_location("5小时前 江苏"), ("5小时前", "江苏"))
        self.assertEqual(split_time_location("07-14"), ("07-14", ""))

    def test_flatten_and_group(self):
        floors = [{
            "comment": {
                "comment_id": "c1",
                "user_id": "u1",
                "user_name": "甲",
                "content": "问题",
                "raw_time": "07-14 四川",
                "like_count": "2",
                "reply_count": "展开 1 条回复",
            },
            "replies": [{
                "comment_id": "c2",
                "user_id": "u2",
                "user_name": "乙",
                "content": "回答",
                "raw_time": "07-15 湖南",
                "like_count": "1",
                "reply_to_user_name": "甲",
            }],
        }]
        comments = flatten_comment_floors(floors, "note-1")
        self.assertEqual(len(comments), 2)
        self.assertEqual(comments[0]["location"], "四川")
        self.assertEqual(comments[0]["reply_count"], 1)
        self.assertEqual(comments[1]["parent_comment_id"], "c1")
        self.assertEqual(comments[1]["reply_to_user_name"], "甲")
        grouped = group_comment_floors(comments)
        self.assertEqual(len(grouped), 1)
        self.assertEqual(len(grouped[0]["replies"]), 1)

    def test_merge_snapshots_keeps_reply_count(self):
        before = [{
            "comment_id": "c1",
            "user_name": "甲",
            "content": "问题",
            "reply_count": 1,
            "like_count": 2,
        }]
        after = [{
            "comment_id": "c1",
            "user_name": "甲",
            "content": "问题",
            "reply_count": 0,
            "like_count": 3,
        }]
        merged = merge_comment_snapshots((before, after))
        self.assertEqual(merged[0]["reply_count"], 1)
        self.assertEqual(merged[0]["like_count"], 3)

    def test_reply_keeps_parent_key_without_platform_comment_id(self):
        floors = [{
            "comment": {
                "user_name": "甲",
                "content": "问题",
                "raw_time": "07-14",
            },
            "replies": [{
                "user_name": "乙",
                "content": "回答",
                "raw_time": "07-15",
            }],
        }]
        comments = flatten_comment_floors(floors, "note-1")
        self.assertEqual(comments[1]["parent_comment_id"], "")
        self.assertEqual(comments[1]["parent_comment_key"], comments[0]["comment_key"])
        grouped = group_comment_floors(comments)
        self.assertEqual(len(grouped), 1)
        self.assertEqual(len(grouped[0]["replies"]), 1)

    def test_comment_scroll_never_moves_note_page(self):
        self.assertNotIn("window.scrollTo", SCROLL_COMMENTS_SCRIPT)
        self.assertNotIn("document.scrollingElement", SCROLL_COMMENTS_SCRIPT)
        self.assertNotIn("scrollIntoView", EXPAND_REPLIES_SCRIPT)


if __name__ == "__main__":
    unittest.main()
