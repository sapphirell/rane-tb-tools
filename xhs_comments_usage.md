# 小红书评论采集

`xhs_comments.py` 使用可见的 Selenium 浏览器读取小红书笔记页面已经渲染的公开评论。它会滚动评论区加载后续楼层，并点击“展开 N 条回复”读取楼中楼；不读取 Cookie，不自动填写登录信息，也不绕过验证码。

在 `gui_xhs.py` 中，点击“采集评论”，填写笔记地址和采集范围，再从 GUI 账号池选择一个账号。GUI 会用该账号的已保存 Cookie 和独立浏览器 profile 登录，遇到登录/风控时仍由 GUI 的二维码、暂停和恢复流程处理。输出文件写入 `output/xhs_comments_<note_id>.json`。

在 `spiders` 目录执行：

```bash
python3 xhs_comments.py \
  --url "https://www.xiaohongshu.com/explore/69986954000000000a02e645?xsec_token=...&xsec_source=pc_search" \
  --output ./output/xhs_comments_69986954000000000a02e645.json
```

浏览器出现登录或风控提示时，在打开的浏览器窗口中手动处理。采集结果是 UTF-8 JSON：

- `comments`：扁平评论列表，包含 `comment_id`、`parent_comment_id`、`user_id`、`user_name`、`content`、`create_time`、`location`、`like_count`、`reply_count`、`reply_to_user_id`、`reply_to_user_name`、`is_reply`。
- `floors`：按顶层评论和 `replies` 组织的相同数据，便于还原楼中楼。
- `total_comments`：页面显示的评论总数；`collected_comments`：本次实际采集条数。

默认不限制评论条数，但最多滚动 240 轮，每轮间隔 1 秒。可以用 `--max-comments`、`--max-rounds` 和 `--wait` 调整。页面未暴露评论 ID 时，`comment_id` 会为空，同时保留 `comment_key` 用于本次采集去重；不会伪造平台评论 ID。

仅应在遵守小红书服务条款、隐私要求和合理访问频率的前提下使用。
