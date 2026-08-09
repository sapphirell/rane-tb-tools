ALTER TABLE artist_spider_log
  ADD COLUMN is_pinned_rule TINYINT NOT NULL DEFAULT 0 COMMENT '是否妆则/置顶帖: 0否 1是'
  AFTER likes;
