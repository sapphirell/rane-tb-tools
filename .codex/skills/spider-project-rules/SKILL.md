---
name: spider-project-rules
description: 适用于 spider 采集、爬虫脚本、数据处理、任务运行以及 Git 提交与推送协作。涉及 spider 项目开发或需要创建 commit、push 代码时使用，确保提交说明使用中文并保留可追踪的变更边界。
---

# Spider 项目规则

在 `spider` 中进行采集脚本、数据处理、任务配置或发布协作时使用此 skill。

## Git 提交规范

- 创建 Git commit 时，commit message 必须使用中文，简洁说明本次变更。
- 可保留 `feat:`、`fix:` 等提交类型前缀，但冒号后的具体描述必须使用中文，例如：`fix: 修复小红书采集账号轮换`。
- push 前先检查最近一次 commit message，确认提交说明符合中文规范；已经推送的 commit 不主动重写历史，除非用户明确要求。
