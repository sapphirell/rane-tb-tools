# `brand_spider_145.py` 一次性商品采集器与自动商品导入开发计划

## 1. 任务范围

新增 `spiders/brand_spider_145.py`，一次性采集 From Switch 英文站 DOLL 分类（`cate_no=25`）的全部商品，并将每个商品写入现有 `spider_log` 采集记录表。

本次范围包括：

- 读取分类页，自动识别总页数并依次翻页。
- 读取每个商品详情页，提取商品名称、图片、正文、价格和 SKU/规格选项。
- 使用 `brand_id=145`、`brand_name=Switch` 写入 `spider_log`。
- 通过 `url_md5` 去重，脚本重复运行不会重复插入相同商品。
- 记录单页、单商品的成功、跳过和失败原因，任务结束输出汇总。
- 可选调用管理后台现有 AI 分类 SSE 流程；From Switch 分类页的每条详情都按商品创建，AI 返回的 `category` 不作为跳过条件。
- 过滤每条详情中固定出现的 From Switch 编辑器废图。
- 使用脚本内临时提示词和已确认的英文官名→中文名白名单，确认匹配后将中文名追加到商品名末尾。

本次不包括：

- 不接入现有 GUI，不增加定时任务。
- 不直接修改服务器上的代码；本地 Go 后端仅补充现有 SSE 接口的临时提示词透传和 `chinese_name` 兼容解析。
- 不重启本地 UniApp、Go 服务或其它容器。
- 脚本本身不直接下载并上传图片到七牛；自动商品模式仍交给现有后台图片上传流程处理。
- 不创建贩售记录，不导入新闻，不调用与“分类为商品”无关的接口。

## 2. 当前勘察结论（2026-09-15）

### 2.1 分类页

目标页：

```text
https://from-switch.com/product/list.html?cate_no=25
```

已通过 `curl` 获取到服务端 HTML，当前结构如下：

| 内容 | 当前结构或结论 |
| --- | --- |
| 商品列表容器 | `.xans-product-listnormal .prdList` |
| 商品卡片 | `.xans-product-listnormal .prdList > li[id^="anchorBoxId_"]` |
| 商品编号 | 卡片 ID 中的数字，例如 `anchorBoxId_670`；链接中也有 `product_no=670` |
| 商品名称 | 卡片内 `.name a` 的文本 |
| 列表封面 | 卡片内 `img.thumb` |
| 详情链接 | 通常是 `/product/detail.html?product_no=...&cate_no=25&display_group=1` |
| 每页数量 | 当前为 16 个 |
| 总页数 | 当前分页最后页为第 17 页；第 17 页有 9 个商品，预计共 265 个商品 |
| 分页容器 | `.xans-product-normalpaging`；“最后一页”链接包含 `?page=17` |

注意：页面生成的下一页链接是相对地址 `?page=2`，直接访问时可能丢失 `cate_no=25` 并得到空列表。因此脚本必须自行构造：

```text
https://from-switch.com/product/list.html?cate_no=25&page={page}
```

不能只拼接页面中读到的 `?page=N`。

### 2.2 商品详情页

示例页：

```text
https://from-switch.com/product/lis/670/?cate_no=25&display_group=1
```

已确认详情页是 Cafe24 模板，商品数据直接出现在 HTML 中，不依赖登录或必须执行的前端请求。当前可使用的结构如下：

| 内容 | 选择器或来源 |
| --- | --- |
| 详情根节点 | `.xans-product.xans-product-detail .detailArea` |
| 规范商品地址 | `<link rel="canonical">`，例如 `https://from-switch.com/product/lis/670/` |
| 商品名称 | `.infoArea > span`；同时可从 Basic Information 表或 JSON-LD 校验 |
| 主图 | `.imgArea img.BigImage` |
| 附加图 | `.imgArea .listImg img.ThumbImage` |
| 商品正文 | `#prdDetail .cont` |
| 正文图片 | `#prdDetail img`；当前页面大量使用 `ec-data-src` 延迟图片属性 |
| 基础价格 | `#span_product_price_text`，示例为 `USD 150.00` |
| 价格脚本兜底 | `var product_price` 与 `CAFE24.SHOP_CURRENCY_INFO`，仅在价格节点缺失时使用 |
| 选项组 | `caption` 为 `Product Option` 的表格下，`tr.xans-product-option` 中的 `select` |
| 选项组名称 | `select[option_title]`；也可从同一行 `th` 读取 |
| 选项值 | `option` 的 `value`、文本、`disabled` 状态和文本中的加价 |
| 组合 SKU | 页面脚本中的 `option_stock_data`；组合型商品含 `option_value`、`option_value_original`、`option_price` 等字段 |

详情页存在两种已观察到的选项类型：

- `option_type=E`：每个选项值可能对应独立的选项编号，例如商品 670 的 Skin。
- `option_type=T`：多个选项组组合成 SKU，例如商品 414 的 Size + Color；组合信息出现在 `option_stock_data` 中。

因此不能只提取第一个 `select`，也不能只保存选项显示文本而丢掉选项值、禁用状态和加价。

### 2.3 数据库现状

只读核对结果：

- `brand.id=145` 存在，`brand_name=Switch`，且品牌有效。
- 目标表为 `spider_log`。
- 现有字段已经包含 `title`、`content`、`url`、`images`、`brand_id`、`brand_name`、`created_at`、`updated_at`、`full_get`、`auth_time`、`url_md5` 等。
- `content` 和 `images` 为 `TEXT`，`url_md5` 为 `CHAR(32)`。
- 现有表没有价格或 SKU 专用字段。

## 3. 技术方案

### 3.1 采集方式

优先使用 `requests.Session + lxml.html + pymysql` 直接读取服务端 HTML，不把 Chrome/Selenium 作为主流程。原因是当前分类页和详情页的目标字段都已在 HTML 中，详情图的 `ec-data-src` 也可以直接读取，直接请求更适合一次性、可重复执行的脚本。

脚本保留浏览器兼容入口的设计：如果目标页面返回非商品页、出现验证码或详情节点持续缺失，只记录明确错误并停止/跳过，不通过不透明的逻辑兜底伪造商品数据。是否临时使用可见 Chrome 进行排查，由运行时人工决定，不纳入脚本主流程。

现有 `spiders/requirements.txt` 已包含 `requests`、`pymysql`、`lxml`，预计不需要新增依赖。

### 3.2 页面遍历流程

1. 初始化请求会话，设置稳定的 User-Agent、Accept-Language、Referer 和超时。
2. 读取分类第 1 页，解析商品卡片和最后页编号。
3. 从第 1 页开始，按 `cate_no=25&page=N` 依次读取每一页。
4. 在每页内按 HTML 顺序提取商品链接；用内存集合按商品编号去重。
5. 逐个打开商品详情页，页面之间加入可配置的小幅随机等待，避免短时间连续请求。
6. 详情解析成功后立即写入一条 `spider_log`，单条提交，失败商品不影响后续商品。
7. 到达解析出的最后页后结束；若页面为空、分页签名重复或页码超过最后页，也要安全停止，避免死循环。
8. 输出总页数、发现商品数、详情成功数、写入数、重复跳过数、失败数和图片/选项统计。

### 3.3 自动分类与商品创建

启用 `--auto-create-goods` 后，脚本按以下顺序复用分类页面中的现有链路：

1. 先写入或按 `url_md5` 找到 `spider_log`；已处理记录（`status=2`）不重复分类，未处理旧记录会重新写入当前已过滤的图片并重置 `full_get=0`。
2. 调用 `/admin/spider-kimi-analyze-sse`，固定传入 `provider=chatgpt`、`model=gpt-5.6-luna`、`mode=goods`，并把脚本内的临时提示词作为 `prompt_suffix` 传入；同时启用当前图片隔离，避免历史图片日志或关联贩售截图进入本次 AI 输入。
3. From Switch 分类页的每条详情都调用 `/admin/merchant-create-goods-by-spd`；提示词要求返回 `category=3`，但即使 AI 偶尔返回 1、2、4 或 9，脚本仍按商品字段继续创建，不把分类结果当作跳过条件。
4. 商品载荷沿用管理后台创建商品表单字段，使用 AI 提取的商品类型、肤色、尺寸等信息，缺失时只从当前来源详情的明确选项中读取，不填虚构值。
   其中只有详情页明确标注为 Size/尺寸的选项组才能成为商品尺寸；`Body painting`、`Chest parts`、`Skin Color` 等选项组不属于尺寸。
   From Switch 商品没有真实尺寸时提交空尺寸，不能把 `No option`、`Hands` 或 `Hands+Foot` 写入尺寸字段；后台仅对该品牌的该采集来源允许空尺寸商品创建。
5. 商品请求通过 `selected_image_list` 显式提交当前已过滤的来源图片；后台按 `images_log.origin_url` 映射为七牛地址，避免历史图片或固定废图重新混入。请求不带 `sale_record`，脚本也不调用新闻导入接口。
6. AI 必须同时返回同一白名单行中的 `official_name` 和 `chinese_name`；脚本还会在来源标题或正文中寻找最具体的英文官名。配对、来源证据任一不一致时都不追加中文名。

### 3.4 临时中文名提示词

提示词保存在 `brand_spider_145.py`，包含截图中能确认的 42 条英文官名→中文名关系。被遮挡或无法确认的行不写入白名单。提示词要求 AI 精确区分 `YIDO inferno`、`YUNSEOL somnia`、`YUNSEOL: Tokki`、`CHOYO robin` 等变体，同时返回成对的 `official_name` 和 `chinese_name`；脚本不会接受白名单之外、配对错误或来源文字中没有证据的中文名。

### 3.5 详情提取规则

#### 商品名称和地址

- 商品编号优先从卡片 ID 和 `product_no` 参数交叉校验。
- 详情链接使用列表页链接访问；解析成功后优先使用站点 `canonical` 作为保存地址。
- 规范化地址时统一协议、域名大小写、去掉 query/fragment 和末尾 `/`，再计算 `url_md5`。
- 详情缺少商品编号、名称或有效来源地址时视为该商品解析失败，不写入不完整记录。

#### 图片

按以下顺序收集并保留第一次出现的地址：

1. `.BigImage` 主图。
2. `.listImg .ThumbImage` 附加图。
3. `#prdDetail` 内所有图片。

图片地址通过 `urljoin` 转为绝对地址，兼容 `//from-switch.com/...`、相对地址和 `ec-data-src` 延迟属性；同一地址去重并移除 fragment。只排除页面按钮、缩放控制和推荐商品卡片图片，正文内的商品说明图默认保留。

每条采集记录都会出现的以下固定编辑器图片属于废图，比较地址时忽略协议、查询参数和 fragment 后精确排除：

```text
https://from-switch.com/web/upload/category/editor/2020/03/02/4ec60c80c2204fa0e8a3f2f2d357c396.gif
```

#### 正文

- 以 `#prdDetail .cont` 为正文范围。
- 移除 `script`、`style`、空的编辑器占位节点和页面操作文字。
- 将 `br`、段落、块级元素转换为换行，解码 HTML 实体，压缩重复空白，但保留正文原有英文内容和段落顺序。
- 不把原始 HTML 直接写入 `content`，避免现有采集详情页把标签当普通文本展示。

#### 价格

- 优先读取 `#span_product_price_text` 的原始展示值，保留币种和小数，例如 `USD 150.00`。
- 同时解析数值和币种，用于生成固定格式的采集正文。
- 如果页面存在选项加价，保留基础价格与每个选项/组合的加价，不擅自计算或换算成人民币。
- 价格节点和价格脚本都缺失时不猜测价格，记录警告并按“价格未提供”写入正文。

#### SKU/规格选项

- 遍历 `Product Option` 表中全部 `select`，读取排序号、选项组名、选项值、页面 value、是否必选、是否禁用和文本中的价格调整。
- 跳过 `*` 的请选择项和 `**` 的分隔线，但保留实际禁用选项并标记为不可售/不可选。
- 解析 `option_stock_data` 时：
  - `option_type=E` 保存选项编号与对应的选项值、库存/销售状态和加价。
  - `option_type=T` 保存组合 SKU 编号、组合值、各原始选项值、组合价格和库存/销售状态。
- 页面没有可解析的组合数据时，至少保存所有选项组和选项值；不虚构 SKU 编号。

## 4. `spider_log` 落库映射

本次选择复用现有字段，不新增数据库结构或迁移 SQL。价格/SKU 的专用列缺失时，使用固定的小节写入 `content`，既能在现有采集详情中阅读，也能在后续需要时按约定解析。自动商品模式直接复用现有商品创建接口，不在业务代码中执行建表或改表语句。

建议的 `content` 格式：

```text
商品编号：670
商品价格：USD 150.00

SKU/规格选项：
- Skin：Rosy White（value=4171；可选）
- Skin：Powder Begie（value=4172；可选）
- Skin：Milktea Rose（value=4173；加价 USD 40.00；可选）

SKU组合：
- sku_code=...；选项=...；价格=...；状态=...

商品正文：
...
```

字段映射如下：

| `spider_log` 字段 | 写入规则 |
| --- | --- |
| `msg_type` | `0`，表示新采集记录 |
| `status` | `1`，保持未处理状态；商品页没有社交发布时间，避免 `auth_time=0` 被现有旧日志维护误判为无用信息 |
| `origin_type` | `from_switch`，明确区分于 `xhs`、`weibo` |
| `title` | 详情页商品名称 |
| `content` | 按固定格式合并商品编号、价格、SKU/规格和清洗后的正文 |
| `url` | 详情页 canonical 绝对地址；没有 canonical 时使用规范化后的列表详情地址 |
| `images` | 主图、附加图和商品正文图片的去重后绝对地址，以逗号分隔 |
| `brand_id` | 固定为 `145`，运行开始时校验品牌行有效 |
| `brand_name` | 从数据库读取，当前应为 `Switch`，不在脚本中重复维护另一份名称 |
| `created_at` | 当前 Unix 秒时间 |
| `updated_at` | 与 `created_at` 相同 |
| `full_get` | `0`；来源图片尚未经过现有七牛上传流程 |
| `auth_time` | `0`；商品页没有社交内容发布时间 |
| `likes` | `0`；商品页没有点赞数据 |
| `ai_think_type` | `0`，等待现有采集流程处理 |
| `ai_summary` | 空字符串 |
| `text` | 空字符串；该字段保留给现有评论楼层 JSON，不承载价格或 SKU |
| `url_md5` | 对规范化详情地址计算 MD5 |

写入时显式列出字段和参数，不使用全字段更新，不使用会覆盖已有记录的 `Save` 或更新式 Upsert。

## 5. 去重、重试和失败处理

### 5.1 去重

- 本次运行内按 `product_no` 去重。
- 入库前按 `url_md5` 查询 `spider_log`，已有记录默认跳过，不覆盖已处理记录。
- 详情页 canonical 统一后再计算哈希，避免列表详情地址和 SEO 地址产生两条记录。
- 不使用模糊标题去重，避免同名商品被错误跳过。
- 自动商品模式下，未处理旧记录只更新过滤后的来源图片并重置 `full_get=0`，随后进入现有分类流程。

### 5.2 请求重试

- 对连接超时、读取超时、429 和 5xx 进行有限次数重试，使用递增等待。
- 403、验证码页、内容类型异常或连续返回非商品 HTML 时记录具体原因，不无限重试。
- 每个商品详情失败后继续下一个商品；品牌行校验失败、数据库不可连接等全局错误直接终止。

### 5.3 记录级校验

- 必填项：商品编号、标题、有效详情地址。
- 可选项：价格、正文、选项、图片。可选项缺失时写入警告，但不能用其它商品或列表页文本冒充详情数据。
- 插入失败时回滚当前记录，日志中保留商品编号和 URL，继续执行后续记录。
- 全部任务结束后，如果存在商品级失败，命令以非零状态结束，便于发现需要补采的商品。

## 6. 命令行与运行控制

脚本计划支持以下参数，默认值服务于本次一次性全量采集：

| 参数 | 用途 |
| --- | --- |
| `--start-page` | 起始页，默认 `1` |
| `--end-page` | 结束页；缺省时使用分类页解析出的最后页 |
| `--limit-products` | 仅处理前 N 个商品，用于试跑 |
| `--detail-delay` | 详情页请求间隔，提供默认值并允许调整 |
| `--dry-run` | 只解析和打印摘要，不写数据库 |
| `--auto-create-goods` | 调用现有 AI 分类；From Switch 每条详情都创建商品 |
| `--api-base-url` | 管理后台 API 地址，默认 `http://localhost:8080` |
| `--api-timeout` | AI 流式响应读取超时秒数 |
| `--log-file` | 可选日志文件路径，默认脚本同目录运行日志 |

管理员 JWT 只从环境变量 `DOGDOGDOLL_ADMIN_TOKEN` 读取；数据库密码只从 `SPIDER_DB_PASSWORD` 读取。自动商品模式不能与 `--dry-run` 同时使用。

为方便本机执行，`run_brand_spider_145.sh` 已封装数据库配置读取和 Chrome 本地登录态读取。默认执行完整自动商品模式，只需执行：

```bash
cd /Users/sapphirell/Documents/git/Dogdogdoll/spiders
./run_brand_spider_145.sh
```

启动器不会打印管理员令牌；如果不使用 Chrome 登录态，也可以提前设置 `DOGDOGDOLL_ADMIN_TOKEN`。只读试跑可使用 `BRAND_SPIDER_DRY_RUN=1 ./run_brand_spider_145.sh --limit-products 1`。

先试跑 1 条自动分类商品：

```bash
cd /Users/sapphirell/Documents/git/Dogdogdoll/spiders
export SPIDER_DB_PASSWORD='数据库密码'
export DOGDOGDOLL_ADMIN_TOKEN='管理后台登录令牌'
./venv/bin/python brand_spider_145.py \
  --auto-create-goods \
  --limit-products 1 \
  --detail-delay 0
```

如果管理后台只通过前端开发代理访问，可将 `--api-base-url` 改为 `http://localhost:9527`；默认地址按项目开发环境使用后端 `8080`。

## 7. 实施步骤

### 阶段一：解析器

1. 新增 `spiders/brand_spider_145.py` 的 URL、HTML 解析、文本清洗、图片规范化和 SKU 解析方法。
2. 用当前保存的 670 详情页结构验证：主图、正文延迟图、价格、单选项和 `option_stock_data`。
3. 增加至少两种详情夹具：单选项商品（670/760）和多组选项商品（414/443）；另加分页第一页与最后一页样例。

### 阶段二：数据库写入

1. 独立实现数据库连接和品牌校验，避免复用 GUI 初始化逻辑带来的副作用。
2. 实现显式字段插入、`url_md5` 去重、单条事务提交和结果统计。
3. 先执行 `--dry-run --start-page 1 --end-page 1 --limit-products 1`，确认正文、图片、价格和 SKU 输出。

### 阶段三：试跑与全量采集

1. 先选 1 个商品完成真实入库核对。
2. 查询后台采集记录详情，确认品牌、来源、标题、图片预览和正文格式。
3. 清理或确认试跑记录后，再执行第 1～17 页全量采集。
4. 根据脚本最终汇总和失败清单补采失败商品；重复运行只能新增缺失记录，不覆盖已存在数据。

### 阶段四：自动分类商品试跑

1. 先用 `--limit-products 1 --auto-create-goods` 验证 SSE 分类结果、中文名追加、商品图片和尺寸字段。
2. 确认后台商品详情无误后，再去掉数量限制执行目标分类的全量自动导入。
3. 对 AI 结果缺少必要商品字段或商品接口失败的记录保留失败清单；`category` 非 3 不跳过商品创建，不自动猜值、不自动补建贩售或新闻记录。

## 8. 验收标准

- 分类页能遍历第 1～17 页，当前页面结构下发现约 265 个商品，最后一页 9 个；页数不写死，以页面分页结果为准。
- 每个成功商品都有正确的 `brand_id=145`、商品标题、来源 URL 和 `url_md5`。
- 670 商品能保存 `USD 150.00`、Skin 三个选项、主图和 `#prdDetail` 中的正文图片。
- 多选项商品能保存全部选项组；存在 `option_stock_data` 时能保存组合 SKU 信息、加价和状态。
- 图片地址没有 `//`、相对路径或重复值；详情中的 `ec-data-src` 图片不能因 `src` 为空而丢失。
- 固定编辑器废图不会出现在 `spider_log.images`、AI 输入图片或新建商品的图片列表中。
- 普通采集模式不会重复插入相同规范 URL，也不会修改已有采集记录；自动模式只对未处理记录更新过滤后的图片并继续分类，已处理记录不重复创建。
- 自动模式固定使用 ChatGPT-Luna（`gpt-5.6-luna`）；From Switch 每条详情都创建商品，`category` 只作为 AI 结果记录，不会携带 `sale_record` 或调用新闻导入。
- 中文名必须与英文官名来自同一白名单行，且能在来源标题或正文中确认；无法确认或不相关时商品名不追加中文括号。
- 任何商品失败都有商品编号、URL 和错误原因；全量结束后能得到可复核的失败清单。
- 不新增数据库结构 SQL；不修改服务器代码；不重启本地服务。

## 9. 预计文件变更

本阶段已写入：

- `spiders/brand_spider_145_development_plan.md`
- `spiders/brand_spider_145.py`
- `spiders/test_brand_spider_145.py`
- `dogdogdoll-go/web/api/admin/spider_kimi_sse.go`
- `dogdogdoll-go/web/service/kimi_service/spider_sale_classifier.go`
- `dogdogdoll-go/web/service/kimi_service/spider_sale_classifier_test.go`
- `dogdogdoll-go/web/service/kimi_service/spider_sale_stream.go`
- `dogdogdoll-go/web/service/kimi_service/spider_sale_stream_test.go`

不修改 Vue、UniApp 或数据库结构；Go 变更只用于复用现有分类流程接收脚本临时规则和保留中文名结果。

## 10. 实施记录

- 已新增 `spiders/brand_spider_145.py`，实现分类翻页、详情解析、有限重试、失败清单、`spider_log` 显式插入和 `url_md5` 去重。
- 已新增 `spiders/test_brand_spider_145.py`，覆盖分页 URL、分类卡片、E/T 两类选项、延迟图片、正文格式、URL 去重和 UTF-8 中文标题。
- 数据库密码只从环境变量 `SPIDER_DB_PASSWORD` 读取；`--dry-run` 不需要数据库密码。
- 已确认现有 `spider_log` 字段足以承载本次数据，不新增或修改数据库结构，因此没有新增 SQL。
- 试跑发现现有维护任务会清理 `status=0、auth_time=0` 的历史记录；脚本已改用 `status=1`，`auth_time` 仍保持 `0`，避免伪造商品发布时间。
- 全量只读核验结果：17 页、265 个商品、详情成功 265、失败 0，提取图片 4,838 张、选项组 266、SKU 778。
- 全量真实入库已完成：新增 264 条，试跑记录按 `url_md5` 去重 1 条，失败 0；数据库现有该来源记录共 265 条且哈希唯一。
- 已抽查 670、443、245，确认标题、价格、SKU/选项、正文和图片已写入；重复试跑再次跳过 760，未新增重复记录。

### 自动分类商品阶段（2026-09-15）

- 脚本新增 `--auto-create-goods`，把固定临时提示词透传给现有 `/admin/spider-kimi-analyze-sse`；调用参数固定为 `chatgpt / gpt-5.6-luna`。
- 脚本对 From Switch 每条详情都调用 `/admin/merchant-create-goods-by-spd`；提示词要求 `category=3`，但脚本不会因 AI 返回 1、2、4 或 9 而跳过，不发送 `sale_record`，不调用新闻接口。
- 新增固定废图过滤、42 条已确认中文名白名单、官名配对与来源证据校验；不相关、无法确认或配对错误时不追加中文名。
- Go 端新增临时提示词后缀透传和 `official_name`、`chinese_name` 结果解析，兼容现有管理后台请求；没有数据库结构变更，因此本阶段无新增 SQL。
