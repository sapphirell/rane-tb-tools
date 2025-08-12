import os
import pickle
import sys
import time
import urllib
import urllib.parse
import pymysql
import logging
from typing import Dict, Optional, Callable
from selenium.webdriver import Chrome
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def convert_xhs_url(original_url):
    parsed_url = urllib.parse.urlparse(original_url)

    # 替换查询参数中的&amp;为&
    new_query = parsed_url.query.replace('&amp;', '&')

    # 分割路径并处理
    path_parts = parsed_url.path.split('/')
    if len(path_parts) >= 5 and path_parts[1] == 'user' and path_parts[2] == 'profile':
        note_id = path_parts[4]
        new_path = f'/explore/{note_id}'
    else:
        new_path = parsed_url.path  # 保留原始路径

    # 构建新URL
    new_parsed = parsed_url._replace(
        path=new_path,
        query=new_query
    )
    return urllib.parse.urlunparse(new_parsed)


def parse_xhs_time(time_str: str) -> int:
    """解析小红书时间格式（新增天前处理）"""
    from datetime import datetime, timedelta
    import re

    clean_str = time_str.replace('编辑于 ', '').strip()
    parts = re.split(r'\s+(?=[\u4e00-\u9fa5]{2,5}$)', clean_str)
    time_part = parts[0]

    now = datetime.now()
    current_year = now.year

    try:
        # 新增天前处理（保持原有结构）
        if '天前' in time_part:
            days = int(re.search(r'\d+', time_part).group())
            return int((now - timedelta(days=days)).timestamp())
        # 原有时间处理逻辑保持不变...
        if '昨天' in time_part:
            date = now - timedelta(days=1)
            time_str = re.sub(r'昨天', date.strftime('%Y-%m-%d'), time_part)
        elif '今天' in time_part:
            time_str = re.sub(r'今天', now.strftime('%Y-%m-%d'), time_part)
        elif '分钟前' in time_part:
            minutes = int(re.search(r'\d+', time_part).group())
            return int((now - timedelta(minutes=minutes)).timestamp())
        elif '小时前' in time_part:
            hours = int(re.search(r'\d+', time_part).group())
            return int((now - timedelta(hours=hours)).timestamp())
        else:
            time_str = time_part

        # 处理带时间的格式（增强匹配模式）
        time_formats = [
            (r'(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{2})', "%m-%d %H:%M"),  # 04-20 15:30
            (r'(\d{4})-(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{2})', "%Y-%m-%d %H:%M"),  # 2023-04-20 15:30
            (r'(\d{1,2})-(\d{1,2})', "%m-%d"),  # 04-20
        ]

        for pattern, time_format in time_formats:
            if re.match(pattern, time_str):
                dt = datetime.strptime(time_str, time_format)
                # 自动补全年份（若需要）
                if dt.year == 1900:  # strptime默认年份
                    dt = dt.replace(year=current_year)
                    # 处理跨年（如当前1月但解析到12月的情况）
                    if dt > now + timedelta(days=60):
                        dt = dt.replace(year=current_year - 1)
                return int(dt.timestamp())

        return 0
    except Exception as e:
        logging.warning(f"时间解析失败: {clean_str} ({str(e)})")
        return 0


class XHSCrawler:
    def __init__(self, url_checker: Optional[Callable] = None, insert_callback: Optional[Callable] = None):
        options = webdriver.ChromeOptions()
        # options.add_argument("--headless")
        options.add_experimental_option("excludeSwitches", ['enable-automation'])
        options.add_argument("--disable-blink-features=AutomationControlled")
        # options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, '
        #                      'like Gecko) Chrome/86.0.4240.198 Safari/537.36')
        print("准备初始化浏览器")
        self.driver = webdriver.Chrome(options=options)
        print("加载stealth.min.js")
        # 运行这段JS隐藏浏览器特征  https://github.com/berstend/puppeteer-extra/blob/stealth-js/stealth.min.js
        with open('./stealth.min.js', 'r') as f:
            stealth_script = f.read()

        print("运行JS")
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': stealth_script})
        print("浏览器运行成功")

        # self.driver.get('https://bot.sannysoft.com/')
        self.seen_links = set()
        self.notes_data = []
        self.url_checker = url_checker
        self.insert_callback = insert_callback
        self.main_window = None
        self.all_links = set()
        self.collected_quick_data = []  # 快速模式数据缓存
        self.wait_rate = 5

    def login(self):
        """优化登录流程"""
        self.driver.get('https://www.xiaohongshu.com/explore')
        self.main_window = self.driver.current_window_handle

        if os.path.exists("xhs_cookie.pkl"):
            try:
                cookies = pickle.load(open("xhs_cookie.pkl", "rb"))
                for cookie in cookies:
                    self.driver.add_cookie(cookie)
                self.driver.refresh()
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'user.side-bar-component'))
                )
                return
            except Exception as e:
                print(f"页面访问失败，错误详情：\n{e.msg}")
                print(f"Cookie加载失败: {str(e)}")

        # 手动登录流程
        WebDriverWait(self.driver, 120).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'user.side-bar-component'))
        )
        pickle.dump(self.driver.get_cookies(), open("xhs_cookie.pkl", "wb"))
        print('登录成功')

    def extract_notes(self):
        """改进的笔记链接提取"""
        notes = []
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.note-item'))
            )
            items = self.driver.find_elements(By.CSS_SELECTOR, '.note-item')

            for item in items:
                try:
                    link_element = item.find_element(By.CSS_SELECTOR, 'a.cover.mask.ld')
                    note_url = link_element.get_attribute('href')
                    print(note_url)
                    if note_url in self.seen_links:
                        continue
                    if self.url_checker and self.url_checker(note_url):
                        continue

                    title_element = item.find_element(By.CSS_SELECTOR, '.title > span')
                    notes.append({
                        'link': note_url,
                        'title': title_element.text[:600],
                        'images': [],
                        'content': ''
                    })
                    self.seen_links.add(note_url)
                except Exception as e:
                    logging.error(f"提取笔记异常: {str(e)}")
        except Exception as e:
            logging.error(f"页面加载异常: {str(e)}")
        return notes

    def process_single_note(self, origin_note_url: str):
        """处理单个笔记详情页（支持视频封面提取）"""
        # URL转换
        note_url = convert_xhs_url(origin_note_url)
        print(f"打开URL: {origin_note_url} -> {note_url}")
        img_urls = []  # 确保变量始终存在
        try:
            # 新标签页操作逻辑
            self.driver.switch_to.window(self.main_window)
            self.driver.execute_script(f"window.open('{note_url}');")
            new_window = [w for w in self.driver.window_handles if w != self.main_window][0]
            self.driver.switch_to.window(new_window)
            print("等待网页加载")

            # 统一等待页面基础元素
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".note-container"))
            )
            print("网页已加载")
            time.sleep(2 * self.wait_rate)

            try:
                # 时间提取
                time_element = self.driver.find_element(By.CSS_SELECTOR, '.bottom-container .date')
                raw_time = time_element.text.strip()
                auth_time = parse_xhs_time(raw_time)
                print(f"解析时间: {raw_time} -> {auth_time}")
            except Exception as te:
                logging.warning(f"时间提取失败: {str(te)}")
                auth_time = 0

            # 新增：点赞数提取

            try:
                like_count = 0
                # 尝试定位点赞数元素
                like_element = self.driver.find_element(
                    By.CSS_SELECTOR,
                    '.interact-container .like-active .count'
                )
                like_text = like_element.text.strip()
                print("like = " + like_text)
                # 处理点赞数的不同表示形式
                if '万' in like_text:
                    # 处理"万"单位的点赞数
                    like_count = int(float(like_text.replace('万', '')) * 10000)
                elif 'k' in like_text.lower():
                    # 处理"k"单位的点赞数
                    like_count = int(float(like_text.lower().replace('k', '')) * 1000)
                else:
                    # 直接转换为整数
                    like_count = int(like_text) if like_text.isdigit() else 0

                print(f"提取到点赞数: {like_count}")
            except Exception as le:
                logging.warning(f"点赞数提取失败: {str(le)}")
                like_count = 0

            # 视频封面提取逻辑
            try:
                # 通过更稳定的选择器判断视频类型
                video_element = self.driver.find_element(By.CSS_SELECTOR, '.player-container')
                if video_element:
                    print("检测到视频笔记，提取封面")
                    # 使用精确选择器定位封面元素
                    poster = self.driver.find_element(By.CSS_SELECTOR, 'xg-poster.xgplayer-poster')
                    style = poster.get_attribute('style')
                    # 提取并清洗封面URL
                    cover_url = style.split('url("')[1].split('")')[0].replace('&quot;', '')
                    print(f"提取到视频封面: {cover_url}")
                    img_urls = [cover_url]  # 包装成列表保持结构统一

            except Exception as ve:
                # 非视频笔记执行原图提取逻辑
                print("是图文笔记，执行常规图片提取")
                try:
                    swiper = self.driver.find_element(By.CLASS_NAME, 'swiper-wrapper')
                    for img in swiper.find_elements(By.TAG_NAME, 'img'):
                        src = img.get_attribute('src')
                        if src and src.startswith('http'):
                            # 判断是否存在相同
                            if src in img_urls:
                                print(f"跳过重复图片: {src}")
                                continue
                            img_urls.append(src)
                            print(f"提取到图片: {src}")

                except Exception as ie:
                    logging.warning(f"图片提取失败: {str(ie)}")

            # 统一内容提取逻辑
            content = ''
            try:
                text_element = self.driver.find_element(By.CSS_SELECTOR, '.note-content .desc')
                content = text_element.text.replace('\n', ' ').strip()[:2000]
                print(f"提取到内容: {content[:50]}...")  # 防止日志过长
            except Exception as te:
                logging.warning(f"内容提取失败: {str(te)}")

            # 标题提取优化
            title = ''
            try:
                title_element = self.driver.find_element(By.ID, 'detail-title')
                title = title_element.text.strip()
                print(f"提取到标题: {title}")
            except Exception as titile_e:
                logging.warning(f"标题提取失败: {str(titile_e)}")

            baseUrl = note_url.split('?')[0]
            return {
                'images': img_urls,
                'content': content,
                'url': baseUrl,
                'title': title,
                'auth_time': auth_time,
                'like_count': like_count,
            }

        except Exception as e:
            logging.error(f"笔记处理失败 {note_url}: {str(e)}", exc_info=True)
            return None
        finally:
            try:
                self.driver.close()
                self.driver.switch_to.window(self.main_window)
            except Exception as close_e:
                logging.warning(f"窗口关闭异常: {str(close_e)}")

    def extract_current_links(self):
        """实时提取当前可见的笔记链接（精确版）"""
        current_links = set()
        try:
            items = self.driver.find_elements(By.CSS_SELECTOR, '.note-item')
            for item in items:
                try:
                    # 通过精确的 CSS 选择器定位笔记链接
                    link_element = item.find_element(
                        By.CSS_SELECTOR, 'a.cover.mask.ld[href^="/user/profile/"]'
                    )
                    raw_url = link_element.get_attribute('href')

                    # 清洗 URL 的两种方式（根据需求选择一种）
                    # 方案二：完全原始 URL（不推荐）
                    clean_url = raw_url.replace('&amp;', '&')  # 转换 HTML 实体

                    current_links.add(clean_url)
                except Exception as e:
                    continue
        except Exception as e:
            logging.warning(f"提取链接时遇到异常: {str(e)}")
        return current_links

    def smart_scroll(self, spd_setting=1, max_scroll=20):
        """智能滚动采集（动态保存链接）"""
        total_scroll = 0
        no_new_count = 0
        max_no_new = 6
        last_height = 0

        # 新增：记录最大滚动次数
        logging.info(f"智能滚动设置: 最大滚动次数={max_scroll}")


        while no_new_count < max_no_new and total_scroll < max_scroll:
            # 获取当前屏幕可见链接
            current_links = self.extract_current_links()
            # 关键修复：逐个转换链接
            converted_new_links = {
                convert_xhs_url(link).split('?')[0]
                for link in (current_links - self.all_links)
            }
            new_links = converted_new_links - self.all_links

            # 快速模式即时处理
            if spd_setting == 2 and new_links:
                self.process_quick_data(new_links)  # 直接传递已转换的链接集合

            # 更新全局链接集合（使用原始链接）
            self.all_links.update(current_links)
            logging.info(f"当前总链接数：{len(self.all_links)} 新增：{len(new_links)}")

            # 执行滚动
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5 * self.wait_rate)  # 等待新内容加载

            # 检查滚动是否生效
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                no_new_count += 1
            else:
                no_new_count = 0
                last_height = new_height

            total_scroll += 1

    def crawl_author(self, brand: Dict):
        """处理单个作者（支持三种采集模式）"""
        try:
            spd_setting = brand.get('rednote_spd_setting', 1)  # 获取采集配置
            logging.info(f"品牌[{brand['brand_name']}]采集配置: {spd_setting}")
            # 配置检查
            if spd_setting == 3:
                logging.info(f"品牌[{brand['brand_name']}]配置不采集，跳过")
                return True

            # 新增：检查已采集数量
            collected_count = self.get_collected_count(brand['id'])
            logging.info(f"品牌[{brand['brand_name']}]已采集数量: {collected_count}")

            # 新增：如果全量采集且已采集>20，则不滚动
            max_scroll = 1 if (spd_setting == 1 and collected_count > 20) else 20

            self.driver.get(brand['rednote_url'])
            self.smart_scroll(spd_setting, max_scroll)  # 传入采集模式参数

            # 全量采集模式处理
            if spd_setting == 1:
                for note_url in self.all_links:
                    base_url = convert_xhs_url(note_url).split('?')[0]
                    if self.url_checker and self.url_checker(base_url):
                        logging.info(f"已处理过，跳过: {base_url}")
                        continue

                    detail = self.process_single_note(note_url)
                    if detail:
                        detail.update({
                            'brand_id': brand['id'],
                            'brand_name': brand['brand_name']
                        })
                        if self.insert_callback:
                            try:
                                self.insert_callback(detail)
                            except Exception as e:
                                logging.error(f"数据库插入失败: {str(e)}")

            # 快速采集模式数据提交
            elif spd_setting == 2:
                for quick_data in self.collected_quick_data:
                    logging.info(f"写入快速采集数据: {quick_data}")
                    quick_data.update({
                        'brand_id': brand['id'],
                        'brand_name': brand['brand_name'],
                        'auth_time': 0  # 快速模式无时间信息
                    })
                    if self.insert_callback:
                        try:
                            self.insert_callback(quick_data)
                        except Exception as e:
                            logging.error(f"数据库插入失败: {str(e)}")
                logging.info(f"快速采集数据入库成功: {len(self.collected_quick_data)} 条")

            # 清理采集缓存
            self.all_links.clear()
            self.collected_quick_data.clear()
            return True
        except Exception as e:
            logging.error(f"作者采集失败 {brand['rednote_url']}: {str(e)}")
            return False

    def get_collected_count(self, brand_id: int) -> int:
        """查询该品牌已采集数量"""
        # 使用url_checker函数（即db.is_url_exists）的底层连接执行查询
        if self.url_checker and hasattr(self.url_checker, '__self__'):
            db = self.url_checker.__self__
            try:
                with db.connection.cursor() as cursor:
                    sql = "SELECT COUNT(*) AS count FROM spider_log WHERE brand_id = %s"
                    cursor.execute(sql, (brand_id,))
                    result = cursor.fetchone()
                    return result['count'] if result else 0
            except Exception as e:
                logging.error(f"查询已采集数量失败: {str(e)}")
                return 0
        return 0

    def process_quick_data(self, new_links):
        """快速采集模式数据处理"""
        current_items = self.driver.find_elements(By.CSS_SELECTOR, '.note-item')
        for item in current_items:
            try:
                link = item.find_element(By.CSS_SELECTOR, 'a.cover.mask.ld').get_attribute('href')
                clean_url = convert_xhs_url(link).split('?')[0]

                # 新增数据库去重检查
                if self.url_checker and self.url_checker(clean_url):
                    logging.info(f"已存在，跳过快速采集: {clean_url}")
                    continue

                if clean_url not in new_links:
                    continue

                # 提取首图
                try:
                    img = item.find_element(By.CSS_SELECTOR, 'img[src*="xhscdn.com"]')
                    cover_url = img.get_attribute('src').split('?')[0]
                except Exception as e:
                    logging.warning(f"首图提取失败: {str(e)}")
                    cover_url = ""

                # 提取标题
                try:
                    title = item.find_element(By.CSS_SELECTOR, '.title > span').text[:600]
                except Exception as e:
                    title = "无标题"
                    logging.warning(f"标题提取失败: {str(e)}")

                self.collected_quick_data.append({
                    'url': clean_url,
                    'images': [cover_url] if cover_url else [],
                    'title': title,
                    'content': ''
                })
            except Exception as e:
                logging.error(f"快速采集异常: {str(e)}")


class DatabaseManager:
    def __init__(self):
        self.connection = pymysql.connect(
            host='111.229.182.88',
            port=3306,
            user='root',
            password='s*xNvd%v@',
            database='sukitime',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def fetch_brand_urls(self) -> list:
        with self.connection.cursor() as cursor:
            # and id not in (SELECT DISTINCT brand_id from spider_log )
            sql = """
                SELECT id, brand_name, rednote_url, rednote_spd_setting 
                FROM brand 
                WHERE rednote_url != '' AND is_delete = 0
                ORDER BY spider_index DESC, last_gather_time ASC
            """
            cursor.execute(sql)
            return cursor.fetchall()

    def update_last_gather_time(self, brand_id: int):
        with self.connection.cursor() as cursor:
            sql = "UPDATE brand SET last_gather_time = NOW() WHERE id = %s"
            cursor.execute(sql, (brand_id,))
            self.connection.commit()

    def is_url_exists(self, title: str) -> bool:
        with self.connection.cursor() as cursor:
            sql = "SELECT 1 FROM spider_log WHERE url = %s LIMIT 1"
            cursor.execute(sql, (title,))
            return bool(cursor.fetchone())

    def insert_one(self, data: Dict):
        """单条插入优化"""
        with self.connection.cursor() as cursor:
            sql = """
                INSERT INTO spider_log (
                    msg_type, status, origin_type, title, 
                    content, url, images, brand_id, 
                    brand_name, auth_time, created_at, updated_at, likes
                ) VALUES (
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s
                )
            """
            cursor.execute(sql, (
                0, 0, 'xhs',
                data['title'],
                data['content'],
                data['url'],
                ','.join(data['images']),
                data['brand_id'],
                data['brand_name'],
                data.get('auth_time', 0),
                int(time.time()),
                int(time.time()),
                data.get('like', 0)
            ))
            self.connection.commit()

    def batch_insert(self, data: list):
        """批量插入优化"""
        with self.connection.cursor() as cursor:
            sql = """
                INSERT INTO spider_log (
                    msg_type, status, origin_type, title, 
                    content, url, images, brand_id, 
                    brand_name, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s, 
                    %s, %s, %s
                )
            """
            batch = []
            for item in data:
                batch.append((
                    0, 0, 'xhs',
                    item['title'],
                    item['content'],
                    item['url'],
                    ','.join(item['images']),
                    item['brand_id'],
                    item['brand_name'],
                    int(time.time()),
                    int(time.time())
                ))

            cursor.executemany(sql, batch)
            self.connection.commit()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            # 文件处理器使用UTF-8编码
            logging.FileHandler('xhs_crawler.log', encoding='utf-8'),

            # 控制台处理器使用带错误处理的UTF-8
            logging.StreamHandler(stream=open(sys.stdout.fileno(),
                                              'w',
                                              encoding='utf-8',
                                              errors='replace'))
        ]
    )
    print("初始化DB")
    db = DatabaseManager()
    print()
    crawler = XHSCrawler(url_checker=db.is_url_exists, insert_callback=db.insert_one)

    try:
        print("准备登录")
        crawler.login()

        for brand in db.fetch_brand_urls():
            try:
                logging.info(f"处理品牌: {brand['brand_name']}")

                # if crawler.url_checker(brand['rednote_url']):
                #     logging.info(f"已处理过，跳过: {brand['brand_name']}")
                #     continue
                if crawler.crawl_author(brand):
                    # 采集成功时更新采集时间
                    db.update_last_gather_time(brand['id'])
                    logging.info(f"已更新采集时间: {brand['brand_name']}")

                # 批量写入数据库
                # db.batch_insert(crawler.notes_data)
                # logging.info(f"成功入库 {len(crawler.notes_data)} 条笔记")
                crawler.notes_data.clear()
                crawler.all_links.clear()



            except Exception as e:
                logging.error(f"品牌处理异常 {brand['brand_name']}: {str(e)}")
                continue

    finally:
        crawler.driver.quit()
        db.connection.close()
        logging.info("爬虫任务正常结束")


if __name__ == "__main__":
    main()
