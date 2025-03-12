import os
import pickle
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
        self.seen_links = set()
        self.notes_data = []
        self.url_checker = url_checker
        self.insert_callback = insert_callback
        self.main_window = None
        self.all_links = set()

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
            print("网页已加载，再等待5s")
            time.sleep(5)

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

    def smart_scroll(self):
        total_scroll = 0
        max_scroll = 20
        no_new_count = 0
        max_no_new = 3

        prev_links = set()
        while no_new_count < max_no_new and total_scroll < max_scroll:
            # 滚动前记录当前链接
            before_links = self.extract_current_links()

            # 执行滚动
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

            # 滚动后捕获新链接
            after_links = self.extract_current_links()

            # 使用集合操作合并链接
            self.all_links |= after_links  # 等价于union更新

            print('当前链接数：', len(self.all_links))

            # 判断是否有新内容
            new_links = after_links - before_links
            if new_links:
                no_new_count = 0
                logging.info(f"本次滚动获得 {len(new_links)} 个新链接")
            else:
                no_new_count += 1
                logging.info(f"无新内容计数：{no_new_count}/{max_no_new}")

            # 底部检测逻辑保持不变...
            total_scroll += 1

    def crawl_author(self, brand: Dict):
        """处理单个作者"""
        try:
            self.driver.get(brand['rednote_url'])
            self.smart_scroll()

            # 提取基础笔记信息
            # base_notes = self.extract_notes()

            # 处理每个笔记详情
            for noteUrl in self.all_links:
                # real url
                baseUrl = noteUrl.split('?')[0]
                # 笔记连接是否在数据库中
                if self.url_checker and self.url_checker(baseUrl):
                    logging.info(f"笔记已处理过，跳过: {noteUrl}")
                    continue
                detail = self.process_single_note(noteUrl)
                if detail:
                    detail.update({
                        'brand_id': brand['id'],
                        'brand_name': brand['brand_name']
                    })
                    print("笔记详情")
                    print(detail)
                    # self.notes_data.append(detail)
                    # 直接调用插入回调函数
                    if self.insert_callback:
                        try:
                            self.insert_callback(detail)  # 实时插入
                        except Exception as e:
                            logging.error(f"数据库插入失败: {str(e)}")

            return True
        except Exception as e:
            logging.error(f"作者采集失败 {brand['rednote_url']}: {str(e)}")
            return False


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
            sql = """
                SELECT id, brand_name, rednote_url 
                FROM brand 
                WHERE rednote_url != '' AND is_delete = 0
                ORDER BY id ASC
            """
            cursor.execute(sql)
            return cursor.fetchall()

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
                    brand_name, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s, 
                    %s, %s, %s
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
                int(time.time()),
                int(time.time())
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
            logging.FileHandler('xhs_crawler.log'),
            logging.StreamHandler()
        ]
    )
    print("初始化DB")
    db = DatabaseManager()
    print()
    crawler = XHSCrawler(url_checker=db.is_url_exists,  insert_callback=db.insert_one)

    try:
        print("准备登录")
        crawler.login()

        for brand in db.fetch_brand_urls():
            try:
                logging.info(f"处理品牌: {brand['brand_name']}")

                if crawler.url_checker(brand['rednote_url']):
                    logging.info(f"已处理过，跳过: {brand['brand_name']}")
                    continue
                crawler.crawl_author(brand)

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
