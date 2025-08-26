import os
import pickle
import time
import urllib
import urllib.parse
import pymysql
import logging
import re
from typing import Dict, Optional, Callable, List
from datetime import datetime, timedelta
from selenium.webdriver import Chrome
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 艺术家采集配置
ARTIST_SPIDER_SETTING = {
    'full_collect': 1,  # 全量采集
    'partial_collect': 2,  # 只采集链接和首图
    'no_collect': 3,  # 不采集
}


def convert_xhs_url(original_url):
    """转换小红书URL格式"""
    parsed_url = urllib.parse.urlparse(original_url)
    new_query = parsed_url.query.replace('&amp;', '&')

    path_parts = parsed_url.path.split('/')
    if len(path_parts) >= 5 and path_parts[1] == 'user' and path_parts[2] == 'profile':
        note_id = path_parts[4]
        new_path = f'/explore/{note_id}'
    else:
        new_path = parsed_url.path

    new_parsed = parsed_url._replace(path=new_path, query=new_query)
    return urllib.parse.urlunparse(new_parsed)


def parse_xhs_time(time_str: str) -> int:
    """解析小红书时间格式"""
    clean_str = time_str.replace('编辑于 ', '').strip()
    parts = re.split(r'\s+(?=[\u4e00-\u9fa5]{2,5}$)', clean_str)
    time_part = parts[0]

    now = datetime.now()
    current_year = now.year

    try:
        # 处理相对时间
        if '天前' in time_part:
            days = int(re.search(r'\d+', time_part).group())
            return int((now - timedelta(days=days)).timestamp())
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

        # 处理绝对时间
        time_formats = [
            (r'(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{2})', "%m-%d %H:%M"),
            (r'(\d{4})-(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{2})', "%Y-%m-%d %H:%M"),
            (r'(\d{1,2})-(\d{1,2})', "%m-%d"),
        ]

        for pattern, time_format in time_formats:
            if re.match(pattern, time_str):
                dt = datetime.strptime(time_str, time_format)
                if dt.year == 1900:
                    dt = dt.replace(year=current_year)
                    if dt > now + timedelta(days=60):
                        dt = dt.replace(year=current_year - 1)
                return int(dt.timestamp())

        return 0
    except Exception as e:
        logging.warning(f"时间解析失败: {clean_str} ({str(e)})")
        return 0


class ArtistXHSCrawler:
    def __init__(self, url_checker: Optional[Callable] = None,
                 insert_callback: Optional[Callable] = None):
        options = webdriver.ChromeOptions()
        # 移除无头模式设置，以支持验证码处理
        # options.add_argument("--headless")
        options.add_experimental_option("excludeSwitches", ['enable-automation'])
        options.add_argument("--disable-blink-features=AutomationControlled")

        print("初始化浏览器...")
        self.driver = webdriver.Chrome(options=options)

        print("加载stealth.min.js...")
        with open('./stealth.min.js', 'r') as f:
            stealth_script = f.read()
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': stealth_script})

        self.seen_links = set()
        self.artwork_data = []
        self.url_checker = url_checker
        self.insert_callback = insert_callback
        self.main_window = None
        self.all_links = set()
        self.collected_quick_data = []

    def login(self):
        """登录小红书"""
        self.driver.get('https://www.xiaohongshu.com/explore')
        self.main_window = self.driver.current_window_handle

        # 尝试加载cookie
        if os.path.exists("xhs_artist_cookie.pkl"):
            try:
                cookies = pickle.load(open("xhs_artist_cookie.pkl", "rb"))
                for cookie in cookies:
                    self.driver.add_cookie(cookie)
                self.driver.refresh()
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'user.side-bar-component'))
                )
                return
            except Exception as e:
                print(f"Cookie加载失败: {str(e)}")

        # 手动登录
        WebDriverWait(self.driver, 300).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'user.side-bar-component'))
        )
        pickle.dump(self.driver.get_cookies(), open("xhs_artist_cookie.pkl", "wb"))
        print('登录成功')

    def extract_artworks(self) -> List[Dict]:
        """提取艺术品链接"""
        artworks = []
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.note-item'))
            )
            items = self.driver.find_elements(By.CSS_SELECTOR, '.note-item')

            for item in items:
                try:
                    link_element = item.find_element(By.CSS_SELECTOR, 'a.cover.mask.ld')
                    artwork_url = link_element.get_attribute('href')

                    if artwork_url in self.seen_links:
                        continue
                    if self.url_checker and self.url_checker(artwork_url):
                        continue

                    title_element = item.find_element(By.CSS_SELECTOR, '.title > span')
                    artworks.append({
                        'link': artwork_url,
                        'title': title_element.text[:600],
                        'images': [],
                        'content': ''
                    })
                    self.seen_links.add(artwork_url)
                except Exception as e:
                    logging.error(f"提取艺术品异常: {str(e)}")
        except Exception as e:
            logging.error(f"页面加载异常: {str(e)}")
        return artworks

    def process_single_artwork(self, origin_url: str) -> Optional[Dict]:
        """处理单个艺术品详情"""
        artwork_url = convert_xhs_url(origin_url)
        print(f"打开艺术品URL: {origin_url} -> {artwork_url}")

        try:
            # 在新标签页打开
            self.driver.switch_to.window(self.main_window)
            self.driver.execute_script(f"window.open('{artwork_url}');")
            new_window = [w for w in self.driver.window_handles if w != self.main_window][0]
            self.driver.switch_to.window(new_window)

            # 等待页面加载
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".note-container"))
            )
            time.sleep(2)

            # 提取发布时间
            try:
                time_element = self.driver.find_element(By.CSS_SELECTOR, '.bottom-container .date')
                raw_time = time_element.text.strip()
                auth_time = parse_xhs_time(raw_time)
                print(f"艺术品发布时间: {raw_time} -> {auth_time}")
            except Exception as te:
                logging.warning(f"时间提取失败: {str(te)}")
                auth_time = 0

            # 提取点赞数
            try:
                like_element = self.driver.find_element(
                    By.CSS_SELECTOR, '.interact-container .like-active .count'
                )
                like_text = like_element.text.strip()

                if '万' in like_text:
                    like_count = int(float(like_text.replace('万', '')) * 10000)
                elif 'k' in like_text.lower():
                    like_count = int(float(like_text.lower().replace('k', '')) * 1000)
                else:
                    like_count = int(like_text) if like_text.isdigit() else 0
                print(f"艺术品点赞数: {like_count}")
            except Exception as le:
                logging.warning(f"点赞数提取失败: {str(le)}")
                like_count = 0

            # 提取图片
            img_urls = []
            try:
                # 视频封面
                video_element = self.driver.find_element(By.CSS_SELECTOR, '.player-container')
                if video_element:
                    poster = self.driver.find_element(By.CSS_SELECTOR, 'xg-poster.xgplayer-poster')
                    style = poster.get_attribute('style')
                    cover_url = style.split('url("')[1].split('")')[0].replace('&quot;', '')
                    img_urls = [cover_url]
            except:
                # 图片作品
                try:
                    swiper = self.driver.find_element(By.CLASS_NAME, 'swiper-wrapper')
                    for img in swiper.find_elements(By.TAG_NAME, 'img'):
                        src = img.get_attribute('src')
                        if src and src.startswith('http'):
                            img_urls.append(src)
                except Exception as ie:
                    logging.warning(f"图片提取失败: {str(ie)}")

            # 提取内容
            content = ''
            try:
                text_element = self.driver.find_element(By.CSS_SELECTOR, '.note-content .desc')
                content = text_element.text.replace('\n', ' ').strip()[:2000]
            except Exception as te:
                logging.warning(f"内容提取失败: {str(te)}")

            # 提取标题
            title = ''
            try:
                title_element = self.driver.find_element(By.ID, 'detail-title')
                title = title_element.text.strip()
            except Exception as titile_e:
                logging.warning(f"标题提取失败: {str(titile_e)}")

            base_url = artwork_url.split('?')[0]
            return {
                'images': img_urls,
                'content': content,
                'url': base_url,
                'title': title,
                'auth_time': auth_time,
                'like_count': like_count,
            }

        except Exception as e:
            logging.error(f"艺术品处理失败 {artwork_url}: {str(e)}", exc_info=True)
            return None
        finally:
            try:
                self.driver.close()
                self.driver.switch_to.window(self.main_window)
            except Exception as close_e:
                logging.warning(f"窗口关闭异常: {str(close_e)}")

    def extract_current_links(self) -> set:
        """提取当前页面的所有链接"""
        current_links = set()
        try:
            items = self.driver.find_elements(By.CSS_SELECTOR, '.note-item')
            for item in items:
                try:
                    link_element = item.find_element(
                        By.CSS_SELECTOR, 'a.cover.mask.ld[href^="/user/profile/"]'
                    )
                    raw_url = link_element.get_attribute('href')
                    clean_url = raw_url.replace('&amp;', '&')
                    current_links.add(clean_url)
                except Exception as e:
                    continue
        except Exception as e:
            logging.warning(f"提取链接时遇到异常: {str(e)}")
        return current_links

    def smart_scroll(self, spd_setting: int):
        """智能滚动加载更多内容，最多滚动5次"""
        total_scroll = 0
        no_new_count = 0
        max_no_new = 3  # 连续3次无新内容则停止
        last_height = 0
        max_scroll = 1  # 最大滚动次数为5

        while no_new_count < max_no_new and total_scroll < max_scroll:
            current_links = self.extract_current_links()
            converted_new_links = {
                convert_xhs_url(link).split('?')[0]
                for link in (current_links - self.all_links)
            }
            new_links = converted_new_links - self.all_links

            # 快速模式处理
            if spd_setting == ARTIST_SPIDER_SETTING['partial_collect'] and new_links:
                self.process_quick_data(new_links)

            self.all_links.update(current_links)
            logging.info(f"当前总链接数：{len(self.all_links)} 新增：{len(new_links)}")

            # 滚动页面
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3.5)

            # 检查是否滚动到底部
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                no_new_count += 1
            else:
                no_new_count = 0
                last_height = new_height

            total_scroll += 1
            logging.info(f"滚动次数: {total_scroll}/{max_scroll}")

    def crawl_artist(self, artist: Dict):
        """采集单个艺术家，无论是否采集过都最多滚动3次"""
        try:

            spd_setting = artist.get('rednote_spd_setting_for_artist', ARTIST_SPIDER_SETTING['no_collect'])
            logging.info(f"艺术家[{artist['brand_name']}]采集配置: {spd_setting}")

            # 检查是否启用采集
            if spd_setting == ARTIST_SPIDER_SETTING['no_collect']:
                logging.info(f"艺术家[{artist['brand_name']}]配置不采集，跳过")
                return True

            # 检查是否设置小红书地址
            if not artist.get('rednote_url'):
                logging.info(f"艺术家[{artist['brand_name']}]未设置小红书地址，跳过")
                return True

            # 无论是否采集过都最多滚动5次
            logging.info(f"艺术家[{artist['brand_name']}]开始采集，最多滚动5次")

            # 开始采集
            self.driver.get(artist['rednote_url'])
            self.smart_scroll(spd_setting)

            # 全量采集模式处理
            if spd_setting == ARTIST_SPIDER_SETTING['full_collect']:
                for artwork_url in self.all_links:
                    base_url = convert_xhs_url(artwork_url).split('?')[0]
                    if self.url_checker and self.url_checker(base_url):
                        logging.info(f"已处理过，跳过: {base_url}")
                        continue

                    detail = self.process_single_artwork(artwork_url)
                    if detail:
                        detail.update({
                            'artist_id': artist['id'],
                            'artist_name': artist['brand_name'],
                            'full_get': 0
                        })
                        if self.insert_callback:
                            try:
                                self.insert_callback(detail)
                            except Exception as e:
                                logging.error(f"数据库插入失败: {str(e)}")

            # 快速采集模式处理
            elif spd_setting == ARTIST_SPIDER_SETTING['partial_collect']:
                for quick_data in self.collected_quick_data:
                    quick_data.update({
                        'artist_id': artist['id'],
                        'artist_name': artist['brand_name'],
                        'auth_time': 0,
                        'full_get': 0
                    })
                    if self.insert_callback:
                        try:
                            self.insert_callback(quick_data)
                        except Exception as e:
                            logging.error(f"数据库插入失败: {str(e)}")
                logging.info(f"快速采集数据入库成功: {len(self.collected_quick_data)} 条")

            # 清理缓存
            self.all_links.clear()
            self.collected_quick_data.clear()
            return True
        except Exception as e:
            logging.error(f"艺术家采集失败 {artist['rednote_url']}: {str(e)}")
            return False

    def get_collected_count(self, artist_id: int) -> int:
        """查询已采集数量"""
        if self.url_checker and hasattr(self.url_checker, '__self__'):
            db = self.url_checker.__self__
            try:
                with db.connection.cursor() as cursor:
                    sql = "SELECT COUNT(*) AS count FROM artist_spider_log WHERE brand_id = %s"
                    cursor.execute(sql, (artist_id,))
                    result = cursor.fetchone()
                    return result['count'] if result else 0
            except Exception as e:
                logging.error(f"查询已采集数量失败: {str(e)}")
                return 0
        return 0

    def process_quick_data(self, new_links: set):
        """处理快速采集数据"""
        current_items = self.driver.find_elements(By.CSS_SELECTOR, '.note-item')
        for item in current_items:
            try:
                link = item.find_element(By.CSS_SELECTOR, 'a.cover.mask.ld').get_attribute('href')
                clean_url = convert_xhs_url(link).split('?')[0]

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


class ArtistDatabaseManager:
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

    def fetch_artists(self) -> list:
        """获取需要采集的艺术家列表"""
        with self.connection.cursor() as cursor:
            sql = """
                SELECT id, brand_name, rednote_url, rednote_spd_setting_for_artist 
                FROM brand 
                WHERE is_delete = 0 
                AND is_bjd_artist = 1 
                AND rednote_url != '' 
                AND rednote_spd_setting_for_artist != 3
                ORDER BY spider_index DESC, last_gather_time ASC
            """
            cursor.execute(sql)
            return cursor.fetchall()

    def update_last_gather_time(self, artist_id: int):
        """更新最后采集时间"""
        with self.connection.cursor() as cursor:
            sql = "UPDATE brand SET last_gather_time = NOW() WHERE id = %s"
            cursor.execute(sql, (artist_id,))
            self.connection.commit()

    def is_url_exists(self, url: str) -> bool:
        """检查URL是否已存在"""
        with self.connection.cursor() as cursor:
            sql = "SELECT 1 FROM artist_spider_log WHERE url = %s LIMIT 1"
            cursor.execute(sql, (url,))
            return bool(cursor.fetchone())

    def insert_artist_data(self, data: Dict):
        """插入艺术家作品数据"""
        with self.connection.cursor() as cursor:
            sql = """
                INSERT INTO artist_spider_log (
                    msg_type, status, origin_type, title, 
                    content, url, images, brand_id, 
                    brand_name, created_at, updated_at, 
                    full_get, auth_time, likes
                ) VALUES (
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s, 
                    %s, %s, %s,
                    %s, %s, %s
                )
            """
            cursor.execute(sql, (
                0, 0, 'xhs',
                data.get('title', ''),
                data.get('content', ''),
                data.get('url', ''),
                ','.join(data.get('images', [])),
                data.get('artist_id', 0),
                data.get('artist_name', ''),
                int(time.time()),
                int(time.time()),
                data.get('full_get', 0),
                data.get('auth_time', 0),
                data.get('like_count', 0)
            ))
            self.connection.commit()


def main():
    """主函数"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('artist_crawler.log'),
            logging.StreamHandler()
        ]
    )

    print("初始化数据库连接...")
    db = ArtistDatabaseManager()

    print("初始化爬虫...")
    crawler = ArtistXHSCrawler(
        url_checker=db.is_url_exists,
        insert_callback=db.insert_artist_data
    )

    try:
        print("准备登录小红书...")
        crawler.login()

        artists = db.fetch_artists()
        logging.info(f"找到 {len(artists)} 位需要采集的艺术家")

        for artist in artists:
            try:
                logging.info(f"开始采集艺术家: {artist['brand_name']}")

                if crawler.crawl_artist(artist):
                    db.update_last_gather_time(artist['id'])
                    logging.info(f"已更新采集时间: {artist['brand_name']}")

                crawler.artwork_data.clear()
                crawler.all_links.clear()
            except Exception as e:
                logging.error(f"艺术家处理异常 {artist['brand_name']}: {str(e)}")
                continue

    finally:
        crawler.driver.quit()
        db.connection.close()
        logging.info("艺术家采集任务完成")


if __name__ == "__main__":
    main()
