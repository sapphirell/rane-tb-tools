"""
Python写的百度贴吧工具
"""

import urllib.request as request
from bs4 import BeautifulSoup
import re
import tieba.log_config
import logging

logger = logging.getLogger()

encoding = 'GBK'

base_url = 'http://tieba.baidu.com/bawu2/platform/listMemberInfo?word=c%D3%EF%D1%D4'
# base_url = 'http://tieba.baidu.com/bawu2/platform/listMemberInfo?word=%B9%FD%C1%CB%BC%B4%CA%C7%BF%CD'
start_page = 1
total_pages = None

connection = _get_connection(host, username, password, db_name)


def _get_total_pages():
    html = request.urlopen(base_url).read().decode(encoding)
    soup = BeautifulSoup(html, 'lxml')
    page_span = soup.find('span', class_='tbui_total_page')
    p = re.compile(r'共(\d+)页')
    result = p.match(page_span.string)
    global total_pages
    total_pages = int(result.group(1))

    logger.info(f'会员共{total_pages}页')


def _find_all_users():
    global connection
    for i in range(start_page, total_pages + 1):
        target_url = f'{base_url}&pn={i}'
        logger.info(f'正在分析第{i}页')
        html = request.urlopen(target_url).read().decode(encoding)
        soup = BeautifulSoup(html, 'lxml')
        outer_div = soup.find('div', class_='forum_info_section member_wrap clearfix bawu-info')
        inner_spans = outer_div.find_all('span', class_='member')
        for index, span in enumerate(inner_spans):
            name_link = span.find('a', class_='user_name')
            name = name_link.string
            logger.info(f'已找到 {name}')

            try:
                _insert_table(connection, name)
            except:
                logger.error(f'第{i}页{index}第个用户 {name} 发生异常')


import datetime

if __name__ == '__main__':
    _get_total_pages()
    _find_all_users()