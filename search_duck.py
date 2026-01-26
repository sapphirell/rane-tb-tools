import json
import time
from ddgs import DDGS
import trafilatura


def search_and_scrape(query):
    print(f"正在搜索: {query} ...")

    # 1. 先搜索获取链接
    search_results = []
    with DDGS() as ddgs:
        results = ddgs.text(query, region='cn-zh', max_results=10)
        if results:
            for r in results:
                search_results.append({
                    "title": r.get('title', ''),
                    "url": r.get('href', ''),
                    "snippet": r.get('body', '')
                })

    # 2. 遍历链接，爬取正文
    final_data = []
    for item in search_results:
        url = item['url']
        print(f"正在抓取: {item['title']} ({url})")

        try:
            # 下载网页 (会自动模拟 User-Agent)
            downloaded = trafilatura.fetch_url(url)

            if downloaded:
                # 提取正文 (自动识别语言、去除噪音)
                # include_formatting=False 保证拿到纯文本
                content = trafilatura.extract(downloaded, include_comments=False)

                if content:
                    # 如果抓取成功，替换掉原来的简短 snippet，或者新增一个字段
                    item['full_content'] = content
                else:
                    item['full_content'] = "正文提取失败 (可能是纯图片或需要登录)"
            else:
                item['full_content'] = "网页下载失败 (403/404 或反爬虫)"

        except Exception as e:
            item['full_content'] = f"抓取出错: {str(e)}"

        final_data.append(item)
        # 礼貌性延时，防止请求过快被封 IP
        time.sleep(1)

    return json.dumps(final_data, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    json_output = search_and_scrape("2026年01-23杭州天气")
    print(json_output)