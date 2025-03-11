import configparser
import time
from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By

# 预先检查配置文件是否合理
cf = configparser.ConfigParser()
cf.read('config.ini')
configTime = cf['DEFAULT']['time']
print("您配置的抢购时间为：" + configTime + "确认无误三秒后开始运行，按ctrl+c终止")
# time.sleep(3)
orderTime = int(time.mktime(time.strptime(configTime, "%Y-%m-%d %H:%M:%S")))

options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ['enable-automation'])
options.add_argument("--disable-blink-features=AutomationControlled")




web = webdriver.Chrome(options=options)

web.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",{
            "source":"""
              Object.defineProperty(navigator,'webdriver',{
                get: () => undefined
              })
              """
        })

web.get('https://sso.weidian.com/login/index.php?redirect=https%3A%2F%2Fshop1725890150.v.weidian.com%2Fitem.html%3FitemID%3D6241829296%26spider_token%3Da5a8')

try:
    switch = web.find_element(By.XPATH, '//li[contains(text(), "登录")]')
except Exception as e:
    print(e)



input('Press Enter to exit...')