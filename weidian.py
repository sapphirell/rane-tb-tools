import configparser
import time
import pickle
import os
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

def login():
    # 点击 login_init_by_login 切换
    loginInit = web.find_element(By.ID, 'login_init_by_login')
    loginInit.click()
    time.sleep(sleepTime1)
    switch = web.find_element(By.XPATH, '//span[contains(text(), "登录")]')
    # switch = web.find_element(By.XPATH, '//span[text()="账号密码登录"]')
    switch.click()
    time.sleep(sleepTime1)
    web.find_element(By.ID, 'login_isRegiTele_input').send_keys('17602114904')
    web.find_element(By.ID, 'login_pwd_input').send_keys('Maoliru233')
    time.sleep(sleepTime1)
    web.find_element(By.ID, 'login_pwd_submit').click()

#加载cookie
if os.path.getsize("weidian_cookie.pkl") > 0:
    web.get('https://shop1725890150.v.weidian.com/item.html?itemID=6241829296&spider_token=a5a8')
    cookies = pickle.load(open("weidian_cookie.pkl", "rb"))
    for cookie in cookies:
        web.add_cookie(cookie)
    web.refresh()
else:
    web.get(
        'https://sso.weidian.com/login/index.php?redirect=https%3A%2F%2Fshop1725890150.v.weidian.com%2Fitem.html%3FitemID%3D6241829296%26spider_token%3Da5a8')
    login()


# web.get('https://shop1725890150.v.weidian.com/item.html?itemID=6241829296&spider_token=a5a8')
sleepTime1 = 0.5
sleepTime2 = 0.05


# 等待登录成功
print('等待登录成功')
while True:
    try:
        time.sleep(0.3)
        cart = web.find_element(By.CLASS_NAME, 'entry-shop')
        print('已确认登录')
        #保留cookie
        pickle.dump(web.get_cookies(), open("weidian_cookie.pkl", "wb"))
        try:
            # 检测是否可购买
            isDisable = web.find_element(By.XPATH, '//div[contains(text(), "商品已下架，为你推荐本店其他商品")]')
        except Exception as e:
            print('已经可以购买')
            break
        # 不可购买 刷新直至可以购买
        print('无库存，刷新')
        web.refresh()
    except Exception as e:
        time.sleep(1)
        print('等待登录成功')
        continue


# try:
#     time.sleep(sleepTime1)
#     hiddenBar = web.find_element(By.XPATH, '//div[contains(text(), "商品已下架，为你推荐本店其他商品")]')
#     hiddenBar.click()
#     time.sleep(1)
# except Exception as e:
#     print('未弹出未购买框')


sku = web.find_element(By.CLASS_NAME, 'sku-content-detail')
web.execute_script("window.scrollTo(0,300)")
sku.click()
time.sleep(0.1)
print('选中奶油')
sku1 = web.find_element(By.XPATH, '//li[contains(text(), "奶油")]')
sku1.click()
sku2 = web.find_element(By.XPATH, '//li[contains(text(), "02")]')
sku2.click()
time.sleep(sleepTime2)
buy = web.find_element(By.CLASS_NAME, 'footer-buy-now')
buy.click()
time.sleep(0.1)
#检测是否到订单提交页面
while True:
    try:
        print('检测是否跳转到结算页面')
        submit = web.find_element(By.CLASS_NAME, 'submit_order');
    except Exception as e:
        print('尚未跳转到')
        continue
    break

print('点击购买')
submit.click()

input('Press Enter to exit...')