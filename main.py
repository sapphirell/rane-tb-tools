# # This is a sample Python script.
#
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
#配置防止淘宝滑块
options = webdriver.ChromeOptions()
# options.add_argument('--proxy-server=127.0.0.1:8080')
options.add_experimental_option("excludeSwitches", ['enable-automation'])

web = webdriver.Chrome(options=options)
web.get('https://login.taobao.com/')
# web.set_window_size(400, 400)
print('请手动扫码登录')
# web.close()

qrcodeElement = web.find_element(By.CSS_SELECTOR, '.icon-qrcode')

qrcodeElement.click()

while True:
    try:
        cart = web.find_element(By.LINK_TEXT, '我的购物车')
        print('已确认登录')
        break
    except Exception as e:
        time.sleep(1)
        print('等待登录成功')
        continue

# 存储登录cookie
cookies = web.get_cookies()
# 获取当前窗口
cart.click()
# 关闭旧的窗口
# web.current_window_handle
windows = web.window_handles
web.switch_to.window(windows[1])


# print('web.current_window_handle')
# web.switch_to.window()

# 跳转到购物车后抢单
def cart_and_pay():
    windows = web.window_handles
    print(windows)
    while True:
        try:
            submit = web.find_element(By.ID, 'J_SmallSubmit')
            print('已确认跳转到购物车页面')
            break
        except Exception as e:
            time.sleep(1)
            print('等待跳转到购物车页面')
            continue
    time.sleep(1)
    # 点击全选
    try:
        print('尝试点击全选购物车')
        selectAll1 = web.find_element(By.ID, 'J_SelectAll1')
        selectAll1.click()
    except Exception as e:
        print('全选失败，原因：'+e)

    time.sleep(1)
    # 检测是否到结算时间
    print('等待中：')
    while True:
        if int(round(time.time() * 1000)) > orderTime * 1000:
            print('开始抢购')
            break
        else:
            # 如果时间还差的远，1秒钟刷新一次
            if orderTime * 1000 - int(round(time.time() * 1000)) > 1000 * 60:
                print('时间还早')
                time.sleep(5)
            print('.', end='')
            continue
    # 点击结算
    go2order = web.find_element(By.CLASS_NAME, 'btn-area')
    go2order.click()
    # 检测是否提交了订单
    while True:
        try:
            submit = web.find_element(By.LINK_TEXT, '提交订单')
            print('已确认跳转到订单结算页面')

            break
        except Exception as e:
            try:
                # 检测是否是因为被拍没了
                invalid = web.find_element(By.ID, 'invalidOrderDescPC_2')
                print('抢拍失败原因：该订单库存不足,自动返回购物车')
                web.get('https://cart.taobao.com/cart.htm')
                return False
            except Exception as e:
                time.sleep(1)
                print('等待跳转到订单结算页面')
            continue
    submit.click()
    return True


# windows = web.window_handles
# web.switch_to.window(windows[2])
while True:
    order_status = cart_and_pay()
    if order_status:
        break
    else:
        continue

input('Press Enter to exit...')
