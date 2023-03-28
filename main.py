# # This is a sample Python script.
#
import configparser
import time

from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By

# 预先检查配置文件是否合理
cf = configparser.ConfigParser()
cf.read('config.ini')
configTime = cf['DEFAULT']['time']

print("您配置的抢购时间为：" + configTime + "确认无误三秒后开始运行，按ctrl+c终止")
time.sleep(3)
orderTime = int(time.mktime(time.strptime(configTime, "%Y-%m-%d %H:%M:%S")))

web = Chrome()
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
# 获取当前窗口
cart.click()
# 关闭旧的窗口
# web.current_window_handle
windows = web.window_handles
web.switch_to.window(windows[1])
# print('web.current_window_handle')
# web.switch_to.window()
while True:
    try:
        submit = web.find_element(By.ID, 'J_SmallSubmit')
        print('已确认跳转到购物车页面')
        break
    except Exception as e:
        time.sleep(1)
        print('等待跳转到购物车页面')
        continue

# 点击全选
selectAll1 = web.find_element(By.ID, 'J_SelectAll1')
selectAll1.click()
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
            print('抢拍失败原因：该订单库存不足')
            time.sleep(1)
        except Exception as e:
            time.sleep(1)
            print('等待跳转到订单结算页面')
        continue


submit.click()

# windows = web.window_handles
# web.switch_to.window(windows[2])
def reorder():
    # 重新下单
    print('重新跳转到购物车页面')
    # item-headers-invalid


input('Press Enter to exit...')
