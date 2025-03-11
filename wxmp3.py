from wxpy import *
from itchat_uos import *


# 登录微信
bot = Bot()

# 查找指定的群聊
group = bot.groups().search('苏打饼干打孔工厂のんの')[0]  # 将 '群聊名称' 替换为你要发送的群聊名称

# 发送本地音频文件
group.send_file('1.mp3')  # 将 '1.mp3' 替换为你要发送的音频文件路径

# 提示发送成功
print("音频文件已发送到群聊：", group.name)

# 保持登录状态，避免脚本立即退出
embed()