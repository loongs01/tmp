'''
@Project:mytest
@File:push_alarm_dingding.py
@IDE:PyCharm
@Author:lichaozhong
@Date:2025/1/21 15:18
'''
"""
添加机器人到钉钉群：https://open.dingtalk.com/document/robots/use-group-robots
参数定义：https://open.dingtalk.com/document/robots/send-group-assistant-messages
secret&access_token来源于钉钉群设置时
"""
import time
import hmac
import hashlib
import base64
import urllib.parse
from dingtalkchatbot.chatbot import DingtalkChatbot
import sys


class Alarmdingding():
    def Dingtalk_alarm(self, phoneNo=[], message=''):
        secret = 'SEC45210bd15bc327502cb06c6eea60287a2dfea968a73d822b182073601828df8f@@'
        access_token = '871380da6a0f88236b3125d5fdbedc5b7b74eb8c6732f6c38efb3a3e62d8f838'
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        webhook = 'https://oapi.dingtalk.com/robot/send?access_token={}&timestamp={}&sign={}'.format(
            access_token, timestamp, sign)
        robot = DingtalkChatbot(webhook)
        # if phoneNo :
        #     mobiles_text='@'+'@'.join(phoneNo)
        # robot.send_markdown(is_at_all=True, is_auto_at=True, at_mobiles=phoneNo,
        #                     title="ETL跑批任务异常", text="{}ETL跑批任务异常,请检查{}任务链路:".format(tx_date, project)+mobiles_text
        #                     )
        # print("告警：" + message)
        print(message)
        robot.send_text(msg=message, is_auto_at=True, at_mobiles=phoneNo)


if __name__ == '__main__':
    # flag = 'prod'
    # phoneNo = ["18824280656", "18291650783"]
    # if flag == 'prod':
    #     tx_date = sys.argv[1]
    #     project = sys.argv[2]
    # else:
    #     tx_date = '2022-08-01'
    #     project = 'chapters'

    tx_date = '2025-02-08'
    project = 'reelshort'
    phoneNo = ["18898722985"]
    message = "test"
    Alarmdingding().Dingtalk_alarm(phoneNo, message)
