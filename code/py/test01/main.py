# This is a sample Python script.
import configparser
import os
from logging import basicConfig, getLogger, INFO

import mysql.connector
import psutil

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from faker import Faker


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    a = []
    print(a)
    print(type(a))
    print(isinstance(a, list))
    print(dir(a))
    n = 3
    mylist = [] * n
    print(mylist.__len__())

    # 数据库连接配置
    db_config = {
        'host': '192.168.10.105',  # 数据库主机
        'user': 'licz.1',  # 数据库用户名
        'password': 'GjFmT5NEiE',  # 数据库密码
        'database': 'sq_liufengdb'  # 数据库名称
    }

    # connection = mysql.connector.connect(**db_config)
    # cursor = connection.cursor()
    # 获取列的值
    # cursor.execute("""
    #                select company_address
    #                -- , school_address
    #                from dim_user_info_di_tmp
    #                where id < 16
    #                """)
    #
    # data = cursor.fetchall()
    # print(f'data: {data}')
    # print(f'cursor.fetchall(): {cursor.fetchall()}')
    # print(cursor)
    # company_address_list = []
    # for row in data:
    #     print(row)
    #     company_address_list.append(row[0])
    # print(f'company_address_list: {company_address_list}')
    #
    # cursor.close()
    # connection.close()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
fake = Faker('zh_CN')
print(type(fake.street_address()))

print(fake.street_address())
a = 1
b = 2
list = [a, b]
print(list)
c = fake.name()
print(c)
print(c.strip().split())

import pymysql
from datetime import datetime, timedelta
from collections import defaultdict
import spacy
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch

# 初始化spaCy和Transformers
# nlp = spacy.load("zh_core_web_sm")  # 或使用中文模型如"zh_core_web_lg"
# print(nlp)


config = configparser.ConfigParser()
config_file_path = os.path.join(os.path.join(os.getcwd(), 'init'), 'config.ini')
parent_dir = os.path.dirname(os.getcwd())
print(parent_dir)

config.read(config_file_path)

DEEPSEEK_API_KEY = config.get('deepseek', 'deepseek.api_key', fallback="")
DEEPSEEK_ENDPOINT = config.get('deepseek', 'deepseek.endpoint', fallback="https://api.deepseek.com/v1/chat/completions")
DEEPSEEK_MODEL = config.get('deepseek', 'deepseek.model', fallback="deepseek-chat")
print(f"DEEPSEEK_API_KEY: {DEEPSEEK_API_KEY}")

import jieba

print("jieba 版本:", jieba.__version__)
print("jieba 可用函数:", dir(jieba))

# 测试 lcut 函数
# try:
#     print(jieba.lcut("测试文本"))
# except AttributeError as e:
#     print("lcut 函数不可用:", e)

basicConfig(level=INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = getLogger(__name__)
print(logger)

"""记录内存使用情况"""
process = psutil.Process(os.getpid())
mem_info = process.memory_info()
memory_infos = logger.debug(
    f"内存使用: RSS={mem_info.rss / 1024 / 1024:.2f}MB, "
    f"VMS={mem_info.vms / 1024 / 1024:.2f}MB"
)

# import requests
#
# # Ollama 服务的 URL，确保使用正确的 IP 地址和端口
# url = "http://192.168.18.154:11434/api/generate"  # 如果从同一台机器访问
# # url = "http://192.168.18.154:11434/api/generate"  # 如果从同一网络中的其他设备访问
#
# headers = {
#     "Content-Type": "application/json"
# }
#
# data = {
#     "model": "deepseek-r1:8b",  # 确保模型名称正确
#     "prompt": "请介绍一下人工智能的发展历程"
# }
#
# try:
#     response = requests.post(url, headers=headers, json=data)
#
#     if response.status_code == 200:
#         result = response.json()
#         print(result["response"])  # 输出模型生成的文本
#     else:
#         print(f"请求失败，状态码：{response.status_code}")
#         print(response.text)
#
# except requests.exceptions.RequestException as e:
#     print(f"请求过程中发生错误：{e}")
import requests

try:
    response = requests.get("https://api-inference.huggingface.co", timeout=5)
    print("连接成功，状态码:", response.status_code)
except requests.exceptions.RequestException as e:
    print("连接失败:", e)

config_file_path = os.path.join(os.path.dirname(os.getcwd()), 'init', 'config.ini')
print(os.getcwd())
print(config_file_path)
print(os.path.dirname(os.getcwd()))

os.environ["JAVA_HOME"] = r"D:\app\jdk"  # 替换为你的 Java 8 路径
os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin;" + os.environ["PATH"]  # 确保 Java 8 在 PATH 最前面
print(f'os.environ["PATH"]: {os.environ["PATH"]}')
import sys
sys.path.append("/path/to/spark/python")
print(sys.path)