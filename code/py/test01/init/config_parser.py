import configparser
import os

# 创建 ConfigParser 对象
config = configparser.ConfigParser()

# 添加配置内容
config['database_lcz'] = {
    'db.host': '192.168.10.105',
    'db.port': '3306',
    'db.user': 'licz.1',
    'db.password': 'GjFmT5NEiE',
    'db.database': 'sq_liufengdb',
    'db.driver': 'com.mysql.cj.jdbc.Driver'
}

config['starrocks'] = {
    'host': '192.168.122.1',  # FE 节点 IP
    'port': '9030',  # MySQL 协议端口
    'user': 'root',  # 用户名
    'password': '0205',  # 密码
    'database': 'lcz',  # 数据库名
    'connect_timeout': 5  # 超时时间（秒）
}

config['database_init'] = {
    'db.host': '192.168.10.105',
    'db.port': '3306',
    'db.user': 'sq_liufengdb',
    'db.password': 'sRYJJfCYC83W3Zza',
    'db.database': 'sq_liufengdb'
}

config['deepseek'] = {
    'deepseek.api_key': 'sk-8836e19e374e458ba9e1e1cd0052a2b0',
    'deepseek.endpoint': 'https://api.deepseek.com/v1/chat/completions',
    'deepseek.model': 'deepseek-chat'
}

config['cleaning'] = {
    'cleaning.prompt': '???????????????????????????',
    'cleaning.max_retries': '3',
    'cleaning.delay': '0.5'
}

# 确保 init 目录存在
# init_dir = os.path.join(os.getcwd(), 'init')
# if not os.path.exists(init_dir):
#     os.makedirs(init_dir)

# 写入配置文件并添加注释
config_file_path = os.path.join(os.getcwd(), 'config.ini')
with open(config_file_path, 'w', encoding='utf-8') as configfile:
    # 添加注释
    configfile.write("# MySQL 配置\n")
    config.write(configfile)

    # configfile.write("# DeepSeek API 配置\n")
    # config.write(configfile)
    #
    # configfile.write("# 清理配置\n")
    # config.write(configfile)

print(f"配置文件已成功创建: {config_file_path}")

print("配置文件已成功创建: config.ini")
