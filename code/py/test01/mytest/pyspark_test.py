import configparser
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = "D:/app/py10/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:/app/py10/python.exe"

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 配置读取
config = configparser.ConfigParser()
config_file_path = os.path.join(os.path.dirname(os.getcwd()), 'init', 'config.ini')
if not os.path.exists(config_file_path):
    logger.warning(f"配置文件未找到: {config_file_path}")
config.read(config_file_path)
# 提取数据库配置
db_section = config["database_lcz"]
jdbc_url = f"jdbc:mysql://{db_section['db.host']}:{db_section['db.port']}/{db_section['db.database']}"
db_user = db_section["db.user"]
db_password = db_section["db.password"]
db_driver = db_section["db.driver"]
print(config.get('database_lcz', 'db.host'))
print(jdbc_url)

# 创建Spark会话
spark = SparkSession.builder.appName("mySparkTest").config("spark.jars",
                                                           r"D:\app\py10\Lib\site-packages\pyspark\jars\mysql-connector-j-9.2.0.jar").getOrCreate()

# 从 MySQL 读取数据
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dwd_user_comprehensive_profile_di_0625") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", db_driver) \
    .load()
df.show(5)
df.printSchema()
df.describe().show()
df.select("user_id", "user_name").show(5)
df.filter(col("stat_date") == '2025-07-03').drop("id").show(5)
