import io
import os
import re
import sys
import subprocess
import logging
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, udf, collect_list, struct, regexp_replace, first, explode
from pyspark.sql.types import *
import spacy
import jieba
from typing import List, Dict, Optional
from pyspark.sql import functions as F

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
# ============================
# 环境配置（强制使用Java 8）
# ============================
os.environ["JAVA_HOME"] = r"D:\app\jdk"  # 替换为你的Java 8路径
os.environ["PATH"] = f"{os.environ['JAVA_HOME']}\\bin;" + os.environ["PATH"]

# 模拟Hadoop环境（解决winutils.exe问题）
os.environ["HADOOP_HOME"] = r"D:\app\hadoop"  # 虚拟目录
os.makedirs(os.environ["HADOOP_HOME"], exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("spark_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 临时禁用Spark的环境检查（hack方式）
os.environ["SPARK_TESTING"] = "1"  # 绕过部分环境验证


# ============================
# 环境检查（放宽Java版本限制）
# ============================
def check_environment():
    """放宽Java版本检查，允许Java 8 + Spark 3.2.4"""
    try:
        java_version = subprocess.check_output(
            ["java", "-version"],
            stderr=subprocess.STDOUT,
            shell=True
        ).decode('utf-8', errors='ignore')

        if "1.8" not in java_version and "jdk version \"1.8" not in java_version:
            logging.warning(
                f"检测到非Java 8环境（当前版本: {java_version.strip()}），"
                "但Spark 3.2.4理论上支持Java 8。如果出现问题，请升级到Java 11/17。"
            )
    except FileNotFoundError:
        raise RuntimeError("未安装Java")

    # 检查spaCy模型
    try:
        spacy.load("zh_core_web_lg")
    except OSError:
        logging.warning("警告: 未找到spaCy中文模型，将使用jieba分词")


# ============================
# Spark UDF定义
# ============================
@udf(returnType=ArrayType(StringType()))
def spark_jieba_cut(text: str) -> List[str]:
    """中文分词UDF"""
    return list(jieba.cut(text)) if text else []


@udf(returnType=ArrayType(StringType()))
def spark_extract_entities(text: str) -> List[str]:
    """命名实体识别UDF"""
    if not text:
        return []
    try:
        nlp = spacy.load("zh_core_web_lg")
        doc = nlp(text)
        return [ent.text for ent in doc.ents]
    except:
        return []


@udf(returnType=MapType(StringType(), ArrayType(StringType())))
def spark_pattern_extract(text: str) -> Dict[str, List[str]]:
    """模式匹配提取UDF"""
    if not text:
        return {}

    patterns = {
        "favorite_foods": r'(?:喜欢吃|爱吃)([^。；，,\n]+)',
        "hobbies": r'(?:爱好|兴趣是)([^。；，,\n]+)',
        "colors": r'(?:喜欢|偏爱)([^。；，,\n]+?色)'
    }

    return {
        field: [m.strip() for m in re.findall(pat, text) if m.strip()]
        for field, pat in patterns.items()
    }


# ============================
# 核心处理流程
# ============================
class ChatDataProcessor:
    def __init__(self):
        self.spark = self._init_spark()
        self._register_udfs()

    def _init_spark(self) -> SparkSession:
        """初始化Spark会话（本地模式）"""
        return (SparkSession.builder
                .master("local[*]")  # 强制本地模式
                .appName("ChatDataCleaning")
                .config("spark.sql.shuffle.partitions", "4")  # 本地模式减少分区
                .config("spark.default.parallelism", "4")
                .config("spark.sql.execution.arrow.enabled", "true")
                .config("spark.driver.memory", "2g")  # 本地模式减少内存
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")  # 禁用HDFS检查
                .getOrCreate())

    def _register_udfs(self):
        """注册UDF函数"""
        self.spark.udf.register("jieba_cut", spark_jieba_cut)
        self.spark.udf.register("extract_entities", spark_extract_entities)
        self.spark.udf.register("pattern_extract", spark_pattern_extract)

    def load_data(self) -> DataFrame:
        """模拟数据加载（替代JDBC连接）"""
        # 实际使用时取消下面注释，删除模拟数据
        """
        try:
            return self.spark.read.jdbc(
                url="jdbc:mysql://192.168.10.105/sq_liufengdb",
                table="ods_user_interaction_di",
                properties={
                    "user": "licz.1",
                    "password": "GjFmT5NEiE",
                    "driver": "com.mysql.cj.jdbc.Driver"
                },
                numPartitions=2,  # 本地模式减少分区
                column="interaction_time"
            ).select(
                "user_id", "user_name",
                "query_text", "response_text",
                "interaction_time"
            )
        except Exception as e:
            logger.error(f"数据加载失败: {str(e)}")
            raise
        """
        # 模拟数据（用于测试）
        data = [
            (1, "用户A", "我喜欢吃苹果和香蕉", "好的", "2023-01-01"),
            (2, "用户B", "我的爱好是游泳", "知道了", "2023-01-02")
        ]
        return self.spark.createDataFrame(data,
                                          ["user_id", "user_name", "query_text", "response_text", "interaction_time"])

    def preprocess(self, df: DataFrame) -> DataFrame:
        """文本预处理"""
        return df.withColumn(
            "cleaned_text",
            regexp_replace(
                regexp_replace(col("query_text"), r"\s+", " "),
                r"[^\u4e00-\u9fa5a-zA-Z0-9，。！？、]", ""
            )
        )

    def analyze(self, df: DataFrame) -> DataFrame:
        """对话分析"""
        window = Window.partitionBy("user_id").orderBy("interaction_time")
        df = df.withColumn(
            "context",
            collect_list("cleaned_text").over(window.rowsBetween(-1, 0))  # 减少上下文范围
        )

        return df.withColumn("extracted", spark_pattern_extract(col("cleaned_text"))) \
            .withColumn("entities", spark_extract_entities(col("cleaned_text"))) \
            .select(
            "user_id",
            "user_name",
            col("extracted.favorite_foods").alias("foods"),
            col("extracted.hobbies").alias("hobbies"),
            col("extracted.colors").alias("colors"),
            "entities"
        )

    def aggregate(self, df: DataFrame) -> DataFrame:
        """聚合用户画像"""
        for field in ["foods", "hobbies", "colors", "entities"]:
            df = df.withColumn(field, explode(col(field)))

        return df.groupBy("user_id").agg(
            F.collect_set("foods").alias("foods"),
            F.collect_set("hobbies").alias("hobbies"),
            F.collect_set("colors").alias("colors"),
            F.collect_set("entities").alias("entities"),
            F.first("user_name").alias("user_name")
        )

    def save_results(self, df: DataFrame):
        """模拟结果保存"""
        # 实际使用时取消下面注释，删除模拟保存
        """
        try:
            df.write.jdbc(
                url="jdbc:mysql://192.168.10.105/sq_liufengdb",
                table="dwd_user_comprehensive_profile_di",
                mode="overwrite",
                properties={
                    "user": "licz.1",
                    "password": "GjFmT5NEiE",
                    "driver": "com.mysql.cj.jdbc.Driver"
                },
                batchsize=1000
            )
            logger.info("结果保存成功")
        except Exception as e:
            logger.error(f"结果保存失败: {str(e)}")
            raise
        """
        # 模拟保存（打印结果）
        df.show(10, False)
        logger.info("模拟保存成功（实际使用时请取消注释JDBC代码）")


# ============================
# 主程序
# ============================
def main():
    # 检查运行环境
    try:
        check_environment()
    except Exception as e:
        logger.error(f"环境检查失败: {str(e)}")
        sys.exit(1)

    processor = None
    try:
        # 初始化处理器
        processor = ChatDataProcessor()
        logger.info(f"Spark处理器初始化成功（版本: {processor.spark.version})")

        # 处理流程
        raw_data = processor.load_data()
        cleaned_data = processor.preprocess(raw_data)
        features = processor.analyze(cleaned_data)
        profiles = processor.aggregate(features)

        # 保存结果
        processor.save_results(profiles)

    except Exception as e:
        logger.error("处理过程中发生错误", exc_info=True)
        sys.exit(1)
    finally:
        if processor and processor.spark:
            processor.spark.stop()
            logger.info("Spark会话已关闭")


if __name__ == "__main__":
    main()
