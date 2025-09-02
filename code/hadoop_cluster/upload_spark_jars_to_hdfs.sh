#!/bin/bash

# 上传Spark JAR包到HDFS
echo "=== 上传Spark JAR包到HDFS ==="

SPARK_HOME="/opt/datasophon/spark-3.1.3"

# 检查HDFS状态
echo "检查HDFS状态..."
hdfs dfsadmin -report | head -10

# 创建HDFS目录
echo "创建HDFS目录..."
hdfs dfs -mkdir -p /spark-jars

# 上传Spark JAR包
echo "上传Spark JAR包到HDFS..."
hdfs dfs -put $SPARK_HOME/jars/* /spark-jars/

# 验证上传结果
echo "验证上传结果..."
jar_count=$(hdfs dfs -ls /spark-jars/*.jar 2>/dev/null | wc -l)
echo "HDFS上的JAR包数量: $jar_count"

# 显示部分JAR包
echo "HDFS上的部分JAR包:"
hdfs dfs -ls /spark-jars/ | head -10

echo "Spark JAR包上传完成！" 