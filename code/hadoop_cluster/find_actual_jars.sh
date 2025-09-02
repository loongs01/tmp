#!/bin/bash

# 查找实际的JAR包
echo "=== 查找实际的JAR包 ==="

SPARK_HOME="/opt/datasophon/spark-3.1.3"
HIVE_HOME="/opt/datasophon/hive-3.1.0"

echo "1. 查找Spark目录中的实际JAR包:"
echo "=================================="

echo "Spark核心相关JAR包:"
ls $SPARK_HOME/jars/ | grep -E "(spark-core|spark-sql|spark-hive)" | head -10

echo ""
echo "Hadoop相关JAR包:"
ls $SPARK_HOME/jars/ | grep -E "(hadoop|yarn)" | head -10

echo ""
echo "序列化相关JAR包:"
ls $SPARK_HOME/jars/ | grep -E "(kryo|minlog|reflectasm|objenesis)" | head -10

echo ""
echo "2. 查找Hive目录中的实际JAR包:"
echo "=================================="

echo "Hadoop相关JAR包:"
ls $HIVE_HOME/lib/ | grep -E "(hadoop|yarn)" | head -10

echo ""
echo "3. 查找Hadoop安装目录中的JAR包:"
echo "=================================="

HADOOP_HOME="/opt/datasophon/hadoop-3.3.3"
echo "Hadoop Client JAR包:"
ls $HADOOP_HOME/share/hadoop/common/ | grep -E "(hadoop-client)" | head -5

echo ""
echo "YARN相关JAR包:"
ls $HADOOP_HOME/share/hadoop/yarn/ | grep -E "(yarn)" | head -5

echo ""
echo "4. 查找所有Spark JAR包:"
echo "=================================="
echo "Spark JAR包总数: $(ls $SPARK_HOME/jars/*.jar | wc -l)"
echo "前20个Spark JAR包:"
ls $SPARK_HOME/jars/*.jar | head -20

echo ""
echo "5. 查找所有Hive JAR包:"
echo "=================================="
echo "Hive JAR包总数: $(ls $HIVE_HOME/lib/*.jar | wc -l)"
echo "前20个Hive JAR包:"
ls $HIVE_HOME/lib/*.jar | head -20

echo ""
echo "6. 查找SLF4J相关JAR包:"
echo "=================================="
echo "Spark目录中的SLF4J JAR包:"
ls $SPARK_HOME/jars/ | grep -i slf4j

echo ""
echo "Hive目录中的SLF4J JAR包:"
ls $HIVE_HOME/lib/ | grep -i slf4j

echo ""
echo "Hadoop目录中的SLF4J JAR包:"
ls $HADOOP_HOME/share/hadoop/common/lib/ | grep -i slf4j 