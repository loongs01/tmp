#!/bin/bash

# 快速检查关键JAR包
echo "=== 快速JAR包检查 ==="

# 检查Spark源目录
echo "检查Spark源JAR包..."
if [ -f "/opt/datasophon/spark-3.1.3/jars/spark-core_2.12-3.1.3.jar" ]; then
    echo "  ✓ spark-core JAR存在"
else
    echo "  ✗ spark-core JAR缺失"
fi

if [ -f "/opt/datasophon/spark-3.1.3/jars/spark-sql_2.12-3.1.3.jar" ]; then
    echo "  ✓ spark-sql JAR存在"
else
    echo "  ✗ spark-sql JAR缺失"
fi

if [ -f "/opt/datasophon/spark-3.1.3/jars/spark-hive_2.12-3.1.3.jar" ]; then
    echo "  ✓ spark-hive JAR存在"
else
    echo "  ✗ spark-hive JAR缺失"
fi

# 检查Hive节点
for host in liuf1 liuf2 liuf3; do
    echo "检查节点 $host..."
    if ssh $host "ls /opt/datasophon/hive-3.1.0/lib/spark-core_*.jar 2>/dev/null" | grep -q .; then
        echo "  ✓ $host: spark-core JAR存在"
    else
        echo "  ✗ $host: spark-core JAR缺失"
    fi
    
    if ssh $host "ls /opt/datasophon/hive-3.1.0/lib/spark-sql_*.jar 2>/dev/null" | grep -q .; then
        echo "  ✓ $host: spark-sql JAR存在"
    else
        echo "  ✗ $host: spark-sql JAR缺失"
    fi
    
    if ssh $host "ls /opt/datasophon/hive-3.1.0/lib/spark-hive_*.jar 2>/dev/null" | grep -q .; then
        echo "  ✓ $host: spark-hive JAR存在"
    else
        echo "  ✗ $host: spark-hive JAR缺失"
    fi
done

# 检查HDFS
echo "检查HDFS上的JAR包..."
if hdfs dfs -test -d /spark-jars 2>/dev/null; then
    jar_count=$(hdfs dfs -ls /spark-jars/*.jar 2>/dev/null | wc -l)
    echo "  ✓ HDFS上有 $jar_count 个JAR包"
else
    echo "  ✗ HDFS目录 /spark-jars 不存在"
fi

echo "检查完成！" 