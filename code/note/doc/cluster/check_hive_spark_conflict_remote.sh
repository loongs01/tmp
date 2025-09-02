#!/bin/bash

# liuf1 上 Hive 的路径
HIVE_HOST="liuf1"
HIVE_PATH="/opt/hive-3.1.0/lib"

# 本地 Spark jars 路径
SPARK_PATH="/opt/spark-3.1.3-bin-hadoop3.3.3/jars"

# 拉取 hive jar 文件名（不含路径）
echo "🔍 正在获取 Hive JAR 列表（来自 $HIVE_HOST:$HIVE_PATH）..."
ssh $HIVE_HOST "ls $HIVE_PATH | grep '^hive-.*\.jar$'" > /tmp/hive_jars.txt

if [ $? -ne 0 ]; then
  echo "❌ 无法连接 $HIVE_HOST 或路径 $HIVE_PATH 错误"
  exit 1
fi

# 获取本地 spark jar 文件名
echo "🔍 获取本地 Spark JAR 列表..."
ls "$SPARK_PATH" | grep '^hive-.*\.jar$' > /tmp/spark_jars.txt

# 统一排序
sort /tmp/hive_jars.txt > /tmp/hive_sorted.txt
sort /tmp/spark_jars.txt > /tmp/spark_sorted.txt

# 找出 jar 名相同但版本可能不同的冲突
echo -e "\n⚠️ 可能存在冲突的 Hive JAR（按 jar 名匹配）:"
comm -12 <(cut -d '-' -f1-2 /tmp/hive_sorted.txt) <(cut -d '-' -f1-2 /tmp/spark_sorted.txt) | while read prefix; do
  HIVE_JAR=$(grep "^$prefix" /tmp/hive_sorted.txt)
  SPARK_JAR=$(grep "^$prefix" /tmp/spark_sorted.txt)
  if [ "$HIVE_JAR" != "$SPARK_JAR" ]; then
    echo "🔸 $prefix:"
    echo "    Hive : $HIVE_JAR"
    echo "    Spark: $SPARK_JAR"
  fi
done

echo -e "\n✅ 检测完成。如果没有输出冲突，说明 Spark 与 Hive JAR 暂无直接冲突。"
