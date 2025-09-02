#!/bin/bash

# 用法: ./check_hive_spark_conflict.sh /path/to/hive/lib /path/to/spark/jars

HIVE_LIB_DIR=$1
SPARK_JARS_DIR=$2

if [[ ! -d "$HIVE_LIB_DIR" || ! -d "$SPARK_JARS_DIR" ]]; then
  echo "❌ 用法错误：请提供 Hive lib 路径 和 Spark jars 路径"
  echo "示例：./check_hive_spark_conflict.sh /opt/hive-3.1.0/lib /opt/spark-3.1.3-bin-hadoop3.3.3/jars"
  exit 1
fi

echo "🔍 检查 Hive (${HIVE_LIB_DIR}) 和 Spark (${SPARK_JARS_DIR}) 的 JAR 冲突..."
echo "-----------------------------------------------------------------------"

# 解析 hive jar 的 artifact 和 version
declare -A hive_jars
for jar in "$HIVE_LIB_DIR"/hive-*.jar; do
  basename=$(basename "$jar")
  artifact=$(echo "$basename" | sed -E 's/(hive-[a-zA-Z0-9\-]+)-([0-9.]+.*)\.jar/\1/')
  version=$(echo "$basename" | sed -E 's/.*-([0-9.]+.*)\.jar/\1/')
  hive_jars["$artifact"]=$version
done

conflict_found=false

# 遍历 spark jars
for jar in "$SPARK_JARS_DIR"/hive-*.jar; do
  basename=$(basename "$jar")
  artifact=$(echo "$basename" | sed -E 's/(hive-[a-zA-Z0-9\-]+)-([0-9.]+.*)\.jar/\1/')
  version=$(echo "$basename" | sed -E 's/.*-([0-9.]+.*)\.jar/\1/')

  hive_version=${hive_jars["$artifact"]}

  if [[ -n "$hive_version" && "$hive_version" != "$version" ]]; then
    echo "⚠️ 冲突: $artifact"
    echo "    Hive:  $artifact-$hive_version.jar"
    echo "    Spark: $artifact-$version.jar"
    conflict_found=true
  fi
done

if [ "$conflict_found" = false ]; then
  echo "✅ 未发现 Hive 与 Spark 之间的 JAR 冲突。"
fi
