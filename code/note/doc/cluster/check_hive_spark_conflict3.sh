#!/bin/bash

# 用法:
#   检查冲突: ./check_hive_spark_conflict.sh /path/to/hive/lib /path/to/spark/jars
#   删除冲突: ./check_hive_spark_conflict.sh /path/to/hive/lib /path/to/spark/jars --delete
#   强制删除: ./check_hive_spark_conflict.sh /path/to/hive/lib /path/to/spark/jars --delete --force

HIVE_LIB_DIR=$1
SPARK_JARS_DIR=$2
DELETE_MODE=false
FORCE_MODE=false

# 处理参数
for arg in "$@"; do
  if [[ "$arg" == "--delete" ]]; then
    DELETE_MODE=true
  elif [[ "$arg" == "--force" ]]; then
    FORCE_MODE=true
  fi
done

if [[ ! -d "$HIVE_LIB_DIR" || ! -d "$SPARK_JARS_DIR" ]]; then
  echo "❌ 用法错误：请提供 Hive lib 路径 和 Spark jars 路径"
  echo "示例：./check_hive_spark_conflict.sh /opt/hive-3.1.0/lib /opt/spark-3.1.3-bin-hadoop3.3.3/jars [--delete] [--force]"
  exit 1
fi

echo "🔍 正在检查 Hive (${HIVE_LIB_DIR}) 与 Spark (${SPARK_JARS_DIR}) 的 JAR 冲突..."
echo "-----------------------------------------------------------------------"

# 白名单（可忽略的 Hive 模块）
WHITELIST=("hive-vector-code-gen" "hive-llap-common")

is_whitelisted() {
  local name="$1"
  for white in "${WHITELIST[@]}"; do
    if [[ "$name" == "$white" ]]; then
      return 0
    fi
  done
  return 1
}

# 解析 Hive JAR 的 artifact 和 version
declare -A hive_jars
for jar in "$HIVE_LIB_DIR"/hive-*.jar; do
  base=$(basename "$jar")
  artifact=$(echo "$base" | sed -E 's/(hive-[a-zA-Z0-9\-]+)-[0-9].*\.jar/\1/')
  version=$(echo "$base" | sed -E 's/.*-([0-9][0-9a-zA-Z.\-]*)\.jar/\1/')
  hive_jars["$artifact"]=$version
done

conflict_count=0

# 遍历 Spark jars 并检查冲突
for jar in "$SPARK_JARS_DIR"/hive-*.jar; do
  [[ ! -f "$jar" ]] && continue
  base=$(basename "$jar")
  artifact=$(echo "$base" | sed -E 's/(hive-[a-zA-Z0-9\-]+)-[0-9].*\.jar/\1/')
  version=$(echo "$base" | sed -E 's/.*-([0-9][0-9a-zA-Z.\-]*)\.jar/\1/')
  hive_version=${hive_jars["$artifact"]}

  if [[ -n "$hive_version" && "$hive_version" != "$version" ]]; then
    if is_whitelisted "$artifact"; then
      echo "⚪ 忽略（白名单）: $artifact"
      continue
    fi

    echo "⚠️ 冲突: $artifact"
    echo "    Hive : $artifact-$hive_version.jar"
    echo "    Spark: $artifact-$version.jar"

    ((conflict_count++))

    if $DELETE_MODE; then
      if ! $FORCE_MODE; then
        read -p "❓ 是否删除 Spark 中的 $base? [y/N]: " confirm
        [[ "$confirm" != "y" && "$confirm" != "Y" ]] && continue
      fi

      echo "🗑️ 删除: $jar"
      rm -f "$jar"
    else
      echo "💡 建议删除 Spark 中的: $base"
    fi

    echo ""
  fi
done

if [[ $conflict_count -eq 0 ]]; then
  echo "✅ 未发现 Hive 与 Spark 的冲突 JAR"
else
  echo "🚨 总共发现 $conflict_count 个冲突 JAR"
  if $DELETE_MODE; then
    echo "🧹 删除已完成。请重新启动 HiveServer2 或 Spark 以生效。"
  fi
fi
