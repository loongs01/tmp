#!/bin/bash

# 用法：
#   检查冲突       : ./check_hive_spark_conflict.sh <hive-lib> <spark-jars>
#   删除 Spark 冲突: ./check_hive_spark_conflict.sh <hive-lib> <spark-jars> --delete
#   覆盖 Spark JAR : ./check_hive_spark_conflict.sh <hive-lib> <spark-jars> --sync
#   强制操作        : 添加 --force 跳过确认

HIVE_LIB_DIR=$1
SPARK_JARS_DIR=$2
DELETE_MODE=false
SYNC_MODE=false
FORCE_MODE=false

# 处理参数
for arg in "$@"; do
  case $arg in
    --delete) DELETE_MODE=true ;;
    --sync) SYNC_MODE=true ;;
    --force) FORCE_MODE=true ;;
  esac
done

if [[ ! -d "$HIVE_LIB_DIR" || ! -d "$SPARK_JARS_DIR" ]]; then
  echo "❌ 用法错误：请提供 Hive lib 路径 和 Spark jars 路径"
  echo "示例：./check_hive_spark_conflict.sh /opt/hive/lib /opt/spark/jars [--delete] [--sync] [--force]"
  exit 1
fi

echo "🔍 正在检查 Hive (${HIVE_LIB_DIR}) 与 Spark (${SPARK_JARS_DIR}) 的 JAR 冲突..."
echo "-----------------------------------------------------------------------"

# 可忽略的 Hive 模块白名单
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

# 解析 Hive JAR 的 artifact -> version & path
declare -A hive_jars
declare -A hive_paths

for jar in "$HIVE_LIB_DIR"/hive-*.jar; do
  base=$(basename "$jar")
  artifact=$(echo "$base" | sed -E 's/(hive-[a-zA-Z0-9\-]+)-[0-9].*\.jar/\1/')
  version=$(echo "$base" | sed -E 's/.*-([0-9][0-9a-zA-Z.\-]*)\.jar/\1/')
  hive_jars["$artifact"]=$version
  hive_paths["$artifact"]=$jar
done

conflict_count=0

for jar in "$SPARK_JARS_DIR"/hive-*.jar; do
  [[ ! -f "$jar" ]] && continue
  base=$(basename "$jar")
  artifact=$(echo "$base" | sed -E 's/(hive-[a-zA-Z0-9\-]+)-[0-9].*\.jar/\1/')
  version=$(echo "$base" | sed -E 's/.*-([0-9][0-9a-zA-Z.\-]*)\.jar/\1/')
  hive_version=${hive_jars["$artifact"]}
  hive_jar=${hive_paths["$artifact"]}

  if [[ -n "$hive_version" && "$hive_version" != "$version" ]]; then
    if is_whitelisted "$artifact"; then
      echo "⚪ 忽略（白名单）: $artifact"
      continue
    fi

    echo "⚠️ 冲突: $artifact"
    echo "    Hive : $artifact-$hive_version.jar"
    echo "    Spark: $artifact-$version.jar"
    ((conflict_count++))

    # 删除模式
    if $DELETE_MODE; then
      if ! $FORCE_MODE; then
        read -p "❓ 删除 Spark 中的 $base? [y/N]: " confirm
        [[ "$confirm" != "y" && "$confirm" != "Y" ]] && continue
      fi
      echo "🗑️ 删除: $jar"
      rm -f "$jar"
    fi

    # 同步模式
    if $SYNC_MODE; then
      if [[ ! -f "$hive_jar" ]]; then
        echo "❌ Hive 中找不到 $artifact 的 JAR，跳过同步"
        continue
      fi
      if ! $FORCE_MODE; then
        read -p "📦 覆盖 Spark 中的 $artifact JAR 为 Hive 版本? [y/N]: " confirm
        [[ "$confirm" != "y" && "$confirm" != "Y" ]] && continue
      fi
      echo "📁 复制: $hive_jar -> $SPARK_JARS_DIR/"
      cp -f "$hive_jar" "$SPARK_JARS_DIR/"
    fi

    echo ""
  fi
done

# 总结
if [[ $conflict_count -eq 0 ]]; then
  echo "✅ 未发现 Hive 与 Spark 的冲突 JAR"
else
  echo "🚨 共发现 $conflict_count 个冲突 JAR"
  if $DELETE_MODE; then
    echo "🧹 冲突 JAR 已删除"
  fi
  if $SYNC_MODE; then
    echo "🔁 Hive JAR 已复制到 Spark"
  fi
  echo "📌 请重启 HiveServer2 或 Spark 以确保变更生效"
fi
