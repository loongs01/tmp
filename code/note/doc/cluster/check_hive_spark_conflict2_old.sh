#!/bin/bash

# =======================================
# Hive vs Spark 冲突 JAR 检测 + 清理工具
# 可选真实删除或 dry-run
# Last Modified: 2025-08-07
# =======================================

# ========== 参数默认 ==========
DELETE_CONFLICTS=false
HIVE_LIB_DIR=""
SPARK_JARS_DIR=""
WHITELIST=(
  "hive-vector-code-gen"
  "hive-llap-common"
)

# ========== 函数定义 ==========
print_help() {
  echo "用法: $0 --hive /path/to/hive/lib --spark /path/to/spark/jars [--delete]"
  echo ""
  echo "参数说明："
  echo "  --hive    Hive lib 目录（包含 hive-*.jar）"
  echo "  --spark   Spark jars 目录"
  echo "  --delete  启用删除冲突 JAR，默认 dry-run"
  echo "  --help    显示帮助信息"
}

log() {
  echo "$@" | tee -a "$LOG_FILE"
}

is_whitelisted() {
  local jar_name="$1"
  for white in "${WHITELIST[@]}"; do
    if [[ "$jar_name" == *"$white"* ]]; then
      return 0
    fi
  done
  return 1
}

# ========== 参数解析 ==========
while [[ $# -gt 0 ]]; do
  case "$1" in
    --hive)
      HIVE_LIB_DIR="$2"
      shift 2
      ;;
    --spark)
      SPARK_JARS_DIR="$2"
      shift 2
      ;;
    --delete)
      DELETE_CONFLICTS=true
      shift
      ;;
    --help|-h)
      print_help
      exit 0
      ;;
    *)
      echo "未知参数: $1"
      print_help
      exit 1
      ;;
  esac
done

# ========== 校验参数 ==========
if [[ ! -d "$HIVE_LIB_DIR" ]]; then
  echo "[错误] Hive lib 路径无效：$HIVE_LIB_DIR"
  exit 1
fi

if [[ ! -d "$SPARK_JARS_DIR" ]]; then
  echo "[错误] Spark jars 路径无效：$SPARK_JARS_DIR"
  exit 1
fi

# ========== 启动执行 ==========
TIMESTAMP=$(date +%Y%m%d%H%M%S)
LOG_FILE="./hive_spark_conflicts_${TIMESTAMP}.log"
conflicts_found=0

log "===== Hive & Spark 冲突检测开始 ====="
log "Hive lib 路径: $HIVE_LIB_DIR"
log "Spark jars 路径: $SPARK_JARS_DIR"
log "是否执行删除: $DELETE_CONFLICTS"
log "白名单模块: ${WHITELIST[*]}"
log "日志文件: $LOG_FILE"
log "====================================="

# 遍历 Hive 中所有 hive-*.jar
for hive_jar in "$HIVE_LIB_DIR"/hive-*.jar; do
  base_name=$(basename "$hive_jar")
  spark_jar="$SPARK_JARS_DIR/$base_name"

  if [[ -f "$spark_jar" ]]; then
    if is_whitelisted "$base_name"; then
      log "[跳过白名单] $base_name"
    else
      log "[冲突] $base_name 存在于 Spark jars 中"
      if [[ "$DELETE_CONFLICTS" == true ]]; then
        rm -f "$spark_jar"
        log "  => 已删除 $spark_jar"
      else
        log "  => dry-run（未删除）"
      fi
      ((conflicts_found++))
    fi
  fi
done

log "====================================="
log "冲突总数: $conflicts_found"
log "执行完成 ✅"
log "====================================="

exit 0
