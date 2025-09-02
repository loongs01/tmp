#!/bin/bash
#
# Spark 3.4.3 编译脚本（适配 Hadoop 3.3.3 + Hive 3.1.0）
# 作者：leeloongs
#

# ===== 配置区 =====
SPARK_SRC_DIR="/tmp/lcz/package/spark-3.4.3"     # Spark 源码目录
MAVEN_BIN="/opt/maven/bin/mvn"                   # Maven 可执行文件路径
MAVEN_REPO="/opt/maven_repo"                     # Maven 本地仓库路径
SETTINGS_XML="/opt/maven/conf/settings.xml"      # Maven settings.xml 路径
HADOOP_VERSION="3.3.3"                           # Hadoop 版本
DIST_NAME="hadoop${HADOOP_VERSION}"              # 编译产物名称
TGZ_OUTPUT=true                                  # 是否打包成 tgz

# ===== 环境检测 =====
echo "=== [1/5] 检查环境 ==="
[ ! -d "$SPARK_SRC_DIR" ] && echo "❌ Spark 源码目录不存在: $SPARK_SRC_DIR" && exit 1
[ ! -x "$MAVEN_BIN" ] && echo "❌ Maven 不存在或不可执行: $MAVEN_BIN" && exit 1
[ ! -f "$SETTINGS_XML" ] && echo "❌ settings.xml 不存在: $SETTINGS_XML" && exit 1

JAVA_VER=$($JAVA_HOME/bin/java -version 2>&1 | head -n 1)
echo "✅ Java 版本: $JAVA_VER"
$JAVA_HOME/bin/java -version 2>&1 | grep -q 'version "1\.8\|11\|17' || {
    echo "❌ Spark 3.4.3 要求 JDK 8 / 11 / 17，请更换 JDK"; exit 1;
}

MAVEN_VER=$($MAVEN_BIN -v | head -n 1)
echo "✅ Maven 版本: $MAVEN_VER"

# ===== 清理旧构建 =====
echo "=== [2/5] 清理旧构建 ==="
cd "$SPARK_SRC_DIR" || exit 1
./dev/make-distribution.sh --clean
rm -rf "$MAVEN_REPO/org/apache/spark"

# ===== 设置 Maven 编译参数 =====
echo "=== [3/5] 设置编译参数 ==="
export MAVEN_OPTS="-Xmx4g -XX:ReservedCodeCacheSize=1g -Dfile.encoding=UTF-8"

# ===== 编译 Spark =====
echo "=== [4/5] 开始编译 Spark 3.4.3（Hadoop $HADOOP_VERSION + Hive 支持）==="
CMD="./dev/make-distribution.sh \
  --name $DIST_NAME \
  $( [ "$TGZ_OUTPUT" = true ] && echo '--tgz' ) \
  --mvn $MAVEN_BIN \
  -Phive \
  -Pyarn \
  -Phadoop-3 \
  -DskipTests \
  -Dhadoop.version=$HADOOP_VERSION \
  -Dmaven.repo.local=$MAVEN_REPO \
  -s $SETTINGS_XML"

echo "执行命令: $CMD"
eval $CMD
BUILD_STATUS=$?

# ===== 检查结果 =====
echo "=== [5/5] 检查结果 ==="
if [ $BUILD_STATUS -eq 0 ]; then
    echo "🎉 Spark 3.4.3 编译成功！"
    if [ "$TGZ_OUTPUT" = true ]; then
        echo "📦 tgz 包路径: $(find $SPARK_SRC_DIR -maxdepth 1 -name "spark-*.tgz")"
    fi
else
    echo "❌ 编译失败，请查看日志并可用以下命令继续编译未完成部分："
    echo "  cd $SPARK_SRC_DIR && $MAVEN_BIN -rf :spark-hive_2.12"
    exit 1
fi
