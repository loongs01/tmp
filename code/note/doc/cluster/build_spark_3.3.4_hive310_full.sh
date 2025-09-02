#!/bin/bash
set -e

# ===== 路径配置 =====
SPARK_SRC="/tmp/lcz/package/spark-3.3.4"
MVN_BIN="/opt/maven/bin/mvn"
MVN_LOCAL="/opt/maven_repo"
MVN_SETTINGS="/opt/maven/conf/settings.xml"
HADOOP_VER="3.3.3"
HIVE_VER="3.1.0"

echo "==== Step 1: 清理 POM 和 XML 文件的 BOM 头 ===="
find "$SPARK_SRC" -type f \( -name "*.xml" -o -name "pom.xml" \) \
  -exec sed -i '1s/^\xEF\xBB\xBF//' {} \;

echo "==== Step 2: 修改 Spark POM 固定 Hive 版本 ===="
# 在主 pom.xml 中加 hive.version 属性
grep -q "<hive.version>" "$SPARK_SRC/pom.xml" || \
  sed -i "/<properties>/a\    <hive.version>$HIVE_VER</hive.version>" "$SPARK_SRC/pom.xml"

# 确保所有模块的 hive.version 一致
find "$SPARK_SRC" -name "pom.xml" -exec \
  sed -i "s/<hive.version>.*<\/hive.version>/<hive.version>$HIVE_VER<\/hive.version>/g" {} \;

echo "==== Step 3: 添加 Hive JDBC / ORC / Parquet 编译依赖 ===="
PATCH_POM="$SPARK_SRC/sql/hive/pom.xml"
if ! grep -q "hive-jdbc" "$PATCH_POM"; then
  sed -i "/<\/dependencies>/i \
    <!-- Hive JDBC -->\n\
    <dependency>\n\
      <groupId>org.apache.hive</groupId>\n\
      <artifactId>hive-jdbc</artifactId>\n\
      <version>\${hive.version}</version>\n\
    </dependency>\n\
    <!-- Hive ORC -->\n\
    <dependency>\n\
      <groupId>org.apache.orc</groupId>\n\
      <artifactId>orc-core</artifactId>\n\
      <version>1.5.12</version>\n\
    </dependency>\n\
    <dependency>\n\
      <groupId>org.apache.orc</groupId>\n\
      <artifactId>orc-mapreduce</artifactId>\n\
      <version>1.5.12</version>\n\
    </dependency>\n\
    <!-- Hive Parquet -->\n\
    <dependency>\n\
      <groupId>org.apache.parquet</groupId>\n\
      <artifactId>parquet-hadoop</artifactId>\n\
      <version>1.12.0</version>\n\
    </dependency>\n\
    <dependency>\n\
      <groupId>org.apache.parquet</groupId>\n\
      <artifactId>parquet-column</artifactId>\n\
      <version>1.12.0</version>\n\
    </dependency>" "$PATCH_POM"
fi

echo "==== Step 4: 开始编译 Spark $SPARK_SRC ===="
cd "$SPARK_SRC"

./dev/make-distribution.sh \
  --name hadoop$HADOOP_VER-hive$HIVE_VER \
  --tgz \
  --mvn "$MVN_BIN" \
  -Phive \
  -Phive-thriftserver \
  -Pyarn \
  -Phadoop-$HADOOP_VER \
  -DskipTests \
  -Dhadoop.version=$HADOOP_VER \
  -Dhive.version=$HIVE_VER \
  -Dmaven.repo.local="$MVN_LOCAL" \
  -s "$MVN_SETTINGS"

echo "==== Step 5: 完成 ===="
echo "最终包位置: $SPARK_SRC/spark-*.tgz"
echo "解压后可直接使用 Hive on Spark，无需额外替换 JAR"
