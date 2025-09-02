./build/mvn -DskipTests clean package \
  -Phadoop-3.3 \
  -Dhadoop.version=3.3.3 \
  -Pprovided \
  -Phive \
  -Dmaven.repo.local=/opt/maven_repo \
  -s /opt/maven/conf/settings.xml



/opt/maven/bin/mvn -DskipTests clean package \
  -Phadoop-3.3 \
  -Dhadoop.version=3.3.3 \
  -Pprovided \
  -Phive \
  -Dmaven.repo.local=/opt/maven_repo \
  -s /opt/maven/conf/settings.xml



./dev/make-distribution.sh \
  --name hadoop3.3.3 \
  --tgz \
  --mvn /opt/maven/bin/mvn \
  --scala 2.12 \
  --hadoop 3.3.3 \
  -Phive \
  -Pkubernetes \
  -Pspark-ganglia-lgpl \
  -DskipTests



✅ 建议使用的优化后编译命令：
bash
复制
编辑
./build/mvn -DskipTests clean package \
  -Dhadoop.version=3.3.3 \
  -Phive \
  -Pkubernetes \
  -Pspark-ganglia-lgpl
解释：

-Dhadoop.version=3.3.3：通过变量方式指定 Hadoop 版本，不依赖 profile。

-Phive：包含 Hive 相关依赖，支持 Hive on Spark。

-Pkubernetes 和 -Pspark-ganglia-lgpl：可选，保留用于调试和资源监控。

不再使用 -Phadoop-3.3 和 -Pprovided，因为这两个在 Spark 3.2.4 的 pom.xml 中本身不存在。



# execute
# 1
./build/mvn -DskipTests clean package \
  -Phadoop-3.3 \
  -Dhadoop.version=3.3.3 \
  -Pprovided \
  -Phive \
  -Dmaven.repo.local=/opt/maven_repo \
  -s /opt/maven/conf/settings.xml


./dev/make-distribution.sh \
  --name hadoop3.3.3 \
  --tgz \
  --mvn /opt/maven/bin/mvn \
  -Phive \
  -Pyarn \
  -Phadoop-3.3 \
  -DskipTests \
  -Dhadoop.version=3.3.3 \
  -Dmaven.repo.local=/opt/maven_repo \
  -s /opt/maven/conf/settings.xml
  
 # 编译spark-3.4.3.tgz 
./dev/make-distribution.sh \
  --name hadoop3.3.3-hive3.1.0 \
  --tgz \
  --mvn /opt/maven/bin/mvn \
  -Phive \
  -Phive-thriftserver \
  -Pyarn \
  -Phadoop-3.3 \
  -DskipTests \
  -Dhadoop.version=3.3.3 \
  -Dhive.version=3.1.0 \
  -Dmaven.repo.local=/opt/maven_repo \
  -s /opt/maven/conf/settings.xml

./dev/make-distribution.sh \
  --name "hadoop3.3.3-hive3.1.0" \
  --tgz \
  --mvn /opt/maven/bin/mvn \
  -s /opt/maven/conf/settings.xml \
  -Dmaven.repo.local=/opt/maven_repo \
  -Phadoop-3 \
  -Pyarn \
  -Phive \
  -Phive-thriftserver \
  -Dhadoop.version=3.3.3 \
  -Dhive.version=3.1.0 \
  -T 1C \
  -DskipTests \
  -Drat.skip=true \
  -Dcheckstyle.skip=true





export SPARK_HOME=/opt/spark-3.1.3-bin-hadoop3.3.3
export PATH=$SPARK_HOME/bin:$PATH
./spark-shell --version

# 查看SPARK_HOME
grep -R "export SPARK_HOME=/opt/datasophon/spark-3.1.3" /opt/datasophon 2>/dev/null
grep -R "SPARK_HOME" /opt/datasophon 2>/dev/null


ls jars | grep hive
ls jars | grep yarn


scp /opt/datasophon/spark-3.1.3/jars/*.jar liuf1:/opt/datasophon/hive-3.1.0/lib

# error
# scp liuf1:/opt/datasophon/hive-3.1.0/lib/*.jar liuf4:/opt/jar_test

scp liuf1:/opt/datasophon/hive-3.1.0/lib/*.jar /opt/jar_test/


./check_hive_spark_conflict3.sh /tmp/lcz/test/apache-hive-3.1.0-bin/lib /tmp/lcz/spark-3.1.3/spark-3.1.3-bin-hadoop3.3.3/jars

./check_hive_spark_conflict_final.sh /opt/jar_test/ /opt/spark-3.1.3-bin-hadoop3.3.3/jars --sync --force

./check_hive_spark_conflict.sh /opt/jar_test/ /tmp/lcz/spark-2.4.8-bin-hadoop2.7/jars




rm -rf /opt/maven_repo/org/apache/hive /opt/maven_repo/org/spark-project/hive


cat pom.xml |grep -i hive.version

ps aux | grep ranger

1. 删除旧的构建产物：
Spark 使用的是 Maven 构建系统，建议删除下面这些目录：

cd /tmp/lcz/spark-3.1.3

# 删除分发包目录（非常重要）
rm -rf assembly/target

# 删除 Maven 编译输出（可选但推荐）
# rm -rf ~/.m2/repository/org/apache/spark

# 如果你有指定 Maven 本地仓库路径（如 /opt/maven_repo），也清理：
rm -rf /opt/maven_repo/org/apache/spark
2. 确认没有旧 .tgz 包存在：
rm -f spark-3.1.3-bin-hadoop3.3.3.tgz


mvn clean package -DskipTests -Pdist -Dhadoop.version=3.3.1


mvn clean package -DskipTests -Dmaven.javadoc.skip=true \
  -pl jdbc,metastore,beeline,serde,common,service,shims,ql \
  -am
  
mvn clean package -DskipTests -Dmaven.javadoc.skip=true \
  -Pdist \
  -pl packaging,jdbc,metastore,beeline,serde,common,service,shims,ql \
  -am -pl '!upgrade-acid'


