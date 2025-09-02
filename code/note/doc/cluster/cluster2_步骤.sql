
# 查找实际的JAR包文件
echo "Spark核心JAR包:"
ls $SPARK_HOME/jars/spark-core_*.jar 2>/dev/null || echo "  未找到 spark-core_*.jar"
ls $SPARK_HOME/jars/spark-sql_*.jar 2>/dev/null || echo "  未找到 spark-sql_*.jar"
ls $SPARK_HOME/jars/spark-hive_*.jar 2>/dev/null || echo "  未找到 spark-hive_*.jar"
ls $SPARK_HOME/jars/spark-network-common_*.jar 2>/dev/null || echo "  未找到 spark-network-common_*.jar"
ls $SPARK_HOME/jars/spark-unsafe_*.jar 2>/dev/null || echo "  未找到 spark-unsafe_*.jar"
ls $SPARK_HOME/jars/spark-launcher_*.jar 2>/dev/null || echo "  未找到 spark-launcher_*.jar"
ls $SPARK_HOME/jars/spark-catalyst_*.jar 2>/dev/null || echo "  未找到 spark-catalyst_*.jar"

echo ""
echo "Hadoop相关JAR包:"
ls $SPARK_HOME/jars/hadoop-client_*.jar 2>/dev/null || echo "  未找到 hadoop-client_*.jar"
ls $SPARK_HOME/jars/hadoop-yarn-api_*.jar 2>/dev/null || echo "  未找到 hadoop-yarn-api_*.jar"
ls $SPARK_HOME/jars/hadoop-yarn-client_*.jar 2>/dev/null || echo "  未找到 hadoop-yarn-client_*.jar"
ls $SPARK_HOME/jars/hadoop-yarn-common_*.jar 2>/dev/null || echo "  未找到 hadoop-yarn-common_*.jar"

echo ""
echo "序列化相关JAR包:"
ls $SPARK_HOME/jars/kryo_*.jar 2>/dev/null || echo "  未找到 kryo_*.jar"
ls $SPARK_HOME/jars/minlog_*.jar 2>/dev/null || echo "  未找到 minlog_*.jar"
ls $SPARK_HOME/jars/reflectasm_*.jar 2>/dev/null || echo "  未找到 reflectasm_*.jar"
ls $SPARK_HOME/jars/objenesis_*.jar 2>/dev/null || echo "  未找到 objenesis_*.jar"

echo ""
echo "=== 复制命令示例 ==="
echo "# 复制Spark核心JAR包"
echo "cp \$SPARK_HOME/jars/spark-core_*.jar \$HIVE_HOME/lib/"
echo "cp \$SPARK_HOME/jars/spark-sql_*.jar \$HIVE_HOME/lib/"
echo "cp \$SPARK_HOME/jars/spark-hive_*.jar \$HIVE_HOME/lib/"
echo "cp \$SPARK_HOME/jars/spark-network-common_*.jar \$HIVE_HOME/lib/"
echo "cp \$SPARK_HOME/jars/spark-unsafe_*.jar \$HIVE_HOME/lib/"
echo "cp \$SPARK_HOME/jars/spark-launcher_*.jar \$HIVE_HOME/lib/"
echo "cp \$SPARK_HOME/jars/spark-catalyst_*.jar \$HIVE_HOME/lib/"



$HIVE_HOME


ls $HIVE_HOME/jars/hadoop-client_*.jar 2>/dev/null || echo "  未找到 hadoop-client_*.jar"
ls $HIVE_HOME/jars/hadoop-yarn-api_*.jar 2>/dev/null || echo "  未找到 hadoop-yarn-api_*.jar"
ls $HIVE_HOME/jars/hadoop-yarn-client_*.jar 2>/dev/null || echo "  未找到 hadoop-yarn-client_*.jar"
ls $HIVE_HOME/jars/hadoop-yarn-common_*.jar 2>/dev/null || echo "  未找到 hadoop-yarn-common_*.jar"

ls $HIVE_HOME/jars/hadoop-client_*.jar 2>/dev/null || echo "  未找到 hadoop-client_*.jar"
ls $HIVE_HOME/jars/hadoop-yarn-api_*.jar 2>/dev/null || echo "  未找到 hadoop-yarn-api_*.jar"
ls $HIVE_HOME/jars/hadoop-yarn-client_*.jar 2>/dev/null || echo "  未找到 hadoop-yarn-client_*.jar"
ls $HIVE_HOME/jars/hadoop-yarn-common_*.jar 2>/dev/null || echo "  未找到 hadoop-yarn-common_*.jar"

第一阶段：环境准备（在liuf5节点执行）
1.1 检查Spark JAR包结构

# 在liuf5节点执行
cd /opt/datasophon/spark-3.1.3

# 查看Spark JAR包总数
echo "Spark JAR包总数: $(ls jars/*.jar | wc -l)"

# 查看关键JAR包
echo "=== 关键Spark JAR包 ==="
ls jars/spark-core_*.jar
ls jars/spark-sql_*.jar
ls jars/spark-hive_*.jar

# 查看所有Spark JAR包
echo "=== 所有Spark JAR包 ==="
ls jars/ | head -20

1.2 上传Spark JAR包到HDFS

# 在liuf5节点执行
# 创建HDFS目录
hdfs dfs -mkdir -p /spark-jars
 -- sudo -u hdfs -i  hdfs dfs -mkdir -p /spark-jars
# 上传所有Spark JAR包到HDFS
hdfs dfs -put /opt/datasophon/spark-3.1.3/jars/* /spark-jars/

 -- sudo -u hdfs -i hdfs dfs -put /opt/datasophon/spark-3.1.3/jars/* /spark-jars/

# 验证上传
hdfs dfs -ls /spark-jars/ | head -10
echo "HDFS上JAR包总数: $(hdfs dfs -ls /spark-jars/*.jar | wc -l)"


第二阶段：Hive节点配置（在liuf1, liuf2, liuf3分别执行）
2.1 在liuf1节点执行（HiveMetaStore）

# 登录liuf1节点
ssh liuf1

# 备份原始lib目录
cd /opt/datasophon/hive-3.1.0
cp -r lib lib.bak.$(date +%Y%m%d_%H%M%S)

# 复制所有Spark JAR包
cp /opt/datasophon/spark-3.1.3/jars/*.jar lib/

-- 实际执行命令
-- scp /opt/datasophon/spark-3.1.3/jars/*.jar liuf1:/opt/datasophon/hive-3.1.0/lib

# 移除冲突的SLF4J绑定
rm -f lib/slf4j-log4j12-*.jar
rm -f lib/log4j-slf4j-impl-*.jar

# 复制统一的SLF4J绑定
cp /opt/datasophon/hadoop-3.3.3/share/hadoop/common/lib/slf4j-reload4j-*.jar lib/

# 验证复制结果
echo "Spark JAR包总数: $(ls lib/spark-*.jar | wc -l)"
ls lib/spark-core_*.jar
ls lib/spark-sql_*.jar
ls lib/spark-hive_*.jar



2.2 在liuf2节点执行（HiveServer2）

# 登录liuf2节点
ssh liuf2

# 备份原始lib目录
cd /opt/datasophon/hive-3.1.0
cp -r lib lib.bak.$(date +%Y%m%d_%H%M%S)

# 复制所有Spark JAR包
cp /opt/datasophon/spark-3.1.3/jars/*.jar lib/

# 移除冲突的SLF4J绑定
rm -f lib/slf4j-log4j12-*.jar
rm -f lib/log4j-slf4j-impl-*.jar

# 复制统一的SLF4J绑定
cp /opt/datasophon/hadoop-3.3.3/share/hadoop/common/lib/slf4j-reload4j-*.jar lib/

# 验证复制结果
echo "Spark JAR包总数: $(ls lib/spark-*.jar | wc -l)"
ls lib/spark-core_*.jar
ls lib/spark-sql_*.jar
ls lib/spark-hive_*.jar


2.3 在liuf3节点执行（HiveServer2）
# 登录liuf3节点
ssh liuf3

# 备份原始lib目录
cd /opt/datasophon/hive-3.1.0
cp -r lib lib.bak.$(date +%Y%m%d_%H%M%S)

# 复制所有Spark JAR包
cp /opt/datasophon/spark-3.1.3/jars/*.jar lib/

# 移除冲突的SLF4J绑定
rm -f lib/slf4j-log4j12-*.jar
rm -f lib/log4j-slf4j-impl-*.jar

# 复制统一的SLF4J绑定
cp /opt/datasophon/hadoop-3.3.3/share/hadoop/common/lib/slf4j-reload4j-*.jar lib/

# 验证复制结果
echo "Spark JAR包总数: $(ls lib/spark-*.jar | wc -l)"
ls lib/spark-core_*.jar
ls lib/spark-sql_*.jar
ls lib/spark-hive_*.jar


第三阶段：配置文件配置
3.1 在liuf1节点配置hive-site.xml


# 在liuf1节点执行
cd /opt/datasophon/hive-3.1.0/conf

# 备份原配置
cp hive-site.xml hive-site.xml.bak.$(date +%Y%m%d_%H%M%S)

# 创建新的hive-site.xml
cat > hive-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <!-- 默认执行引擎（允许动态切换） -->
    <property>
        <name>hive.execution.engine</name>
        <value>mr</value>
    </property>

    <!-- Spark相关配置 -->
    <property>
        <name>spark.master</name>
        <value>yarn</value>
    </property>

    <property>
        <name>spark.yarn.jars</name>
        <value>hdfs://nameservice1/spark-jars/*</value>
    </property>

    <property>
        <name>spark.eventLog.enabled</name>
        <value>true</value>
    </property>

    <property>
        <name>spark.eventLog.dir</name>
        <value>hdfs://nameservice1/spark-history</value>
    </property>

    <property>
        <name>spark.serializer</name>
        <value>org.apache.spark.serializer.KryoSerializer</value>
    </property>

    <property>
        <name>spark.sql.adaptive.enabled</name>
        <value>true</value>
    </property>

    <property>
        <name>spark.sql.adaptive.coalescePartitions.enabled</name>
        <value>true</value>
    </property>

    <!-- Spark资源配置 -->
    <property>
        <name>spark.executor.memory</name>
        <value>2g</value>
    </property>

    <property>
        <name>spark.executor.cores</name>
        <value>2</value>
    </property>

    <property>
        <name>spark.executor.instances</name>
        <value>2</value>
    </property>

    <property>
        <name>spark.driver.memory</name>
        <value>1g</value>
    </property>

    <property>
        <name>spark.driver.maxResultSize</name>
        <value>1g</value>
    </property>

    <!-- Hive与Spark集成配置 -->
    <property>
        <name>hive.spark.client.connect.timeout</name>
        <value>10000ms</value>
    </property>

    <property>
        <name>hive.spark.client.server.connect.timeout</name>
        <value>10000ms</value>
    </property>

    <property>
        <name>hive.spark.client.rpc.server.address</name>
        <value>liuf1:10000</value>
    </property>

    <property>
        <name>hive.spark.client.rpc.server.port</name>
        <value>10000</value>
    </property>

    <property>
        <name>hive.spark.client.secret.bits</name>
        <value>256</value>
    </property>

    <!-- 日志配置 -->
    <property>
        <name>hive.log.dir</name>
        <value>/var/log/hive</value>
    </property>

    <property>
        <name>hive.log.file</name>
        <value>hive.log</value>
    </property>
</configuration>
EOF


3.2 在liuf2和liuf3节点复制配置文件

-- add 备份
      cp hive-site.xml hive-site.xml.bak.$(date +%Y%m%d_%H%M%S)
	  
# 在liuf2节点执行
scp liuf1:/opt/datasophon/hive-3.1.0/conf/hive-site.xml /opt/datasophon/hive-3.1.0/conf/

# 在liuf3节点执行
scp liuf1:/opt/datasophon/hive-3.1.0/conf/hive-site.xml /opt/datasophon/hive-3.1.0/conf/


第四阶段：启动服务
4.1 在liuf5节点启动Spark History Server

# 在liuf5节点执行
cd /opt/datasophon/spark-3.1.3

# 启动Spark History Server
sbin/start-history-server.sh

# 验证History Server状态
curl http://liuf5:18080
echo "Spark History Server启动状态: $?"


4.2 在liuf1节点启动HiveMetaStore
# 在liuf1节点执行
cd /opt/datasophon/hive-3.1.0

# 创建日志目录
mkdir -p /var/log/hive

# 停止现有服务
pkill -f 'hive.*metastore'

# 启动HiveMetaStore
nohup bin/hive --service metastore > /var/log/hive/metastore.$(date +%Y%m%d_%H%M%S).log 2>&1 &

# 验证服务状态
sleep 5
jps -m | grep -E "(RunJar|HiveMetaStore)"
echo "HiveMetaStore启动状态: $?"
ps -ef | grep metastore

4.3 在liuf2节点启动HiveServer2

# 在liuf2节点执行
cd /opt/datasophon/hive-3.1.0

# 创建日志目录
mkdir -p /var/log/hive

# 停止现有服务
pkill -f 'hive.*hiveserver2'

# 启动HiveServer2
nohup bin/hive --service hiveserver2 > /var/log/hive/hiveserver2.$(date +%Y%m%d_%H%M%S).log 2>&1 &

# 验证服务状态
sleep 10
jps -m | grep -E "(RunJar|HiveServer)"
echo "HiveServer2启动状态: $?"
ps -ef | grep HiveServer



-- 增加
cd /opt/datasophon/hive-3.1.0/bin
./schematool -dbType mysql -info

-dbType mysql：指定你使用的元数据库类型，这里是 MySQL。

Hive 会读取你的 $HIVE_HOME/conf/hive-site.xml 中的连接信息来连接元数据库。


第五阶段：验证与测试
5.1 在liuf1节点测试连接
# 在liuf1节点执行
cd /opt/datasophon/hive-3.1.0

# 测试HiveServer2连接
bin/beeline -u jdbc:hive2://liuf2:10000 -e "SET hive.execution.engine;"

# 测试引擎切换
bin/beeline -u jdbc:hive2://liuf2:10000 -e "
SET hive.execution.engine=spark;
SET hive.execution.engine;
SET hive.execution.engine=mr;
SET hive.execution.engine;
"

5.2 在liuf1节点创建测试表
# 在liuf1节点执行
cd /opt/datasophon/hive-3.1.0

# 创建测试脚本
cat > /tmp/test_engine_switch.sql << 'EOF'
-- 创建测试表
CREATE TABLE IF NOT EXISTS test_engine_switch (
    id INT,
    name STRING,
    value DOUBLE
);

-- 插入测试数据
INSERT INTO test_engine_switch VALUES 
(1, 'test1', 10.5),
(2, 'test2', 20.3),
(3, 'test3', 30.7);

-- 使用Spark引擎查询
SET hive.execution.engine=spark;
SELECT COUNT(*), AVG(value) FROM test_engine_switch;

-- 使用MapReduce引擎查询
SET hive.execution.engine=mr;
SELECT COUNT(*), AVG(value) FROM test_engine_switch;
EOF

# 执行测试
bin/beeline -u jdbc:hive2://liuf2:10000 -f /tmp/test_engine_switch.sql

第六阶段：监控与验证
6.1 在liuf1节点监控服务状态
# 在liuf1节点执行
echo "=== 服务状态检查 ==="

echo "HiveMetaStore状态:"
jps | grep -E "(RunJar|HiveMetaStore)"

echo ""
echo "HiveServer2状态:"
ssh liuf2 "jps | grep -E '(RunJar|HiveServer)'"

echo ""
echo "Spark History Server状态:"
ssh liuf5 "jps | grep -E '(HistoryServer)'"

echo ""
echo "YARN应用状态:"
yarn application -list | grep -E "(hive|spark)"

6.2 在liuf1节点查看日志
# 在liuf1节点执行
echo "=== 日志检查 ==="

echo "HiveMetaStore日志:"
tail -20 /var/log/hive/metastore.log

echo ""
echo "HiveServer2日志:"
ssh liuf2 "tail -20 /var/log/hive/hiveserver2.log"
第七阶段：故障排除
7.1 在liuf1节点检查JAR包
# 在liuf1节点执行
echo "=== JAR包检查 ==="

echo "Hive lib目录中的Spark JAR包:"
ls /opt/datasophon/hive-3.1.0/lib/spark-*.jar | wc -l
ls /opt/datasophon/hive-3.1.0/lib/spark-core_*.jar
ls /opt/datasophon/hive-3.1.0/lib/spark-sql_*.jar
ls /opt/datasophon/hive-3.1.0/lib/spark-hive_*.jar

echo ""
echo "SLF4J冲突检查:"
ls /opt/datasophon/hive-3.1.0/lib/slf4j-log4j12-*.jar 2>/dev/null || echo "✓ 冲突JAR包已移除"
ls /opt/datasophon/hive-3.1.0/lib/log4j-slf4j-impl-*.jar 2>/dev/null || echo "✓ 冲突JAR包已移除"
ls /opt/datasophon/hive-3.1.0/lib/slf4j-reload4j-*.jar && echo "✓ 统一SLF4J绑定存在"
7.2 在liuf1节点检查网络连接
# 在liuf1节点执行
echo "=== 网络连接检查 ==="

echo "检查HiveServer2连接:"
telnet liuf2 10000 < /dev/null && echo "✓ HiveServer2连接正常" || echo "✗ HiveServer2连接失败"

echo "检查Spark History Server连接:"
telnet liuf5 18080 < /dev/null && echo "✓ Spark History Server连接正常" || echo "✗ Spark History Server连接失败"

执行顺序总结
1、  liuf5节点：上传Spark JAR包到HDFS，启动Spark History Server
2、  liuf1节点：复制JAR包，配置hive-site.xml，启动HiveMetaStore
3、  liuf2节点：复制JAR包，复制配置文件，启动HiveServer2
4、  liuf3节点：复制JAR包，复制配置文件
5、  liuf1节点：测试连接和引擎切换功能
这个方案确保了每个节点都按照正确的顺序执行相应的操作，避免了跨节点操作的复杂性。