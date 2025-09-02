./flink run -c com.example.App /home/lcz/lcz/flink-1.0-SNAPSHOT.jar 192.168.18.154 9999


./bin/flink run -m 192.168.231.131:8081 -c org.example.App /home/lcz/lcz/flink-1.0-SNAPSHOT.jar 192.168.18.154 9999

metrics.profiler.include: "org.apache.flink.runtime.profiler.AsyncProfiler"
metrics.profiler.async-profiler.path: "/opt/async-profiler/lib/libasyncProfiler.so"
192.168.231.131：9092


bin/kafka-consumer-groups.sh --bootstrap-server 192.168.231.131:9092 --list



mvn dependency:purge-local-repository
mvn clean compile -U  


/opt/flink/bin/flink run -m 192.168.231.131:8081 -c org.example.App /home/lcz/lcz/flink-1.0-SNAPSHOT.jar 192.168.231.131:9092




org.example.FlinkKafkaConsumer

set hive.execution.engine=spark;

spark-sql --queue root.bdp.bdd.etl

--conf spark.sql.sources.default=parquet



spark-sql --queue root.bdp.bdd.etl 

--conf spark.sql.sources.bucketing.enabled=false



spark-sql --queue root.bdp.bdd.etl -e "sql"



--queue root.bdp.bdd.etl --conf spark.executor.memory=8G

--

~/.bashrc

/etc/profile

select current_database();

set hive.cli.print.current.db=true;

set hive.execution.engine=spark;

set hive.execution.engine;



# 检查Hive版本
hive --version

# 检查Spark版本
spark-submit --version

# 检查Hadoop版本
hadoop version

# 检查YARN状态
yarn application -list



# 检查Hive安装路径
echo $HIVE_HOME
# 输出：/opt/datasophon/hive-3.1.0

# 检查Spark安装路径
echo $SPARK_HOME
# 输出：/opt/datasophon/spark-3.1.3

# 检查Hadoop安装路径
echo $HADOOP_HOME
# 输出：/opt/datasophon/hadoop-3.3.3

-- hive
jps |grep -iE '(HiveServer|RunJar)'
-- spark
jps | grep -iE '(SparkSubmit|SparkMaster)'

ps -ef | grep metastore

hdfs路径：
NameNode :liuf2,3
/opt/datasophon/hadoop-3.3.3/etc/hadoop/hdfs-site.xml


-- 检查是否安装了 libxml2 包（xmllint 属于该包）
yum list installed | grep libxml2


-- # 验证 hive-site.xml 格式:xmllint 是 libxml2 工具包的一部分，可以验证 XML 文件的语法是否正确。
xmllint --noout hive-site.xml


-- 检查 Metastore 数据库中是否存在 NOTIFICATION_LOG 表
-- # 在 $HIVE_HOME/scripts/metastore/upgrade/mysql/ 下查找建表语句
cat hive-schema-*.mysql.sql | grep -A 20 NOTIFICATION_LOG

ll |grep -iE '.*hive-schema-.*.mysql.sql'


docker exec -it mysql-container mysql -u root -p
密码：123456

-- 检查状态
docker ps -f name=mysql-container
-- 重启：
docker restart mysql-container

1. 先进入 mysql 容器的 shell：
docker exec -it mysql-container bash



SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE  TABLE_NAME = 'NEXT_EVENT_ID';



find / -name spark-defaults.conf 2>/dev/null

find /opt/ -name spark-defaults.conf 2>/dev/null

