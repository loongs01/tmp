org.example.FlinkKafkaConsumer


/opt/datasophon/hadoop-3.3.3

export CLASSPATH=$CLASSPATH:/opt/datasophon/hive-3.1.0/lib/*
export CLASSPATH=$CLASSPATH:/opt/datasophon/hadoop-3.3.3/share/hadoop/common/lib/*
export CLASSPATH=$CLASSPATH:/opt/datasophon/hive-3.1.0/conf

cat > hive-site.xml << 'EOF'
内容
EOF



# 检查各节点服务状态
for host in liuf1 liuf2 liuf3; do
    echo "=== $host Hive服务状态 ==="
    ssh $host "jps | grep -E '(HiveServer|RunJar)'"
done

for host in liuf5 liuf6 liuf7; do
    echo "=== $host Spark服务状态 ==="
    ssh $host "jps | grep -E '(SparkSubmit|SparkMaster)'"
done



ls -l /opt/datasophon/hive-3.1.0/lib/ | grep -E "hive-service|hive-metastore|hive-exec"

source /opt/datasophon/hive-3.1.0/conf/hive-env.sh
echo $HIVE_LIB

ls -l /opt/datasophon/hive-3.1.0/lib.bak.20250801_191602/ | grep -E "hive-service|hive-metastore|hive-exec"

ps -ef | grep metastore

telnet liuf1 9083

find /var -type d -name "log"  # 查找 /var 下名为 log 的目录
-- i忽略大小写
find /tmp -type d -iname '*lcz*'  


# 检查Hadoop集群状态（若已部署）
hdfs dfsadmin -report  # HDFS状态
yarn node -list        # YARN节点状态

-- 如何确认 NameNode 地址？
-- 在 NameNode 主机上执行以下命令查看 HDFS 配置：

hdfs getconf -confKey fs.defaultFS

hdfs haadmin -getAllServiceState nameservice1
