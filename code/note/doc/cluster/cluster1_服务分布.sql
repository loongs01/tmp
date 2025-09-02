#!/bin/bash

# 快速检查关键JAR包
echo "=== 快速JAR包检查 ==="

# 检查Spark源目录
echo "检查Spark源JAR包..."
if [ -f "/opt/datasophon/spark-3.1.3/jars/spark-core_2.12-3.1.3.jar" ]; then
    echo "  ✓ spark-core JAR存在"
else
    echo "  ✗ spark-core JAR缺失"
fi

if [ -f "/opt/datasophon/spark-3.1.3/jars/spark-sql_2.12-3.1.3.jar" ]; then
    echo "  ✓ spark-sql JAR存在"
else
    echo "  ✗ spark-sql JAR缺失"
fi

if [ -f "/opt/datasophon/spark-3.1.3/jars/spark-hive_2.12-3.1.3.jar" ]; then
    echo "  ✓ spark-hive JAR存在"
else
    echo "  ✗ spark-hive JAR缺失"
fi

# 检查Hive节点
for host in liuf1 liuf2 liuf3; do
    echo "检查节点 $host..."
    if ssh $host "ls /opt/datasophon/hive-3.1.0/lib/spark-core_*.jar 2>/dev/null" | grep -q .; then
        echo "  ✓ $host: spark-core JAR存在"
    else
        echo "  ✗ $host: spark-core JAR缺失"
    fi
    
    if ssh $host "ls /opt/datasophon/hive-3.1.0/lib/spark-sql_*.jar 2>/dev/null" | grep -q .; then
        echo "  ✓ $host: spark-sql JAR存在"
    else
        echo "  ✗ $host: spark-sql JAR缺失"
    fi
    
    if ssh $host "ls /opt/datasophon/hive-3.1.0/lib/spark-hive_*.jar 2>/dev/null" | grep -q .; then
        echo "  ✓ $host: spark-hive JAR存在"
    else
        echo "  ✗ $host: spark-hive JAR缺失"
    fi
done

# 检查HDFS
echo "检查HDFS上的JAR包..."
if hdfs dfs -test -d /spark-jars 2>/dev/null; then
    jar_count=$(hdfs dfs -ls /spark-jars/*.jar 2>/dev/null | wc -l)
    echo "  ✓ HDFS上有 $jar_count 个JAR包"
else
    echo "  ✗ HDFS目录 /spark-jars 不存在"
fi

echo "检查完成！" 




集群节点分布：
Hive服务：liuf1, liuf2, liuf3
Spark服务：liuf5, liuf6, liuf7
HDFS服务：liuf1, liuf2, liuf3, liuf6
cp /opt/datasophon/spark-3.1.3/jars/*.jar lib/无法执行


scp /opt/datasophon/spark-3.1.3/jars/*.jar liuf1:/opt/datasophon/hive-3.1.0/lib


NodeManager 节点所在服务器是liuf1，liuf2，liuf3，liuf5，liuf6，liuf7
集群节点分布：
Hive服务：liuf1, liuf2, liuf3
Spark服务：liuf5, liuf6, liuf7
HDFS服务：liuf1, liuf2, liuf3, liuf5，liuf6，liuf7
如何判断NodeManager

--new
hive on spark ，配置可以进行hive引擎在mr和spark引擎直接自由切换,已安装了： hadoop-3.3.3，hive-3.1.0
集群部署情况：
NodeManager 节点所在服务器是liuf1，liuf2，liuf3，liuf5，liuf6，liuf7
集群节点分布：
Hive服务：liuf1, liuf2, liuf3，其中HiveMetaStore是liuf1；
         HiveServer2是liuf2, liuf3，HiveClient是liuf1, liuf2, liuf3
HDFS服务：liuf1, liuf2, liuf3, liuf5，liuf6，liuf7
问题如下：

根据历史会话上下文，spark手动编译完后:spark-3.2.4-bin-hadoop3.3.3.tgz,如何操作可以使用hive on spark




-- 查看NodeManager
yarn node -list

YARN 应用日志（yarn logs -applicationId <appid>）;


which spark-submit

cp hive-site.xml hive-site.xml.bak.$(date +%Y%m%d_%H%M%S)

hdfs dfs -chmod go+rwx /tmp/hadoop-yarn/staging


-- 查看属组信息
cat /etc/group
