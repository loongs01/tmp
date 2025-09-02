#!/bin/bash

# Hive配置文件分发脚本
echo "=== Hive配置文件分发 ==="

HIVE_HOSTS="liuf1 liuf2 liuf3"
HIVE_HOME="/opt/datasophon/hive-3.1.0"

# 创建优化的hive-site.xml
create_hive_site_xml() {
    cat > /tmp/hive-site.xml << 'XML_EOF'
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

    <!-- 元数据配置 -->
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://liuf1:9083</value>
    </property>

    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/user/hive/warehouse</value>
    </property>
</configuration>
XML_EOF
}

# 分发配置文件到Hive节点
distribute_config() {
    local host=$1
    echo "分发配置文件到节点: $host"
    
    # 备份原配置
    ssh $host "cp $HIVE_HOME/conf/hive-site.xml $HIVE_HOME/conf/hive-site.xml.bak.$(date +%Y%m%d_%H%M%S)"
    
    # 复制新配置
    scp /tmp/hive-site.xml $host:$HIVE_HOME/conf/hive-site.xml
    
    # 创建日志目录
    ssh $host "mkdir -p /var/log/hive"
    
    echo "节点 $host 配置完成"
}

# 主函数
main() {
    echo "开始配置文件分发..."
    
    # 创建配置文件
    create_hive_site_xml
    
    # 分发到各Hive节点
    for host in $HIVE_HOSTS; do
        distribute_config $host
        echo ""
    done
    
    echo "配置文件分发完成！"
}

# 运行主函数
main "$@" 