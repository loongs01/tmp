#!/bin/bash

# 基于实际JAR包名称的复制脚本
echo "=== 复制实际的Spark JAR包到Hive ==="

SPARK_HOME="/opt/datasophon/spark-3.1.3"
HIVE_HOME="/opt/datasophon/hive-3.1.0"
HADOOP_HOME="/opt/datasophon/hadoop-3.3.3"
HIVE_HOSTS="liuf1 liuf2 liuf3"

# 函数：复制JAR包到指定主机
copy_jars_to_host() {
    local host=$1
    echo "正在处理节点: $host"
    
    # 创建备份
    ssh $host "cp -r $HIVE_HOME/lib $HIVE_HOME/lib.bak.$(date +%Y%m%d_%H%M%S)"
    
    # 复制Spark核心JAR包（使用实际文件名）
    echo "  复制Spark核心JAR包..."
    ssh $host "cp $SPARK_HOME/jars/spark-core_*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到spark-core JAR'"
    ssh $host "cp $SPARK_HOME/jars/spark-sql_*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到spark-sql JAR'"
    ssh $host "cp $SPARK_HOME/jars/spark-hive_*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到spark-hive JAR'"
    ssh $host "cp $SPARK_HOME/jars/spark-network-common_*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到spark-network-common JAR'"
    ssh $host "cp $SPARK_HOME/jars/spark-unsafe_*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到spark-unsafe JAR'"
    ssh $host "cp $SPARK_HOME/jars/spark-launcher_*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到spark-launcher JAR'"
    ssh $host "cp $SPARK_HOME/jars/spark-catalyst_*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到spark-catalyst JAR'"
    
    # 复制所有Spark JAR包（更全面的方法）
    echo "  复制所有Spark JAR包..."
    ssh $host "cp $SPARK_HOME/jars/spark-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '复制Spark JAR包失败'"
    
    # 复制Hadoop相关JAR包（从Hadoop安装目录）
    echo "  复制Hadoop相关JAR包..."
    ssh $host "cp $HADOOP_HOME/share/hadoop/common/hadoop-client-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到hadoop-client JAR'"
    ssh $host "cp $HADOOP_HOME/share/hadoop/yarn/hadoop-yarn-api-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到hadoop-yarn-api JAR'"
    ssh $host "cp $HADOOP_HOME/share/hadoop/yarn/hadoop-yarn-client-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到hadoop-yarn-client JAR'"
    ssh $host "cp $HADOOP_HOME/share/hadoop/yarn/hadoop-yarn-common-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到hadoop-yarn-common JAR'"
    
    # 复制序列化相关JAR包（查找实际名称）
    echo "  复制序列化相关JAR包..."
    ssh $host "cp $SPARK_HOME/jars/kryo-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到kryo JAR'"
    ssh $host "cp $SPARK_HOME/jars/minlog-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到minlog JAR'"
    ssh $host "cp $SPARK_HOME/jars/reflectasm-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到reflectasm JAR'"
    ssh $host "cp $SPARK_HOME/jars/objenesis-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到objenesis JAR'"
    
    # 移除冲突的SLF4J绑定
    echo "  移除冲突的SLF4J绑定..."
    ssh $host "rm -f $HIVE_HOME/lib/slf4j-log4j12-*.jar"
    ssh $host "rm -f $HIVE_HOME/lib/log4j-slf4j-impl-*.jar"
    
    # 复制统一的SLF4J绑定
    echo "  复制统一的SLF4J绑定..."
    ssh $host "cp $HADOOP_HOME/share/hadoop/common/lib/slf4j-reload4j-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到slf4j-reload4j JAR'"
    
    echo "节点 $host 处理完成"
}

# 主函数
main() {
    echo "开始复制JAR包到Hive节点..."
    
    # 首先显示实际的JAR包
    echo "=== 显示Spark目录中的实际JAR包 ==="
    echo "Spark核心JAR包:"
    ls $SPARK_HOME/jars/spark-core_*.jar 2>/dev/null || echo "  未找到 spark-core_*.jar"
    ls $SPARK_HOME/jars/spark-sql_*.jar 2>/dev/null || echo "  未找到 spark-sql_*.jar"
    ls $SPARK_HOME/jars/spark-hive_*.jar 2>/dev/null || echo "  未找到 spark-hive_*.jar"
    
    echo ""
    echo "Hadoop目录中的JAR包:"
    ls $HADOOP_HOME/share/hadoop/common/hadoop-client-*.jar 2>/dev/null || echo "  未找到 hadoop-client-*.jar"
    ls $HADOOP_HOME/share/hadoop/yarn/hadoop-yarn-api-*.jar 2>/dev/null || echo "  未找到 hadoop-yarn-api-*.jar"
    
    echo ""
    echo "=== 开始复制到各节点 ==="
    
    # 复制到各Hive节点
    for host in $HIVE_HOSTS; do
        copy_jars_to_host $host
        echo ""
    done
    
    echo "JAR包复制完成！"
    echo ""
    echo "=== 验证复制结果 ==="
    
    # 验证复制结果
    for host in $HIVE_HOSTS; do
        echo "验证节点 $host:"
        ssh $host "ls $HIVE_HOME/lib/spark-core_*.jar 2>/dev/null && echo '  ✓ spark-core JAR存在' || echo '  ✗ spark-core JAR缺失'"
        ssh $host "ls $HIVE_HOME/lib/spark-sql_*.jar 2>/dev/null && echo '  ✓ spark-sql JAR存在' || echo '  ✗ spark-sql JAR缺失'"
        ssh $host "ls $HIVE_HOME/lib/spark-hive_*.jar 2>/dev/null && echo '  ✓ spark-hive JAR存在' || echo '  ✗ spark-hive JAR缺失'"
        echo ""
    done
}

# 运行主函数
main "$@" 