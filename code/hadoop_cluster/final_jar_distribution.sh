#!/bin/bash

# 最终JAR包分发脚本
echo "=== 最终JAR包分发配置 ==="

# 环境变量
SPARK_HOME="/opt/datasophon/spark-3.1.3"
HIVE_HOME="/opt/datasophon/hive-3.1.0"
HADOOP_HOME="/opt/datasophon/hadoop-3.3.3"
HIVE_HOSTS="liuf1 liuf2 liuf3"

# 函数：分发JAR包到Hive节点
distribute_jars_to_hive() {
    local host=$1
    echo "正在处理Hive节点: $host"
    
    # 创建备份
    ssh $host "cp -r $HIVE_HOME/lib $HIVE_HOME/lib.bak.$(date +%Y%m%d_%H%M%S)"
    
    # 复制所有Spark JAR包（简化策略）
    echo "  复制所有Spark JAR包..."
    ssh $host "cp $SPARK_HOME/jars/*.jar $HIVE_HOME/lib/"
    
    # 移除冲突的SLF4J绑定
    echo "  移除冲突的SLF4J绑定..."
    ssh $host "rm -f $HIVE_HOME/lib/slf4j-log4j12-*.jar"
    ssh $host "rm -f $HIVE_HOME/lib/log4j-slf4j-impl-*.jar"
    
    # 确保统一的SLF4J绑定
    echo "  确保统一的SLF4J绑定..."
    ssh $host "cp $HADOOP_HOME/share/hadoop/common/lib/slf4j-reload4j-*.jar $HIVE_HOME/lib/ 2>/dev/null || echo '未找到slf4j-reload4j JAR'"
    
    echo "Hive节点 $host 处理完成"
}

# 主函数
main() {
    echo "开始JAR包分发..."
    
    # 显示Spark JAR包信息
    echo "=== Spark JAR包信息 ==="
    echo "Spark JAR包总数: $(ls $SPARK_HOME/jars/*.jar 2>/dev/null | wc -l)"
    echo "关键Spark JAR包:"
    ls $SPARK_HOME/jars/spark-core_*.jar 2>/dev/null || echo "  未找到 spark-core_*.jar"
    ls $SPARK_HOME/jars/spark-sql_*.jar 2>/dev/null || echo "  未找到 spark-sql_*.jar"
    ls $SPARK_HOME/jars/spark-hive_*.jar 2>/dev/null || echo "  未找到 spark-hive_*.jar"
    
    echo ""
    echo "=== 分发到Hive节点 ==="
    
    # 分发到各Hive节点
    for host in $HIVE_HOSTS; do
        distribute_jars_to_hive $host
        echo ""
    done
    
    echo "JAR包分发完成！"
}

# 运行主函数
main "$@" 