#!/bin/bash

# Hive引擎监控脚本
echo "=== Hive引擎监控 ==="

HIVE_HOSTS="liuf1 liuf2 liuf3"
SPARK_HOSTS="liuf5 liuf6 liuf7"

# 监控Hive服务状态
monitor_hive_services() {
    echo "=== Hive服务状态监控 ==="
    for host in $HIVE_HOSTS; do
        echo "节点 $host:"
        ssh $host "jps | grep -E '(HiveServer|RunJar)'" || echo "  无Hive服务运行"
    done
}

# 监控Spark服务状态
monitor_spark_services() {
    echo ""
    echo "=== Spark服务状态监控 ==="
    for host in $SPARK_HOSTS; do
        echo "节点 $host:"
        ssh $host "jps | grep -E '(SparkSubmit|SparkMaster)'" || echo "  无Spark服务运行"
    done
}

# 监控YARN应用
monitor_yarn_applications() {
    echo ""
    echo "=== YARN应用监控 ==="
    yarn application -list | grep -E "(hive|spark)" || echo "  无Hive/Spark应用运行"
}

# 监控HDFS状态
monitor_hdfs_status() {
    echo ""
    echo "=== HDFS状态监控 ==="
    hdfs dfsadmin -report | grep -E "(Live datanodes|Configured Capacity)" | head -5
}

# 检查Spark History Server
check_spark_history() {
    echo ""
    echo "=== Spark History Server状态 ==="
    curl -s http://liuf5:18080 | grep -o "Running Applications: [0-9]*" || echo "  无法访问Spark History Server"
}

# 检查Hive日志
check_hive_logs() {
    echo ""
    echo "=== Hive日志检查 ==="
    for host in $HIVE_HOSTS; do
        echo "节点 $host 的Hive日志:"
        ssh $host "tail -5 /var/log/hive/hiveserver2.log 2>/dev/null || echo '  无日志文件'"
        echo ""
    done
}

# 测试引擎切换
test_engine_switch() {
    echo ""
    echo "=== 引擎切换测试 ==="
    /opt/datasophon/hive-3.1.0/bin/beeline -u jdbc:hive2://liuf2:10000 -e "SET hive.execution.engine;" 2>/dev/null || echo "  无法连接到HiveServer2"
}

# 主函数
main() {
    echo "开始Hive引擎监控..."
    echo "时间: $(date)"
    echo "=================================="
    
    # 执行各项监控
    monitor_hive_services
    monitor_spark_services
    monitor_yarn_applications
    monitor_hdfs_status
    check_spark_history
    check_hive_logs
    test_engine_switch
    
    echo "=================================="
    echo "监控完成！"
}

# 运行主函数
main "$@" 