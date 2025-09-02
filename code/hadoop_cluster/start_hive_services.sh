#!/bin/bash

# Hive服务启动脚本
echo "=== Hive服务启动 ==="

HIVE_HOSTS="liuf1 liuf2 liuf3"
HIVE_HOME="/opt/datasophon/hive-3.1.0"

# 停止Hive服务
stop_hive_services() {
    echo "停止Hive服务..."
    for host in $HIVE_HOSTS; do
        echo "停止节点 $host 的Hive服务..."
        ssh $host "pkill -f 'hive.*metastore'"
        ssh $host "pkill -f 'hive.*hiveserver2'"
        sleep 2
    done
}

# 启动Hive Metastore
start_hive_metastore() {
    echo "启动Hive Metastore..."
    ssh liuf1 "nohup $HIVE_HOME/bin/hive --service metastore > /var/log/hive/metastore.log 2>&1 &"
    sleep 5
    
    # 检查Metastore状态
    echo "检查Metastore状态..."
    ssh liuf1 "jps | grep -E '(RunJar|HiveMetaStore)'"
}

# 启动HiveServer2
start_hive_server2() {
    echo "启动HiveServer2..."
    ssh liuf2 "nohup $HIVE_HOME/bin/hive --service hiveserver2 > /var/log/hive/hiveserver2.log 2>&1 &"
    sleep 10
    
    # 检查HiveServer2状态
    echo "检查HiveServer2状态..."
    ssh liuf2 "jps | grep -E '(RunJar|HiveServer)'"
}

# 验证服务状态
verify_services() {
    echo "验证Hive服务状态..."
    for host in $HIVE_HOSTS; do
        echo "=== $host 服务状态 ==="
        ssh $host "jps | grep -E '(HiveServer|RunJar)'"
    done
}

# 测试连接
test_connection() {
    echo "测试Hive连接..."
    ssh liuf1 "$HIVE_HOME/bin/beeline -u jdbc:hive2://liuf2:10000 -e 'SET hive.execution.engine;' 2>/dev/null || echo '连接测试失败'"
}

# 主函数
main() {
    echo "开始Hive服务启动流程..."
    
    # 停止现有服务
    stop_hive_services
    
    # 启动Metastore
    start_hive_metastore
    
    # 启动HiveServer2
    start_hive_server2
    
    # 验证服务状态
    verify_services
    
    # 测试连接
    test_connection
    
    echo "Hive服务启动完成！"
}

# 运行主函数
main "$@" 