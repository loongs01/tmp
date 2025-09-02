#!/bin/bash

# Hive-Spark集成一键部署脚本
echo "=== Hive-Spark集成一键部署 ==="

# 环境变量
export SPARK_HOME="/opt/datasophon/spark-3.1.3"
export HIVE_HOME="/opt/datasophon/hive-3.1.0"
export HADOOP_HOME="/opt/datasophon/hadoop-3.3.3"
export HIVE_HOSTS="liuf1 liuf2 liuf3"
export SPARK_HOSTS="liuf5 liuf6 liuf7"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 步骤1: 环境检查
check_environment() {
    log_info "步骤1: 环境检查"
    
    # 检查环境变量
    if [ ! -d "$SPARK_HOME" ]; then
        log_error "SPARK_HOME不存在: $SPARK_HOME"
        return 1
    fi
    
    if [ ! -d "$HIVE_HOME" ]; then
        log_error "HIVE_HOME不存在: $HIVE_HOME"
        return 1
    fi
    
    if [ ! -d "$HADOOP_HOME" ]; then
        log_error "HADOOP_HOME不存在: $HADOOP_HOME"
        return 1
    fi
    
    log_info "环境检查通过"
    return 0
}

# 步骤2: JAR包分发
distribute_jars() {
    log_info "步骤2: JAR包分发"
    
    # 运行JAR包分发脚本
    if [ -f "final_jar_distribution.sh" ]; then
        chmod +x final_jar_distribution.sh
        ./final_jar_distribution.sh
    else
        log_error "final_jar_distribution.sh脚本不存在"
        return 1
    fi
    
    log_info "JAR包分发完成"
    return 0
}

# 步骤3: 上传JAR包到HDFS
upload_jars_to_hdfs() {
    log_info "步骤3: 上传JAR包到HDFS"
    
    # 运行HDFS上传脚本
    if [ -f "upload_spark_jars_to_hdfs.sh" ]; then
        chmod +x upload_spark_jars_to_hdfs.sh
        ./upload_spark_jars_to_hdfs.sh
    else
        log_error "upload_spark_jars_to_hdfs.sh脚本不存在"
        return 1
    fi
    
    log_info "HDFS上传完成"
    return 0
}

# 步骤4: 配置文件分发
distribute_config() {
    log_info "步骤4: 配置文件分发"
    
    # 运行配置文件分发脚本
    if [ -f "distribute_hive_config.sh" ]; then
        chmod +x distribute_hive_config.sh
        ./distribute_hive_config.sh
    else
        log_error "distribute_hive_config.sh脚本不存在"
        return 1
    fi
    
    log_info "配置文件分发完成"
    return 0
}

# 步骤5: 启动Hive服务
start_hive_services() {
    log_info "步骤5: 启动Hive服务"
    
    # 运行服务启动脚本
    if [ -f "start_hive_services.sh" ]; then
        chmod +x start_hive_services.sh
        ./start_hive_services.sh
    else
        log_error "start_hive_services.sh脚本不存在"
        return 1
    fi
    
    log_info "Hive服务启动完成"
    return 0
}

# 步骤6: 功能测试
test_functionality() {
    log_info "步骤6: 功能测试"
    
    # 运行功能测试脚本
    if [ -f "test_engine_switch.sh" ]; then
        chmod +x test_engine_switch.sh
        ./test_engine_switch.sh
    else
        log_error "test_engine_switch.sh脚本不存在"
        return 1
    fi
    
    log_info "功能测试完成"
    return 0
}

# 步骤7: 监控验证
monitor_and_verify() {
    log_info "步骤7: 监控验证"
    
    # 运行监控脚本
    if [ -f "monitor_hive_engines.sh" ]; then
        chmod +x monitor_hive_engines.sh
        ./monitor_hive_engines.sh
    else
        log_error "monitor_hive_engines.sh脚本不存在"
        return 1
    fi
    
    log_info "监控验证完成"
    return 0
}

# 主函数
main() {
    log_info "开始Hive-Spark集成部署..."
    echo "=================================="
    
    # 执行各步骤
    steps=(
        "check_environment"
        "distribute_jars"
        "upload_jars_to_hdfs"
        "distribute_config"
        "start_hive_services"
        "test_functionality"
        "monitor_and_verify"
    )
    
    for step in "${steps[@]}"; do
        log_info "执行步骤: $step"
        if ! $step; then
            log_error "步骤 $step 执行失败"
            exit 1
        fi
        echo ""
    done
    
    log_info "Hive-Spark集成部署完成！"
    echo "=================================="
    log_info "部署总结:"
    log_info "- Hive节点: $HIVE_HOSTS"
    log_info "- Spark节点: $SPARK_HOSTS"
    log_info "- HiveServer2: liuf2:10000"
    log_info "- HiveMetaStore: liuf1:9083"
    log_info "- Spark History Server: liuf5:18080"
    echo ""
    log_info "使用说明:"
    log_info "1. 连接到Hive: beeline -u jdbc:hive2://liuf2:10000"
    log_info "2. 切换引擎: SET hive.execution.engine=spark;"
    log_info "3. 查看引擎: SET hive.execution.engine;"
}

# 运行主函数
main "$@" 