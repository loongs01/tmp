#!/bin/bash

# 设置变量
SPARK_HOME="/opt/datasophon/spark-3.1.3"
HIVE_HOME="/opt/datasophon/hive-3.1.0"
HIVE_HOSTS="liuf1 liuf2 liuf3"

# 定义需要检查的JAR包列表
declare -a spark_core_jars=(
    "spark-core_*.jar"
    "spark-sql_*.jar"
    "spark-hive_*.jar"
    "spark-network-common_*.jar"
    "spark-unsafe_*.jar"
    "spark-launcher_*.jar"
    "spark-catalyst_*.jar"
)

declare -a hadoop_jars=(
    "hadoop-client_*.jar"
    "hadoop-yarn-api_*.jar"
    "hadoop-yarn-client_*.jar"
    "hadoop-yarn-common_*.jar"
)

declare -a serialization_jars=(
    "kryo_*.jar"
    "minlog_*.jar"
    "reflectasm_*.jar"
    "objenesis_*.jar"
)

# 检查函数
check_jars_on_host() {
    local host=$1
    local hive_lib_dir="$HIVE_HOME/lib"
    local missing_jars=()
    local found_jars=()
    
    echo "=== 检查节点: $host ==="
    
    # 检查Spark核心JAR包
    echo "检查Spark核心JAR包..."
    for jar_pattern in "${spark_core_jars[@]}"; do
        if ssh $host "ls $hive_lib_dir/$jar_pattern 2>/dev/null" | grep -q .; then
            found_jars+=("$jar_pattern")
            echo "  ✓ $jar_pattern"
        else
            missing_jars+=("$jar_pattern")
            echo "  ✗ $jar_pattern (缺失)"
        fi
    done
    
    # 检查Hadoop相关JAR包
    echo "检查Hadoop相关JAR包..."
    for jar_pattern in "${hadoop_jars[@]}"; do
        if ssh $host "ls $hive_lib_dir/$jar_pattern 2>/dev/null" | grep -q .; then
            found_jars+=("$jar_pattern")
            echo "  ✓ $jar_pattern"
        else
            missing_jars+=("$jar_pattern")
            echo "  ✗ $jar_pattern (缺失)"
        fi
    done
    
    # 检查序列化相关JAR包
    echo "检查序列化相关JAR包..."
    for jar_pattern in "${serialization_jars[@]}"; do
        if ssh $host "ls $hive_lib_dir/$jar_pattern 2>/dev/null" | grep -q .; then
            found_jars+=("$jar_pattern")
            echo "  ✓ $jar_pattern"
        else
            missing_jars+=("$jar_pattern")
            echo "  ✗ $jar_pattern (缺失)"
        fi
    done
    
    # 检查SLF4J冲突JAR包是否已移除
    echo "检查SLF4J冲突JAR包..."
    if ssh $host "ls $hive_lib_dir/slf4j-log4j12-*.jar 2>/dev/null" | grep -q .; then
        echo "  ⚠ slf4j-log4j12-*.jar (存在冲突，需要移除)"
    else
        echo "  ✓ slf4j-log4j12-*.jar (已移除)"
    fi
    
    if ssh $host "ls $hive_lib_dir/log4j-slf4j-impl-*.jar 2>/dev/null" | grep -q .; then
        echo "  ⚠ log4j-slf4j-impl-*.jar (存在冲突，需要移除)"
    else
        echo "  ✓ log4j-slf4j-impl-*.jar (已移除)"
    fi
    
    # 检查统一的SLF4J绑定
    if ssh $host "ls $hive_lib_dir/slf4j-reload4j-1.7.36.jar 2>/dev/null" | grep -q .; then
        echo "  ✓ slf4j-reload4j-1.7.36.jar"
    else
        echo "  ✗ slf4j-reload4j-1.7.36.jar (缺失)"
        missing_jars+=("slf4j-reload4j-1.7.36.jar")
    fi
    
    # 统计结果
    echo ""
    echo "节点 $host 检查结果:"
    echo "  找到的JAR包: ${#found_jars[@]}"
    echo "  缺失的JAR包: ${#missing_jars[@]}"
    
    if [ ${#missing_jars[@]} -eq 0 ]; then
        echo "  ✓ 所有必需的JAR包都已存在"
        return 0
    else
        echo "  ✗ 以下JAR包缺失:"
        for jar in "${missing_jars[@]}"; do
            echo "    - $jar"
        done
        return 1
    fi
}

# 检查Spark源JAR包是否存在
check_spark_source_jars() {
    echo "=== 检查Spark源JAR包 ==="
    local missing_source_jars=()
    
    for jar_pattern in "${spark_core_jars[@]}" "${hadoop_jars[@]}" "${serialization_jars[@]}"; do
        if ls $SPARK_HOME/jars/$jar_pattern 2>/dev/null | grep -q .; then
            echo "  ✓ $jar_pattern (在Spark目录中存在)"
        else
            echo "  ✗ $jar_pattern (在Spark目录中缺失)"
            missing_source_jars+=("$jar_pattern")
        fi
    done
    
    if [ ${#missing_source_jars[@]} -eq 0 ]; then
        echo "  ✓ 所有必需的Spark源JAR包都存在"
    else
        echo "  ✗ 以下Spark源JAR包缺失:"
        for jar in "${missing_source_jars[@]}"; do
            echo "    - $jar"
        done
    fi
}

# 检查HDFS上的JAR包
check_hdfs_jars() {
    echo "=== 检查HDFS上的JAR包 ==="
    
    if hdfs dfs -test -d /spark-jars 2>/dev/null; then
        echo "  ✓ HDFS目录 /spark-jars 存在"
        local jar_count=$(hdfs dfs -ls /spark-jars/*.jar 2>/dev/null | wc -l)
        echo "  ✓ HDFS上有 $jar_count 个JAR包"
    else
        echo "  ✗ HDFS目录 /spark-jars 不存在"
    fi
}

# 主函数
main() {
    echo "开始检查Spark JAR包..."
    echo "=================================="
    
    # 检查Spark源JAR包
    check_spark_source_jars
    echo ""
    
    # 检查HDFS上的JAR包
    check_hdfs_jars
    echo ""
    
    # 检查各Hive节点
    local overall_success=true
    for host in $HIVE_HOSTS; do
        if ! check_jars_on_host $host; then
            overall_success=false
        fi
        echo ""
    done
    
    echo "=================================="
    if $overall_success; then
        echo "✓ 所有检查通过！JAR包配置正确。"
        exit 0
    else
        echo "✗ 发现JAR包缺失，请运行分发脚本修复。"
        exit 1
    fi
}

# 运行主函数
main "$@" 