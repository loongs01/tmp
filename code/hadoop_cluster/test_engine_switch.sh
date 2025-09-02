#!/bin/bash

# 引擎切换测试脚本
echo "=== 引擎切换功能测试 ==="

HIVE_HOME="/opt/datasophon/hive-3.1.0"

# 创建测试SQL文件
create_test_sql() {
    cat > /tmp/test_engine_switch.sql << 'SQL_EOF'
-- 引擎切换测试脚本

-- 查看当前引擎
SELECT '当前执行引擎:' as info;
SET hive.execution.engine;

-- 切换到Spark引擎
SELECT '切换到Spark引擎' as info;
SET hive.execution.engine=spark;

-- 验证Spark引擎设置
SELECT '验证Spark引擎设置' as info;
SET hive.execution.engine;

-- 创建测试表
SELECT '创建测试表' as info;
CREATE TABLE IF NOT EXISTS test_engine_switch (
    id INT,
    name STRING,
    value DOUBLE
);

-- 插入测试数据
SELECT '插入测试数据' as info;
INSERT INTO test_engine_switch VALUES 
(1, 'test1', 10.5),
(2, 'test2', 20.3),
(3, 'test3', 30.7);

-- 使用Spark引擎查询
SELECT '使用Spark引擎查询' as info;
SELECT COUNT(*), AVG(value) FROM test_engine_switch;

-- 切换到MapReduce引擎
SELECT '切换到MapReduce引擎' as info;
SET hive.execution.engine=mr;

-- 验证MapReduce引擎设置
SELECT '验证MapReduce引擎设置' as info;
SET hive.execution.engine;

-- 使用MapReduce引擎查询
SELECT '使用MapReduce引擎查询' as info;
SELECT COUNT(*), AVG(value) FROM test_engine_switch;

-- 清理测试表
SELECT '清理测试表' as info;
DROP TABLE IF EXISTS test_engine_switch;
SQL_EOF
}

# 执行测试
run_test() {
    echo "执行引擎切换测试..."
    
    # 连接到Hive并执行测试
    $HIVE_HOME/bin/beeline -u jdbc:hive2://liuf2:10000 -f /tmp/test_engine_switch.sql
    
    if [ $? -eq 0 ]; then
        echo "✓ 引擎切换测试成功！"
    else
        echo "✗ 引擎切换测试失败！"
    fi
}

# 手动测试
manual_test() {
    echo "手动测试引擎切换..."
    
    echo "1. 测试MapReduce引擎:"
    $HIVE_HOME/bin/beeline -u jdbc:hive2://liuf2:10000 -e "SET hive.execution.engine=mr; SELECT COUNT(*) FROM (SELECT 1 as id) t;"
    
    echo ""
    echo "2. 测试Spark引擎:"
    $HIVE_HOME/bin/beeline -u jdbc:hive2://liuf2:10000 -e "SET hive.execution.engine=spark; SELECT COUNT(*) FROM (SELECT 1 as id) t;"
}

# 主函数
main() {
    echo "开始引擎切换测试..."
    
    # 创建测试SQL
    create_test_sql
    
    # 执行测试
    run_test
    
    echo ""
    echo "=== 手动测试 ==="
    manual_test
    
    echo ""
    echo "测试完成！"
}

# 运行主函数
main "$@" 