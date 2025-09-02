CREATE TABLE dwd_user_basic_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户唯一ID',
    stat_date DATE NOT NULL COMMENT '统计日期(分区字段)',
    
    -- 基础身份信息
    nickname VARCHAR(50) COMMENT '用户昵称',
    gender TINYINT COMMENT '性别:1-男 2-女 0-未知',
    birth_date DATE COMMENT '出生日期',
    age INT COMMENT '年龄(根据出生日期计算)',
    id_card VARCHAR(18) COMMENT '身份证号'
)
PARTITION BY RANGE(stat_date) ()
-- DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",          -- 启用动态分区
    "dynamic_partition.time_unit" = "MONTH",      -- 按月动态分区
    "dynamic_partition.start" = "-3",             -- 保留最近3个月分区
    "dynamic_partition.end" = "3",                -- 预创建未来3个月分区
    "dynamic_partition.prefix" = "p",             -- 分区前缀
    "dynamic_partition.buckets" = "10",           -- 分桶数
    "enable_persistent_index" = "true",
    "bloom_filter_columns" = "user_id,id_card"
);

-- 可选：创建物化视图加速聚合查询（如按城市等级分析）
CREATE materialized VIEW mv_user_city_tier 
DISTRIBUTED BY HASH(user_id) BUCKETS 10
REFRESH ASYNC
AS SELECT 
    user_id, stat_date, city_tier, current_province, current_city
FROM dwd_user_basic_info_di;



