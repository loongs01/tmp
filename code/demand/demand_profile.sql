1. 存储原始聊天记录、基因检测数据等最原始数据
2. DWD层(数据明细层)
存放经过清洗、转换后的明细数据

应该放置以下表：

user_basic_info (基础信息表)

user_career_info (职业信息表)

user_family_relations (家庭关系表)

user_health_info (健康信息表)

user_behavior_stats (用户行为表)


3. DWS层(数据服务层)
面向主题的轻度汇总

应该放置以下表：

user_crowd_tags (人群标签表)

user_hobbies (兴趣爱好表)

user_life_events (人生事件表)

user_personality_traits (性格分析表)

user_tag_relation (用户标签关联表)

tag_definition (标签定义表)



理由：这些表已经是对用户特征的轻度汇总和标签化处理，适合放在DWS层。

4. ADS层(应用数据层)
面向具体应用场景的高度汇总

应该放置：

dws_user_profile (用户画像宽表)

理由：这是面向业务应用的高度聚合表，直接服务于推荐系统、精准营销等应用场景。




-- implements

-- DIM层(维度层) 或 DWD层(明细层)
CREATE TABLE dim_tag_definition_da (
    tag_id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '标签ID',
    tag_name VARCHAR(50) COMMENT '标签名称',
    tag_type VARCHAR(20) COMMENT '标签类型',
    tag_category VARCHAR(20) COMMENT '标签类别',
    definition TEXT COMMENT '标签定义',
    business_rules TEXT COMMENT '业务规则',
    applicable_scenarios TEXT COMMENT '适用场景',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间'
) COMMENT '标签定义维度表';

-- DWS层(服务层)
CREATE TABLE dws_user_tags_di (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    user_id BIGINT COMMENT '用户ID',
    tag_id BIGINT COMMENT '标签ID',
    tag_source VARCHAR(20) COMMENT '标签来源',
    confidence_score DECIMAL(5,2) COMMENT '置信度0-1',
    effective_date DATE COMMENT '生效日期',
    expiry_date DATE COMMENT '失效日期',
    create_time TIMESTAMP COMMENT '创建时间',
	updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    FOREIGN KEY (tag_id) REFERENCES dim_tag_definition_da(tag_id)
) COMMENT '用户标签关联事实表';


-- 标签计算
INSERT INTO dws_user_tags_di
SELECT 
    u.user_id,
    t.tag_id,
    'system' AS tag_source,
    CASE 
        WHEN u.annual_income >= 30 THEN 0.95
        WHEN u.annual_income BETWEEN 20 AND 30 THEN 0.8
        ELSE 0.5
    END AS confidence_score,
    CURRENT_DATE AS effective_date,
    NULL AS expiry_date,
    CURRENT_TIMESTAMP AS create_time
FROM dim_user_basic u
JOIN dim_tag_definition t ON t.tag_name = '都市精英'
JOIN dim_user_career c ON u.user_id = c.user_id
WHERE u.age BETWEEN 25 AND 35
AND u.city_tier IN (1,2) -- 一二线城市
AND u.annual_income >= 20 -- 年收入20万+
AND c.industry IN ('互联网','金融','高科技');




CREATE TABLE dim_tag_definition_di (
    tag_id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '标签ID',
    tag_code VARCHAR(50) NOT NULL UNIQUE COMMENT '标签编码(英文唯一标识)',
    tag_name VARCHAR(50) NOT NULL COMMENT '标签名称',
    tag_type VARCHAR(20) NOT NULL COMMENT '标签类型(基础/人群/行为/兴趣等)',
    tag_category VARCHAR(20) NOT NULL COMMENT '标签类别',
    tag_level TINYINT COMMENT '标签层级(1-一级标签,2-二级标签)',
    parent_tag_id BIGINT COMMENT '父标签ID',
    is_multi_valued BOOLEAN DEFAULT FALSE COMMENT '是否多值标签',
    definition TEXT COMMENT '标签定义',
    business_rules TEXT COMMENT '业务规则',
    applicable_scenarios TEXT COMMENT '适用场景',
    version VARCHAR(20) COMMENT '标签版本',
    status TINYINT DEFAULT 1 COMMENT '状态(0-禁用,1-启用)',
    creator VARCHAR(50) COMMENT '创建人',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updater VARCHAR(50) COMMENT '更新人',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_tag_type (tag_type),
    INDEX idx_tag_category (tag_category)
) COMMENT '标签定义维度表';


CREATE TABLE dwd_user_tag_relation_di (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    tag_id BIGINT NOT NULL COMMENT '标签ID',
    tag_value VARCHAR(255) COMMENT '标签值(适用于多值标签)',
    tag_source VARCHAR(20) NOT NULL COMMENT '标签来源(系统/人工/模型等)',
    confidence_score DECIMAL(5,2) COMMENT '置信度0-1',
    effective_date DATE NOT NULL COMMENT '生效日期',
    expiry_date DATE COMMENT '失效日期',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    FOREIGN KEY (tag_id) REFERENCES dim_tag_definition(tag_id),
    UNIQUE KEY uk_user_tag_date (user_id, tag_id, effective_date),
    INDEX idx_user_id (user_id),
    INDEX idx_tag_id (tag_id),
    INDEX idx_effective_date (effective_date),
    INDEX idx_expiry_date (expiry_date)
) COMMENT '用户标签关系明细表'
PARTITION BY RANGE (TO_DAYS(effective_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



CREATE TABLE dws_user_tag_profile_di (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 动态生成标签JSON
    basic_tags JSON COMMENT '基础标签',
    crowd_tags JSON COMMENT '人群标签',
    behavior_tags JSON COMMENT '行为标签',
    interest_tags JSON COMMENT '兴趣标签',
    custom_tags JSON COMMENT '自定义标签',
    
    -- 标签统计指标
    total_tag_count INT COMMENT '标签总数',
    high_value_tag_count INT COMMENT '高价值标签数',
    active_tag_count INT COMMENT '活跃标签数',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_stat_date (stat_date)
) COMMENT='用户标签画像宽表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);