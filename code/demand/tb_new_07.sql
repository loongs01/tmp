DROP TABLE IF EXISTS dwd_user_basic_info_di;
CREATE TABLE dwd_user_basic_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户唯一ID',
    stat_date DATE NOT NULL COMMENT '统计日期(分区字段)',
    
    -- 基础身份信息
	user_name                   VARCHAR(20)          COMMENT '用户姓名',
    nickname VARCHAR(20) COMMENT '用户昵称',
    gender TINYINT COMMENT '性别:1-男 2-女 0-未知',
    birth_date DATE COMMENT '出生日期',
    age INT COMMENT '年龄(根据出生日期计算)',
    id_card VARCHAR(18) COMMENT '身份证号',
    
    -- 地域信息
    native_province VARCHAR(20) COMMENT '籍贯省份',
    native_city VARCHAR(20) COMMENT '籍贯城市',
    current_province VARCHAR(20) COMMENT '当前居住省份',
    current_city VARCHAR(20) COMMENT '当前居住城市',
    city_tier TINYINT COMMENT '城市等级:1-一线 2-二线 3-三线 4-四线',
    ip_location VARCHAR(20) COMMENT '最近登录IP所在地',
    
    -- 社会关系状态
    marital_status TINYINT COMMENT '婚姻状况:1-未婚 2-已婚 3-离异 4-丧偶',
    relationship_status TINYINT COMMENT '感情状态:1-单身 2-恋爱中 3-已婚 4-离异',
    
    -- 教育信息
    education_level TINYINT COMMENT '教育程度:1-初中及以下 2-高中 3-大专 4-本科 5-硕士 6-博士',
    school_name VARCHAR(100) COMMENT '毕业院校',
    major VARCHAR(50) COMMENT '专业',
    
    -- 经济状况
    annual_income DECIMAL(10,2) COMMENT '年收入(万元)',
    has_mortgage BOOLEAN COMMENT '是否有房贷',
    has_car_loan BOOLEAN COMMENT '是否有车贷',
    
    -- 账号信息
    login_name VARCHAR(50) COMMENT '登录账号',
    password VARCHAR(64) COMMENT '密码(MD5加密)',
    status TINYINT COMMENT '账号状态:1-正常 2-冻结',
    last_login_time DATETIME COMMENT '最后登录时间',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_city_tier (city_tier),
    INDEX idx_education (education_level)
) COMMENT='用户基础信息明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);


DROP TABLE IF EXISTS dwd_user_tier_info_di;
CREATE TABLE dwd_user_tier_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 用户等级体系
    user_level TINYINT COMMENT '用户等级:1-普通 2-白银 3-黄金 4-铂金 5-钻石',
    level_expire_date DATE COMMENT '等级有效期',
    
    -- RFM模型
    r_score TINYINT COMMENT '最近一次消费(Recency):1-5分',
    f_score TINYINT COMMENT '消费频率(Frequency):1-5分',
    m_score TINYINT COMMENT '消费金额(Monetary):1-5分',
    rfm_score VARCHAR(10) COMMENT 'RFM综合分值(如3-4-5)',
    rfm_group VARCHAR(20) COMMENT 'RFM分组(如高价值用户)',
    
    -- 生命周期
    user_lifecycle VARCHAR(20) COMMENT '用户生命周期:新用户/成长期/成熟期/衰退期/流失',
    lifecycle_score TINYINT COMMENT '生命周期评分：1-100',
    
    -- VIP信息
    is_vip BOOLEAN COMMENT '是否VIP会员',
    vip_level TINYINT COMMENT 'VIP等级',
    vip_expire_date DATE COMMENT 'VIP到期日',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_rfm (rfm_score),
    INDEX idx_lifecycle (user_lifecycle)
) COMMENT='用户分层信息明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

DROP TABLE IF EXISTS dwd_user_career_info_di;
CREATE TABLE dwd_user_career_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 职业基础信息
    industry VARCHAR(50) COMMENT '行业',
    profession VARCHAR(50) COMMENT '职业',
    job_title VARCHAR(50) COMMENT '职位名称',
    company_name VARCHAR(100) COMMENT '公司名称',
    company_size VARCHAR(20) COMMENT '公司规模',
    
    -- 职业发展
    work_experience INT COMMENT '工作年限',
    is_entrepreneur BOOLEAN COMMENT '是否创业者',
    entrepreneurial_stage VARCHAR(20) COMMENT '创业阶段:初创期/成长期/成熟期',
    entrepreneurial_goal VARCHAR(100) COMMENT '创业目标',
    
    -- 职业讨论
    career_discussion_freq VARCHAR(20) COMMENT '职业讨论频率:高频/中频/低频',
    career_concerns JSON COMMENT '职业关注点(JSON数组)',
    
    -- 技能信息
    skills JSON COMMENT '技能列表(JSON数组)',
    certifications JSON COMMENT '证书列表(JSON数组)',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_industry (industry),
    INDEX idx_profession (profession)
) COMMENT='用户职业信息明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

DROP TABLE IF EXISTS dwd_user_family_info_di;
CREATE TABLE dwd_user_family_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 家庭结构
    family_type VARCHAR(20) COMMENT '家庭类型:核心家庭/丁克家庭/单身家庭/三代同堂/单亲家庭/空巢家庭/合租群体',
    family_members JSON COMMENT '家庭成员信息(JSON数组)：[
      {
        "relation": "父亲",
        "age": 65,
        "health": "心脑血管疾病",
        "occupation": "退休",
        "is_retired": true
      }
    ]',

    
    -- 家庭需求
    elderly_care_needed BOOLEAN COMMENT '是否需要养老照料',
    child_education_needed BOOLEAN COMMENT '是否需要子女教育',
    
    -- 家庭关系
    relationship_quality VARCHAR(10) COMMENT '家庭关系质量:和睦/一般/紧张',
    contact_frequency VARCHAR(20) COMMENT '联系频率:每天/每周/每月',
    
    -- 经济压力
    financial_pressure_type VARCHAR(20) COMMENT '经济压力类型:多子女负担/赡养老人压力/无贷家庭',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_family_type (family_type)
) COMMENT='用户家庭关系明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);


DROP TABLE IF EXISTS dwd_user_interest_info_di;
CREATE TABLE dwd_user_interest_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 旅游兴趣
    travel_preferences JSON COMMENT '旅游偏好(JSON数组)',
    memorable_trips JSON COMMENT '印象深刻的旅行(JSON数组)',
    travel_wishlist JSON COMMENT '旅行愿望清单(JSON数组)',
    travel_constraints JSON COMMENT '旅行限制(JSON对象)',
    
    -- 游戏兴趣
    games_played JSON COMMENT '玩过的游戏(JSON数组)',
    game_preferences JSON COMMENT '游戏偏好分布(JSON对象)',
    game_play_style VARCHAR(50) COMMENT '游戏风格',
    game_frequency VARCHAR(20) COMMENT '游戏频率',
    
    -- 运动兴趣
    sports_preferences JSON COMMENT '运动偏好(JSON数组)',
    fitness_level VARCHAR(20) COMMENT '健身水平',
    sports_frequency VARCHAR(20) COMMENT '运动频率',
    genetic_sports_ability VARCHAR(50) COMMENT '基因运动能力',
    
    -- 其他兴趣
    reading_preferences JSON COMMENT '阅读偏好',
    music_preferences JSON COMMENT '音乐偏好',
    movie_preferences JSON COMMENT '电影偏好',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_game_style (game_play_style)
) COMMENT='用户兴趣爱好明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);


DROP TABLE IF EXISTS dwd_user_health_info_di;
CREATE TABLE dwd_user_health_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 基础健康指标
    height DECIMAL(5,2) COMMENT '身高(cm)',
    weight DECIMAL(5,2) COMMENT '体重(kg)',
    bmi DECIMAL(4,2) COMMENT 'BMI指数',
    blood_type VARCHAR(5) COMMENT '血型',
    
    -- 健康风险
    genetic_risks JSON COMMENT '遗传疾病风险(JSON数组：[{
      "disease": "遗传疾病名称",
      "risk_level": "风险等级(高/中/低)",
      "probability": "患病概率(0-1)",
      "affected_genes": ["相关基因1", "相关基因2"],
      "family_history": "家族病史情况"
    }])',
    potential_diseases JSON COMMENT '潜在疾病(JSON数组)',
    allergies JSON COMMENT '过敏信息(JSON数组)',
    
    -- 营养代谢
    nutrition_needs JSON COMMENT '营养需求(JSON对象:{"营养元素": "需求程度"})',
    metabolic_rate VARCHAR(20) COMMENT '代谢率',
    
    -- 生活方式
    exercise_habits VARCHAR(50) COMMENT '运动习惯',
    sleep_quality VARCHAR(20) COMMENT '睡眠质量',
    alcohol_reaction VARCHAR(50) COMMENT '酒精反应',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_bmi (bmi)
) COMMENT='用户健康信息明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);


DROP TABLE IF EXISTS dwd_user_behavior_info_di;
CREATE TABLE dwd_user_behavior_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 使用行为
    usage_frequency VARCHAR(10) COMMENT '使用频率:低/中/高',
    total_conversations INT COMMENT '总对话数',
    voice_call_count INT COMMENT '语音通话次数',
    text_message_count INT COMMENT '文字消息数',
    last_login_time DATETIME COMMENT '最后登录时间',
    
    -- 内容偏好
    preferred_topics JSON COMMENT '偏好话题(JSON数组)',
    content_categories JSON COMMENT '内容分类偏好(JSON对象)',
    communication_style VARCHAR(20) COMMENT '沟通风格',
    
    -- 设备信息
    device_type VARCHAR(50) COMMENT '设备类型',
    os_version VARCHAR(50) COMMENT '操作系统版本',
    app_version VARCHAR(50) COMMENT '应用版本',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_usage_freq (usage_frequency)
) COMMENT='用户行为信息明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



DROP TABLE IF EXISTS dwd_user_life_events_di;
CREATE TABLE dwd_user_life_events_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 事件信息
    event_date DATE NOT NULL COMMENT '事件日期',
    event_type VARCHAR(50) NOT NULL COMMENT '事件类型',
    event_content TEXT COMMENT '事件内容',
    
    -- 事件属性
    importance_level TINYINT COMMENT '重要程度:1-5',
    emotional_tone TINYINT COMMENT '情感基调:1-负面 2-中性 3-正面',
    is_milestone BOOLEAN COMMENT '是否里程碑事件',
    
    -- 关联信息
    related_persons JSON COMMENT '相关人员(JSON数组)',
    location VARCHAR(100) COMMENT '事件地点',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_event (stat_date,user_id, event_date, event_type),
    INDEX idx_event_type (event_type),
    INDEX idx_event_date (event_date)
) COMMENT='用户人生事件明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



DROP TABLE IF EXISTS dwd_user_aspiration_info_di;
CREATE TABLE dwd_user_aspiration_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 梦想信息
	user_name                   VARCHAR(20)          COMMENT '用户姓名',
    dream VARCHAR(255) COMMENT '长期梦想',
    dream_detail TEXT COMMENT '梦想详情',
    dream_importance TINYINT COMMENT '梦想重要性:1-5',
    
    -- 理想信息
    ideal VARCHAR(255) COMMENT '短期理想',
    ideal_detail TEXT COMMENT '理想详情',
    ideal_deadline DATE COMMENT '理想截止日期',
    
    -- 行动计划
    related_actions JSON COMMENT '相关行动(JSON数组):[
        {
          "date": "行动日期(YYYY-MM-DD)",
          "action": "具体行动描述"
        }
      ]',
    progress_percentage TINYINT COMMENT '完成进度百分比',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_dream (dream(20)),
    INDEX idx_ideal (ideal(20))
) COMMENT='用户梦想与理想明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);


DROP TABLE IF EXISTS dwd_user_consumption_info_di;
CREATE TABLE dwd_user_consumption_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 经济压力
    financial_pressure VARCHAR(50) COMMENT '经济压力描述',
    debt_to_income_ratio DECIMAL(5,2) COMMENT '债务收入比(%)',
    
    -- 消费偏好
    consumption_preferences JSON COMMENT '消费偏好(JSON对象)',
    preferred_payment_method VARCHAR(20) COMMENT '首选支付方式',
    
    -- 消费水平
    travel_consumption_level VARCHAR(20) COMMENT '旅行消费档次',
    entertainment_consumption_level VARCHAR(20) COMMENT '娱乐消费档次',
    health_consumption_level VARCHAR(20) COMMENT '健康消费档次',
    
    -- 消费习惯
    is_bargain_hunter BOOLEAN COMMENT '是否喜欢优惠',
    is_premium_shopper BOOLEAN COMMENT '是否偏好高端商品',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_fin_pressure (financial_pressure)
) COMMENT='用户消费特征明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



DROP TABLE IF EXISTS dwd_user_personality_info_di;
CREATE TABLE dwd_user_personality_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    
    -- 性格特征
    personality_traits JSON COMMENT '性格特质(JSON数组)',
    personality_strengths JSON COMMENT '性格优点(JSON数组)',
    personality_weaknesses JSON COMMENT '性格缺点(JSON数组)',
    
    -- 分析结果
    personality_summary TEXT COMMENT '性格总结',
    strengths_suggestions TEXT COMMENT '扬长建议',
    weaknesses_suggestions TEXT COMMENT '避短建议',
    
    -- 来源信息
    analysis_method VARCHAR(50) COMMENT '分析方法',
    confidence_score DECIMAL(3,2) COMMENT '分析置信度',
    
    -- 系统字段
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id,stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date)
) COMMENT='用户性格分析明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



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
	stat_date DATE NOT NULL COMMENT '统计日期',
    tag_id BIGINT NOT NULL COMMENT '标签ID',
    tag_value VARCHAR(255) COMMENT '标签值(适用于多值标签)',
    tag_source VARCHAR(20) NOT NULL COMMENT '标签来源(系统/人工/模型等)',
    confidence_score DECIMAL(5,2) COMMENT '置信度0-1',
    effective_date DATE NOT NULL COMMENT '生效日期',
    expiry_date DATE COMMENT '失效日期',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    FOREIGN KEY (tag_id) REFERENCES dim_tag_definition_di(tag_id),
    UNIQUE KEY uk_user_tag_date (user_id, tag_id, effective_date),
    INDEX idx_user_id (user_id),
    INDEX idx_tag_id (tag_id),
    INDEX idx_effective_date (effective_date),
    INDEX idx_expiry_date (expiry_date)
) COMMENT '用户标签关系明细表'
;


