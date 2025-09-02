CREATE TABLE IF NOT EXISTS ads_user_profile_da (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    stat_date DATE COMMENT '统计分析日期(分区字段)，格式为YYYY-MM-DD',
    user_id BIGINT COMMENT '用户唯一标识ID',
    user_name VARCHAR(32) COMMENT '用户姓名',
    
    -- 基础信息
    basic_nickname VARCHAR(100) COMMENT '用户昵称',
    basic_gender VARCHAR(10) COMMENT '性别(男/女/未知)',
    basic_age INT COMMENT '实际年龄',
    basic_birth_date datetime COMMENT '出生日期(YYYY-MM-DD HH:MM:SS)',
    basic_native_province VARCHAR(50) COMMENT '籍贯省份',
    basic_native_city VARCHAR(50) COMMENT '籍贯城市',
    basic_current_city VARCHAR(50) COMMENT '当前居住城市',
    basic_ip_location VARCHAR(100) COMMENT '最近登录IP所在地',
    basic_marital_status VARCHAR(20) COMMENT '婚姻状态(未婚/已婚/离异/丧偶)',
    basic_relationship_status VARCHAR(20) COMMENT '感情状态(单身/恋爱中/已婚/离异)',
    basic_education VARCHAR(20) COMMENT '最高学历(初中/高中/大专/本科/硕士/博士)',
    basic_income_annual DECIMAL(10,2) COMMENT '年收入(万元)',
    -- basic_income_unit VARCHAR(10) COMMENT '货币单位',
    basic_has_mortgage BOOLEAN COMMENT '是否有房贷',
    basic_has_car_loan BOOLEAN COMMENT '是否有车贷',
    basic_city_tier TINYINT COMMENT '城市等级(1-一线,2-二线,3-三线,4-四线及以下)',
    
    -- 职业信息
    career_industry VARCHAR(100) COMMENT '所属行业',
    career_profession VARCHAR(100) COMMENT '职业名称',
    career_discussion_freq VARCHAR(20) COMMENT '职业相关讨论频率(高频/中频/低频)',
    career_concerns VARCHAR(100) COMMENT '职业关注点',
    -- career_concerns_2 VARCHAR(100) COMMENT '职业关注点2',
    career_is_entrepreneur BOOLEAN COMMENT '是否创业者',
    career_entrepreneur_stage VARCHAR(20) COMMENT '创业阶段(初创期/成长期/成熟期)',
    career_entrepreneur_goal VARCHAR(200) COMMENT '创业目标',
    
    -- 家庭信息
    family_type VARCHAR(50) COMMENT '家庭类型(核心家庭/丁克家庭/单身家庭/三代同堂/单亲家庭/空巢家庭/合租群体)',
    family_member_relation VARCHAR(20) COMMENT '家庭成员关系(父亲/母亲/配偶/子女等)',
    family_member_age INT COMMENT '家庭成员年龄',
    family_member_health VARCHAR(200) COMMENT '家庭成员健康状况描述',
    family_member_retired BOOLEAN COMMENT '家庭成员是否退休',
    -- family_member_2_relation VARCHAR(20) COMMENT '家庭成员2关系(父亲/母亲/配偶/子女等)',
    -- family_member_2_age INT COMMENT '家庭成员2年龄',
    -- family_member_2_health VARCHAR(200) COMMENT '家庭成员2健康状况描述',
    -- family_member_2_retired BOOLEAN COMMENT '家庭成员2是否退休',
    family_elderly_care_needed BOOLEAN COMMENT '是否需要养老照料',
    family_relationship_quality VARCHAR(20) COMMENT '家庭关系质量(和睦/一般/紧张)',
    
    -- 兴趣爱好 - 旅行
    interest_travel_preference VARCHAR(100) COMMENT '喜欢的旅行方式',
    -- interest_travel_preference_2 VARCHAR(100) COMMENT '喜欢的旅行方式2',
    interest_travel_memorable VARCHAR(200) COMMENT '印象深刻的旅行',
    -- interest_travel_memorable_2 VARCHAR(200) COMMENT '印象深刻的旅行2',
    interest_travel_wishlist VARCHAR(200) COMMENT '想去的旅行目的地',
    -- interest_travel_wishlist_2 VARCHAR(200) COMMENT '想去的旅行目的地2',
    interest_travel_constraint_destination VARCHAR(100) COMMENT '旅行目的地约束原因',
    
    -- 兴趣爱好 - 游戏
    interest_games_played VARCHAR(100) COMMENT '玩过的游戏',
    -- interest_games_played_2 VARCHAR(100) COMMENT '玩过的游戏2',
    interest_games_preference_type VARCHAR(50) COMMENT '游戏类型偏好',
    interest_games_preference_percent DECIMAL(5,2) COMMENT '游戏类型偏好百分比',
    interest_games_play_style VARCHAR(100) COMMENT '游戏风格描述',
    
    -- 兴趣爱好 - 运动
    interest_sports_preference VARCHAR(100) COMMENT '喜欢的运动项目',
    -- interest_sports_preference_2 VARCHAR(100) COMMENT '喜欢的运动项目2',
    interest_sports_genetic_ability VARCHAR(100) COMMENT '运动能力基因检测结果',
    
    -- 健康信息 - 遗传风险
    health_genetic_risk_disease VARCHAR(100) COMMENT '遗传疾病名称',
    health_genetic_risk_level VARCHAR(20) COMMENT '遗传疾病风险等级(高/中/低)',
    -- health_genetic_risk_2_disease VARCHAR(100) COMMENT '遗传疾病2名称',
    -- health_genetic_risk_2_level VARCHAR(20) COMMENT '遗传疾病2风险等级(高/中/低)',
    health_allergy VARCHAR(100) COMMENT '过敏源',
    -- health_allergy_2 VARCHAR(100) COMMENT '过敏源2',
    health_alcohol_reaction VARCHAR(200) COMMENT '酒精反应描述',
    health_nutrition_need_element VARCHAR(50) COMMENT '营养元素需求',
    health_nutrition_need_level VARCHAR(20) COMMENT '营养元素需求程度',
    health_potential_issue VARCHAR(100) COMMENT '潜在健康问题',
    -- health_potential_issue_2 VARCHAR(100) COMMENT '潜在健康问题2',
    
    -- 行为特征
    behavior_usage_frequency VARCHAR(20) COMMENT '使用频率(高/中/低)',
    behavior_total_conversations INT COMMENT '总对话数',
    behavior_call_count INT COMMENT '语音通话次数',
    behavior_text_count INT COMMENT '文字消息数',
    behavior_last_login DATE COMMENT '最后登录时间(YYYY-MM-DD)',
    behavior_preferred_topic VARCHAR(100) COMMENT '高频讨论话题',
    -- behavior_preferred_topic_2 VARCHAR(100) COMMENT '高频讨论话题2',
    behavior_communication_style VARCHAR(20) COMMENT '沟通偏好(语音/文字/视频等)',
    
    -- 人生大事记 (保留最近2件大事)
    life_event_date DATE COMMENT '事件日期(YYYY-MM-DD)',
    life_event_description VARCHAR(200) COMMENT '事件描述',
    life_event_importance VARCHAR(20) COMMENT '事件重要程度(高/中/低)',
    life_event_sentiment VARCHAR(20) COMMENT '事件情感倾向(积极/中性/消极等)',
    life_event_type VARCHAR(50) COMMENT '事件类型',
    -- life_event_2_date DATE COMMENT '事件2日期(YYYY-MM-DD)',
    -- life_event_2_description VARCHAR(200) COMMENT '事件2描述',
    -- life_event_2_importance VARCHAR(20) COMMENT '事件2重要程度(高/中/低)',
    -- life_event_2_sentiment VARCHAR(20) COMMENT '事件2情感倾向(积极/中性/消极)',
    -- life_event_2_type VARCHAR(50) COMMENT '事件2类型',
    
    -- 梦想与理想
    aspiration_dream VARCHAR(500) COMMENT '长期梦想描述',
    aspiration_ideal VARCHAR(500) COMMENT '短期理想目标',
    aspiration_action_date DATE COMMENT '行动日期(YYYY-MM-DD)',
    aspiration_action_description VARCHAR(200) COMMENT '行动描述',
    -- aspiration_action_2_date DATE COMMENT '行动2日期(YYYY-MM-DD)',
    -- aspiration_action_2_description VARCHAR(200) COMMENT '行动2描述',
    
    -- 人群标签 (保留最多5个标签)
    crowd_tag_name VARCHAR(50) COMMENT '标签名称',
    crowd_tag_confidence DECIMAL(3,2) COMMENT '标签置信度(0-1)',
    crowd_tag_source VARCHAR(50) COMMENT '标签来源(系统计算/行为分析/人工标注)',
    -- crowd_tag_2_name VARCHAR(50) COMMENT '标签2名称',
    -- crowd_tag_2_confidence DECIMAL(3,2) COMMENT '标签2置信度(0-1)',
    -- crowd_tag_2_source VARCHAR(50) COMMENT '标签2来源(系统计算/行为分析/人工标注)',
    -- crowd_tag_3_name VARCHAR(50) COMMENT '标签3名称',
    -- crowd_tag_3_confidence DECIMAL(3,2) COMMENT '标签3置信度(0-1)',
    -- crowd_tag_3_source VARCHAR(50) COMMENT '标签3来源(系统计算/行为分析/人工标注)',
    -- crowd_tag_4_name VARCHAR(50) COMMENT '标签4名称',
    -- crowd_tag_4_confidence DECIMAL(3,2) COMMENT '标签4置信度(0-1)',
    -- crowd_tag_4_source VARCHAR(50) COMMENT '标签4来源(系统计算/行为分析/人工标注)',
    -- crowd_tag_5_name VARCHAR(50) COMMENT '标签5名称',
    -- crowd_tag_5_confidence DECIMAL(3,2) COMMENT '标签5置信度(0-1)',
    -- crowd_tag_5_source VARCHAR(50) COMMENT '标签5来源(系统计算/行为分析/人工标注)',
    
    -- 消费特征
    consumption_financial_pressure VARCHAR(200) COMMENT '经济压力描述',
    consumption_travel_preference VARCHAR(50) COMMENT '旅行消费档次',
    consumption_entertainment_preference VARCHAR(50) COMMENT '娱乐消费偏好',
    consumption_health_preference VARCHAR(50) COMMENT '健康消费偏好',
    
    -- 系统信息
    profile_score DECIMAL(5,2) COMMENT '用户画像完整度评分(0-100分)',
    data_version VARCHAR(20) COMMENT '数据版本号',  
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录更新时间',
	
    PRIMARY KEY (id, stat_date),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户画像汇总表(按天分区，宽表结构)'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202507 VALUES LESS THAN (TO_DAYS('2025-07-01')) COMMENT '2025年7月分区',
    PARTITION p202508 VALUES LESS THAN (TO_DAYS('2025-08-01')) COMMENT '2025年8月分区',
    PARTITION pmax VALUES LESS THAN MAXVALUE COMMENT '最大分区'
);