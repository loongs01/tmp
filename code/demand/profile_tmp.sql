-- ods
CREATE TABLE `tb_ai_dialogue` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `user_id` bigint DEFAULT NULL COMMENT '用户ID',
  `questions` text COLLATE utf8mb4_general_ci COMMENT '提问',
  `answers` longtext COLLATE utf8mb4_general_ci COMMENT '回答',
  `questions_time` datetime DEFAULT NULL COMMENT '提问时间',
  `answers_time` datetime DEFAULT NULL COMMENT '回答时间',
  `type` smallint DEFAULT NULL COMMENT '1文字/2语音',
  `is_adopt` tinyint DEFAULT NULL COMMENT '1赞/2踩',
  `user_code` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '用户code',
  `session_id` varchar(64) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'websocket sessionID',
  `message_id` varchar(64) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '消息ID',
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`)
);

CREATE TABLE ods_user_raw_data (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    data_source VARCHAR(20) COMMENT '数据来源:chat/gene/test等',
    raw_data JSON COMMENT '原始数据(JSON格式)',
    data_time TIMESTAMP COMMENT '数据产生时间',
    create_time TIMESTAMP COMMENT '数据入库时间',
    is_processed BOOLEAN DEFAULT FALSE COMMENT '是否已处理'
) COMMENT '用户原始数据表';

CREATE TABLE ods_user_chat_logs (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT,
    chat_time TIMESTAMP,
    chat_type TINYINT COMMENT '1-文字,2-语音',
    content TEXT,
    topic_category VARCHAR(50),
    sentiment_score DECIMAL(3,1),
    create_time TIMESTAMP
) COMMENT '用户聊天记录表';

-- dwd

-- 基础信息清洗 (ods → dim_user_basic)
INSERT INTO dim_user_basic
SELECT 
    user_id,
    COALESCE(nickname, '未知') AS nickname,
    CASE gender 
        WHEN '男' THEN 1 
        WHEN '女' THEN 2 
        ELSE 0 
    END AS gender,
    -- 生日处理：修正明显错误（如2008年出生但年龄30岁）
    CASE 
        WHEN birth_date LIKE '2008%' AND age > 25 THEN 
            DATE_SUB(CURRENT_DATE, INTERVAL age YEAR)
        ELSE STR_TO_DATE(birth_date, '%Y.%m.%d')
    END AS birth_date,
    -- 年龄重新计算
    TIMESTAMPDIFF(YEAR, 
        CASE 
            WHEN birth_date LIKE '2008%' AND age > 25 THEN 
                DATE_SUB(CURRENT_DATE, INTERVAL age YEAR)
            ELSE STR_TO_DATE(birth_date, '%Y.%m.%d')
        END, 
        CURRENT_DATE) AS age,
    -- 地域信息标准化
    REGEXP_REPLACE(native_place, '中国|人|省|市', '') AS native_province,
    -- 收入标准化
    CASE 
        WHEN annual_income LIKE '%W%' THEN 
            CAST(REPLACE(annual_income, 'W', '') AS DECIMAL) 
        ELSE annual_income/10000 
    END AS annual_income,
    -- 其他字段处理...
    CURRENT_TIMESTAMP AS create_time,
    CURRENT_TIMESTAMP AS update_time
FROM ods_user_raw_data
WHERE data_source = 'profile';


-- 话题分类分析
WITH topic_analysis AS (
    SELECT 
        user_id,
        DATE(chat_time) AS stat_date,
        topic_category,
        COUNT(*) AS topic_count
    FROM ods_user_chat_logs
    WHERE topic_category IS NOT NULL
    GROUP BY user_id, DATE(chat_time), topic_category
)

INSERT INTO fact_user_behavior
SELECT 
    user_id,
    stat_date,
    SUM(CASE WHEN chat_type = 2 THEN 1 ELSE 0 END) AS voice_call_count,
    SUM(CASE WHEN chat_type = 1 THEN 1 ELSE 0 END) AS text_message_count,
    -- 高频话题提取
    (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'topic', topic_category,
                'count', topic_count,
                'percentage', ROUND(topic_count*100/total,1)
            )
        )
        FROM topic_analysis t
        WHERE t.user_id = l.user_id AND t.stat_date = DATE(l.chat_time)
        JOIN (SELECT SUM(topic_count) AS total FROM topic_analysis) totals
    ) AS frequent_topics,
    CURRENT_TIMESTAMP AS create_time
FROM ods_user_chat_logs l
GROUP BY user_id, DATE(chat_time);



INSERT INTO dim_user_health
SELECT 
    user_id,
    -- 遗传疾病风险解析
    (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'disease_name', REGEXP_SUBSTR(risk_item, '[^（]+'),
                'risk_level', CASE 
                    WHEN risk_item LIKE '%异常项%' THEN '高'
                    ELSE '中'
                END
            )
        )
        FROM JSON_TABLE(
            REPLACE(raw_data->'$.health_risks', '，', ','),
            '$[*]' COLUMNS(risk_item VARCHAR(100) PATH '$'
        ) risks
    ) AS genetic_disease_risks,
    
    -- 过敏信息处理
    JSON_OBJECT(
        'milk_allergy', IF(raw_data->'$.allergies' LIKE '%牛奶过敏%', TRUE, FALSE),
        'shrimp_allergy', IF(raw_data->'$.allergies' LIKE '%虾过敏%', TRUE, FALSE),
        'alcohol_reaction', IF(raw_data->'$.allergies' LIKE '%喝酒脸红%', TRUE, FALSE)
    ) AS allergy_info,
    
    CURRENT_TIMESTAMP AS update_time
FROM ods_user_raw_data
WHERE data_source = 'gene_test';



-- 都市精英标签
INSERT INTO fact_user_tags
SELECT 
    u.user_id,
    (SELECT tag_id FROM dim_tag_definition WHERE tag_name = '都市精英') AS tag_id,
    'system' AS tag_source,
    CASE 
        WHEN u.age BETWEEN 25 AND 35 
             AND u.city_tier IN (1,2) 
             AND u.annual_income >= 20 THEN 0.9
        ELSE 0.6
    END AS confidence_score,
    CURRENT_DATE AS effective_date,
    NULL AS expiry_date,
    CURRENT_TIMESTAMP AS create_time
FROM dim_user_basic u
JOIN dim_user_career c ON u.user_id = c.user_id
WHERE u.age BETWEEN 25 AND 35
AND u.city_tier IN (1,2)
AND u.annual_income >= 20
AND c.industry = '互联网';


-- 用户画像宽表全量刷新
INSERT OVERWRITE TABLE dws_user_profile
SELECT 
    u.user_id,
    JSON_OBJECT(
        'basic', JSON_OBJECT(
            'name', u.nickname,
            'demographic', JSON_OBJECT(
                'age', u.age,
                'gender', CASE u.gender WHEN 1 THEN '男' ELSE '女' END,
                'location', CONCAT(u.current_province, '-', u.current_city),
                'income', u.annual_income
            ),
            'education', CASE u.education_level
                WHEN 1 THEN '初中及以下'
                WHEN 4 THEN '本科'
                -- 其他教育水平映射
            END
        ),
        'career', JSON_OBJECT(
            'industry', c.industry,
            'profession', c.profession,
            'is_entrepreneur', c.is_entrepreneur
        ),
        'health', JSON_OBJECT(
            'risk_factors', h.genetic_disease_risks,
            'allergies', h.allergy_info
        )
    ) AS basic_info,
    
    -- 人群标签数组
    (
        SELECT JSON_ARRAYAGG(t.tag_name)
        FROM fact_user_tags ut
        JOIN dim_tag_definition t ON ut.tag_id = t.tag_id
        WHERE ut.user_id = u.user_id
        AND t.tag_type = 'crowd'
        AND (ut.expiry_date IS NULL OR ut.expiry_date >= CURRENT_DATE)
    ) AS crowd_tags,
    
    -- 计算画像完整度评分
    (
        20 * IF(u.user_id IS NOT NULL, 1, 0) +
        15 * IF(c.user_id IS NOT NULL, 1, 0) +
        10 * IF(h.user_id IS NOT NULL, 1, 0) +
        -- 其他维度权重...
        5 * (SELECT COUNT(*) FROM fact_user_tags WHERE user_id = u.user_id)/10
    ) AS profile_score,
    
    CURRENT_TIMESTAMP AS update_time
FROM dim_user_basic u
LEFT JOIN dim_user_career c ON u.user_id = c.user_id
LEFT JOIN dim_user_health h ON u.user_id = h.user_id;



drop table if exists ads_user_profile_da;
create table if not exists ads_user_profile_da (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    stat_date DATE COMMENT '统计分析日期(分区字段)，格式为YYYY-MM-DD',
    user_id BIGINT COMMENT '用户唯一标识ID',
    user_name VARCHAR(32) COMMENT '用户姓名',
    
    -- 基础信息
    basic_info JSON COMMENT '{
      "nickname": "用户昵称:都市精英,小镇青年等",
      "gender": "性别(男/女/未知)",
      "age": "实际年龄",
      "birth_date": "出生日期(YYYY-MM-DD HH:MM:SS)",
      "native_place": {"province": "籍贯省份", "city": "籍贯城市"},
      "current_city": "当前居住城市",
      "ip_location": "最近登录IP所在地",
      "marital_status": "婚姻状态(未婚/已婚/离异/丧偶)",
      "relationship_status": "感情状态(单身/恋爱中/已婚/离异)",
      "education": "最高学历(初中/高中/大专/本科/硕士/博士)",
      "income":"收入" {
        "annual": "年收入(万元)",
        "unit": "货币单位",
        "has_mortgage": "是否有房贷(true/false)",
        "has_car_loan": "是否有车贷(true/false)"
      },
      "city_tier": "城市等级(1-一线,2-二线,3-三线,4-四线及以下)"
    }',
    
    -- 职业信息
    career_info JSON COMMENT '{
      "industry": "所属行业",
      "profession": "职业名称",
      "career_discussion_freq": "职业相关讨论频率(高频/中频/低频)",
      "career_concerns": ["职业关注点1", "职业关注点2"],
      "entrepreneur_status": {
        "is_entrepreneur": "是否创业者(true/false)",
        "stage": "创业阶段(初创期/成长期/成熟期)",
        "goal": "创业目标"
      }
    }',
    
    -- 家庭信息
    family_info JSON COMMENT '{
      "family_type": "家庭类型(核心家庭/丁克家庭/单身家庭/三代同堂/单亲家庭/空巢家庭/合租群体)",
      "members": [
        {
          "relation": "家庭成员关系(父亲/母亲/配偶/子女等)",
          "age": "成员年龄",
          "health": "健康状况描述",
          "retired": "是否退休(true/false)"
        }
      ],
      "elderly_care_needed": "是否需要养老照料(true/false)",
      "relationship_quality": "家庭关系质量(和睦/一般/紧张)"
    }',
    
    -- 兴趣爱好
    interest_info JSON COMMENT '{
      "travel": {
        "preferences": ["喜欢的旅行方式1", "喜欢的旅行方式2"],
        "memorable_trips": ["印象深刻的旅行1", "印象深刻的旅行2"],
        "wishlist": ["想去的旅行目的地1", "想去的旅行目的地2"],
        "constraints": {"目的地": "未能前往的原因"}
      },
      "games": {
        "played": ["玩过的游戏1", "玩过的游戏2"],
        "preference_distribution": {"游戏类型": "偏好百分比"},
        "play_style": "游戏风格描述"
      },
      "sports": {
        "preferences": ["喜欢的运动项目1", "喜欢的运动项目2"],
        "genetic_analysis": {"运动能力": "基因检测结果"}
      }
    }',
    
    -- 健康信息
    health_info JSON COMMENT '{
      "genetic_risks": [
        {
          "disease": "遗传疾病名称",
          "risk_level": "风险等级(高/中/低)"
        }
      ],
      "allergies": ["过敏源1", "过敏源2"],
      "alcohol_reaction": "酒精反应描述",
      "nutrition_needs": {"营养元素": "需求程度"},
      "potential_issues": ["潜在健康问题1", "潜在健康问题2"]
    }',
    
    -- 行为特征
    behavior_info JSON COMMENT '{
      "usage_pattern": {
        "frequency": "使用频率(高/中/低)",
        "total_conversations": "总对话数",
        "call_count": "语音通话次数",
        "text_count": "文字消息数",
        "last_login": "最后登录时间(YYYY-MM-DD)"
      },
      "preferred_topics": ["高频讨论话题1", "高频讨论话题2"],
      "communication_style": "沟通偏好(语音/文字/视频等)"
    }',
    
    -- 人生大事记
    life_events JSON COMMENT '[
      {
        "date": "事件日期(YYYY-MM-DD)",
        "event": "事件描述",
        "importance": "重要程度(高/中/低)",
        "sentiment": "情感倾向(积极/中性/消极等)",
        "type": "事件类型"
      }
    ]',
    
    -- 梦想与理想
    aspirations JSON COMMENT '{
      "dream": "长期梦想描述",
      "ideal": "短期理想目标",
      "related_actions": [
        {
          "date": "行动日期(YYYY-MM-DD)",
          "action": "具体行动描述"
        }
      ]
    }',
    
    -- 人群标签
    crowd_tags JSON COMMENT '[
      {
        "tag_name": "标签名称",
        "confidence": "置信度(0-1)",
        "source": "标签来源(系统计算/行为分析/人工标注)"
      }
    ]',
    
    -- 消费特征
    consumption_traits JSON COMMENT '{
      "financial_pressure": "经济压力描述",
      "consumption_preferences": {
        "travel": "旅行消费档次",
        "entertainment": "娱乐消费偏好",
        "health": "健康消费偏好"
      }
    }',
    
    -- 系统信息
    profile_score DECIMAL(5,2) COMMENT '用户画像完整度评分(0-100分)，数值越高表示画像信息越完整',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录更新时间',
    data_version VARCHAR(20) COMMENT '数据版本号，用于区分不同版本的画像数据',  
    PRIMARY KEY (id, stat_date),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户画像汇总表(按天分区)'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202507 VALUES LESS THAN (TO_DAYS('2025-07-01')) COMMENT '2025年7月分区',
    PARTITION p202508 VALUES LESS THAN (TO_DAYS('2025-08-01')) COMMENT '2025年8月分区',
    PARTITION pmax VALUES LESS THAN MAXVALUE COMMENT '最大分区'
);

-- 创建JSON字段索引
CREATE INDEX idx_basic_city ON ads_user_profile_da((JSON_EXTRACT(basic_info, '$.current_city')));
CREATE INDEX idx_career_industry ON ads_user_profile_da((JSON_EXTRACT(career_info, '$.industry')));
CREATE INDEX idx_crowd_tags ON ads_user_profile_da((JSON_EXTRACT(crowd_tags, '$[*].tag_name')));