CREATE TABLE IF NOT EXISTS ads_user_profile_json_da (
    id BIGINT COMMENT '自增主键ID',
    user_id BIGINT COMMENT '用户唯一标识ID',
    user_name STRING COMMENT '用户姓名',
    
    -- 基础信息
    basic_info MAP<STRING, STRING> COMMENT '{
      "nickname": "用户昵称:都市精英,小镇青年等",
      "gender": "性别(男/女/未知)",
      "age": "实际年龄",
      "birth_date": "出生日期(YYYY-MM-DD HH:MM:SS)",
      "native_place": "{\"province\":\"籍贯省份\",\"city\":\"籍贯城市\"}",
      "current_city": "当前居住城市",
      "ip_location": "最近登录IP所在地",
      "marital_status": "婚姻状态(未婚/已婚/离异/丧偶)",
      "relationship_status": "感情状态(单身/恋爱中/已婚/离异)",
      "education": "最高学历(初中/高中/大专/本科/硕士/博士)",
      "income": "{\"annual\":\"年收入(万元)\",\"unit\":\"货币单位\",\"has_mortgage\":\"是否有房贷(true/false)\",\"has_car_loan\":\"是否有车贷(true/false)\"}",
      "city_tier": "城市等级(1-一线,2-二线,3-三线,4-四线及以下)"
    }',
    
    -- 职业信息
    career_info MAP<STRING, STRING> COMMENT '{
      "industry": "所属行业",
      "profession": "职业名称",
      "career_discussion_freq": "职业相关讨论频率(高频/中频/低频)",
      "career_concerns": "[\"职业关注点1\", \"职业关注点2\"]",
      "entrepreneur_status": "{\"is_entrepreneur\":\"是否创业者(true/false)\",\"stage\":\"创业阶段(初创期/成长期/成熟期)\",\"goal\":\"创业目标\"}"
    }',
    
    -- 家庭信息
    family_info MAP<STRING, STRING> COMMENT '{
      "family_type": "家庭类型(核心家庭/丁克家庭/单身家庭/三代同堂/单亲家庭/空巢家庭/合租群体)",
      "members": "[{\"relation\":\"家庭成员关系(父亲/母亲/配偶/子女等)\",\"age\":\"成员年龄\",\"health\":\"健康状况描述\",\"retired\":\"是否退休(true/false)\"}]",
      "elderly_care_needed": "是否需要养老照料(true/false)",
      "relationship_quality": "家庭关系质量(和睦/一般/紧张)"
    }',
    
    -- 兴趣爱好
    interest_info MAP<STRING, STRING> COMMENT '{
      "travel": "{\"preferences\":[\"喜欢的旅行方式1\",\"喜欢的旅行方式2\"],\"memorable_trips\":[\"印象深刻的旅行1\",\"印象深刻的旅行2\"],\"wishlist\":[\"想去的旅行目的地1\",\"想去的旅行目的地2\"],\"constraints\":{\"目的地\":\"未能前往的原因\"}}",
      "games": "{\"played\":[\"玩过的游戏1\",\"玩过的游戏2\"],\"preference_distribution\":{\"游戏类型\":\"偏好百分比\"},\"play_style\":\"游戏风格描述\"}",
      "sports": "{\"preferences\":[\"喜欢的运动项目1\",\"喜欢的运动项目2\"],\"genetic_analysis\":{\"运动能力\":\"基因检测结果\"}}"
    }',
    
    -- 健康信息
    health_info MAP<STRING, STRING> COMMENT '{
      "genetic_risks": "[{\"disease\":\"遗传疾病名称\",\"risk_level\":\"风险等级(高/中/低)\"}]",
      "allergies": "[\"过敏源1\",\"过敏源2\"]",
      "alcohol_reaction": "酒精反应描述",
      "nutrition_needs": "{\"营养元素\":\"需求程度\"}",
      "potential_issues": "[\"潜在健康问题1\",\"潜在健康问题2\"]"
    }',
    
    -- 行为特征
    behavior_info MAP<STRING, STRING> COMMENT '{
      "usage_pattern": "{\"frequency\":\"使用频率(高/中/低)\",\"total_conversations\":\"总对话数\",\"call_count\":\"语音通话次数\",\"text_count\":\"文字消息数\",\"last_login\":\"最后登录时间(YYYY-MM-DD)\"}",
      "preferred_topics": "[\"高频讨论话题1\",\"高频讨论话题2\"]",
      "communication_style": "沟通偏好(语音/文字/视频等)"
    }',
    
    -- 人生大事记
    life_events ARRAY<MAP<STRING, STRING>> COMMENT '[
      {
        "date": "事件日期(YYYY-MM-DD)",
        "event": "事件描述",
        "importance": "重要程度(高/中/低)",
        "sentiment": "情感倾向(积极/中性/消极等)",
        "type": "事件类型"
      }
    ]',
    
    -- 梦想与理想
    aspirations MAP<STRING, STRING> COMMENT '{
      "dream": "长期梦想描述",
      "ideal": "短期理想目标",
      "related_actions": "[{\"date\":\"行动日期(YYYY-MM-DD)\",\"action\":\"具体行动描述\"}]"
    }',
    
    -- 人群标签
    crowd_tags ARRAY<MAP<STRING, STRING>> COMMENT '[
      {
        "tag_name": "标签名称",
        "confidence": "置信度(0-1)",
        "source": "标签来源(系统计算/行为分析/人工标注)"
      }
    ]',
    
    -- 消费特征
    consumption_traits MAP<STRING, STRING> COMMENT '{
      "financial_pressure": "经济压力描述",
      "consumption_preferences": "{\"travel\":\"旅行消费档次\",\"entertainment\":\"娱乐消费偏好\",\"health\":\"健康消费偏好\"}"
    }',
    
    -- 系统信息
    profile_score DECIMAL(5,2) COMMENT '用户画像完整度评分(0-100分)，数值越高表示画像信息越完整',
    update_time TIMESTAMP COMMENT '记录更新时间',
    data_version STRING COMMENT '数据版本号，用于区分不同版本的画像数据'
)
COMMENT '用户画像汇总表'
PARTITIONED BY (dt STRING COMMENT '分区字段，格式为yyyy-MM-dd')
LOCATION '/user/hive/warehouse/ads.db/ads_user_profile_json_da';