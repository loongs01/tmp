-- 用户画像汇总表每日加工脚本 - 宽表版本
-- INSERT INTO ads_user_profile_da_wide (
--     stat_date,
--     user_id,
--     user_name,
--     
--     -- 基础信息
--     basic_nickname,
--     basic_gender,
--     basic_age,
--     basic_birth_date,
--     basic_native_province,
--     basic_native_city,
--     basic_current_city,
--     basic_ip_location,
--     basic_marital_status,
--     basic_relationship_status,
--     basic_education,
--     basic_income_annual,
--     basic_income_unit,
--     basic_has_mortgage,
--     basic_has_car_loan,
--     basic_city_tier,
--     
--     -- 职业信息
--     career_industry,
--     career_profession,
--     career_discussion_freq,
--     career_concerns_1,
--     career_concerns_2,
--     career_is_entrepreneur,
--     career_entrepreneur_stage,
--     career_entrepreneur_goal,
--     
--     -- 家庭信息
--     family_type,
--     family_member_1_relation,
--     family_member_1_age,
--     family_member_1_health,
--     family_member_1_retired,
--     family_member_2_relation,
--     family_member_2_age,
--     family_member_2_health,
--     family_member_2_retired,
--     family_elderly_care_needed,
--     family_relationship_quality,
--     
--     -- 兴趣爱好 - 旅行
--     interest_travel_preference_1,
--     interest_travel_preference_2,
--     interest_travel_memorable_1,
--     interest_travel_memorable_2,
--     interest_travel_wishlist_1,
--     interest_travel_wishlist_2,
--     interest_travel_constraint_destination,
--     
--     -- 兴趣爱好 - 游戏
--     interest_games_played_1,
--     interest_games_played_2,
--     interest_games_preference_type,
--     interest_games_preference_percent,
--     interest_games_play_style,
--     
--     -- 兴趣爱好 - 运动
--     interest_sports_preference_1,
--     interest_sports_preference_2,
--     interest_sports_genetic_ability,
--     
--     -- 健康信息
--     health_genetic_risk_1_disease,
--     health_genetic_risk_1_level,
--     health_genetic_risk_2_disease,
--     health_genetic_risk_2_level,
--     health_allergy_1,
--     health_allergy_2,
--     health_alcohol_reaction,
--     health_nutrition_need_element,
--     health_nutrition_need_level,
--     health_potential_issue_1,
--     health_potential_issue_2,
--     
--     -- 行为特征
--     behavior_usage_frequency,
--     behavior_total_conversations,
--     behavior_call_count,
--     behavior_text_count,
--     behavior_last_login,
--     behavior_preferred_topic_1,
--     behavior_preferred_topic_2,
--     behavior_communication_style,
--     
--     -- 人生大事记
--     life_event_1_date,
--     life_event_1_description,
--     life_event_1_importance,
--     life_event_1_sentiment,
--     life_event_1_type,
--     life_event_2_date,
--     life_event_2_description,
--     life_event_2_importance,
--     life_event_2_sentiment,
--     life_event_2_type,
--     
--     -- 梦想与理想
--     aspiration_dream,
--     aspiration_ideal,
--     aspiration_action_1_date,
--     aspiration_action_1_description,
--     aspiration_action_2_date,
--     aspiration_action_2_description,
--     
--     -- 人群标签
--     crowd_tag_1_name,
--     crowd_tag_1_confidence,
--     crowd_tag_1_source,
--     crowd_tag_2_name,
--     crowd_tag_2_confidence,
--     crowd_tag_2_source,
--     crowd_tag_3_name,
--     crowd_tag_3_confidence,
--     crowd_tag_3_source,
--     crowd_tag_4_name,
--     crowd_tag_4_confidence,
--     crowd_tag_4_source,
--     crowd_tag_5_name,
--     crowd_tag_5_confidence,
--     crowd_tag_5_source,
--     
--     -- 消费特征
--     consumption_financial_pressure,
--     consumption_travel_preference,
--     consumption_entertainment_preference,
--     consumption_health_preference,
--     
--     -- 系统信息
--     profile_score,
--     data_version
-- )
WITH 
-- 基础信息整合
user_basic AS (
    SELECT
        user_id,
        stat_date,
        user_name,
        nickname AS basic_nickname,
        CASE gender 
            WHEN 1 THEN '男' 
            WHEN 2 THEN '女' 
            ELSE '未知' 
        END AS basic_gender,
        age AS basic_age,
        birth_date AS basic_birth_date,
        native_province AS basic_native_province,
        native_city AS basic_native_city,
        current_city AS basic_current_city,
        ip_location AS basic_ip_location,
        CASE marital_status
            WHEN 1 THEN '未婚'
            WHEN 2 THEN '已婚'
            WHEN 3 THEN '离异'
            WHEN 4 THEN '丧偶'
            ELSE NULL
        END AS basic_marital_status,
        CASE relationship_status
            WHEN 1 THEN '单身'
            WHEN 2 THEN '恋爱中'
            WHEN 3 THEN '已婚'
            WHEN 4 THEN '离异'
            ELSE NULL
        END AS basic_relationship_status,
        CASE education_level
            WHEN 1 THEN '初中及以下'
            WHEN 2 THEN '高中'
            WHEN 3 THEN '大专'
            WHEN 4 THEN '本科'
            WHEN 5 THEN '硕士'
            WHEN 6 THEN '博士'
            ELSE NULL
        END AS basic_education,
        annual_income AS basic_income_annual,
        '万元' AS basic_income_unit,
        has_mortgage AS basic_has_mortgage,
        has_car_loan AS basic_has_car_loan,
        city_tier AS basic_city_tier
    FROM dwd_user_basic_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 职业信息整合
user_career AS (
    SELECT
        user_id,
        stat_date,
        industry AS career_industry,
        profession AS career_profession,
        career_discussion_freq AS career_discussion_freq,
        -- 取前两个职业关注点
        JSON_UNQUOTE(JSON_EXTRACT(career_concerns, '$[0]')) AS career_concerns_1,
        JSON_UNQUOTE(JSON_EXTRACT(career_concerns, '$[1]')) AS career_concerns_2,
        is_entrepreneur AS career_is_entrepreneur,
        entrepreneurial_stage AS career_entrepreneur_stage,
        entrepreneurial_goal AS career_entrepreneur_goal
    FROM dwd_user_career_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 家庭信息整合
user_family AS (
    SELECT
        user_id,
        stat_date,
        family_type AS family_type,
        -- 取前两个家庭成员
        JSON_UNQUOTE(JSON_EXTRACT(family_members, '$[0].relation')) AS family_member_1_relation,
        JSON_EXTRACT(family_members, '$[0].age') AS family_member_1_age,
        JSON_UNQUOTE(JSON_EXTRACT(family_members, '$[0].health')) AS family_member_1_health,
        JSON_EXTRACT(family_members, '$[0].retired') AS family_member_1_retired,
        JSON_UNQUOTE(JSON_EXTRACT(family_members, '$[1].relation')) AS family_member_2_relation,
        JSON_EXTRACT(family_members, '$[1].age') AS family_member_2_age,
        JSON_UNQUOTE(JSON_EXTRACT(family_members, '$[1].health')) AS family_member_2_health,
        JSON_EXTRACT(family_members, '$[1].retired') AS family_member_2_retired,
        elderly_care_needed AS family_elderly_care_needed,
        relationship_quality AS family_relationship_quality
    FROM dwd_user_family_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 兴趣爱好整合 - 旅行
user_interest_travel AS (
    SELECT
        user_id,
        stat_date,
        -- 旅行偏好
        JSON_UNQUOTE(JSON_EXTRACT(travel_preferences, '$[0]')) AS interest_travel_preference_1,
        JSON_UNQUOTE(JSON_EXTRACT(travel_preferences, '$[1]')) AS interest_travel_preference_2,
        -- 难忘旅行
        JSON_UNQUOTE(JSON_EXTRACT(memorable_trips, '$[0]')) AS interest_travel_memorable_1,
        JSON_UNQUOTE(JSON_EXTRACT(memorable_trips, '$[1]')) AS interest_travel_memorable_2,
        -- 愿望清单
        JSON_UNQUOTE(JSON_EXTRACT(travel_wishlist, '$[0]')) AS interest_travel_wishlist_1,
        JSON_UNQUOTE(JSON_EXTRACT(travel_wishlist, '$[1]')) AS interest_travel_wishlist_2,
        -- 旅行约束
        JSON_UNQUOTE(JSON_EXTRACT(JSON_KEYS(travel_constraints), '$[0]')) AS interest_travel_constraint_destination
    FROM dwd_user_interest_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 兴趣爱好整合 - 游戏
user_interest_games AS (
    SELECT
        user_id,
        stat_date,
        -- 玩过的游戏
        JSON_UNQUOTE(JSON_EXTRACT(games_played, '$[0]')) AS interest_games_played_1,
        JSON_UNQUOTE(JSON_EXTRACT(games_played, '$[1]')) AS interest_games_played_2,
        -- 游戏偏好分布 (取第一个)
        JSON_UNQUOTE(JSON_EXTRACT(JSON_KEYS(game_preferences), '$[0]')) AS interest_games_preference_type,
        -- JSON_EXTRACT(game_preferences, '$.*') AS interest_games_preference_percent,
		30 as interest_games_preference_percent,
        game_play_style AS interest_games_play_style
    FROM dwd_user_interest_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 兴趣爱好整合 - 运动
user_interest_sports AS (
    SELECT
        user_id,
        stat_date,
		sports_preferences,
		genetic_sports_ability
        -- 运动偏好
        -- JSON_UNQUOTE(JSON_EXTRACT(sports_preferences, '$[0]')) AS interest_sports_preference_1,
        -- JSON_UNQUOTE(JSON_EXTRACT(sports_preferences, '$[1]')) AS interest_sports_preference_2,
        -- 运动能力基因分析 (取第一个)
        -- JSON_UNQUOTE(JSON_EXTRACT(JSON_KEYS(genetic_sports_ability), '$[0]')) AS interest_sports_genetic_ability_element,
        -- JSON_UNQUOTE(JSON_EXTRACT(genetic_sports_ability, '$.*')) AS interest_sports_genetic_ability
    FROM dwd_user_interest_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 健康信息整合
user_health AS (
    SELECT
        user_id,
        stat_date,
        -- 遗传风险 (取前两个)
        JSON_UNQUOTE(JSON_EXTRACT(genetic_risks, '$[0].disease')) AS health_genetic_risk_1_disease,
        JSON_UNQUOTE(JSON_EXTRACT(genetic_risks, '$[0].risk_level')) AS health_genetic_risk_1_level,
        JSON_UNQUOTE(JSON_EXTRACT(genetic_risks, '$[1].disease')) AS health_genetic_risk_2_disease,
        JSON_UNQUOTE(JSON_EXTRACT(genetic_risks, '$[1].risk_level')) AS health_genetic_risk_2_level,
        -- 过敏源 (取前两个)
        JSON_UNQUOTE(JSON_EXTRACT(allergies, '$[0]')) AS health_allergy_1,
        JSON_UNQUOTE(JSON_EXTRACT(allergies, '$[1]')) AS health_allergy_2,
        alcohol_reaction AS health_alcohol_reaction,
        -- 营养需求 (取第一个)
        JSON_UNQUOTE(JSON_EXTRACT(JSON_KEYS(nutrition_needs), '$[0]')) AS health_nutrition_need_element,
        JSON_UNQUOTE(JSON_EXTRACT(nutrition_needs, '$.*')) AS health_nutrition_need_level,
        -- 潜在健康问题 (取前两个)
        JSON_UNQUOTE(JSON_EXTRACT(potential_diseases, '$[0]')) AS health_potential_issue_1,
        JSON_UNQUOTE(JSON_EXTRACT(potential_diseases, '$[1]')) AS health_potential_issue_2
    FROM dwd_user_health_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 行为特征整合
user_behavior AS (
    SELECT
        user_id,
        stat_date,
        usage_frequency AS behavior_usage_frequency,
        total_conversations AS behavior_total_conversations,
        voice_call_count AS behavior_call_count,
        text_message_count AS behavior_text_count,
        DATE(last_login_time) AS behavior_last_login,
        -- 高频话题 (取前两个)
        JSON_UNQUOTE(JSON_EXTRACT(preferred_topics, '$[0]')) AS behavior_preferred_topic_1,
        JSON_UNQUOTE(JSON_EXTRACT(preferred_topics, '$[1]')) AS behavior_preferred_topic_2,
        communication_style AS behavior_communication_style
    FROM dwd_user_behavior_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 人生大事记整合 (取最近两件)
user_life_events AS (
    SELECT 
        user_id,
        stat_date,
        -- 第一件大事
        DATE(MAX(CASE WHEN rn = 1 THEN event_date END)) AS life_event_1_date,
        MAX(CASE WHEN rn = 1 THEN event_content END) AS life_event_1_description,
        MAX(CASE WHEN rn = 1 THEN 
            CASE importance_level
                WHEN 1 THEN '低'
                WHEN 2 THEN '中低'
                WHEN 3 THEN '中'
                WHEN 4 THEN '中高'
                WHEN 5 THEN '高'
                ELSE NULL
            END 
        END) AS life_event_1_importance,
        MAX(CASE WHEN rn = 1 THEN 
            CASE emotional_tone
                WHEN 1 THEN '消极'
                WHEN 2 THEN '中性'
                WHEN 3 THEN '积极'
                ELSE NULL
            END 
        END) AS life_event_1_sentiment,
        MAX(CASE WHEN rn = 1 THEN event_type END) AS life_event_1_type,
        -- 第二件大事
        DATE(MAX(CASE WHEN rn = 2 THEN event_date END)) AS life_event_2_date,
        MAX(CASE WHEN rn = 2 THEN event_content END) AS life_event_2_description,
        MAX(CASE WHEN rn = 2 THEN 
            CASE importance_level
                WHEN 1 THEN '低'
                WHEN 2 THEN '中低'
                WHEN 3 THEN '中'
                WHEN 4 THEN '中高'
                WHEN 5 THEN '高'
                ELSE NULL
            END 
        END) AS life_event_2_importance,
        MAX(CASE WHEN rn = 2 THEN 
            CASE emotional_tone
                WHEN 1 THEN '消极'
                WHEN 2 THEN '中性'
                WHEN 3 THEN '积极'
                ELSE NULL
            END 
        END) AS life_event_2_sentiment,
        MAX(CASE WHEN rn = 2 THEN event_type END) AS life_event_2_type
    FROM (
        SELECT 
            user_id,
            stat_date,
            event_date,
            event_content,
            importance_level,
            emotional_tone,
            event_type,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date DESC) AS rn
        FROM dwd_user_life_events_di
        -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
    ) t
    WHERE rn <= 2
    GROUP BY user_id, stat_date
),

-- 梦想与理想整合
user_aspirations AS (
    SELECT
        user_id,
        stat_date,
        dream AS aspiration_dream,
        ideal AS aspiration_ideal,
        -- 相关行动 (取最近两个)
        DATE(MAX(CASE WHEN rn = 1 THEN action_date END)) AS aspiration_action_1_date,
        MAX(CASE WHEN rn = 1 THEN action_description END) AS aspiration_action_1_description,
        DATE(MAX(CASE WHEN rn = 2 THEN action_date END)) AS aspiration_action_2_date,
        MAX(CASE WHEN rn = 2 THEN action_description END) AS aspiration_action_2_description
    FROM (
        SELECT 
            user_id,
            stat_date,
            dream,
            ideal,
            action_date,
            action_description,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY action_date DESC) AS rn
        FROM dwd_user_aspiration_info_di, JSON_TABLE(related_actions, '$[*]' COLUMNS (
            action_date DATE PATH '$.date',
            action_description VARCHAR(200) PATH '$.action'
        )) AS actions
        -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
    ) t
    GROUP BY user_id, stat_date
),

-- 消费特征整合
user_consumption AS (
    SELECT
        user_id,
        stat_date,
        financial_pressure AS consumption_financial_pressure,
        travel_consumption_level AS consumption_travel_preference,
        entertainment_consumption_level AS consumption_entertainment_preference,
        health_consumption_level AS consumption_health_preference
    FROM dwd_user_consumption_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 人群标签整合 (取前5个标签)
user_crowd_tags AS (
    SELECT
        user_id,
        stat_date,
        MAX(CASE WHEN rn = 1 THEN tag_name END) AS crowd_tag_1_name,
        MAX(CASE WHEN rn = 1 THEN confidence_score END) AS crowd_tag_1_confidence,
        MAX(CASE WHEN rn = 1 THEN tag_source END) AS crowd_tag_1_source,
        MAX(CASE WHEN rn = 2 THEN tag_name END) AS crowd_tag_2_name,
        MAX(CASE WHEN rn = 2 THEN confidence_score END) AS crowd_tag_2_confidence,
        MAX(CASE WHEN rn = 2 THEN tag_source END) AS crowd_tag_2_source,
        MAX(CASE WHEN rn = 3 THEN tag_name END) AS crowd_tag_3_name,
        MAX(CASE WHEN rn = 3 THEN confidence_score END) AS crowd_tag_3_confidence,
        MAX(CASE WHEN rn = 3 THEN tag_source END) AS crowd_tag_3_source,
        MAX(CASE WHEN rn = 4 THEN tag_name END) AS crowd_tag_4_name,
        MAX(CASE WHEN rn = 4 THEN confidence_score END) AS crowd_tag_4_confidence,
        MAX(CASE WHEN rn = 4 THEN tag_source END) AS crowd_tag_4_source,
        MAX(CASE WHEN rn = 5 THEN tag_name END) AS crowd_tag_5_name,
        MAX(CASE WHEN rn = 5 THEN confidence_score END) AS crowd_tag_5_confidence,
        MAX(CASE WHEN rn = 5 THEN tag_source END) AS crowd_tag_5_source
    FROM (
        SELECT 
            r.user_id,
            r.stat_date,
            d.tag_name,
            r.confidence_score,
            r.tag_source,
            ROW_NUMBER() OVER (PARTITION BY r.user_id ORDER BY r.confidence_score DESC) AS rn
        FROM dwd_user_tag_relation_di r
        JOIN dim_tag_definition_di d ON r.tag_id = d.tag_id
        -- WHERE r.stat_date = CURRENT_DATE - INTERVAL 1 DAY
        -- AND d.tag_type = '人群'
    ) t
    WHERE rn <= 5
    GROUP BY user_id, stat_date
),

-- 画像完整度评分计算
profile_score AS (
    SELECT
        base.user_id,
        base.stat_date,
        -- 计算各维度完整度得分（每维度10分，满分100）
        (CASE WHEN b.user_id IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN c.user_id IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN f.user_id IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN i.user_id IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN h.user_id IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN be.user_id IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN l.user_id IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN a.user_id IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN co.user_id IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN ct.user_id IS NOT NULL THEN 10 ELSE 0 END) AS completeness_score
    FROM 
        (SELECT DISTINCT user_id, stat_date FROM dwd_user_basic_info_di 
        -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
        ) base
    LEFT JOIN user_basic b ON base.user_id = b.user_id AND base.stat_date = b.stat_date
    LEFT JOIN user_career c ON base.user_id = c.user_id AND base.stat_date = c.stat_date
    LEFT JOIN user_family f ON base.user_id = f.user_id AND base.stat_date = f.stat_date
    LEFT JOIN user_interest_travel i ON base.user_id = i.user_id AND base.stat_date = i.stat_date
    LEFT JOIN user_health h ON base.user_id = h.user_id AND base.stat_date = h.stat_date
    LEFT JOIN user_behavior be ON base.user_id = be.user_id AND base.stat_date = be.stat_date
    LEFT JOIN user_life_events l ON base.user_id = l.user_id AND base.stat_date = l.stat_date
    LEFT JOIN user_aspirations a ON base.user_id = a.user_id AND base.stat_date = a.stat_date
    LEFT JOIN user_consumption co ON base.user_id = co.user_id AND base.stat_date = co.stat_date
    LEFT JOIN user_crowd_tags ct ON base.user_id = ct.user_id AND base.stat_date = ct.stat_date
)

-- 最终结果汇总
SELECT
    CURRENT_DATE - INTERVAL 1 DAY AS stat_date,
    b.user_id,
    b.user_name,
    
    -- 基础信息
    b.basic_nickname,
    b.basic_gender,
    b.basic_age,
    b.basic_birth_date,
    b.basic_native_province,
    b.basic_native_city,
    b.basic_current_city,
    b.basic_ip_location,
    b.basic_marital_status,
    b.basic_relationship_status,
    b.basic_education,
    b.basic_income_annual,
    b.basic_income_unit,
    b.basic_has_mortgage,
    b.basic_has_car_loan,
    b.basic_city_tier,
    
    -- 职业信息
    COALESCE(c.career_industry, '') AS career_industry,
    COALESCE(c.career_profession, '') AS career_profession,
    COALESCE(c.career_discussion_freq, '') AS career_discussion_freq,
    COALESCE(c.career_concerns_1, '') AS career_concerns_1,
    COALESCE(c.career_concerns_2, '') AS career_concerns_2,
    COALESCE(c.career_is_entrepreneur, FALSE) AS career_is_entrepreneur,
    COALESCE(c.career_entrepreneur_stage, '') AS career_entrepreneur_stage,
    COALESCE(c.career_entrepreneur_goal, '') AS career_entrepreneur_goal,
    
    -- 家庭信息
    COALESCE(f.family_type, '') AS family_type,
    COALESCE(f.family_member_1_relation, '') AS family_member_1_relation,
    COALESCE(f.family_member_1_age, 0) AS family_member_1_age,
    COALESCE(f.family_member_1_health, '') AS family_member_1_health,
    COALESCE(f.family_member_1_retired, FALSE) AS family_member_1_retired,
    COALESCE(f.family_member_2_relation, '') AS family_member_2_relation,
    COALESCE(f.family_member_2_age, 0) AS family_member_2_age,
    COALESCE(f.family_member_2_health, '') AS family_member_2_health,
    COALESCE(f.family_member_2_retired, FALSE) AS family_member_2_retired,
    COALESCE(f.family_elderly_care_needed, FALSE) AS family_elderly_care_needed,
    COALESCE(f.family_relationship_quality, '') AS family_relationship_quality,
    
    -- 兴趣爱好 - 旅行
    COALESCE(it.interest_travel_preference_1, '') AS interest_travel_preference_1,
    COALESCE(it.interest_travel_preference_2, '') AS interest_travel_preference_2,
    COALESCE(it.interest_travel_memorable_1, '') AS interest_travel_memorable_1,
    COALESCE(it.interest_travel_memorable_2, '') AS interest_travel_memorable_2,
    COALESCE(it.interest_travel_wishlist_1, '') AS interest_travel_wishlist_1,
    COALESCE(it.interest_travel_wishlist_2, '') AS interest_travel_wishlist_2,
    COALESCE(it.interest_travel_constraint_destination, '') AS interest_travel_constraint_destination,
    
    -- 兴趣爱好 - 游戏
    COALESCE(ig.interest_games_played_1, '') AS interest_games_played_1,
    COALESCE(ig.interest_games_played_2, '') AS interest_games_played_2,
    COALESCE(ig.interest_games_preference_type, '') AS interest_games_preference_type,
    COALESCE(ig.interest_games_preference_percent, 0) AS interest_games_preference_percent,
    COALESCE(ig.interest_games_play_style, '') AS interest_games_play_style,
    
    -- 兴趣爱好 - 运动
    COALESCE(isport.sports_preferences, '') AS interest_sports_preference_1,
    COALESCE(isport.sports_preferences, '') AS interest_sports_preference_2,
    COALESCE(isport.genetic_sports_ability, '') AS interest_sports_genetic_ability,
    
    -- 健康信息
    COALESCE(h.health_genetic_risk_1_disease, '') AS health_genetic_risk_1_disease,
    COALESCE(h.health_genetic_risk_1_level, '') AS health_genetic_risk_1_level,
    COALESCE(h.health_genetic_risk_2_disease, '') AS health_genetic_risk_2_disease,
    COALESCE(h.health_genetic_risk_2_level, '') AS health_genetic_risk_2_level,
    COALESCE(h.health_allergy_1, '') AS health_allergy_1,
    COALESCE(h.health_allergy_2, '') AS health_allergy_2,
    COALESCE(h.health_alcohol_reaction, '') AS health_alcohol_reaction,
    COALESCE(h.health_nutrition_need_element, '') AS health_nutrition_need_element,
    COALESCE(h.health_nutrition_need_level, '') AS health_nutrition_need_level,
    COALESCE(h.health_potential_issue_1, '') AS health_potential_issue_1,
    COALESCE(h.health_potential_issue_2, '') AS health_potential_issue_2,
    
    -- 行为特征
    COALESCE(be.behavior_usage_frequency, '') AS behavior_usage_frequency,
    COALESCE(be.behavior_total_conversations, 0) AS behavior_total_conversations,
    COALESCE(be.behavior_call_count, 0) AS behavior_call_count,
    COALESCE(be.behavior_text_count, 0) AS behavior_text_count,
    COALESCE(be.behavior_last_login, '1970-01-01') AS behavior_last_login,
    COALESCE(be.behavior_preferred_topic_1, '') AS behavior_preferred_topic_1,
    COALESCE(be.behavior_preferred_topic_2, '') AS behavior_preferred_topic_2,
    COALESCE(be.behavior_communication_style, '') AS behavior_communication_style,
    
    -- 人生大事记
    COALESCE(l.life_event_1_date, '1970-01-01') AS life_event_1_date,
    COALESCE(l.life_event_1_description, '') AS life_event_1_description,
    COALESCE(l.life_event_1_importance, '') AS life_event_1_importance,
    COALESCE(l.life_event_1_sentiment, '') AS life_event_1_sentiment,
    COALESCE(l.life_event_1_type, '') AS life_event_1_type,
    COALESCE(l.life_event_2_date, '1970-01-01') AS life_event_2_date,
    COALESCE(l.life_event_2_description, '') AS life_event_2_description,
    COALESCE(l.life_event_2_importance, '') AS life_event_2_importance,
    COALESCE(l.life_event_2_sentiment, '') AS life_event_2_sentiment,
    COALESCE(l.life_event_2_type, '') AS life_event_2_type,
    
    -- 梦想与理想
    COALESCE(a.aspiration_dream, '') AS aspiration_dream,
    COALESCE(a.aspiration_ideal, '') AS aspiration_ideal,
    COALESCE(a.aspiration_action_1_date, '1970-01-01') AS aspiration_action_1_date,
    COALESCE(a.aspiration_action_1_description, '') AS aspiration_action_1_description,
    COALESCE(a.aspiration_action_2_date, '1970-01-01') AS aspiration_action_2_date,
    COALESCE(a.aspiration_action_2_description, '') AS aspiration_action_2_description,
    
    -- 人群标签
    COALESCE(ct.crowd_tag_1_name, '') AS crowd_tag_1_name,
    COALESCE(ct.crowd_tag_1_confidence, 0) AS crowd_tag_1_confidence,
    COALESCE(ct.crowd_tag_1_source, '') AS crowd_tag_1_source,
    COALESCE(ct.crowd_tag_2_name, '') AS crowd_tag_2_name,
    COALESCE(ct.crowd_tag_2_confidence, 0) AS crowd_tag_2_confidence,
    COALESCE(ct.crowd_tag_2_source, '') AS crowd_tag_2_source,
    COALESCE(ct.crowd_tag_3_name, '') AS crowd_tag_3_name,
    COALESCE(ct.crowd_tag_3_confidence, 0) AS crowd_tag_3_confidence,
    COALESCE(ct.crowd_tag_3_source, '') AS crowd_tag_3_source,
    COALESCE(ct.crowd_tag_4_name, '') AS crowd_tag_4_name,
    COALESCE(ct.crowd_tag_4_confidence, 0) AS crowd_tag_4_confidence,
    COALESCE(ct.crowd_tag_4_source, '') AS crowd_tag_4_source,
    COALESCE(ct.crowd_tag_5_name, '') AS crowd_tag_5_name,
    COALESCE(ct.crowd_tag_5_confidence, 0) AS crowd_tag_5_confidence,
    COALESCE(ct.crowd_tag_5_source, '') AS crowd_tag_5_source,
    
    -- 消费特征
    COALESCE(co.consumption_financial_pressure, '') AS consumption_financial_pressure,
    COALESCE(co.consumption_travel_preference, '') AS consumption_travel_preference,
    COALESCE(co.consumption_entertainment_preference, '') AS consumption_entertainment_preference,
    COALESCE(co.consumption_health_preference, '') AS consumption_health_preference,
    
    -- 系统信息
    COALESCE(p.completeness_score, 0) AS profile_score,
    'v1.0' AS data_version
FROM user_basic b
LEFT JOIN user_career c ON b.user_id = c.user_id AND b.stat_date = c.stat_date
LEFT JOIN user_family f ON b.user_id = f.user_id AND b.stat_date = f.stat_date
LEFT JOIN user_interest_travel it ON b.user_id = it.user_id AND b.stat_date = it.stat_date
LEFT JOIN user_interest_games ig ON b.user_id = ig.user_id AND b.stat_date = ig.stat_date
LEFT JOIN user_interest_sports isport ON b.user_id = isport.user_id AND b.stat_date = isport.stat_date
LEFT JOIN user_health h ON b.user_id = h.user_id AND b.stat_date = h.stat_date
LEFT JOIN user_behavior be ON b.user_id = be.user_id AND b.stat_date = be.stat_date
LEFT JOIN user_life_events l ON b.user_id = l.user_id AND b.stat_date = l.stat_date
LEFT JOIN user_aspirations a ON b.user_id = a.user_id AND b.stat_date = a.stat_date
LEFT JOIN user_consumption co ON b.user_id = co.user_id AND b.stat_date = co.stat_date
LEFT JOIN user_crowd_tags ct ON b.user_id = ct.user_id AND b.stat_date = ct.stat_date
LEFT JOIN profile_score p ON b.user_id = p.user_id AND b.stat_date = p.stat_date;