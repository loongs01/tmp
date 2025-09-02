-- 用户画像汇总表每日加工脚本
-- INSERT INTO ads_user_profile_da (
    -- stat_date,
    -- user_id,
    -- user_name,
    -- basic_info,
    -- career_info,
    -- family_info,
    -- interest_info,
    -- health_info,
    -- behavior_info,
    -- life_events,
    -- aspirations,
    -- crowd_tags,
    -- consumption_traits,
    -- profile_score,
    -- data_version
-- )
WITH 
-- 基础信息整合
user_basic AS (
    SELECT
        user_id,
        stat_date,
        JSON_OBJECT(
		    'user_name',user_name,
            'nickname', nickname,
            'gender', CASE gender 
                         WHEN 1 THEN '男' 
                         WHEN 2 THEN '女' 
                         ELSE '未知' 
                      END,
            'age', age,
            'birth_date', DATE_FORMAT(birth_date, '%Y-%m-%d'),
            'native_place', JSON_OBJECT(
                'province', native_province,
                'city', native_city
            ),
            'current_city', current_city,
            'ip_location', ip_location,
            'marital_status', CASE marital_status
                                WHEN 1 THEN '未婚'
                                WHEN 2 THEN '已婚'
                                WHEN 3 THEN '离异'
                                WHEN 4 THEN '丧偶'
                                ELSE NULL
                              END,
            'relationship_status', CASE relationship_status
                                    WHEN 1 THEN '单身'
                                    WHEN 2 THEN '恋爱中'
                                    WHEN 3 THEN '已婚'
                                    WHEN 4 THEN '离异'
                                    ELSE NULL
                                  END,
            'education', CASE education_level
                           WHEN 1 THEN '初中及以下'
                           WHEN 2 THEN '高中'
                           WHEN 3 THEN '大专'
                           WHEN 4 THEN '本科'
                           WHEN 5 THEN '硕士'
                           WHEN 6 THEN '博士'
                           ELSE NULL
                         END,
            'income', JSON_OBJECT(
                'annual', annual_income,
                'unit', '万元',
                'has_mortgage', has_mortgage,
                'has_car_loan', has_car_loan
            ),
            'city_tier', city_tier
        ) AS basic_info_json
    FROM dwd_user_basic_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 职业信息整合
user_career AS (
    SELECT
        user_id,
        stat_date,
        JSON_OBJECT(
            'industry', industry,
            'profession', profession,
            'career_discussion_freq', career_discussion_freq,
            'career_concerns', COALESCE(career_concerns, JSON_ARRAY()),
            'entrepreneur_status', JSON_OBJECT(
                'is_entrepreneur', is_entrepreneur,
                'stage', entrepreneurial_stage,
                'goal', entrepreneurial_goal
            )
        ) AS career_info_json
    FROM dwd_user_career_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 家庭信息整合
user_family AS (
    SELECT
        user_id,
        stat_date,
        JSON_OBJECT(
            'family_type', family_type,
            'members', COALESCE(family_members, JSON_ARRAY()),
            'elderly_care_needed', elderly_care_needed,
            'relationship_quality', relationship_quality
        ) AS family_info_json
    FROM dwd_user_family_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 兴趣爱好整合
user_interest AS (
    SELECT
        user_id,
        stat_date,
        JSON_OBJECT(
            'travel', JSON_OBJECT(
                'preferences', COALESCE(travel_preferences, JSON_ARRAY()),
                'memorable_trips', COALESCE(memorable_trips, JSON_ARRAY()),
                'wishlist', COALESCE(travel_wishlist, JSON_ARRAY()),
                'constraints', COALESCE(travel_constraints, JSON_OBJECT())
            ),
            'games', JSON_OBJECT(
                'played', COALESCE(games_played, JSON_ARRAY()),
                'preference_distribution', COALESCE(game_preferences, JSON_OBJECT()),
                'play_style', game_play_style
            ),
            'sports', JSON_OBJECT(
                'preferences', COALESCE(sports_preferences, JSON_ARRAY()),
                'genetic_analysis', COALESCE(genetic_sports_ability, JSON_OBJECT())
            )
        ) AS interest_info_json
    FROM dwd_user_interest_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 健康信息整合
user_health AS (
    SELECT
        user_id,
        stat_date,
        JSON_OBJECT(
            'genetic_risks', COALESCE(genetic_risks, JSON_ARRAY()),
            'allergies', COALESCE(allergies, JSON_ARRAY()),
            'alcohol_reaction', alcohol_reaction,
            'nutrition_needs', COALESCE(nutrition_needs, JSON_OBJECT()),
            'potential_issues', COALESCE(potential_diseases, JSON_ARRAY())
        ) AS health_info_json
    FROM dwd_user_health_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 行为特征整合
user_behavior AS (
    SELECT
        user_id,
        stat_date,
        JSON_OBJECT(
            'usage_pattern', JSON_OBJECT(
                'frequency', usage_frequency,
                'total_conversations', total_conversations,
                'call_count', voice_call_count,
                'text_count', text_message_count,
                'last_login', DATE_FORMAT(last_login_time, '%Y-%m-%d')
            ),
            'preferred_topics', COALESCE(preferred_topics, JSON_ARRAY()),
            'communication_style', communication_style
        ) AS behavior_info_json
    FROM dwd_user_behavior_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 人生大事记整合
user_life_events AS (
    SELECT
        user_id,
        stat_date,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'date', DATE_FORMAT(event_date, '%Y-%m-%d'),
                'event', event_content,
                'importance', CASE importance_level
                                WHEN 1 THEN '低'
                                WHEN 2 THEN '中低'
                                WHEN 3 THEN '中'
                                WHEN 4 THEN '中高'
                                WHEN 5 THEN '高'
                                ELSE NULL
                              END,
                'sentiment', CASE emotional_tone
                                WHEN 1 THEN '消极'
                                WHEN 2 THEN '中性'
                                WHEN 3 THEN '积极'
                                ELSE NULL
                              END,
                'type', event_type
            )
        ) AS life_events_json
    FROM dwd_user_life_events_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
    GROUP BY user_id, stat_date
),

-- 梦想与理想整合
user_aspirations AS (
    SELECT
        user_id,
        stat_date,
        JSON_OBJECT(
            'dream', dream,
            'ideal', ideal,
            'related_actions', COALESCE(related_actions, JSON_ARRAY())
        ) AS aspirations_json
    FROM dwd_user_aspiration_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 消费特征整合
user_consumption AS (
    SELECT
        user_id,
        stat_date,
        JSON_OBJECT(
            'financial_pressure', financial_pressure,
            'consumption_preferences', JSON_OBJECT(
                'travel', travel_consumption_level,
                'entertainment', entertainment_consumption_level,
                'health', health_consumption_level
            )
        ) AS consumption_traits_json
    FROM dwd_user_consumption_info_di
    -- WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY
),

-- 人群标签整合
user_crowd_tags AS (
    SELECT
        r.user_id,
        r.stat_date,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'tag_name', d.tag_name,
                'confidence', r.confidence_score,
                'source', r.tag_source
            )
        ) AS crowd_tags_json
    FROM dwd_user_tag_relation_di r
    JOIN dim_tag_definition_di d ON r.tag_id = d.tag_id
    -- WHERE r.stat_date = CURRENT_DATE - INTERVAL 1 DAY
    AND d.tag_type = '人群'
    GROUP BY r.user_id, r.stat_date
),

-- 画像完整度评分计算
profile_score AS (
    SELECT
        base.user_id,
        base.stat_date,
        -- 计算各维度完整度
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
        (SELECT DISTINCT user_id, stat_date FROM dwd_user_basic_info_di WHERE stat_date = CURRENT_DATE - INTERVAL 1 DAY) base
    LEFT JOIN user_basic b ON base.user_id = b.user_id AND base.stat_date = b.stat_date
    LEFT JOIN user_career c ON base.user_id = c.user_id AND base.stat_date = c.stat_date
    LEFT JOIN user_family f ON base.user_id = f.user_id AND base.stat_date = f.stat_date
    LEFT JOIN user_interest i ON base.user_id = i.user_id AND base.stat_date = i.stat_date
    LEFT JOIN user_health h ON base.user_id = h.user_id AND base.stat_date = h.stat_date
    LEFT JOIN user_behavior be ON base.user_id = be.user_id AND base.stat_date = be.stat_date
    LEFT JOIN user_life_events l ON base.user_id = l.user_id AND base.stat_date = l.stat_date
    LEFT JOIN user_aspirations a ON base.user_id = a.user_id AND base.stat_date = a.stat_date
    LEFT JOIN user_consumption co ON base.user_id = co.user_id AND base.stat_date = co.stat_date
    LEFT JOIN user_crowd_tags ct ON base.user_id = ct.user_id AND base.stat_date = ct.stat_date
)

SELECT
    CURRENT_DATE - INTERVAL 1 DAY AS stat_date,
    b.user_id,
    b.basic_info_json->>'$.user_name' AS user_name,
    b.basic_info_json AS basic_info,
    COALESCE(c.career_info_json, JSON_OBJECT()) AS career_info,
    COALESCE(f.family_info_json, JSON_OBJECT()) AS family_info,
    COALESCE(i.interest_info_json, JSON_OBJECT()) AS interest_info,
    COALESCE(h.health_info_json, JSON_OBJECT()) AS health_info,
    COALESCE(be.behavior_info_json, JSON_OBJECT()) AS behavior_info,
    COALESCE(l.life_events_json, JSON_ARRAY()) AS life_events,
    COALESCE(a.aspirations_json, JSON_OBJECT()) AS aspirations,
    COALESCE(ct.crowd_tags_json, JSON_ARRAY()) AS crowd_tags,
    COALESCE(co.consumption_traits_json, JSON_OBJECT()) AS consumption_traits,
    COALESCE(p.completeness_score, 0) AS profile_score,
    'v1.0' AS data_version
FROM user_basic b
LEFT JOIN user_career c ON b.user_id = c.user_id AND b.stat_date = c.stat_date
LEFT JOIN user_family f ON b.user_id = f.user_id AND b.stat_date = f.stat_date
LEFT JOIN user_interest i ON b.user_id = i.user_id AND b.stat_date = i.stat_date
LEFT JOIN user_health h ON b.user_id = h.user_id AND b.stat_date = h.stat_date
LEFT JOIN user_behavior be ON b.user_id = be.user_id AND b.stat_date = be.stat_date
LEFT JOIN user_life_events l ON b.user_id = l.user_id AND b.stat_date = l.stat_date
LEFT JOIN user_aspirations a ON b.user_id = a.user_id AND b.stat_date = a.stat_date
LEFT JOIN user_consumption co ON b.user_id = co.user_id AND b.stat_date = co.stat_date
LEFT JOIN user_crowd_tags ct ON b.user_id = ct.user_id AND b.stat_date = ct.stat_date
LEFT JOIN profile_score p ON b.user_id = p.user_id AND b.stat_date = p.stat_date;