-- 用户基础信息表
drop Table if  exists dim_user_info_di;
Create Table if not exists dim_user_info_di(
  id                           BIGINT AUTO_INCREMENT COMMENT '自增主键id'
  ,user_id                     BIGINT                COMMENT '用户id'
  ,stat_date                   DATE                  COMMENT '统计日期(分区字段)'
  ,user_name                   VARCHAR(255)          COMMENT '用户姓名'
  ,sex                         VARCHAR(12)           COMMENT '性别'
  ,marital_status VARCHAR(20) COMMENT '婚姻状况(如: 已婚 未婚 离异)'
  ,id_card                     VARCHAR(18)           COMMENT '身份证编号'
  ,birthday                    VARCHAR(32)           COMMENT '生日'
  ,home_address                VARCHAR(255)          COMMENT '家庭地址'
  ,company_address             VARCHAR(255)          COMMENT '公司地址'
  ,school_address              VARCHAR(255)          COMMENT '学校地址'
  ,occupation                  VARCHAR(255)          COMMENT '职业'
  ,education_background        VARCHAR(255)          COMMENT '教育背景'
  ,age                         TINYINT               COMMENT '年龄'
  ,work                        VARCHAR(255)          COMMENT '工作'
  ,login_name                  VARCHAR(255)          COMMENT '登录名'
  ,password                    VARCHAR(64)           COMMENT '登录密码（MD5保存）'
  ,status                      TINYINT               COMMENT '账号状态：1正常/2冻结'
  ,logout_time                 BIGINT                COMMENT '最后登录时间'
  ,leave_reason                VARCHAR(255)          COMMENT '注销原因'
  -- ,friend_id                BIGINT                COMMENT '好友ID：一对多数据发散,分表'
  -- 用户分层
  ,user_level                  TINYINT               COMMENT '用户等级：1-普通 2-白银 3-黄金 4-铂金 5-钻石'
  ,rfm_score                   VARCHAR(10)           COMMENT 'rfm分值(如3-4-5)'
  ,user_lifecycle              VARCHAR(20)           COMMENT '用户生命周期：新用户、成长期、成熟期、衰退期、流失'
  -- 标签体系
  ,is_high_value               TINYINT(1) DEFAULT 0  COMMENT '是否高价值用户'
  ,is_premium                  TINYINT(1) DEFAULT 0  COMMENT '是否付费会员'
  ,is_content_creator          TINYINT(1) DEFAULT 0  COMMENT '是否内容创作者'
  ,created_time                DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
  ,updated_time                DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
  ,PRIMARY KEY (id,stat_date)
  ,UNIQUE KEY indx_user_date (stat_date,user_id)
)COMMENT='用户基础信息表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-06-01'))
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-07-01'))
    PARTITION p_current VALUES LESS THAN MAXVALUE
);

DROP TABLE IF EXISTS dws_user_health_summary_di;
CREATE TABLE dws_user_health_summary_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id'
    ,user_id   INT                     COMMENT '用户id'
    ,stat_date DATE                    COMMENT '统计日期'
    ,user_name VARCHAR(255)            COMMENT '用户姓名'

    -- 基础健康信息
    ,height     DECIMAL(5, 2)          COMMENT '身高(cm)'
    ,weight     DECIMAL(5, 2)          COMMENT '体重(kg)'
    ,bmi        DECIMAL(4, 2)          COMMENT 'BMI指数'
    ,blood_type CHAR(2)                COMMENT '血型(A/B/AB/O)'
    ,rh_factor  CHAR(1)                COMMENT 'RH值(+/-)'
    ,body_fat_rate      DECIMAL(4, 2)  COMMENT '体脂率(%)'
    ,first_measure_date DATE           COMMENT '首次测量日期'
    ,last_measure_date  DATE           COMMENT '最近测量日期'

    -- 健康指标
    ,avg_heart_rate INT                COMMENT '平均心率(次/分钟)'
    ,max_heart_rate INT                COMMENT '最大心率'
    ,min_heart_rate INT                COMMENT '最小心率'
    ,avg_blood_pressure VARCHAR(10)    COMMENT '平均血压(如120/80)'
    ,avg_blood_oxygen   DECIMAL(4, 1)  COMMENT '平均血氧饱和度(%)'
    ,avg_body_temp      DECIMAL(3, 1)  COMMENT '平均体温(℃)'

    -- 运动健康指标
    ,total_steps            INT           COMMENT '总步数'
    ,avg_daily_steps        INT           COMMENT '日均步数'
    ,total_calories         INT           COMMENT '总消耗卡路里(千卡)'
    ,total_exercise_minutes INT           COMMENT '总运动时长(分钟)'
    ,avg_sleep_duration     DECIMAL(4, 1) COMMENT '平均睡眠时长(小时)'
    ,sleep_quality_score    INT           COMMENT '睡眠质量评分(1-100)'

    -- 健康风险
    ,is_high_blood_pressure INT           DEFAULT 0 COMMENT '是否高血压风险'
    ,is_heart_disease_risk  INT           DEFAULT 0 COMMENT '是否心脏病风险'
    ,is_obesity             INT           DEFAULT 0 COMMENT '是否肥胖'
    ,is_sleep_disorder      INT           DEFAULT 0 COMMENT '是否睡眠障碍'
    ,allergies              VARCHAR(255)  COMMENT '过敏史'
    ,chronic_diseases       VARCHAR(255)  COMMENT '慢性病史'

    -- 健康行为
    ,exercise_frequency     INT           COMMENT '运动频率(1-不运动2-偶尔3-经常4-每天)'
    ,diet_habit             INT           COMMENT '饮食习惯(1-不健康2-一般3-健康)'
    ,health_management_flag INT           DEFAULT 0 COMMENT '是否进行健康管理'
    ,created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'
    ,PRIMARY KEY (id)
    ,UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户健康信息汇总事实表';

-- 用户职业信息明细表：
-- 同构 dwd_user_career_info与dws_user_career_info

DROP TABLE IF EXISTS dws_user_career_info;
CREATE TABLE dws_user_career_info (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT                       COMMENT '用户id',
    stat_date DATE                    COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    -- 行业信息
    industry_name VARCHAR(100) COMMENT '行业名称（国标）',
    occupation_name VARCHAR(100) COMMENT '职业名称（国标）',
    position_name VARCHAR(100) COMMENT '职位名称',
    -- 工作相关
    company_name VARCHAR(255) COMMENT '公司名称',
    company_size VARCHAR(20) COMMENT '公司规模(如: 小型 中型 大型)',
    work_experience_years INT COMMENT '工作年限(年)',
    current_salary DECIMAL(20, 2) COMMENT '当前薪资(元)',
    -- 用户行为
    job_hopping_frequency     INT COMMENT '跳槽频率(1-低 2-中 3-高)',
    career_satisfaction_score INT COMMENT '职业满意度(1-10分)',
    has_attended_training     INT COMMENT '是否参加过培训(0-否 1-是)',
    -- 时间维度
    first_job_date DATE     COMMENT '首次工作日期',
    created_time   DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建日期',
    updated_time   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录最后更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户职业信息明细表';



drop Table if  exists dws_user_education_learning_di;
CREATE TABLE dws_user_education_learning_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT NOT NULL COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
	user_name VARCHAR(255)            COMMENT '用户姓名',
    
    -- 教育背景信息
    education_level VARCHAR(20) COMMENT '教育程度(如: 高中 本科 硕士 博士)',
    major VARCHAR(100) COMMENT '专业名称',
    graduation_year INT COMMENT '毕业年份',
    school_name VARCHAR(255) COMMENT '毕业院校名称',
	-- 新增的高考相关字段
    college_entrance_exam_rank INT COMMENT '高考排名',
    college_entrance_exam_score INT COMMENT '高考分数',
    
    -- 新增的考研相关字段
    postgraduate_entrance_exam_rank INT COMMENT '考研排名',
    postgraduate_entrance_exam_score INT COMMENT '考研分数',
    
    -- 新增的入学和毕业时间
    admission_date DATE COMMENT '入学时间',
    graduation_date DATE COMMENT '毕业时间',
	
    -- 学习行为信息
    total_courses_completed INT COMMENT '完成课程总数',
    avg_course_score DECIMAL(4, 2) COMMENT '平均课程得分',
    total_learning_hours INT COMMENT '总学习时长(小时)',
    avg_daily_learning_time DECIMAL(4, 2) COMMENT '日均学习时长(小时)',
    learning_frequency TINYINT COMMENT '学习频率(1-低 2-中 3-高)',
    first_learning_date DATE COMMENT '首次学习日期',
    
    -- 技能水平信息
    skill_level_coding TINYINT COMMENT '技能水平(1-初级 2-中级 3-高级)',
    certification_name VARCHAR(255) COMMENT '获得证书名称',
    
    -- 学习偏好信息
    preferred_learning_method VARCHAR(50) COMMENT '偏好学习方式(如: 在线课程 书籍 培训)',
    preferred_subject VARCHAR(100) COMMENT '偏好学习科目',
    learning_goal VARCHAR(255) COMMENT '学习目标',
    
    -- 时间戳
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date),
    KEY idx_education_level (education_level),
    KEY idx_major (major),
    KEY idx_skill_level_coding (skill_level_coding)
) COMMENT='用户教育与学习信息事实表';

-- 用户家庭关系信息事实表 
-- 同构
-- dwd_user_family_relations_di
-- dws_user_family_relations_di
drop Table if  exists dws_user_family_relations_di;
CREATE TABLE dws_user_family_relations_di (
id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id'
,user_id int NOT NULL COMMENT '用户id'
,stat_date DATE NOT NULL COMMENT '统计日期'
,user_name           VARCHAR(255)            COMMENT '用户姓名'

-- 家庭成员基本信息
,family_member_id VARCHAR(64) NOT NULL COMMENT '家庭成员id'
,family_member_name VARCHAR(100) COMMENT '家庭成员姓名'
,relation_type VARCHAR(20) NOT NULL COMMENT '关系类型(如: 父母 配偶 子女 兄弟姐妹)'
,relation_description VARCHAR(255) COMMENT '关系描述(如: 父亲 长子)'

-- 家庭成员特征
,age INT COMMENT '年龄'
,gender CHAR(1) COMMENT '性别(M-男 F-女 U-未知)'
,occupation VARCHAR(100) COMMENT '职业'
,education_level VARCHAR(20) COMMENT '教育程度(如: 高中 本科 硕士)'
,marital_status VARCHAR(20) COMMENT '婚姻状况(如: 已婚 未婚 离异)'

-- 家庭关系状态
,is_living_together TINYINT COMMENT '是否同住(0-否 1-是)'
,contact_frequency VARCHAR(20) COMMENT '联系频率(如: 每天 每周 每月)'
,last_contact_date DATE COMMENT '最后联系日期'

-- 家庭经济相关
-- ,family_income_contribution DECIMAL(65) COMMENT '家庭收入贡献(元/年)'
,is_financial_dependent TINYINT COMMENT '是否经济依赖(0-否 1-是)'

-- 家庭健康相关
,health_status VARCHAR(50) COMMENT '健康状况(如: 健康 慢性病 残疾)'
,has_chronic_disease TINYINT COMMENT '是否有慢性病(0-否 1-是)'
,create_date DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP             COMMENT '创建日期'
,update_time DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP             COMMENT '数据更新时间'
,PRIMARY KEY (id)
,UNIQUE KEY idx_user_member_date (user_id,family_member_id,stat_date)
,KEY idx_relation_type (relation_type)
,KEY idx_age (age)
,KEY idx_is_living_together (is_living_together)
) COMMENT='用户家庭关系信息事实表';


-- 用户财务理财信息事实表
-- 同构
-- dwd_user_financial_management_di
-- dws_user_financial_management_di
drop Table if  exists dws_user_financial_management_di;
CREATE TABLE dws_user_financial_management_di (
id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id'
,user_id int NOT NULL COMMENT '用户id'
,stat_date DATE NOT NULL COMMENT '统计日期'
,user_name VARCHAR(255)         COMMENT '用户姓名'
-- 财务状况
,total_assets DECIMAL(20) COMMENT '总资产(元)'
,total_liabilities DECIMAL(20) COMMENT '总负债(元)'
,net_worth DECIMAL(20) COMMENT '净资产(元)'
,monthly_income DECIMAL(20) COMMENT '月收入(元)'
,monthly_expenses DECIMAL(20) COMMENT '月支出(元)'
,savings_rate DECIMAL(20) COMMENT '储蓄率(%)'

-- 理财行为
,total_investments DECIMAL(20) COMMENT '总投资金额(元)'
,investment_return_rate DECIMAL(20) COMMENT '投资回报率(%)'
,risk_tolerance VARCHAR(20) COMMENT '风险承受能力(如: 低 中 高)'
,investment_experience INT COMMENT '投资经验年限(年)'
,investment_frequency VARCHAR(20) COMMENT '投资频率(如: 每日 每周 每月)'

-- 资产配置
,cash_ratio DECIMAL(52) COMMENT '现金类资产占比(%)'
,fixed_income_ratio DECIMAL(52) COMMENT '固定收益类资产占比(%)'
,equity_ratio DECIMAL(52) COMMENT '权益类资产占比(%)'
,real_estate_ratio DECIMAL(52) COMMENT '房地产类资产占比(%)'
,alternative_investments_ratio DECIMAL(52) COMMENT '另类投资类资产占比(%)'

-- 理财目标
,financial_goal VARCHAR(255) COMMENT '理财目标(如: 购房 子女教育 退休)'
,goal_completion_percentage DECIMAL(52) COMMENT '理财目标完成率(%)'
,goal_time_horizon INT COMMENT '理财目标时间跨度(年)'

-- 信用与债务
,credit_score INT COMMENT '信用评分'
,debt_to_income_ratio DECIMAL(52) COMMENT '债务收入比(%)'
,mortgage_status VARCHAR(20) COMMENT '房贷状态(如: 无 有且还款中 已还清)'
,car_loan_status VARCHAR(20) COMMENT '车贷状态(如: 无 有且还款中 已还清)'
,created_date DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP             COMMENT '创建日期'
,updated_time DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP             COMMENT '数据更新时间'
,PRIMARY KEY (id)
,UNIQUE KEY idx_user_date (user_id,stat_date)
,KEY idx_risk_tolerance (risk_tolerance)
,KEY idx_financial_goal (financial_goal)
,KEY idx_credit_score (credit_score)
) COMMENT='用户财务理财信息事实表';

drop Table if  exists dws_user_leisure_entertainment_di;
CREATE TABLE dws_user_leisure_entertainment_di (
id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id'
,user_id int NOT NULL COMMENT '用户id'
,stat_date DATE NOT NULL COMMENT '统计日期'
,user_name VARCHAR(255)            COMMENT '用户姓名'

-- 用户基本信息关联
,age_group VARCHAR(20) COMMENT '年龄组(如: 18-24 25-34)'
,gender CHAR(1) COMMENT '性别(M-男 F-女 U-未知)'
,region VARCHAR(50) COMMENT '地区'

-- 休闲娱乐行为
,daily_leisure_time INT COMMENT '日均休闲娱乐时长(分钟)'
,weekly_leisure_days INT COMMENT '每周休闲娱乐天数'
,leisure_activity_types VARCHAR(255) COMMENT '休闲活动类型(如: 电影 游戏 阅读 运动 旅游)'
,preferred_activity VARCHAR(50) COMMENT '首选休闲活动'
,leisure_activity_frequency VARCHAR(50) COMMENT '休闲活动频率(如: 每天 每周 每月)'

-- 娱乐消费
,monthly_leisure_spending DECIMAL(20) COMMENT '月均休闲娱乐消费(元)'
,spending_categories VARCHAR(255) COMMENT '消费类别(如: 电影票 游戏充值 书籍 运动装备)'
,preferred_payment_method VARCHAR(50) COMMENT '首选支付方式(如: 支付宝 微信 信用卡)'

-- 内容偏好
,preferred_genres VARCHAR(255) COMMENT '偏好内容类型(如: 动作 喜剧 科幻 悬疑)'
,favorite_artists_or_celebrities VARCHAR(255) COMMENT '喜欢的艺人或名人'
,followed_entertainment_accounts INT COMMENT '关注的娱乐账号数量'

-- 社交互动
,leisure_social_frequency VARCHAR(50) COMMENT '休闲社交频率(如: 每天 每周 每月)'
,preferred_social_platforms VARCHAR(255) COMMENT '首选社交平台(如: 微信 微博 抖音)'
,leisure_group_activities VARCHAR(255) COMMENT '参与的休闲团体活动(如: 读书会 运动俱乐部)'

-- 设备与平台使用
,preferred_devices VARCHAR(255) COMMENT '常用设备(如: 手机 平板 电脑 电视)'
,leisure_app_usage VARCHAR(255) COMMENT '常用休闲娱乐应用(如: 腾讯视频 王者荣耀 豆瓣)'
,daily_app_usage_time INT COMMENT '日均应用使用时长(分钟)'
,create_date DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP             COMMENT '创建日期'
,update_time DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP             COMMENT '数据更新时间'
,PRIMARY KEY (id)
,KEY idx_age_group (age_group)
,KEY idx_gender (gender)
) COMMENT='用户休闲娱乐信息事实表';


-- 用户人生事件信息事实表
-- 同构
-- dws_user_life_events_di

DROP TABLE IF EXISTS dws_user_life_events_di;

CREATE TABLE dws_user_life_events_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT NOT NULL COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    event_id VARCHAR(64) NOT NULL COMMENT '事件唯一标识',
    event_type VARCHAR(50) NOT NULL COMMENT '事件类型(如: 结婚, 生子, 购房, 升学, 离职)',
    event_date DATE NOT NULL COMMENT '事件发生日期',

    -- 事件详细信息
    event_description VARCHAR(255) COMMENT '事件描述',
    related_persons VARCHAR(255) COMMENT '相关人员(如: 配偶姓名, 孩子姓名)',
    event_location VARCHAR(100) COMMENT '事件发生地点',
    event_importance_score INT COMMENT '事件重要性评分(1-10)',

    -- 用户关联信息
    user_age_at_event INT COMMENT '事件发生时用户年龄',
    user_gender CHAR(1) COMMENT '性别(M-男, F-女, U-未知)',
    user_region VARCHAR(50) COMMENT '用户所在地区',

    -- 事件影响与后续行为
    post_event_behavior_change VARCHAR(255) COMMENT '事件后用户行为变化(如: 消费增加, 职业转变)',
    financial_impact_amount DECIMAL(20, 2) COMMENT '事件对财务的影响金额(元, 正值表示收入增加, 负值表示支出增加)',
    emotional_impact_description VARCHAR(50) COMMENT '事件对情感的影响(如: 开心, 压力大)',

    -- 记录时间戳
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建日期',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录最后更新时间',

    PRIMARY KEY (id),
    UNIQUE KEY idx_user_event_date (user_id, event_id, event_date),
    KEY idx_event_type (event_type),
    KEY idx_event_date (event_date),
    KEY idx_user_age_at_event (user_age_at_event),
    KEY idx_user_gender (user_gender)
) COMMENT='用户人生事件信息明细表';




-- 用户特征汇总宽表
DROP TABLE IF EXISTS dws_user_characteristic_aggregate_di;
CREATE TABLE IF NOT EXISTS dws_user_characteristic_aggregate_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id'
    ,user_id INT COMMENT '用户id'
    ,stat_date DATE NOT NULL COMMENT '统计日期'

    -- 用户基础信息
    ,user_name VARCHAR(255) COMMENT '用户姓名'
    ,sex VARCHAR(12) COMMENT '性别'
	,marital_status VARCHAR(20) COMMENT '婚姻状况(如: 已婚 未婚 离异)'
    ,id_card VARCHAR(18) COMMENT '身份证编号'
    ,birthday VARCHAR(32) COMMENT '生日'
    ,home_address VARCHAR(255) COMMENT '家庭地址'
    ,company_address VARCHAR(255) COMMENT '公司地址'
    ,school_address VARCHAR(255) COMMENT '学校地址'
    ,occupation VARCHAR(255) COMMENT '职业'
    ,education_background VARCHAR(255) COMMENT '教育背景'
    ,age TINYINT COMMENT '年龄'
    ,work VARCHAR(255) COMMENT '工作'
    ,login_name VARCHAR(255) COMMENT '登录名'
    ,password VARCHAR(64) COMMENT '登录密码（MD5保存）'
    ,status TINYINT COMMENT '账号状态：1正常/2冻结'
    ,logout_time BIGINT COMMENT '最后登录时间'
    ,leave_reason VARCHAR(255) COMMENT '注销原因'
    ,user_level TINYINT COMMENT '用户等级：1-普通 2-白银 3-黄金 4-铂金 5-钻石'
    ,rfm_score VARCHAR(10) COMMENT 'rfm分值(如3-4-5)'
    ,user_lifecycle VARCHAR(20) COMMENT '用户生命周期：新用户、成长期、成熟期、衰退期、流失'
    ,is_high_value TINYINT(1) DEFAULT 0 COMMENT '是否高价值用户'
    ,is_premium TINYINT(1) DEFAULT 0 COMMENT '是否付费会员'
    ,is_content_creator TINYINT(1) DEFAULT 0 COMMENT '是否内容创作者'

    -- 健康信息
    ,height DECIMAL(5, 2) COMMENT '身高(cm)'
    ,weight DECIMAL(5, 2) COMMENT '体重(kg)'
    ,bmi DECIMAL(4, 2) COMMENT 'BMI指数'
    ,blood_type CHAR(2) COMMENT '血型(A/B/AB/O)'
    ,rh_factor CHAR(1) COMMENT 'RH值(+/-)'
    ,body_fat_rate DECIMAL(4, 2) COMMENT '体脂率(%)'
    ,first_measure_date DATE COMMENT '首次测量日期'
    ,last_measure_date DATE COMMENT '最近测量日期'
    ,avg_heart_rate SMALLINT COMMENT '平均心率(次/分钟)'
    ,max_heart_rate SMALLINT COMMENT '最大心率'
    ,min_heart_rate SMALLINT COMMENT '最小心率'
    ,avg_blood_pressure VARCHAR(10) COMMENT '平均血压(如120/80)'
    ,avg_blood_oxygen DECIMAL(4, 1) COMMENT '平均血氧饱和度(%)'
    ,avg_body_temp DECIMAL(3, 1) COMMENT '平均体温(℃)'
    ,total_steps INT COMMENT '总步数'
    ,avg_daily_steps INT COMMENT '日均步数'
    ,total_calories INT COMMENT '总消耗卡路里(千卡)'
    ,total_exercise_minutes INT COMMENT '总运动时长(分钟)'
    ,avg_sleep_duration DECIMAL(4, 1) COMMENT '平均睡眠时长(小时)'
    ,sleep_quality_score TINYINT COMMENT '睡眠质量评分(1-100)'
    ,is_high_blood_pressure TINYINT(1) DEFAULT 0 COMMENT '是否高血压风险'
    ,is_heart_disease_risk TINYINT(1) DEFAULT 0 COMMENT '是否心脏病风险'
    ,is_obesity TINYINT(1) DEFAULT 0 COMMENT '是否肥胖'
    ,is_sleep_disorder TINYINT(1) DEFAULT 0 COMMENT '是否睡眠障碍'
    ,allergies VARCHAR(255) COMMENT '过敏史'
    ,chronic_diseases VARCHAR(255) COMMENT '慢性病史'
    ,exercise_frequency TINYINT COMMENT '运动频率(1-不运动2-偶尔3-经常4-每天)'
    ,diet_habit TINYINT COMMENT '饮食习惯(1-不健康2-一般3-健康)'
    ,health_management_flag TINYINT(1) DEFAULT 0 COMMENT '是否进行健康管理'

    -- 社交关系
    ,family_member_id bigint COMMENT '家人ID'
    ,family_member_name VARCHAR(255) COMMENT '家人'
    ,friend_id bigint COMMENT '朋友ID'
    ,friend_name VARCHAR(255) COMMENT '朋友'
    ,colleague_id bigint COMMENT '同事ID'
    ,colleague_name VARCHAR(255) COMMENT '同事'

    -- 综合属性
    ,favorite_foods VARCHAR(255) COMMENT '喜欢吃的东西'
    ,dietary_restrictions VARCHAR(255) COMMENT '饮食限制(如: 素食 无麸质)'
    ,favorite_colors VARCHAR(255) COMMENT '喜欢的颜色'
    ,hobbies VARCHAR(255) COMMENT '兴趣爱好'
    ,favorite_games VARCHAR(255) COMMENT '喜欢的游戏'
    ,favorite_books VARCHAR(255) COMMENT '喜欢的书籍'
    ,favorite_movies VARCHAR(255) COMMENT '喜欢的电影'
    ,achievements VARCHAR(255) COMMENT '用户成就'
    ,dreams VARCHAR(255) COMMENT '用户梦想'
    ,ideals VARCHAR(255) COMMENT '用户理想'
    ,plans VARCHAR(255) COMMENT '用户计划'
    ,goals VARCHAR(255) COMMENT '用户目标'
    ,thoughts TEXT COMMENT '想法'
    ,inspirations TEXT COMMENT '灵感'
    ,family_memories TEXT COMMENT '家人记忆'
    ,friend_memories TEXT COMMENT '朋友记忆'
    ,colleague_memories TEXT COMMENT '同事记忆'
    ,visited_places VARCHAR(255) COMMENT '曾经去哪里旅行'
    ,desired_travel_places VARCHAR(255) COMMENT '想去哪里旅行'

    -- 时间戳
    ,created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期'
    ,updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'

    ,PRIMARY KEY (id)
    ,UNIQUE KEY idx_user_date (user_id, stat_date) COMMENT '唯一键约束，确保每个用户在每个统计日期只有一条记录'
) COMMENT='用户特征汇总事实表';


DROP TABLE IF EXISTS dws_user_destiny_analysis_di;
CREATE TABLE IF NOT EXISTS dws_user_destiny_analysis_di (
id                          BIGINT AUTO_INCREMENT COMMENT '自增主键ID'
,user_id                    BIGINT NOT NULL COMMENT '用户ID'
,stat_date                  DATE NOT NULL COMMENT '分析日期(分区字段)'
,user_name                  VARCHAR(255)         COMMENT '用户姓名'
-- 性格分析部分
,personality_keywords        VARCHAR(255) COMMENT '性格关键词(逗号分隔)'
,personality_overview        TEXT COMMENT '性格概览'
,personality_strengths       TEXT COMMENT '性格优点'
,personality_weaknesses      TEXT COMMENT '性格缺点'
,strengths_suggestions       TEXT COMMENT '分身建议(扬长)'
,weaknesses_suggestions      TEXT COMMENT '分身建议(避短)'
-- 健康症状部分
,symptom                     VARCHAR(255) COMMENT '症状'
,symptom_description         TEXT COMMENT '症状描述'
,symptom_analysis            TEXT COMMENT '症状分析'
-- 调理方法部分
,diet_recommendations        TEXT COMMENT '饮食调理建议'
,herbal_recommendations      TEXT COMMENT '中药调理建议'
,lifestyle_recommendations   TEXT COMMENT '生活方式建议'
,acupoint_recommendations    TEXT COMMENT '穴位调理建议'
,other_recommendations       TEXT COMMENT '其他调理建议'
-- 元数据
,created_time                  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
,updated_time                  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
,PRIMARY KEY (id,stat_date)
,KEY idx_user_date (user_id,stat_date)
) COMMENT='用户命理推断事实表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-07-01'))
,PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-08-01'))
,PARTITION p202503 VALUES LESS THAN (TO_DAYS('2025-09-01'))
,PARTITION p_current VALUES LESS THAN MAXVALUE
);


--  extend add and modify 202507

-- 创建视图来替代生成列
-- CREATE VIEW vw_clothing_user_behavior AS
-- SELECT 
    -- t.*,
    -- CASE 
        -- WHEN t.purchase_count > 0 THEN c.name
        -- ELSE NULL 
    -- END AS preferred_category
-- FROM 
    -- dws_clothing_user_behavior_agg_di t
-- LEFT JOIN 
    -- dim_clothing_category c ON t.category_id = c.id;
	
	


DROP TABLE IF EXISTS dws_user_clothing_behavior_agg_di;
CREATE TABLE dws_user_clothing_behavior_agg_di (
   id BIGINT AUTO_INCREMENT COMMENT '自增主键'
   ,stat_date DATE NOT NULL COMMENT '日期'
   ,user_id BIGINT NOT NULL COMMENT '用户ID'
   ,user_name VARCHAR(64)            COMMENT '用户姓名'
   ,user_gender TINYINT COMMENT '性别（0女/1男）'
   ,age_range VARCHAR(16) COMMENT '年龄段（如20-25）'
   ,category_id BIGINT COMMENT '服装品类ID'
   ,brand_id BIGINT COMMENT '品牌ID'
   ,view_count INT DEFAULT 0 COMMENT '浏览次数'
   ,cart_count INT DEFAULT 0 COMMENT '加购次数'
   ,purchase_count INT DEFAULT 0 COMMENT '购买次数'
   ,total_amount DECIMAL(18,2) DEFAULT 0 COMMENT '购买总金额'
   ,avg_discount_rate DECIMAL(5,2) DEFAULT 0 COMMENT '平均折扣率'
   ,preferred_category VARCHAR(64) 
   -- GENERATED ALWAYS AS (
       -- CASE 
           -- WHEN purchase_count > 0 THEN 
               -- (SELECT name FROM dim_clothing_category WHERE id = category_id)
           -- ELSE NULL 
       -- END
   -- ) STORED 
   COMMENT '偏好品类（基于购买行为）'
   ,create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
   ,update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
   ,PRIMARY KEY (id)
   ,UNIQUE KEY idx_dt_user_category (stat_date, user_id, category_id)
   ,KEY idx_user_behavior (user_id, purchase_count)
   ,KEY idx_brand (brand_id)
)COMMENT='用户服装行为日汇总表';


DROP TABLE IF EXISTS dws_user_food_dining_agg_di;
CREATE TABLE dws_user_food_dining_agg_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键'
    ,stat_date DATE NOT NULL COMMENT '日期'
    ,user_id BIGINT NOT NULL COMMENT '用户ID'
    ,user_name VARCHAR(64)            COMMENT '用户姓名'
    ,restaurant_id BIGINT COMMENT '门店ID'
    ,cuisine_type VARCHAR(32) COMMENT '菜系（如川菜/粤菜）'
    ,time_slot VARCHAR(16) COMMENT '时段（早餐/午餐/晚餐）'
    ,visit_count INT DEFAULT 0 COMMENT '到店次数'
    ,order_count INT DEFAULT 0 COMMENT '点餐数'
    ,total_spend DECIMAL(18,2) DEFAULT 0 COMMENT '消费总金额'
    ,avg_rating DECIMAL(3,1) DEFAULT 0 COMMENT '平均评分'
    ,is_weekend TINYINT(1) DEFAULT 0 COMMENT '是否周末（0否/1是）'
    ,preferred_cuisine VARCHAR(32) 
	-- GENERATED ALWAYS AS (
        -- CASE 
            -- WHEN total_spend > 0 THEN cuisine_type
            -- ELSE NULL 
        -- END
    -- ) STORED 
	COMMENT '偏好菜系'
    ,create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
    ,update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
    ,PRIMARY KEY (id)
    ,UNIQUE KEY idx_dt_user_restaurant (stat_date, user_id, restaurant_id)
    ,KEY idx_user_cuisine (user_id, cuisine_type)
    ,KEY idx_time_slot (time_slot)
)COMMENT='用户餐饮行为日汇总表';

DROP TABLE IF EXISTS dws_user_housing_activity_agg_mi;
CREATE TABLE dws_user_housing_activity_agg_mi (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键'
    ,month DATE NOT NULL COMMENT '月份（取每月1日）'
    -- ,stat_date DATE NOT NULL COMMENT '日期'
    ,user_id BIGINT NOT NULL COMMENT '用户ID'
    ,user_name VARCHAR(64)            COMMENT '用户姓名'
    ,district VARCHAR(64) COMMENT '行政区'
    ,property_type VARCHAR(32) COMMENT '房型（公寓/别墅）'
    ,viewing_count INT DEFAULT 0 COMMENT '看房次数'
    ,contract_count INT DEFAULT 0 COMMENT '签约次数'
    ,avg_rent DECIMAL(18,2) DEFAULT 0 COMMENT '平均租金'
    ,preferred_district VARCHAR(64) 
	-- GENERATED ALWAYS AS (
        -- CASE 
            -- WHEN contract_count > 0 THEN district
            -- ELSE NULL 
        -- END
    -- ) STORED
	COMMENT '偏好区域'
    ,create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
    ,update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
    ,PRIMARY KEY (id)
    ,UNIQUE KEY idx_month_user_district (month, user_id, district)
    ,KEY idx_user_property (user_id, property_type)
)COMMENT='用户住房行为月汇总表';






DROP TABLE IF EXISTS dws_user_transport_trip_agg_di;
CREATE TABLE dws_user_transport_trip_agg_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键'
    ,stat_date DATE NOT NULL COMMENT '日期'
    ,user_id BIGINT NOT NULL COMMENT '用户ID'
    ,vehicle_type VARCHAR(32) NOT NULL COMMENT '交通工具（快车/地铁/共享单车）'
    ,start_district VARCHAR(64) COMMENT '出发区域'
    ,trip_count INT DEFAULT 0 COMMENT '出行次数'
    ,total_distance DECIMAL(10,2) DEFAULT 0 COMMENT '总里程（公里）'
    ,total_cost DECIMAL(18,2) DEFAULT 0 COMMENT '总费用'
    ,peak_hour_ratio DECIMAL(5,2) DEFAULT 0 COMMENT '高峰时段占比'
    ,preferred_vehicle VARCHAR(32) 
	-- GENERATED ALWAYS AS (
        -- CASE 
            -- WHEN trip_count > 0 THEN vehicle_type
            -- ELSE NULL 
        -- END
    -- ) STORED 
	COMMENT '偏好交通工具'
    ,create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
    ,update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
    ,PRIMARY KEY (id)
    ,UNIQUE KEY idx_dt_user_vehicle (stat_date, user_id, vehicle_type)
    ,KEY idx_user_district (user_id, start_district)
) COMMENT='用户出行行为日汇总表';



-- new
-- DROP TABLE IF EXISTS dws_user_clothing_behavior_agg_di;
-- CREATE TABLE dws_user_clothing_behavior_agg_di (
    -- id BIGINT AUTO_INCREMENT COMMENT '自增主键',
    -- stat_date DATE NOT NULL COMMENT '日期',
    -- user_id BIGINT NOT NULL COMMENT '用户ID',
    -- user_name VARCHAR(64) COMMENT '用户姓名',
    
    -- 用户基础属性
    -- user_gender TINYINT COMMENT '性别(0女/1男/2其他)',
    -- age_range VARCHAR(16) COMMENT '年龄段(如20-25)',
    -- body_type VARCHAR(20) COMMENT '体型(S/M/L/XL等)',
    
    -- 商品维度
    -- category_id BIGINT COMMENT '服装品类ID',
    -- category_level VARCHAR(16) COMMENT '品类层级(1级/2级/3级)',
    -- brand_id BIGINT COMMENT '品牌ID',
    -- brand_tier VARCHAR(10) COMMENT '品牌层级(奢侈/高端/大众)',
    
    -- 行为指标
    -- view_count INT DEFAULT 0 COMMENT '浏览次数',
    -- favorite_count INT DEFAULT 0 COMMENT '收藏次数',
    -- cart_count INT DEFAULT 0 COMMENT '加购次数',
    -- purchase_count INT DEFAULT 0 COMMENT '购买次数',
    -- return_count INT DEFAULT 0 COMMENT '退货次数',
    
    -- 消费指标
    -- total_amount DECIMAL(18,2) DEFAULT 0 COMMENT '购买总金额',
    -- avg_price DECIMAL(10,2) DEFAULT 0 COMMENT '件均价',
    -- avg_discount_rate DECIMAL(5,2) DEFAULT 0 COMMENT '平均折扣率',
    
    -- 新增时尚维度
    -- style_preference VARCHAR(32) COMMENT '风格偏好(商务/休闲/运动等)',
    -- color_preference VARCHAR(32) COMMENT '颜色偏好',
    -- seasonality VARCHAR(10) COMMENT '季节特征(春夏/秋冬)',
    
    -- 社交属性
    -- social_share_count INT DEFAULT 0 COMMENT '社交分享次数',
    -- review_length INT DEFAULT 0 COMMENT '平均评价字数',
    
    -- 偏好分析
    -- preferred_category VARCHAR(64) COMMENT '偏好品类',
    -- preferred_price_range VARCHAR(16) COMMENT '偏好价格带',
    
    -- create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    -- update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- PRIMARY KEY (id),
    -- UNIQUE KEY idx_dt_user_category (stat_date, user_id, category_id),
    -- KEY idx_user_behavior (user_id, purchase_count),
    -- KEY idx_brand_tier (brand_tier),
    -- KEY idx_style (style_preference),
    -- KEY idx_season (seasonality)
-- ) COMMENT='用户服装行为日汇总表';
DROP TABLE IF EXISTS dws_user_clothing_behavior_agg_di;
CREATE TABLE dws_user_clothing_behavior_agg_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键',
    stat_date DATE NOT NULL COMMENT '日期',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    user_name VARCHAR(64) COMMENT '用户姓名',
    
    -- 用户基础属性
    user_gender TINYINT COMMENT '性别(0女/1男/2其他)',
    age_range VARCHAR(16) COMMENT '年龄段(如20-25)',
    body_type VARCHAR(20) COMMENT '体型(S/M/L/XL等)',
    body_measurements VARCHAR(64) COMMENT '三围数据(胸围-腰围-臀围,cm)',
    
    -- 商品维度
    category_id BIGINT COMMENT '服装品类ID',
    category_level VARCHAR(16) COMMENT '品类层级(1级/2级/3级)',
    brand_id BIGINT COMMENT '品牌ID',
    brand_tier VARCHAR(10) COMMENT '品牌层级(奢侈/高端/大众)',
    product_quality TINYINT COMMENT '产品质量评分(1-5)',
    
    -- 行为指标
    view_count INT DEFAULT 0 COMMENT '浏览次数',
    favorite_count INT DEFAULT 0 COMMENT '收藏次数',
    cart_count INT DEFAULT 0 COMMENT '加购次数',
    purchase_count INT DEFAULT 0 COMMENT '购买次数',
    return_count INT DEFAULT 0 COMMENT '退货次数',
    exchange_count INT DEFAULT 0 COMMENT '换货次数',
    
    -- 消费指标
    total_amount DECIMAL(18,2) DEFAULT 0 COMMENT '购买总金额',
    avg_price DECIMAL(10,2) DEFAULT 0 COMMENT '件均价',
    avg_discount_rate DECIMAL(5,2) DEFAULT 0 COMMENT '平均折扣率',
    full_price_purchase_ratio DECIMAL(5,2) COMMENT '全价购买比例',
    
    -- 新增时尚维度
    style_preference VARCHAR(32) COMMENT '风格偏好(商务/休闲/运动等)',
    color_preference VARCHAR(32) COMMENT '颜色偏好',
    seasonality VARCHAR(10) COMMENT '季节特征(春夏/秋冬)',
    fashion_sensitivity VARCHAR(10) COMMENT '时尚敏感度(高/中/低)',
    
    -- 新增服装体验维度
    fit_feedback TINYINT COMMENT '合身度反馈(1-5)',
    size_accuracy TINYINT COMMENT '尺码准确性(1-5)',
    material_satisfaction TINYINT COMMENT '材质满意度(1-5)',
    comfort_rating TINYINT COMMENT '舒适度评分(1-5)',
    durability_rating TINYINT COMMENT '耐用性评分(1-5)',
    
    -- 购买渠道与行为
    purchase_channel VARCHAR(20) COMMENT '购买渠道(线上/线下/社交电商)',
    repeat_purchase_flag TINYINT DEFAULT 0 COMMENT '是否复购(0否1是)',
    cross_brand_purchase_ratio DECIMAL(5,2) COMMENT '跨品牌购买率',
    
    -- 社交属性
    social_share_count INT DEFAULT 0 COMMENT '社交分享次数',
    review_length INT DEFAULT 0 COMMENT '平均评价字数',
    review_sentiment DECIMAL(3,1) COMMENT '评价情感分值(-5~5)',
    outfit_photo_count INT DEFAULT 0 COMMENT '穿搭照片数量',
    
    -- 偏好分析
    preferred_category VARCHAR(64) COMMENT '偏好品类',
    preferred_price_range VARCHAR(16) COMMENT '偏好价格带',
    preferred_fabric VARCHAR(32) COMMENT '偏好面料',
    
    -- 时间特征
    purchase_cycle INT COMMENT '平均购买周期(天)',
    seasonal_spend_ratio DECIMAL(5,2) COMMENT '季节性消费占比',
    
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id),
    UNIQUE KEY idx_dt_user_category (stat_date, user_id, category_id),
    KEY idx_user_behavior (user_id, purchase_count),
    KEY idx_brand_tier (brand_tier),
    KEY idx_style (style_preference),
    KEY idx_season (seasonality),
    KEY idx_fit_experience (fit_feedback, size_accuracy),
    KEY idx_channel_behavior (purchase_channel, repeat_purchase_flag)
) COMMENT='用户服装行为日汇总表';






DROP TABLE IF EXISTS dws_user_food_dining_agg_di;
CREATE TABLE dws_user_food_dining_agg_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键',
    stat_date DATE NOT NULL COMMENT '日期',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    user_name VARCHAR(64) COMMENT '用户姓名',
    
    -- 用户画像维度
    user_age_group VARCHAR(16) COMMENT '年龄段(18-25/26-35等)',
    health_condition VARCHAR(32) COMMENT '健康状况(如糖尿病/高血压)',
    dietary_restriction VARCHAR(64) COMMENT '饮食限制(素食/清真/无麸质等)',
    
    -- 商户维度
    restaurant_id BIGINT COMMENT '门店ID',
    restaurant_chain VARCHAR(64) COMMENT '连锁品牌',
    merchant_level VARCHAR(16) COMMENT '商户等级(米其林/黑珍珠等)',
    
    -- 基础餐饮信息
    cuisine_type VARCHAR(32) COMMENT '菜系(川菜/粤菜/西餐等)',
    cuisine_subtype VARCHAR(32) COMMENT '菜系子类(如川菜-火锅)',
    food_category VARCHAR(32) COMMENT '餐品大类(主食/小吃/饮品等)',
    
    -- 消费行为指标
    visit_count INT DEFAULT 0 COMMENT '到店次数',
    order_count INT DEFAULT 0 COMMENT '点餐数',
    total_spend DECIMAL(18,2) DEFAULT 0 COMMENT '消费总金额',
    avg_spend_per_visit DECIMAL(10,2) COMMENT '单次消费金额',
    avg_rating DECIMAL(3,1) DEFAULT 0 COMMENT '平均评分',
    tip_ratio DECIMAL(5,2) COMMENT '小费比例',
    
    -- 健康营养分析
    avg_calorie INT COMMENT '平均餐食卡路里',
    avg_nutrition_score DECIMAL(3,1) COMMENT '营养评分(1-5)',
    macro_balance VARCHAR(16) COMMENT '宏量营养素平衡(高蛋白/低碳水等)',
    sodium_level VARCHAR(10) COMMENT '钠含量等级',
    
    -- 消费场景深化
    consumption_tier VARCHAR(10) COMMENT '消费层级(经济/中端/高端)',
    group_size TINYINT DEFAULT 1 COMMENT '用餐人数',
    dining_purpose VARCHAR(20) COMMENT '用餐目的(日常/约会/商务等)',
    is_solo_dining TINYINT(1) DEFAULT 0 COMMENT '是否独自用餐',
    
    -- 外卖与配送
    delivery_type VARCHAR(10) COMMENT '用餐方式(堂食/外卖/自提)',
    delivery_time_cost INT COMMENT '平均配送时长(分钟)',
    packaging_rating TINYINT COMMENT '包装满意度(1-5)',
    
    -- 时间与频次
    is_weekend TINYINT(1) DEFAULT 0 COMMENT '是否周末',
    peak_hours_flag TINYINT DEFAULT 0 COMMENT '是否高峰时段',
    dining_frequency VARCHAR(16) COMMENT '用餐频次(低频/中频/高频)',
    
    -- 社交与互动
    social_share_count INT DEFAULT 0 COMMENT '社交分享次数',
    photo_taken_count INT DEFAULT 0 COMMENT '拍照次数',
    review_sentiment DECIMAL(3,1) COMMENT '评价情感分值(-5~5)',
    
    -- 支付与优惠
    payment_method VARCHAR(16) COMMENT '支付方式',
    coupon_usage_ratio DECIMAL(5,2) COMMENT '优惠券使用率',
    membership_flag TINYINT(1) DEFAULT 0 COMMENT '是否会员',
    
    -- 偏好分析
    preferred_cuisine VARCHAR(32) COMMENT '偏好菜系',
    preferred_price_range VARCHAR(16) COMMENT '偏好价格带',
    preferred_dining_time VARCHAR(16) COMMENT '偏好用餐时段',
    
    -- 天气与环境
    weather_condition VARCHAR(20) COMMENT '天气状况',
    temperature_range VARCHAR(10) COMMENT '温度区间',
    
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id),
    UNIQUE KEY idx_dt_user_restaurant (stat_date, user_id, restaurant_id),
    KEY idx_user_health (user_id, health_condition),
    KEY idx_cuisine_analysis (cuisine_type, cuisine_subtype),
    KEY idx_health_nutrition (avg_nutrition_score, macro_balance),
    KEY idx_consumption_scene (dining_purpose, group_size),
    KEY idx_delivery_quality (delivery_type, packaging_rating),
    KEY idx_social_behavior (social_share_count, photo_taken_count)
) COMMENT='用户餐饮行为日汇总表';


DROP TABLE IF EXISTS dws_user_housing_activity_agg_mi;
CREATE TABLE dws_user_housing_activity_agg_mi (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键',
    month DATE NOT NULL COMMENT '月份（取每月1日）',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    user_name VARCHAR(64) COMMENT '用户姓名',
    
    -- 用户画像维度
    user_age_group VARCHAR(16) COMMENT '年龄段(25-30/31-40等)',
    family_structure VARCHAR(32) COMMENT '家庭结构(单身/夫妻/三代同堂)',
    occupation_type VARCHAR(32) COMMENT '职业类型',
    
    -- 房产基础信息
    district VARCHAR(64) COMMENT '行政区',
    subdistrict VARCHAR(64) COMMENT '街道/商圈',
    property_type VARCHAR(32) COMMENT '房型(公寓/别墅/商铺等)',
    property_age TINYINT COMMENT '房龄(年)',
    floor_area DECIMAL(10,2) COMMENT '建筑面积(㎡)',
    
    -- 租房行为增强
    viewing_count INT DEFAULT 0 COMMENT '看房次数',
    virtual_viewing_count INT DEFAULT 0 COMMENT 'VR看房次数',
    contract_count INT DEFAULT 0 COMMENT '签约次数',
    avg_rent DECIMAL(18,2) DEFAULT 0 COMMENT '平均租金',
    rent_to_income_ratio DECIMAL(5,2) COMMENT '租金收入比',
    lease_term INT COMMENT '平均租期(月)',
    
    -- 购房行为深化
    is_homeowner TINYINT DEFAULT 0 COMMENT '是否业主(0否1是)',
    purchase_count INT DEFAULT 0 COMMENT '购房次数',
    avg_purchase_price DECIMAL(18,2) DEFAULT 0 COMMENT '平均购房价格',
    mortgage_flag TINYINT DEFAULT 0 COMMENT '是否有房贷(0无1有)',
    down_payment_ratio DECIMAL(5,2) COMMENT '首付比例',
    loan_term INT COMMENT '贷款年限',
    property_valuation DECIMAL(18,2) COMMENT '房产估值',
    
    -- 居住状态全景
    current_residence_type VARCHAR(20) COMMENT '当前居住类型(自有/租赁/公司宿舍等)',
    current_district VARCHAR(64) COMMENT '实际居住行政区',
    residence_duration INT COMMENT '现居住时长(月)',
    commute_time INT COMMENT '通勤时间(分钟)',
    school_district_flag TINYINT DEFAULT 0 COMMENT '是否学区房',
    
    -- 住房活动扩展
    decoration_count INT DEFAULT 0 COMMENT '装修次数',
    decoration_cost DECIMAL(18,2) COMMENT '装修花费',
    moving_count INT DEFAULT 0 COMMENT '搬家次数',
    property_consult_count INT DEFAULT 0 COMMENT '房产咨询次数',
    utility_spend DECIMAL(18,2) COMMENT '水电煤支出',
    
    -- 偏好与需求分析
    preferred_district_type TINYINT COMMENT '区域偏好类型(1工作近2学校近3环境好)',
    preferred_house_type VARCHAR(32) COMMENT '偏好户型',
    price_sensitivity VARCHAR(10) COMMENT '价格敏感度(高/中/低)',
    urgency_level VARCHAR(10) COMMENT '购房紧迫度',
    
    -- 金融属性
    credit_score INT COMMENT '信用评分',
    debt_to_income_ratio DECIMAL(5,2) COMMENT '负债收入比',
    
    -- 时间趋势
    month_over_month_change DECIMAL(5,2) COMMENT '环比变化率',
    activity_trend VARCHAR(10) COMMENT '活动趋势(上升/下降/平稳)',
    
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id),
    UNIQUE KEY idx_month_user_property (month, user_id, property_type),
    KEY idx_user_financial (user_id, credit_score, debt_to_income_ratio),
    KEY idx_property_valuation (property_type, property_valuation),
    KEY idx_location_preference (preferred_district_type, school_district_flag),
    KEY idx_market_activity (month, district, activity_trend)
) COMMENT='用户住房行为月汇总表';


DROP TABLE IF EXISTS dws_user_transport_trip_agg_di;
CREATE TABLE dws_user_transport_trip_agg_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键',
    stat_date DATE NOT NULL COMMENT '日期',
    user_id BIGINT NOT NULL COMMENT '用户ID',
	user_name VARCHAR(64) COMMENT '用户姓名',
    user_city VARCHAR(32) COMMENT '用户常驻城市',
    
    -- 基础出行信息
    vehicle_type VARCHAR(32) NOT NULL COMMENT '交通工具(快车/地铁/共享单车/自驾等)',
    vehicle_subtype VARCHAR(32) COMMENT '子类型(如经济型/豪华型/电动自行车等)',
    start_district VARCHAR(64) COMMENT '出发区域',
    end_district VARCHAR(64) COMMENT '到达区域',
    route_id VARCHAR(64) COMMENT '常用路线ID',
    
    -- 量化指标
    trip_count INT DEFAULT 0 COMMENT '出行次数',
    total_distance DECIMAL(10,2) DEFAULT 0 COMMENT '总里程(公里)',
    total_duration INT DEFAULT 0 COMMENT '总时长(分钟)',
    total_cost DECIMAL(18,2) DEFAULT 0 COMMENT '总费用',
    
    -- 出行特征
    trip_purpose VARCHAR(20) COMMENT '出行目的(通勤/商务/休闲/接送)',
    trip_mode VARCHAR(20) COMMENT '出行模式(单程/往返/多段)',
    passenger_count TINYINT DEFAULT 1 COMMENT '同行人数',
    
    -- 时间特征
    peak_hour_ratio DECIMAL(5,2) DEFAULT 0 COMMENT '高峰时段占比',
    earliest_trip TIME COMMENT '最早出行时间',
    latest_trip TIME COMMENT '最晚出行时间',
    
    -- 质量与效率
    comfort_rating TINYINT COMMENT '舒适度评分(1-5)',
    punctuality_ratio DECIMAL(5,2) COMMENT '准时率',
    congestion_level TINYINT COMMENT '平均拥堵等级(1-5)',
    
    -- 环保与健康
    estimated_co2 DECIMAL(10,2) COMMENT '碳排放量(kg)',
    calorie_burned INT COMMENT '估算燃烧卡路里',
    green_score DECIMAL(5,2) COMMENT '绿色出行评分',
    
    -- 多式联运
    transfer_count INT DEFAULT 0 COMMENT '换乘次数',
    main_transport VARCHAR(32) COMMENT '主要交通工具',
    
    -- 偏好分析
    preferred_vehicle VARCHAR(32) COMMENT '偏好交通工具',
    preferred_route VARCHAR(64) COMMENT '偏好路线',
    green_trip_ratio DECIMAL(5,2) COMMENT '绿色出行占比',
    
    -- 天气影响
    weather_condition VARCHAR(20) COMMENT '主要天气状况',
    
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id),
    UNIQUE KEY idx_dt_user_vehicle (stat_date, user_id, vehicle_type, route_id),
    KEY idx_user_route_pattern (user_id, start_district, end_district),
    KEY idx_purpose_time (trip_purpose, peak_hour_ratio),
    KEY idx_environment (green_score, estimated_co2),
    KEY idx_city_vehicle (user_city, vehicle_type)
) COMMENT='用户出行行为日汇总表';



服装消费：包含体型特征、时尚偏好、社交互动的全维度分析体系，实现从"买什么"到"为什么买"的深度洞察。

餐饮消费：融合场景目的（商务/日常）、外卖体验、商户分级和环境影响数据，构建从堂食到外卖、从口味到营养的立体化餐饮画像。

住房行为：整合房产金融属性（贷款/估值）、区位价值（学区/商圈）、居住质量（装修/通勤）和市场趋势，形成租购一体的住房需求分析平台。

出行行为：通过出行目的识别、环保指标（碳足迹）、效率评估（准时率）和城市交通OD分析，实现从交通工具记录到出行生态研究的跨越。
