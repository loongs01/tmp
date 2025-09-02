-- 用户基础信息表
drop Table if  exists dwd_user_info_di;
Create Table if not exists dwd_user_info_di(
  id                           BIGINT AUTO_INCREMENT COMMENT '自增主键id'
  ,user_id                     BIGINT                COMMENT '用户id'
  ,stat_date                   DATE                  COMMENT '统计日期(分区字段)'
  ,user_name                   VARCHAR(255)          COMMENT '用户姓名'
  ,sex                         VARCHAR(12)           COMMENT '性别'
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
  ,created_time                 DATETIME not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
  ,updated_time                 DATETIME not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
  ,PRIMARY KEY (id,stat_date)
  ,UNIQUE KEY indx_user_date (stat_date,user_id)
)COMMENT='用户基础信息表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-06-01'))
    ,PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-07-01'))
    ,PARTITION p_current VALUES LESS THAN MAXVALUE
);

drop Table if  exists dwd_user_tier_info_di;
CREATE TABLE IF NOT EXISTS dwd_user_tier_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键id'
    ,user_id BIGINT COMMENT '用户id'
    ,stat_date DATE COMMENT '统计日期(分区字段)'
    ,user_name VARCHAR(255)          COMMENT '用户姓名'
    ,user_level TINYINT COMMENT '用户等级：1-普通 2-白银 3-黄金 4-铂金 5-钻石'
    ,rfm_score VARCHAR(10) COMMENT 'rfm分值(如3,4,5)'
    ,user_lifecycle VARCHAR(20) COMMENT '用户生命周期：新用户、成长期、成熟期、衰退期、流失'
    ,created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
    ,PRIMARY KEY (id, stat_date)
    ,UNIQUE KEY indx_user_date (stat_date, user_id)
) COMMENT='用户分层信息表';

drop Table if  exists dwd_user_tag_info_di;
CREATE TABLE IF NOT EXISTS dwd_user_tag_info_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键id'
    ,user_id BIGINT COMMENT '用户id'
    ,stat_date DATE COMMENT '统计日期(分区字段)'
    ,user_name VARCHAR(255)          COMMENT '用户姓名'
    ,user_name VARCHAR(255)          COMMENT '用户姓名'
    ,is_high_value TINYINT(1) DEFAULT 0 COMMENT '是否高价值用户'
    ,is_premium TINYINT(1) DEFAULT 0 COMMENT '是否付费会员'
    ,is_content_creator TINYINT(1) DEFAULT 0 COMMENT '是否内容创作者'
    ,created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
    ,PRIMARY KEY (id, stat_date)
    ,UNIQUE KEY indx_user_date (stat_date, user_id)
) COMMENT='用户标签信息表';

-- truncate table dim_user_info_di;
delete table dim_user_info_di where stat_date ='2025-07-01';
insert into dim_user_info_di
select
   null as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  ,t1.sex
  ,t1.id_card
  ,t1.birthday
  ,t1.home_address
  ,t1.company_address
  ,t1.school_address
  ,t1.occupation
  ,t1.education_background
  ,t1.age
  ,t1.work
  ,t1.login_name
  ,t1.password
  ,t1.status
  ,t1.logout_time
  ,t1.leave_reason
  -- ,friend_id
  ,t2.user_level
  ,t2.rfm_score
  ,t2.user_lifecycle
  ,t3.is_high_value
  ,t3.is_premium
  ,t3.is_content_creator
  ,coalesce(t1.created_time, t2.created_time, t3.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time) as updated_time
from dwd_user_info_di as t1
left join dwd_user_tier_info_di as t2
on t2.user_id=t1.user_id
and t2.stat_date=t2.stat_date
left join dwd_user_tag_info_di as t3
on t3.user_id=t1.user_id
and t3.stat_date=t1.stat_date
where t1.stat_date>'2025-05-10'
-- where t1.stat_date like'2025%'
;




insert into dim_user_info_di
select
id
,user_id
,stat_date
,user_name
,sex
,id_card
,birthday
,home_address
,company_address
,school_address
,occupation
,education_background
,age
,work
,login_name
,password
,status
,logout_time
,leave_reason
-- ,friend_id
-- 用户分层
,user_level
,rfm_score
,user_lifecycle
-- 标签体系
,is_high_value
,is_premium
,is_content_creator
,created_time
,updated_time
from
(select
   null as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  ,t1.sex
  ,t1.id_card
  ,t1.birthday
  ,t1.home_address
  ,t1.company_address
  ,t1.school_address
  ,t1.occupation
  ,t1.education_background
  ,t1.age
  ,t1.work
  ,t1.login_name
  ,t1.password
  ,t1.status
  ,t1.logout_time
  ,t1.leave_reason
  -- ,friend_id
  ,t2.user_level
  ,t2.rfm_score
  ,t2.user_lifecycle
  ,t3.is_high_value
  ,t3.is_premium
  ,t3.is_content_creator
  ,coalesce(t1.created_time, t2.created_time, t3.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time) as updated_time
  ,row_number() over(partition by t1.user_id order by t1.created_time asc) as rn
from dwd_user_info_di as t1
left join dwd_user_tier_info_di as t2
on t2.user_id=t1.user_id
-- and t2.stat_date=t2.stat_date
left join dwd_user_tag_info_di as t3
on t3.user_id=t1.user_id
-- and t3.stat_date=t1.stat_date
) as tmp
where rn=1
;



-- 用户健康信息事实表
drop Table if  exists dwd_user_basic_health_di;
CREATE TABLE dwd_user_basic_health_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id'
    ,user_id INT COMMENT '用户id'
    ,stat_date DATE COMMENT '统计日期'
    ,user_name VARCHAR(255)          COMMENT '用户姓名'
    ,height DECIMAL(5, 2) COMMENT '身高(cm)'
    ,weight DECIMAL(5, 2) COMMENT '体重(kg)'
    ,bmi DECIMAL(4, 2) COMMENT 'BMI指数'
    ,blood_type CHAR(2) COMMENT '血型(A/B/AB/O)'
    ,rh_factor CHAR(1) COMMENT 'RH值(+/-)'
    ,body_fat_rate DECIMAL(4, 2) COMMENT '体脂率(%)'
    ,first_measure_date DATE COMMENT '首次测量日期'
    ,last_measure_date DATE COMMENT '最近测量日期'
    ,created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'
    ,PRIMARY KEY (id)
    ,UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户基础健康信息表';

drop Table if  exists dwd_user_health_metrics_di;
CREATE TABLE dwd_user_health_metrics_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id'
    ,user_id INT COMMENT '用户id'
    ,stat_date DATE COMMENT '统计日期'
    ,user_name VARCHAR(255)          COMMENT '用户姓名'
    ,avg_heart_rate SMALLINT COMMENT '平均心率(次/分钟)'
    ,max_heart_rate SMALLINT COMMENT '最大心率'
    ,min_heart_rate SMALLINT COMMENT '最小心率'
    ,avg_blood_pressure VARCHAR(10) COMMENT '平均血压(如120/80)'
    ,avg_blood_oxygen DECIMAL(4, 1) COMMENT '平均血氧饱和度(%)'
    ,avg_body_temp DECIMAL(3, 1) COMMENT '平均体温(℃)'
    ,created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'
    ,PRIMARY KEY (id)
    ,UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户健康指标汇总表';

drop Table if  exists dwd_user_exercise_health_di;
CREATE TABLE dwd_user_exercise_health_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id'
    ,user_id INT COMMENT '用户id'
    ,stat_date DATE COMMENT '统计日期'
    ,user_name VARCHAR(255)          COMMENT '用户姓名'
    ,total_steps INT COMMENT '总步数'
    ,avg_daily_steps INT COMMENT '日均步数'
    ,total_calories INT COMMENT '总消耗卡路里(千卡)'
    ,total_exercise_minutes INT COMMENT '总运动时长(分钟)'
    ,avg_sleep_duration DECIMAL(4, 1) COMMENT '平均睡眠时长(小时)'
    ,sleep_quality_score TINYINT COMMENT '睡眠质量评分(1-100)'
    ,created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'
    ,PRIMARY KEY (id)
    ,UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户运动健康指标表';

drop Table if  exists dwd_user_health_risks_di;
CREATE TABLE dwd_user_health_risks_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id'
    ,user_id INT COMMENT '用户id'
    ,stat_date DATE COMMENT '统计日期'
    ,user_name VARCHAR(255)          COMMENT '用户姓名'
    ,is_high_blood_pressure TINYINT(1) DEFAULT 0 COMMENT '是否高血压风险'
    ,is_heart_disease_risk TINYINT(1) DEFAULT 0 COMMENT '是否心脏病风险'
    ,is_obesity TINYINT(1) DEFAULT 0 COMMENT '是否肥胖'
    ,is_sleep_disorder TINYINT(1) DEFAULT 0 COMMENT '是否睡眠障碍'
    ,allergies VARCHAR(255) COMMENT '过敏史'
    ,chronic_diseases VARCHAR(255) COMMENT '慢性病史'
    ,created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'
    ,PRIMARY KEY (id)
    ,UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户健康风险标签表';

drop Table if  exists dwd_user_health_behaviors_di;
CREATE TABLE dwd_user_health_behaviors_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id'
    ,user_id INT COMMENT '用户id'
    ,stat_date DATE COMMENT '统计日期'
    ,user_name VARCHAR(255)          COMMENT '用户姓名'
    ,exercise_frequency TINYINT COMMENT '运动频率(1-不运动2-偶尔3-经常4-每天)'
    ,diet_habit TINYINT COMMENT '饮食习惯(1-不健康2-一般3-健康)'
    ,health_management_flag TINYINT(1) DEFAULT 0 COMMENT '是否进行健康管理'
    ,created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'
    ,PRIMARY KEY (id)
    ,UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户健康行为标签表';

insert into dws_user_health_summary_di
select
  null as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  ,t1.height
  ,t1.weight
  ,t1.bmi
  ,t1.blood_type
  ,t1.rh_factor
  ,t1.body_fat_rate
  ,t1.first_measure_date
  ,t1.last_measure_date
  ,t2.avg_heart_rate
  ,t2.max_heart_rate
  ,t2.min_heart_rate
  ,t2.avg_blood_pressure
  ,t2.avg_blood_oxygen
  ,t2.avg_body_temp
  ,t3.total_steps
  ,t3.avg_daily_steps
  ,t3.total_calories
  ,t3.total_exercise_minutes
  ,t3.avg_sleep_duration
  ,t3.sleep_quality_score
  ,t4.is_high_blood_pressure
  ,t4.is_heart_disease_risk
  ,t4.is_obesity
  ,t4.is_sleep_disorder
  ,t4.allergies
  ,t4.chronic_diseases
  ,t5.exercise_frequency
  ,t5.diet_habit
  ,t5.health_management_flag
  ,coalesce(t1.created_time, t2.created_time, t3.created_time, t4.created_time, t5.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time, t4.updated_time, t5.updated_time) as updated_time
from dwd_user_basic_health_di as t1
left join dwd_user_health_metrics_di as t2
on t1.user_id = t2.user_id
-- and t1.stat_date = t2.stat_date
left join dwd_user_exercise_health_di as t3
on t1.user_id = t3.user_id
-- and t1.stat_date = t3.stat_date
left join dwd_user_health_risks_di as t4
on t1.user_id = t4.user_id
-- and t1.stat_date = t4.stat_date
left join dwd_user_health_behaviors_di as t5
on t1.user_id = t5.user_id
-- and t1.stat_date = t5.stat_date
;
insert into dws_user_health_summary_di
select 
id
,user_id
,stat_date
,user_name
,height
,weight
,bmi
,blood_type
,rh_factor
,body_fat_rate
,first_measure_date
,last_measure_date
,avg_heart_rate
,max_heart_rate
,min_heart_rate
,avg_blood_pressure
,avg_blood_oxygen
,avg_body_temp
,total_steps
,avg_daily_steps
,total_calories
,total_exercise_minutes
,avg_sleep_duration
,sleep_quality_score
,is_high_blood_pressure
,is_heart_disease_risk
,is_obesity
,is_sleep_disorder
,allergies
,chronic_diseases
,exercise_frequency
,diet_habit
,health_management_flag
,created_time
,updated_time
from(
select
  null as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  ,t1.height
  ,t1.weight
  ,t1.bmi
  ,t1.blood_type
  ,t1.rh_factor
  ,t1.body_fat_rate
  ,t1.first_measure_date
  ,t1.last_measure_date
  ,t2.avg_heart_rate
  ,t2.max_heart_rate
  ,t2.min_heart_rate
  ,t2.avg_blood_pressure
  ,t2.avg_blood_oxygen
  ,t2.avg_body_temp
  ,t3.total_steps
  ,t3.avg_daily_steps
  ,t3.total_calories
  ,t3.total_exercise_minutes
  ,t3.avg_sleep_duration
  ,t3.sleep_quality_score
  ,t4.is_high_blood_pressure
  ,t4.is_heart_disease_risk
  ,t4.is_obesity
  ,t4.is_sleep_disorder
  ,t4.allergies
  ,t4.chronic_diseases
  ,t5.exercise_frequency
  ,t5.diet_habit
  ,t5.health_management_flag
  ,coalesce(t1.created_time, t2.created_time, t3.created_time, t4.created_time, t5.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time, t4.updated_time, t5.updated_time) as updated_time
  ,row_number() over(partition by t1.user_id order by t1.created_time asc) as rn
from dwd_user_basic_health_di as t1
left join dwd_user_health_metrics_di as t2
on t1.user_id = t2.user_id
-- and t1.stat_date = t2.stat_date
left join dwd_user_exercise_health_di as t3
on t1.user_id = t3.user_id
-- and t1.stat_date = t3.stat_date
left join dwd_user_health_risks_di as t4
on t1.user_id = t4.user_id
-- and t1.stat_date = t4.stat_date
left join dwd_user_health_behaviors_di as t5
on t1.user_id = t5.user_id
-- and t1.stat_date = t5.stat_date
)tmp
where rn=1
;












-- 用户职业信息明细表：
-- 同构 
-- dwd_user_career_info与dws_user_career_info
DROP TABLE IF EXISTS dwd_user_career_info;
CREATE TABLE dwd_user_career_info (
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




-- 用户教育与学习信息事实表
DROP TABLE IF EXISTS dwd_user_education_background_di;
CREATE TABLE dwd_user_education_background_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT NOT NULL COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    education_level VARCHAR(20) COMMENT '教育程度(如: 高中 本科 硕士 博士)',
    major VARCHAR(100) COMMENT '专业名称',
    graduation_year INT COMMENT '毕业年份',
    school_name VARCHAR(255) COMMENT '毕业院校名称',
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date),
    KEY idx_education_level (education_level),
    KEY idx_major (major)
) COMMENT='用户教育背景明细表';

DROP TABLE IF EXISTS dwd_user_learning_behavior_di;
CREATE TABLE dwd_user_learning_behavior_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT NOT NULL COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    total_courses_completed INT COMMENT '完成课程总数',
    avg_course_score DECIMAL(4, 2) COMMENT '平均课程得分',
    total_learning_hours INT COMMENT '总学习时长(小时)',
    avg_daily_learning_time DECIMAL(4, 2) COMMENT '日均学习时长(小时)',
    learning_frequency TINYINT COMMENT '学习频率(1-低 2-中 3-高)',
    first_learning_date DATE COMMENT '首次学习日期',
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户学习行为明细表';


DROP TABLE IF EXISTS dwd_user_skill_level_di;
CREATE TABLE dwd_user_skill_level_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT NOT NULL COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    skill_level_coding TINYINT COMMENT '技能水平(1-初级 2-中级 3-高级)',
    certification_name VARCHAR(255) COMMENT '获得证书名称',
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date),
    KEY idx_skill_level_coding (skill_level_coding)
) COMMENT='用户技能水平明细表';

DROP TABLE IF EXISTS dwd_user_learning_preference_di;
CREATE TABLE dwd_user_learning_preference_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT NOT NULL COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    preferred_learning_method VARCHAR(50) COMMENT '偏好学习方式(如: 在线课程 书籍 培训)',
    preferred_subject VARCHAR(100) COMMENT '偏好学习科目',
    learning_goal VARCHAR(255) COMMENT '学习目标',
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户学习偏好明细表';

insert into dws_user_education_learning_di
select
  null  as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  ,t1.education_level
  ,t1.major
  ,t1.graduation_year
  ,t1.school_name
  ,t2.total_courses_completed
  ,t2.avg_course_score
  ,t2.total_learning_hours
  ,t2.avg_daily_learning_time
  ,t2.learning_frequency
  ,t2.first_learning_date
  ,t3.skill_level_coding
  ,t3.certification_name
  ,t4.preferred_learning_method
  ,t4.preferred_subject
  ,t4.learning_goal
  ,coalesce(t1.created_time, t2.created_time, t3.created_time, t4.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time, t4.updated_time) as updated_time
from dwd_user_education_background_di as t1
left join dwd_user_learning_behavior_di as t2
on t2.user_id=t1.user_id
   and t2.stat_date=t1.stat_date
left join dwd_user_skill_level_di as t3
on t3.user_id=t1.user_id
   and t3.stat_date=t1.stat_date
left join dwd_user_learning_preference_di as t4
on t4.user_id=t1.user_id
   and t4.stat_date=t1.stat_date


insert into dws_user_education_learning_di
select
  id
  ,user_id
  ,stat_date
  ,user_name
  ,education_level
  ,major
  ,graduation_year
  ,school_name
  ,total_courses_completed
  ,avg_course_score
  ,total_learning_hours
  ,avg_daily_learning_time
  ,learning_frequency
  ,first_learning_date
  ,skill_level_coding
  ,certification_name
  ,preferred_learning_method
  ,preferred_subject
  ,learning_goal
  ,created_time
  ,updated_time
from(
select
  null  as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  ,t1.education_level
  ,t1.major
  ,t1.graduation_year
  ,t1.school_name
  ,t2.total_courses_completed
  ,t2.avg_course_score
  ,t2.total_learning_hours
  ,t2.avg_daily_learning_time
  ,t2.learning_frequency
  ,t2.first_learning_date
  ,t3.skill_level_coding
  ,t3.certification_name
  ,t4.preferred_learning_method
  ,t4.preferred_subject
  ,t4.learning_goal
  ,coalesce(t1.created_time, t2.created_time, t3.created_time, t4.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time, t4.updated_time) as updated_time
  ,row_number() over(partition by t1.user_id order by t1.created_time asc) as rn
from dwd_user_education_background_di as t1
left join dwd_user_learning_behavior_di as t2
on t2.user_id=t1.user_id
   and t2.stat_date=t1.stat_date
left join dwd_user_skill_level_di as t3
on t3.user_id=t1.user_id
   and t3.stat_date=t1.stat_date
left join dwd_user_learning_preference_di as t4
on t4.user_id=t1.user_id
   and t4.stat_date=t1.stat_date
) as tmp
where rn=1

-- 用户家庭关系信息事实表 
-- 同构
-- dwd_user_family_relations_di
-- dws_user_family_relations_di
drop Table if  exists dwd_user_family_relations_di;
CREATE TABLE dwd_user_family_relations_di (
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
drop Table if  exists dwd_user_financial_management_di;
CREATE TABLE dwd_user_financial_management_di (
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




-- 用户休闲娱乐信息事实表
CREATE TABLE dwd_user_leisure_behavior_di (
    id                  BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id             INT NOT NULL COMMENT '用户id',
    stat_date           DATE NOT NULL COMMENT '统计日期',
    user_name           VARCHAR(255)            COMMENT '用户姓名',
    age_group VARCHAR(20) COMMENT '年龄组(如: 18-24 25-34)',
    gender CHAR(1) COMMENT '性别(M-男 F-女 U-未知)',
    region VARCHAR(50) COMMENT '地区',
    daily_leisure_time  INT COMMENT '日均休闲娱乐时长(分钟)',
    weekly_leisure_days INT COMMENT '每周休闲娱乐天数',
    leisure_activity_types VARCHAR(255) COMMENT '休闲活动类型(如: 电影 游戏 阅读 运动 旅游)',
    preferred_activity     VARCHAR(50) COMMENT '首选休闲活动',
    leisure_activity_frequency VARCHAR(50) COMMENT '休闲活动频率(如: 每天 每周 每月)',
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户休闲娱乐行为明细表';

CREATE TABLE dwd_user_entertainment_spending_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT NOT NULL COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    monthly_leisure_spending DECIMAL(20, 2) COMMENT '月均休闲娱乐消费(元)',
    spending_categories VARCHAR(255) COMMENT '消费类别(如: 电影票 游戏充值 书籍 运动装备)',
    preferred_payment_method VARCHAR(50) COMMENT '首选支付方式(如: 支付宝 微信 信用卡)',
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户娱乐消费明细表';

CREATE TABLE dwd_user_content_preference_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT NOT NULL COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    preferred_genres VARCHAR(255) COMMENT '偏好内容类型(如: 动作 喜剧 科幻 悬疑)',
    favorite_artists_or_celebrities VARCHAR(255) COMMENT '喜欢的艺人或名人',
    followed_entertainment_accounts INT COMMENT '关注的娱乐账号数量',
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户内容偏好明细表';

CREATE TABLE dwd_user_social_interaction_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT NOT NULL COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    leisure_social_frequency VARCHAR(50) COMMENT '休闲社交频率(如: 每天 每周 每月)',
    preferred_social_platforms VARCHAR(255) COMMENT '首选社交平台(如: 微信 微博 抖音)',
    leisure_group_activities VARCHAR(255) COMMENT '参与的休闲团体活动(如: 读书会 运动俱乐部)',
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户社交互动明细表';


CREATE TABLE dwd_user_device_platform_usage_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT NOT NULL COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    preferred_devices VARCHAR(255) COMMENT '常用设备(如: 手机 平板 电脑 电视)',
    leisure_app_usage VARCHAR(255) COMMENT '常用休闲娱乐应用(如: 腾讯视频 王者荣耀 豆瓣)',
    daily_app_usage_time INT COMMENT '日均应用使用时长(分钟)',
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户设备与平台使用明细表';


insert into dws_user_leisure_entertainment_di
select
  null as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  ,t1.age_group
  ,t1.gender
  ,t1.region
  ,t1.daily_leisure_time
  ,t1.weekly_leisure_days
  ,t1.leisure_activity_types
  ,t1.preferred_activity
  ,t1.leisure_activity_frequency
  ,t2.monthly_leisure_spending
  ,t2.spending_categories
  ,t2.preferred_payment_method
  ,t3.preferred_genres
  ,t3.favorite_artists_or_celebrities
  ,t3.followed_entertainment_accounts
  ,t4.leisure_social_frequency
  ,t4.preferred_social_platforms
  ,t4.leisure_group_activities
  ,t5.preferred_devices
  ,t5.leisure_app_usage
  ,t5.daily_app_usage_time
  ,coalesce(t1.created_time, t2.created_time, t3.created_time, t4.created_time, t5.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time, t4.updated_time, t5.updated_time) as updated_time
from dwd_user_leisure_behavior_di as t1
left join dwd_user_entertainment_spending_di as t2
on t2.user_id=t1.user_id
   and t2.stat_date=t1.stat_date
left join dwd_user_content_preference_di as t3
on t3.user_id=t1.user_id
   and t3.stat_date=t1.stat_date
left join dwd_user_social_interaction_di as t4
on t4.user_id=t1.user_id
   and t4.stat_date=t1.stat_date
left join dwd_user_device_platform_usage_di as t5
on t5.user_id=t1.user_id
   and t5.stat_date=t1.stat_date





insert into dws_user_leisure_entertainment_di
select 
  id
  ,user_id
  ,stat_date
  ,user_name
  ,age_group
  ,gender
  ,region
  ,daily_leisure_time
  ,weekly_leisure_days
  ,leisure_activity_types
  ,preferred_activity
  ,leisure_activity_frequency
  ,monthly_leisure_spending
  ,spending_categories
  ,preferred_payment_method
  ,preferred_genres
  ,favorite_artists_or_celebrities
  ,followed_entertainment_accounts
  ,leisure_social_frequency
  ,preferred_social_platforms
  ,leisure_group_activities
  ,preferred_devices
  ,leisure_app_usage
  ,daily_app_usage_time
  ,created_time
  ,updated_time
from(
select
  null as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  ,t1.age_group
  ,t1.gender
  ,t1.region
  ,t1.daily_leisure_time
  ,t1.weekly_leisure_days
  ,t1.leisure_activity_types
  ,t1.preferred_activity
  ,t1.leisure_activity_frequency
  ,t2.monthly_leisure_spending
  ,t2.spending_categories
  ,t2.preferred_payment_method
  ,t3.preferred_genres
  ,t3.favorite_artists_or_celebrities
  ,t3.followed_entertainment_accounts
  ,t4.leisure_social_frequency
  ,t4.preferred_social_platforms
  ,t4.leisure_group_activities
  ,t5.preferred_devices
  ,t5.leisure_app_usage
  ,t5.daily_app_usage_time
  ,coalesce(t1.created_time, t2.created_time, t3.created_time, t4.created_time, t5.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time, t4.updated_time, t5.updated_time) as updated_time
  ,row_number() over(partition by t1.user_id order by t1.created_time asc) as rn
from dwd_user_leisure_behavior_di as t1
left join dwd_user_entertainment_spending_di as t2
on t2.user_id=t1.user_id
   -- and t2.stat_date=t1.stat_date
left join dwd_user_content_preference_di as t3
on t3.user_id=t1.user_id
   -- and t3.stat_date=t1.stat_date
left join dwd_user_social_interaction_di as t4
on t4.user_id=t1.user_id
   -- and t4.stat_date=t1.stat_date
left join dwd_user_device_platform_usage_di as t5
on t5.user_id=t1.user_id
   -- and t5.stat_date=t1.stat_date
) as tmp
where rn=1








-- 用户人生事件信息事实表
-- 同构
-- dws_user_life_events_di

DROP TABLE IF EXISTS dwd_user_life_events_di;

CREATE TABLE dwd_user_life_events_di (
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
    user_region VARCHAR(50) COMMENT '地区',

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




-- 用户特征汇总宽表source
-- add
DROP TABLE IF EXISTS dwd_user_comprehensive_profile_di;
CREATE TABLE IF NOT EXISTS dwd_user_comprehensive_profile_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',

    -- 饮食偏好
    favorite_foods VARCHAR(255) COMMENT '喜欢吃的东西',
    dietary_restrictions VARCHAR(255) COMMENT '饮食限制(如: 素食 无麸质)',

    -- 颜色偏好
    favorite_colors VARCHAR(255) COMMENT '喜欢的颜色',

    -- 兴趣爱好
    hobbies VARCHAR(255) COMMENT '兴趣爱好',
    favorite_games VARCHAR(255) COMMENT '喜欢的游戏',
    favorite_books VARCHAR(255) COMMENT '喜欢的书籍',
    favorite_movies VARCHAR(255) COMMENT '喜欢的电影',

    -- 个人发展
    achievements VARCHAR(255) COMMENT '用户成就',
    dreams VARCHAR(255) COMMENT '用户梦想',
    ideals VARCHAR(255) COMMENT '用户理想',
    plans VARCHAR(255) COMMENT '用户计划',
    goals VARCHAR(255) COMMENT '用户目标',

    -- 想法与灵感
    thoughts TEXT COMMENT '想法',
    inspirations TEXT COMMENT '灵感',

    -- 记忆
    family_memories TEXT COMMENT '家人记忆',
    friend_memories TEXT COMMENT '朋友记忆',
    colleague_memories TEXT COMMENT '同事记忆',

    -- 旅行信息
    visited_places VARCHAR(255) COMMENT '曾经去哪里旅行',
    desired_travel_places VARCHAR(255) COMMENT '想去哪里旅行',

    -- 时间戳
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',

    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date) COMMENT '唯一键约束，确保每个用户在每个统计日期只有一条记录'
) COMMENT='用户综合属性表(包含饮食偏好、颜色偏好、兴趣爱好、个人发展、想法灵感、记忆、旅行信息)';





DROP TABLE IF EXISTS dwd_user_social_relationships_di;
CREATE TABLE IF NOT EXISTS dwd_user_social_relationships_di (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
    user_id INT COMMENT '用户id',
    stat_date DATE NOT NULL COMMENT '统计日期',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    family_member_id bigint COMMENT '家人ID',
    family_member_name VARCHAR(255) COMMENT '家人',
    friend_id bigint COMMENT '朋友ID',
    friend_name VARCHAR(255) COMMENT '朋友',
    colleague_id bigint COMMENT '同事ID',
    colleague_name VARCHAR(255) COMMENT '同事',
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户社交关系表';

insert into dws_user_characteristic_aggregate_di
select
  null as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  ,t1.sex
  ,t1.id_card
  ,t1.birthday
  ,t1.home_address
  ,t1.company_address
  ,t1.school_address
  ,t1.occupation
  ,t1.education_background
  ,t1.age
  ,t1.work
  ,t1.login_name
  ,t1.password
  ,t1.status
  ,t1.logout_time
  ,t1.leave_reason
  -- ,friend_id
  ,t1.user_level
  ,t1.rfm_score
  ,t1.user_lifecycle
  ,t1.is_high_value
  ,t1.is_premium
  ,t1.is_content_creator
  ,t2.height
  ,t2.weight
  ,t2.bmi
  ,t2.blood_type
  ,t2.rh_factor
  ,t2.body_fat_rate
  ,t2.first_measure_date
  ,t2.last_measure_date
  ,t2.avg_heart_rate
  ,t2.max_heart_rate
  ,t2.min_heart_rate
  ,t2.avg_blood_pressure
  ,t2.avg_blood_oxygen
  ,t2.avg_body_temp
  ,t2.total_steps
  ,t2.avg_daily_steps
  ,t2.total_calories
  ,t2.total_exercise_minutes
  ,t2.avg_sleep_duration
  ,t2.sleep_quality_score
  ,t2.is_high_blood_pressure
  ,t2.is_heart_disease_risk
  ,t2.is_obesity
  ,t2.is_sleep_disorder
  ,t2.allergies
  ,t2.chronic_diseases
  ,t2.exercise_frequency
  ,t2.diet_habit
  ,t2.health_management_flag
  ,t4.family_member_id
  ,t4.family_member_name
  ,t4.friend_id
  ,t4.friend_name
  ,t4.colleague_id
  ,t4.colleague_name
  ,t3.favorite_foods
  ,t3.dietary_restrictions
  ,t3.favorite_colors
  ,t3.hobbies
  ,t3.favorite_games
  ,t3.favorite_books
  ,t3.favorite_movies
  ,t3.achievements
  ,t3.dreams
  ,t3.ideals
  ,t3.plans
  ,t3.goals
  ,t3.thoughts
  ,t3.inspirations
  ,t3.family_memories
  ,t3.friend_memories
  ,t3.colleague_memories
  ,t3.visited_places
  ,t3.desired_travel_places
  ,coalesce(t1.created_time, t2.created_time, t3.created_time, t4.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time, t4.updated_time) as updated_time
from dim_user_info_di as t1
right join dws_user_health_summary_di as t2
on t2.user_id=t1.user_id
   -- and t2.stat_date=t1.stat_date
right join dwd_user_comprehensive_profile_di as t3
on t3.user_id=t1.user_id
   -- and t3.stat_date=t1.stat_date
right join dwd_user_social_relationships_di as t4
on t4.user_id=t1.user_id
   -- and t4.stat_date=t1.stat_date




-- 用户命理推断事实表
DROP TABLE IF EXISTS dwd_user_personality_analysis_di;
CREATE TABLE IF NOT EXISTS dwd_user_personality_analysis_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '分析日期(分区字段)',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    personality_keywords VARCHAR(255) COMMENT '性格关键词(逗号分隔)',
    personality_overview TEXT COMMENT '性格概览',
    personality_strengths TEXT COMMENT '性格优点',
    personality_weaknesses TEXT COMMENT '性格缺点',
    strengths_suggestions TEXT COMMENT '扬长建议',
    weaknesses_suggestions TEXT COMMENT '避短建议',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id, stat_date),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户性格分析明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-07-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-08-01')),
    PARTITION p202503 VALUES LESS THAN (TO_DAYS('2025-09-01')),
    PARTITION p_current VALUES LESS THAN MAXVALUE
);

DROP TABLE IF EXISTS dwd_user_health_symptoms_di;
CREATE TABLE IF NOT EXISTS dwd_user_health_symptoms_di (
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    stat_date DATE NOT NULL COMMENT '分析日期(分区字段)',
    user_name VARCHAR(255)            COMMENT '用户姓名',
    symptom VARCHAR(255) COMMENT '症状',
    symptom_description TEXT COMMENT '症状描述',
    symptom_analysis TEXT COMMENT '症状分析',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id, stat_date),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户健康症状明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-07-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-08-01')),
    PARTITION p202503 VALUES LESS THAN (TO_DAYS('2025-09-01')),
    PARTITION p_current VALUES LESS THAN MAXVALUE
);


DROP TABLE IF EXISTS dwd_user_treatment_methods_di;
CREATE TABLE IF NOT EXISTS dwd_user_treatment_methods_di (
    id BIGINT AUTO_INCREMENT       COMMENT '自增主键ID',
    user_id BIGINT NOT NULL        COMMENT '用户ID',
    stat_date DATE NOT NULL        COMMENT '分析日期(分区字段)',
    user_name VARCHAR(255)         COMMENT '用户姓名',
    diet_recommendations TEXT      COMMENT '饮食调理建议',
    herbal_recommendations TEXT    COMMENT '中药调理建议',
    lifestyle_recommendations TEXT COMMENT '生活方式建议',
    acupoint_recommendations TEXT  COMMENT '穴位调理建议',
    other_recommendations TEXT     COMMENT '其他调理建议',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id, stat_date),
    UNIQUE KEY idx_user_date (user_id, stat_date)
) COMMENT='用户调理方法明细表'
PARTITION BY RANGE (TO_DAYS(stat_date)) (
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-07-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-08-01')),
    PARTITION p202503 VALUES LESS THAN (TO_DAYS('2025-09-01')),
    PARTITION p_current VALUES LESS THAN MAXVALUE
);

insert into dws_user_destiny_analysis_di
select
  null as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  -- 性格分析部分
  ,t1.personality_keywords
  ,t1.personality_overview
  ,t1.personality_strengths
  ,t1.personality_weaknesses
  ,t1.strengths_suggestions
  ,t1.weaknesses_suggestions
  ,t2.symptom
  ,t2.symptom_description
  ,t2.symptom_analysis
  ,t3.diet_recommendations
  ,t3.herbal_recommendations
  ,t3.lifestyle_recommendations
  ,t3.acupoint_recommendations
  ,t3.other_recommendations
  ,coalesce(t1.created_time, t2.created_time, t3.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time) as updated_time
from dwd_user_personality_analysis_di as t1
left join dwd_user_health_symptoms_di as t2
on t2.user_id=t1.user_id
   and t2.stat_date=t1.stat_date
left join dwd_user_treatment_methods_di as t3
on t3.user_id=t1.user_id
   and t3.stat_date=t1.stat_date
;



insert into dws_user_destiny_analysis_di
select 
   id
  ,user_id
  ,stat_date
  ,user_name
  -- 性格分析部分
  ,personality_keywords
  ,personality_overview
  ,personality_strengths
  ,personality_weaknesses
  ,strengths_suggestions
  ,weaknesses_suggestions
  ,symptom
  ,symptom_description
  ,symptom_analysis
  ,diet_recommendations
  ,herbal_recommendations
  ,lifestyle_recommendations
  ,acupoint_recommendations
  ,other_recommendations
  ,created_time
  ,updated_time
from 
(
select
  null as id
  ,t1.user_id
  ,t1.stat_date
  ,t1.user_name
  -- 性格分析部分
  ,t1.personality_keywords
  ,t1.personality_overview
  ,t1.personality_strengths
  ,t1.personality_weaknesses
  ,t1.strengths_suggestions
  ,t1.weaknesses_suggestions
  ,t2.symptom
  ,t2.symptom_description
  ,t2.symptom_analysis
  ,t3.diet_recommendations
  ,t3.herbal_recommendations
  ,t3.lifestyle_recommendations
  ,t3.acupoint_recommendations
  ,t3.other_recommendations
  ,coalesce(t1.created_time, t2.created_time, t3.created_time) as created_time
  ,coalesce(t1.updated_time, t2.updated_time, t3.updated_time) as updated_time
  ,row_number() over(partition by t1.user_id,t1.stat_date order by t1.created_time asc) as rn
from dwd_user_personality_analysis_di as t1
left join dwd_user_health_symptoms_di as t2
on t2.user_id=t1.user_id
   and t2.stat_date=t1.stat_date
left join dwd_user_treatment_methods_di as t3
on t3.user_id=t1.user_id
   and t3.stat_date=t1.stat_date
) as tmp 
where rn=1


,floor(rand() * 100000) as college_entrance_exam_rank
,floor(rand() * 1000)as college_entrance_exam_score
,floor(rand() * 100000) as postgraduate_entrance_exam_rank
,floor(rand() * 1000) as postgraduate_entrance_exam_score
,date_add('2010-01-01', interval floor(rand() * 365 * 10) day) as admission_date
,date_add('2014-01-01', interval floor(rand() * 365 * 10) day) as graduation_date

select 
    date_add(
        '2025-05-15',  -- 你的基础日期
        interval 
            floor(rand() * 24) * 3600 +  -- 随机小时（0-23）
            floor(rand() * 60) * 60 +    -- 随机分钟（0-59）
            floor(rand() * 60)           -- 随机秒（0-59）
        second
    ) as random_datetime