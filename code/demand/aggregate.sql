CREATE TABLE IF NOT EXISTS dim_user_info_di (
    -- 主键和分区字段
    id BIGINT AUTO_INCREMENT COMMENT '自增主键ID',
    user_id BIGINT NOT NULL COMMENT '用户唯一ID',
    stat_date DATE NOT NULL COMMENT '统计日期(分区字段)',
    
    -- 基础身份信息
    user_name VARCHAR(255) COMMENT '用户姓名',
    nickname VARCHAR(50) COMMENT '用户昵称',
    login_name VARCHAR(255) COMMENT '登录名',
    password VARCHAR(64) COMMENT '登录密码（MD5保存）',
    status TINYINT COMMENT '账号状态：1正常/2冻结',
    last_login_time DATETIME COMMENT '最后登录时间',
    -- logout_time BIGINT COMMENT '最后登录时间(保留原字段，但建议使用last_login_time)',
    
    -- 性别信息(合并两种表示方式)
    -- sex VARCHAR(12) COMMENT '性别(文本描述)',
    gender TINYINT COMMENT '性别:1-男 2-女 0-未知',
    
    -- 婚姻和社会关系
    marital_status VARCHAR(20) COMMENT '婚姻状况(如: 已婚 未婚 离异)',
    relationship_status TINYINT COMMENT '感情状态:1-单身 2-恋爱中 3-已婚 4-离异',
    
    -- 出生和年龄信息
    -- birthday VARCHAR(32) COMMENT '生日(字符串格式)',
    birth_date DATETIME COMMENT '出生日期(日期格式)',
    age TINYINT COMMENT '年龄',
    
    -- 证件信息
    id_card VARCHAR(18) COMMENT '身份证编号',
    
    -- 地址信息
    home_address VARCHAR(255) COMMENT '家庭地址',
    company_address VARCHAR(255) COMMENT '公司地址',
    school_address VARCHAR(255) COMMENT '学校地址',
    native_province VARCHAR(20) COMMENT '籍贯省份',
    native_city VARCHAR(20) COMMENT '籍贯城市',
    current_province VARCHAR(20) COMMENT '当前居住省份',
    current_city VARCHAR(20) COMMENT '当前居住城市',
    city_tier TINYINT COMMENT '城市等级:1-一线 2-二线 3-三线 4-四线',
    ip_location VARCHAR(20) COMMENT '最近登录IP所在地',
    
    -- 职业和教育信息
    occupation VARCHAR(255) COMMENT '职业',
    -- work VARCHAR(255) COMMENT '工作(可能与occupation重复)',
    education_background VARCHAR(255) COMMENT '教育背景(文本描述)',
    education_level TINYINT COMMENT '教育程度:1-初中及以下 2-高中 3-大专 4-本科 5-硕士 6-博士',
    school_name VARCHAR(100) COMMENT '毕业院校',
    major VARCHAR(50) COMMENT '专业',
    
    -- 经济状况
    annual_income DECIMAL(10,2) COMMENT '年收入(万元)',
    has_mortgage BOOLEAN COMMENT '是否有房贷',
    has_car_loan BOOLEAN COMMENT '是否有车贷',
    
    -- 账号相关
    leave_reason VARCHAR(255) COMMENT '注销原因',
    
    -- 用户分层和标签
    user_level TINYINT COMMENT '用户等级：1-普通 2-白银 3-黄金 4-铂金 5-钻石',
    rfm_score VARCHAR(10) COMMENT 'rfm分值(如3-4-5)',
    user_lifecycle VARCHAR(20) COMMENT '用户生命周期：新用户、成长期、成熟期、衰退期、流失',
    is_high_value TINYINT(1) DEFAULT 0 COMMENT '是否高价值用户',
    is_premium TINYINT(1) DEFAULT 0 COMMENT '是否付费会员',
    is_content_creator TINYINT(1) DEFAULT 0 COMMENT '是否内容创作者',
    
    -- 系统字段
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (id, stat_date),
    UNIQUE KEY uk_user_date (user_id, stat_date),
    INDEX idx_city_tier (city_tier),
    INDEX idx_education (education_level),
    INDEX idx_user_level (user_level),
    INDEX idx_lifecycle (user_lifecycle)
) COMMENT='用户综合基础信息表(合并dim_user_info_di和dwd_user_basic_info_di)';




服装消费：从基础购买记录升级为包含体型特征、时尚偏好、社交互动和健康指标的全维度分析体系，实现从"买什么"到"为什么买"的深度洞察。

餐饮消费：融合场景目的（商务/日常）、外卖体验、商户分级和环境影响数据，构建从堂食到外卖、从口味到营养的立体化餐饮画像。

住房行为：整合房产金融属性（贷款/估值）、区位价值（学区/商圈）、居住质量（装修/通勤）和市场趋势，形成租购一体的住房需求分析平台。

出行行为：通过出行目的识别、环保指标（碳足迹）、效率评估（准时率）和城市交通OD分析，实现从交通工具记录到出行生态研究的跨越。