CREATE TABLE ods_user_interaction_di (
    id                BIGINT  AUTO_INCREMENT COMMENT '自增主键id'
    ,stat_date        DATE                   COMMENT '统计日期(分区字段)'
    ,interaction_id   BIGINT                 COMMENT '交互记录id'
    ,user_id          BIGINT                 COMMENT '用户id'
    ,session_id       VARCHAR(255)           COMMENT '用户会话的唯一标识符'
    ,interaction_time DATETIME               COMMENT '交互发生的时间'
    ,interaction_type VARCHAR(50)            COMMENT '交互类型（如提问、反馈、评价等）'
    ,query_text       TEXT                   COMMENT '用户输入的查询文本'
    ,response_text    TEXT                   COMMENT '系统生成的响应文本'
    ,response_time    DATETIME               COMMENT '系统生成响应的时间'
    ,created_time     DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time     DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
    ,PRIMARY KEY (id,stat_date)
    ,UNIQUE KEY indx_date_interaction (stat_date,interaction_id)
) COMMENT '用户交互表：记录用户与系统的每一次交互，包括提问、系统响应以及用户可能的反馈';

CREATE TABLE ods_retrieved_content_di (
    id                BIGINT  AUTO_INCREMENT COMMENT '自增主键id'
    ,stat_date        DATE                   COMMENT '统计日期(分区字段)'
    ,retrieval_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '检索记录id'
    ,interaction_id BIGINT NOT NULL COMMENT '交互记录ID'
    ,document_id VARCHAR(255) NOT NULL COMMENT '检索到的文档id'
    ,document_title VARCHAR(255) COMMENT '文档标题'
    ,document_content TEXT COMMENT '文档内容（或内容摘要）'
    ,retrieval_score FLOAT COMMENT '检索得分，表示文档与查询的相关性'
    ,retrieval_time DATETIME NOT NULL COMMENT '检索发生的时间'
    ,created_time     DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time     DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
    ,PRIMARY KEY (id,stat_date)
    ,FOREIGN KEY (interaction_id) REFERENCES ods_user_interaction_di(interaction_id)
) COMMENT '检索内容表:记录每次交互中检索到的相关文档信息，包括文档内容、相关性得分等';
 

CREATE TABLE ods_context_info_di (
    id                BIGINT  AUTO_INCREMENT COMMENT '自增主键id'
    ,stat_date        DATE                   COMMENT '统计日期(分区字段)'
    ,context_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '上下文记录id'
    ,interaction_id BIGINT NOT NULL COMMENT '交互记录ID'
    ,context_type VARCHAR(50) NOT NULL COMMENT '上下文类型（如用户历史查询、系统推荐内容等）'
    ,context_data TEXT NOT NULL COMMENT '上下文的具体数据（如JSON格式的历史查询列表）'
    ,context_time DATETIME NOT NULL COMMENT '上下文记录的时间'
    ,created_time     DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'
    ,updated_time     DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
    ,PRIMARY KEY (id,stat_date)
    ,FOREIGN KEY (interaction_id) REFERENCES ods_user_interaction_di(interaction_id)
) COMMENT '上下文信息表:记录与交互相关的上下文信息，如用户的历史查询记录，这些信息可能用于增强生成模型的响应';

-- modify
CREATE TABLE ods_user_interaction_di (
    interaction_id BIGINT AUTO_INCREMENT COMMENT '交互记录ID',
    stat_date DATE NOT NULL COMMENT '统计日期(分区字段)',
    session_id VARCHAR(64) NOT NULL COMMENT '会话ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
	user_name                   VARCHAR(255)          COMMENT '用户姓名',
    interaction_seq INT NOT NULL COMMENT '会话中的交互序号',
    interaction_time DATETIME NOT NULL COMMENT '交互发生的时间',
    interaction_type VARCHAR(20) NOT NULL COMMENT '交互类型(question/feedback/evaluation)',
    interaction_mode VARCHAR(20) NOT NULL COMMENT '交互模式(model/rag/hybrid)',
    query_text TEXT COMMENT '用户输入的查询文本',
    response_text TEXT COMMENT '系统生成的响应文本',
    response_source VARCHAR(50) COMMENT '响应来源(model/knowledge_base/hybrid)',
    response_time DATETIME COMMENT '系统生成响应的时间',
    satisfaction_score TINYINT DEFAULT NULL COMMENT '用户满意度评分(1-5, NULL:未评价)',
    model_name VARCHAR(64) COMMENT '使用的模型名称',
    rag_config JSON COMMENT 'RAG配置信息',
    is_rag_used TINYINT(1) DEFAULT 0 COMMENT '是否使用了RAG技术',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (interaction_id, stat_date),
    KEY idx_session_id (session_id),
    KEY idx_user_id (user_id),
    KEY idx_interaction_time (interaction_time),
    KEY idx_interaction_mode (interaction_mode)
) COMMENT '用户系统交互表：记录用户与系统的每一次交互';


CREATE TABLE ods_retrieval_content_di (
    retrieval_id BIGINT AUTO_INCREMENT COMMENT '检索记录ID',
    stat_date DATE NOT NULL COMMENT '统计日期(分区字段)',
    interaction_id BIGINT NOT NULL COMMENT '关联交互ID',
    session_id VARCHAR(64) NOT NULL COMMENT '会话ID',
    document_id VARCHAR(64) NOT NULL COMMENT '检索到的文档id',
    doc_title VARCHAR(512) COMMENT '文档标题',
    doc_content TEXT COMMENT '文档内容摘要',
    retrieval_score FLOAT COMMENT '检索得分',
    retrieval_source VARCHAR(50) NOT NULL COMMENT '检索来源(vector_db/keyword/hybrid)',
    vector_db_name VARCHAR(64) COMMENT '使用的向量数据库名称',
    retrieval_time DATETIME NOT NULL COMMENT '检索发生的时间',
    is_used_in_response TINYINT(1) DEFAULT 0 COMMENT '是否用于最终响应',
    retrieval_query TEXT COMMENT '检索使用的查询语句',
    retrieval_strategy JSON COMMENT '检索策略详情',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (retrieval_id, stat_date),
    KEY idx_interaction_id (interaction_id),
    KEY idx_session_id (session_id),
    KEY idx_document_id (document_id),
    KEY idx_retrieval_time (retrieval_time),
    KEY idx_retrieval_source (retrieval_source)
) COMMENT '检索内容表:记录每次交互中检索到的相关文档信息';



CREATE TABLE ods_context_info_di (
    context_id BIGINT AUTO_INCREMENT COMMENT '上下文记录ID',
    stat_date DATE NOT NULL COMMENT '统计日期(分区字段)',
    interaction_id BIGINT COMMENT '关联交互ID',
    session_id VARCHAR(64) NOT NULL COMMENT '会话ID',
    context_type VARCHAR(50) NOT NULL COMMENT '上下文类型(history/session/profile)',
    context_scope VARCHAR(20) NOT NULL COMMENT '上下文范围(short_term/long_term)',
    context_data TEXT NOT NULL COMMENT '上下文的具体数据',
    context_time DATETIME NOT NULL COMMENT '上下文记录的时间',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (context_id, stat_date),
    KEY idx_interaction_id (interaction_id),
    KEY idx_session_id (session_id),
    KEY idx_context_time (context_time),
    KEY idx_context_type (context_type)
) COMMENT '上下文信息表:记录与交互相关的上下文信息';


CREATE TABLE ods_chat_session_di (
    session_id VARCHAR(64) NOT NULL COMMENT '会话唯一ID',
    stat_date DATE NOT NULL COMMENT '统计日期(分区字段)',
    user_id BIGINT NOT NULL COMMENT '用户ID',
	user_name                   VARCHAR(255)          COMMENT '用户姓名',
    start_time DATETIME NOT NULL COMMENT '会话开始时间',
    end_time DATETIME DEFAULT NULL COMMENT '会话结束时间',
    session_type VARCHAR(32) NOT NULL COMMENT '会话类型(llm/rag/hybrid)',
    primary_model VARCHAR(64) COMMENT '主模型名称',
    fallback_model VARCHAR(64) COMMENT '回退模型名称',
    default_rag_config JSON COMMENT '默认RAG配置',
    device_info VARCHAR(512) COMMENT '设备信息',
    ip_address VARCHAR(64) COMMENT 'IP地址',
    channel VARCHAR(32) COMMENT '交互渠道(web/app/api)',
    overall_satisfaction TINYINT DEFAULT NULL COMMENT '整体满意度评分',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (session_id, stat_date),
    KEY idx_user_id (user_id),
    KEY idx_start_time (start_time),
    KEY idx_session_type (session_type),
    KEY idx_end_time (end_time)
) COMMENT '聊天会话基本信息表';


CREATE TABLE ods_model_invocation_di (
    invocation_id BIGINT AUTO_INCREMENT COMMENT '调用记录ID',
    stat_date DATE NOT NULL COMMENT '统计日期(分区字段)',
    session_id VARCHAR(64) NOT NULL COMMENT '会话ID',
    interaction_id BIGINT NOT NULL COMMENT '关联交互ID',
    model_name VARCHAR(64) NOT NULL COMMENT '模型名称',
    model_version VARCHAR(32) NOT NULL COMMENT '模型版本',
    model_type VARCHAR(32) NOT NULL COMMENT '模型类型(llm/rag/embedding)',
    invocation_mode VARCHAR(20) COMMENT '调用模式(standalone/part_of_rag)',
    prompt_template TEXT COMMENT '使用的prompt模板',
    input_tokens INT COMMENT '输入token数',
    output_tokens INT COMMENT '输出token数',
    invocation_time DATETIME NOT NULL COMMENT '调用时间',
    latency_ms INT COMMENT '延迟(毫秒)',
    http_status INT COMMENT 'HTTP状态码',
    error_message TEXT COMMENT '错误信息',
    parameters JSON COMMENT '调用参数',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (invocation_id, stat_date),
    KEY idx_session_id (session_id),
    KEY idx_interaction_id (interaction_id),
    KEY idx_model_name (model_name),
    KEY idx_model_type (model_type),
    KEY idx_invocation_time (invocation_time)
) COMMENT '模型调用记录表';

CREATE TABLE ods_user_feedback_di (
    feedback_id BIGINT AUTO_INCREMENT COMMENT '反馈记录ID',
    stat_date DATE NOT NULL COMMENT '统计日期(分区字段)',
    session_id VARCHAR(64) NOT NULL COMMENT '会话ID',
    interaction_id BIGINT NOT NULL COMMENT '关联交互ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
	user_name                   VARCHAR(255)          COMMENT '用户姓名',
    feedback_type VARCHAR(32) NOT NULL COMMENT '反馈类型(rating/comment/report/correction)',
    rating_score TINYINT CHECK (rating_score BETWEEN 1 AND 5) COMMENT '评分(1-5)',
    comment_content TEXT COMMENT '评论内容',
    correction_content TEXT COMMENT '用户纠正内容',
    feedback_time DATETIME NOT NULL COMMENT '反馈时间',
    feedback_channel VARCHAR(32) COMMENT '反馈渠道',
    is_anonymous TINYINT(1) DEFAULT 0 COMMENT '是否匿名(0:否,1:是)',
    is_processed TINYINT(1) DEFAULT 0 COMMENT '是否已处理',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (feedback_id, stat_date),
    KEY idx_session_id (session_id),
    KEY idx_interaction_id (interaction_id),
    KEY idx_user_id (user_id),
    KEY idx_feedback_time (feedback_time),
    KEY idx_feedback_type (feedback_type)
) COMMENT '用户反馈记录表';


CREATE TABLE ods_knowledge_doc_di (
    doc_id VARCHAR(64) NOT NULL COMMENT '文档唯一ID',
    stat_date DATE NOT NULL COMMENT '统计日期(分区字段)',
    doc_title VARCHAR(512) NOT NULL COMMENT '文档标题',
    doc_content TEXT NOT NULL COMMENT '文档内容',
    content_chunks JSON COMMENT '分块后的内容',
    vector_representation JSON COMMENT '向量表示',
    metadata JSON COMMENT '文档元数据',
    source_system VARCHAR(64) NOT NULL COMMENT '来源系统',
    doc_type VARCHAR(32) NOT NULL COMMENT '文档类型(faq/product/manual/news等)',
    doc_format VARCHAR(32) NOT NULL COMMENT '文档格式(text/pdf/html等)',
    create_time DATETIME NOT NULL COMMENT '创建时间',
    update_time DATETIME NOT NULL COMMENT '更新时间',
    last_accessed DATETIME COMMENT '最后访问时间',
    access_count INT DEFAULT 0 COMMENT '访问次数',
    is_deleted TINYINT(1) DEFAULT 0 COMMENT '是否已删除(0:否,1:是)',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (doc_id, stat_date),
    KEY idx_doc_title (doc_title(255)),
    KEY idx_source_system (source_system),
    KEY idx_doc_type (doc_type),
    KEY idx_create_time (create_time),
    KEY idx_update_time (update_time)
) COMMENT '外部知识库文档表';
