	
	Received: b'0-key'
	
	org.example.FlinkKafkaConsumer
	
	deepseek-r1:8b
	hdfs://nameservice1/user/hive/warehouse/ads.db/dim_user_info_di
	sudo -u hdfs -i hdfs dfs -put /tmp/lcz/output.txt /user/hive/warehouse/ods.db/ods_disease_info_di
	
	<dependency>
    <groupId>com.hankcs</groupId>
    <artifactId>hanlp</artifactId>
    <version>portable-1.8.6</version>
    </dependency>


CREATE TABLE `ods.ods_app_user_info_di` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` varchar(32) COLLATE utf8mb4_bin DEFAULT NULL comment '文本MD5',
  `sent_no` int DEFAULT NULL comment '句子序列号：1,2等',
  `sent_text` text COLLATE utf8mb4_bin comment '句子内容',
  `sent_md5` varchar(32) COLLATE utf8mb4_bin DEFAULT NULL comment '句子MD5内容',
  `sent_split_param` varchar(32) COLLATE utf8mb4_bin DEFAULT NULL comment '句子标注：问句等',
  `word_no` int DEFAULT NULL comment '词语序列号',
  `word_text` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL comment '词语内容',
  `part_of_speech` varchar(32) COLLATE utf8mb4_bin DEFAULT NULL comment '词性标注（str 类型，如 "n" 表示名词，"v" 表示动词）',
  `load_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `t_people_health_text_word_info_id_IDX` (`id`) USING BTREE,
  KEY `t_people_health_text_word_info_word_text_IDX` (`word_text`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=383547 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

set hive.cli.print.current.db=true;
set hive.execution.engine=spark;
set spark.home;
set hive.execution.engine;
msck repair table ods.ods_app_user_sentence_info_di;
-- 确认回收站状态：默认 0 分钟，即禁用回收站需手动设置
hdfs getconf -confKey fs.trash.interval


CREATE TABLE ods.ods_app_user_sentence_info_di (
  user_id STRING COMMENT '用户id',
  sent_no INT COMMENT '句子序列号：1,2等',
  sent_text STRING COMMENT '句子内容',
  sent_md5 STRING COMMENT '句子MD5内容',
  sent_split_param STRING COMMENT '句子标注：?问句等',
  load_time TIMESTAMP COMMENT '数据加载时间'
)
COMMENT 'ODS层应用用户句子信息表'
PARTITIONED BY (dt STRING COMMENT '分区日期（yyyyMMdd）')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE ods.ods_app_user_info_di (
  user_id STRING COMMENT '用户id',
  sent_no INT COMMENT '句子序列号：1,2等',
  sent_text STRING COMMENT '句子内容',
  sent_md5 STRING COMMENT '句子MD5内容',
  sent_split_param STRING COMMENT '句子标注：?问句等',
  word_no INT COMMENT '词语序列号',
  word_text STRING COMMENT '词语内容',
  part_of_speech STRING COMMENT '词性标注（str 类型，如 "n" 表示名词，"v" 表示动词）',
  load_time TIMESTAMP COMMENT '数据加载时间'
)
COMMENT 'ODS层应用用户信息表'
PARTITIONED BY (dt STRING COMMENT '分区日期（yyyyMMdd）')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;




CREATE TABLE disease_info (
    id STRING COMMENT '唯一标识ID',
    disease STRING COMMENT '疾病名称',
    disease_md5 STRING COMMENT '疾病名称的MD5哈希值',
    description STRING COMMENT '疾病描述',
    description_md5 STRING COMMENT '疾病描述的MD5哈希值',
    summary STRING COMMENT '疾病摘要',
    summary_md5 STRING COMMENT '疾病摘要的MD5哈希值',
    symptoms STRING COMMENT '疾病症状',
    symptoms_md5 STRING COMMENT '疾病症状的MD5哈希值',
    cause_of_disease STRING COMMENT '病因',
    cause_of_disease_md5 STRING COMMENT '病因的MD5哈希值',
    complication STRING COMMENT '并发症',
    complication_md5 STRING COMMENT '并发症的MD5哈希值',
    prevention STRING COMMENT '预防措施',
    prevention_md5 STRING COMMENT '预防措施的MD5哈希值',
    identify STRING COMMENT '诊断方法',
    identify_md5 STRING COMMENT '诊断方法的MD5哈希值',
    treatment STRING COMMENT '治疗方案',
    treatment_md5 STRING COMMENT '治疗方案的MD5哈希值',
    visit_doctor STRING COMMENT '就诊科室',
    visit_doctor_md5 STRING COMMENT '就诊科室的MD5哈希值',
    examine STRING COMMENT '检查项目',
    examine_md5 STRING COMMENT '检查项目的MD5哈希值',
    nursing STRING COMMENT '护理建议',
    nursing_md5 STRING COMMENT '护理建议的MD5哈希值',
    diet STRING COMMENT '饮食建议',
    diet_md5 STRING COMMENT '饮食建议的MD5哈希值',
    load_time TIMESTAMP COMMENT '数据加载时间'
)
COMMENT '疾病信息表，包含疾病详情及其MD5哈希值'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


CREATE TABLE ods.ods_disease_info_di_test (
    id STRING COMMENT '唯一标识ID',
    disease STRING COMMENT '疾病名称'
)
COMMENT '疾病信息表，包含疾病详情及其MD5哈希值'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;