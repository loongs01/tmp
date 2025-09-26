# WordSplitMR Hive集成说明

## 概述

我已经成功修改了`WordSplitMR.java`中的`WordSplitReducer`类，实现了将分词结果同时写入HDFS文件和Hive表的功能。

## 主要功能

### 1. 双重输出
- **HDFS文件输出**: 保持原有的文件输出功能
- **Hive表输出**: 新增的数据库存储功能

### 2. 自动表管理
- 自动创建`biology_db`数据库
- 自动创建`word_split_result`表（可配置表名）
- 支持分区表（按年月分区）

### 3. 批量处理
- 批量插入数据到Hive表（默认1000条/批）
- 提高写入性能，减少数据库连接开销

## 表结构

```sql
CREATE TABLE biology_db.word_split_result (
    text_md5 STRING COMMENT '文本MD5',
    sent_no STRING COMMENT '句子编号', 
    sent_text STRING COMMENT '句子文本',
    sent_md5 STRING COMMENT '句子MD5',
    sent_split_param STRING COMMENT '分词参数',
    word_no INT COMMENT '词语编号',
    word_text STRING COMMENT '词语文本',
    part_of_speech STRING COMMENT '词性',
    load_time STRING COMMENT '加载时间',
    dt STRING COMMENT '日期分区'
) 
PARTITIONED BY (year_month STRING)
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/word_split_result'
```

## 使用方法

### 1. 基本用法
```bash
# 使用默认表名
hadoop jar BiologyLp-1.0-SNAPSHOT.jar org.example.mapreduce.WordSplitMR \
  /input/data /output/result

# 指定自定义表名
hadoop jar BiologyLp-1.0-SNAPSHOT.jar org.example.mapreduce.WordSplitMR \
  /input/data /output/result my_custom_table
```

### 2. 输入数据格式
```
text_md5\tsent_no\tsent_text\tsent_md5\tsent_split_param
```

### 3. 输出格式
- **HDFS文件**: 保持原有格式
- **Hive表**: 包含所有字段和分区信息

## 核心实现

### 1. Reducer类结构
```java
public static class WordSplitReducer extends Reducer<Text, Text, NullWritable, Text> {
    private HiveUtil hiveUtil;
    private String tableName;
    private int batchSize = 1000;
    private List<String> batchData = new ArrayList<>();
    private int totalRecords = 0;
    
    // setup() - 初始化Hive连接和创建表
    // reduce() - 处理数据并批量写入
    // cleanup() - 处理剩余数据并关闭连接
}
```

### 2. 关键方法

#### setup() - 初始化
- 创建HiveUtil连接
- 创建数据库和表
- 配置表名

#### reduce() - 数据处理
- 写入HDFS文件（保持原有功能）
- 准备Hive数据
- 批量插入到Hive表

#### cleanup() - 清理
- 处理剩余的批量数据
- 关闭Hive连接
- 输出统计信息

### 3. 批量插入机制
```java
private void batchInsertToHive() {
    StringBuilder insertSQL = new StringBuilder();
    insertSQL.append("INSERT INTO TABLE biology_db.").append(tableName);
    insertSQL.append(" PARTITION(year_month) VALUES ");
    insertSQL.append(String.join(", ", batchData));
    
    hiveUtil.executeUpdate(insertSQL.toString());
}
```

## 配置参数

### 1. 命令行参数
- `args[0]`: 输入路径
- `args[1]`: 输出路径  
- `args[2]`: Hive表名（可选，默认为word_split_result）

### 2. 系统配置
```java
// 从AppConfig获取
conf.set("hive.jdbc.url", AppConfig.HIVE_JDBC_URL);
conf.set("hive.user", AppConfig.HIVE_USER);
conf.set("hive.password", AppConfig.HIVE_PASSWORD);
conf.set("fs.defaultFS", AppConfig.HADOOP_HDFS_URI);
```

### 3. 可调参数
- `batchSize`: 批量插入大小（默认1000）
- `tableName`: Hive表名（可通过配置设置）
- `databaseName`: 数据库名（固定为biology_db）

## 异常处理

### 1. 容错机制
- Hive连接失败时，仍可写入HDFS文件
- 批量插入失败时，清空批量数据避免重复
- 详细的错误日志记录

### 2. 资源管理
- 使用try-with-resources确保连接关闭
- 在cleanup()中处理剩余数据
- 自动关闭Hive连接

## 性能优化

### 1. 批量处理
- 默认1000条记录批量插入
- 减少数据库连接开销
- 提高写入性能

### 2. 分区表
- 按年月分区存储
- 提高查询性能
- 便于数据管理

### 3. 并行处理
- 支持多个Reducer并行写入
- 每个Reducer独立管理Hive连接

## 监控和日志

### 1. 进度监控
```java
System.out.println("Batch inserted " + batchData.size() + " records to Hive table");
System.out.println("WordSplitReducer cleanup completed. Total records processed: " + totalRecords);
```

### 2. 错误日志
```java
System.err.println("Failed to initialize HiveUtil: " + e.getMessage());
System.err.println("Failed to batch insert to Hive: " + e.getMessage());
```

## 使用示例

### 1. 运行MapReduce作业
```bash
# 提交作业
hadoop jar BiologyLp-1.0-SNAPSHOT.jar org.example.mapreduce.WordSplitMR \
  /user/input/sentences /user/output/word_split word_split_result

# 查看HDFS输出
hadoop fs -ls /user/output/word_split
hadoop fs -cat /user/output/word_split/part-r-00000 | head -10
```

### 2. 查询Hive表
```sql
-- 连接到Hive
hive

-- 查看表结构
DESCRIBE biology_db.word_split_result;

-- 查询数据
SELECT * FROM biology_db.word_split_result LIMIT 10;

-- 按分区查询
SELECT * FROM biology_db.word_split_result 
WHERE year_month = '202412' LIMIT 10;

-- 统计信息
SELECT COUNT(*) FROM biology_db.word_split_result;
SELECT year_month, COUNT(*) FROM biology_db.word_split_result 
GROUP BY year_month;
```

## 注意事项

1. **Hive服务**: 确保HiveServer2和Metastore服务正常运行
2. **权限**: 确保Hadoop用户有Hive数据库的读写权限
3. **网络**: 确保MapReduce节点能访问Hive服务
4. **资源**: 批量大小可根据内存情况调整
5. **分区**: 分区字段基于dt字段的年月部分自动生成

## 故障排除

### 1. Hive连接失败
- 检查HiveServer2服务状态
- 验证JDBC URL配置
- 检查网络连接

### 2. 表创建失败
- 检查数据库权限
- 验证表名是否冲突
- 查看Hive日志

### 3. 数据插入失败
- 检查数据格式
- 验证字段类型匹配
- 查看SQL语法错误

这个集成方案既保持了原有的HDFS文件输出功能，又增加了Hive数据库存储能力，为后续的数据分析和查询提供了便利。
