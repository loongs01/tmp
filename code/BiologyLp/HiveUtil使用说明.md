# HiveUtil 使用说明

## 概述

HiveUtil 是一个基于您提供的 Hive 配置文件开发的 Hive 操作工具类，提供了完整的 Hive 数据库操作功能。

## 配置信息

基于您的配置文件，工具类使用以下配置：

### Hive 环境配置 (hive-env.sh)
- **HIVE_HOME**: 自动检测
- **TEZ_JARS**: `/opt/datasophon/tez`
- **HADOOP_HOME**: `/opt/datasophon/hadoop-3.3.3`
- **HADOOP_HEAPSIZE**: 8192MB
- **HIVE_SERVER2_HEAPSIZE**: 8192MB

### Hive 服务配置 (hive-site.xml)
- **JDBC URL**: `jdbc:hive2://liuf1:10000/default`
- **Metastore URI**: `thrift://liuf1:9083`
- **Warehouse 目录**: `/user/hive/warehouse`
- **ZooKeeper 集群**: `liuf1:2181,liuf2:2181,liuf3:2181`
- **执行引擎**: MapReduce (mr)

## 主要功能

### 1. 连接管理
```java
try (HiveUtil hiveUtil = new HiveUtil()) {
    // 自动管理连接
    boolean connected = hiveUtil.isConnected();
    boolean testResult = hiveUtil.testConnection();
}
```

### 2. 数据库操作
```java
// 创建数据库
hiveUtil.createDatabase("test_db");

// 删除数据库
hiveUtil.dropDatabase("test_db", true); // true表示级联删除

// 使用数据库
hiveUtil.useDatabase("test_db");

// 显示所有数据库
List<Map<String, Object>> databases = hiveUtil.showDatabases();
```

### 3. 表操作
```java
// 创建内部表
String columns = "id INT, name STRING, description STRING";
hiveUtil.createTable("biology_data", columns, "TEXTFILE");

// 创建外部表
String hdfsLocation = "/user/hive/warehouse/external_data";
hiveUtil.createExternalTable("external_table", columns, hdfsLocation, "TEXTFILE");

// 删除表
hiveUtil.dropTable("biology_data");

// 显示表结构
List<Map<String, Object>> tableInfo = hiveUtil.describeTable("biology_data");

// 显示所有表
List<Map<String, Object>> tables = hiveUtil.showTables();
```

### 4. 数据操作
```java
// 插入数据
String values = "(1, '细胞生物学', '研究细胞结构和功能')";
hiveUtil.insertIntoTable("biology_data", values);

// 从HDFS加载数据
hiveUtil.loadDataFromHdfs("biology_data", "/user/hive/warehouse/data.txt", false);

// 查询数据
List<Map<String, Object>> results = hiveUtil.executeQuery("SELECT * FROM biology_data");

// 获取表行数
long rowCount = hiveUtil.getTableRowCount("biology_data");
```

### 5. SQL执行
```java
// 执行查询
List<Map<String, Object>> results = hiveUtil.executeQuery("SELECT * FROM table_name");

// 执行更新
int affectedRows = hiveUtil.executeUpdate("INSERT INTO table_name VALUES (1, 'test')");

// 自动判断SQL类型
Object result = hiveUtil.execute("SELECT COUNT(*) FROM table_name");
```

### 6. 事务管理
```java
// 关闭自动提交
hiveUtil.setAutoCommit(false);

try {
    // 执行多个操作
    hiveUtil.executeUpdate("INSERT INTO table1 VALUES (1, 'test1')");
    hiveUtil.executeUpdate("INSERT INTO table2 VALUES (2, 'test2')");
    
    // 提交事务
    hiveUtil.commit();
} catch (SQLException e) {
    // 回滚事务
    hiveUtil.rollback();
    throw e;
}
```

## 使用示例

### 基本使用
```java
public class HiveExample {
    public static void main(String[] args) {
        try (HiveUtil hiveUtil = new HiveUtil()) {
            // 测试连接
            if (!hiveUtil.testConnection()) {
                System.out.println("连接失败");
                return;
            }
            
            // 创建数据库和表
            hiveUtil.createDatabase("biology_db");
            hiveUtil.useDatabase("biology_db");
            
            String columns = "id INT, name STRING, description STRING";
            hiveUtil.createTable("biology_data", columns, "TEXTFILE");
            
            // 插入数据
            String values = "(1, '细胞生物学', '研究细胞结构和功能')";
            hiveUtil.insertIntoTable("biology_data", values);
            
            // 查询数据
            List<Map<String, Object>> results = hiveUtil.executeQuery("SELECT * FROM biology_data");
            for (Map<String, Object> row : results) {
                System.out.println("ID: " + row.get("id") + ", 名称: " + row.get("name"));
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
```

### 与MapReduce集成
```java
// 在MapReduce作业中使用HiveUtil
public class BiologyDataProcessor {
    public void processData() throws SQLException {
        try (HiveUtil hiveUtil = new HiveUtil()) {
            // 创建结果表
            String resultColumns = "word STRING, count INT, category STRING";
            hiveUtil.createTable("biology_word_count", resultColumns, "TEXTFILE");
            
            // 从MapReduce结果加载数据
            String mrOutputPath = "/user/hive/warehouse/biology_mr_output";
            hiveUtil.loadDataFromHdfs("biology_word_count", mrOutputPath, true);
            
            // 查询处理结果
            List<Map<String, Object>> topWords = hiveUtil.executeQuery(
                "SELECT word, count FROM biology_word_count ORDER BY count DESC LIMIT 10"
            );
        }
    }
}
```

## 依赖配置

在 `pom.xml` 中已添加必要的依赖：

```xml
<!-- Hive JDBC Driver -->
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-jdbc</artifactId>
    <version>3.1.0</version>
</dependency>
```

## 注意事项

1. **连接管理**: 使用 `try-with-resources` 确保连接正确关闭
2. **异常处理**: 所有方法都会抛出 `SQLException`，需要适当处理
3. **资源清理**: 工具类实现了 `AutoCloseable` 接口，支持自动资源管理
4. **日志记录**: 使用 SLF4J 记录操作日志，便于调试和监控
5. **配置匹配**: 工具类配置与您的 Hive 环境配置完全匹配

## 运行环境要求

- Java 8+
- Hive 3.1.0
- Hadoop 3.3.3
- MySQL (用于Metastore)
- ZooKeeper 集群

## 故障排除

### 常见问题

1. **连接失败**: 检查 HiveServer2 是否启动，端口 10000 是否可访问
2. **驱动加载失败**: 确保 `hive-jdbc` 依赖已正确添加
3. **权限问题**: 确保用户有访问 Hive 和 HDFS 的权限
4. **Metastore 连接失败**: 检查 MySQL 连接和 Metastore 服务状态

### 调试建议

1. 启用详细日志记录
2. 使用 `testConnection()` 方法验证连接
3. 检查 Hive 服务日志
4. 验证网络连接和防火墙设置
