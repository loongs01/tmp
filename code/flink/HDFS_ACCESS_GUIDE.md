# Windows 本地 Flink 访问服务器 HDFS 配置指南

## 配置说明

### 1. 项目依赖
已在 `pom.xml` 中添加了以下依赖：
- `hadoop-client` - Hadoop 客户端
- `hadoop-hdfs` - HDFS 客户端
- `hadoop-common` - Hadoop 通用组件
- `flink-connector-files` - Flink 文件连接器

### 2. 配置文件
创建了以下配置文件在 `src/main/resources/` 目录下：

#### core-site.xml
- 配置了默认文件系统为 `hdfs://nameservice1`
- 设置了 ZooKeeper 集群地址
- 配置了安全认证为简单模式

#### hdfs-site.xml
- 复制了服务器的 HDFS 配置
- 包含高可用 NameNode 配置
- 设置了故障转移代理提供者

### 3. 网络配置要求

#### Windows hosts 文件配置
需要在 Windows 的 `C:\Windows\System32\drivers\etc\hosts` 文件中添加服务器 IP 映射：

```
# 替换为实际的服务器 IP 地址
192.168.1.100  liuf1
192.168.1.101  liuf2  
192.168.1.102  liuf3
```

#### 防火墙端口
确保以下端口在服务器防火墙中开放：
- 8020 (NameNode RPC)
- 9870 (NameNode HTTP)
- 8485 (JournalNode)
- 2181 (ZooKeeper)
- 1025, 1026 (DataNode)

## 使用方式

### 方法一：使用 HdfsUtils 工具类

```java
// 测试连接
HdfsUtils.testConnection();

// 列出目录
HdfsUtils.listFiles("/user");

// 读取文件
HdfsUtils.readFile("/user/test/sample.txt");

// 检查文件是否存在
boolean exists = HdfsUtils.exists("/user/test/sample.txt");
```

### 方法二：在 Flink 程序中使用

```java
// 设置 HDFS 路径
String hdfsPath = "hdfs://nameservice1/user/test/input.txt";

// 创建文件源
FileSource<String> source = FileSource
    .forRecordStreamFormat(new TextLineInputFormat(), new Path(hdfsPath))
    .build();

// 创建数据流
DataStream<String> stream = env.fromSource(source, 
    WatermarkStrategy.noWatermarks(), "hdfs-source");
```

## 运行测试

### 1. 编译项目
```bash
mvn clean compile
```

### 2. 运行测试类
```bash
mvn exec:java -Dexec.mainClass="org.example.HdfsTest"
```

### 3. 运行 Flink HDFS 示例
```bash
mvn exec:java -Dexec.mainClass="org.example.HdfsFlinkExample"
```

## 常见问题排查

### 1. 连接超时
- 检查网络连通性：`ping liuf1`, `ping liuf2`, `ping liuf3`
- 检查端口是否开放：`telnet liuf2 8020`

### 2. 权限问题
- 确保设置了 `HADOOP_USER_NAME` 系统属性
- 检查 HDFS 目录权限设置

### 3. 域名解析问题
- 确保 hosts 文件配置正确
- 可以尝试直接使用 IP 地址替换域名

### 4. 版本兼容性
- 确保 Hadoop 客户端版本与服务器版本兼容
- 当前配置使用 Hadoop 3.3.3 版本

## 示例文件路径
根据你的服务器配置，常用的 HDFS 路径可能包括：
- `/user/hive/warehouse/` - Hive 数据仓库目录
- `/user/test/` - 测试数据目录
- `/tmp/` - 临时目录

## 注意事项
1. Windows 环境下需要正确配置主机名映射
2. 确保网络连通性和防火墙设置
3. 建议先使用 `HdfsTest` 类测试基本连接
4. 成功连接后再在 Flink 程序中使用 HDFS
