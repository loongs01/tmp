# Maven依赖问题解决方案

## 问题描述
在添加Hive JDBC依赖时遇到以下错误：
```
org.apache.hive:hive-llap-client:jar:3.1.0 failed to transfer from https://repo1.maven.org/maven2/
```

## 解决方案

### 方案1：使用Hive 2.3.9版本（推荐）
我已经将Hive JDBC版本从3.1.0降级到2.3.9，并排除了有问题的传递依赖。这个版本更稳定，在Maven中央仓库中可用。

### 方案2：清理Maven缓存并强制更新
如果仍然遇到问题，请执行以下命令：

```bash
# 清理Maven缓存
mvn dependency:purge-local-repository

# 强制更新依赖
mvn clean compile -U

# 或者删除本地仓库中的缓存
rm -rf ~/.m2/repository/org/apache/hive/
```

### 方案3：使用离线模式（如果网络问题持续）
如果网络连接不稳定，可以：

1. 下载Hive JDBC JAR文件到本地
2. 安装到本地Maven仓库：
```bash
mvn install:install-file \
  -Dfile=hive-jdbc-2.3.9.jar \
  -DgroupId=org.apache.hive \
  -DartifactId=hive-jdbc \
  -Dversion=2.3.9 \
  -Dpackaging=jar
```

### 方案4：使用Hive Thrift客户端（替代方案）
如果JDBC方式仍有问题，可以使用Thrift客户端：

```xml
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-service</artifactId>
    <version>2.3.9</version>
    <exclusions>
        <exclusion>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-llap-client</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

## 当前配置说明

我已经更新了pom.xml，包含以下改进：

1. **添加了多个Maven仓库**：
   - Apache Releases
   - Apache Snapshots  
   - Cloudera Releases

2. **使用Hive 2.3.9版本**：
   - 更稳定，依赖更少
   - 在Maven中央仓库中可用

3. **排除了有问题的传递依赖**：
   - hive-llap-client
   - hive-llap-common
   - hive-llap-tez
   - hive-exec

4. **排除了Hadoop依赖冲突**：
   - 使用项目已有的Hadoop 3.0.0版本

## 测试步骤

1. 清理项目：
```bash
mvn clean
```

2. 编译项目：
```bash
mvn compile -U
```

3. 如果仍有问题，尝试：
```bash
mvn dependency:purge-local-repository
mvn clean compile -U
```

## 注意事项

- Hive 2.3.9与Hive 3.1.0在JDBC接口上基本兼容
- 如果您的Hive服务器是3.1.0版本，2.3.9的客户端仍然可以正常连接
- 主要功能（查询、插入、表操作）都支持

## 如果问题持续存在

如果上述方案都不能解决问题，请考虑：

1. 检查网络连接和防火墙设置
2. 使用VPN或代理服务器
3. 联系网络管理员检查Maven仓库访问权限
4. 使用公司内部的Maven仓库镜像
