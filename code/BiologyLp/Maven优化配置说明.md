# Maven优化配置说明

## 问题分析

您遇到的警告信息表明：
1. `repo.hankcs.com` 仓库无法访问
2. `http://0.0.0.0/` 被Maven的HTTP阻止器拦截

## 已完成的优化

### 1. 移除了有问题的仓库
- ❌ 移除了 `hanlp-releases` (repo.hankcs.com)
- ❌ 移除了 `cloudera-releases` (可能访问慢)
- ❌ 移除了 `apache-snapshots` (通常不需要)

### 2. 添加了阿里云镜像
- ✅ 阿里云中央仓库镜像 (速度快)
- ✅ 阿里云公共仓库 (包含更多依赖)
- ✅ 保留了Maven中央仓库作为备用
- ✅ 保留了Apache Releases仓库 (用于Hive等)
- ✅ 保留了JitPack (用于GitHub项目)

## 推荐的Maven设置

### 方案1：使用阿里云镜像 (推荐)
我已经在pom.xml中配置了阿里云镜像，这是国内访问最快的选择。

### 方案2：全局Maven配置
如果您想全局使用阿里云镜像，可以在 `~/.m2/settings.xml` 中添加：

```xml
<settings>
  <mirrors>
    <mirror>
      <id>aliyun-central</id>
      <name>Aliyun Central</name>
      <url>https://maven.aliyun.com/repository/central</url>
      <mirrorOf>central</mirrorOf>
    </mirror>
    <mirror>
      <id>aliyun-public</id>
      <name>Aliyun Public</name>
      <url>https://maven.aliyun.com/repository/public</url>
      <mirrorOf>*</mirrorOf>
    </mirror>
  </mirrors>
</settings>
```

## 解决步骤

### 1. 清理Maven缓存
```bash
# 清理本地仓库缓存
mvn dependency:purge-local-repository

# 或者手动删除缓存目录
# Windows: %USERPROFILE%\.m2\repository
# Linux/Mac: ~/.m2/repository
```

### 2. 强制更新依赖
```bash
mvn clean compile -U
```

### 3. 如果仍有问题，尝试离线模式
```bash
# 先在线下载所有依赖
mvn dependency:resolve

# 然后离线编译
mvn compile -o
```

## 网络问题排查

### 检查网络连接
```bash
# 测试阿里云镜像连接
ping maven.aliyun.com

# 测试Maven中央仓库连接
ping repo1.maven.org
```

### 检查防火墙和代理
- 确保防火墙允许Maven访问网络
- 如果使用代理，在 `~/.m2/settings.xml` 中配置代理设置

## 替代方案

### 如果阿里云镜像也有问题
可以尝试其他国内镜像：

```xml
<!-- 华为云镜像 -->
<repository>
    <id>huawei-central</id>
    <name>Huawei Central</name>
    <url>https://repo.huaweicloud.com/repository/maven/</url>
</repository>

<!-- 腾讯云镜像 -->
<repository>
    <id>tencent-central</id>
    <name>Tencent Central</name>
    <url>https://mirrors.cloud.tencent.com/nexus/repository/maven-public/</url>
</repository>
```

## 验证配置

运行以下命令验证Maven配置：

```bash
# 显示有效的仓库列表
mvn help:effective-settings

# 显示项目依赖树
mvn dependency:tree

# 显示项目信息
mvn help:describe -Dplugin=compiler
```

## 预期结果

优化后，您应该看到：
- ✅ 更快的依赖下载速度
- ✅ 减少或消除连接超时警告
- ✅ 成功的项目编译

如果问题持续存在，请检查：
1. 网络连接是否稳定
2. 防火墙设置是否正确
3. 是否需要配置代理服务器
