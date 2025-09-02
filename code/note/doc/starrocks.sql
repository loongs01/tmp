-- 编辑 /etc/profile 文件
export JAVA_HOME=/home/lcz/lcz/jdk1.8.0_441
export PATH=$JAVA_HOME/bin:$PATH
export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar

echo "export JAVA_HOME=/usr/local/jdk17" >> /etc/profile
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> /etc/profile
source /etc/profile

echo "export STARROCKS_HOME=/opt/StarRocks-3.5.1-centos-amd64" >> ~/.bashrc
echo "export PATH=\$PATH:\$STARROCKS_HOME/bin" >> ~/.bashrc
source ~/.bashrc


--begin

2. 安装步骤
2.1 关闭 SELinux 和防火墙（测试环境）
bash
# 临时关闭 SELinux
setenforce 0
# 永久关闭（可选）
sed -i 's/SELINUX=enforcing/SELINUX=disabled/g' /etc/selinux/config
 
# 关闭防火墙（生产环境建议放行必要端口）
systemctl stop firewalld
systemctl disable firewalld
2.2 下载 StarRocks 安装包
从 StarRocks 官方下载页 获取最新版本（如 StarRocks-3.2.0-release）：


wget https://releases.starrocks.com/StarRocks-3.2.0-release/StarRocks-3.2.0-x86_64-bin.tar.gz
2.3 解压并安装

tar -xzvf StarRocks-3.2.0-x86_64-bin.tar.gz -C /opt
cd /opt/StarRocks-3.2.0
2.4 配置环境变量（可选）

echo "export STARROCKS_HOME=/opt/StarRocks-3.2.0" >> ~/.bashrc
echo "export PATH=\$PATH:\$STARROCKS_HOME/bin" >> ~/.bashrc
source ~/.bashrc
3. 部署模式选择
StarRocks 支持 单节点部署（测试）和 集群部署（生产）。以下以 单节点 为例：

-- 3.1 单节点部署（快速测试）
3.1.1 修改配置文件
编辑 fe/conf/fe.conf（Frontend 配置）：


vim fe/conf/fe.conf
修改以下参数（根据实际资源调整）：

ini
# 元数据目录（需持久化）
-- meta_dir = /data/starrocks/fe/meta

meta_dir =/home/lcz/lcz/data/starrocks/fe/meta
 
# JVM 内存（建议 FE 占 4-8GB）
JAVA_OPTS="-Xms4g -Xmx4g -XX:+UseMembar -XX:SurvivorRatio=8"
 
# 查询端口（默认 9030）
http_port = 9030
rpc_port = 9020
query_port = 9050
编辑 be/conf/be.conf（Backend 配置）：


vim be/conf/be.conf
修改以下参数：

ini
# 数据目录（需持久化）
storage_root_path = /data/starrocks/be/storage
 
# JVM 内存（建议 BE 占 8-16GB）
JAVA_OPTS="-Xms8g -Xmx8g -XX:+UseMembar -XX:SurvivorRatio=8"
 
# Web 端口（默认 8040）
webserver_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
3.1.2 创建数据目录
bash
mkdir -p /data/starrocks/fe/meta
mkdir -p /data/starrocks/be/storage
chown -R $(whoami):$(whoami) /data/starrocks
3.1.3 启动 FE 和 BE
bash
# 启动 FE（Frontend）
./fe/bin/start_fe.sh --daemon
 
# 启动 BE（Backend）
./be/bin/start_be.sh --daemon
3.1.4 检查服务状态
bash
# FE 日志
tail -f fe/log/fe.log
 
# BE 日志
tail -f be/log/be.INFO
 
# 检查进程
ps -ef | grep starrocks



df -h /home/lcz/lcz/data/starrocks/fe/meta


-- 1
# 启动 FE（Frontend）
./fe/bin/start_fe.sh --daemon
 
# 启动 BE（Backend）
./be/bin/start_be.sh --daemon

-- 2
mysql -h 127.0.0.1 -P 9030 -u root -p


-- 修改 root 密码（适用于 StarRocks 2.0+）
alter user 'root'@'%' identified by '0205';



-- 创建用户 'analyst'，允许从任意主机连接
create user 'lcz'@'%' identified by '0205';
 
-- 创建用户 'app_user'，仅允许从 192.168.1.100 连接
CREATE USER 'app_user'@'192.168.1.100' IDENTIFIED BY 'AppUser@456';

-- 查看用户 'analyst' 的权限
show grants for 'lcz'@'%';

-- 授予用户 'writer' 对数据库 'log_db' 的读写权限
grant select, insert, update on lcz.* to 'lcz'@'%';


-- 2. 创建新用户
-- 基本语法
CREATE USER 'username'@'host' IDENTIFIED BY 'password';
username：用户名（如 analyst、app_user）。
host：允许连接的客户端 IP 或域名（% 表示任意主机）。
password：用户密码（建议使用强密码）。


-- 授权语法
GRANT 权限列表 ON 数据库.表 TO 'username'@'host';
权限列表：用逗号分隔（如 SELECT, INSERT）。
数据库.表：
*.*：所有数据库和表。
db_name.*：指定数据库的所有表。
db_name.table_name：指定表。



-- StarRocks:
StarRocks FE节点地址 :192.168.122.1 9030


SHOW PROC '/frontends'; 
-- 是 StarRocks 中的一个系统命令，
-- 用于查看 Frontend（FE）节点 的运行状态和配置信息。
-- 它是 StarRocks 集群管理的重要工具，可以帮助管理员监控 FE 节点的健康状况、角色分布以及集群拓扑。
