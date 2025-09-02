-- 单机模式（Standalone）
-- 修改内存配置（可选，根据虚拟机资源调整）：
-- 编辑 /opt/flink/conf/flink-conf.yaml：

jobmanager.memory.process.size: 1024m  # JobManager 内存
taskmanager.memory.process.size: 2048m # TaskManager 内存
taskmanager.numberOfTaskSlots: 2        # 每个 TaskManager 的插槽数


-- 单机模式启动

-- # 启动 JobManager 和 TaskManager
/opt/flink/bin/start-cluster.sh
 
-- # 检查进程
jps     ：应看到 JobManager 和 TaskManager 进程


-- 停止 Flink

/opt/flink/bin/stop-cluster.sh

-- Flink Web UI 访问设置 ：默认端口是 8081
-- 确认 Flink 绑定地址：
-- 检查 Flink 配置文件 (flink-conf.yaml) 中的以下参数：

rest.bind-address: 0.0.0.0  # 允许外部访问
rest.address: 0.0.0.0
rest.port: 8081
如果绑定到 127.0.0.1，则只能本地访问。


1. 确认 Flink 的绑定地址和端口
Flink Web UI 默认端口是 8081，但需要确认其绑定地址：

-- 1
如果 Flink 绑定到 0.0.0.0（允许所有网络接口访问）：
可以通过虚拟机的 物理网卡 IP（ens33 的 192.168.231.131）访问。
-- 2
如果 Flink 绑定到 127.0.0.1（仅本地访问）：
只能通过 http://127.0.0.1:8081 在虚拟机内部访问。






web ui ：  http://192.168.231.131:8081

-- #1 启动 JobManager 和 TaskManager
/opt/flink/bin/start-cluster.sh

-- # 检查进程
jps   # 应看到 JobManager 和 TaskManager 进程

netstat -tulnp | grep 8081

ps aux | grep flink-jobmanager
ps aux | grep flink-taskmanager

-- stop
/opt/flink/bin/stop-cluster.sh



-- netcat
nc -lk 9999






-- 任务提交 如下都可以
/opt/flink/bin/flink run -m 192.168.231.131:8081 -c org.example.App /home/lcz/lcz/flink-1.0-SNAPSHOT.jar 192.168.18.154 9999

/opt/flink/bin/flink run -m 192.168.231.131:8081 -c org.example.App /home/lcz/lcz/flink-1.0-SNAPSHOT.jar



-- 查看日志
grep -i "async-profiler" /opt/flink/log/flink-*.log

grep "rest.flamegraph.enabled" /opt/flink/log/flink-*.log

curl http://192.168.231.131:8081/taskmanagers/config


-- Flink配置文件（通常是flink-conf.yaml）启用火焰图功能
rest.flamegraph.enabled: true

-- 历史记录但火焰图实际有效的配置是
-- 在/opt/flink/conf/flink-conf.yaml中增加：   rest.flamegraph.enabled: true
# 启用火焰图功能
metrics.profiler.enable: true
# 可选：设置采样间隔（默认10ms）
# metrics.profiler.interval: 10 ms
# metrics.profiler.include: "org.apache.flink.runtime.profiler.JFRProfiler"
metrics.profiler.include: "org.apache.flink.runtime.profiler.AsyncProfiler"
metrics.profiler.async-profiler.path: "/opt/async-profiler/lib/libasyncProfiler.so"
metrics.profiler.async-profiler.event: "cpu"  # 或 "lock"
metrics.profiler.async-profiler.duration: 30s
metrics.profiler.async-profiler.output: "/tmp/async-profiler-results"
rest.flamegraph.enabled: true


-- mvn
mvn dependency:purge-local-repository

mvn clean compile -U
mvn clean compile  

-- 检查依赖是否下载成功 flink-connector-kafka
mvn dependency:tree | findstr flink-connector-kafka




-- 彻底清理并重新编译   \强制清理并重新下载依赖
mvn dependency:purge-local-repository


mvn clean compile

-- 如果你在 IDE（如 IntelliJ IDEA 或 Eclipse）里还看到“包不存在”：
-- 强制刷新 Maven 项目
IntelliJ IDEA：右键项目 → Maven → Reload Project


du -sh /* 2>/home/lcz/lcz/null | sort -h




mvn clean package