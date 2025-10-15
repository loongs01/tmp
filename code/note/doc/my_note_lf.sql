webrtc 网页实时通信  （Web Real-Time Communication）

websocket WebSocket是基于TCP的协议

http

rtmp 实时消息协议（英语：Real-Time Messaging Protocol，缩写RTMP）也称实时消息传输协议。

obs:
Open Broadcaster Software

Flask框架
fastapi

FFmpeg 通常涉及到调用 FFmpeg 命令行工具来处理视频或音频文件

srs  :Simple Realtime Server


  0: ffmpeg -version
  1: ffmpeg -protocols | findstr rtmp
  2: ffmpeg -version
  3: netstat -ano
  4: ipconfig
  
  
 ▸5: netstat -ano
  6: netstat -ano | findstr ":8765"
  7: tasklist /FI "PID eq 24252"

>echo Proto协议 Local Address本地地址 Foreign Address外部地址 State状态 PID && netstat -ano | findstr ":8765"


ws://192.168.18.154:8756/test?connection_id=123

http://192.168.18.154:8756/voice-to-text





-- 运行以下命令查看 spaCy 支持的语言模型：
python -m spacy validate


>python -m pip uninstall jieba

python  -m pip install jieba


-- 直接查看 pip 关联的 Python 路径
pip -V
# 或
pip --version
python -m pip -V

-- 使用pip show命令
-- 通过运行以下命令，你可以直接查看包的安装路径：
python -m pip show mysql

-- 清理 pip 缓存
python -m pip cache purge

-- 升级模块/包
python -m pip install --upgrade pip

-- 使用阿里云源
python -m pip install tenacity -i https://mirrors.aliyun.com/pypi/simple/

-- -i 或 --index-url：指定自定义的 PyPI 镜像源地址。
-- PyPI（python package index）


python -m pip install pyspark==3.2.4


-- elasticsearch 命令


-- 停止es
docker stop elasticsearch

-- 启动elasticsearch

docker run -d ^
  --name elasticsearch ^
  -p 9200:9200 ^
  -p 9300:9300 ^
  -e "discovery.type=single-node" ^
  -e "ES_JAVA_OPTS=-Xms512m -Xmx512m" ^
  elasticsearch:9.0.2
  
-- 自定义账号密码（可选）
-- 如果不想使用默认密码，可以通过以下步骤重置：

-- 步骤 1：进入容器
docker exec -it elasticsearch bash
-- 步骤 2：使用 elasticsearch-reset-password 工具
-- 重置 elastic 用户的密码：

bin/elasticsearch-reset-password -u elastic -i
-- 按提示输入新密码。

elastic
lcz0205


-- 删除已存在的同名容器
docker rm -f elasticsearch


docker run -d ^
  --name elasticsearch ^
  -p 9200:9200 ^
  -p 9300:9300 ^
  -e "discovery.type=single-node" ^
  -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" ^
  -e "xpack.security.enabled=true" ^
  -e "xpack.security.authc.api_key.enabled=true" ^
  elasticsearch:9.0.2


-- 仅修改 kibana_system 用户的密码
-- 1. 直接修改密码（无需创建用户）
curl -XPOST -u elastic:lcz0205 "http://localhost:9200/_security/user/kibana_system" \
  -H "Content-Type: application/json" \
  -d'
  {
    "password": "lcz0205",
    "roles": ["kibana_system"],
    "full_name": "Kibana Service Account"
  }'
  
  
  
  
-- 1
 
-- docker run -d \
  name elasticsearch \
  -- -p 9200:9200 \
  -- -p 9300:9300 \
  -- -e "discovery.type=single-node" \
  -- -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
  -- -e "xpack.security.enabled=false" \  # 禁用所有安全功能
  -- elasticsearch:9.0.2

 
 
-- 检查端口映射是否生效 
docker port elasticsearch
 
 
-- 检查容器是否正在运行
docker ps
docker ps -a


-- linux
-- lscpu 是专门用于查看 CPU 信息的工具
lscpu



6. 性能对比建议
建议为不同场景设置不同的默认引擎：
小数据量查询：MapReduce或Tez
大数据量分析：Spark
复杂SQL：Spark
简单聚合：MapReduce



-- Windows：打开命令提示符，输入以下命令查看 Ollama 是否在运行：
tasklist | findstr ollama

在 Windows 上通过 Ollama 安装 DeepSeek 模型时，
默认路径是 C:\Users\<用户名>\.ollama\models，
但你可以通过以下方法自定义安装路径：
1、设置环境变量：setx OLLAMA_MODELS D:\llm
              setx 会永久修改环境变量（重启终端或系统后生效）。
  如果只需临时生效（当前会话有效），使用：
                set OLLAMA_MODELS="D:\path\to\your\models"
2、验证环境变量：
          echo %OLLAMA_MODELS%   -- 输出应显示你设置的路径
		  
		  
使用 setx 设置系统变量（需管理员权限）：
     setx OLLAMA_MODELS D:\llm /M
                              /M 表示修改系统变量（对所有用户生效）


-- 在 CMD 中，PATH 变量默认以分号分隔，但可以通过以下命令临时转换为换行显示：
             echo %PATH:;=&echo.%




-- linux 查看系统重启时间
uptime  最准确
who -b
last reboot | head -n 1
journalctl --list-boots

-- linux
查看当前时间	date
查看硬件时钟	sudo hwclock --show
查看时区	timedatectl
修改时区 timedatectl set-timezone Asia/Shanghai



-- 查看HDFS HA（High Availability）集群中NameNode的ID，可以通过以下几种方法
方法1：通过hdfs haadmin命令列出所有NameNode
运行以下命令查看HA配置中的NameNode ID列表：
hdfs haadmin -getAllServiceState
输出示例：

nn1: active
nn2: standby
这里的 nn1 和 nn2 就是NameNode的ID。


-- 检查DataNode状态：
hdfs dfsadmin -report
