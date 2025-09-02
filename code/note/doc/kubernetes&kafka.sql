for pkg in epel-release conntrack ipvsadm ipset jq sysstat curl libseccomp wget socat git; do
  if rpm -q $pkg &>/dev/null; then
    echo "$pkg: 已安装"
  else
    echo "$pkg: 未安装"
  fi
done

 
-- yum（Yellowdog Updater Modified）是 RHEL（Red Hat Enterprise Linux）
-- 及其衍生发行版（如 CentOS、Fedora、AlmaLinux、Rocky Linux） 
-- 的默认 包管理工具，用于自动化安装、更新、删除和管理 RPM 软件包及其依赖关系。
yum install <package>	安装软件包
yum update	更新所有软件包
yum remove <package>	删除软件包
yum search <keyword>	搜索软件包
yum info <package>	查看软件包信息
yum repolist	列出已启用的仓库
yum clean all	清理缓存

5. 查看软件包信息

yum info <package_name>     # 查看包的详细信息
yum list installed          # 列出所有已安装的包
yum list available          # 列出所有可用的包
示例：


yum info httpd             # 查看 Apache 的详细信息
yum list installed | grep nginx # 检查 Nginx 是否已安装

-- example
yum list installed | grep -i 'cri-dockerd.*'

yum list installed | grep -i 'jq.*'


-- 1. 检查 jq 是否已安装
-- 方法 1：使用 yum list installed

yum list installed | grep jq




yum install -y conntrack ipvsadm  jq  socat git



tee /etc/docker/daemon.json <<-'EOF'
{
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://mirror.baidubce.com",
    "https://registry.cn-hangzhou.aliyuncs.com"
  ],  # 可选：配置镜像加速
  "storage-driver": "overlay2",                      # 推荐存储驱动
  "log-driver": "json-file",                         # 日志驱动
  "log-opts": {
    "max-size": "100m",                              # 单个日志文件最大大小
    "max-file": "3"                                  # 保留的日志文件数量
  }
}
EOF


cat > /etc/docker/daemon.json <<'EOF'
{
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://mirror.baidubce.com",
    "https://registry.cn-hangzhou.aliyuncs.com"
  ],
  "storage-driver": "overlay2",
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "3"
  },
  "exec-opts": ["native.cgroupdriver=systemd"]
}
EOF





"exec-opts": ["native.cgroupdriver=systemd"]
-- jq使用
直接输出输入：
如果输入是一个合法的 JSON，. 会原样输出它。

echo '{"name": "Alice", "age": 25}' | jq '.'


jq . /etc/docker/daemon.json

-- . 检查 Docker 是否正常运行
-- 确保 Docker 服务已启动：


systemctl status docker
如果 Docker 未运行，启动它：


systemctl start docker


-- # 查看 Swap 使用情况
free -h
swapon --show  # 确认是否有活跃 Swap



kubectl cluster-info
-- Kubernetes control plane is running at https://192.168.231.131:6443


-- 1. 查看 <控制平面IP>
控制平面 IP 是 Master 节点的 可访问 IP（通常是内网 IP 或公网 IP，取决于集群部署环境）。

方法 1：在 Master 节点上查看

# 查看 Master 节点的 IP（通常选择第一个非本地 IP）

hostname -I | awk '{print $1}'  # Linux（大多数发行版）

# 或
ip a | grep inet | grep -v 127.0.0.1 | awk '{print $2}' | cut -d/ -f1 | head -n 1


-- 2. 查看 <token> 和 <hash>
<token> 和 <hash> 是动态生成的，用于工作节点加入集群时的身份验证。它们可以通过以下方式获取：

方法 1：在 Master 节点上重新生成 join 命令
# 在 Master 节点上运行（会输出完整的 kubeadm join 命令）
kubeadm token create --print-join-command
输出示例：
kubeadm join 192.168.1.100:6443 --token abcdef.1234567890abcdef \
    --discovery-token-ca-cert-hash sha256:a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890
--token：abcdef.1234567890abcdef（格式为 <6字符>.<16字符>）
--discovery-token-ca-cert-hash：sha256:...（64字符哈希值）

-- 5. 加入 Worker 节点
kubeadm join 192.168.231.131:6443 --token d1nmr8.m87m03owishqaqds --discovery-token-ca-cert-hash sha256:67ad46a8bbd5435305d7e0c4bc71eccd0f10bae63f9c8d48ead85808c5781e89



-- k8s install 步骤

1. 系统准备
1.1 关闭防火墙和 SELinux

# 关闭防火墙
systemctl stop firewalld
systemctl disable firewalld
 
# 临时禁用 SELinux
setenforce 0
# 永久禁用（修改配置文件）
sed -i 's/^SELINUX=enforcing$/SELINUX=disabled/' /etc/selinux/config
1.2 关闭 Swap

swapoff -a
# 永久禁用（注释掉 /etc/fstab 中的 swap 行）
sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab
1.3 配置内核参数

cat <<EOF > /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-ip6tables = 1
net.bridge.bridge-nf-call-iptables = 1
net.ipv4.ip_forward = 1
vm.swappiness = 0
EOF
 
# 加载配置
sysctl --system
1.4 安装依赖工具

yum install -y epel-release
yum install -y conntrack ipvsadm ipset jq sysstat curl libseccomp wget socat git
2. 安装 Docker
2.1 添加 Docker 仓库

yum install -y yum-utils
yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
2.2 安装 Docker CE

yum install -y docker-ce-19.03.15 docker-ce-cli-19.03.15 containerd.io
2.3 配置 Docker 镜像加速（可选）
编辑 /etc/docker/daemon.json：

json
{
  "registry-mirrors": ["https://<your-mirror-url>"]
}
启动 Docker：


systemctl enable docker
systemctl start docker
3. 安装 Kubernetes 组件
3.1 添加 Kubernetes 仓库

cat <<EOF > /etc/yum.repos.d/kubernetes.repo
[kubernetes]
name=Kubernetes
baseurl=https://packages.cloud.google.com/yum/repos/kubernetes-el7-\$basearch
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
exclude=kube*
EOF
3.2 安装 kubeadm/kubelet/kubectl

yum install -y kubelet-1.20.15 kubeadm-1.20.15 kubectl-1.20.15 --disableexcludes=kubernetes
3.3 启动 kubelet 并设置开机自启

systemctl enable kubelet
systemctl start kubelet
4. 初始化 Kubernetes 集群
4.1 初始化 Master 节点

kubeadm init --kubernetes-version=v1.20.15 --pod-network-cidr=10.244.0.0/16 --service-cidr=10.96.0.0/12 --ignore-preflight-errors=Swap
记录输出：初始化完成后会显示 kubeadm join 命令，用于加入工作节点。

-- 替换为
kubeadm init \
  --kubernetes-version=v1.20.15 \
  --pod-network-cidr=10.244.0.0/16 \
  --service-cidr=10.96.0.0/12 \
  --ignore-preflight-errors=Swap \
  --image-repository=registry.aliyuncs.com/google_containers


# 修改 DaemonSet 的镜像版本
kubectl set image daemonset/calico-node -n kube-system \
  calico-node=docker.io/calico/node:v3.27.0 \
  upgrade-ipam=docker.io/calico/cni:v3.27.0 \
  install-cni=docker.io/calico/cni:v3.27.0 \
  flexvol-driver=docker.io/calico/pod2daemon-flexvol:v3.27.0

--example
-- 清理旧集群：

kubeadm reset -f
rm -rf /etc/kubernetes/ /var/lib/kubelet/ /var/lib/etcd/
systemctl restart docker
重新初始化：

kubeadm init \
  --kubernetes-version=v1.20.15 \
  --pod-network-cidr=10.244.0.0/16 \
  --service-cidr=10.96.0.0/12 \
  --ignore-preflight-errors=Swap \
  --image-repository=registry.aliyuncs.com/google_containers


4.2 配置 kubectl

mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config
4.3 安装网络插件（Calico）

kubectl apply -f https://docs.projectcalico.org/manifests/calico.yaml


-- example
-- 对于 Kubernetes v1.20 或更低，使用 Calico v3.20 或更低：

kubectl apply -f https://docs.projectcalico.org/v3.20/manifests/calico.yaml

5. 加入 Worker 节点
在 Worker 节点上执行 Master 节点初始化时生成的 kubeadm join 命令，例如：


kubeadm join <master-ip>:6443 --token <token> --discovery-token-ca-cert-hash sha256:<hash>
6. 验证集群
在 Master 节点上执行：


kubectl get nodes
输出应显示所有节点状态为 Ready。






-- 方法 2：直接下载 GitHub 原始文件（推荐） 安装 cri-dockerd
GitHub 的原始链接可能只是临时网络问题，可以：

使用 curl 替代 wget（有时更稳定）：

curl -L -O https://github.com/Mirantis/cri-dockerd/releases/download/v0.3.4/cri-dockerd-0.3.4-3.el7.x86_64.rpm
参数说明：
-L：跟随重定向（GitHub 可能返回重定向链接）。
-O：保存为文件名（而非输出到终端）。
如果仍失败，可能是 GitHub 被限制，尝试通过代理：

export https_proxy=http://your-proxy-ip:port
curl -L -O https://github.com/Mirantis/cri-dockerd/releases/download/v0.3.4/cri-dockerd-0.3.4-3.el7.x86_64.rpm


-- 运行以下命令查看集群版本：
kubectl version --short




/var/lib/docker/overlay2/


docker save -o ./calico-images.tar quay.io/calico/cni:v3.20.6 quay.io/calico/pod2daemon-flexvol:v3.20.6





-- kafaka 启动
-- 1 先启动 zookeeper
/opt/kafka_2.12-3.7.2/bin/zookeeper-server-start.sh -daemon /opt/kafka_2.12-3.7.2/config/zookeeper.properties

netstat -tulnp | grep 2181
-- 2 后启动kafaka
/opt/kafka_2.12-3.7.2/bin/kafka-server-start.sh -daemon /opt/kafka_2.12-3.7.2/config/server.properties

netstat -tulnp | grep 9092

-- 查看topic
/opt/kafka_2.12-3.7.2/bin/kafka-topics.sh --bootstrap-server 192.168.231.131:9092 --list
-- 删除topic
/opt/kafka_2.12-3.7.2/bin/kafka-topics.sh --bootstrap-server 192.168.231.131:9092 --delete --topic test-topic

-- 查看 Topic 数据

/opt/kafka_2.12-3.7.2/bin/kafka-console-consumer.sh \
  --bootstrap-server 192.168.231.131:9092 \
  --topic test-topic \
  --from-beginning

--作用同上，只不过转为同一行
/opt/kafka_2.12-3.7.2/bin/kafka-console-consumer.sh   --bootstrap-server 192.168.231.131:9092   --topic test-topic   --from-beginning

-- 列出所有消费者组
/opt/kafka_2.12-3.7.2/bin/kafka-consumer-groups.sh --bootstrap-server 192.168.231.131:9092 --list

 -- 删除消费者组的命令
/opt/kafka_2.12-3.7.2/bin/kafka-consumer-groups.sh \
  --bootstrap-server 192.168.231.131:9092 \
  --delete \
  --group python-consumer-group

/opt/kafka_2.12-3.7.2/bin/kafka-consumer-groups.sh --bootstrap-server 192.168.231.131:9092 --describe --group flink-word-count-group



-- 每个分区在每个broker上的日志大小和消息数量
/opt/kafka_2.12-3.7.2/bin/kafka-log-dirs.sh --bootstrap-server 192.168.231.131:9092 \
  --topic-list test-topic \
  --describe




-- stop kafaka 
-- 2 停止 ZooKeeper
/opt/kafka_2.12-3.7.2/bin/zookeeper-server-stop.sh

# 查找 ZooKeeper 进程（通常包含 "QuorumPeerMain"）
ps aux | grep QuorumPeerMain
 
# 强制终止（使用 PID）
kill -9 <PID>

-- 1 停止 Kafka Broker

/opt/kafka_2.12-3.7.2/bin/kafka-server-stop.sh


# 查找 Kafka 进程（通常包含 "kafka.Kafka"）
ps aux | grep kafka.Kafka

# 强制终止（使用 PID）
kill -9 <PID>



-- 1 先停止 Kafka Broker（避免数据写入冲突）：

/opt/kafka_2.12-3.7.2/bin/kafka-server-stop.sh
-- 2  再停止 ZooKeeper（Kafka 依赖 ZooKeeper 存储元数据）：

/opt/kafka_2.12-3.7.2/bin/zookeeper-server-stop.sh





# 列出所有消费者组
bin/kafka-consumer-groups.sh --bootstrap-server <broker_host:port> --list
 
# 查看特定消费者组的详细信息
bin/kafka-consumer-groups.sh --bootstrap-server <broker_host:port> --describe --group <group_id>
 
# 查看所有消费者组的详细信息
bin/kafka-consumer-groups.sh --bootstrap-server <broker_host:port> --describe --all-groups


输出字段说明：
    GROUP：消费者组ID
    TOPIC：消费的主题
    PARTITION：分区号
    CURRENT-OFFSET：当前消费偏移量
    LOG-END-OFFSET：分区最新消息偏移量
    LAG：未消费的消息数(LOG-END-OFFSET - CURRENT-OFFSET)
    CONSUMER-ID：消费者实例ID
    HOST：消费者主机
    CLIENT-ID：客户端ID
	
	
-- 数据保存路径查看	
Kafka 的配置文件通常为 server.properties（位于 config/ 目录下），其中定义了数据存储路径：

properties
# 主配置参数（优先使用 log.dirs）
log.dirs=/data/kafka-logs  # 推荐：支持多目录（逗号分隔）
# 旧版兼容参数（不推荐）
log.dir=/data/kafka-logs-backup
