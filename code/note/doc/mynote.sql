-- window：撤销操作的快捷键是“Ctrl+Z”，而恢复（取消撤销）的快捷键是“Ctrl+Y”。 
alt+f4 :关闭当前窗口（具有通用性）

--idea
Ctrl+H 显示类结构图
Ctrl + F12 查看类中所有方法  alt+7
Ctrl+F8 打上/取消断点

Ctrl+Shift+N找到这个类。


idea方法重写快捷键
Ctrl+O  方法重写
生成getter和setter方法的快捷键：Alt+Insert

Ctrl+E 最近更改代码  显示最近打开的文件列表
Ctrl+Shift+空格 类型匹配代码补全
Ctrl+Alt+空格   第二次代码补全

Ctrl+N：按名字搜索类，这是最常用的查找类的方法之一。输入类名可以定位到这个类文件。12

Shift+Shift：搜索任何东西，包括类、资源、配置项、方法等。这个功能非常强大，可以搜索路径，例如，如果你在Java、JS、CSS、JSP中都有名为"hello"的文件夹，你可以使用这个快捷键来快速找到它们。



ctrl+alt+l 格式化代码


-- pycharm
切换列选择模式 : Alt + Shift + Insert
---------------------

--NEW
-- 万能解错/生成返回值：Alt+回车  
:该快捷键可以进行一定的信息提示、优化建议、提供解决方案等。

Ctrl+N按名称搜索类（自定义类+依赖项类）
Ctrl+Shit+N按文件名称搜索文件（所有类型文件）
Crl+H查看当前的类的继承关系
Ctrl+AIt+B查看子类方法实现
Alt+F7查找类或方法在哪被使用
Ctrl+F/Ctrl+Shit+F按照文本的内容查找（不搜依赖项）
Shit+Shit搜索任何东西


复制当前行到下一行‌：Ctrl + D
‌生成try-catch结构‌：Ctrl + Alt + T
‌最近更改的代码‌：Ctrl + E

1.快速生成main()方法：psvm,回车
2.快速生成输出语句:sout,回车
3.ctrl+Z撤▣，ctr+shift+z取消撤▣
4.ctrl+r替换
5.Ctrl+AIt+space(内容提示，代码补全等)
6.Ctrl+句号。最小化方法，恢复最小化方法。
7.注释：
单行：选中代码，Ctrl+/,再来一次，就是取消
多行：选中代码，Ctrl+Shit+/,再来一次，就是取消
8.格式化：不用选中代码，你随便点击一个代码的位置，然后Ctrl+Alt+L
9.导包：Alt+回车
10.查看源代码：Ctrl+左键（但是是查看那个对象的表面类型里面的方法，所以你要是多态接收一个对象，然后调用方法，c+左键员
入到那个表面类型的类里面看他的方法，不是看实际执行的方法的方法体的)
11.“你好”.sout一>点击回车就是生成System.out.println(你好”)；这个语句。
12.在main里面写一个没有定义的方法，然后按下alt+enter会跳出选项，帮你快捷地创建对象
13.导包，光标放到对应的类的那个单词里面，然后Alt+enter
‌14.Ctrl + Alt + V‌：在赋值语句中按下Ctrl + Alt + V，可以自动提取方法的返回值为一个变量‌；
或者，在IDEA使用过程中，在接收返回值时，通常采用ALT+Enter快捷键或者.var，可以快速完成参数接收，但会出现接收参数前面有final修饰的情况
15.控制台里信息多时，使用shift+鼠标右键点击到哪就可以选中对应信息
      快捷键辅助选中：结合键盘的 Shift 键可以实现更灵活的选中。
      例如，先将光标移动到起始位置，然后按住 Shift 键，再用鼠标点击结束位置，
      或者使用方向键（↑、↓、←、→）来微调结束位置，这样可以更精确地选中所需的文本范围。
16.

--2025 pycharm
查找文件 ctrl + shift + n





-- python
进入cmd
pip list
pip show 模块名   例如：pip show pytz

-- 使用pip show命令
-- 通过运行以下命令，你可以直接查看包的安装路径：
python -m pip show mysql


-- 进入cmd
mvn help:system     ：这个命令主要用于显示有关 Maven 系统属性和环境变量的信息



--linux 

export end_date=2024-11-22
echo $end_date




-- 一、df 、du 常见命令
　　du 查看目录大小，df 查看磁盘使用情况。

1、du (disk usage)：显示每个文件和目录的磁盘使用空间，也就是文件的大小。

-a   // 显示目录中文件的大小  单位 KB 。
-b  // 显示目录中文件的大小，以字节byte为单位。
-c  // 显示目录中文件的大小，同时也显示总和；单位KB。
-k 、 -m  // 显示目录中文件的大小，-k 单位KB，-m 单位MB.
-s  // 仅显示目录的总值，单位KB。

-h  // 以K  M  G为单位显示，提高可读性~~~（最常用的一个~也可能只用这一个就满足需求了）
--max-depth=1  // 显示层级

2、df（disk free）：显示磁盘分区上可以使用的磁盘空间

// 命令参数
-a    // 查看全部文件系统，单位默认KB
-h    // 使用-h选项以KB、MB、GB的单位来显示，可读性高~~~（最常用）


3、df 与 du 的区别：

（1）df 命令用于查询整个文件系统的使用情况；du命令只统计目录或文件使用的空间（对于目录，递归统计）。所以 df 查询到的空间要大于 du。

（2）df 命令的实现是通过调用函数 statfs 查询文件系统的信息，这些信息是保存在文件系统中的；du 是通过 opendir()、stat() 函数查询文件大小，累加而出结果。

-- 2025

--git
 
 git clone https://gitlab.jpushoa.com/licz/etl-mapreduce.git
 -- 添加、更新
 git add  DeviceInfoLog.java
 git commit -m "device info upadte"
 git push
 
-- 删除文件 
git rm file.txt
git commit -m "删除文件"
git push
 
--克隆分支
git clone -b 营销系统v4.13.22_爱租机小程序搭建榜单频道 http://gitlab.internal.azj/loverent-toc-server/bigdata/loverent-oneservice-center.git

git clone -b 自动化选品修改商户类型与商户编码筛选规则（营销系统v4.13.24：活动模板自动化选品） http://gitlab.internal.azj/loverent-toc-server/bigdata/loverent-oneservice-center.git

git clone -b push指标加工  https://gitlab.stardustgod.com/esee/data5-server.git

git clone -b 分支名 网址.git


--修改分支名步骤
git checkout 当前分支名                 --切换到对应分支
git branch -m 新分支名                  -- 修改分支名称
git push origin --delete 旧分支名
git push origin 新分支名



# 查看当前分支（可选）
git branch
 
# 重命名当前分支
git branch -m new-feature
 
# 查看远程分支（可选）
git branch -r
 
# 删除旧的远程分支
git push origin --delete old-feature
 
# 推送新的远程分支
git push origin new-feature
 
# 设置新的上游跟踪分支（可选）
git branch --set-upstream-to=origin/new-feature


--new
git remote update origin --prune
用于同步本地仓库与远程仓库origin的分支，同时删除那些在远程已不存在的本地分支，保持两者同步。


一、
配置用户名和邮箱
使用git config命令配置用户名和邮箱
git config --global user.name "username"   有空格需要加双引号，没空格可以省略双引号

     git config --global user.name "lichaozhong"

git config --global user.email "xxx.xxx@.com"

    git config --global user.email "lichaozhong@crazymaplestudio.com"
加global表示对所有的git仓库都有效，不加则只对当前的git仓库有效
git config --global credential.helper store/保存用户名和密码
git config --global --istI  //查看配置信息

1.1切换分支命令
1 git checkout main
·注释：
git checkout main：此命令将当前工作目录切换到main分支，确保你在合并时位于正确的分支，避免不必要的错误


2.1基本合并命令
1 git merge<分支名
·示例
1   git merge feature-branch
·注释：
当你执行上述命令时，Git会尝试自动将feature-branch的更改合并到当前的main分支。如果没有冲突，合并将自动完
成。

-- 合并push指标加工 分支到test分支 步骤：
0 git branch            ：查看分支    
1 git checkout test
2 git pull origin test  ：git pul1命令将从远程仓库获取最新的test分支代码并合并到当前分支，确保代码的最新性。
3 git merge push指标加工 ：合并'push指标加工' 分支到test分支
4 git push

-- 从远程master分支更新本地分支lcz
1 git checkout lcz           -- 确保你已经切换到你想要更新的分支上
2 git checkout master
3 git fetch origin master    -- 拉取最新的master分支代码到本地
1 git checkout lcz           -- 确保你已经切换到你想要更新的分支上
4 git merge origin/master    -- 将master分支的更新合并到你当前的分支上
或者 git pull origin master   -- 你可以使用git pull命令来拉取并合并master分支的更新：

-- git pull origin master   # 等价于 git fetch + git merge origin/master

-- procedure 步骤
git checkout main 
git pull origin main 
git merge lcz
git push 或 git push origin main

-- 方法 1：强制允许合并无关历史（--allow-unrelated-histories）
-- 如果确实需要合并两个不相关的历史（例如，合并另一个仓库的代码），可以强制允许：

git merge lcz --allow-unrelated-histories

-- 是一个强制推送命令，它会覆盖远程仓库的 main 分支，使其与本地 main 分支完全一致
git push --force origin main 


--new
git remote update origin --prune
用于同步本地仓库与远程仓库origin的分支，同时删除那些在远程已不存在的本地分支，保持两者同步。



-- add
git add -A ./  → 添加当前目录及其子目录的 所有变更（包括删除的文件）。
git add ./     → 仅添加当前目录的新增和修改的文件（不包含删除的文件）。

git add -A    → 添加整个仓库的所有变更（包括删除的文件）。

-- 在 Git 中，如果你执行了 git add -A（或 git add --all）但想要撤销这些暂存的更改，可以使用以下命令：
1. 撤销所有暂存的更改
git reset
或等价于：
git reset HEAD
2. 撤销特定文件的暂存
如果只想取消暂存某些文件：
git reset HEAD <file1> <file2> ...



-- 查看当前分支的远程仓库 URL
git remote -v

-- 直接获取当前分支的远程 URL
git config --get remote.origin.url

--new 20250718

--graph：以图形化显示提交历史
--all：显示所有分支的提交
--oneline	简化输出，每行显示一个提交的缩写哈希和标题
git log --graph --all
git log --graph --all --oneline  # 简洁版
git log --graph --all --decorate --date=short  # 详细版


-- 查看分支关联的远程分支
git branch -vv





-- 无需联网，但数据可能不是最新的（需先 git fetch）。
git log origin/main  # 查看本地记录的远程 main 分支的提交历史
git show origin/main # 查看远程 main 分支的最新提交详情


-- 明确指定远程和分支（避免歧义）：
git pull origin lcz
-- 如果确定默认远程是 origin 且已设置跟踪分支，可以简写：  
-- 备注： git pull 是默认远程是 origin且当前分支已是git checkout lcz 所以省略了origin lcz

git pull  # 自动拉取当前分支的跟踪分支





git merge --abort   # 取消当前合并

-- 个人github创建代码库提交时需要设置下代理地址才能访问github
    如果需要代理：
    确保代理地址正确（如 Clash、V2Ray 等）：
    git config --global http.proxy "http://127.0.0.1:7890"
    git config --global https.proxy "http://127.0.0.1:7890"

-- 1. 暂存所有更改（包括新增、修改、删除的文件）
git add -A
或更明确的写法：
git add --all
-A 或 --all 会暂存：
新增的文件（untracked）
修改的文件（modified）
删除的文件（deleted）


-- linux 202507

vim下搜索
-- 进入搜索模式：按 / 键。
-- 输入搜索内容：输入 搜索内容，然后按回车。

1. 如果编辑器是 vim（默认情况）：
-- 进入搜索模式：按 / 键。
-- 输入搜索内容：输入 initContainers，然后按回车。
导航匹配项：
按 n 跳转到下一个匹配项。
按 N 跳转到上一个匹配项。
快速定位到 spec 下的 initContainers：
如果文件较大，可以先搜索 spec:，然后手动向下查找 initContainers（通常在 spec 的嵌套结构中）。




--linux
-- sftp user@xxx.xxx.xxx.xxx    : sftp -P 16333 etl@47.251.23.220   pw:crazymaple123

sftp上传下载文件、文件夹常用操作

1.查看上传下载目录lpwd

2.改变上传和下载的目录（例如D盘）：lcd  d:/  

3.查看当前路径
pwd

4.下载文件
进入你要下的文件所在的文件夹：
cd  【文件夹目录】
下载：
get  【文件名】

5.上传文件
进入你想要上传文件的目录
cd  【文件夹目录】
上传文件
put  【文件名】

6.上传下载文件夹格式：
下载文件夹
get -r 【文件夹名称】
上传文件夹
put -r 【文件夹名称】

使用绝对路径:

sftp> get /home/xxxx/filename D:\download\













--

spu(standard product unit)：标准化产品单元
sku（stock keeping unit）：最小库存单元
----------------
---添加字段

alter table azods.ods_loverent_platform_merchant_base_info_extend_da
 add column connectinvestid bigint default null comment '对接招商id',
 add column connectinvestname varchar default null comment '对接招商名称',
 add column agentscode varchar default null comment '代理商编码';
 

 
 
 
 alter table azods.ods_loverent_platform_merchant_base_info_extend_da
 add columns (connectinvestid bigint  comment '对接招商id',
 connectinvestname varchar  comment '对接招商名称',
 agentscode varchar comment '代理商编码');
 
 
 ALTER TABLE my_table change old_name new_name STRING;
 
 ----------
show partitions azods_dev.ods_rcas_whitelist_user_config_01_da;

show partitions azods.ods_loverent_aliorder_new_order_cancel_record_da; 


desc azods.ods_loverent_overdue_overdue_order_da_test;

DESC azods_dev.ods_risk_engine_data_order_info_da；

show partitions azods.ods_loverent_aliorder_new_order_point_use_record_da;


select count(1) 
from azods.ods_loverent_aliorder_new_order_point_use_record_da
where dt='20240318'
group by dt;

-----------
--新建采集ods表

CREATE TABLE IF NOT EXISTS azods.ods_loverent_aliorder_new_order_point_use_record_da(
`id`                            BIGINT COMMENT '',
`rentrecordno`                  STRING COMMENT '订单号',
`userid`                        BIGINT COMMENT '用户id',
`point`                         BIGINT COMMENT '积分使用',
`amount`                        DECIMAL COMMENT '积分换算金额',
`usetradeno`                    STRING COMMENT '使用流水号',
`refundtradeno`                 STRING COMMENT '回退流水号',
`usetime`                       DATETIME COMMENT '使用时间',
`refundtime`                    DATETIME COMMENT '退回时间'
)
COMMENT '订单积分使用表'
PARTITIONED BY (dt STRING) 
lifecycle 365;


alter table azods.ods_loverent_aliorder_new_order_point_use_record_da drop partition (dt='20240319');


alter table ads_azj_tag_usr_rfm_undeal_td drop if exists partition (dt In('20240415'));

alter table ads_azj_tag_usr_rfm_undeal_td drop if exists partition (dt<'20240415');



def query(sql,host = '47.100.160.60',user = '',database = 's1',password = ''):
    conn = pymysql.connect(
        host = host,
        user = user,
        database = database,
        password = password,
        charset = 'utf8')
    df = pd.read_sql(sql,con = conn)
    df.columns = [x.lower() for x in df.columns]
    conn.close()
    return df
    
    
    
--alter table azads.ads_azj_tag_log_visit_latest_usr_1d
--add column goods_code string  comment '商品编码';

-- 202505
ragflow:
-- git下载ragflow 
   git clone https://github.com/infiniflow/ragflow.git
   
-- windows powershell 进入ragflow下载目录，进入docker目录
   docker compose -f docker/docker-compose.yml up -d
   
   docker logs -f ragflow-server
   

-- 以管理员身份运行 PowerShell
-- 右键点击「开始菜单」 → 选择 Windows PowerShell (管理员)。 
 
# 允许入站连接（11434 端口）
New-NetFirewallRule -DisplayName "Ollama Port" -Direction Inbound -LocalPort 11434 -Protocol TCP -Action Allow

# 验证规则是否生效
Get-NetFirewallRule -DisplayName "Ollama Port" | Select-Object Enabled,Profile





-- tmp
Windows系统‌：
打开命令提示符（Win + R，输入cmd），
输入 echo %processor_architecture%
返回“amd64”为amd架构，返回“arm64”为arm架构。‌‌

Win + R → 输入 sysdm.cpl → 高级 → 环境变量
‌sysdm.cpl ：是 System Data Manager 的缩写‌，
全称为“System Data Manager Control Panel”‌它是一个控制面板项，
用于显示和管理系统的常规设置、硬件设置和高级设置。

‌appwiz.cpl：是Windows系统中“添加或删除程序”
（Application Wizard Control Panel）的缩写文件‌，属于控制面板的功能模块之一，
用于管理软件的安装与卸载。
