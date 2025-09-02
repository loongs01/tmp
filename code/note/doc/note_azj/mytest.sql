alter table edw.ad_push_bid_v3_latest_hour_log add columns (
uninstalled_app string
);

alter table edw.ad_push_bid_v3_log add columns (
uninstalled_app string
);


alter table edw.app_active_latest_hour_log add columns (
source string comment '统计获取方式：1 系统统计应用接口； 2  流量； 4 进程；8 默认目录(sdcard/anroid/data/packagename)；16 配置目录；Jcore 2.8.8新增'
);


alter table edw.app_active_log add columns (
source string comment '统计获取方式：1 系统统计应用接口； 2  流量； 4 进程；8 默认目录(sdcard/anroid/data/packagename)；16 配置目录；Jcore 2.8.8新增'
);
 
 
-------------------git
 
 git clone https://gitlab.jpushoa.com/licz/etl-mapreduce.git
 git add  DeviceInfoLog.java
 git commit -m "device info upadte"
 git push
---------------------
git clone -b 营销系统v4.13.22_爱租机小程序搭建榜单频道 http://gitlab.internal.azj/loverent-toc-server/bigdata/loverent-oneservice-center.git

git clone -b 自动化选品修改商户类型与商户编码筛选规则（营销系统v4.13.24：活动模板自动化选品） http://gitlab.internal.azj/loverent-toc-server/bigdata/loverent-oneservice-center.git

git clone -b 分支名 网址.git
--------------
--idea
Ctrl+H 显示类结构图
Ctrl + F12 查看类中所有方法  alt+7
Ctrl+F8 打上/取消断点

Ctrl+Shift+N找到这个类。


idea方法重写快捷键
Ctrl+O  方法重写

Ctrl+E 最近更改代码  显示最近打开的文件列表
Ctrl+Shift+空格 类型匹配代码补全
Ctrl+Alt+空格   第二次代码补全

Ctrl+N：按名字搜索类，这是最常用的查找类的方法之一。输入类名可以定位到这个类文件。12
Shift+Shift：搜索任何东西，包括类、资源、配置项、方法等。这个功能非常强大，可以搜索路径，例如，如果你在Java、JS、CSS、JSP中都有名为"hello"的文件夹，你可以使用这个快捷键来快速找到它们。



ctrl+alt+l 格式化代码
---------------------
SPU(Standard Product Unit)：标准化产品单元
SKU（Stock Keeping Unit）：最小库存单元
----------------
---添加字段

alter table azods.ods_loverent_platform_merchant_base_info_extend_da
 add column connectinvestid bigint default null comment '对接招商id',
 add column connectinvestname varchar default null comment '对接招商名称',
 add column agentscode varchar default null comment '代理商编码';
 
 
 
alter table azods.ods_loverent_platform_merchant_base_info_extend_da
 add columns (connectinvestid bigint default null comment '对接招商id',
 connectinvestname varchar default null comment '对接招商名称',
 agentscode varchar default null comment '代理商编码');
 
 
 
 alter table azods.ods_loverent_platform_merchant_base_info_extend_da
 add columns (connectinvestid bigint  comment '对接招商id',
 connectinvestname varchar  comment '对接招商名称',
 agentscode varchar comment '代理商编码');
 
 
 shippingnotice  勾选平台月选发货须知 1：已勾选 2：未勾选
 
 ALTER TABLE my_table CHANGE old_name new_name STRING;
 
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



def query(sql,
          host = '47.100.160.60',
          user = '',
          database = 's1',
          password = ''):
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