登录网址：https:/www.google.com.hk/gmail
袖邮箱名：
lichaozhong@crazymaplestudio.com
初始密码：cms123456  lcx990125

LDAP账号密码为：
  username: lichaozhong
  password: Licz@2204

-- 2025
showdoc：lichaozhong@crazymaplestudio.com/secret同飞书登录密码



--python export 配置信息
D:\git\data-dw-sz\branches\v1.0.0\scripts\reelshort\sync\common\handleMongodb.py


/dolphinscheduler/etl/resources/scripts/config.ini







-- analyticDB
-- ADB测试库url: 

公网： amv-2ev8c441hro07g58800000101o.ads.aliyuncs.com  3306  

内网： amv-2ev8c441hro07g58800000101.ads.aliyuncs.com  3306  
测试环境ADB: 账号  lichaozhong  密码： lichaozhong123%

conn = pymysql.connect(host='amv-2ev8c441hro07g58800000101o.ads.aliyuncs.com',
                          port=3306,
                          user='fyhd_dla',
                          passwd='fyhd_dla123%',
                          db='dw_data')


--ddl
## ADB US3.0 
host=am-2evbgp6s6eu7yci6e90650.ads.aliyuncs.com
user=fyhd_dla
passwd=h43Yaq46vf0dV0ZEK#752
db=chapters_log
port=3306

-- pro:
Dbhost: am-2evbgp6s6eu7yci6e90650o.ads.aliyuncs.com
Dbport:  3306
Dbuser:  lichaozhong
Dbpass:  PK50stAQJUujq#b6H8Eh4N2TLBWlGdXO

-- mysql -h am-2evbgp6s6eu7yci6e90650.ads.aliyuncs.com -u fyhd_dla -P 3306 -p


-- Reelshort 性能上报ADB实例信息
Dbhost: amv-2ev3jku42323trej800000118o.ads.aliyuncs.com
-- 内网：amv-2ev3jku42323trej800000118.ads.aliyuncs.com
Dbport: 3306
Dbuser: lichaozhong
Dbpass:  HN8sAd2Kxck*ds9B

-- Reelshort 性能上报ADB实例信息
Dbuser: data_warehouse
Dbpass: QHg8s^Ad2Kxck*ds3






-- test
dolphinscheduler:
http://172.16.20.220:12345/dolphinscheduler/ui/projects/15004634165280/workflow/definitions/15299401565868

账号： etl  密码：crazymaple123

-- pro
http://dolphin.crazymaplestudios.com/dolphinscheduler/ui/home

lichaozhong lichaozhong123


etl服务器
测试： 172.16.20.220 root 123456
-- port:22

正式ETL服务器：
47.251.23.220 
port: 16333  
etl
crazymaple123
new :crazymaple147


离线代码库，git 
https://gitlab.stardustgod.com/data-adb/data-dw-sz.git



git log --author lichaozhong --since="2024-03-01T00:00:00+08:00" --until="2025-03-07T23:59:59+08:00" --numstat --no-merges -- . | grep -E "^[0-9]+\s+[0-9]+\s+" | awk '{ add += $1; subs += $2 } END { printf "Added lines: %s\nRemoved lines: %s\nTotal changes: %s\n", add, subs, add + subs }'