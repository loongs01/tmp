1.

select  usr.alipay_user_id            as alipay_user_id       --芝麻ID

​    ,count(distinct ro.user_idno_md5)     as alipay_user_id_idno_num   --同一芝麻ID，分享的商品链接（1个或多个），关联的订单对应的账号的身份证号数量

from   azods.ods_loverent_user_user_share_record_da sr

join   azcdm.dim_azj_usr_user as usr

on    usr.user_id = sr.shareruserid

and   usr.dt = '${bizdate}'

lateral view outer EXPLODE(SPLIT(sr.orderid,',')) tmp as rent_order --通过分享的商品链接（1个或多个）产生的订单

left join azcdm.dwd_azj_trd_ord_rent_order_da as ro

on    ro.dt = '${bizdate}'

and   tmp.rent_order = ro.rent_record_no

where  sr.dt = '${bizdate}'

--AND   SIZE(SPLIT(sr.orderid,',')) > 2

group by usr.alipay_user_id



;