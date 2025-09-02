1.
交易域-租赁订单核心累积快照事实表：
dwd_azj_trd_ord_rent_order_da  

2.白名单源表：

---loverent_aliorder_new.rent_record
---loverent_aliorder_new.rent_record_goods
--loverent_aliorder_new.rent_record_delivery_address
---loverent_user.app_user
--loverent_liquidation.repayment_schedule
s1.bscore2107   -----风控模型组回写业务库表
risk_engine_data.order_info  ----ods无无同步
--loverent_aliorder_new.order_credit_advice
---loverent_overdue.overdue_order
rcas.black_list_20201118---风控模型组回写业务库表
---loverent_aliorder_new.rent_state
---rcas.whitelist_user_config_01
--rcas.whitelist_user_config_02
rcas.user_creditcline -----风控模型组回写业务库表




ods已同步：
---loverent_aliorder_new.rent_record
---loverent_aliorder_new.rent_record_goods
--loverent_aliorder_new.rent_record_delivery_address
---loverent_user.app_user
--loverent_liquidation.repayment_schedule
--loverent_aliorder_new.order_credit_advice
---loverent_overdue.overdue_order
---loverent_aliorder_new.rent_state
---rcas.whitelist_user_config_01
--rcas.whitelist_user_config_02

ods无同步：
rcas.user_creditcline -----风控模型组回写业务库表
s1.bscore2107   -----风控模型组回写业务库表
rcas.black_list_20201118---风控模型组回写业务库表
risk_engine_data.order_info  ----ods无无同步,dw无同步数据源

---------------------------
SPU(Standard Product Unit)：标准化产品单元
SKU（Stock Keeping Unit）：最小库存单元
--------------------------

------ps  loverent_oss_new
goods_spu   spuCode
goods_info   spuCode ->goodsCode     spuCode : goodsCode       1：n
goods_info_fp   goodsCode->productNo  goodsCode ：productNo   1：n    拿productNo
goods_fp_term  productNo:leaseTerm     1: n       productNo +leaseTerm    signContractAmount
------

--
1个SPU对应多个goods商品                                              --goods_info
商家上架的1个商品叫做1个SPU，
1个SPU可以配置多个售卖标准叫goods商品，                                 --goods_info
1个goods商品可以配置多个goods_fp 即商品的租赁方案，每个方案可以有多个SKU    --goods_info_fp
平台最终以goods商品作为最细粒度上架，在规格面板选择的其实是goods_fp
--


--state
 -5申请中取消  
 0提交 
 1 --待审批 --(可能还没风控等级)         
 2 已拒绝 3 待支付 4 待签约 5 已取消 6 待配货 7 待拣货 8 待发货 
 9 发货中 10 正常履约（已经签收）--(9,10,根据长短租 9,10,才会生成还款计划）
 11 提前解约中 12 提前解约 13 换机订单状态 14 维修订单状态 15 已逾期 16 归还中 17 提前买断 18 提前归还（废弃） 19 正常买断 20 已归还 
 21提前买断中 22正常买断中 23提前归还中（废弃） 24履约完成 25 强制买断中 26 强制买断 27待赔付 28 强制履约中 29 强制履约完成 
 30 强制归还中 31 强制定损完成 32 强制归还完成 33退货中 34已退货 35待复核 36待支付押金
 40 租赁到期 41还机逾期 42 续租中 43 续租逾期 44 续租履约完成 45续租到期 46续租还机逾期  47定损赔付逾期 48续租处理中


--营销系统v4.14.8：企业微信增加RFM标签
3.

loverent_liquidation.repayment_schedule         azods.ods_loverent_liquidation_repayment_schedule_da --还款计划表？ 

订单信息扩展表  loverent_aliorder_new.rent_record_goods.signContractAmount          azods.ods_loverent_aliorder_new_rent_record_goods_da          rentRecordNo
                                                                                                                        
loverent_aliorder_new.rent_record    根据申请时间           azods.ods_loverent_aliorder_new_rent_record_da 
埋点：
azods.ods_zhongan_log_xflowcloud_xcx_dev_di_rt_view     open_id        goods_code


loverent_aliorder_new.rent_record_apply_cancel   -- 申请中取消-订单主表    ：只包含-5申请中取消 order  azods.ods_loverent_aliorder_new.rent_record_apply_cancel

---风控等级：   
loverent_aliorder_new.order_credit_advice    ---订单审核建议表


---rent_record_cancel_apply  订单用户取消表  非




loverent_oss_new.goods_fp_term  ---: 商品租期表   signContractAmount :产品签约价值,租金+买断价 = 产品签约价值  《——dim_azj_itm_product_term
                 goods_info_fp  ---:商品表fp？
-- ps:
商品表 loverent_oss_new.goods_info->azcdm.dim_azj_itm_goods
azods.ods_zhongan_sdk_event_record_ck_all_his  ------open_id ，goods_code


goods_info商品表goodscode  goods_spu.spucode      goods_info_fp  商品表fp  

goods_info_fp   商品表fp
`goodsId` bigint(20) NOT NULL COMMENT '商品id',
  `spuCode` varchar(50) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'spu编码',
  `goodsCode` varchar(50) COLLATE utf8mb4_general_ci NOT NULL COMMENT '商品编码',
  `productNo` varchar(32) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '产品编号',


goods_fp_term  商品租期表
`productNo` varchar(32) DEFAULT NULL COMMENT '产品编号',
  `leaseTerm` int(6) DEFAULT NULL COMMENT '产品租期',
  `leaseAmount` decimal(10,2) DEFAULT NULL COMMENT '租金',
  `totalAmount` decimal(10,2) DEFAULT NULL COMMENT '总租金',
  `signContractAmount` decimal(10,2) DEFAULT NULL COMMENT '产品签约价值',





---product_code

--------租赁订单主表逻辑
-- 租赁订单商品信息
LEFT JOIN 
        azods.ods_loverent_aliorder_new_rent_record_goods_da AS rrgs
ON      rr.rentrecordno = rrgs.rentrecordno 


LEFT JOIN 
        azcdm.dim_azj_itm_goods goods 
ON      rrgs.goodscode = goods.goods_code
AND     goods.dt = '${bizdate}'



SELECT 
        open_id
        ,'TAGd3d99ce7' as tag_code
        ,'企微_偏好商品型号' as tag_name
        ,modelname  as tag_value
    FROM ( 
        SELECT lg.open_id,spu.modelname,COUNT(1) num
                ,ROW_NUMBER() OVER(PARTITION BY lg.open_id ORDER BY COUNT(1) DESC) AS rn 
        FROM   azods.ods_zhongan_log_xflowcloud_xcx_dev_di_rt_view lg
        LEFT JOIN azods.ods_loverent_oss_new_goods_info_da  goods
        ON     GET_JSON_OBJECT(json_data,'$.data.goods_code') = goods.goodscode
        AND    goods.dt = '${bizdate}'
        LEFT JOIN azods.ods_loverent_oss_new_goods_spu_da spu
        ON     goods.spucode = spu.spucode
        AND    spu.dt = '${bizdate}'
        WHERE  lg.dt = '${bizdate}'
        AND    COALESCE(open_id,'')!=''
        AND    is_test_source=0
        -- AND    is_custom_exposure_event=0
        AND    event_name='details_product' -- 商品详情页
        AND    GET_JSON_OBJECT(json_data,'$.data.goods_code') is not null
        GROUP BY lg.open_id,spu.modelname
    ) t WHERE t.rn = 1 AND modelname IS NOT NULL 



-----------
select periodOf as '第几期账期'
,paymentDueDate as '还款日期'
,periods as '还款期数',
rs.goodsValue as  '商品价值'
,concat(rs.signContractAmount/1000,'k') as '签约价值'
,rs.signContractAmount
,rs.batchNo as '生成还款计划批次'
,length(rs.remark)
,rs.*
from loverent_liquidation.repayment_schedule rs
where  rs.paymentDueDate >date'2024-03-22'
and rs.remark !='订单取消' 
-- and  length(trim(rs.remark))>0
and rs.remark is not null
and rs.periods-rs.periodOf>3 
and rs.periods>12
order by rs.paymentDueDate 
limit 50;



select
rr.state --
,rs.periodOf as '第几期账期'
,rs.paymentDueDate as '还款日期'
,rs.periods as '还款期数'
,rs.goodsValue as  '商品价值'
,concat(rs.signContractAmount/1000,'k') as '签约价值'
,rs.signContractAmount
,rs.batchNo as '生成还款计划批次'
,length(rs.remark)
,rs.*
from loverent_aliorder_new.rent_record rr
join loverent_liquidation.repayment_schedule rs
on rr.rentRecordNo = rs.reletOrderNo 
where  rs.paymentDueDate >date'2024-03-22'
and rs.remark !='订单取消' 
-- and  length(trim(rs.remark))>0
and rs.remark is not null
and rs.periods-rs.periodOf>3 
and rs.periods>12

and rr.state in (10 ,7 ,9 ,14 ,40 ,42 ,45)
order by rs.paymentDueDate 

limit 50;



select  
v.open_id
,v.json_data

from azods_dev.ods_zhongan_log_xflowcloud_xcx_dev_di_rt_view v
left join (
    select
    rr1.rentrecordno
    ,rr1.userid
    ,rr1.zmorderno
    ,rr1.zmuserid
    ,rr1.state
from azods_dev.ods_loverent_aliorder_new_rent_record_da rr1
where rr1.dt='20240325'
and rr1.state!=2
) rr
on rr.zmuserid=v.open_id 
where dt>'20240301'
and rr.userid is not  null
limit 10;




-------------
select
au.userId
,aub.userId
,rr.rentRecordNo 
,rr.state --
,rs.periodOf as '第几期账期'
,rs.paymentDueDate as '还款日期'
,rs.periods as '还款期数'
,rs.goodsValue as  '商品价值'
,concat(rs.signContractAmount/1000,'k') as '签约价值'
,rs.signContractAmount  as 'b签约价值'
,rs.batchNo as '生成还款计划批次'
,rs.remark
,length(rs.remark)
,oca.azGrade 
,rrg.leaseTerm   as '产品租期（月）'
,rrg.reletLeaseTerm  as '续租租期'
,rrg.periods   as '实际期数'
,concat(rrg.signContractAmount/1000,'k') as '扩展表签约价值'
,rrg.showAmount as '显示价值'
,'-------'
,rs.*
from loverent_aliorder_new.rent_record rr
join loverent_liquidation.repayment_schedule rs
on  rs.reletOrderNo=rr.rentRecordNo
left join loverent_aliorder_new.order_credit_advice oca
on oca.rentRecordNo=rr.rentRecordNo 
left join loverent_aliorder_new.rent_record_goods rrg
on rrg.rentRecordNo=rr.rentRecordNo 
left join loverent_user.app_user au
on au.userId =rr.userId 
left join loverent_user.app_user_business aub
on aub.userId=rr.userId 
where  rs.paymentDueDate >date'2024-03-22'
 and rr.state in (10 ,7 ,9 ,14 ,40 ,42 ,45)
 -- and rr.state in (5,2)
 and rs.periods>12
order by rs.paymentDueDate 
limit 50;




----tag
INSERT INTO loverent_tag.tag
(tagName, tagNo, tagDesc, tagRule, bindNum, createTime, updateTime, dr, `type`, platformCode, tagType)
VALUES('RFM标签', 'TAG04d8e310', '企微目标用户打标', NULL, 0, '2024-04-09 16:44:54', '2024-04-17 13:54:56', 0, 0, 'LOVERENT_TAG', NULL);



select 
a.materiel_newold_config_id
,a.materiel_newold_config_name
,concat_ws(',',a.materiel_newold_config_name)
from azads_dev.ads_azj_activity_auto_goods_info_da a
where a.dt= '${bizdate}' 
and size(a.materiel_newold_config_id)>1
limit 20;