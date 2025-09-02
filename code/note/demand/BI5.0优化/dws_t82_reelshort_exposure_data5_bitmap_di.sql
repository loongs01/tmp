-- drop table if exists dws_data.dws_t82_reelshort_exposure_data5_bitmap_di;
create table if not exists dws_data.dws_t82_reelshort_exposure_data5_bitmap_di (
  id bigint auto_increment,
  -- uuid varchar default '' comment '用户uuid,即idfa或者adid',
  analysis_date date comment '分析日期',
  is_pay int comment '1 支付用户，0 未付费用户',
  is_login int comment '是否登录1-是',
  user_type int comment '1-新用户',
  vip_type int default '0' comment 'sub_status用户订阅状态:0=未购买  1=生效中 2=已过期',
  af_network_name varchar default '' comment 'appsflyer归因渠道（媒体来源）',
  af_channel varchar default '' comment 'appsflyer渠道',
  country_id varchar default '' comment '国家id',
  channel_id varchar default '' comment '渠道id',
  version varchar default '' comment '底包版本',
  cversion varchar default '' comment '代码版本',
  res_version varchar default '' comment '资源版本',
  language_id varchar default '' comment '语言id',
  platform varchar default '' comment '平台id',

  sub_event_name varchar default '' comment 'sub_event_name',
  scene_name varchar default '' comment '场景名称',
  page_name varchar default '' comment '页面名称',
  pre_page_name varchar default '' comment '上一个页面名称',
  action varchar default '' comment 'action',
  shelf_id varchar default '' comment '书架id',
  story_id varchar default '' comment '书籍id',
  -- referrer_story_id varchar default '' comment '来源书籍id',
  -- chap_id varchar default '' comment '章节id',
  -- chap_order_id int comment '章节序列id',

  exposure_uv varbinary comment '剧目曝光人数',
  exposure_cnt bigint comment '剧目曝光次数',
  report_cnt bigint comment '上报次数',
  click_cnt bigint comment '点击次数',
  primary key (id,analysis_date,story_id)
) distribute by hash(id,analysis_date,story_id) partition by value(date_format(analysis_date, '%y%m')) lifecycle 120 index_all='y' storage_policy='mixed' hot_partition_count=6 engine='xuanwu' table_properties='{"format":"columnstore"}' comment='reelshort曝光bitmap汇总表';

-- alter table dws_data.dws_t82_reelshort_exposure_data5_bitmap_di
-- add column shelf_name varchar comment '书架名称'
-- ;



delete from dws_data.dws_t82_reelshort_exposure_data5_bitmap_di where analysis_date='${TX_DATE}';
insert into dws_data.dws_t82_reelshort_exposure_data5_bitmap_di
select
      null as id
      ,etl_date as analysis_date
      ,is_pay
      ,is_login
      ,user_type
      ,vip_type
      ,af_network_name
      ,af_channel
      ,country_id
      ,channel_id
      ,version
      ,cversion
      ,res_version
      ,language_id
      ,platform
      ,sub_event_name
      ,scene_name
      ,page_name
      ,pre_page_name
      ,action
      ,shelf_id
      ,story_id
      ,referrer_story_id
      -- ,chap_id
      -- ,chap_order_id
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.show_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as exposure_uv                    -- 剧目曝光人数
      ,sum(show_cnt) as exposure_cnt -- 剧目曝光次数
      ,sum(report_cnt) as report_cnt
      ,sum(click_cnt)  as click_cnt
      ,shelf_name
from dws_data.dws_t82_reelshort_user_exposure_data5_detail_di as t
where t.etl_date='${TX_DATE}'
      and t.uuid regexp '^[0-9]+$'
      and t.uuid<2147483647
group by
        etl_date
        ,is_pay
        ,is_login
        ,user_type
        ,vip_type
        ,af_network_name
        ,af_channel
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,sub_event_name
        ,scene_name
        ,page_name
        ,pre_page_name
        ,action
        ,shelf_id
        ,story_id
        ,referrer_story_id
        -- ,chap_id
        -- ,chap_order_id
        ,shelf_name
;