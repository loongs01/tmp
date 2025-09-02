-- drop table if exists dws_data.dws_t82_reelshort_play_data5_bitmap_di;
create table if not exists dws_data.dws_t82_reelshort_play_data5_bitmap_di (
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
  -- cversion varchar default '' comment '代码版本',
  -- res_version varchar default '' comment '资源版本',
  language_id varchar default '' comment '语言id',
  platform varchar default '' comment '平台id',

  sub_event_name varchar default '' comment 'sub_event_name',
  scene_name varchar default '' comment '场景名称',
  page_name varchar default '' comment '页面名称',
  pre_page_name varchar default '' comment '上一个页面名称',
  action varchar default '' comment 'action',
  shelf_id varchar default '' comment '书架id',
  story_id varchar default '' comment '书籍id',
  referrer_story_id varchar default '' comment '来源书籍id',
  -- chap_id varchar default '' comment '章节id',
  -- chap_order_id int comment '章节序列id',
  -- video_id varchar default '' comment '视频id',


  cover_click_uv      varbinary      comment '剧目点击人数'
  ,cover_click_cnt    bigint         comment '剧目点击次数'
  ,show_uv            varbinary      comment 'APP元素曝光人数'
  ,show_cnt           decimal(38,4)  comment 'APP元素曝光次数'
  ,click_uv           varbinary      comment 'APP元素点击人数'
  ,click_cnt          decimal(38,4)  comment 'APP元素点击次数'
  ,play_duration      decimal(38,4)  comment '播放时长'
  ,play_uv            varbinary      comment '播放人数'
  ,play_cnt           bigint         comment '播放次数'
  -- ,play_storys        varbinary      comment '播放剧集个数(story_id数）'
  -- ,play_chaps         varbinary      comment '播放集数(chapid)'
  ,play_complete_uv   varbinary      comment '完播人数'
  ,play_complete_cnt  bigint         comment '完播次数',
  primary key (id,analysis_date,story_id)
) distribute by hash(id,analysis_date,story_id) partition by value(date_format(analysis_date, '%y%m')) lifecycle 120 index_all='y' storage_policy='HOT' engine='xuanwu' table_properties='{"format":"columnstore"}' comment='reelshort用户内容分发bitmap汇总表';



delete from dws_data.dws_t82_reelshort_play_data5_bitmap_di where analysis_date='${TX_DATE}';
insert into dws_data.dws_t82_reelshort_play_data5_bitmap_di
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
      -- ,cversion
      -- ,res_version
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
      -- ,video_id
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.cover_click_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as cover_click_uv
      ,sum(cover_click_cnt) as cover_click_cnt
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.show_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as show_uv
      ,cast(sum(show_cnt) as decimal(38,4)) as show_cnt
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.click_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as click_uv
      ,cast(sum(click_cnt) as decimal(38,4)) as click_cnt
      ,cast(sum(if(sub_event_name='play_end',play_duration/60,0.0)) as decimal(38,4)) as play_duration
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct t.uuid SEPARATOR '#' ),'#') as array(int)))) as play_uv
      ,sum(play_start_cnt) as play_cnt
      -- ,0 as play_storys
      -- ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.sub_event_name='play_start',concat(t1.book_id,uuid)) SEPARATOR '#' ),'#') as array(int)))) as play_storys
      -- ,0 as play_chaps
      -- ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.sub_event_name='play_start',concat(t1.book_id,chap_order_id,uuid)) SEPARATOR '#' ),'#') as array(int)))) as play_chaps
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.play_complete_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as play_complete_uv
      ,sum(play_complete_cnt) as play_complete_cnt
from dws_data.dws_t82_reelshort_user_play_data5_detail_di as t
-- left join (
           -- select
           -- t1._id as id
           -- ,t1.book_id
           -- ,row_number() over(partition by t1._id order by t1.updated_at desc) as rn
           -- from chapters_log.dts_project_v_new_book as t1
           -- ) as t1
-- on t1.id=t.story_id and t1.rn=1
where etl_date='${TX_DATE}'
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
       -- ,cversion
       -- ,res_version
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
       -- ,video_id
;