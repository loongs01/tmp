/*+cte_execution_mode=shared*/
SELECT
--       {{analysis_date}}
       {{group_date_field}}
       {{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,count(1)                                                                                 as sent_cnt                      -- 发送push任务次数
       -- ,count(distinct t1.uuid)                                                                  as sent_uv                       -- 发送push任务用户数
       -- ,sum(if(t1.is_success=1,1,0))                                                             as success_sent_cnt              -- 成功发送push任务次数
       -- ,count(distinct if(t1.is_success=1, t1.uuid))                                             as success_sent_uv               -- 成功发送push任务用户数
       -- ,sum(if(t1.is_success=1,t1.delivered_cnt,0))                                              as accepted_cnt                  -- 接收push任务次数
       -- ,count(distinct if(t1.is_success=1 and t1.delivered_cnt>=1 ,t1.uuid))                     as accepted_uv                   -- 接收push任务用户数
       -- ,sum(t1.show_cnt)                                                                         as show_cnt                      -- 展示push任务次数
       -- ,count(distinct if(t1.show_cnt>=1, t1.uuid))                                              as show_uv                       -- 展示push任务用户数
       -- ,count(distinct if(t1.is_play=1, t1.uuid))                                                as push_play_uv                  -- Push转化播放用户数
       -- ,count(distinct if(t1.is_pay=1, t1.uuid))                                                 as push_purchase_uv              -- Push转化内购用户数
       -- ,sum(t1.sku_price)                                                                        as push_purchase_amt             -- Push转化内购金额
       -- ,count(distinct if(t1.ad_revenue>0, t1.uuid))                                             as push_advertising_uv        -- push广告收入用户数
       -- ,sum(t1.ad_revenue)                                                                       as push_advertising_amt          -- Push广告收入
       -- ,count(distinct if(t1.is_first_subscribe=1, t1.uuid))                                     as push_first_subscribe_uv       -- Push转化首订用户数
       -- ,sum(t1.first_subscribe_sku_price)                                                        as push_first_subscribe_amt      -- Push转化首订总收入
       -- ,count(distinct if(t1.is_pay=1 or t1.is_subscribe=1, t1.uuid))                            as push_pay_cnt               -- Push转化付费用户数:点击push当次会话内，内购+订阅收入用户数
       -- ,sum(t1.sum_online_times)                                                                 as push_total_use_duration       -- Push转化总使用时长 -- Push转化人均使用时长（分钟）点击push当次会话内，总使用时长/当日点击push用户数
       -- ,sum(t1.click_cnt)                                                                        as click_cnt                     -- 点击push任务次数
       -- ,count(distinct if(t1.click_cnt>=1, t1.uuid))                                             as click_uv                      -- 点击push任务用户数
FROM dwd_data.dwd_t04_reelshort_opc_push_detail_di t1
{{join_oth_dim}}
where 1=1 and analysis_date between  '{{start_date}}' and '{{end_date}}'
       {{where_and}}
-- group by {{analysis_date}}  {{fields}}
{{group_by}}
{{order_by}}
{{limit}}
;
