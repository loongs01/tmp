/*+cte_execution_mode=shared*/
SELECT
--       {{analysis_date}}
       {{group_date_field}}
       {{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,sum(sent_cnt)                                                         as sent_cnt                      -- 发送push任务次数
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(sent_uv))                    as sent_uv                       -- 发送push任务用户数
       -- ,sum(success_sent_cnt)                                                 as success_sent_cnt              -- 成功发送push任务次数
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(success_sent_uv))            as success_sent_uv               -- 成功发送push任务用户数
       -- ,sum(accepted_cnt)                                                     as accepted_cnt                  -- 接收push任务次数
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(accepted_uv))                as accepted_uv                   -- 接收push任务用户数
       -- ,sum(show_cnt)                                                         as show_cnt                      -- 展示push任务次数
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(show_uv))                    as show_uv                       -- 展示push任务用户数
       -- ,sum(click_cnt)                                                        as click_cnt                     -- 点击push任务次数
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(click_uv))                   as click_uv                      -- 点击push任务用户数
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(push_play_uv))               as push_play_uv                  -- Push转化播放用户数
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(push_purchase_uv))           as push_purchase_uv              -- Push转化内购用户数
       -- ,sum(push_purchase_amt)                                                as push_purchase_amt             -- Push转化内购金额
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(push_advertising_uv))        as push_advertising_uv            -- push广告收入用户数
       -- ,sum(push_advertising_amt)                                             as push_advertising_amt          -- Push广告收入
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(push_first_subscribe_uv))    as push_first_subscribe_uv       -- Push转化首订用户数
       -- ,sum(push_first_subscribe_amt)                                         as push_first_subscribe_amt      -- Push转化首订总收入
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(push_pay_cnt))               as push_pay_cnt                  -- Push转化付费用户数:点击push当次会话内，内购+订阅收入用户数
       -- ,sum(push_total_use_duration)                                          as push_total_use_duration       -- Push转化总使用时长 -- Push转化人均使用时长（分钟）点击push当次会话内，总使用时长/当日点击push用户数
FROM dws_data.dws_t88_reelshort_opc_push_aggregate_di t1
{{join_oth_dim}}
where 1=1 and analysis_date between  '{{start_date}}' and '{{end_date}}'
       {{where_and}}
-- group by {{analysis_date}}  {{fields}}
{{group_by}}
{{order_by}}
{{limit}}
;
