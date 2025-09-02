dwd_data.dwd_t02_reelshort_custom_event_di  --1              ：12
dwd_data.dwd_t02_reelshort_custom_event_di
dws_data.dws_t85_reelshort_srv_currency_change_stat_di  -2   ：7
dws_data.dws_t85_reelshort_srv_currency_change_stat_di
dwd_data.dwd_t04_reelshort_checkpoint_unlock_di  --3         ：7



dwd_data.dwd_t02_reelshort_custom_event_di  1
dws_data.dws_t85_reelshort_srv_currency_change_stat_di 2
dwd_data.dwd_t04_reelshort_checkpoint_unlock_di  3
dwd_data.dwd_t05_reelshort_order_detail_di   4
dwd_data.dwd_t05_reelshort_transaction_detail_di 5
dwd_data.dwd_t02_reelshort_play_event_di 6


18:33dwd_data.dwd_t02_reelshort_custom_event_di  --1  9 
dws_data.dws_t85_reelshort_srv_currency_change_stat_di --2 4 &&
dwd_data.dwd_t04_reelshort_checkpoint_unlock_di  --3 4 &&
dwd_data.dwd_t05_reelshort_order_detail_di    --4 1
dwd_data.dwd_t05_reelshort_transaction_detail_di  --5 2
dwd_data.dwd_t02_reelshort_play_event_di  --6 2

3.互动剧选项卡解锁成功pv
4.互动剧选项卡解锁成功uv
5.剧集解锁pv
6.剧集解锁uv
9.剧集广告解锁完成pv
10.剧集广告解锁完成uv
13.剧集金币解锁完成pv
14.剧集金币解锁完成uv

vt_dwd_t02_reelshort_play_event_di_hour.sql

-- 小时task
vt_view_dwd_reelshort_total_hour
-- 分钟
vt_view_dwd_reelshort_total_n1_min


dws_data.dim_t87_reelshort_ads_user_attr_df
dws_data.dws_t87_reelshort_book_attr_di
dwd_data.`dwd_t01_reelshort_book_attr_di`

dw_view.`dws_t87_reelshort_book_attr_di` 
<-dw_view.`vt_dwd_t01_reelshort_book_attr_di`小时
+dws_data.dws_t87_reelshort_book_attr_di 天

--dws_t80_reelshort_user_bhv_cnt_detail_di_v2.sql
dw_view.dws_t87_reelshort_book_attr_di