-- 播放量、点赞量 收藏量
dws_data.dws_t82_reelshort_user_play_chapter_detail_di


-- 运营日报-钉钉预警
dm_reelshort_data.dm_t80_reelshort_opc_lang_stat_df


-- dau
dw_view.dws_t80_reelshort_user_bhv_cnt_detail_di


-- reelshort用户当天动作统计类信息详情表
dws_t80_reelshort_user_bhv_cnt_detail_di

-- reelshort日报
dm_reelshort_data.dm_t80_reelshort_daily_stat_df



--test
/usr/bin/python3 /dolphinscheduler/etl/resources/scripts/reelshort/common/py_excute_sql.py /dolphinscheduler/etl/resources/scripts/reelshort/language_info_v2_202412061021.sql 2024-12-03


/usr/bin/python3 /dolphinscheduler/etl/resources/scripts/reelshort/sync/exp_mongodb_reelshort_book_level_info.py 2024-12-31


/usr/bin/python3 /dolphinscheduler/etl/resources/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dwd/dwd_t01_reelshort_device_info.sql ${TX_DATE}



--pro
/usr/local/bin/python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dm/dm_t82_reelshort_story_play_init_di.sql 2024-12-10


--pro
/usr/local/bin/python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dwd/dwd_t04_reelshort_opc_push_detail_di.sql 2024-12-28

/usr/local/bin/python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dws/dws_t88_reelshort_opc_push_aggregate_di.sql 2024-12-27

--mongodb
/usr/local/bin/python3 /home/etl/scripts/reelshort/sync/exp_mongodb_reelshort_story_play_init_di.py 2024-12-10



/usr/local/bin/python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dwd/dim_t99_sku_info.sql ${TX_DATE}


-- dolphin config
python3 ${scriptsPath}/common/py_excute_sql.py ${scriptsPath}/view/dws/dws_t85_magic_prop_detail_stat_di.sql ${TX_DATE}

