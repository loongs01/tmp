echo "${TX_DATE}"
/usr/local/bin/python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dm/dm_t82_reelshort_story_play_statistic_di.sql ${TX_DATE}
exit $?