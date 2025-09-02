echo "${TX_DATE}"
/usr/local/bin/python3 /home/etl/scripts/reelshort/sync/exp_mongodb_reelshort_story_play_init_di.py ${TX_DATE}
exit $?