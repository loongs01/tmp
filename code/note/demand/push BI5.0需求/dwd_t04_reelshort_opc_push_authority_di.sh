echo "${TX_DATE}"
/usr/local/bin/python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dwd/dwd_t04_reelshort_opc_push_authority_di.sql ${TX_DATE}
exit $?