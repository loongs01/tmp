insert into dws_data.dws_t82_reelshort_user_exposure_data5_detail_di_bak07
select * from dws_data.dws_t82_reelshort_user_exposure_data5_detail_di
where etl_date='${TX_DATE}'
;