sqoop import \
-D mapred.job.name=ads_user_profile_json_da \
-m 1 \
--connect "jdbc:mysql://192.168.10.105:3306/sq_liufengdb?allowLoadLocalInfile=false&autoDeserialize=false&allowLocalInfile=false&allowUrlInLocalInfile=false" \
--username licz.1 \
--password GjFmT5NEiE \
--query "SELECT 
id,
user_id,
user_name,
basic_info,
career_info,
family_info,
interest_info,
health_info,
behavior_info,
life_events,
aspirations,
crowd_tags,
consumption_traits,
profile_score,
update_time,
data_version,
stat_date
FROM ads_user_profile_json_da WHERE \$CONDITIONS" \
--hive-import \
--hive-database ads \
--hive-table ads_user_profile_json_da \
--hive-overwrite \
--hive-partition-key dt \
--hive-partition-value \$PARTITION_VALUE \
--delete-target-dir


sqoop import \
-D mapred.job.name=ads_user_profile_json_da \
-m 1 \
--connect "jdbc:mysql://192.168.10.105:3306/sq_liufengdb?allowLoadLocalInfile=false&autoDeserialize=false&allowLocalInfile=false&allowUrlInLocalInfile=false" \
--username licz.1 \
--password GjFmT5NEiE \
--query "SELECT 
id,
user_id,
user_name,
basic_info,
career_info,
family_info,
interest_info,
health_info,
behavior_info,
life_events,
aspirations,
crowd_tags,
consumption_traits,
profile_score,
update_time,
data_version,
stat_date
FROM ads_user_profile_json_da WHERE \$CONDITIONS" \
--hive-import \
--hive-database ads \
--hive-table ads_user_profile_json_da \
--hive-overwrite \
--hive-partition-key dt \
--hive-partition-value \$PARTITION_VALUE \
--delete-target-dir