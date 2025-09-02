,t1.user_id                
,t1.stat_date              
,coalesce(t2.real_name,t1.user_name) 
            
,t1.user_id              
,t1.stat_date            
,t1.user_name            
,t1.sex                  
,t1.id_card              
-- ,t1.birthday
,date_add(
        t1.birthday,  -- 你的基础日期
        interval 
            floor(rand() * 24) * 3600 +  -- 随机小时（0-23）
            floor(rand() * 60) * 60 +    -- 随机分钟（0-59）
            floor(rand() * 60)           -- 随机秒（0-59）
        second
    )             
,t1.home_address         
,t1.company_address      
,t1.school_address       
,t1.occupation           
,t1.education_background 
,t1.age                  
,t1.work                 
,t1.login_name           
,t1.password             
,t1.status               
,t1.logout_time          
,t1.leave_reason         
,t1.user_level           
,t1.rfm_score            
,t1.user_lifecycle       
,t1.is_high_value        
,t1.is_premium           
,t1.is_content_creator   
,t1.created_time         
,t1.updated_time         



基于Transformer和Prompt的重构方案
select * from dim_user_info_di where  user_id in('25123451','25123452','25123453','52847682','94270657') order by user_id asc;


select * from dws_user_health_summary_di where  user_id in('25123451','25123452','25123453','52847682','94270657') order by user_id asc;


select * from dws_user_career_info where  user_id in('25123451','25123452','25123453','52847682','94270657') order by user_id asc;

select * from dws_user_education_learning_di where  user_id in('25123451','25123452','25123453','52847682','94270657') order by user_id asc;

select * from dws_user_family_relations_di where  user_id in('25123451','25123452','25123453','52847682','94270657') order by user_id asc;

select * from dws_user_financial_management_di where  user_id in('25123451','25123452','25123453','52847682','94270657') order by user_id asc;


select * from dws_user_leisure_entertainment_di where  user_id in('25123451','25123452','25123453','52847682','94270657') order by user_id asc;

select * from dws_user_life_events_di where  user_id in('25123451','25123452','25123453','52847682','94270657') order by user_id asc;

select * from dws_user_characteristic_aggregate_di where  user_id in('25123451','25123452','25123453','52847682','94270657') order by user_id asc;

select * from dws_user_destiny_analysis_di where  user_id in('25123451','25123452','25123453','52847682','94270657') order by user_id asc;

select * from ods_genetic_report_introductions where ybxx like '%女%32岁%'  52847682 窦娜

select * from ods_genetic_report_introductions where ybxx like '%女%27岁%'  94270657 苗小燕


select * from dws_user_clothing_behavior_agg_di 
where user_id in ('25123451','25123452','25123453','52847682','94270657') order by user_id asc;

select * from dws_user_food_dining_agg_di 
where user_id in ('25123451','25123452','25123453','52847682','94270657') order by user_id asc;

select * from dws_user_housing_activity_agg_mi 
where user_id in ('25123451','25123452','25123453','52847682','94270657') order by user_id asc;

select * from dws_user_transport_trip_agg_di 
where user_id in ('25123451','25123452','25123453','52847682','94270657') order by user_id asc;


