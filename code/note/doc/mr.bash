java -jar BiologyLp-1.0-SNAPSHOT.jar org.example.mapreduce.WordSplitMR /user/hive/warehouse/ods.db/ods_disease_info_di/output/test01/part-r-00000 /user/hive/warehouse/ods.db/ods_disease_info_di/output/test02

hadoop jar BiologyLp-1.0-SNAPSHOT.jar org.example.mapreduce.WordSplitMR /user/hive/warehouse/ods.db/ods_disease_info_di/output/test01/part-r-00000 /user/hive/warehouse/ods.db/ods_disease_info_di/output/test02


/dolphinscheduler/hdfs/resources/mapreduce/BiologyLp-1.0-SNAPSHOT.jar



hdfs://nameservice1/


/opt/datasophon/hadoop-3.3.3/


#!/bin/bash
/opt/datasophon/hadoop-3.3.3/bin/hadoop jar hdfs://nameservice1/dolphinscheduler/hdfs/resources/mapreduce/BiologyLp-1.0-SNAPSHOT.jar org.example.mapreduce.WordSplitMR /user/hive/warehouse/ods.db/ods_disease_info_di/output/test01/part-r-00000 /user/hive/warehouse/ods.db/ods_disease_info_di/output/test02

#!/bin/bash
/opt/datasophon/hadoop-3.3.3/bin/hadoop jar mr/BiologyLp-1.0-SNAPSHOT.jar org.example.mapreduce.WordSplitMR /user/hive/warehouse/ods.db/ods_disease_info_di/output/test01/part-r-00000 /user/hive/warehouse/ods.db/ods_disease_info_di/output/test02

# test
cd /home/licz.1  
$HADOOP_HOME/bin/hadoop jar BiologyLp-1.0-SNAPSHOT.jar org.example.mapreduce.WordSplitMR \
/user/hive/warehouse/ods.db/ods_disease_info_di/output/test01/part-r-00000 \
/user/hive/warehouse/ods.db/ods_disease_info_di/output/test02


-- 调度
$HADOOP_HOME/bin/hadoop jar BiologyLp-1.0-SNAPSHOT.jar org.example.mapreduce.WordSplitMR \
/user/hive/warehouse/ods.db/ods_app_user_sentence_info_di/dt=20250922/part-r-00000 \
ods_app_user_info_di

$HADOOP_HOME/bin/hadoop jar BiologyLp-1.0-SNAPSHOT.jar org.example.mapreduce.SentenceSplitMR \
/user/hive/warehouse/ods.db/test_export/20250922/test.txt \
/user/hive/warehouse/ods.db/ods_app_user_sentence_info_di/dt=20250922



#!/bin/bash
BASEDIR=$(cd `dirname $0`; pwd)
cd $BASEDIR
echo $BASEDIR
#source /opt/datasophon/dolphinscheduler-3.1.1/worker-server/conf/dolphinscheduler_env.sh

# 下载 JAR 文件
HADOOP_HOME=/opt/datasophon/hadoop-3.3.3
JAR_HDFS_PATH="hdfs://nameservice1/dolphinscheduler/hdfs/resources/mr/BiologyLp-1.0-SNAPSHOT.jar"
JAR_LOCAL_PATH="$BASEDIR/BiologyLp-1.0-SNAPSHOT.jar"

$HADOOP_HOME/bin/hadoop fs -get $JAR_HDFS_PATH $BASEDIR

chmod a+rwx $JAR_LOCAL_PATH
# 执行 MapReduce 任务
$HADOOP_HOME/bin/hadoop jar $JAR_LOCAL_PATH org.example.mapreduce.WordSplitMR \
  /user/hive/warehouse/ods.db/ods_disease_info_di/output/test01/part-r-00000 \
  /user/hive/warehouse/ods.db/ods_disease_info_di/output/test02
  
  
  
  