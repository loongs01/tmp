CREATE USER 'ranger'@'localhost' IDENTIFIED BY 'ranger';


CREATE DATABASE ranger;
CREATE USER 'ranger'@'%' IDENTIFIED BY 'ranger';
GRANT ALL PRIVILEGES ON ranger.* TO 'ranger'@'%';
FLUSH PRIVILEGES;

CREATE USER 'ranger'@'%' IDENTIFIED BY 'ranger';




scp -r /opt/datasophon/spark2/conf/spark-defaults.conf liuf1:/opt/datasophon/spark2/conf/

scp /opt/datasophon/spark2/conf/spark-defaults.conf liuf1:/opt/datasophon/spark2/conf/

scp  /opt/datasophon/datasophon-worker/script/datasophon-env.sh liuf3:/opt/datasophon/datasophon-worker/script/

sudo -u hdfs -i hdfs dfs -put /opt/datasophon/spark2/jars/* /spark-jars/

set hive.execution.engine=spark;
set spark.home;
set hive.execution.engine;

#spark
export SPARK_HOME=/opt/datasophon/spark2
export PATH=$SPARK_HOME/bin:$PATH


cd /opt/datasophon/datasophon-worker/script/
echo $SPARK_HOME


grep -R "DOOP_CONF_DIR" /opt/datasophon 2>/dev/null

DOOP_CONF_DIR 
grep -R "export SPARK_HOME=/opt/datasophon/spark-3.1.3" /opt/datasophon 2>/dev/null