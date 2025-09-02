cat > hive-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>com.mysql.jdbc.Driver</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>hive</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>hive</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:mysql://liuf1:3306/hive?useUnicode=true&amp;characterEncoding=UTF-8&amp;useSSL=false</value>
    </property>
    <property>
        <name>hive.server2.enable.doAs</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/user/hive/warehouse</value>
    </property>
    <property>
        <name>hive.metastore.port</name>
        <value>9083</value>
    </property>
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://liuf1:9083</value>
    </property>
    <property>
        <name>hive.server2.map.fair.scheduler.queue</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.server2.support.dynamic.service.discovery</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.server2.zookeeper.namespace</name>
        <value>hiveserver2</value>
    </property>
    <property>
        <name>hive.zookeeper.quorum</name>
        <value>liuf1:2181,liuf2:2181,liuf3:2181</value>
    </property>
    <property>
        <name>hive.zookeeper.client.port</name>
        <value>2181</value>
    </property>
    <property>
        <name>hive.server2.thrift.bind.host</name>
        <value>liuf1</value>
    </property>
    <property>
        <name>hive.server2.thrift.port</name>
        <value>10000</value>
    </property>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>hive.execution.engine</name>
        <value>mr</value>
    </property>
    <property>
        <name>hive.merge.mapfiles</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.merge.mapredfiles</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.merge.size.per.task</name>
        <value>268435456</value>
    </property>
    <property>
        <name>hive.merge.smallfiles.avgsize</name>
        <value>104857600</value>
    </property>
    <property>
        <name>hive.exec.submit.local.task.via.child</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.drop.managed.table.data</name>
        <value>true</value>
    </property>
    <!--    add by lcz-->
    <!-- Spark相关配置 -->
    <property>
        <name>spark.master</name>
        <value>yarn</value>
    </property>

    <property>
        <name>spark.yarn.jars</name>
        <value>hdfs://nameservice1/spark-jars/*</value>
    </property>

    <property>
        <name>spark.eventLog.enabled</name>
        <value>true</value>
    </property>

    <property>
        <name>spark.eventLog.dir</name>
        <value>hdfs://nameservice1/spark-history</value>
    </property>

    <property>
        <name>spark.serializer</name>
        <value>org.apache.spark.serializer.KryoSerializer</value>
    </property>

    <property>
        <name>spark.sql.adaptive.enabled</name>
        <value>true</value>
    </property>

    <property>
        <name>spark.sql.adaptive.coalescePartitions.enabled</name>
        <value>true</value>
    </property>

    <!-- Spark资源配置 -->
    <property>
        <name>spark.executor.memory</name>
        <value>2g</value>
    </property>

    <property>
        <name>spark.executor.cores</name>
        <value>2</value>
    </property>

    <property>
        <name>spark.executor.instances</name>
        <value>2</value>
    </property>

    <property>
        <name>spark.driver.memory</name>
        <value>1g</value>
    </property>

    <property>
        <name>spark.driver.maxResultSize</name>
        <value>1g</value>
    </property>

    <!-- Hive与Spark集成配置 -->
    <property>
        <name>hive.spark.client.connect.timeout</name>
        <value>10000ms</value>
    </property>

    <property>
        <name>hive.spark.client.server.connect.timeout</name>
        <value>10000ms</value>
    </property>

    <property>
        <name>hive.spark.client.rpc.server.address</name>
        <value>liuf1:10000</value>
    </property>

    <property>
        <name>hive.spark.client.rpc.server.port</name>
        <value>10000</value>
    </property>

    <property>
        <name>hive.spark.client.secret.bits</name>
        <value>256</value>
    </property>


</configuration>
EOF