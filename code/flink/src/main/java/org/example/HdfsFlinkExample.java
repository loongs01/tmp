package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HdfsFlinkExample {

    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 Hadoop 配置
        setupHadoopConfiguration();

        // 从 HDFS 读取文件
        String hdfsPath = "hdfs://nameservice1/user/hive/warehouse/ads.db/ads_user_profile_json_da/dt=20250726/part-m-00000";

        // 创建文件源
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(hdfsPath))
                .build();

        System.out.println("Flink Path 的文件系统: " + new Path(hdfsPath).toUri());

        // 创建数据流
        DataStream<String> stream = env.fromSource(source,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "hdfs-source");

        // 处理数据并输出
        stream.print();

        // 执行任务
        env.execute("HDFS Flink Example");
    }

    /**
     * 设置 Hadoop 配置
     */
    private static void setupHadoopConfiguration() {
        // 获取当前类加载器加载配置文件
        Configuration hadoopConf = new Configuration();

        // 从 resources 目录加载配置文件
        hadoopConf.addResource("core-site.xml");
        hadoopConf.addResource("hdfs-site.xml");

        // 设置默认文件系统
        hadoopConf.set("fs.defaultFS", "hdfs://nameservice1");

        // 设置 HDFS 高可用配置
        hadoopConf.set("dfs.nameservices", "nameservice1");
        hadoopConf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        hadoopConf.set("dfs.namenode.rpc-address.nameservice1.nn1", "liuf2:8020");
        hadoopConf.set("dfs.namenode.rpc-address.nameservice1.nn2", "liuf3:8020");
        hadoopConf.set("dfs.client.failover.proxy.provider.nameservice1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        // 在 Windows 环境下设置用户名（避免权限问题）
        System.setProperty("HADOOP_USER_NAME", "root");

        // 设置全局 Hadoop 配置
        org.apache.hadoop.fs.FileSystem.setDefaultUri(hadoopConf, "hdfs://nameservice1");
        System.out.println("默认文件系统: " + hadoopConf.get("fs.defaultFS"));

        try {
            // 测试连接
            FileSystem fs = FileSystem.get(hadoopConf);
            System.out.println("成功连接到 HDFS: " + fs.getUri());
        } catch (Exception e) {
            System.err.println("连接 HDFS 失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

