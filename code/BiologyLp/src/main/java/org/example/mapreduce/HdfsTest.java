package org.example.mapreduce;

import util.HdfsUtils;

public class HdfsTest {

    public static void main(String[] args) {
        try {
            System.out.println("开始测试 HDFS 连接...");

            // 测试连接
            HdfsUtils.testConnection();
            // 如果你有特定的文件路径，可以测试读取
            // 例如: HdfsUtils.readFile("/user/test/sample.txt");

            // 测试列出目录
            // HdfsUtils.listFiles("/user");
//            HdfsUtils.readFile("hdfs://nameservice1/user/hive/warehouse/ads.db/dim_user_info_di");
//            HdfsUtils.readFileLimit("/user/hive/warehouse/ads.db/dim_user_info_di/dt=20250726/part-m-00000");
//            HdfsUtils.readFileLimit("/user/hive/warehouse/ods.db/ods_disease_info_di/output.txt", 3);
            HdfsUtils.readFileLimit("/user/hive/warehouse/ads.db/dim_user_info_di/dt=20250726/part-m-00000", 3);
            HdfsUtils.readFileToList("/user/hive/warehouse/ods.db/c_food_code_export/20250913/c_food_code.txt");


            System.out.println("HDFS 连接测试完成。");
        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                HdfsUtils.close();
            } catch (Exception e) {
                System.err.println("关闭连接失败: " + e.getMessage());
            }
        }
    }
}

