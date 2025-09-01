package org.example;

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
