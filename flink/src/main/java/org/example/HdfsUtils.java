package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class HdfsUtils {

    private static Configuration conf;
    private static FileSystem fs;

    static {
        try {
            setupConfiguration();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化 HDFS 配置
     */
    private static void setupConfiguration() throws IOException {
        conf = new Configuration();

        // 从 resources 目录加载配置文件
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");

        // 设置用户名，避免权限问题
        System.setProperty("HADOOP_USER_NAME", "root");

        // 创建 FileSystem 实例
        fs = FileSystem.get(URI.create("hdfs://nameservice1"), conf);
    }

    /**
     * 获取 HDFS FileSystem 实例
     */
    public static FileSystem getFileSystem() {
        return fs;
    }

    /**
     * 检查文件是否存在
     */
    public static boolean exists(String path) throws IOException {
        return fs.exists(new Path(path));
    }

    /**
     * 列出目录下的文件
     */
    public static void listFiles(String dirPath) throws IOException {
        Path path = new Path(dirPath);
        FileStatus[] fileStatuses = fs.listStatus(path);

        System.out.println("目录 " + dirPath + " 下的文件:");
        for (FileStatus status : fileStatuses) {
            System.out.println(status.getPath().getName() + " - " +
                    (status.isDirectory() ? "目录" : "文件") +
                    " - 大小: " + status.getLen() + " bytes");
        }
    }

    /**
     * 读取文件内容
     */
    public static void readFile(String filePath) throws IOException {
        Path path = new Path(filePath);

        if (!fs.exists(path)) {
            System.out.println("文件不存在: " + filePath);
            return;
        }

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(path), "UTF-8"));

        String line;
        System.out.println("读取文件 " + filePath + " 的内容:");
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        IOUtils.closeStream(reader);
    }

    /**
     * 测试 HDFS 连接
     */
    public static void testConnection() {
        try {
            System.out.println("HDFS URI: " + fs.getUri());
            System.out.println("HDFS 工作目录: " + fs.getWorkingDirectory());
            System.out.println("HDFS 连接成功!");

            // 测试根目录访问
            listFiles("/");

        } catch (Exception e) {
            System.err.println("HDFS 连接失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 关闭 FileSystem
     */
    public static void close() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }
}
