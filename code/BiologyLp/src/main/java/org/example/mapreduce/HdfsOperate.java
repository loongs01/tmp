package org.example.mapreduce;

import config.AppConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import util.MySQLUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class HdfsOperate implements AutoCloseable {
    private MySQLUtil mySQLUtil;
    private FileSystem hadoopFS;

    public HdfsOperate() throws Exception {
        this.mySQLUtil = new MySQLUtil();

        // 初始化Hadoop连接
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", AppConfig.HADOOP_HDFS_URI);
        this.hadoopFS = FileSystem.get(conf);
    }

    /**
     * 从MySQL表读取数据并写入HDFS文件
     *
     * @param tableName MySQL表名
     * @param hdfsPath  HDFS输出路径
     * @param fileName  输出文件名
     * @param query     自定义查询SQL（可选，为null时使用默认查询）
     */
    public void exportTableToHdfs(String tableName, String hdfsPath, String fileName, String query) throws Exception {
        // 确保HDFS目录存在
        Path path = new Path(hdfsPath);
        if (!hadoopFS.exists(path)) {
            hadoopFS.mkdirs(path);
        }

        // 按日期分区
        String datePartition = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        Path fullPath = new Path(hdfsPath + "/" + datePartition + "/" + fileName + ".txt");

        // 构建查询SQL
        String sql = query != null ? query : buildDefaultQuery(tableName);

        try (Connection conn = mySQLUtil.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql);
             BufferedWriter writer = new BufferedWriter(
                     new OutputStreamWriter(hadoopFS.create(fullPath), StandardCharsets.UTF_8))) {

            // 获取列信息
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 写入列标题
            StringBuilder header = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) header.append("\t");
                header.append(metaData.getColumnName(i));
            }
            writer.write(header.toString());
            writer.newLine(); //写入换行符

            // 写入数据行
            int rowCount = 0;
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) row.append("\t");
                    String value = rs.getString(i);
                    if (value != null) {
                        // 转义制表符和换行符
                        value = value.replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r");
                        row.append(value);
                    }
                }
                writer.write(row.toString());
                writer.newLine();
                rowCount++;

                // 每1000行输出一次进度
                if (rowCount % 1000 == 0) {
                    System.out.println("已处理 " + rowCount + " 行数据...");
                }
            }

            System.out.println("数据导出完成！");
            System.out.println("表名: " + tableName);
            System.out.println("HDFS路径: " + fullPath);
            System.out.println("总行数: " + rowCount);
        }
    }

    /**
     * 构建默认查询SQL
     */
    private String buildDefaultQuery(String tableName) {
        return "SELECT * FROM " + tableName;
    }

    /**
     * 导出指定表的所有数据
     */
    public void exportTableToHdfs(String tableName, String hdfsPath, String fileName) throws Exception {
        exportTableToHdfs(tableName, hdfsPath, fileName, null);
    }

    /**
     * 导出指定表的所有数据，使用表名作为文件名
     */
    public void exportTableToHdfs(String tableName, String hdfsPath) throws Exception {
        exportTableToHdfs(tableName, hdfsPath, tableName);
    }

    /**
     * 导出指定表的所有数据，使用默认HDFS路径
     */
    public void exportTableToHdfs(String tableName) throws Exception {
        String defaultPath = "/user/hive/warehouse/ods.db/" + tableName + "_export";
        exportTableToHdfs(tableName, defaultPath, tableName);
    }

    /**
     * 批量导出多个表
     */
    public void exportMultipleTables(String[] tableNames, String baseHdfsPath) throws Exception {
        for (String tableName : tableNames) {
            System.out.println("开始导出表: " + tableName);
            try {
                exportTableToHdfs(tableName, baseHdfsPath + "/" + tableName);
                System.out.println("表 " + tableName + " 导出成功！");
            } catch (Exception e) {
                System.err.println("表 " + tableName + " 导出失败: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * 检查HDFS连接状态
     */
    public boolean checkHdfsConnection() {
        try {
            return hadoopFS.exists(new Path("/"));
        } catch (IOException e) {
            System.err.println("HDFS连接检查失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 关闭连接
     */
    public void close() throws IOException {
        if (hadoopFS != null) {
            hadoopFS.close();
        }
        if (mySQLUtil != null) {
            mySQLUtil.close();
        }
    }

    /**
     * 主方法，用于测试
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("用法: java HdfsOperate <table_name> [hdfs_path] [file_name]");
            System.out.println("示例: java HdfsOperate user_info /user/data user_export");
            return;
        }

        try (HdfsOperate hdfsOp = new HdfsOperate()) {
            // 检查HDFS连接
            if (!hdfsOp.checkHdfsConnection()) {
                System.err.println("HDFS连接失败，请检查配置！");
                return;
            }

            String tableName = args[0];
            String hdfsPath = args.length > 1 ? args[1] : "/user/hive/warehouse/ods.db/" + tableName + "_export";
            String fileName = args.length > 2 ? args[2] : tableName;

            System.out.println("开始导出表: " + tableName);
            hdfsOp.exportTableToHdfs(tableName, hdfsPath, fileName);
            System.out.println("导出完成！");

        } catch (Exception e) {
            System.err.println("导出失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
