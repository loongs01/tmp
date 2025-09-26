package org.example.mapreduce;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import util.HdfsUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * 将 MySQL 查询结果导出到 HDFS，按天分区。
 * 用法：
 * args[0] = jdbcUrl (例如 jdbc:mysql://192.168.10.103:3306/sqtext?useUnicode=true&characterEncoding=utf8&useSSL=false)
 * args[1] = username
 * args[2] = password
 * args[3] = sql (如：select user_id, text_md5, text, dt from t_people_health_text)
 * args[4] = hdfsBasePath (如：/data/sqtext/t_people_health_text)
 * args[5] = partitionDate (yyyy-MM-dd)，为空则使用今天
 */
public class MySQLToHdfsExporter {

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: MySQLToHdfsExporter <jdbcUrl> <user> <password> <sql> <hdfsBasePath> [partitionDate]");
            System.exit(1);
        }

        String jdbcUrl = args[0];
        String user = args[1];
        String password = args[2];
        String sql = args[3];
        String hdfsBasePath = args[4];
        String partitionDate = args.length >= 6 ? args[5] : LocalDate.now().format(DateTimeFormatter.ISO_DATE);

        FileSystem fs = HdfsUtils.getFileSystem();
        String partitionPath = hdfsBasePath + "/dt=" + partitionDate;
        Path dir = new Path(partitionPath);
        if (!fs.exists(dir)) {
            fs.mkdirs(dir);
        }

        Path outFile = new Path(partitionPath + "/part-00000.csv");
        if (fs.exists(outFile)) {
            fs.delete(outFile, false);
        }

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement ps = conn.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)) {

            // 避免一次性加载过多数据
            ps.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            try (FSDataOutputStream out = fs.create(outFile, true)) {
                StringBuilder header = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) header.append(',');
                    header.append(meta.getColumnLabel(i));
                }
                header.append('\n');
                out.write(header.toString().getBytes("UTF-8"));

                while (rs.next()) {
                    StringBuilder line = new StringBuilder();
                    for (int i = 1; i <= columnCount; i++) {
                        if (i > 1) line.append(',');
                        String val = rs.getString(i);
                        if (val == null) {
                            // 空值留空
                        } else {
                            // 简单 CSV 转义：替换换行，转义双引号并用引号包裹
                            String v = val.replace("\r", " ").replace("\n", " ");
                            if (v.contains(",") || v.contains("\"") || v.contains("\n")) {
                                v = '"' + v.replace("\"", "\"\"") + '"';
                            }
                            line.append(v);
                        }
                    }
                    line.append('\n');
                    out.write(line.toString().getBytes("UTF-8"));
                }
                out.hflush();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Export failed: " + e.getMessage(), e);
        }

        System.out.println("Exported to HDFS: " + outFile);
    }
}


