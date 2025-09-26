package org.example.mapreduce;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import config.AppConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.HiveUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class WordSplitMR25 {

    // 简单的分词结果类
    public static class SimpleWord {
        public String word;
        public String nature;

        public SimpleWord(String word, String nature) {
            this.word = word;
            this.nature = nature;
        }
    }

    // 简单的分词方法
    public static List<SimpleWord> simpleSegment(String text) {
        List<SimpleWord> words = new ArrayList<>();
        System.out.println("SimpleSegment input: " + text);

        // 如果没有标点符号，按字符分割（简单的中文分词）
        if (!text.matches(".*[\\p{Punct}\\s].*")) {
            // 按字符分割，每2-3个字符组成一个词
            for (int i = 0; i < text.length(); i += 2) {
                int end = Math.min(i + 3, text.length());
                String word = text.substring(i, end);
                if (!word.trim().isEmpty()) {
                    // 简单的词性判断
                    String nature = "n"; // 默认为名词
                    if (word.matches(".*[的得地].*")) {
                        nature = "u"; // 助词
                    } else if (word.matches(".*[了着过].*")) {
                        nature = "u"; // 助词
                    } else if (word.matches(".*[很非常十分].*")) {
                        nature = "d"; // 副词
                    } else if (word.matches(".*[是有的在].*")) {
                        nature = "v"; // 动词
                    } else if (word.matches(".*[一二三四五六七八九十百千万].*")) {
                        nature = "m"; // 数词
                    }

                    words.add(new SimpleWord(word, nature));
                    System.out.println("Added word: " + word + " (" + nature + ")");
                }
            }
        } else {
            // 按标点符号和空格分割
            String[] parts = text.split("[\\s\\p{Punct}]+");

            for (String part : parts) {
                if (!part.trim().isEmpty()) {
                    // 简单的词性判断
                    String nature = "n"; // 默认为名词
                    if (part.matches(".*[的得地].*")) {
                        nature = "u"; // 助词
                    } else if (part.matches(".*[了着过].*")) {
                        nature = "u"; // 助词
                    } else if (part.matches(".*[很非常十分].*")) {
                        nature = "d"; // 副词
                    } else if (part.matches(".*[是有的在].*")) {
                        nature = "v"; // 动词
                    } else if (part.matches(".*[一二三四五六七八九十百千万].*")) {
                        nature = "m"; // 数词
                    }

                    words.add(new SimpleWord(part.trim(), nature));
                    System.out.println("Added word: " + part.trim() + " (" + nature + ")");
                }
            }
        }

        System.out.println("SimpleSegment output count: " + words.size());
        return words;
    }

    // Mapper类：读取句子信息并进行分词
    public static class WordSplitMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                // 输入格式：text_md5\tsent_no\tsent_text\tsent_md5\tsent_split_param
                String line = value.toString().trim();
                if (line.isEmpty()) return;

                String[] parts = line.split("\t");
                String loadTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                String dt = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                if (parts.length < 5) {
                    System.err.println("Invalid input line: " + line);
                    return;
                }
             /*   System.out.println(line);
                System.out.println(parts.length);*/
                String textMd5 = parts[0];
                String sentNo = parts[1];
                String sentText = parts[2];
                String sentMd5 = parts[3];
                String sentSplitParam = parts[4];
//                System.out.println(sentText.length());
                if (sentText.isEmpty()) return;
//                System.out.println("Processing sentence: " + sentText);
                // 处理短文本情况（原Python逻辑）
                if (sentText.length() <= 6) {
                    String wordInfo = String.format("%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s",
                            textMd5, sentNo, sentText, sentMd5, sentSplitParam, 0, sentText, sentSplitParam, loadTime, dt);
//                    System.out.println(wordInfo);
                    context.write(new Text(textMd5), new Text(wordInfo));
                } else {
                    try {
                        // 使用简单分词方法
//                        List<SimpleWord> wordList = simpleSegment(sentText);
                        List<Term> wordList = HanLP.segment(sentText);

                        // 输出分词结果
                        for (int i = 0; i < wordList.size(); i++) {
                            Term word = wordList.get(i);
                            // 格式：word_no\tword_text\tpart_of_speech\tsent_md5
                            String wordInfo = String.format("%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s",
                                    textMd5, sentNo, sentText, sentMd5, sentSplitParam, i + 1, word.word, word.nature, loadTime, dt);
                            System.out.println(wordInfo);
                            context.write(new Text(textMd5), new Text(wordInfo));
                        }
                    } catch (Exception e) {
                        System.err.println("Error in word segmentation: " + e.getMessage());
                        e.printStackTrace();
                        // 如果分词失败，将整个句子作为一个词处理
                        String wordInfo = String.format("%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s",
                                textMd5, sentNo, sentText, sentMd5, sentSplitParam, 1, sentText, "unknown", loadTime, dt);
                        System.out.println(wordInfo);
                        context.write(new Text(textMd5), new Text(wordInfo));
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in map method: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    // Reducer类：将mapper输出写入Hive表和HDFS文件
    public static class WordSplitReducer extends Reducer<Text, Text, NullWritable, Text> {

        private HiveUtil hiveUtil;
        private String tableName;
        private int batchSize = 1000; // 批量插入大小
        // 分区值 dt -> 该分区下的多行 VALUES 片段（不含分区列），使用Set去重
        private java.util.Map<String, java.util.LinkedHashSet<String>> dtToValues = new java.util.HashMap<>();
        private int totalRecords = 0;
        // 可配置：与目标表非分区列顺序严格对应的列清单（留空则不指定列名）
        private String insertColumns = "";
        // 可配置：数据库名（默认 ods）
        private String databaseName = "ods";
        // 可配置：是否覆盖分区（重跑分区）
        private boolean overwritePartition = false;
        // 可配置：仅处理目标分区 dt（例如 20250922），为空则处理所有
        private String targetDt = "";
        // 记录已清理过的分区，避免重复 DROP
        private java.util.Set<String> clearedPartitions = new java.util.HashSet<>();
        // 控制单次 INSERT 的最大VALUES行数与SQL长度，避免SQL过长
        private static final int MAX_VALUES_PER_INSERT = 2000;
        private static final int MAX_SQL_LENGTH = 900000;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            try {
                // 初始化Hive连接
                hiveUtil = new HiveUtil();

                // 从配置中获取表名
                Configuration conf = context.getConfiguration();
                tableName = conf.get("hive.table.name", "");
                System.out.println("Config hive.table.name: " + tableName);
                insertColumns = conf.get("hive.table.columns", "");
                if (insertColumns != null) insertColumns = insertColumns.trim();
                System.out.println("Config hive.table.columns: " + (insertColumns == null ? "null" : insertColumns));
                databaseName = conf.get("hive.database", "ods");
                overwritePartition = Boolean.parseBoolean(conf.get("hive.overwrite", "TRUE"));
                targetDt = conf.get("hive.target.dt", "");
                System.out.println("Config hive.database: " + databaseName);
                System.out.println("Config hive.overwrite: " + overwritePartition);
                System.out.println("Config hive.target.dt: " + targetDt);


                // 创建数据库和表（如需要可打开）
//                createHiveTable();

                // 会话级别启用动态分区（尽管本实现按分区静态插入，也设置上以防需要）
                try {
                    hiveUtil.executeUpdate("SET hive.exec.dynamic.partition=true");
                    hiveUtil.executeUpdate("SET hive.exec.dynamic.partition.mode=nonstrict");
                    // 避免某些环境下 StatsTask 执行失败导致 INSERT 失败
                    hiveUtil.executeUpdate("SET hive.stats.autogather=false");
                } catch (SQLException se) {
                    System.err.println("Failed to set Hive session properties: " + se.getMessage());
                }

                System.out.println("HiveUtil initialized successfully, table: " + tableName);

            } catch (Exception e) {
                System.err.println("Failed to initialize HiveUtil: " + e.getMessage());
                e.printStackTrace();
                // 如果Hive初始化失败，仍然可以写入HDFS文件
            }
        }

        /**
         * 创建Hive表
         */
        private void createHiveTable() throws SQLException {
            try {
                // 创建数据库（如果不存在）
                String databaseName = "ods";
//                hiveUtil.createDatabase(databaseName);
                hiveUtil.useDatabase(databaseName);

                // 创建表结构
                String createTableSQL = String.format(
                        "CREATE TABLE IF NOT EXISTS %s.%s (" +
                                "text_md5 STRING COMMENT '文本MD5', " +
                                "sent_no STRING COMMENT '句子编号', " +
                                "sent_text STRING COMMENT '句子文本', " +
                                "sent_md5 STRING COMMENT '句子MD5', " +
                                "sent_split_param STRING COMMENT '分词参数', " +
                                "word_no INT COMMENT '词语编号', " +
                                "word_text STRING COMMENT '词语文本', " +
                                "part_of_speech STRING COMMENT '词性', " +
                                "load_time STRING COMMENT '加载时间', " +
                                "dt STRING COMMENT '日期分区' " +
                                ") " +
                                "PARTITIONED BY (year_month STRING) " +
                                "STORED AS TEXTFILE " +
                                "LOCATION '%s/%s'",
                        databaseName, tableName, AppConfig.HIVE_WAREHOUSE_DIR, tableName
                );

                hiveUtil.executeUpdate(createTableSQL);
                System.out.println("Hive table created successfully: " + tableName);

            } catch (SQLException e) {
                System.err.println("Failed to create Hive table: " + e.getMessage());
                throw e;
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            try {
                for (Text value : values) {
                    String line = value.toString();

                    // 写入HDFS文件（保持原有功能）
//                    context.write(NullWritable.get(), value);

                    // 准备写入Hive表的数据
                    if (hiveUtil != null) {
                        prepareHiveData(line);
                    }

                    totalRecords++;
                }

                // 批量写入Hive表
                if (hiveUtil != null && !dtToValues.isEmpty()) {
                    batchInsertToHive();
                }

            } catch (Exception e) {
                System.err.println("Error in reduce method: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * 准备Hive数据
         */
        private void prepareHiveData(String line) {
            try {
                // 解析数据行
                String[] parts = line.split("\t");
                if (parts.length >= 10) {
                    String textMd5 = parts[0];
                    String sentNo = parts[1];
                    String sentText = parts[2];
                    String sentMd5 = parts[3];
                    String sentSplitParam = parts[4];
                    String wordNo = parts[5];
                    String wordText = parts[6];
                    String partOfSpeech = parts[7];
                    String loadTime = parts[8];
                    String dt = parts[9];

                    // 若配置了只处理某个目标分区，则过滤其余分区
                    if (targetDt != null && !targetDt.isEmpty() && !targetDt.equals(dt)) {
                        return;
                    }

                    // 对字符串进行转义，避免SQL注入/语法错误
                    String vTextMd5 = escapeSql(textMd5);
                    String vSentNo = escapeSql(sentNo);
                    String vSentText = escapeSql(sentText);
                    String vSentMd5 = escapeSql(sentMd5);
                    String vSentSplitParam = escapeSql(sentSplitParam);
                    String vWordNo = wordNo; // 数值列
                    String vWordText = escapeSql(wordText);
                    String vPartOfSpeech = escapeSql(partOfSpeech);
                    String vLoadTime = escapeSql(loadTime);
                    String vDt = escapeSql(dt);

                    // VALUES 片段（不包含分区列 dt，按静态分区方式插入）
                    String values = String.format("('%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s')",
                            vTextMd5, vSentNo, vSentText, vSentMd5, vSentSplitParam,
                            vWordNo, vWordText, vPartOfSpeech, vLoadTime);

                    java.util.LinkedHashSet<String> set = dtToValues.computeIfAbsent(dt, k -> new java.util.LinkedHashSet<>());
                    set.add(values);

                    // 达到批量大小时执行插入
                    if (set.size() >= Math.max(1, batchSize)) {
                        flushOnePartition(dt, set);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error preparing Hive data: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * 批量插入数据到Hive表
         */
        private void batchInsertToHive() {
            if (dtToValues.isEmpty()) return;

            try {
                int inserted = 0;
                for (java.util.Map.Entry<String, java.util.LinkedHashSet<String>> entry : dtToValues.entrySet()) {
                    String dt = entry.getKey();
                    java.util.LinkedHashSet<String> valuesSet = entry.getValue();
                    if (valuesSet.isEmpty()) continue;

                    inserted += flushOnePartition(dt, valuesSet);
                }


                System.out.println("Batch inserted " + inserted + " records to Hive table");

                // 清空批量数据
                dtToValues.clear();

            } catch (SQLException e) {
                System.err.println("Failed to batch insert to Hive: " + e.getMessage());
                e.printStackTrace();
                // 清空批量数据，避免重复插入
                dtToValues.clear();
            }
        }

        /**
         * 刷新单个分区，将当前缓存的VALUES分批写入，控制每次SQL长度和行数。
         */
        private int flushOnePartition(String dt, java.util.LinkedHashSet<String> valuesSet) throws SQLException {
            if (valuesSet == null || valuesSet.isEmpty()) return 0;
            if (tableName == null || tableName.isEmpty()) return 0;

            // 首次写该分区时，如开启重跑，清理旧分区
            if (overwritePartition && !clearedPartitions.contains(dt)) {
                String dropSql = new StringBuilder()
                        .append("ALTER TABLE ").append(databaseName).append(".").append(tableName)
                        .append(" DROP IF EXISTS PARTITION (dt='").append(escapeSql(dt)).append("')")
                        .toString();
                try {
                    hiveUtil.executeUpdate(dropSql);
                    clearedPartitions.add(dt);
                    System.out.println("Dropped existing partition dt=" + dt + " for table " + databaseName + "." + tableName);
                    System.out.println(!clearedPartitions.contains(dt));
                } catch (SQLException dropEx) {
                    System.err.println("Failed to drop partition dt=" + dt + ": " + dropEx.getMessage());
                }
            }

            int count = 0;
            String[] all = valuesSet.toArray(new String[0]);
            int start = 0;
            while (start < all.length) {
                StringBuilder valuesChunk = new StringBuilder();
                int rows = 0;
                int i = start;
                while (i < all.length && rows < MAX_VALUES_PER_INSERT) {
                    String v = all[i];
                    int addLen = v.length() + (rows == 0 ? 0 : 2); // include ", "
                    if (valuesChunk.length() + addLen > MAX_SQL_LENGTH) break;
                    if (rows > 0) valuesChunk.append(", ");
                    valuesChunk.append(v);
                    rows++;
                    i++;
                }

                if (rows == 0) break; // fallback safety

                StringBuilder insert = new StringBuilder();
                insert.append(overwritePartition ? "INSERT INTO TABLE " : "INSERT OVERWRITE TABLE ")
                        .append(databaseName).append(".").append(tableName)
                        .append(" PARTITION (dt='").append(escapeSql(dt)).append("') ");
                if (insertColumns != null && !insertColumns.isEmpty()) {
                    insert.append("( ").append(insertColumns).append(" ) ");
                }
                insert.append("VALUES ").append(valuesChunk);

                hiveUtil.executeUpdate(insert.toString());
                count += rows;
                start = i;
            }

            // 已写入后清空该分区当前缓存
            valuesSet.clear();
            return count;
        }

        private int getBufferedRowCount() {
            int total = 0;
            for (java.util.LinkedHashSet<String> set : dtToValues.values()) {
                total += set.size();
            }
            return total;
        }

        private String escapeSql(String s) {
            if (s == null) return "";
            // 简单转义单引号与反斜杠
            return s.replace("\\", "\\\\").replace("'", "''");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            try {
                // 处理剩余的批量数据
                if (hiveUtil != null && !dtToValues.isEmpty()) {
                    batchInsertToHive();
                }

                // 关闭Hive连接
                if (hiveUtil != null) {
                    hiveUtil.close();
                }

                System.out.println("WordSplitReducer cleanup completed. Total records processed: " + totalRecords);

            } catch (Exception e) {
                System.err.println("Error in cleanup: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 反射验证
        System.out.println(WordSplitReducer.class.getName());

        if (args.length < 2) {
            System.err.println("Usage: WordSplitMR <input path> <output path> [hive_table_name]");
            System.err.println("Example: WordSplitMR /input/data /output/result word_split_result");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String hiveTableName = args.length == 2 ? args[1] : "word_split_result";

        Configuration conf = new Configuration();

        // 设置Hive相关配置
        conf.set("hive.table.name", hiveTableName);
        conf.set("hive.jdbc.url", AppConfig.HIVE_JDBC_URL);
        conf.set("hive.user", AppConfig.HIVE_USER);
        conf.set("hive.password", AppConfig.HIVE_PASSWORD);

        // 设置Hadoop配置
        conf.set("fs.defaultFS", AppConfig.HADOOP_HDFS_URI);
        conf.set("hadoop.job.ugi", AppConfig.HADOOP_USER);

        // 设置MapReduce配置
        conf.set("mapreduce.job.reduces", "1"); // 设置Reducer数量
        conf.set("mapreduce.job.queuename", "default"); // 设置队列名

        Job job = Job.getInstance(conf, "Word Split with Hive Integration");

        job.setJarByClass(WordSplitMR25.class);
        job.setMapperClass(WordSplitMR25.WordSplitMapper.class);
        job.setReducerClass(WordSplitMR25.WordSplitReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 设置输入路径
        FileInputFormat.addInputPath(job, new Path(inputPath));

        // 设置输出路径
        Path outputPathObj = new Path(outputPath);
        FileSystem fs = FileSystem.get(conf);

        // 如果输出路径已存在，则删除
        if (fs.exists(outputPathObj)) {
            System.out.println("Output path exists, deleting: " + outputPath);
            fs.delete(outputPathObj, true); // true 表示递归删除
        }

        FileOutputFormat.setOutputPath(job, outputPathObj);

        // 设置输出格式
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);

        System.out.println("Starting WordSplitMR job...");
        System.out.println("Input path: " + inputPath);
        System.out.println("Output path: " + outputPath);
        System.out.println("Hive table: " + hiveTableName);
        System.out.println("Hive JDBC URL: " + AppConfig.HIVE_JDBC_URL);

        // 提交作业并等待完成
        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println("WordSplitMR job completed successfully!");
            System.out.println("Results written to HDFS: " + outputPath);
            System.out.println("Results also written to Hive table:" + hiveTableName);
        } else {
            System.err.println("WordSplitMR job failed!");
        }

        System.exit(success ? 0 : 1);
    }
}