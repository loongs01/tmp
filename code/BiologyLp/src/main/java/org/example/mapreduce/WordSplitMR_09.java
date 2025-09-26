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

public class WordSplitMR_09 {

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
                            textMd5, sentNo, sentText, sentMd5, sentSplitParam, 0, sentText, sentSplitParam, loadTime,dt);
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
                                    textMd5, sentNo, sentText, sentMd5, sentSplitParam, i + 1, word.word, word.nature, loadTime,dt);
                            System.out.println(wordInfo);
                            context.write(new Text(textMd5), new Text(wordInfo));
                        }
                    } catch (Exception e) {
                        System.err.println("Error in word segmentation: " + e.getMessage());
                        e.printStackTrace();
                        // 如果分词失败，将整个句子作为一个词处理
                        String wordInfo = String.format("%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s",
                                textMd5, sentNo, sentText, sentMd5, sentSplitParam, 1, sentText, "unknown", loadTime,dt);
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
        private List<String> batchData = new ArrayList<>();
        private int totalRecords = 0;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            
            try {
                // 初始化Hive连接
                hiveUtil = new HiveUtil();
                
                // 从配置中获取表名，默认为word_split_result
                Configuration conf = context.getConfiguration();
                tableName = conf.get("hive.table.name", "word_split_result");
                
                // 创建数据库和表
                createHiveTable();
                
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
                String databaseName = "biology_db";
                hiveUtil.createDatabase(databaseName);
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
                    context.write(NullWritable.get(), value);
                    
                    // 准备写入Hive表的数据
                    if (hiveUtil != null) {
                        prepareHiveData(line);
                    }
                    
                    totalRecords++;
                }
                
                // 批量写入Hive表
                if (hiveUtil != null && !batchData.isEmpty()) {
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
                    
                    // 生成分区字段
                    String yearMonth = dt.substring(0, 6); // 取年月部分
                    
                    // 构建INSERT语句的数据部分
                    String values = String.format("('%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s')",
                        textMd5, sentNo, sentText, sentMd5, sentSplitParam, wordNo, 
                        wordText, partOfSpeech, loadTime, dt, yearMonth);
                    
                    batchData.add(values);
                    
                    // 达到批量大小时执行插入
                    if (batchData.size() >= batchSize) {
                        batchInsertToHive();
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
            if (batchData.isEmpty()) return;
            
            try {
                // 构建批量INSERT语句
                StringBuilder insertSQL = new StringBuilder();
                insertSQL.append("INSERT INTO TABLE biology_db.").append(tableName);
                insertSQL.append(" PARTITION(year_month) VALUES ");
                insertSQL.append(String.join(", ", batchData));
                
                // 执行插入
                hiveUtil.executeUpdate(insertSQL.toString());
                
                System.out.println("Batch inserted " + batchData.size() + " records to Hive table");
                
                // 清空批量数据
                batchData.clear();
                
            } catch (SQLException e) {
                System.err.println("Failed to batch insert to Hive: " + e.getMessage());
                e.printStackTrace();
                // 清空批量数据，避免重复插入
                batchData.clear();
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            
            try {
                // 处理剩余的批量数据
                if (hiveUtil != null && !batchData.isEmpty()) {
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
        String hiveTableName = args.length > 2 ? args[2] : "word_split_result";

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
        conf.set("mapreduce.job.reduces", "3"); // 设置Reducer数量
        conf.set("mapreduce.job.queuename", "default"); // 设置队列名
        
        Job job = Job.getInstance(conf, "Word Split with Hive Integration");

        job.setJarByClass(WordSplitMR_09.class);
        job.setMapperClass(WordSplitMR_09.WordSplitMapper.class);
        job.setReducerClass(WordSplitMR_09.WordSplitReducer.class);

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
            System.out.println("Results also written to Hive table: biology_db." + hiveTableName);
        } else {
            System.err.println("WordSplitMR job failed!");
        }

        System.exit(success ? 0 : 1);
    }
}