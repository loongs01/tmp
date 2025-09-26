package org.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Driver 类（配置和运行作业）
public class WordCount {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\app\\hadoop");
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");

        System.out.println("hadoop.tmp.dir: " + conf.get("hadoop.tmp.dir"));
        System.out.println("java.io.tmpdir: " + System.getProperty("java.io.tmpdir"));
        System.out.println("Hadoop home: " + System.getProperty("hadoop.home.dir"));

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 输出路径
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);

        // 如果输出路径已存在，则删除
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true 表示递归删除
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
