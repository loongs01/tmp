package org.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SentenceSplitMR {

    // Mapper类：读取输入文本并分割成句子
    public static class SentenceSplitMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Pattern SENTENCE_SPLIT_PATTERN =
                Pattern.compile("(?<![\\da-zA-Z])(。|！|!|\\.(?<!\\d)|？|\\?|：|:|;|；|,|，)(?<![\\da-zA-Z])");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // 假设输入格式为：text_md5\ttext_content
            String[] parts = line.split("\t", 4);
            if (parts.length < 4) return;

            String textMd5 = parts[1];
            String text = parts[2].replace("\t", "").replace("\n", "");
            String loadeTime = parts[3];
            String dt = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            System.out.println(text);

            // 分割句子并保留标点符号
            List<String> processedSentences = new ArrayList<>();
            Matcher matcher = SENTENCE_SPLIT_PATTERN.matcher(text);
            int lastEnd = 0;
            int sentenceIndex = 0;

            while (matcher.find()) {
                // 获取匹配的标点符号
                String punctuation = matcher.group();
                // 获取标点符号前的句子内容
                String sentence = text.substring(lastEnd, matcher.start()).trim();
                if (!sentence.isEmpty()) {
                    // 添加标点符号到句子末尾
                    sentence += punctuation;
                    System.out.println("Sentence " + sentenceIndex + ": " + sentence);
                    processedSentences.add(sentence);
                    sentenceIndex++;
                }
                lastEnd = matcher.end();
            }

            // 处理最后一个句子（如果存在且不以标点结尾）
            if (lastEnd < text.length()) {
                String lastSentence = text.substring(lastEnd).trim();
                if (!lastSentence.isEmpty()) {
                    System.out.println("Sentence " + sentenceIndex + ": " + lastSentence);
                    processedSentences.add(lastSentence + " "); // 添加空格作为结尾标点
                }
            }
            System.out.println("Processed Sentences: " + processedSentences);
            // 这里不进行合并，保持每个句子独立
            for (int i = 0; i < processedSentences.size(); i++) {

                String sent = processedSentences.get(i);


                // 生成句子MD5
                String sentMd5 = DocumentTools.getTextMd5(sent);

                // 输出：text_md5作为key，句子信息作为value
                // 格式：sent_no\tsent_text\tsent_md5\tsent_split_param
                String sentInfo = String.format("%d\t%s\t%s\t%s\t%s\t%s",
                        i + 1, sent, sentMd5,
                        sent.isEmpty() ? " " : sent.substring(sent.length() - 1), loadeTime, dt);

                context.write(new Text(textMd5), new Text(sentInfo));
            }
        }
    }

    // Reducer类：简单地将mapper输出写入文件
    public static class SentenceSplitReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                // 添加text_md5到每行开头
                String output = key.toString() + "\t" + value.toString();
                System.out.println("Output: " + output);
                context.write(NullWritable.get(), new Text(output));
//                System.out.println(key.toString());
//                System.out.println(NullWritable.get());
//                System.out.println(output);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }

//        System.out.println("Hadoop home: " + System.getProperty("hadoop.home.dir"));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sentence Split");

        job.setJarByClass(SentenceSplitMR.class);
        job.setMapperClass(SentenceSplitMapper.class);
        job.setReducerClass(SentenceSplitReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 输出路径
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);

        // 如果输出路径已存在，则删除
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true 表示递归删除
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}