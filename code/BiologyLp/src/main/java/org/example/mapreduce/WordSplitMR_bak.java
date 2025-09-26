package org.example.mapreduce;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordSplitMR_bak {

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
                if (parts.length < 5) {
                    System.err.println("Invalid input line: " + line);
                    return;
                }
                System.out.println(line);
                System.out.println(parts.length);
                String textMd5 = parts[0];
                String sentNo = parts[1];
                String sentText = parts[2];
                String sentMd5 = parts[3];
                String sentSplitParam = parts[4];
                System.out.println(sentText.length());
                if (sentText.isEmpty()) return;
                System.out.println("Processing sentence: " + sentText);
                // 处理短文本情况（原Python逻辑）
                if (sentText.length() <= 6) {
                    String wordInfo = String.format("%s\t%s\t%s\t%s\t%d\t%s\t%s",
                            sentNo, sentText, sentMd5, sentSplitParam, 0, sentText, sentSplitParam);
                    System.out.println(wordInfo);
                    context.write(new Text(sentMd5), new Text(wordInfo));
                } else {
                    try {
                        // 使用简单分词方法
                        List<SimpleWord> wordList = simpleSegment(sentText);

                        // 输出分词结果
                        for (int i = 0; i < wordList.size(); i++) {
                            SimpleWord word = wordList.get(i);
                            // 格式：word_no\tword_text\tpart_of_speech\tsent_md5
                            String wordInfo = String.format("%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s",
                                    sentNo, sentText, sentMd5, sentSplitParam, i, word.word, word.nature, sentMd5);
                            System.out.println(wordInfo);
                            context.write(new Text(sentMd5), new Text(wordInfo));
                        }
                    } catch (Exception e) {
                        System.err.println("Error in word segmentation: " + e.getMessage());
                        e.printStackTrace();
                        // 如果分词失败，将整个句子作为一个词处理
                        String wordInfo = String.format("%s\t%s\t%s\t%s\t%d\t%s\t%s",
                                sentNo, sentText, sentMd5, sentSplitParam, 0, sentText, "unknown");
                        System.out.println(wordInfo);
                        context.write(new Text(sentMd5), new Text(wordInfo));
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in map method: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    // Reducer类：简单地将mapper输出写入文件
    public static class WordSplitReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                // 添加text_md5到每行开头（需要从上下文中获取，这里简化处理）
                // 实际实现中可能需要额外的join操作来获取text_md5
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Split");

        job.setJarByClass(WordSplitMR_bak.class);
        job.setMapperClass(WordSplitMapper.class);
        job.setReducerClass(WordSplitReducer.class);

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