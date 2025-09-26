package org.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 将原始文本中的词根据映射表替换为 ICD11 编码。
 * 输入：CSV 文本，包含至少 text_md5,text,dt 等字段（第一行为表头）。
 * 参数：
 *   -D mapping.path=/path/to/mapping_word.csv (CSV 头：icd11_name,distance_similarity,contain_similarity,info_similarity,mapping_word)
 *   -D partition.date=yyyy-MM-dd （用于输出路径）
 * 输出：覆盖原文本字段后的 CSV。
 */
public class TextReplaceWithMappingMR {

    public static class ReplaceMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private final List<MappingItem> mappingItems = new ArrayList<>();
        private boolean isHeaderProcessed = false;
        private int textIndex = -1;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String mappingPath = conf.get("mapping.path");
            if (mappingPath == null || mappingPath.isEmpty()) {
                throw new IOException("mapping.path is required");
            }
            loadMapping(mappingPath, conf);
        }

        private void loadMapping(String mappingPath, Configuration conf) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(mappingPath);
            if (!fs.exists(path)) {
                throw new IOException("Mapping file not found: " + mappingPath);
            }
            try (FSDataInputStream in = fs.open(path);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String header = br.readLine();
                // columns: icd11_name,distance_similarity,contain_similarity,info_similarity,mapping_word
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    String[] cols = parseCsvLine(line);
                    if (cols.length < 5) continue;
                    String icd11Name = cols[0];
                    String mappingWord = cols[4];
                    mappingItems.add(new MappingItem(mappingWord, icd11Name));
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!isHeaderProcessed) {
                // first line is header from input CSV
                String[] headers = parseCsvLine(line);
                textIndex = indexOf(headers, "text");
                if (textIndex < 0) {
                    // fallback: try content
                    textIndex = indexOf(headers, "content");
                }
                context.write(NullWritable.get(), new Text(line));
                isHeaderProcessed = true;
                return;
            }

            String[] cols = parseCsvLine(line);
            if (textIndex >= 0 && textIndex < cols.length) {
                String original = cols[textIndex];
                String replaced = replaceByMapping(original, mappingItems);
                cols[textIndex] = replaced;
            }
            context.write(NullWritable.get(), new Text(toCsv(cols)));
        }

        private static int indexOf(String[] arr, String name) {
            for (int i = 0; i < arr.length; i++) {
                if (name.equalsIgnoreCase(stripQuotes(arr[i]))) return i;
            }
            return -1;
        }

        private static String stripQuotes(String s) {
            if (s == null) return null;
            String t = s.trim();
            if (t.startsWith("\"") && t.endsWith("\"")) {
                return t.substring(1, t.length() - 1);
            }
            return t;
        }

        private static String replaceByMapping(String text, List<MappingItem> items) {
            if (text == null || text.isEmpty()) return text;
            String result = text;
            // 为保证确定性，使用插入顺序（可按词长降序以减少嵌套替换问题）
            items.sort((a, b) -> Integer.compare(b.word.length(), a.word.length()));
            for (MappingItem item : items) {
                if (item.word == null || item.word.isEmpty()) continue;
                if (result.contains(item.word)) {
                    result = result.replace(item.word, item.icd11Name);
                }
            }
            return result;
        }

        private static String[] parseCsvLine(String line) {
            // 简单 CSV 解析：支持用双引号包裹且含逗号的字段
            List<String> list = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            boolean inQuotes = false;
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '"') {
                    if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        sb.append('"');
                        i++;
                    } else {
                        inQuotes = !inQuotes;
                    }
                } else if (c == ',' && !inQuotes) {
                    list.add(sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
            list.add(sb.toString());
            return list.toArray(new String[0]);
        }

        private static String toCsv(String[] cols) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < cols.length; i++) {
                if (i > 0) sb.append(',');
                String v = cols[i] == null ? "" : cols[i];
                boolean needQuote = v.contains(",") || v.contains("\"") || v.contains("\n") || v.contains("\r");
                if (needQuote) {
                    sb.append('"').append(v.replace("\"", "\"\"")).append('"');
                } else {
                    sb.append(v);
                }
            }
            return sb.toString();
        }
    }

    public static class PassthroughReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                context.write(NullWritable.get(), v);
            }
        }
    }

    private static class MappingItem {
        String word;
        String icd11Name;

        MappingItem(String word, String icd11Name) {
            this.word = word;
            this.icd11Name = icd11Name;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TextReplaceWithMappingMR <input path> <output path> -D mapping.path=/path/to/mapping.csv");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Text Replace With Mapping");
        job.setJarByClass(TextReplaceWithMappingMR.class);

        job.setMapperClass(ReplaceMapper.class);
        job.setReducerClass(PassthroughReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


