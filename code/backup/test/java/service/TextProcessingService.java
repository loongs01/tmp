package service;

import com.hankcs.hanlp.seg.common.Term;
import config.AppConfig;
import model.MappingResult;
import model.WordInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import util.FastTextUtil;
import util.HanLPUtil;
import util.MySQLUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextProcessingService {
    private MySQLUtil mySQLUtil;
    private FastTextUtil fastTextUtil;
    private FileSystem hadoopFS;
    private Map<String, List<MappingResult>> mappingCache;

    public TextProcessingService() throws Exception {
        this.mySQLUtil = new MySQLUtil();
        this.fastTextUtil = new FastTextUtil();

        // 初始化Hadoop连接
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", AppConfig.HADOOP_HDFS_URI);
        this.hadoopFS = FileSystem.get(conf);

        // 加载映射关系缓存
        this.mappingCache = loadMappingCache();
    }

    //    加载映射缓存
    private Map<String, List<MappingResult>> loadMappingCache() throws SQLException {
        Map<String, List<MappingResult>> cache = new HashMap<>();

        String query = "SELECT icd11_cn_name as src_word, icd11_cn_name as dest_word FROM " + AppConfig.ICD11_CODE_TABLE +
                " UNION SELECT fst_class AS src_word, fst_class AS dest_word FROM " + AppConfig.ICD11_CODE_TABLE +
                " UNION SELECT mapping_word AS src_word, icd11_cn_name AS dest_word FROM " + AppConfig.ICD11_OTH_CODE_TABLE;

        try (Connection conn = mySQLUtil.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                String srcWord = rs.getString("src_word");
                String destWord = rs.getString("dest_word");
                List<MappingResult> results = fastTextUtil.findSimilarWords(srcWord, destWord);
                cache.put(srcWord, results);
            }
        }

        return cache;
    }

    public void processTextData(String inputTable, String outputPath) throws Exception {
        // 1. 从MySQL读取原始文本数据
        List<WordInfo> wordInfos = fetchRawTextData(inputTable);

        // 2. 分词处理
        List<WordInfo> segmentedWords = segmentTexts(wordInfos);

        // 3. 保存分词结果到Hadoop
        saveSegmentedResultsToHadoop(segmentedWords, outputPath + "/segmented");

        // 4. 文本替换为ICD11编码
        List<WordInfo> replacedWords = replaceWithICD11Codes(segmentedWords);

        // 5. 保存最终结果到Hadoop
        saveFinalResultsToHadoop(replacedWords, outputPath + "/final");
    }

    //分句
    private List<WordInfo> fetchRawTextData(String tableName) throws SQLException {
        List<WordInfo> wordInfos = new ArrayList<>();

        String query = "SELECT id as user_id, text, create_time FROM " + tableName;

        try (Connection conn = mySQLUtil.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
//            提取数据并分割句子
            while (rs.next()) {
                String userId = rs.getString("user_id");
                String text = rs.getString("text");
                Timestamp createTime = rs.getTimestamp("create_time");

                String textMd5 = calculateMD5(text);
                String[] sentences = text.split("[。！？；]");

                for (int i = 0; i < sentences.length; i++) {
                    String sentence = sentences[i].trim();
                    if (sentence.isEmpty()) continue;
                    System.out.println("Processing user_id: " + userId + ", sentence: " + sentence);
                    WordInfo info = new WordInfo();
                    info.setTextMd5(textMd5);
                    info.setSentNo(i + 1);
                    info.setSentText(sentence);
                    info.setSentMd5(calculateMD5(sentence));
                    info.setLoadTime(createTime.toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                    wordInfos.add(info);
                }
            }
        }

        return wordInfos;
    }

    //    分词处理
    private List<WordInfo> segmentTexts(List<WordInfo> wordInfos) {
        List<WordInfo> segmentedWords = new ArrayList<>();

        for (WordInfo info : wordInfos) {
            List<Term> terms = HanLPUtil.segment(info.getSentText());

            for (int i = 0; i < terms.size(); i++) {
                Term term = terms.get(i);

                WordInfo wordInfo = new WordInfo();
                wordInfo.setTextMd5(info.getTextMd5());
                wordInfo.setSentNo(info.getSentNo());
                wordInfo.setSentText(info.getSentText());
                wordInfo.setSentMd5(info.getSentMd5());
                wordInfo.setWordNo(i + 1);
                wordInfo.setWordText(term.word);
                wordInfo.setPartOfSpeech(term.nature.toString());
                wordInfo.setLoadTime(info.getLoadTime());

                segmentedWords.add(wordInfo);
            }
        }

        return segmentedWords;
    }

    //    替换为ICD-11编码
    private List<WordInfo> replaceWithICD11Codes(List<WordInfo> segmentedWords) {
        List<WordInfo> replacedWords = new ArrayList<>();

        for (WordInfo info : segmentedWords) {
            String originalWord = info.getWordText();
            String replacedWord = originalWord;

            // 查找最佳映射
            MappingResult bestMapping = findBestMapping(originalWord);
            if (bestMapping != null) {
                replacedWord = bestMapping.getIcd11Name();
            }

            WordInfo newInfo = new WordInfo();
            newInfo.setTextMd5(info.getTextMd5());
            newInfo.setSentNo(info.getSentNo());
            newInfo.setSentText(info.getSentText());
            newInfo.setSentMd5(info.getSentMd5());
            newInfo.setWordNo(info.getWordNo());
            newInfo.setWordText(replacedWord);
            newInfo.setPartOfSpeech(info.getPartOfSpeech());
            newInfo.setLoadTime(info.getLoadTime());

            replacedWords.add(newInfo);
        }

        return replacedWords;
    }

    //    查找最佳映射
    private MappingResult findBestMapping(String word) {
        // 先检查缓存
        List<MappingResult> cachedResults = mappingCache.get(word);
        if (cachedResults != null && !cachedResults.isEmpty()) {
            return cachedResults.get(0); // 返回第一个最佳匹配
        }

        // 如果没有缓存，尝试查找相似词
        try {
            // 这里简化处理，实际应该从数据库获取dest_word
            List<MappingResult> results = fastTextUtil.findSimilarWords(word, word);
            if (!results.isEmpty()) {
                return results.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private void saveSegmentedResultsToHadoop(List<WordInfo> wordInfos, String hdfsPath) throws IOException {
        saveToHadoop(wordInfos, hdfsPath, "segmented_word_info");
    }

    private void saveFinalResultsToHadoop(List<WordInfo> wordInfos, String hdfsPath) throws IOException {
        saveToHadoop(wordInfos, hdfsPath, "final_word_info");
    }

    //    保存到Hadoop
    private void saveToHadoop(List<WordInfo> wordInfos, String hdfsPath, String fileName) throws IOException {
        // 确保目录存在
        Path path = new Path(hdfsPath);
        if (!hadoopFS.exists(path)) {
            hadoopFS.mkdirs(path);
        }

        // 按日期分区
        String datePartition = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        Path fullPath = new Path(hdfsPath + "/" + datePartition + "/" + fileName + ".csv");

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(hadoopFS.create(fullPath), StandardCharsets.UTF_8))) {

            // 写入CSV头部
            writer.write("text_md5,sent_no,sent_text,sent_md5,word_no,word_text,part_of_speech,load_time\n");

            // 写入数据
            for (WordInfo info : wordInfos) {
                writer.write(String.format("%s,%d,\"%s\",%s,%d,%s,%s,%s\n",
                        info.getTextMd5(),
                        info.getSentNo(),
                        info.getSentText().replace("\"", "\"\""),
                        info.getSentMd5(),
                        info.getWordNo(),
                        info.getWordText().replace("\"", "\"\""),
                        info.getPartOfSpeech(),
                        info.getLoadTime()));
            }
        }
    }

    //    计算MD5
    private String calculateMD5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(input.getBytes(StandardCharsets.UTF_8));

            StringBuilder hexString = new StringBuilder();
            for (byte b : messageDigest) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }

            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {
        if (hadoopFS != null) {
            hadoopFS.close();
        }
        if (mySQLUtil != null) {
            mySQLUtil.close();
        }
    }
}