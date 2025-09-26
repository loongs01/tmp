package org.example;

import com.github.jfasttext.JFastText;
import config.AppConfig;
import model.MappingResult;
import util.HdfsUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;

public class FastTextUtil {
    private JFastText jft;
    private List<String> icdXaNameList;
    private List<String> containWordList;
    private List<String> noEffectWordList;

    // 常量定义
    private static final double SIMILARITY_THRESHOLD = 0.7;
    private static final double CONTAIN_SIMILARITY_THRESHOLD = 0.8;
    private static final double DISTANCE_SIMILARITY_THRESHOLD = 0.7;
    private static final int MAX_RESULTS = 1000;

    public FastTextUtil() throws IOException {
        // 初始化FastText模型
        jft = new JFastText();
        String resolvedModelPath = resolveModelPath(AppConfig.FASTTEXT_MODEL_PATH);
        jft.loadModel(resolvedModelPath);

        // 加载ICD11 XA名称列表
        icdXaNameList = loadResourceFile("icd_xa_names.txt");

        // 加载特殊词列表
        containWordList = loadResourceFile("contain_word_list.txt");
        noEffectWordList = loadResourceFile("no_effect_word_list.txt");
    }

    private String resolveModelPath(String configuredPath) throws IOException {
        if (configuredPath == null || configuredPath.trim().isEmpty()) {
            throw new IOException("FASTTEXT_MODEL_PATH is empty");
        }

        // 1) 直接按文件系统路径查找
        java.nio.file.Path fsPath = Paths.get(configuredPath);
        if (Files.exists(fsPath)) {
            return fsPath.toString();
        }

        // 2) 从类路径提取到临时文件
        String[] candidates = new String[]{
                configuredPath,
                configuredPath.startsWith("/") ? configuredPath.substring(1) : configuredPath,
                // 常见放置位置
                "model/" + fsPath.getFileName().toString(),
                fsPath.getFileName().toString()
        };

        ClassLoader cl = getClass().getClassLoader();
        for (String candidate : candidates) {
            if (candidate == null || candidate.trim().isEmpty()) continue;
            try (InputStream is = cl.getResourceAsStream(candidate)) {
                if (is != null) {
                    java.nio.file.Path tmp = Files.createTempFile("fasttext-model-", ".bin");
                    Files.copy(is, tmp, StandardCopyOption.REPLACE_EXISTING);
                    tmp.toFile().deleteOnExit();
                    return tmp.toString();
                }
            }
        }

        throw new IOException("FastText model not found at '" + configuredPath + "' or classpath resources");
    }

    private List<String> loadResourceFile(String fileName) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(fileName)) {
            if (is == null) {
                System.err.println("Warning: Resource file " + fileName + " not found, using empty list");
                return new ArrayList<>();
            }
            return new BufferedReader(new InputStreamReader(is))
                    .lines()
                    .collect(Collectors.toList());
        }
    }

    private List<String> loadResourceFileFromPath(String filePath) throws IOException {
        HdfsUtils.readFileLimit("/user/hive/warehouse/ads.db/dim_user_info_di/dt=20250726/part-m-00000", 3);
        return new ArrayList<>();
    }


    public List<MappingResult> findSimilarWords(String srcWord, String destWord) {
        if (srcWord == null || srcWord.trim().isEmpty()) {
            return new ArrayList<>();
        }

        List<MappingResult> results = new ArrayList<>();

        try {
            // 获取源词的向量
            List<Float> srcVector = jft.getVector(srcWord);
            if (srcVector == null) {
                return results;
            }

            // 在ICD11名称列表中搜索相似词
            for (String word : icdXaNameList) {
                if (word == null || word.trim().isEmpty()) continue;

                try {
                    // 获取目标词的向量
                    List<Float> wordVector = jft.getVector(word);
                    if (wordVector == null) continue;

                    // 计算余弦相似度
                    double similarity = calculateCosineSimilarity(srcVector, wordVector);

                    if (similarity <= SIMILARITY_THRESHOLD) continue;

                    MappingResult result = new MappingResult();
                    result.setIcd11Name(destWord);
                    result.setInfoSimilarity(Double.valueOf(similarity));

                    // 计算表层相似度
                    double distanceSimilarity = calculateStringSimilarity(srcWord, word);
                    result.setDistanceSimilarity(Double.valueOf(distanceSimilarity));

                    // 计算包含相似度
                    double containSimilarity = calculateContainScore(srcWord, word);
                    result.setContainSimilarity(Double.valueOf(containSimilarity));

                    result.setMappingWord(word);

                    // 过滤条件
                    if ((distanceSimilarity > DISTANCE_SIMILARITY_THRESHOLD && containSimilarity >= SIMILARITY_THRESHOLD)
                            || containSimilarity >= CONTAIN_SIMILARITY_THRESHOLD) {
                        results.add(result);
                    }
                } catch (Exception e) {
                    // 跳过无法处理的词
                    continue;
                }
            }
        } catch (Exception e) {
            System.err.println("Error finding similar words for: " + srcWord + ", " + e.getMessage());
        }

        // 按相似度排序并限制结果数量
        return results.stream()
                .sorted((a, b) -> Double.compare(b.getInfoSimilarity(), a.getInfoSimilarity()))
                .limit(MAX_RESULTS)
                .collect(Collectors.collectingAndThen(
                        Collectors.toCollection(() -> new TreeSet<>(
                                Comparator.comparing(MappingResult::getMappingWord))),
                        ArrayList::new));
    }

    private double calculateCosineSimilarity(List<Float> vectorA, List<Float> vectorB) {
        if (vectorA == null || vectorB == null || vectorA.size() != vectorB.size()) {
            return 0.0;
        }

        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < vectorA.size(); i++) {
            float a = vectorA.get(i);
            float b = vectorB.get(i);
            dotProduct += a * b;
            normA += a * a;
            normB += b * b;
        }

        if (normA == 0.0 || normB == 0.0) {
            return 0.0;
        }

        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    private double calculateStringSimilarity(String w1, String w2) {
        // 实现类似Python的difflib.SequenceMatcher.quick_ratio()
        int[][] dp = new int[w1.length() + 1][w2.length() + 1];

        for (int i = 0; i <= w1.length(); i++) {
            for (int j = 0; j <= w2.length(); j++) {
                if (i == 0 || j == 0) {
                    dp[i][j] = 0;
                } else if (w1.charAt(i - 1) == w2.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
                }
            }
        }

        int maxLength = Math.max(w1.length(), w2.length());
        return maxLength == 0 ? 0 : (double) dp[w1.length()][w2.length()] / maxLength;
    }

    private double calculateContainScore(String w1, String w2) {
        if (w1 == null || w2 == null) return 0.0;

        // 双向包含检查
        if (w1.contains(w2) || w2.contains(w1)) {
            return 0.8;
        }

        // 如果特殊词列表为空，返回默认值
        if (containWordList == null || containWordList.isEmpty()) {
            return 0.5;
        }

        List<String> w1Matches = containWordList.stream()
                .filter(word -> word != null && w1.contains(word))
                .collect(Collectors.toList());

        List<String> w2Matches = containWordList.stream()
                .filter(word -> word != null && w2.contains(word))
                .collect(Collectors.toList());

        Set<String> intersection = new HashSet<>(w1Matches);
        intersection.retainAll(w2Matches);

        if (!intersection.isEmpty() && !w1Matches.isEmpty() && !w2Matches.isEmpty()) {
            return 0.7;
        } else if (!w2Matches.isEmpty() && intersection.isEmpty() && !w1Matches.isEmpty()) {
            return 0.3;
        } else if (!w1Matches.isEmpty() || !w2Matches.isEmpty()) {
            return 0.5;
        }

        return 0.3;
    }
}