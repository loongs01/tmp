package util;

import java.util.List;

public class TextUtils {
    public static double getSimilarWordPubLenScore(String w1, String w2) {
        // 实现类似Python的difflib.SequenceMatcher.quick_ratio()
        // 这里可以使用Java的字符串相似度算法，如Levenshtein距离或Jaro-Winkler距离
        return calculateSimilarity(w1, w2);
    }

    //    计算两个字符串 w1 和 w2 的“包含关系得分”
    public static double getContainScore(String w1, String w2, List<String> icdXaNameList) {
        if (w1.contains(w2)) {
            return 0.8;
        }

        boolean w1HasOrgans = icdXaNameList.stream().anyMatch(org -> w1.toLowerCase().contains(org.toLowerCase()));
        boolean w2HasOrgans = icdXaNameList.stream().anyMatch(org -> w2.toLowerCase().contains(org.toLowerCase()));
//        检查 w1 和 w2 是否共享至少一个共同器官
        boolean hasCommonOrgans = icdXaNameList.stream()
                .anyMatch(org -> w1.toLowerCase().contains(org.toLowerCase()) && w2.toLowerCase().contains(org.toLowerCase()));

        if (hasCommonOrgans && w1HasOrgans && w2HasOrgans) {
            return 0.7;
        } else if (w2HasOrgans && !w1HasOrgans) {
            return 0.3;  // w2有器官但w1没有
        } else if (w1HasOrgans && !w2HasOrgans) {
            return 0.4;  // w1有器官但w2没有
        } else if (w2HasOrgans && !hasCommonOrgans) {
            return 0.2;    // 原有逻辑：w2有器官但无共同器官
        }
        return 0;      // 两者均无器官
    }

    private static double calculateSimilarity(String s1, String s2) {
        // 简单的相似度计算实现
        String longer = s1.length() > s2.length() ? s1 : s2;
        String shorter = s1.length() > s2.length() ? s2 : s1;

        if (longer.length() == 0) {
            return 1.0;
        }

        return (longer.length() - editDistance(longer, shorter)) / (double) longer.length();
    }

    //    计算两个字符串 s1 和 s2 之间的编辑距离
    private static int editDistance(String s1, String s2) {
        if (s1 == null || s2 == null) {
            throw new IllegalArgumentException("Input strings cannot be null");
        }
        s1 = s1.toLowerCase();
        s2 = s2.toLowerCase();

        // 确保s1是较短的字符串，减少空间占用
        if (s1.length() > s2.length()) {
            String temp = s1;
            s1 = s2;
            s2 = temp;
        }

        int[] costs = new int[s1.length() + 1];
        for (int i = 0; i <= s2.length(); i++) {
            int lastValue = i;
            for (int j = 0; j <= s1.length(); j++) {
                if (i == 0) {
                    costs[j] = j;
                } else {
                    if (j > 0) {
                        int newValue = costs[j - 1];
                        if (s2.charAt(i - 1) != s1.charAt(j - 1)) {
                            newValue = Math.min(Math.min(newValue, lastValue), costs[j]) + 1;
                        }
                        costs[j - 1] = lastValue;
                        lastValue = newValue;
                    }
                }
            }
            if (i > 0) {
                costs[s1.length()] = lastValue;
            }
        }
        return costs[s1.length()];
    }
}