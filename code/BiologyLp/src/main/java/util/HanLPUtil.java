package util;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.common.Term;
import config.AppConfig;

import java.io.*;
import java.util.List;

public class HanLPUtil {

    static {
        // 加载自定义词典
        loadCustomDict();
        // 加载停用词
        loadStopWords();
    }

    private static void loadCustomDict() {
        File dictDir = new File(AppConfig.DICT_PATH);
        if (dictDir.exists() && dictDir.isDirectory()) {
            File[] dictFiles = dictDir.listFiles();
            if (dictFiles != null) {
                for (File file : dictFiles) {
                    if (file.getName().endsWith(".txt")) {
                        CustomDictionary.add(file.getAbsolutePath());
                    }
                }
            }
        }
    }

    private static void loadStopWords() {
        // 优先从类路径读取，其次回退到文件系统路径
        InputStream is = null;
        try {
            ClassLoader cl = HanLPUtil.class.getClassLoader();
            // 优先尝试配置的原始路径
            is = cl.getResourceAsStream(AppConfig.STOPWORDS_PATH);
            if (is == null) {
                // 再尝试仅文件名
                String fileName = new File(AppConfig.STOPWORDS_PATH).getName();
                is = cl.getResourceAsStream(fileName);
            }

            if (is == null) {
                // 回退到文件系统路径
                File file = new File(AppConfig.STOPWORDS_PATH);
                if (file.exists()) {
                    is = new FileInputStream(file);
                }
            }

            if (is == null) {
                System.err.println("Warning: stopwords file not found: " + AppConfig.STOPWORDS_PATH);
                return;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    CoreStopWordDictionary.add(line.trim());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<Term> segment(String text) {
        return HanLP.segment(text);
    }


    public static void main(String[] args) {
        String testText = "患者，女，25岁，主诉头痛3天，加重1天。";
        List<Term> terms = segment(testText);
        for (Term term : terms) {
            System.out.println(term.word + " : " + term.nature);
        }
        loadCustomDict();

    }
}
