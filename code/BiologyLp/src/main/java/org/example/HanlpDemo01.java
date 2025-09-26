package org.example;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;

import java.io.IOException;
import java.util.List;

public class HanlpDemo01 {

    // 中文分词测试
    public static void main(String[] args) throws IOException {

        String text = "HanLP 1.x 提供了便携版的Java API！";
        // 基础分词功能（便携版支持）
        List<Term> termList = HanLP.segment(text);
        System.out.println("分词结果: " + termList);

        // 词性标注（便携版支持）
        System.out.println("词性标注: " + termList);

        // 关键词提取（便携版支持）
        System.out.println("关键词提取: " + HanLP.extractKeyword(text, 3));

        // 注意：依赖解析需要完整版模型文件，便携版不支持
        // System.out.println("依赖解析: " + HanLP.parseDependency(text));

        // 其他便携版支持的功能
        System.out.println("文本摘要: " + HanLP.extractSummary(text, 2));
    }
}