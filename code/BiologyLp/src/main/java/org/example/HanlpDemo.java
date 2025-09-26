package org.example;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import util.HdfsUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class HanlpDemo {

    // 中文分词测试
    public static void main(String[] args) throws IOException {

        FileSystem fs = HdfsUtils.getFileSystem();

        String filePath = "/user/hive/warehouse/ads.db/dim_user_info_di/dt=20250726/part-m-00000";
        Path path = new Path(filePath);

        if (!fs.exists(path)) {
            System.out.println("文件不存在: " + filePath);
            return;
        }

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(path), "UTF-8"));

        String line;
        String text = "";
        int count = 0;
        System.out.println("读取文件 " + filePath + " 的内容:");
        while ((line = reader.readLine()) != null && count < 1) {
//            System.out.println(line);


//        String text = "HanLP 1.x 提供了便携版的Java API！";
            text = line;
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
            count++;
        }

        IOUtils.closeStream(reader);
    }
}