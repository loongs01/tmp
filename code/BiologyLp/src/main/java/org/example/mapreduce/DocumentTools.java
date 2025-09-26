package org.example.mapreduce;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DocumentTools {

    // 生成文本的MD5哈希值
    public static String getTextMd5(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(text.getBytes());
            BigInteger no = new BigInteger(1, messageDigest);
            String hashtext = no.toString(16);
            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext;
            }
            return hashtext;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        String text = "痔疮是怎么回事？向您详细介绍痔疮的病理病因，痔疮主要是由什么原因引起的。痔疮病因主要病因：";
        String md5 = getTextMd5(text);
        System.out.println("Text: " + text);
        System.out.println("MD5: " + md5);
    }
}