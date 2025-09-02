import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

public class hashUdf {

    // 使用SHA-256算法生成哈希值，并将其转换为长整数
    public static long hashString(String input) {
        try {
            // 创建MessageDigest实例，并指定哈希算法（这里使用SHA-256）
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            // 将输入字符串转换为字节数组，并更新MessageDigest
            digest.update(input.getBytes(StandardCharsets.UTF_8));
            // 完成哈希计算，并获取哈希值的字节数组
            byte[] hashBytes = digest.digest();
            // 将哈希值的字节数组转换为正整数（BigInteger），并取其低64位作为哈希值
            BigInteger hashBigInt = new BigInteger(1, hashBytes);
            return hashBigInt.longValue() & 0x00000000FFFFFFFFL; // 取低64位
        } catch (NoSuchAlgorithmException e) {
            // 如果指定的哈希算法不存在，则抛出异常
            throw new RuntimeException("No such hashing algorithm: SHA-256", e);
        }
    }

    public static void main(String[] args) {
        // 测试自定义哈希函数
        String testString = "Hello,World!";
        long hashValue = hashString(testString);
        long l = hashString(testString);
        System.out.println("The hash value of \"" + testString + "\" is: " + hashValue);
    }
}