import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class test01 {
    public static void main(String[] args) {
        String filePath = "D:\\code\\java\\demo.txt";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
//                line.replaceAll("[^a-zA-Z0-9\\s]", "？");
                String s = line.replaceAll("[\\u4e00-\\u9fa5]", "");
//                System.out.println("原行：" + s);
                String[] split = s.split(":");


                String[] str = line.split(":");
                if (str.length > 1) {
                    System.out.println("select count(1) from dwd_data." + str[1].replaceAll(" ", "") + ";");
                }
//                System.out.println(str.length);
              /*  if (str.length > 1) {
                    System.out.println(str[1]);
                }*/
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}