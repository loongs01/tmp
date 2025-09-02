import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * @BelongProject: Default (Template) Project
 * @BelongPackage:
 * @ClassName ${NAME}
 * @Author: lichaozhong
 * @CreateTime: 2024-05-09  10:36
 *///TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class testMain {
    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome!" + "\n");

        for (int i = 1; i <= 5; i++) {
            //TIP Press <shortcut actionId="Debug"/> to start debugging your code. We have set one <icon src="AllIcons.Debugger.Db_set_breakpoint"/> breakpoint
            // for you, but you can always add more by pressing <shortcut actionId="ToggleLineBreakpoint"/>.
            System.out.println("i = " + i);
        }


        LocalDate yesterday = LocalDate.now().minusDays(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDate = yesterday.format(formatter);
        System.out.println(formattedDate);

        HashSet<String> strings = new HashSet<>();


        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add(2, "c");
        System.out.println(list);
//        验证类静态变量
        System.out.println("验证类静态变量");
        System.out.println("Test.list=" + Test.list);
        System.out.println(Test.var);
        System.out.println("---------");
//        System.out.println("Test.list=" + Test.list.size());

        Test.list = new ArrayList<>();


        int isNewAll = 0;
        int isNew99 = 0;
        int isNew95 = 0;
        int isNew90 = 0;
        int isNewQuasi = 0;
        int isNew80 = 0;
        int isNew70 = 0;
//        if (Test.list.size() > 0) {
        if (Test.list != null) {
            System.out.println("if");
            for (Integer item : Test.list) {
                System.out.println("for");
                if (item == 1) {
                    isNewAll = 1;
                } else if (item == 2) {
                    isNew99 = 1;

                } else if (item == 3) {
                    isNew95 = 1;

                } else if (item == 4) {
                    isNew90 = 1;

                } else if (item == 5) {
                    isNewQuasi = 1;

                } else if (item == 6) {
                    isNew80 = 1;

                } else if (item == 7) {
                    isNew70 = 1;

                } else {
                    System.out.println("for" + Test.list.size());
                }

            }
        }


//        System.out.println(Test.list.size() > 0);
        System.out.println("重新赋值后 Test.list=" + Test.list);
        System.out.println(Test.list.size());

        System.out.println("-------");
        System.out.println("isNewAll=" + isNewAll);
        System.out.println("isNew99=" + isNew99);
//modify
        int[] a;
        a = new int[1];
        int[] b = {1, 2, 3};
        System.out.println(b);
        System.out.println("Main.main");
        System.out.println("args = " + Arrays.toString(args));
        System.out.println("b = " + b);
        System.out.println(Arrays.toString(b));
        int c = 1;

        String[] sensitivewords = {"敏感词", "另一敏感词"};
        String regex = String.join("|", sensitivewords);
        System.out.println(sensitivewords);
        System.out.println(regex);


        String str = "Hello全局参数World!";
        str = str.replaceAll("[\\u4e00-\\u9fa5]", "");
        System.out.println(str);//输出：Hello World
        int[] array = new int[10];
        System.out.println(array);

    }
}


