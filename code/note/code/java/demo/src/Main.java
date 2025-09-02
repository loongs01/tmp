/**
 * @Project:Default (Template) Project
 * @FileName:${NAME}
 * @Author:lichaozhong
 * @Date:2025/1/17 10:39
 *///TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.println("当前使用的们ava版本：" + System.getProperty("java.version"));
        System.out.printf("Hello and welcome!\n");

        for (int i = 1; i <= 5; i++) {
            //TIP Press <shortcut actionId="Debug"/> to start debugging your code. We have set one <icon src="AllIcons.Debugger.Db_set_breakpoint"/> breakpoint
            // for you, but you can always add more by pressing <shortcut actionId="ToggleLineBreakpoint"/>.
            System.out.println("i = " + i);

        }

        int[] twoValues = getTwoValues();
        System.out.println(twoValues.getClass().getName());
        System.out.println(twoValues);


    }

    public static int[] getTwoValues() {
        return new int[]{1, 2};
    }
}