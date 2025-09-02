import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;


/**
 * @BelongProject: test01
 * @BelongPackage: PACKAGE_NAME
 * @ClassName Test
 * @Author: lichaozhong
 * @CreateTime: 2024-05-10  17:38
 */
public class Test {
    public static List<Integer> list;
    public static int var;

    private List<Integer> discountTypeList;
    int[] array;


    public static List<Integer> discountTypeListTemp(List<Integer> discountTypeList) {
        List<Integer> list = new ArrayList<>();
        HashSet<Integer> discountTypeSet = new HashSet<>(discountTypeList);

        for (int i = 1; i <= 6; i++) {
            int e = discountTypeList.contains(i) ? 1 : 0;
            list.add(e);
        }
        System.out.println("discountTypeSet=" + discountTypeSet);
        System.out.println("list=" + list);
        return list;
    }

    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(1, 2, 2, 3, 5);
        discountTypeListTemp(list);
        List<Integer> integers = discountTypeListTemp(list);
        System.out.println("integers=" + integers);
        int[] atest;
        atest = null;
        System.out.println(atest);
        System.out.println(integers.getClass());

    }

}

