package misc;

import org.junit.Test;

import java.util.*;

public class Other {


    @Test
    public void keyGroupHashing_test() {

        List<String> keyGroup = new ArrayList<>(Arrays.asList("99", "98", "66", "67", "0", "2", "64", "33", "1", "4",
                "71", "73", "5", "72", "68", "65", "3", "38", "34", "37", "69", "36", "35", "40", "39", "70"));


        HashMap<Integer, Integer> table1 = new HashMap<>();
        HashMap<Integer, Integer> table2 = new HashMap<>();

        int count1 = 0;
        int count2 = 0;
        for(int j = 2; j < 21; j++) {


            for (String key : keyGroup) {
                Integer k = Integer.parseInt(key) % j;
                table1.put(k, table1.getOrDefault(k, 0) + 1);
            }
            for (String key : keyGroup) {
                Integer k = key.hashCode() % j;
                table2.put(k, table2.getOrDefault(k, 0) + 1);
            }

            if (j != table1.size()) count1++;
            if (j != table2.size()) count2++;

            table1.clear();
            table2.clear();
        }
        System.out.println("IntHash: "+count1+", hashCode: "+count2);
    }
}
