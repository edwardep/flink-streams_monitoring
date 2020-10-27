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
        HashMap<Integer, Integer> table3 = new HashMap<>();
        HashMap<Integer, Integer> table4 = new HashMap<>();
        HashMap<Long, Integer> table5 = new HashMap<>();

        int count1 = 0;
        int count2 = 0;
        for(int j = 2; j < 28; j++) {


            for (String key : keyGroup) {
                Integer k = Integer.parseInt(key) % j;
                table1.put(k, table1.getOrDefault(k, 0) + 1);
            }
            for (String key : keyGroup) {
                Integer k = key.hashCode() % j;
                table2.put(k, table2.getOrDefault(k, 0) + 1);
            }
            for(String key : keyGroup) {
                Integer k = String.valueOf(key.hashCode()).hashCode() % j;
                table3.put(k, table3.getOrDefault(k, 0) + 1);
            }
            for(String key : keyGroup) {
                long u = UUID.nameUUIDFromBytes(key.getBytes()).getMostSignificantBits();
                int i = ((int) u > 0) ? (int) u : (int) - u;
                Integer k = i % j;
                table4.put(k, table4.getOrDefault(k, 0) + 1);
            }
            for(String key : keyGroup) {
                Long k = hash(key) % j;
                table5.put(k, table5.getOrDefault(k, 0) + 1);
            }

            System.out.println("For "+j+" keys");
            System.out.println("Key % k:             "+table1.size()+"/"+j);
            System.out.println("hashCode(key) % k:   "+table2.size()+"/"+j);
            System.out.println("hash(hash(key)) % k: "+table3.size()+"/"+j);
            System.out.println("UUID.getBytes() % k: "+table4.size()+"/"+j);
            System.out.println("hash(k) % k:         "+table5.size()+"/"+j);

            if (j != table1.size()) count1++;
            if (j != table2.size()) count2++;

            table1.clear();
            table2.clear();
            table3.clear();
            table4.clear();
            table5.clear();
        }
        System.out.println("IntHash: "+count1+", hashCode: "+count2);
    }

    @Test
    public void hashcode_test() {
        List<String> keyGroup = new ArrayList<>(Arrays.asList("99", "98", "66", "67", "0", "2", "64", "33", "1", "4",
                "71", "73", "5", "72", "68", "65", "3", "38", "34", "37", "69", "36", "35", "40", "39", "70"));

        for(String key : keyGroup){
            assert key.hashCode() == hash1(key);
            System.out.println(hash1(key)+" : "+hash2(key));
        }
    }

    public static long hash(String s) {
        return 2^32 * hash1(s) + hash2(s);
    }

    public static int hash1(String s) {
        int var1 = 0;
        char[] var2 = s.toCharArray();
        for (char c : var2) var1 = 31 * var1 + c;
        return var1;
    }
    public static int hash2(String s) {
        int var1 = 0;
        char[] var2 = s.toCharArray();
        for (char c : var2) var1 = 46 * var1 + c;
        return var1;
    }
}
