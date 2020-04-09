package common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SortFunction {
    public static String[] mergeSort(List<String[]> input){

        int length = input.size();
        if(length == 1){
            return input.get(0);
        }
        if(length == 2){
           return mergeTwoList(input.get(0), input.get(1));
        }
        List<String[]> l1 = input.subList(0, length / 2);
        List<String[]> l2 = input.subList(length / 2, input.size());
        String[] res1 = mergeSort(l1);
        String[] res2 = mergeSort(l2);
        return mergeTwoList(res1, res2);
    }

    public static String[] mergeTwoList(String[] l1, String[] l2){
        String[] res = new String[l1.length + l2.length];
        int idx1 = 0, idx2 = 0;
        int idx = 0;
        while (idx1 < l1.length && idx2 < l2.length){
            String s1 = l1[idx1];
            String s2 = l2[idx2];
            if(s1.compareTo(s2) < 0){
                res[idx ++] = l1[idx1++];
            } else {
                res[idx ++] = l2[idx2++];
            }
        }
        while (idx1 < l1.length){
            res[idx ++] = l1[idx1++];
        }

        while (idx2 < l2.length){
            res[idx ++] = l2[idx2++];
        }

        return res;
    }
}
