package com.virtusai.clickhouseclient.utils;

import java.util.concurrent.ThreadLocalRandom;

public class RandomTest {
    public static void main(String[] args) {


        long start = System.currentTimeMillis();
        for (int i = 0; i <1000000 ; i++) {



        int intv = new Double(Math.random() * 10000).intValue();
        long longv = new Double(Math.random() * 1000000).longValue();
        double doublev = Math.random() * 100;
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start);

        long start1 = System.currentTimeMillis();
        for (int i = 0; i <10000 ; i++) {



            int intv = ThreadLocalRandom.current().nextInt(10000);
            long longv = ThreadLocalRandom.current().nextLong(1000000);
            double doublev = ThreadLocalRandom.current().nextDouble(500);
//            System.out.println(intv);
//            System.out.println(longv);
//            System.out.println(doublev);
//            System.out.println("===============");
        }
        long end1 = System.currentTimeMillis();
        System.out.println(end1-start1);



    }
}
