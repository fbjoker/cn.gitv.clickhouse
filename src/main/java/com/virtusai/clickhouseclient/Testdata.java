package com.virtusai.clickhouseclient;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.sql.SQLOutput;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.virtusai.clickhouseclient.produer.ProduerData.generateRow;
import static com.virtusai.clickhouseclient.utils.DataUtils.getRandomIp;
import static com.virtusai.clickhouseclient.utils.DataUtils.getString;
import static com.virtusai.clickhouseclient.utils.DataUtils.nextTime;

public class Testdata {

    public static void main(String[] args) {
        int fieldNumber = 420;

        String fieldsType = "String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Long, Long, Long, Long, " +
                "Long, Double, Double";
        int length = fieldsType.split(",").length;
        System.out.println(length);

//        Object[] data = generateRow(fieldsType, fieldNumber);
//        System.out.println(data.length);
        long start = System.currentTimeMillis();
        String randomIp = getRandomIp();
        long end = System.currentTimeMillis();
        System.out.println(randomIp);
        System.out.println(end-start);

        String ttt = RandomStringUtils.randomAlphanumeric(new Random().nextInt(100));
        System.out.println(ttt);
        System.out.println(new Random().nextInt(2147483647));
        System.out.println(Math.random()*1000000);
        System.out.println(new Random().nextDouble());
        System.out.println(new RandomDataGenerator().nextLong(0, Long.MAX_VALUE));
        System.out.println(new RandomDataGenerator().nextInt(0, Integer.MAX_VALUE));
        System.out.println(RandomUtils.nextDouble(0, 100000000));
        System.out.println(nextTime());
        long start1 = System.currentTimeMillis();

        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(nextTime()));
        long end1= System.currentTimeMillis();
        System.out.println(end1-start1);
        System.out.println(Math.random()*100);
        System.out.println(getString());
        System.out.println(getString());
        long test1 = new Double( Math.random() * 1000000).longValue();
        String string = getString();
        int testi = new Double( Math.random() * 10000).intValue();
        double testd = Math.random() * 100;

        System.out.println(test1);
        System.out.println(testi);
        List<Object[]> rows = new ArrayList<>();
        Object[] data=new Object[2];
        data[1]=2;
        rows.add(data);
        data[1]=5;
        rows.add(data);
        for (int i = 0; i < rows.size(); i++) {
            Object[] objectsdata = rows.get(i);
            for (int i1 = 0; i1 < objectsdata.length; i1++) {
                System.out.println(objectsdata[i1]);


            }
        }



    }





}
