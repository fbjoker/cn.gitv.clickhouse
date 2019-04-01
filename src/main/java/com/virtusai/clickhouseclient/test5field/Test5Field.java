package com.virtusai.clickhouseclient.test5field;

import com.virtusai.clickhouseclient.ClickHouseClient;
import org.apache.commons.lang3.RandomStringUtils;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static com.virtusai.clickhouseclient.utils.DataUtils.getRandomIp;
import static com.virtusai.clickhouseclient.utils.DataUtils.nextTime;

public class Test5Field {
    public static void main(String[] args) {

        String[] partner= {"YNYDZX","YNYDHW","JS_CMCC_CP","JS_CMCC_CP_ZX","HNYD","SD_CMCC_JN","LNYD","SAXYD"};
        String table=args[0];
        Long rowNum=Long.parseLong(args[1])*1000000;
        Long iterNum=Long.parseLong(args[2]);
        Long sleep=Long.parseLong(args[3]);

        //生产数据
        for (Long i = 0L; i < iterNum; i++) {
            System.out.println("第"+i+"轮生产");
            try {
                producer(rowNum,table,partner,sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void producer(long item,String table, String[] partner,Long sleep) throws InterruptedException {
        ClickHouseClient client = new ClickHouseClient("http://localhost:8123", "default", "");
//        ClickHouseClient client = new ClickHouseClient("http://10.10.130.6:8123", "default", "");


// Insert data
//        List<Object[]> rows = new ArrayList<>();
        List<Object[]> rows = new LinkedList<>();

        long start = System.currentTimeMillis();
        for (int i=0;i<item;i++){
            rows.add(generateRow(i,partner));

        }
        long end = System.currentTimeMillis();
        System.out.println("生成数据花费时长："+(end-start));
        long startInsertTime = System.currentTimeMillis();
         client.post("INSERT INTO "+table, rows);
        long endInsertTime = System.currentTimeMillis();
        System.out.println("插入时长："+(endInsertTime-startInsertTime));
        Thread.sleep(sleep);
        client.close();
    }

    public static Object[] generateRow(int id,String[] partner) {

        Object[] data=new Object[6];
        data[0]=id;
        data[1]=partner[new Random().nextInt(8)];

        data[2]=ThreadLocalRandom.current().nextInt(10000);
        data[3]=ThreadLocalRandom.current().nextDouble(500);
//        data[4]=RandomStringUtils.randomAlphanumeric(new Random().nextInt(12)+1);
        data[4]=new SimpleDateFormat("yyyy-MM-dd").format(nextTime());
        data[5]=getRandomIp();
        return data;
    }
}
