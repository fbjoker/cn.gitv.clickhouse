package com.virtusai.clickhouseclient.produer;

import com.virtusai.clickhouseclient.ClickHouseClient;
import org.apache.commons.lang3.RandomStringUtils;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static com.virtusai.clickhouseclient.utils.DataUtils.getRandomIp;
import static com.virtusai.clickhouseclient.utils.DataUtils.nextTime;

public class MthreadProduerDataFast {
    public static void main(String[] args) throws InterruptedException {


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
        String[] partner= {"YNYDZX","YNYDHW","JS_CMCC_CP","JS_CMCC_CP_ZX","HNYD","SD_CMCC_JN","LNYD","SAXYD"};
        // Initialize client (endpoint, username, password)

        String table=args[0];
        Long iterNum=Long.parseLong(args[1]);

        //生产数据
        for (Long i = 0L; i < iterNum; i++) {
            System.out.println("第"+i+"轮生产");
            producer(500000,table, fieldNumber, fieldsType, partner);
        }
    }

    private static void producer(long item,String table, int fieldNumber, String fieldsType, String[] partner) throws InterruptedException {
        ClickHouseClient client = new ClickHouseClient("http://localhost:8123", "default", "");
//        ClickHouseClient client = new ClickHouseClient("http://10.10.121.213:8123", "default", "");


// Insert data
//        List<Object[]> rows = new ArrayList<>();
        List<Object[]> rows = new LinkedList<>();

//        rows.add(generateRow(1,fieldsType,fieldNumber,partner));
//        rows.add(generateRow());
        long start = System.currentTimeMillis();
        for (int i=0;i<item;i++){
            long start1 = System.currentTimeMillis();
            rows.add(generateRow(i,fieldsType,fieldNumber,partner));
            long end1 = System.currentTimeMillis();
           // System.out.println("一条生成的时间"+(end1-start1));

        }
        long end = System.currentTimeMillis();
        System.out.println("生成数据花费时长："+(end-start));
        System.out.println(rows.size());

        ExecutorService fixedThreadPool = Executors.newCachedThreadPool();
        fixedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                client.post("INSERT INTO "+table, rows);
            }
        });
        fixedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                client.post("INSERT INTO "+table, rows);
            }
        });
        fixedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                client.post("INSERT INTO "+table, rows);
            }
        });
        fixedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                client.post("INSERT INTO "+table, rows);
            }
        });
        long startInsertTime = System.currentTimeMillis();

        client.post("INSERT INTO "+table, rows);
        long endInsertTime = System.currentTimeMillis();
        System.out.println("单条插入时长："+(endInsertTime-startInsertTime));

        Thread.sleep(30000);
        client.close();
    }

    public static Object[] generateRow(int id,String fieldsType, int fieldNumber,String[] partner) {

         Object[] data=new Object[fieldNumber];
        data[0]=id;
        data[1]=partner[new Random().nextInt(8)];
        data[2]=new SimpleDateFormat("yyyy-MM-dd ").format(nextTime());
        data[3]=getRandomIp();
        String[] fieldsTypeList = fieldsType.split(",");
        for(int i=4;i<fieldNumber;i++){

            switch(fieldsTypeList[i-4].trim()){
//                case "String":data[i]="abcdefg";
                case "String":data[i]=RandomStringUtils.randomAlphanumeric(new Random().nextInt(8)+1);
                    break;
                case "Int":data[i]= ThreadLocalRandom.current().nextInt(10000);
                    break;
                case "Long":data[i]=ThreadLocalRandom.current().nextLong(1000000);
                    break;
                case "Double":data[i]= ThreadLocalRandom.current().nextDouble(500);
                    break;
                default:
                    data[i]=1;
                    break;
            }

//            switch(fieldsTypeList[i-4].trim()){
////                case "String":data[i]="abcdefg";
//                case "String":data[i]=RandomStringUtils.randomAlphanumeric(new Random().nextInt(12)+1);
//                    break;
//                case "Int":data[i]=new Double( Math.random() * 10000).intValue();
//                    break;
//                case "Long":data[i]=new Double( Math.random() * 1000000).longValue();
//                    break;
//                case "Double":data[i]= (Math.random()*100);
//                    break;
//                default:
//                    data[i]=1;
//                    break;
//            }


        }

        return data;
    }
}
