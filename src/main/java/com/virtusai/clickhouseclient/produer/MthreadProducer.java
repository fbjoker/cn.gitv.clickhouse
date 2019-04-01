package com.virtusai.clickhouseclient.produer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MthreadProducer {
    public static void main(String[] args) {


        MyckproducerRunnable p1= new MyckproducerRunnable();
        p1.setRowNum(100000);
        p1.setTableName("test.vodplay");
//        Thread thread1 = new Thread(p1);
//        thread1.start();
        int iter = Integer.parseInt(args[0]);

        for (int j = 0; j < iter; j++) {


        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(11);
        for (int i = 0; i < 10; i++) {
            fixedThreadPool.execute(p1);
        }
        //一定要关闭
        fixedThreadPool.shutdown();

        }

    }
}
