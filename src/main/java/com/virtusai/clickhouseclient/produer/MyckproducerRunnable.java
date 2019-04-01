package com.virtusai.clickhouseclient.produer;


import com.virtusai.clickhouseclient.ClickHouseClient;

import java.util.ArrayList;
import java.util.List;

import static com.virtusai.clickhouseclient.produer.ProduerData.generateRow;

public class MyckproducerRunnable implements Runnable {

    private String tableName;
    private int rowNum;
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }
    public void setRowNum(int rowNum)
    {
        this.rowNum = rowNum;
    }


    @Override
     public void run() {
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
        ClickHouseClient client = new ClickHouseClient("http://localhost:8123", "default", "");

        String table=tableName;
//        Long item=Long.parseLong(rowNum);
//        System.out.println(item);
// Insert data
        List<Object[]> rows = new ArrayList<Object[]>();

//        rows.add(generateRow(1,fieldsType,fieldNumber,partner));
//        rows.add(generateRow());
        long start = System.currentTimeMillis();
        for (int i=0;i<rowNum;i++){
            long start1 = System.currentTimeMillis();
            rows.add(generateRow(i,fieldsType,fieldNumber,partner));
            long end1 = System.currentTimeMillis();
            // System.out.println("一条生成的时间"+(end1-start1));

        }
        long end = System.currentTimeMillis();
        System.out.println(Thread.currentThread().getName()+"生成数据花费时长："+(end-start));
        System.out.println(rows.size());
        System.out.println(rows.size());
        long startInsertTime = System.currentTimeMillis();
        client.post("INSERT INTO "+table, rows);
        long endInsertTime = System.currentTimeMillis();
        System.out.println(Thread.currentThread().getName()+"插入时长："+(endInsertTime-startInsertTime));
        try {
            Thread.sleep(5000);



        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client.close();
    } }
