package com.virtusai.clickhouseclient.produer;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;

import static com.virtusai.clickhouseclient.produer.ProduerData.generateRow;
import static com.virtusai.clickhouseclient.utils.DataUtils.getRandomIp;
import static com.virtusai.clickhouseclient.utils.DataUtils.nextTime;

public class WriteOneMiliion {

    public static void main(String[] args) {
        File file = new File("oneMillion.data");

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

        //创建文件
        try{
            if(!file.exists()){
                file.createNewFile();
            }
        }catch(Exception e){
            e.printStackTrace();
        }




        try {
            //构造函数中的第二个参数true表示以追加形式写文件
            FileWriter fw = new FileWriter(file,true);

            for (int i = 0; i <10000000 ; i++) {

                String data = generateData(i, fieldsType, fieldNumber, partner);
                fw.write(data);
                if(i%1000==0){
                    fw.flush();
                }
            }
            fw.close();
        } catch (IOException e) {
            System.out.println("文件写入失败！" + e);
        }

        System.out.println(new Random().nextInt(12));
    }

    public static String generateData(int id,String fieldsType, int fieldNumber,String[] partner) {

        StringBuilder data = new StringBuilder();
        data.append (id+",");
        data.append(partner[new Random().nextInt(8)]+",");
        data.append( new SimpleDateFormat("yyyy-MM-dd").format(nextTime())+",");
        data.append( getRandomIp()+",");
        String[] fieldsTypeList = fieldsType.split(",");
        for (int i = 4; i < fieldNumber-2; i++) {

            switch (fieldsTypeList[i - 4].trim()) {
                case "String":
                    data.append(RandomStringUtils.randomAlphanumeric(new Random().nextInt(8)+1)+",");
                    break;
                case "Int":
                    data.append( new Double(Math.random() * 10000).intValue()+",");
                    break;
                case "Long":
                    data.append(new Double(Math.random() * 1000000).longValue()+",");
                    break;
                default:
                    data.append(1+",");
                    break;
            }
        }
        data.append( (Math.random() * 100)+",");
        data.append( (Math.random() * 100)+"\n");
        return data.toString();
    }
}
