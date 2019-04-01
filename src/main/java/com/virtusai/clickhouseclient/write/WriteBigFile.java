package com.virtusai.clickhouseclient.write;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


import static com.virtusai.clickhouseclient.utils.DataUtils.getRandomIp;
import static com.virtusai.clickhouseclient.utils.DataUtils.nextTime;

/**
 * NIO写大文件比较
 * @author Will
 *
 */
public class WriteBigFile {

    // data chunk be written per time
    private static final int DATA_CHUNK = 128 * 1024 * 1024;


    // total data size is 2G
//    private static final long LEN = 1L * 1024 * 1024  * 1024L;
    private static final long LEN = 30L * 1024 * 1024  * 1024L;


    public static void writeWithFileChannel() throws IOException {
        File file = new File("d:/test.dat");
        if (file.exists()) {
            file.delete();
        }

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel fileChannel = raf.getChannel();

        byte[] data = null;
        long len = LEN;
        ByteBuffer buf = ByteBuffer.allocate(DATA_CHUNK);
        int dataChunk = DATA_CHUNK / (1024 * 1024);
        while (len >= DATA_CHUNK) {
            System.out.println("write a data chunk: " + dataChunk + "MB");

            buf.clear(); // clear for re-write
            data = new byte[DATA_CHUNK];
            for (int i = 0; i < DATA_CHUNK; i++) {
                buf.put(data[i]);
            }

            data = null;

            buf.flip(); // switches a Buffer from writing mode to reading mode
            fileChannel.write(buf);
            fileChannel.force(true);

            len -= DATA_CHUNK;
        }

        if (len > 0) {
            System.out.println("write rest data chunk: " + len + "B");
            buf = ByteBuffer.allocateDirect((int) len);
            data = new byte[(int) len];
            for (int i = 0; i < len; i++) {
                buf.put(data[i]);
            }

            buf.flip(); // switches a Buffer from writing mode to reading mode, position to 0, limit not changed
            fileChannel.write(buf);
            fileChannel.force(true);
            data = null;
        }

        fileChannel.close();
        raf.close();
    }

    /**
     * write big file with MappedByteBuffer
     *
     * @throws IOException
     */
    public static void writeWithMappedByteBuffer() throws IOException {
        File file = new File("d:/test.dat");
        if (file.exists()) {
            file.delete();
        }

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel fileChannel = raf.getChannel();
        long pos = 0;
        MappedByteBuffer mbb = null;
        byte[] data = null;
        long len = LEN;
        int dataChunk = DATA_CHUNK / (1024 * 1024);
        while (len >= DATA_CHUNK) {
            System.out.println("write a data chunk: " + dataChunk + "MB");

            mbb = fileChannel.map(FileChannel.MapMode.READ_WRITE, pos, DATA_CHUNK);
            data = new byte[DATA_CHUNK];
            mbb.put(data);

            data = null;

            len -= DATA_CHUNK;
            pos += DATA_CHUNK;
        }



        data = null;
        //unmap(mbb);   // release MappedByteBuffer
        fileChannel.close();
    }

    public static void writeWithTransferTo() throws IOException {
        File file = new File("d:/test.dat");
        if (file.exists()) {
            file.delete();
        }

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel toFileChannel = raf.getChannel();

        long len = LEN;
        byte[] data = null;
        ByteArrayInputStream bais = null;
        ReadableByteChannel fromByteChannel = null;
        long position = 0;
        int dataChunk = DATA_CHUNK / (1024 * 1024);
        while (len >= DATA_CHUNK) {
            System.out.println("write a data chunk: " + dataChunk + "MB");

            data = new byte[DATA_CHUNK];
            bais = new ByteArrayInputStream(data);
            fromByteChannel = Channels.newChannel(bais);

            long count = DATA_CHUNK;
            toFileChannel.transferFrom(fromByteChannel, position, count);

            data = null;
            position += DATA_CHUNK;
            len -= DATA_CHUNK;
        }

        if (len > 0) {
            System.out.println("write rest data chunk: " + len + "B");

            data = new byte[(int) len];
            bais = new ByteArrayInputStream(data);
            fromByteChannel = Channels.newChannel(bais);

            long count = len;
            toFileChannel.transferFrom(fromByteChannel, position, count);
        }

        data = null;
        toFileChannel.close();
        fromByteChannel.close();
    }

    /**
     * 在MappedByteBuffer释放后再对它进行读操作的话就会引发jvm crash，在并发情况下很容易发生
     * 正在释放时另一个线程正开始读取，于是crash就发生了。所以为了系统稳定性释放前一般需要检
     * 查是否还有线程在读或写
     *
     * @param mappedByteBuffer
     */
    public static void unmap(final MappedByteBuffer mappedByteBuffer) {
        try {
            if (mappedByteBuffer == null) {
                return;
            }

            mappedByteBuffer.force();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                @SuppressWarnings("restriction")
                public Object run() {
                    try {
                        Method getCleanerMethod = mappedByteBuffer.getClass()
                                .getMethod("cleaner", new Class[0]);
                        getCleanerMethod.setAccessible(true);
                        sun.misc.Cleaner cleaner =
                                (sun.misc.Cleaner) getCleanerMethod
                                        .invoke(mappedByteBuffer, new Object[0]);
                        cleaner.clean();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("clean MappedByteBuffer completed");
                    return null;
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * write big file with MappedByteBuffer
     *
     * @throws IOException
     */
    public static void writeBigDataWithMappedByteBuffer(long id,String fieldsType, int fieldNumber,String[] partner,int row,String path) throws IOException {

        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel fileChannel = raf.getChannel();
        long pos = 0;
        MappedByteBuffer mbb = null;
        byte[] data = null;
        long len = LEN*row;



        //一次插入的数据是10W，
        for (int iter = 0; iter <row*10; iter++) {
            StringBuilder stringBuilder = new StringBuilder();
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10000; i++) {

                stringBuilder.append( generateData(id, fieldsType, fieldNumber, partner));
            }
            long end = System.currentTimeMillis();
            System.out.println(end-start);

            data = stringBuilder.toString().getBytes();
            System.out.println("write a data chunk: " + data.length/1024/1024 + "MB");
            mbb = fileChannel.map(FileChannel.MapMode.READ_WRITE, pos, data.length);
//            data = new byte[DATA_CHUNK];
            mbb.put(data);

            len -= data.length;
            pos += data.length;
            data = null;
            //System.out.println("len:"+len);
        }


        data = null;
        //unmap(mbb);   // release MappedByteBuffer
        fileChannel.close();
    }
    public static String generateData(Long id,String fieldsType, int fieldNumber,String[] partner) {

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
                    data.append( ThreadLocalRandom.current().nextInt(10000)+",");
                    break;
                case "Long":
                    data.append(ThreadLocalRandom.current().nextLong(1000000)+",");
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


    public static void main(String[] args) throws IOException {


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
        int row = Integer.parseInt(args[0]);
        String path=args[1];

        long start = System.currentTimeMillis();

        writeBigDataWithMappedByteBuffer(ThreadLocalRandom.current().nextLong(100000000), fieldsType, fieldNumber, partner,row,path);
        long end = System.currentTimeMillis();
        System.out.println(end-start);

    }
}