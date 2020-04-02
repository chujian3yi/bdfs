package com.hadoop.yi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * hdfs的IO流操作
 */
public class HdfsIO {

    private Configuration conf;
    private FileSystem fs;

    /**
     * 1.设置hadoop_home_dir解决 winutils 的问题
     * 2.初始化一个hdfs文件系统 fs
     */
    @Before
    public void init() throws IOException {
        System.setProperty("hadoop.home.dir", "E:\\repository\\hadoop-2.6.0-cdh5.14.0");
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node01:8020");
        fs = FileSystem.get(conf);
    }

    /**
     * 文件上传（IO流操作）
     */
    @Test
    public void putFileToHdfs() throws IOException {

        // 创建输如流
        FileInputStream fis = new FileInputStream(new File("E:\\yibds\\readme.md"));
        // 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/1100/1/readme.md"));
        // 流对拷
        IOUtils.copyBytes(fis, fos, conf);
        // 关闭流资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    /**
     * 文件下载（IO流操作）
     */
    @Test
    public void getFileToHdfs() throws IOException {

        // 创建输入流
        FSDataInputStream fis = fs.open(new Path("/1100/1/rd.md"));
        // 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("E:\\yibds\\rd.md"));
        // 流对拷
        IOUtils.copyBytes(fis, fos, conf);
        // 关闭流资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    /**
     * 定位文件读取
     * 分块读取hdfs上的大文件，比如根目录下的/tmp/hadoop-2.6.0-cdh5.14.0.tar.gz
     * 下载第一块
     */
    @Test
    public void readFileSeek01() throws IOException {
        //获取输入流
        FSDataInputStream fis = fs.open(new Path("/tmp/hadoop-2.6.0-cdh5.14.0.tar.gz"));
        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File("E:\\tmp\\hadoop-2.6.0-cdh5.14.0.tar.gz.part1"));
        //流对拷
        byte[] buff = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buff);
            fos.write(buff);
        }
        //关闭流资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    /**
     * 定位文件读取
     * 分块读取hdfs上的大文件，比如根目录下的/tmp/hadoop-2.6.0-cdh5.14.0.tar.gz
     * 下载第二块
     */
    @Test
    public void readFileSeek02() throws IOException {
        //获取输入流
        FSDataInputStream fis = fs.open(new Path("/tmp/hadoop-2.6.0-cdh5.14.0.tar.gz"));

        //定位输入数据的位置
        fis.seek(1024 * 1024 * 128);
        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File("E:\\tmp\\hadoop-2.6.0-cdh5.14.0.tar.gz.part2"));
        //流对拷
        IOUtils.copyBytes(fis, fos, conf);
        //关闭流资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    /**
     * 关闭资源
     */
    @After
    public void closeResource() throws IOException {
        fs.close();
    }
}
