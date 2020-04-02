package com.hadoop.yi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/***
 * hdfs 客户端api的测试
 */
public class HdfsClient {

    private FileSystem fs;

    /**
     * 1.解决hadoop在window上运行的hadoop_home_dir问题
     * 2.初始化一个hdfs文件系统fs
     */
    @Before
    public void initHadoopHomeDir() throws URISyntaxException, IOException, InterruptedException {

        System.setProperty("hadoop.home.dir","E:\\repository\\hadoop-2.6.0-cdh5.14.0");
        //获取文件系统
        Configuration conf = new Configuration();
        // 配置在集群上运行
        //conf.set("fs.defaultFS","hdfs://node01:8020");
        //FileSystem fs = FileSystem.get(conf);
        fs = FileSystem.get(new URI("hdfs://node01:8020"), conf, "root");
    }

    /**
     * 创建目录
     */
    @Test
    public void testMkdirs() throws Exception {
        // 创建目录
        fs.mkdirs(new Path("/1100/3"));
        // 关闭资源
        fs.close();
    }

    /**
     * 文件上传（测试参数优先级）
     */
    @Test
    public void testCopyFromLocalFile() throws IOException {
        //执行上传
        fs.copyFromLocalFile(
                new Path("E:\\yibds\\readme.md")
                ,new Path("/1100/1"));
    }

    /**
     * 文件下载
     *  boolean delSrc 指是否将原文件删除
     * 	Path src 指要下载的文件路径
     * 	Path dst 指将文件下载到的路径
     * 	boolean useRawLocalFileSystem 是否开启文件校验
     */
    @Test
    public void testCopyToLocalFile() throws IOException {
        //执行下载
        fs.copyToLocalFile(false,
                new Path("/1100/1/readme.md"),
                new Path("E:\\yibds\\rd.md"),
                true
        );
    }

    /**
     * 文件夹删除
     */
    @Test
    public void testDeleteDir() throws IOException {
        //执行删除
        fs.delete(new Path("/1100/1/rdme.md"),true);
        // fs.deleteOnExit(new Path("/1"));
    }

    /**
     * 文件名更改
     */
    @Test
    public void testRename() throws IOException {
        //执行更改
        fs.rename(new Path("/1100/1/readme.md"),
                new Path("/1100/1/rd.md"));
    }

    /**
     * 查看文件名称、权限、长度、块信息
     */
    @Test
    public void testListFiles() throws IOException {
        //获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/1100" ), true);
        // 遍历listFiles
        while (listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            //获取存储的块信息

            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("==== over ====");
        }

    }

    /**
     * 文件和文件夹判断
     */
    @Test
    public void testListStatus() throws IOException {
        // 判断是文件还是文件夹
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }
            else{
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
    }

    /**
     * 关闭资源
     */
    @After
    public void close() throws IOException {
        fs.close();
        System.out.println("over");
    }

}
