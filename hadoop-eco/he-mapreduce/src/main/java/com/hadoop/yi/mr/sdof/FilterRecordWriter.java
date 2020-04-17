package com.hadoop.yi.mr.sdof;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * 自定义 RecordWriter ，用于输出不同分区的数据路径
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream filters = null;
    FSDataOutputStream fothers = null;

    public FilterRecordWriter(TaskAttemptContext job){
        // 获取文件系统
        FileSystem fs;
        try {
            fs = FileSystem.get(job.getConfiguration());
            //创建输出文件路径
            Path filterPath = new Path("E:/tmp/outputlog/1/filter.log");
            Path otherPath = new Path("E:/tmp/outputlog/1/other.log");
            // 创建输出流
            filters = fs.create(filterPath);
            fothers = fs.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 重写 writer，控制最终输出文件的输出路径和输出格式
     * @param key
     * @param nullWritable
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
            // 判断是否符合规则
        if (key.toString().contains("yi")){
            filters.write(key.toString().getBytes());
        }else {
            fothers.write(key.toString().getBytes());
        }
    }

    /**
     * 关闭io资源
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        filters.close();
        fothers.close();
    }
}
