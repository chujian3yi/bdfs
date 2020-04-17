package com.hadoop.yi.mr.sdif;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * 自定义 InputFormat，定义 WholeFileInputFormat 继承 FileInputFormat
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    /**
     * 分片规则
     *
     * @param job
     * @return
     * @throws IOException
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        return super.getSplits(job);
    }

    /**
     * 设置不可切片
     *
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * 改写RecordReader，实现一次读取一个完整文件封装为kv
     *
     * @param inputSplit
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit
            , TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }

}
