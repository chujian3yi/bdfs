package com.hadoop.yi.mr.sdif;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义 InputFormat的 RecordReader
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    // 封装配置对象
    private Configuration conf;
    // 文件切片对象
    private FileSplit split;
    // 读取状态标识
    private boolean isProgress = true;
    private Text k = new Text();
    private BytesWritable value = new BytesWritable();

    /**
     * 初始化，切片、上下文对象
     *
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        this.conf = taskAttemptContext.getConfiguration();
    }

    /**
     * io流一次读取一个文件输出到value中
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (isProgress) {
            //1.定义缓冲区
            byte[] contents = new byte[(int) split.getLength()];
            // 文件系统
            FileSystem fs = null;
            // 文件输入流
            FSDataInputStream fis = null;
            try {
                //2.获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(conf);
                //3.读取数据
                fis = fs.open(path);
                //4.读取文件内容
                IOUtils.readFully(fis, contents, 0, contents.length);
                //5.输出文件内容
                value.set(contents, 0, contents.length);
                //6.获取文件路径及名称
                String name = split.getPath().toString();
                //7.设置输出的key值
                k.set(name);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(fis);
            }
            isProgress = false;
            return false;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
