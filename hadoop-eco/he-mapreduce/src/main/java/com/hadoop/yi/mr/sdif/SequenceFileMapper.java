package com.hadoop.yi.mr.sdif;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map 处理流程：输入由 Driver 指定自定义 InputFormat；直接输出key value。
 */
public class SequenceFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
