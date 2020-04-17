package com.hadoop.yi.mr.sdif;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer 处理流程：输入是map输出；输出又 Driver 指定 SequenceFileOutputFormat。
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        // 取每一文件输出一次，遍历之
        context.write(key, values.iterator().next());
    }
}
