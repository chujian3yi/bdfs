package com.hadoop.yi.mr.sdof;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer
 * <line,~>,直接输出，输出格式由自定义格式处理
 */
public class FilterReducer extends Reducer<Text, NullWritable,Text,NullWritable> {

    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = key.toString();
        // 拼接,换行
        line = line + "\r\n";
        // 设置k
        k.set(line);
        // 写出
        context.write(k,NullWritable.get());
    }
}
