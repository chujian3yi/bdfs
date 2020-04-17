package com.hadoop.yi.mr.kvif;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * 使用 KyeValueTextInputFormat 做首单词相同行统计
 */
public class KVTextIFMapper extends Mapper<Text, Text, Text, LongWritable> {

    // 设置value，替代掉KeyValueTextInputFormat 中的value，方便后期统计用
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, v);
    }
}
