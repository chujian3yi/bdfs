package com.hadoop.yi.mr.nlif;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * NLineTextInputFormat 做每N行一个切片的单词统计
 */
public class NLineMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    private Text k = new Text();
    private LongWritable v = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 取一行
        String line = value.toString();
        // 切割
        String[] words = line.split(" ");
        // 循环写出
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }
    }
}
