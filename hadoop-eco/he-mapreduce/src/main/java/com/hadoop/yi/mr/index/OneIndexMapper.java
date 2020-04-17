package com.hadoop.yi.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 第一次 map 处理
 * 1122--a.txt   1 1122--a.txt   1 1122--a.txt   1
 * 1122--b.txt   1 1122--b.txt   1
 * 1122--c.txt   1
 *
 */
public class OneIndexMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    // 文件名
    String name;
    // 索引字段
    Text k = new Text();
    // 词频统计
    IntWritable v = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件名称
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split(" ");
        for (String word : fields) {
            k.set(word+"--"+name);
            v.set(1);
            context.write(k,v);
        }
    }
}
