package com.hadoop.yi.mr.index;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * * 第一次 map 处理
 * k    v
 * * 1122--a.txt   3  1122 a.txt 3
 * * 1122--b.txt   2  1122 b.txt 2
 * * 1122--c.txt   1  1122 c.txt 1
 */
public class SecIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("--");

        k.set(fields[0]);
        v.set(fields[1]);
        context.write(k, v);
    }
}