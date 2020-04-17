package com.hadoop.yi.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第一次 reduce 处理
 * 1122--a.txt   3
 * 1122--b.txt   2
 * 1122--c.txt   1
 */
public class OneIndexReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { super.reduce(key, values, context);
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}

