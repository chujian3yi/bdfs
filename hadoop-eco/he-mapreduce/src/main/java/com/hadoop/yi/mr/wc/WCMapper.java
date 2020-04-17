package com.hadoop.yi.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * word count item
 */
public class WCMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            k.set(word);
            v.set(1);
            context.write(k,v);
        }
    }
}
