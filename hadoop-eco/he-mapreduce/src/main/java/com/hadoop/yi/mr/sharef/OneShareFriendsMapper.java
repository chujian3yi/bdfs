package com.hadoop.yi.mr.sharef;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输出《朋友，人》
 */
public class OneShareFriendsMapper extends Mapper<LongWritable, Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split(":");
        String person = split[0];
        String[] friends = split[1].split(",");

        for (String friend : friends) {
            //<好友，人>
            context.write(new Text(friend),new Text(person));
        }
    }
}
