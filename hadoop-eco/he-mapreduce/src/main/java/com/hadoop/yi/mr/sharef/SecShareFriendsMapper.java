package com.hadoop.yi.mr.sharef;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * 输出《人-人，友》，相同人-人的所有好友回到同一个reduce去
 */
public class SecShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 友 人，人，人，人，
        String line = value.toString();
        String[] friend_persons = line.split("\t");
        String friend = friend_persons[0];
        String[] persons = friend_persons[1].split(",");

        Arrays.sort(persons);
        for (int i = 0; i < persons.length; i++) {
            for (int j = i + 1; j < persons.length; j++) {
                //《人-人，友》
                context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
            }
        }

    }
}
