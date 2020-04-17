package com.hadoop.yi.mr.sharef;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 输出 《人-人 友 友 友 友》
 */
public class SecShareFriendsReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for (Text friend : values) {
            sb.append(friend).append(" ");
        }
        context.write(key,new Text(sb.toString()));
    }
}