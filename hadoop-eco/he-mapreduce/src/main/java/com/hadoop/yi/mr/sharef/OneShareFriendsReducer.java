package com.hadoop.yi.mr.sharef;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 输出 《朋友 人1，人2....》
 */
public class OneShareFriendsReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();

        for (Text person : values) {
            sb.append(person).append(",");
        }
        context.write(key,new Text(sb.toString()));
    }
}
