package com.hadoop.yi.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第二次 reduce 处理
 *                    k    v
 *  1122 a.txt 3      1122  a.txt-->3 b.txt-->2 c.txt-->1
 *  1122 b.txt 2
 *  1122 c.txt 1
 */
public class SecIndexReducer extends Reducer<Text,Text,Text,Text> {

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            sb.append(value.toString().replace("\t","-->"));
        }
        v.set(sb.toString());
        context.write(key,v);
    }
}
