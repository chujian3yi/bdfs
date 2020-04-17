package com.hadoop.yi.mr.compare;

import com.hadoop.yi.mr.writable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducerForComparable extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 循环输出
        for (Text text : values) {
            // 写出
            context.write(text, key);
        }
    }
}