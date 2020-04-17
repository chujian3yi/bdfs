package com.hadoop.yi.mr.flow;

import com.hadoop.yi.mr.writable.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    FlowBean v = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //取出一行，切割
        String line = value.toString();
        String[] fields = line.split("\t");
        //封装到 FlowBean
        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        //set<k,v>
        k.set(phoneNum);
        v.setDownFlow(downFlow);
        v.setUpFlow(upFlow);
        //写出
        context.write(k,v);
    }
}
