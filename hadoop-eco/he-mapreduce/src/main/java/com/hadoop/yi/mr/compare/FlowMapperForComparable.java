package com.hadoop.yi.mr.compare;

import com.hadoop.yi.mr.writable.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapperForComparable extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean bean = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取一行
        String line = value.toString();
        // 切割
        String[] fields = line.split("\t");
        // 封装对象
        // 取出手机号
        String phoneNum = fields[1];
        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[3]);
        long downFlow = Long.parseLong(fields[4]);
        // set <K,V>
        v.set(phoneNum);
        bean.set(upFlow,downFlow);
        // 写出
        context.write(bean, v);
    }
}
