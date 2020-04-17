package com.hadoop.yi.mr.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 简单的etl处理，只需要在 Mapper 程序进行处理
 * 需求：过滤每行长度为小于11的数据
 */
public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取一行数据
        String line = value.toString();
        // 解析日志
        boolean result = parseLog(line,context);
        // 日志不合法退出
        if (!result){
            return;
        }
        // 设置k
        k.set(line);
        // 写出
        context.write(k,NullWritable.get());
    }

    /*
     * 解析日志
     * */
    private boolean parseLog(String line, Context context) {

        // 截取
        String[] fields = line.split(" ");
        // 日志长度大于11 为合法数据
        if (fields.length > 11){
            // 系统计数器
            context.getCounter("map","true").increment(1);
            return true;
        }else {
            context.getCounter("map","false").increment(1);
            return false;
        }
    }
}
