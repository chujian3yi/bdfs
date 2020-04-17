package com.hadoop.yi.mr.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogBeanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取一行
        String line = value.toString();
        // 解析日志是否合法
        LogBean bean = parseLog(line);
        // 不合法数据剔除
        if (!bean.isValid()) {
            return;
        }
        // 设置k
        k.set(bean.toString());
        // 写出
        context.write(k, NullWritable.get());
    }

    private LogBean parseLog(String line) {

        LogBean bean = new LogBean();
        // 截取
        String[] fields = line.split(" ");
        if (fields.length > 11) {
            // 封装数据
            bean.setRemote_addr(fields[0]);
            bean.setRemote_user(fields[1]);
            bean.setTime_local(fields[3].substring(1));
            bean.setRequest(fields[6]);
            bean.setStatus(fields[8]);
            bean.setBody_bytes_sent(fields[9]);
            bean.setHttp_referer(fields[10]);
            if (fields.length > 12) {
                bean.setHttp_user_agent(fields[11] + " " + fields[12]);
            } else {
                bean.setHttp_user_agent(fields[11]);
            }
            // 大于400 ，http错误
            if (Integer.parseInt(bean.getStatus()) >= 400) {
                bean.setValid(false);
            }
        }
        return bean;
    }
}