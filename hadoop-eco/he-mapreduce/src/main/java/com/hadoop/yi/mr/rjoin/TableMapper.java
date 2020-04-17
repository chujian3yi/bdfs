package com.hadoop.yi.mr.rjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 为不同来源数据记录，切割字段以及打标签
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    String name;
    TableBean bean = new TableBean();
    Text k = new Text();

    /**
     * 预处理
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取输入的文件切片
        FileSplit split = (FileSplit) context.getInputSplit();
        // 获取输入文件名称
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取输入的数据
        String line = value.toString();
        //不同文件分别处理
        if (name.startsWith("order")) {
            // 订单表处理
            String[] fields = line.split("\t");
            bean.setOrder_id(fields[0]);
            bean.setP_id(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            bean.setPname("");
            bean.setFlag("order");
            k.set(fields[1]);
        } else {
            // 产品表处理
            String[] fields = line.split("\t");
            bean.setP_id(fields[0]);
            bean.setPname(fields[1]);
            bean.setFlag("pd");
            bean.setAmount(0);
            bean.setOrder_id("");
            k.set(fields[0]);
        }
        // 写出
        context.write(k, bean);
    }
}
