package com.hadoop.yi.mr.ordergc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderSortDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("hadoop.home.dir","E:\\repository\\hadoop-2.6.0-cdh5.14.0");

        //输入输出路径参数
        args = new String[]{"E:/tmp/inputorder/groupCompareOrder.txt","E:/tmp/outputorder/1"};

        //1 获取配置信息和实例化job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 设置 jar包加载路径
        job.setJarByClass(OrderSortDriver.class);
        //3 设置 加载 mapper、reducer类
        job.setMapperClass(OrderSortMapper.class);
        job.setReducerClass(OrderSortReducer.class);
        //4 设置 map输出数据kv类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5 设置 最终输出数据kv类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        //6 设置 输入输出数据路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //8 设置 reduce端的分组
        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

        //7 提交job
        job.waitForCompletion(true);
    }
}
