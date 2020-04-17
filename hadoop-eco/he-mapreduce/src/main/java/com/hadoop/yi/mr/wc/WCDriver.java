package com.hadoop.yi.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WCDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir","E:\\repository\\hadoop-2.6.0-cdh5.14.0");

        args = new String[]{"E:\\tmp\\inputwc","E:\\tmp\\outputwc\\2"};

        //1.获取配置信息连接、封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.jar path
        job.setJarByClass(WCDriver.class);

        //3. Mapper & Reducer By Class
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        //4.map output <key,value>
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.finally output <key,value>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.set input format & path
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7. job submit
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);

    }
}
