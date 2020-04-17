package com.hadoop.yi.mr.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class LogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 输入输出路径根据数据源配置
        args = new String[]{"e://tmp/inputweblog/web.log","e://tmp/outputlog/"};

        // 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 加载 jar包
        job.setJarByClass(LogDriver.class);
        // 设置mapper / reducer（无reducer）
        job.setMapperClass(LogMapper.class);
        // 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置 reducetask 个数为0
        job.setNumReduceTasks(0);
        // 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 提交job
        boolean result = job.waitForCompletion(true);
    }
}
