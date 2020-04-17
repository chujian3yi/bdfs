package com.hadoop.yi.mr.nlif;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NLineDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("hadoop.home.dir", "E:/repository/hadoop-2.6.0-cdh5.14.0");

        args = new String[]{"E:\\tmp\\inputNL\\NL.txt", "E:\\tmp\\outputNL\\1"};

        // 1.封装配置信息，获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //7.设置每个切片 InputSplit 中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job, 3);

        //8.使用 NLineInputFormat 处理记录数
        job.setInputFormatClass(NLineInputFormat.class);

        // 2.设置jar包位置，关联 Mapper 和 Reducer
        job.setJarByClass(NLineDriver.class);
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        // 3.设置map输出kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 4.设置最终输出kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 5.设置数据输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6.提交job，到yarn上运行
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
