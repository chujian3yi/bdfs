package com.hadoop.yi.mr.sdif;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * 使用 SequenceFileOutputFormat 输出合并文件
 * Driver 处理流程：常规；指定自定义的输入格式；指定输出格式为 SequenceFileOutputFormat
 */
public class SequenceFileDriver {
    // 解决WindowsUtils
    static {
        try {
            // 设置 HADOOP_HOME 目录
            System.setProperty("hadoop.home.dir", "E:/repository/hadoop-2.6.0-cdh5.14.0");
            // 加载库文件
            System.load("E:/repository/hadoop-2.6.0-cdh5.14.0/bin/hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"E:\\tmp\\inputsdif", "E:\\tmp\\outputsdif\\2"};

        // 1.获取配置信息，获取Job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 7.设置 InputFormat，使用自定义的InputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);

        // 8.设置 OutputFormat，使用 SequenceFileOutputFormat，合并输出文件
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 2.设置jar
        job.setJarByClass(SequenceFileDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 3.设置map输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 4.设置最终输出kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 5.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6. 提交 job
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);


    }
}
