package com.hadoop.yi.mr.kvif;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用 KyeValueTextInputFormat 做首单词相同行统计
 */
public class KVTextIFDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "E:\\repository\\hadoop-2.6.0-cdh5.14.0");

        args = new String[]{"E:/tmp/inputkv/kv.txt", "E:/tmp/outputkv/1"};

        //1 获取 job对象和封装配置信息
        Configuration conf = new Configuration();
        // 设置切割符
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
        Job job = Job.getInstance(conf);
        //2 设置jar包位置，关联 mapper和reducer
        job.setJarByClass(KVTextIFDriver.class);
        job.setMapperClass(KVTextIFMapper.class);
        job.setReducerClass(KVTextIFReducer.class);

        //3 设置 map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //4设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //5 设置输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //6 设置输出数据路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7 提交 job
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
