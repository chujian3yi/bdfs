package com.hadoop.yi.mr.compare;

import com.hadoop.yi.mr.writable.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriverForComparable {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("hadoop.home.dir", "E:\\repository\\hadoop-2.6.0-cdh5.14.0");

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[]{"E:/tmp/inputflow/phone_flow.txt", "E:/tmp/outputflow/3"};

        // 1.获取配置信息，或者 job对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 6.指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowDriverForComparable.class);

        // 2.指定本业务 job要使用的 mapper、reducer业务类
        job.setMapperClass(FlowMapperForComparable.class);
        job.setReducerClass(FlowReducerForComparable.class);

        // 3.指定 mapper输出数据 kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 4.指定 reducer输出数据 kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 5.指定job的输入输出文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7.将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);

    }
}