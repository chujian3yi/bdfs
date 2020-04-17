package com.hadoop.yi.mr.sdpart;

import com.hadoop.yi.mr.flow.FlowMapper;
import com.hadoop.yi.mr.flow.FlowReducer;
import com.hadoop.yi.mr.writable.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MrAppMaster flow
 */
public class SDFlowDriver {

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

        //System.setProperty("hadoop.home.dir","E:\\repository\\hadoop-2.6.0-cdh5.14.0");

        //数据输入路径
        args = new String[]{"E:/tmp/inputflow","E:/tmp/outputflow/1"};
        // 1.获取配置信息，或者 job对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowBean.class);

        // 3.指定本业务 job要使用的 mapper、reducer业务类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4.指定 mapper输出数据 kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5.指定 reducer输出数据 kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 8.指定自定义分区
        job.setPartitionerClass(ProvincePartitioner.class);
        // 9.同时指定相应数量的 reducer task
        job.setNumReduceTasks(5);
        int numReduceTasks = job.getNumReduceTasks();
        System.out.println(numReduceTasks);
        // 6.指定job的输入输出文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7.将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
