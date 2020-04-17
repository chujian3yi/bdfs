package com.hadoop.yi.mr.mjoin;

import com.hadoop.yi.mr.rjoin.TableBean;
import com.hadoop.yi.mr.rjoin.TableDriver;
import com.hadoop.yi.mr.rjoin.TableMapper;
import com.hadoop.yi.mr.rjoin.TableReducer;
import com.sun.jndi.toolkit.url.Uri;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DistributeCacheTableDriver {
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

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        args = new String[]{"E:\\tmp\\inputmj", "E:\\tmp\\outputmj\\2"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //job.addCacheFile(new URI("file:///e:/tmp/inputmj/pd.txt"));

        // 使用 DistributeCache的分布式缓存需要集群环境
        /*JobConf jobConf = new JobConf();
        DistributedCache.addCacheFile(new URI("file:/e:/xxx"),jobConf);*/


        job.setJarByClass(DistributeCacheTableDriver.class);

        job.setMapperClass(DistributedCacheTableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 此处不需要 reducer
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
