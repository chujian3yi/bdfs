package com.hadoop.yi.mr.mjoin;

import com.hadoop.yi.mr.rjoin.TableBean;
import javafx.scene.control.Tab;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * map join：使用 DistributeCache 缓存小文件，在mapper中合并，减少reduce端业务处理压力
 */
public class DistributedCacheTableMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


    Map<String, String> pdMap = new HashMap<>();
    Text k = new Text();

    /**
     * 预加载小表数据缓存，使用DistributeCache,在 Driver设置
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Mapper<LongWritable,Text,Text,NullWritable>.Context context) throws IOException, InterruptedException {

        //获取缓存文件
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].toString();
        //缓冲流去数据写到map
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(path), "UTF-8"));
        String line;
        while (StringUtils.isNotBlank(line = reader.readLine())) {
            //切割
            String[] fields = line.split("\t");
            //缓存数据到集合map
            pdMap.put(fields[0], fields[1]);
        }
        reader.close();
    }

    /**
     * 在map端完成小表join操作
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");
        String pdName = pdMap.get(fields[1]);
        k.set(line + "\t" + pdName);

        context.write(k, NullWritable.get());
    }
}
