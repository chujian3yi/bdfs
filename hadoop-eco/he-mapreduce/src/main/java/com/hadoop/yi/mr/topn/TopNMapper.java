package com.hadoop.yi.mr.topn;

import com.hadoop.yi.mr.writable.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
/**
 * 每个maptask只要10条，最极端topn都在一个maptask
 **/
public class TopNMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    // 定义一个 TreeMap 作为存储数据的容器（天然按照key排序）
    private TreeMap<FlowBean,Text> flowMap = new TreeMap<>();
    private FlowBean kBean;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        kBean = new FlowBean();
        Text v = new Text();

        String line = value.toString();
        String[] fields = line.split("\t ");

        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);

        kBean.setUpFlow(upFlow);
        kBean.setDownFlow(downFlow);
        kBean.setSumFlow(sumFlow);
        v.set(phoneNum);

        // 向 TreeMap 中添加数据
        flowMap.put(kBean,v);
        // 限制 TreeMap 的数据量，超过十条就删除流量最小的一条数据
        if (flowMap.size() > 10){
            // flowMap.remove(flowMap.firstKey());
            flowMap.remove(flowMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 遍历 treeMap 集合，输出数据
        Iterator<FlowBean> beans = flowMap.keySet().iterator();
        while (beans.hasNext()){
            FlowBean k = beans.next();
            context.write(k,flowMap.get(k));
        }
    }
}
