package com.hadoop.yi.mr.sdpart;

import com.hadoop.yi.mr.writable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 自定义分区，实现按照省区号码前缀分区
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {


    public static HashMap<String,Integer> province = new HashMap<>();
    static {
        province.put("136",0);
        province.put("137",1);
        province.put("138",2);
        province.put("139",3);
    }

    /**
     * 自定义分区，实现手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中
     * @param key
     * @param flowBean
     * @param numReduceTasks
     * @return
     */
    @Override
    public int getPartition(Text key, FlowBean flowBean, int numReduceTasks) {

        // 获取电话号码前三位
        String preNum = key.toString().substring(0, 3);
        int partition = 4;
        // 判断分区
        partition = province.get(preNum);
        if (preNum != null){
            return partition;
        }
        return 4;
    }

    /*
     * 报错 illegal partition for  xxx()的原因：
     * 1. 非集群环境下，需要 apache版本hadoop依赖
     * 2. 检查代码逻辑和原理：
     *      partition默认值是从0开始的
     *      setNumReduceTasks(n); n=partition
     * */
}
