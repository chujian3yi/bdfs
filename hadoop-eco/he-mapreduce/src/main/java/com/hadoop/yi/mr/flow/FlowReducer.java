package com.hadoop.yi.mr.flow;

import com.hadoop.yi.mr.writable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 统计流量
 */
public class FlowReducer extends Reducer<Text, FlowBean,Text,FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //上行流量总
        long sum_upFlow = 0;
        // 下行流量总
        long sum_downFlow = 0;
        // 遍历所有 bean，将其中的上行流量和下行流量分别累加
        for (FlowBean bean : values) {
            sum_upFlow += bean.getUpFlow();
            sum_downFlow += bean.getDownFlow();
        }
        // 封装bean
        FlowBean resultBean = new FlowBean(sum_upFlow, sum_downFlow);
        // 写出
        context.write(key,resultBean);
    }
}
